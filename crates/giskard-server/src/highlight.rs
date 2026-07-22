//! Syntax highlighting for the code overlay (spec §11.2/§11.3).
//!
//! Uses [`syntect`] to produce highlighted HTML from file contents. Results are
//! cached per `(path, mtime)` pair so repeated requests for the same file (e.g.
//! when paginating through line ranges) avoid re-tokenizing.
//!
//! Highlighting is done **one source line at a time** (via [`HighlightLines`]) and
//! the per-line HTML fragments are cached. A range request slices those fragments
//! and re-wraps them in a `<pre>` element, so line ranges map exactly to source
//! lines and always produce well-formed HTML (§11.3 pagination).
//!
//! Binary files (detected via null-byte check in the first 8 KiB) and files
//! exceeding the configurable size threshold return an empty HTML body with
//! metadata only, so the UI can show a fallback message.

use std::path::Path;
use std::sync::{Arc, LazyLock};

use syntect::easy::HighlightLines;
use syntect::highlighting::{Color, Theme, ThemeSet};
use syntect::html::{IncludeBackground, styled_line_to_highlighted_html};
use syntect::parsing::{SyntaxReference, SyntaxSet};
use syntect::util::LinesWithEndings;
use tokio::sync::Mutex;
use tracing::warn;

/// Default maximum file size for highlighting (10 MiB).
const DEFAULT_MAX_HIGHLIGHT_SIZE: usize = 10 * 1024 * 1024;

/// Maximum number of cached highlight results before FIFO eviction kicks in.
const MAX_CACHE_ENTRIES: usize = 128;

static SNIPPET_SYNTAX_SET: LazyLock<SyntaxSet> = LazyLock::new(two_face::syntax::extra_newlines);
static SNIPPET_THEME_SET: LazyLock<ThemeSet> = LazyLock::new(ThemeSet::load_defaults);

/// The cached, fully-highlighted form of a file: one HTML fragment per source line
/// plus the `<pre …>` opener carrying the theme background. Range requests slice
/// `lines` and re-wrap, so one cache entry serves every range of the file.
struct CachedHighlight {
    /// Per-source-line highlighted HTML (each fragment includes the line's trailing newline).
    lines: Vec<String>,
    /// `<pre style="background-color:#…;">` opener matching the theme.
    pre_open: String,
    language: Option<String>,
    is_binary: bool,
    file_size: u64,
}

/// A cached highlight result keyed by file mtime.
struct CacheEntry {
    mtime: std::time::SystemTime,
    cached: Arc<CachedHighlight>,
}

/// Syntax highlighter with per-file caching (spec §11.2).
///
/// Created once at startup and stored in [`AppState`](crate::AppState) as an
/// `Arc<Highlighter>`. The `max_size` field controls the size threshold above
/// which files are not highlighted (spec §11.3: "configurable size threshold").
pub struct Highlighter {
    syntax_set: SyntaxSet,
    theme_set: ThemeSet,
    cache: Mutex<Vec<(std::path::PathBuf, CacheEntry)>>,
    /// Maximum file size in bytes for highlighting (spec §11.3).
    max_size: usize,
}

/// Result of highlighting a file (spec §11.2/§11.3).
///
/// Carries the highlighted HTML, detected language, binary flag, total line count,
/// and file size — everything the code overlay needs to display the file metadata
/// alongside its syntax-highlighted content.
#[derive(Debug, Clone)]
pub struct HighlightResult {
    /// Syntax-highlighted HTML (empty for binary or oversized files). For a line-range
    /// request this is a complete `<pre>…</pre>` containing only the requested lines.
    pub html: String,
    /// Language name detected from the file extension (e.g. "Rust", "Python").
    pub language: Option<String>,
    /// True if the file contains null bytes in its header (§11.3).
    pub is_binary: bool,
    /// Total number of lines in the file (before any range slicing).
    pub total_lines: usize,
    /// File size in bytes (spec §11.2: overlay shows path, size, and language).
    pub file_size: u64,
}

/// Syntax-highlighted HTML for a Markdown fenced code block.
#[derive(Debug, Clone)]
pub struct SnippetHighlight {
    /// Inner HTML fragments for the code body, without a surrounding `<pre>` wrapper.
    pub html: String,
    /// Human-readable language label for the code block header.
    pub language: String,
    /// True when the fence language was recognized by syntect.
    pub recognized_language: bool,
}

impl Highlighter {
    /// Create a new highlighter with the default 10 MiB size threshold.
    pub fn new() -> Self {
        Self::with_max_size(DEFAULT_MAX_HIGHLIGHT_SIZE)
    }

    /// Create a new highlighter with a custom maximum file size (spec §11.3).
    pub fn with_max_size(max_size: usize) -> Self {
        Self {
            syntax_set: two_face::syntax::extra_newlines(),
            theme_set: ThemeSet::load_defaults(),
            cache: Mutex::new(Vec::new()),
            max_size,
        }
    }

    /// Highlight a file, returning cached HTML if the file hasn't changed.
    ///
    /// `start_line` / `end_line` are 1-based and inclusive. When both are `None`,
    /// the entire file is returned. Range slicing operates on the cached per-line
    /// HTML, so different ranges of the same file share one cache entry and the
    /// output is always a well-formed `<pre>` block containing exactly the
    /// requested source lines.
    pub async fn highlight_file(
        &self,
        path: &Path,
        start_line: Option<usize>,
        end_line: Option<usize>,
    ) -> Result<HighlightResult, String> {
        let metadata = tokio::fs::metadata(path)
            .await
            .map_err(|e| format!("cannot read file metadata: {e}"))?;

        let mtime = metadata
            .modified()
            .map_err(|e| format!("cannot read mtime: {e}"))?;

        let file_size = metadata.len();

        if (file_size as usize) > self.max_size {
            return Ok(HighlightResult {
                html: String::new(),
                language: detect_language(path),
                is_binary: false,
                total_lines: 0,
                file_size,
            });
        }

        let cache_key = path.to_path_buf();
        {
            let cache = self.cache.lock().await;
            if let Some((_, entry)) = cache.iter().find(|(k, _)| *k == cache_key) {
                if entry.mtime == mtime {
                    return Ok(apply_range(&entry.cached, start_line, end_line));
                }
            }
        }

        let bytes = tokio::fs::read(path)
            .await
            .map_err(|e| format!("cannot read file: {e}"))?;

        if is_binary(&bytes) {
            let cached = Arc::new(CachedHighlight {
                lines: Vec::new(),
                pre_open: String::new(),
                language: detect_language(path),
                is_binary: true,
                file_size,
            });
            self.store_cache(cache_key, mtime, cached.clone()).await;
            return Ok(apply_range(&cached, start_line, end_line));
        }

        let text = String::from_utf8_lossy(&bytes).to_string();

        let syntax = self
            .syntax_set
            .find_syntax_for_file(path)
            .ok()
            .flatten()
            .unwrap_or_else(|| self.syntax_set.find_syntax_plain_text());
        let language = syntax.name.clone();

        let theme = select_theme(&self.theme_set).ok_or("no syntax theme available")?;
        let bg = theme_background(theme);
        let pre_open = format!(
            "<pre style=\"background-color:#{:02x}{:02x}{:02x};\">",
            bg.r, bg.g, bg.b
        );

        // Highlight one source line at a time so a range request maps exactly to source lines.
        let mut highlighter = HighlightLines::new(syntax, theme);
        let mut lines = Vec::new();
        for line in LinesWithEndings::from(&text) {
            let fragment = match highlighter.highlight_line(line, &self.syntax_set) {
                Ok(ranges) => styled_line_to_highlighted_html(&ranges, IncludeBackground::No)
                    .unwrap_or_else(|_| escape_html(line)),
                Err(e) => {
                    warn!(?path, %e, "line highlighting failed, escaping");
                    escape_html(line)
                }
            };
            lines.push(fragment);
        }

        let cached = Arc::new(CachedHighlight {
            lines,
            pre_open,
            language: Some(language),
            is_binary: false,
            file_size,
        });

        self.store_cache(cache_key, mtime, cached.clone()).await;
        Ok(apply_range(&cached, start_line, end_line))
    }

    /// Store a cached highlight, evicting the oldest entry (FIFO) if full.
    async fn store_cache(
        &self,
        key: std::path::PathBuf,
        mtime: std::time::SystemTime,
        cached: Arc<CachedHighlight>,
    ) {
        let mut cache = self.cache.lock().await;
        if cache.len() >= MAX_CACHE_ENTRIES {
            cache.remove(0);
        }
        cache.push((key, CacheEntry { mtime, cached }));
    }
}

impl Default for Highlighter {
    fn default() -> Self {
        Self::new()
    }
}

/// Highlight a Markdown fenced code block using the same server-side highlighter family as the
/// code overlay. Unknown language tokens fall back to escaped plain text while still returning a
/// label derived from the fence, so the UI can show what the agent declared.
pub fn highlight_snippet(source: &str, language_token: Option<&str>) -> SnippetHighlight {
    let token = language_token.and_then(safe_language_token);
    let syntax = token
        .as_deref()
        .and_then(|t| SNIPPET_SYNTAX_SET.find_syntax_by_token(t));
    let recognized_language = syntax.is_some();
    let syntax = syntax.unwrap_or_else(|| SNIPPET_SYNTAX_SET.find_syntax_plain_text());
    let language = snippet_language_label(token.as_deref(), syntax, recognized_language);

    let Some(theme) = select_theme(&SNIPPET_THEME_SET) else {
        return SnippetHighlight {
            html: escape_html(source),
            language,
            recognized_language: false,
        };
    };

    let mut highlighter = HighlightLines::new(syntax, theme);
    let mut html = String::new();
    for line in LinesWithEndings::from(source) {
        match highlighter.highlight_line(line, &SNIPPET_SYNTAX_SET) {
            Ok(ranges) => html.push_str(
                &styled_line_to_highlighted_html(&ranges, IncludeBackground::No)
                    .unwrap_or_else(|_| escape_html(line)),
            ),
            Err(e) => {
                warn!(language = ?token, %e, "snippet highlighting failed, escaping line");
                html.push_str(&escape_html(line));
            }
        }
    }

    SnippetHighlight {
        html,
        language,
        recognized_language,
    }
}

/// Build a [`HighlightResult`] for the requested 1-based inclusive line range from a cached file.
///
/// Slicing operates on whole per-line HTML fragments, so the result is always a complete
/// `<pre>…</pre>` and the returned lines correspond exactly to the requested source lines.
fn apply_range(
    cached: &CachedHighlight,
    start: Option<usize>,
    end: Option<usize>,
) -> HighlightResult {
    let total_lines = cached.lines.len();

    if cached.is_binary || cached.file_size == 0 {
        return HighlightResult {
            html: String::new(),
            language: cached.language.clone(),
            is_binary: cached.is_binary,
            total_lines,
            file_size: cached.file_size,
        };
    }

    let start_idx = start.unwrap_or(1).saturating_sub(1).min(total_lines);
    let end_idx = end.unwrap_or(total_lines).min(total_lines).max(start_idx);

    let body: String = cached.lines[start_idx..end_idx].concat();
    let html = format!("{}{}</pre>", cached.pre_open, body);

    HighlightResult {
        html,
        language: cached.language.clone(),
        is_binary: false,
        total_lines,
        file_size: cached.file_size,
    }
}

/// Heuristic binary detection: check the first 8 KiB for null bytes (spec §11.3).
fn is_binary(bytes: &[u8]) -> bool {
    if bytes.is_empty() {
        return false;
    }
    let check_len = bytes.len().min(8192);
    bytes[..check_len].contains(&0)
}

/// Map a file extension to a human-readable language name for the overlay metadata.
fn detect_language(path: &Path) -> Option<String> {
    let ext = path.extension()?.to_string_lossy().to_string();
    Some(match ext.as_str() {
        "rs" => "Rust".into(),
        "py" => "Python".into(),
        "js" | "mjs" => "JavaScript".into(),
        "ts" => "TypeScript".into(),
        "go" => "Go".into(),
        "java" => "Java".into(),
        "c" | "h" => "C".into(),
        "cpp" | "hpp" | "cc" => "C++".into(),
        "rb" => "Ruby".into(),
        "sh" | "bash" => "Shell".into(),
        "toml" => "TOML".into(),
        "json" => "JSON".into(),
        "yaml" | "yml" => "YAML".into(),
        "md" => "Markdown".into(),
        "html" => "HTML".into(),
        "css" => "CSS".into(),
        "sql" => "SQL".into(),
        _ => ext,
    })
}

fn select_theme(theme_set: &ThemeSet) -> Option<&Theme> {
    theme_set
        .themes
        .get("base16-ocean.dark")
        .or_else(|| theme_set.themes.values().next())
}

fn theme_background(theme: &Theme) -> Color {
    theme.settings.background.unwrap_or(Color {
        r: 0,
        g: 0,
        b: 0,
        a: 0,
    })
}

fn safe_language_token(token: &str) -> Option<String> {
    let token: String = token
        .trim()
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '-' || *c == '+')
        .collect();
    if token.is_empty() { None } else { Some(token) }
}

fn snippet_language_label(
    token: Option<&str>,
    syntax: &SyntaxReference,
    recognized_language: bool,
) -> String {
    if recognized_language {
        return syntax.name.clone();
    }

    token
        .map(display_language_token)
        .unwrap_or_else(|| "Plain Text".into())
}

fn display_language_token(token: &str) -> String {
    match token.to_ascii_lowercase().as_str() {
        "rs" | "rust" => "Rust".into(),
        "js" | "javascript" | "mjs" => "JavaScript".into(),
        "ts" | "typescript" => "TypeScript".into(),
        "py" | "python" => "Python".into(),
        "sh" | "bash" | "shell" => "Shell".into(),
        "json" => "JSON".into(),
        "yaml" | "yml" => "YAML".into(),
        "toml" => "TOML".into(),
        "html" => "HTML".into(),
        "css" => "CSS".into(),
        "sql" => "SQL".into(),
        "md" | "markdown" => "Markdown".into(),
        _ => token.to_string(),
    }
}

/// Escape HTML special characters as a fallback when syntect highlighting fails.
fn escape_html(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binary_detection() {
        assert!(!is_binary(b"hello world"));
        assert!(is_binary(b"hello\x00world"));
        assert!(!is_binary(b""));
    }

    #[test]
    fn language_detection() {
        assert_eq!(
            detect_language(Path::new("/src/main.rs")),
            Some("Rust".into())
        );
        assert_eq!(
            detect_language(Path::new("/script.py")),
            Some("Python".into())
        );
        assert_eq!(
            detect_language(Path::new("/config.toml")),
            Some("TOML".into())
        );
        assert_eq!(detect_language(Path::new("/noext")), None);
    }

    #[test]
    fn highlight_rust_snippet_reports_language() {
        let result = highlight_snippet("fn main() {}\n", Some("rust"));
        assert_eq!(result.language, "Rust");
        assert!(result.recognized_language);
        assert!(result.html.contains("fn"));
    }

    #[test]
    fn highlight_toml_snippet_reports_language() {
        let result = highlight_snippet("[server]\nbind = \"127.0.0.1:0\"\n", Some("toml"));
        assert_eq!(result.language, "TOML");
        assert!(result.recognized_language);
        assert!(result.html.contains("<span"));
    }

    #[test]
    fn highlight_diff_snippet_reports_language() {
        let result = highlight_snippet("--- old\n+++ new\n-removed\n+added\n", Some("diff"));
        assert_eq!(result.language, "Diff");
        assert!(result.recognized_language);
        assert!(result.html.contains("<span"));
    }

    #[test]
    fn highlight_unknown_snippet_escapes_and_keeps_label() {
        let result = highlight_snippet("<tag>\n", Some("not-real"));
        assert_eq!(result.language, "not-real");
        assert!(!result.recognized_language);
        assert!(result.html.contains("&lt;tag&gt;"));
        assert!(!result.html.contains("<tag>"));
    }

    #[tokio::test]
    async fn highlight_rust_file() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let content = "fn main() {\n    println!(\"hello\");\n}\n";
        tokio::fs::write(tmp.path(), content).await.unwrap();

        let h = Highlighter::new();
        let result = h.highlight_file(tmp.path(), None, None).await.unwrap();
        assert!(!result.is_binary);
        assert_eq!(result.total_lines, 3);
        assert!(result.html.contains("fn"));
        assert!(result.html.starts_with("<pre"));
        assert!(result.html.ends_with("</pre>"));
        assert_eq!(result.file_size, content.len() as u64);
    }

    #[tokio::test]
    async fn highlight_toml_file() {
        let tmp = tempfile::NamedTempFile::with_suffix(".toml").unwrap();
        let content = "[server]\nbind = \"127.0.0.1:0\"\nsecure_cookies = false\n";
        tokio::fs::write(tmp.path(), content).await.unwrap();

        let h = Highlighter::new();
        let result = h.highlight_file(tmp.path(), None, None).await.unwrap();
        assert!(!result.is_binary);
        assert_eq!(result.language, Some("TOML".into()));
        assert!(result.html.contains("<span"));
        assert_eq!(result.file_size, content.len() as u64);
    }

    #[tokio::test]
    async fn highlight_binary_file() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let content = b"hello\x00binary\x00data";
        tokio::fs::write(tmp.path(), content).await.unwrap();

        let h = Highlighter::new();
        let result = h.highlight_file(tmp.path(), None, None).await.unwrap();
        assert!(result.is_binary);
        assert!(result.html.is_empty());
        assert_eq!(result.file_size, content.len() as u64);
    }

    #[tokio::test]
    async fn highlight_cache_hit() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        tokio::fs::write(tmp.path(), "let x = 42;\n").await.unwrap();

        let h = Highlighter::new();
        let r1 = h.highlight_file(tmp.path(), None, None).await.unwrap();
        let r2 = h.highlight_file(tmp.path(), None, None).await.unwrap();
        assert_eq!(r1.html, r2.html);
        assert_eq!(r1.file_size, r2.file_size);
    }

    #[tokio::test]
    async fn highlight_line_range_is_well_formed_and_correct() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        // Distinct, easily-identifiable content per line.
        tokio::fs::write(tmp.path(), "alpha\nbravo\ncharlie\ndelta\necho\n")
            .await
            .unwrap();

        let h = Highlighter::new();
        let full = h.highlight_file(tmp.path(), None, None).await.unwrap();
        assert_eq!(full.total_lines, 5);
        for word in ["alpha", "bravo", "charlie", "delta", "echo"] {
            assert!(full.html.contains(word));
        }

        // Lines 2..=4 only.
        let ranged = h
            .highlight_file(tmp.path(), Some(2), Some(4))
            .await
            .unwrap();
        assert_eq!(ranged.total_lines, 5, "total_lines reflects the whole file");
        assert!(
            ranged.html.starts_with("<pre"),
            "range output is well-formed"
        );
        assert!(ranged.html.ends_with("</pre>"));
        assert!(ranged.html.contains("bravo"));
        assert!(ranged.html.contains("charlie"));
        assert!(ranged.html.contains("delta"));
        assert!(
            !ranged.html.contains("alpha"),
            "line 1 must be excluded from range 2..=4"
        );
        assert!(
            !ranged.html.contains("echo"),
            "line 5 must be excluded from range 2..=4"
        );
    }

    #[tokio::test]
    async fn highlight_oversized_file() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let content = "x".repeat(200);
        tokio::fs::write(tmp.path(), &content).await.unwrap();

        let h = Highlighter::with_max_size(100);
        let result = h.highlight_file(tmp.path(), None, None).await.unwrap();
        assert!(result.html.is_empty());
        assert_eq!(result.file_size, 200);
    }
}
