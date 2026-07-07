//! Syntax highlighting for the code overlay (spec §11.2/§11.3).
//!
//! Uses [`syntect`] to produce highlighted HTML from file contents. Results are
//! cached per `(path, mtime)` pair so repeated requests for the same file (e.g.
//! when paginating through line ranges) avoid re-tokenizing.
//!
//! Binary files (detected via null-byte check in the first 8 KiB) and files
//! exceeding the configurable size threshold return an empty HTML body with
//! metadata only, so the UI can show a fallback message.

use std::path::Path;
use std::sync::Arc;

use syntect::highlighting::ThemeSet;
use syntect::html::highlighted_html_for_string;
use syntect::parsing::SyntaxSet;
use tokio::sync::Mutex;
use tracing::warn;

/// Default maximum file size for highlighting (10 MiB).
const DEFAULT_MAX_HIGHLIGHT_SIZE: usize = 10 * 1024 * 1024;

/// Maximum number of cached highlight results before FIFO eviction kicks in.
const MAX_CACHE_ENTRIES: usize = 128;

/// A cached highlight result keyed by file mtime.
struct CacheEntry {
    mtime: std::time::SystemTime,
    result: Arc<HighlightResult>,
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
    /// Syntax-highlighted HTML (empty for binary or oversized files).
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

impl Highlighter {
    /// Create a new highlighter with the default 10 MiB size threshold.
    pub fn new() -> Self {
        Self::with_max_size(DEFAULT_MAX_HIGHLIGHT_SIZE)
    }

    /// Create a new highlighter with a custom maximum file size (spec §11.3).
    pub fn with_max_size(max_size: usize) -> Self {
        Self {
            syntax_set: SyntaxSet::load_defaults_newlines(),
            theme_set: ThemeSet::load_defaults(),
            cache: Mutex::new(Vec::new()),
            max_size,
        }
    }

    /// Highlight a file, returning cached HTML if the file hasn't changed.
    ///
    /// `start_line` / `end_line` are 1-based and inclusive. When both are
    /// `None`, the entire file is returned. The cache stores the full-file
    /// result; range slicing is applied on every call (cheap — just string
    /// line splitting) so different ranges of the same file share one cache
    /// entry.
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

        let file_size = metadata.len() as u64;

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
                    let result = entry.result.clone();
                    return Ok(apply_range(&result, start_line, end_line));
                }
            }
        }

        let bytes = tokio::fs::read(path)
            .await
            .map_err(|e| format!("cannot read file: {e}"))?;

        if is_binary(&bytes) {
            let result = Arc::new(HighlightResult {
                html: String::new(),
                language: detect_language(path),
                is_binary: true,
                total_lines: 0,
                file_size,
            });
            self.store_cache(cache_key, mtime, result.clone()).await;
            return Ok(apply_range(&result, start_line, end_line));
        }

        let text = String::from_utf8_lossy(&bytes).to_string();
        let total_lines = text.lines().count();

        let syntax = self
            .syntax_set
            .find_syntax_for_file(path)
            .ok()
            .flatten()
            .unwrap_or_else(|| self.syntax_set.find_syntax_plain_text());

        let language = syntax.name.clone();

        let theme = self
            .theme_set
            .themes
            .get("base16-ocean.dark")
            .or_else(|| self.theme_set.themes.values().next())
            .ok_or("no syntax theme available")?;

        let html = match highlighted_html_for_string(&text, &self.syntax_set, syntax, theme) {
            Ok(html) => html,
            Err(e) => {
                warn!(?path, %e, "syntax highlighting failed, serving plain text");
                escape_html(&text)
            }
        };

        let result = Arc::new(HighlightResult {
            html,
            language: Some(language),
            is_binary: false,
            total_lines,
            file_size,
        });

        self.store_cache(cache_key, mtime, result.clone()).await;
        Ok(apply_range(&result, start_line, end_line))
    }

    /// Store a highlight result in the cache, evicting the oldest entry if full.
    async fn store_cache(
        &self,
        key: std::path::PathBuf,
        mtime: std::time::SystemTime,
        result: Arc<HighlightResult>,
    ) {
        let mut cache = self.cache.lock().await;
        if cache.len() >= MAX_CACHE_ENTRIES {
            cache.remove(0);
        }
        cache.push((key, CacheEntry { mtime, result }));
    }
}

impl Default for Highlighter {
    fn default() -> Self {
        Self::new()
    }
}

/// Slice a cached highlight result to the requested 1-based line range.
///
/// The `file_size` field is always preserved from the cached full-file result,
/// since range slicing doesn't change the underlying file size.
fn apply_range(
    result: &HighlightResult,
    start: Option<usize>,
    end: Option<usize>,
) -> HighlightResult {
    if result.is_binary || start.is_none() && end.is_none() {
        return result.clone();
    }

    let start = start.unwrap_or(1).saturating_sub(1);
    let end = end.unwrap_or(result.total_lines);

    let lines: Vec<&str> = result.html.lines().collect();
    let slice: Vec<&str> = lines
        .iter()
        .skip(start)
        .take(end.saturating_sub(start))
        .copied()
        .collect();

    HighlightResult {
        html: slice.join("\n"),
        language: result.language.clone(),
        is_binary: result.is_binary,
        total_lines: result.total_lines,
        file_size: result.file_size,
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
        assert_eq!(detect_language(Path::new("/noext")), None);
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
    async fn highlight_line_range() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        tokio::fs::write(tmp.path(), "line1\nline2\nline3\nline4\nline5\n")
            .await
            .unwrap();

        let h = Highlighter::new();
        let result = h
            .highlight_file(tmp.path(), Some(2), Some(4))
            .await
            .unwrap();
        assert_eq!(result.total_lines, 5);
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
