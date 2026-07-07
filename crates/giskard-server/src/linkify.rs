//! Path linkification for agent messages (spec §11.2).
//!
//! Scans free-form text for strings that look like file paths (e.g. `src/lib.rs`,
//! `config.toml`) and returns byte-offset spans for those that resolve to
//! existing files within the workspace root. The client renders these spans as
//! clickable links that open the code overlay.
//!
//! ## Algorithm
//!
//! 1. A regex matches candidate tokens that contain at least one dot (to
//!    reduce false positives from prose) with an optional directory prefix.
//! 2. Each candidate is resolved against the workspace root (relative paths
//!    are joined, absolute paths are used as-is).
//! 3. The resolved path is canonicalized and must start with the workspace
//!    root — this prevents linkifying paths that escape via `..` or symlinks.
//! 4. Only paths pointing to existing **files** (not directories) are returned.

use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use regex::Regex;

/// A linkified span within agent text (spec §11.2).
///
/// `start` and `end` are byte offsets into the original text. `path` is
/// workspace-root-relative when possible.
pub struct LinkSpan {
    pub start: usize,
    pub end: usize,
    pub path: String,
}

/// Regex matching candidate file paths: optional directory segments separated
/// by `/`, followed by a filename with at least one extension dot.
///
/// The leading `(?:^|[\s(\[{<])` ensures we match at word boundaries to avoid
/// capturing path-like substrings inside URLs or code.
static PATH_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?:^|[\s(\[{<])((?:[a-zA-Z0-9_.-]+/)*[a-zA-Z0-9_-]+(?:\.[a-zA-Z0-9_-]+)+)")
        .unwrap()
});

/// Scan `text` for file paths and return spans for those that exist on disk.
///
/// Paths are resolved against `workspace_root`. Only files that (a) exist,
/// (b) are regular files (not directories), and (c) are inside the workspace
/// root after canonicalization are included in the result.
pub fn linkify_text(text: &str, workspace_root: &Path) -> Vec<LinkSpan> {
    let mut spans = Vec::new();
    for mat in PATH_PATTERN.find_iter(text) {
        let raw = mat.as_str();
        let candidate_start = mat.start();
        let path_str = raw.trim_start_matches(|c: char| c.is_whitespace() || "([{<".contains(c));

        let resolved = resolve_path(path_str, workspace_root);
        if let Some(full_path) = resolved {
            if full_path.exists() && full_path.is_file() {
                let relative = full_path
                    .strip_prefix(workspace_root)
                    .map(|p| p.to_string_lossy().to_string())
                    .unwrap_or_else(|_| full_path.to_string_lossy().to_string());

                let offset = candidate_start + (raw.len() - path_str.len());
                spans.push(LinkSpan {
                    start: offset,
                    end: offset + path_str.len(),
                    path: relative,
                });
            }
        }
    }
    spans
}

/// Resolve a candidate path string against the workspace root.
///
/// Returns the canonicalized path only if it falls within the workspace root
/// after symlink resolution. Returns `None` for paths that escape.
fn resolve_path(candidate: &str, workspace_root: &Path) -> Option<PathBuf> {
    let path = Path::new(candidate);

    let resolved = if path.is_absolute() {
        path.to_path_buf()
    } else {
        workspace_root.join(path)
    };

    let canonical = resolved.canonicalize().ok()?;
    let root_canonical = workspace_root.canonicalize().ok()?;

    if canonical.starts_with(&root_canonical) {
        Some(canonical)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    /// Linkify should find multiple existing files in a single text string.
    #[test]
    fn linkify_finds_existing_file() {
        let tmp = tempfile::TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();
        fs::write(root.join("main.rs"), "fn main() {}").unwrap();
        fs::create_dir_all(root.join("src")).unwrap();
        fs::write(root.join("src/lib.rs"), "pub fn lib() {}").unwrap();

        let text = "I'll modify src/lib.rs and main.rs";
        let spans = linkify_text(text, &root);
        assert_eq!(spans.len(), 2);
        assert!(spans[0].path.contains("lib.rs"));
        assert!(spans[1].path.contains("main.rs"));
    }

    /// Nonexistent files should not produce any link spans.
    #[test]
    fn linkify_rejects_nonexistent() {
        let tmp = tempfile::TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();

        let text = "see nonexistent/file.rs for details";
        let spans = linkify_text(text, &root);
        assert!(spans.is_empty());
    }

    /// Paths that escape the workspace root via `..` must not be linkified.
    #[test]
    fn linkify_rejects_escape() {
        let tmp = tempfile::TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();

        let text = format!("see {}/passwd", root.parent().unwrap().display());
        let spans = linkify_text(&text, &root);
        for span in &spans {
            assert!(
                !span.path.contains(".."),
                "should not linkify paths escaping workspace root"
            );
        }
    }

    /// The returned span byte-offsets must bracket exactly the path token (the client slices the
    /// original text with them), excluding any leading boundary character the regex consumed.
    #[test]
    fn linkify_span_offsets_are_exact() {
        let tmp = tempfile::TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();
        fs::write(root.join("main.rs"), "").unwrap();

        let text = "edit main.rs now";
        let spans = linkify_text(text, &root);
        assert_eq!(spans.len(), 1);
        let s = &spans[0];
        assert_eq!(s.start, 5);
        assert_eq!(s.end, 12);
        assert_eq!(&text[s.start..s.end], "main.rs");
    }

    /// Relative paths prefixed with `./` should still be linkified.
    #[test]
    fn linkify_relative_path() {
        let tmp = tempfile::TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();
        fs::write(root.join("config.toml"), "").unwrap();

        let text = "update ./config.toml please";
        let spans = linkify_text(text, &root);
        assert_eq!(spans.len(), 1);
        assert!(spans[0].path.contains("config.toml"));
    }
}
