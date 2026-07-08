//! Path linkification for agent messages (spec §11.2).
//!
//! Scans free-form text for strings that look like file paths (e.g. `src/lib.rs`,
//! `config.toml`, `src/lib.rs#42`, `src/lib.rs:42`) and returns byte-offset
//! spans for those that resolve to existing files within the workspace root.
//! The client renders these spans as clickable links that open the code overlay.
//!
//! ## Algorithm
//!
//! 1. A regex matches candidate tokens that look like absolute paths, explicit
//!    relative paths, slash-containing paths, or bare filenames with a dot.
//! 2. An optional `#<line>`, `:<line>`, or `:<line>:<column>` suffix is parsed
//!    as a target line and removed before the path is resolved against the
//!    workspace root.
//! 3. Each candidate is resolved against the workspace root (relative paths
//!    are joined, absolute paths are used as-is).
//! 4. The resolved path is canonicalized and must start with the workspace
//!    root — this prevents linkifying paths that escape via `..` or symlinks.
//! 5. Only paths pointing to existing **files** (not directories) are returned.

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
    pub line: Option<usize>,
}

/// Regex matching candidate file paths.
///
/// A candidate must be an absolute path, a `./` or `../` relative path, contain at least one
/// directory separator, or be a bare filename with a dot. The leading boundary avoids capturing
/// path-like substrings inside URLs or identifiers; final existence and confinement checks decide
/// whether a candidate is a real link. A final line fragment is part of the clickable span but
/// not the path validated against the filesystem.
static PATH_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r#"(?x)
        (?:^|[\s('"`\[{<])
        (
            (?:
                (?:\.{1,2}/|/)[A-Za-z0-9_.@+-]+(?:/[A-Za-z0-9_.@+-]+)*
                |
                [A-Za-z0-9_.@+-]+(?:/[A-Za-z0-9_.@+-]+)+
                |
                [A-Za-z0-9_@+-]*\.[A-Za-z0-9_.@+-]+
            )
            (?:
                \#[1-9][0-9]*
                |
                :[1-9][0-9]*(?::[1-9][0-9]*)?
            )?
        )
        "#,
    )
    .expect("hard-coded path linkification regex must compile")
});

/// Scan `text` for file paths and return spans for those that exist on disk.
///
/// Paths are resolved against `workspace_root`. Only files that (a) exist,
/// (b) are regular files (not directories), and (c) are inside the workspace
/// root after canonicalization are included in the result.
pub fn linkify_text(text: &str, workspace_root: &Path) -> Vec<LinkSpan> {
    let mut spans = Vec::new();
    for captures in PATH_PATTERN.captures_iter(text) {
        let Some(mat) = captures.get(1) else {
            continue;
        };
        let mut candidate = mat.as_str();
        let mut end = mat.end();
        while let Some(ch) = trailing_punctuation(candidate) {
            let new_len = candidate.len() - ch.len_utf8();
            candidate = &candidate[..new_len];
            end -= ch.len_utf8();
        }
        let (path_str, line) = split_line_fragment(candidate);
        if path_str.is_empty() {
            continue;
        }

        let resolved = resolve_path(path_str, workspace_root);
        if let Some(full_path) = resolved {
            if full_path.exists() && full_path.is_file() {
                let relative = full_path
                    .strip_prefix(workspace_root)
                    .map(|p| p.to_string_lossy().to_string())
                    .unwrap_or_else(|_| full_path.to_string_lossy().to_string());

                spans.push(LinkSpan {
                    start: mat.start(),
                    end,
                    path: relative,
                    line,
                });
            }
        }
    }
    spans
}

fn trailing_punctuation(candidate: &str) -> Option<char> {
    let ch = candidate.chars().next_back()?;
    matches!(ch, '.' | ',' | ';' | ':' | '!' | '?' | ')' | ']' | '}').then_some(ch)
}

fn split_line_fragment(candidate: &str) -> (&str, Option<usize>) {
    if let Some((path, line)) = candidate.rsplit_once('#') {
        return match parse_positive_usize(line) {
            Some(line) => (path, Some(line)),
            None => (candidate, None),
        };
    }

    let Some((before_last_colon, last)) = candidate.rsplit_once(':') else {
        return (candidate, None);
    };
    let Some(last_number) = parse_positive_usize(last) else {
        return (candidate, None);
    };

    if let Some((path, maybe_line)) = before_last_colon.rsplit_once(':') {
        if let Some(line) = parse_positive_usize(maybe_line) {
            return (path, Some(line));
        }
    }

    (before_last_colon, Some(last_number))
}

fn parse_positive_usize(value: &str) -> Option<usize> {
    match value.parse::<usize>() {
        Ok(value) if value > 0 => Some(value),
        _ => None,
    }
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

        let escaped = root.parent().unwrap().join("outside.rs");
        fs::write(&escaped, "").unwrap();
        let text = format!("see {}", escaped.display());
        let spans = linkify_text(&text, &root);
        assert!(spans.is_empty());
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

    /// Absolute paths inside the workspace should be linkified and returned as workspace-relative
    /// paths for stable client requests.
    #[test]
    fn linkify_absolute_workspace_path() {
        let tmp = tempfile::TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();
        fs::create_dir_all(root.join("src")).unwrap();
        let path = root.join("src/main.rs");
        fs::write(&path, "fn main() {}").unwrap();

        let text = format!("Created the file at {}.", path.display());
        let spans = linkify_text(&text, &root);
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].path, "src/main.rs");
        assert_eq!(&text[spans[0].start..spans[0].end], path.to_str().unwrap());
    }

    /// Sentence punctuation after a path should not become part of the filesystem candidate.
    #[test]
    fn linkify_trims_sentence_punctuation() {
        let tmp = tempfile::TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();
        fs::write(root.join("main.rs"), "").unwrap();

        let text = "Open main.rs.";
        let spans = linkify_text(text, &root);
        assert_eq!(spans.len(), 1);
        assert_eq!(&text[spans[0].start..spans[0].end], "main.rs");
    }

    /// A `#<line>` suffix should be clickable with the path but should not be part of the
    /// filesystem path that gets validated.
    #[test]
    fn linkify_line_fragment() {
        let tmp = tempfile::TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();
        fs::create_dir_all(root.join("src")).unwrap();
        fs::write(root.join("src/main.rs"), "fn main() {}\n").unwrap();

        let text = "Open src/main.rs#12 now";
        let spans = linkify_text(text, &root);
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].path, "src/main.rs");
        assert_eq!(spans[0].line, Some(12));
        assert_eq!(&text[spans[0].start..spans[0].end], "src/main.rs#12");
    }

    /// A `:<line>` suffix is also recognized because compiler output and some agents use that
    /// form for source locations.
    #[test]
    fn linkify_colon_line_fragment() {
        let tmp = tempfile::TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();
        fs::write(root.join("main.rs"), "fn main() {}\n").unwrap();

        let text = "Open main.rs:7 now";
        let spans = linkify_text(text, &root);
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].path, "main.rs");
        assert_eq!(spans[0].line, Some(7));
        assert_eq!(&text[spans[0].start..spans[0].end], "main.rs:7");
    }

    /// A compiler-style `:<line>:<column>` suffix should target the line and keep the whole source
    /// location clickable.
    #[test]
    fn linkify_colon_line_column_fragment() {
        let tmp = tempfile::TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();
        fs::create_dir_all(root.join("src")).unwrap();
        fs::write(root.join("src/main.rs"), "fn main() {}\n").unwrap();

        let text = "Open src/main.rs:7:13 now";
        let spans = linkify_text(text, &root);
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].path, "src/main.rs");
        assert_eq!(spans[0].line, Some(7));
        assert_eq!(&text[spans[0].start..spans[0].end], "src/main.rs:7:13");
    }

    /// Malformed line fragments should not fabricate a line target. The base file may still be
    /// linkified when it exists, but navigation remains a normal file open.
    #[test]
    fn linkify_invalid_line_fragment_has_no_line_target() {
        let tmp = tempfile::TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();
        fs::write(root.join("main.rs"), "").unwrap();

        for text in ["Open main.rs:abc", "Open main.rs:0", "Open main.rs#0"] {
            let spans = linkify_text(text, &root);
            assert_eq!(spans.len(), 1, "{text}");
            assert_eq!(spans[0].path, "main.rs", "{text}");
            assert_eq!(spans[0].line, None, "{text}");
            assert_eq!(&text[spans[0].start..spans[0].end], "main.rs", "{text}");
        }
    }

    /// Sentence punctuation after a line fragment should stay outside the clickable span.
    #[test]
    fn linkify_line_fragment_trims_sentence_punctuation() {
        let tmp = tempfile::TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();
        fs::write(root.join("main.rs"), "").unwrap();

        let text = "Open main.rs#3.";
        let spans = linkify_text(text, &root);
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].line, Some(3));
        assert_eq!(&text[spans[0].start..spans[0].end], "main.rs#3");
    }
}
