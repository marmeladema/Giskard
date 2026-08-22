//! Bounded previews of unbounded text.
//!
//! One UTF-8-safe truncation primitive, shared by every capped text field on disk. Retention work
//! and the history index cap different fields for different reasons, but they must agree on what
//! "capped at N bytes" means, or two records of the same text disagree about where it ends.

/// Cap for the prompt preview carried by a turn record.
///
/// Large enough that the first line or two of an ordinary prompt survives — which is what makes a
/// turn identifiable at a glance — and small enough that the index stays bounded per row.
pub const PROMPT_PREVIEW_MAX_BYTES: usize = 512;

/// Cap for the status message carried by a turn record.
///
/// A turn's status *kind* is strictly bounded, but its message is composed from provider error text
/// (message + supplementary details + classification) and has no ceiling. The index keeps a capped
/// rendering as a display hint; the payload file holds the message the harness actually reported.
pub const STATUS_MESSAGE_MAX_BYTES: usize = 512;

/// Truncate `text` to at most `max_bytes`, never splitting a UTF-8 character.
///
/// Returns the preview and whether anything was dropped. The flag is what a renderer uses to decide
/// whether to offer expansion; it is never a claim about *how much* was dropped.
pub fn bounded_preview(text: &str, max_bytes: usize) -> (String, bool) {
    if text.len() <= max_bytes {
        return (text.to_string(), false);
    }
    // `is_char_boundary` is true at 0, so this terminates even for a first character wider than
    // the cap — which yields an empty preview rather than invalid UTF-8.
    let mut end = max_bytes;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    (text[..end].to_string(), true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_within_the_cap_is_returned_whole_and_unflagged() {
        let (preview, truncated) = bounded_preview("fix the flaky test", 512);
        assert_eq!(preview, "fix the flaky test");
        assert!(!truncated);
    }

    #[test]
    fn truncation_never_splits_a_character() {
        // Four bytes per character, so a cap of 6 has to stop after the first one.
        let text = "🙂🙂";
        let (preview, truncated) = bounded_preview(text, 6);
        assert_eq!(preview, "🙂");
        assert!(truncated);

        // A cap narrower than the first character yields an empty preview, not invalid UTF-8.
        let (preview, truncated) = bounded_preview(text, 2);
        assert!(preview.is_empty());
        assert!(truncated);
    }

    #[test]
    fn a_cap_landing_exactly_on_a_boundary_keeps_everything_before_it() {
        let (preview, truncated) = bounded_preview("abcdef", 3);
        assert_eq!(preview, "abc");
        assert!(truncated);
    }
}
