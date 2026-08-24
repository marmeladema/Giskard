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

/// Maximum completed-command output preview sent eagerly to a browser.
pub const COMMAND_OUTPUT_PREVIEW_MAX_BYTES: usize = 8 * 1024;

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

/// Count newline-separated logical lines. A final newline does not introduce an empty line.
pub fn logical_line_count(text: &str) -> u64 {
    giskard_core::command_output_logical_lines(text)
}

/// Retain a UTF-8-safe tail with a marker that counts toward `max_bytes`.
pub fn bounded_tail_preview(text: &str, max_bytes: usize) -> (String, bool) {
    bounded_tail_preview_for_original(text, text.len() as u64, max_bytes)
}

/// Retain a tail from a durable representation while describing a larger original value.
pub fn bounded_tail_preview_for_original(
    text: &str,
    original_bytes: u64,
    max_bytes: usize,
) -> (String, bool) {
    giskard_core::command_output_tail_preview(text, original_bytes, max_bytes)
}

/// Retain UTF-8-safe head and tail sections around a stable omission marker.
pub fn bounded_head_tail(text: &str, max_bytes: usize) -> (String, bool) {
    if text.len() <= max_bytes {
        return (text.to_owned(), false);
    }
    let marker = |omitted| format!("\n[… {omitted} bytes omitted from durable command output …]\n");
    let mut omitted = text.len();
    loop {
        let separator = marker(omitted);
        let remaining = max_bytes.saturating_sub(separator.len());
        let head_budget = remaining / 2;
        let tail_budget = remaining - head_budget;
        let mut retained_head_end = head_end(text, head_budget);
        let mut retained_tail_start = tail_start(text, tail_budget);

        // UTF-8 retreat can leave usable bytes. Give them to the tail first, then the head.
        let used = retained_head_end + text.len().saturating_sub(retained_tail_start);
        let spare = remaining.saturating_sub(used);
        if spare > 0 {
            let retained = text.len() - retained_tail_start;
            retained_tail_start = tail_start(text, retained + spare);
        }
        let used = retained_head_end + text.len().saturating_sub(retained_tail_start);
        let spare = remaining.saturating_sub(used);
        if spare > 0 {
            retained_head_end = head_end(text, retained_head_end + spare);
        }
        if retained_head_end > retained_tail_start {
            retained_head_end = retained_tail_start;
        }
        let actual = text.len() - retained_head_end - (text.len() - retained_tail_start);
        if actual == omitted {
            return (
                format!(
                    "{}{separator}{}",
                    &text[..retained_head_end],
                    &text[retained_tail_start..]
                ),
                true,
            );
        }
        omitted = actual;
    }
}

fn head_end(text: &str, budget: usize) -> usize {
    let mut end = budget.min(text.len());
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    end
}

fn tail_start(text: &str, budget: usize) -> usize {
    let mut start = text.len().saturating_sub(budget);
    while !text.is_char_boundary(start) {
        start += 1;
    }
    start
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

    #[test]
    fn logical_lines_do_not_count_a_final_empty_line() {
        assert_eq!(logical_line_count(""), 0);
        assert_eq!(logical_line_count("one"), 1);
        assert_eq!(logical_line_count("one\n"), 1);
        assert_eq!(logical_line_count("one\ntwo"), 2);
    }

    #[test]
    fn head_tail_is_utf8_safe_and_within_the_exact_cap() {
        let text = "🙂".repeat(20_000);
        let (retained, truncated) = bounded_head_tail(&text, 32_769);
        assert!(truncated);
        assert!(retained.len() <= 32_769, "length was {}", retained.len());
        assert!(retained.starts_with('🙂'));
        assert!(retained.ends_with('🙂'));
        assert!(retained.contains("bytes omitted from durable command output"));
    }
}
