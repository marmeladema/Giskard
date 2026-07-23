/// Trim surrounding whitespace and reject an empty result.
pub fn trimmed_non_empty(value: &str) -> Option<&str> {
    let value = value.trim();
    (!value.is_empty()).then_some(value)
}

#[cfg(test)]
mod tests {
    use super::trimmed_non_empty;

    #[test]
    fn trims_and_rejects_empty_text() {
        assert_eq!(trimmed_non_empty("  value  "), Some("value"));
        assert_eq!(trimmed_non_empty(" \n\t "), None);
    }
}
