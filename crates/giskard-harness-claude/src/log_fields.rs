use std::fmt::Display;

pub(crate) fn display_opt<T: Display>(value: Option<T>) -> Option<tracing::field::DisplayValue<T>> {
    value.map(tracing::field::display)
}

#[cfg(test)]
mod tests {
    use super::display_opt;

    #[test]
    fn optional_display_preserves_presence_without_formatting_eagerly() {
        assert_eq!(
            display_opt(Some(42)).map(|value| value.to_string()),
            Some("42".into())
        );
        assert!(display_opt::<u8>(None).is_none());
    }
}
