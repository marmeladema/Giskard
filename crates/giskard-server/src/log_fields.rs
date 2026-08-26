use std::fmt;

use chrono::{DateTime, Utc};
use tracing::field::DisplayValue;

/// Adapts an optional displayable value for a tracing field without allocating.
pub(crate) fn display_opt<T: fmt::Display>(value: Option<T>) -> Option<DisplayValue<T>> {
    value.map(tracing::field::display)
}

pub(crate) struct Rfc3339<'a>(&'a DateTime<Utc>);

impl fmt::Display for Rfc3339<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        // `%.f` writes the fractional second only when it is non-zero and preserves its
        // significant millisecond, microsecond, or nanosecond precision.
        write!(formatter, "{}", self.0.format("%Y-%m-%dT%H:%M:%S%.fZ"))
    }
}

pub(crate) fn rfc3339(timestamp: &DateTime<Utc>) -> Rfc3339<'_> {
    Rfc3339(timestamp)
}

pub(crate) fn rfc3339_opt(timestamp: Option<&DateTime<Utc>>) -> Option<DisplayValue<Rfc3339<'_>>> {
    display_opt(timestamp.map(Rfc3339))
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone as _, Timelike as _, Utc};

    use super::{display_opt, rfc3339, rfc3339_opt};

    #[test]
    fn optional_display_values_are_omitted_or_formatted_without_option_syntax() {
        assert!(display_opt::<u64>(None).is_none());
        assert_eq!(
            display_opt(Some(42)).map(|value| value.to_string()),
            Some("42".into())
        );
    }

    #[test]
    fn timestamps_use_compact_rfc3339_with_significant_precision() {
        let cases = [
            (0, "2026-08-26T18:23:48Z"),
            (381_000_000, "2026-08-26T18:23:48.381Z"),
            (381_483_000, "2026-08-26T18:23:48.381483Z"),
            (381_483_434, "2026-08-26T18:23:48.381483434Z"),
        ];

        for (nanos, expected) in cases {
            let timestamp = Utc
                .with_ymd_and_hms(2026, 8, 26, 18, 23, 48)
                .single()
                .and_then(|timestamp| timestamp.with_nanosecond(nanos));
            let timestamp = timestamp.unwrap_or_else(|| panic!("valid test timestamp"));
            assert_eq!(rfc3339(&timestamp).to_string(), expected);
            assert_eq!(
                rfc3339_opt(Some(&timestamp)).map(|value| value.to_string()),
                Some(expected.into())
            );
        }
        assert!(rfc3339_opt(None).is_none());
    }
}
