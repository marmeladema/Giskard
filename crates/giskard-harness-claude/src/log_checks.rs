//! Checks for `#[traced_test]`'s `logs_assert`, which hands them this test's log lines.
//!
//! Each returns `Err` instead of panicking: `logs_assert` runs the check while holding the global
//! log buffer's lock, and a panic there would poison it for every other test in the binary.

type Check<'a> = Box<dyn Fn(&[&str]) -> Result<(), String> + 'a>;

/// No line contains `needle`.
pub(crate) fn no_line_with(needle: &str) -> Check<'_> {
    Box::new(
        move |lines| match lines.iter().find(|line| line.contains(needle)) {
            Some(line) => Err(format!("unexpected line containing {needle:?}: {line}")),
            None => Ok(()),
        },
    )
}

/// Exactly `count` lines contain every one of `needles`.
pub(crate) fn lines_with<'a>(count: usize, needles: &'a [&'a str]) -> Check<'a> {
    Box::new(move |lines| {
        let found = lines
            .iter()
            .filter(|line| needles.iter().all(|needle| line.contains(needle)))
            .count();
        if found == count {
            Ok(())
        } else {
            Err(format!(
                "expected {count} line(s) containing all of {needles:?}, found {found}:\n{}",
                lines.join("\n")
            ))
        }
    })
}

/// At least one line contains every one of `needles`.
pub(crate) fn a_line_with<'a>(needles: &'a [&'a str]) -> Check<'a> {
    Box::new(move |lines| {
        if lines
            .iter()
            .any(|line| needles.iter().all(|needle| line.contains(needle)))
        {
            Ok(())
        } else {
            Err(format!(
                "no line contains all of {needles:?}:\n{}",
                lines.join("\n")
            ))
        }
    })
}
