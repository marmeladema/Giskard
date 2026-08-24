use giskard_core::{CommandOutputDescriptor, NormalizedCommandOutput};

use crate::preview::{
    COMMAND_OUTPUT_PREVIEW_MAX_BYTES, bounded_head_tail, bounded_tail_preview_for_original,
    logical_line_count,
};

/// Normalize provider output once into its durable representation and bounded wire descriptor.
pub fn normalize_command_output(output: String, max_bytes: usize) -> NormalizedCommandOutput {
    let original_bytes = output.len() as u64;
    let original_lines = logical_line_count(&output);
    let (output, output_truncated) = bounded_head_tail(&output, max_bytes);
    let (preview, preview_truncated) = bounded_tail_preview_for_original(
        &output,
        original_bytes,
        COMMAND_OUTPUT_PREVIEW_MAX_BYTES,
    );
    let descriptor = descriptor(
        &output,
        output_truncated,
        Some(original_bytes),
        Some(original_lines),
        preview,
        preview_truncated,
        false,
    );
    NormalizedCommandOutput {
        output,
        output_truncated,
        output_original_bytes: output_truncated.then_some(original_bytes),
        output_original_lines: output_truncated.then_some(original_lines),
        descriptor,
    }
}

/// Build a bounded descriptor from structurally valid durable fields.
pub fn command_output_descriptor(
    output: &str,
    output_truncated: bool,
    output_original_bytes: Option<u64>,
    output_original_lines: Option<u64>,
    output_available: bool,
) -> Result<CommandOutputDescriptor, &'static str> {
    validate_command_output_metadata(
        output_truncated,
        output_original_bytes,
        output_original_lines,
    )?;
    let retained_bytes = output.len() as u64;
    let original_bytes = if output_truncated {
        output_original_bytes.unwrap_or(retained_bytes)
    } else {
        retained_bytes
    };
    let original_lines = if output_truncated {
        output_original_lines.unwrap_or_else(|| logical_line_count(output))
    } else {
        logical_line_count(output)
    };
    let (preview, preview_truncated) =
        bounded_tail_preview_for_original(output, original_bytes, COMMAND_OUTPUT_PREVIEW_MAX_BYTES);
    Ok(descriptor(
        output,
        output_truncated,
        Some(original_bytes),
        Some(original_lines),
        preview,
        preview_truncated || original_bytes > retained_bytes,
        output_available,
    ))
}

/// Validate the additive persistence contract without inferring facts lost by truncation.
pub(crate) fn validate_command_output_payload(
    payload: &giskard_core::ItemPayload,
) -> Result<(), &'static str> {
    let giskard_core::ItemPayload::CommandExecution {
        output_truncated,
        output_original_bytes,
        output_original_lines,
        ..
    } = payload
    else {
        return Ok(());
    };
    validate_command_output_metadata(
        *output_truncated,
        *output_original_bytes,
        *output_original_lines,
    )
}

pub(crate) fn ignored_command_output_metadata(payload: &giskard_core::ItemPayload) -> (bool, bool) {
    let giskard_core::ItemPayload::CommandExecution {
        output_truncated,
        output_original_bytes,
        output_original_lines,
        ..
    } = payload
    else {
        return (false, false);
    };
    (
        !*output_truncated && output_original_bytes.is_some(),
        !*output_truncated && output_original_lines.is_some(),
    )
}

fn validate_command_output_metadata(
    output_truncated: bool,
    output_original_bytes: Option<u64>,
    output_original_lines: Option<u64>,
) -> Result<(), &'static str> {
    match (
        output_truncated,
        output_original_bytes,
        output_original_lines,
    ) {
        (false, _, _) | (true, Some(_), Some(_)) => Ok(()),
        (true, _, _) => Err("truncated command output is missing original-size metadata"),
    }
}

fn descriptor(
    output: &str,
    durable_truncated: bool,
    original_bytes: Option<u64>,
    original_lines: Option<u64>,
    preview: String,
    preview_truncated: bool,
    output_available: bool,
) -> CommandOutputDescriptor {
    let mut descriptor = CommandOutputDescriptor::from_durable(
        output,
        durable_truncated,
        original_bytes.unwrap_or(output.len() as u64),
        original_lines.unwrap_or_else(|| logical_line_count(output)),
        output_available,
    );
    descriptor.preview = preview;
    descriptor.preview_bytes = descriptor.preview.len() as u64;
    descriptor.preview_lines = logical_line_count(&descriptor.preview);
    descriptor.preview_truncated = preview_truncated;
    descriptor
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_and_reloaded_descriptors_are_identical() {
        let original = format!("BEGIN-SENTINEL\n{}\nEND-SENTINEL", "x".repeat(80_000));
        let normalized = normalize_command_output(original, 32_768);
        let reloaded = command_output_descriptor(
            &normalized.output,
            normalized.output_truncated,
            normalized.output_original_bytes,
            normalized.output_original_lines,
            false,
        )
        .unwrap();
        assert_eq!(normalized.descriptor, reloaded);
        assert!(normalized.output.len() <= 32_768);
        assert!(!normalized.descriptor.preview.contains("BEGIN-SENTINEL"));
        assert!(normalized.descriptor.preview.contains("END-SENTINEL"));
    }

    #[test]
    fn truncated_output_requires_both_original_counts() {
        assert_eq!(
            command_output_descriptor("kept\n", true, Some(10), None, true).unwrap_err(),
            "truncated command output is missing original-size metadata"
        );
    }

    #[test]
    fn untruncated_output_ignores_original_counts() {
        let descriptor =
            command_output_descriptor("kept\n", false, Some(500), Some(100), true).unwrap();
        assert_eq!(descriptor.original_bytes, 5);
        assert_eq!(descriptor.original_lines, 1);
    }
}
