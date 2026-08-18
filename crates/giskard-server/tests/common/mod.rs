//! Helpers shared by the `giskard-server` integration test binaries.
//!
//! Cargo builds every file in `tests/` as its own crate, so this module is compiled into each
//! binary that declares `mod common;` rather than linked once. That has a consequence worth
//! knowing before adding to it: an item only *some* of those binaries use is dead code in the
//! rest, and the workspace denies warnings. Keep this to helpers essentially all of them want.

use giskard_core::model::ModelRef;

/// The model a fake harness reports for a thread it is asked to import. A real harness answers this
/// from the thread itself; an import names no model, so the fake stands in with a fixed one rather
/// than claiming not to know.
pub fn fake_native_model() -> ModelRef {
    ModelRef {
        provider: "openai".into(),
        model: "gpt-5.5".into(),
        reasoning_effort: None,
    }
}
