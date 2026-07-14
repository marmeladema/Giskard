# Model Provider Switching Analysis

This note documents the provider-switching problem seen while using Giskard with Codex
app-server and LiteLLM-backed models, plus the Codex source analysis performed on
2026-07-12 against `/home/elie/Sources/codex`.

It is an analysis note, not an authoritative product spec. Once the implementation direction is
chosen, fold the final contract into `specs/giskard-specification.md`.

## Problem

Giskard lets a user select a model identified by `(provider, model)`, for example:

- `openai/gpt-5.5`
- `proxy/glm-5.2-workers-ai`

The Codex app-server protocol does not treat provider selection the same way at every boundary.
`thread/start` and `thread/resume` accept `modelProvider`, but `turn/start` does not. `turn/start`
can override `model`, not provider.

This matters when Giskard creates or resumes a native Codex thread with one provider and the user
then selects a model from another provider before sending a message. If Giskard only passes the new
model through `turn/start`, Codex can send the request to the old provider. In the observed
LiteLLM case, a model configured under the Giskard/Codex `proxy` provider was attempted through the
wrong provider path, yielding provider-side `model_not_found` errors.

## Codex Protocol Shape

From the app-server v2 protocol types in the Codex checkout:

- `ThreadStartParams` includes `modelProvider`:
  `/home/elie/Sources/codex/codex-rs/app-server-protocol/src/protocol/v2/thread.rs:56`
  and `:60`.
- `ThreadResumeParams` includes `modelProvider`:
  `/home/elie/Sources/codex/codex-rs/app-server-protocol/src/protocol/v2/thread.rs:324`
  and `:350`.
- `TurnStartParams` does not include `modelProvider`:
  `/home/elie/Sources/codex/codex-rs/app-server-protocol/src/protocol/v2/turn.rs:68`.

Giskard's current Codex adapter already sends `modelProvider` on `thread/resume`:

- `crates/giskard-harness-codex/src/lib.rs:954`

The conclusion is that provider switching must happen at a native-thread boundary, not as a
per-turn override.

## Codex Source Analysis

The relevant Codex source is under:

- `/home/elie/Sources/codex/codex-rs/app-server/src/request_processors/thread_processor.rs`
- `/home/elie/Sources/codex/codex-rs/app-server/src/request_processors/thread_lifecycle.rs`
- `/home/elie/Sources/codex/codex-rs/app-server/src/request_processors/thread_processor_tests.rs`
- `/home/elie/Sources/codex/codex-rs/core/src/thread_manager.rs`
- `/home/elie/Sources/codex/codex-rs/app-server/README.md`

### Cold Resume Can Apply Provider Overrides

`thread_resume_inner` destructures `ThreadResumeParams`, including `model` and
`model_provider`, then passes them into `build_thread_config_overrides`:

- `thread_processor.rs:2732`
- `thread_processor.rs:2777`
- `thread_processor.rs:1376`

Those overrides are used to load a fresh `Config` for the resumed thread before calling
`resume_thread_with_history`:

- `thread_processor.rs:2798`
- `thread_processor.rs:2814`

Codex's app-server README states the intended behavior: by default, resume uses the latest
persisted `model` and `reasoningEffort`; supplying `model`, `modelProvider`, `config.model`, or
`config.model_reasoning_effort` disables that persisted fallback and uses explicit overrides plus
normal config resolution:

- `codex-rs/app-server/README.md:316`
- `codex-rs/app-server/README.md:322`

The implementation matches that statement. `merge_persisted_resume_metadata` only applies the
persisted model/provider if there is no model/provider override:

- `thread_processor.rs:147`
- `thread_processor.rs:219`

Codex has tests covering this precedence. In particular,
`merge_persisted_resume_metadata_skips_persisted_values_when_provider_overridden` verifies that an
explicit provider override is preserved instead of being overwritten by persisted metadata:

- `thread_processor_tests.rs:921`

Core resume then respawns the thread from `InitialHistory::Resumed` using the resolved config, while
keeping the resumed `conversation_id`:

- `thread_manager.rs:780`
- `thread_manager.rs:1535`
- `thread_manager.rs:1714`

Therefore, for a cold or unloaded native Codex thread, `thread/resume` should be able to switch the
provider for an existing non-empty thread while preserving thread id and history.

### Loaded Threads Are Different

Codex has a separate fast path for a thread that is already loaded in the app-server process:

- `thread_processor.rs:3012`

When a loaded thread exists, Codex compares the resume request's overrides with the active thread
configuration:

- `thread_processor.rs:26`
- `thread_processor.rs:3077`

If there are mismatches, Codex tries to shut down the loaded idle thread only when all of these are
true:

- there are no subscribed app-server connections for the thread;
- the loaded status is idle;
- the agent is not running.

If that shutdown path succeeds, Codex removes the loaded thread and falls back to a cold resume,
where overrides can apply:

- `thread_processor.rs:3092`
- `thread_processor.rs:3096`
- `thread_processor.rs:3102`

Otherwise Codex preserves rejoin semantics and logs that the `thread/resume` overrides were ignored:

- `thread_processor.rs:3113`
- `thread_processor.rs:3115`

The response in this loaded-thread path is built from the existing active `config_snapshot`, so it
returns the old provider/model:

- `thread_processor.rs:3150`
- `request_processors/thread_lifecycle.rs:621`

This means `thread/resume` is not enough by itself if Giskard already has the native Codex thread
loaded and subscribed. A provider switch could be ignored by Codex and still return a successful
JSON-RPC response.

## Implications For Giskard

Giskard should not rely on `turn/start` for provider switching. The reliable places to switch
provider are:

1. Create a fresh native Codex thread with `thread/start`.
2. Cold-resume an existing native Codex thread with `thread/resume` and `modelProvider`.
3. Possibly resume a loaded idle thread if Codex can unload it first, but only if the response is
   verified.

Giskard must verify the `thread/resume` response. A successful JSON-RPC response is not sufficient,
because Codex can intentionally ignore resume overrides for loaded threads.

Verification should check at least:

- `response.modelProvider == requested.provider`
- `response.model == requested.model`
- `response.thread.id == existing_harness_thread_id`

If Codex returns the old provider/model, Giskard should surface a clear browser-visible error and
log the ignored switch with the native thread id, requested provider/model, and returned
provider/model.

## Recommended Giskard Behavior

### New Thread Draft

Giskard now avoids the empty-native-thread case for normal UI creation. Clicking project `+` opens
an unpersisted browser draft. The first send calls Giskard's `POST /api/projects/{id}/threads/start`
with the selected model/provider, mode, approval policy, and initial text. The server then calls
Codex `thread/start` with that provider/model and immediately starts the first turn.

This is better than creating an empty native thread and later replacing it: the selected provider is
known before the native Codex thread exists, so no provider rebinding is necessary.

### Non-Empty Thread

If the thread has persisted history and no active turn/tasks/requests, Giskard can support provider
switching by attempting a controlled native re-resume:

1. Call `thread/resume` with the existing `harness_thread_id`, selected `model`, and selected
   `modelProvider`.
2. Require the response to report the selected provider/model.
3. If verified, update the registry's native model binding and broadcast thread state.
4. If not verified, keep the old binding and return a structured error such as
   `thread_provider_switch_ignored`.

This should preserve Giskard history and Codex history while allowing intentional provider changes.

### Active Thread

If there is an active turn, running command/tool, pending approval, or pending server request,
provider switching should be rejected. Switching provider while work is in flight would create a
hard-to-explain split between the currently loaded Codex session and Giskard's selected model.

### Already-Loaded Idle Thread

This is the subtle case. Giskard's current harness keeps Codex threads loaded after opening them.
Calling `thread/resume` with provider overrides may be ignored if Codex still sees the thread as
loaded and subscribed.

Possible implementation strategies:

1. Add a harness method that attempts `thread/resume` with provider/model overrides and validates
   the response.
2. If Codex reports the old provider/model, surface a clear error and ask the user to retry after
   reload/restart, or implement an explicit unload/reopen flow if Codex exposes one.
3. Only update Giskard's selected provider after the native switch is verified.

The important invariant is: never persist or display a provider switch as effective until the
native Codex thread has confirmed that provider.

## Test Coverage To Add

Giskard-side tests should cover:

- blank thread creation is rejected unless an explicit native resume/import id is supplied;
- first-message creation starts the native thread with the selected provider/model;
- first-message `turn/start` rejection cleans up the just-created local/native thread;
- non-empty cold-resume provider switch succeeds and updates the registry binding;
- non-empty resume returns old provider/model and Giskard surfaces a structured error;
- active turn rejects provider switch before calling Codex;
- running task or pending approval/request rejects provider switch before calling Codex;
- send path cannot proceed with a persisted provider mismatch unless the native switch is verified.

Codex-harness unit tests should cover response validation:

- requested provider/model matches response;
- response provider differs;
- response model differs;
- response thread id differs;
- transport/protocol failure leaves the old registry binding intact.

## Current Patch

Spec v1.42 (PS1–PS3) implements the verified re-resume for the **cold** case:

- First-message provider failures remain fixed by delaying native Codex thread creation until the
  first message carries the selected provider/model.
- Provider changes on **loaded** (warm) threads are still rejected (`thread_provider_locked`),
  because a loaded thread can silently ignore `thread/resume` overrides.
- Provider changes on **cold** threads (not loaded this server run — restarts, or orphaned
  threads whose provider left the config) perform a verified `thread/resume` with the requested
  `modelProvider`: the response's effective model/provider must match the request before anything
  is persisted, otherwise the switch fails with `thread_provider_switch_ignored` and the old
  binding/state is preserved.

The remaining unimplemented piece is the already-loaded idle case (unload/reopen flow).
