# Codex `add` and `delete` file changes are not diffs, but Giskard renders them as one

Plan for a defect in how Codex file-change items reach the diff overlay. Written against `main` at
`dbd4834` with `codex-codes` 0.153.4; every file and line reference below was checked against that
tree. Re-check them if the branch has moved.

**Status: implemented** — option A plus the client guard, as recommended below, with **one part of
the plan rejected in review**: the harness translates on Codex's change kind alone, not on a test
of the body's shape. See **Correction: the shape check belongs only in the browser**. Three smaller
things also landed beyond the plan as written, each noted in place: `unified_stats` became
hunk-aware, the whole-file listing relabels the copy button rather than hiding it, and the replay
harness got its own trigger instead of extending the lazy-diff turn.

## The defect

Codex reports a file-change item as a list of `FileUpdateChange { path, kind, diff }`
(`codex_codes::FileUpdateChange`, the generated app-server type). `kind` is `add`, `delete`, or
`update`. Only `update` carries a unified diff in `diff`; `add` and `delete` carry the file's **raw
content**, which is the shape the legacy approval protocol still states outright in
`codex_codes::protocol::FileChange`:

```rust
pub enum FileChange {
    Add    { content: String },
    Delete { content: String },
    Update { move_path: Option<String>, unified_diff: String },
}
```

Giskard treats the `diff` field as a unified diff regardless of `kind`
(`crates/giskard-harness-codex/src/mapping.rs:3230`), and everything downstream inherits that
belief. A created or deleted file whose content happens to contain lines starting with `+` or `-` —
a Markdown list, a changelog, a YAML sequence, a patch file, a CSV of signed numbers — is therefore
painted with added and deleted lines that do not exist.

Three separate things go wrong, and they need separating because a display-only fix leaves two of
them standing:

1. **The content is mislabelled.** `CapturedDiffState::capture`
   (`crates/giskard-server/src/thread_runtime/diffs.rs:56`) hands every file-change body to
   `giskard_core::capture_unified_diff` (`crates/giskard-core/src/diff.rs:47`), which stamps it
   `DiffContentKind::Unified`. The lazy-diff endpoint then serves raw file content under
   `{"kind":"unified"}`, so every consumer is entitled to parse it as a patch.
2. **The line counts are wrong.** `unified_stats` (`crates/giskard-core/src/diff.rs:183`) derives
   `additions`/`deletions` by counting `+`/`-` prefixes. Over raw content those counts are
   arbitrary: a new 400-line file with two `+`-prefixed lines reports `+2 −0`. The counts are
   persisted in the descriptor and sent on the wire
   (`crates/giskard-proto/src/wire.rs:250`); today nothing in `app.js` reads them, but they are
   part of the stored record and of the documented descriptor contract.
3. **The overlay mis-renders it.** `openCapturedDiff` (`crates/giskard-server/static/app.js:8011`)
   takes `content.kind === "unified"` at face value and passes the text to `openDiffOverlay`
   (`:8857`), which runs `diffStats` (`:8602`) for the header and `parseUnifiedDiff` (`:8764`) for
   the body. With no `@@` header in the text every line stays in the pre-hunk branch (`:8798-8807`),
   which deliberately colours a leading `+` as an addition and a leading `-` as a deletion — the
   right call for a headerless patch from an agent, the wrong one for a file that was never a patch.

Nothing about the *shape* of `diff` is written down. The upstream JSON Schema declares it as a bare
`{"type": "string"}` with no description, and the doc comment that does call it "a unified-diff
snippet" is on `codex_codes::io::items::FileUpdateChange`, the JSONL exec-protocol type, which
Giskard does not use (`codex_codes` re-exports the *generated* type through `protocol::*`). That is
because `kind` carries the meaning: it is a required, typed discriminator, and reading it is how a
client learns what the body is. The bug is that Giskard ignores it.

## Where raw content reaches the browser

One path, which is what makes this fixable in one place:

- `map_thread_item_complete` → `ItemPayload::FileChange`
  (`crates/giskard-harness-codex/src/mapping.rs:2778`) calls `map_file_changes` (`:3230`) and is the
  only producer of a `FileChangeEntry.diff` the browser ever sees.

Two other call sites carry the same bodies but never show them:

- `Notification::ItemStarted` (`:520`) and `Notification::FileChangePatchUpdated` (`:603`) store
  `map_file_changes(...)` into `file_change_previews`. That map exists solely so a later
  `FileChangeApproval` request can name the changed paths (`:1026-1045`,
  `file_change_approval_metadata` at `:1830`); it reads only `path` and `change`, never `diff`.
  Retaining the bodies there is pure waste — for an `add` of a large generated file, a full second
  copy of the file held for the length of the turn.
- `Notification::TurnDiffUpdated` (`:653`) is a genuine `git diff` of the whole turn and is captured
  structurally (`capture_structured_diff`). It is not affected.

Approvals are also unaffected: `ApprovalKind::FileChange`
(`crates/giskard-core/src/approval.rs:28`) carries a path and a change kind, no body, and the legacy
`apply_patch` metadata path (`mapping.rs:1874`) only reads paths. Spec §S6 still describes an
approval preview built from "the raw diff string"; no such preview exists in the code.

## Correction: the shape check belongs only in the browser

The plan argued that because the schema types `diff` as a bare string and documents nothing about
its shape, the mapper should test the body rather than trust `kind`. That is the wrong conclusion
from a true premise, and the implementation does not do it.

`kind` is the protocol's own required, typed discriminator. It is not a hint about the body — it is
the statement of what the body is, and the shape of that field is undocumented precisely because
`kind` already says. Testing the content instead trades a guarantee for a guess, and the guess has
a failure mode the guarantee does not: **a created file that merely contains a patch.** A `.patch`
or `.diff` fixture, a test case, a README with a diff in a fenced block — all ordinary things for a
coding agent to write, all of which the shape test would wave through untranslated, to be rendered
as the diff they contain. That is the original defect, reintroduced for a plausible class of file,
silently, on ordinary content.

What the shape test was meant to protect against — a future Codex that sends a real patch for
`add`, double-wrapped into a diff of a diff — is both less likely and much cheaper. `codex-codes`
is a pinned dependency (`AGENTS.md`: raising it is a deliberate act), so a change of that kind
arrives through a version bump, which is where it gets caught; and its failure is conspicuous
rather than silent. Trading a silent wrong answer on today's ordinary content for a loud wrong
answer on a hypothetical future protocol is a bad trade.

So `file_change_body` matches on `kind` and translates every `add`/`delete` body. The unit test
that asserted a patch-shaped body passes through unwrapped was replaced by one asserting the
opposite: a created `tests/fixture.patch` is translated like any other file, and all five of its
lines — including the ones reading `---`, `+++`, and `@@` — count as additions.

The browser keeps its shape test, because there it is not a choice between a guess and a
discriminator. A turn captured before this translation existed stored raw content in the same
field, under the same `unified` content kind, with nothing in the record to distinguish it. The
shape is the only signal that exists, its worst case (a created `.patch` stored back then, still
shown as a diff) is exactly the status quo for that data rather than a regression, and it goes away
as old turns age out. Step 3 below is unchanged; step 1's `looks_like_unified_diff` was never
written.

## Options

### A. Translate at the Codex boundary (recommended)

In `map_file_changes`, turn an `add` or `delete` body into a real unified diff before it becomes a
`FileChangeEntry`:

For an `add` of an N-line file:

```
--- /dev/null
+++ b/<path>
@@ -0,0 +1,N @@
+<line 1>
...
```

and for a `delete`:

```
--- a/<path>
+++ /dev/null
@@ -1,N +0,0 @@
-<line 1>
...
```

with `\ No newline at end of file` when the content has no trailing newline, and an empty body with
`@@ -0,0 +0,0 @@` for an empty file. Drive it from `kind` — see **Correction: the shape check
belongs only in the browser** for why this paragraph originally said to test the body's shape
instead, and why that was wrong.

- Every downstream concern — content kind, `additions`/`deletions`, the overlay, **Copy diff** —
  becomes correct with no change to the wire types, the payload format, or the persistence schema.
  `TURN_PAYLOAD_FORMAT` stays at 1.
- **Copy diff** starts handing back something `git apply` accepts, which it does not today.
- The Codex-specific knowledge ("`add`/`delete` mean raw content") stays inside
  `giskard-harness-codex`, where the conventions in `AGENTS.md` put it.
- Costs one extra copy of the body at map time (roughly content size plus one byte per line). That
  is paid back immediately by dropping the bodies from `file_change_previews`, which holds a full
  copy today for no reader.
- `DiffId` is content-addressed, so the identity of a newly captured `add` changes shape. Nothing
  keys across versions on it, so there is no migration; already-persisted turns keep their old
  bodies (see **Already-persisted history**).

### B. A distinct content kind for whole-file bodies

Carry the raw content as its own thing — either a new `CapturedDiffContent::FileContent { text }`
plus a `DiffContentKind` variant, or a `CapturedDiffContent::Structured` whose `hunks` are empty and
whose `old_text`/`new_text` hold the content.

The structured route is tempting because both halves already exist: `capture_structured_diff`
counts full-text-only bodies by line
(`crates/giskard-core/src/diff.rs:74-81`, tested at `:330`), and `structuredCapturedDiffText`
(`app.js:7977-7991`) already renders a hunk-less structured diff by synthesising exactly the unified
form option A would have produced. But a `FileChangeEntry` persists its body as
`diff: Option<String>` and `turn_with_inline_diffs` (`crates/giskard-persist/src/history.rs:332`)
requires `CapturedDiffContent::Unified` for a file-change entry; a structured body cannot round-trip
through that field. So this option costs a payload format bump, a migration story, and
`serde` tolerance for the new tag in older builds — for a display that lands in the same place as
option A's (its synthesised header names `a/`/`b/` rather than `/dev/null` and it has no
`\ No newline` handling), computed three layers later and in JavaScript rather than once at the
boundary.

### C. Display `add`/`delete` differently, not as a diff

Render a created or deleted file as a whole-file listing: real file line numbers, one uniform
add/delete colour, no `+`/`-` prefixes, and — since the code overlay already does this for source
files — potentially syntax-highlighted. For reading a newly created file this is genuinely better
than a wall of `+`.

It is not a substitute, though: it fixes (3) and leaves (1) and (2) — the mislabelled content kind
and the wrong persisted counts — exactly as they are, and it turns **Copy diff** into a button that
copies something that is not a diff. It is a display refinement worth doing *on top of* a correct
body, not instead of one.

## Recommendation

**Option A, plus a client-side guard.** A owns correctness: the harness stops claiming that raw
content is a patch, and the label, the counts, the rendering and the copy button all follow from one
change. The guard owns the data A cannot reach: already-persisted turns, and any future harness that
hands over a body that is not a patch.

C stays on the table as a follow-up, and becomes easy once bodies are honest — the overlay can
choose the listing layout from the descriptor's `change` kind without having to guess what the text
is.

## Plan

### 1. `giskard-harness-codex` — synthesise the patch

`crates/giskard-harness-codex/src/mapping.rs`

- Add `unified_diff_for_whole_file(path: &str, change: FileChangeKind, content: &str) -> String`
  next to `map_file_changes` (`:3230`), producing the headers and hunk shown above. Handle: empty
  content (`@@ -0,0 +0,0 @@`, no body lines), no trailing newline (`\ No newline at end of file`),
  CRLF content (prefix the marker, do not rewrite the line ending), and a final newline (which must
  not produce a trailing empty `+` line).
- In `map_file_changes`, for `PatchChangeKind::Add | Delete` with a non-empty body, store the
  synthesised text; `Update` is untouched. The change kind decides, with no test of the body — see
  **Correction: the shape check belongs only in the browser**. Log the translation at `debug`.
- Give the preview call sites (`:521`, `:607`) a body-free variant — `map_file_change_previews`, or
  a flag on `map_file_changes` — so `file_change_previews` stores path and kind only. Update the
  field's doc comment (`:157`) to say the previews deliberately carry no bodies.

Tests in the same file's `mod tests`: an `add` whose content contains `+`/`-`/`@@`-prefixed lines
round-trips to a diff whose only additions are every line; a `delete` likewise; an `update` body is
byte-identical to what Codex sent; an empty `add`; content with and without a trailing newline;
CRLF content; a created `.patch` fixture — a body that *is* a diff — is translated like any other
file; and `file_change_previews` holds no bodies while still producing the right approval metadata
(extend `file_change_previews_are_replaced_scoped_and_cleared`, `:4865`).

### 2. `giskard-core` — say what the counts assume

`crates/giskard-core/src/diff.rs`

`capture_unified_diff` (`:47`) and `unified_stats` (`:183`) are now the load-bearing definition of
"these counts are only meaningful over a unified diff". State that in a doc comment on both, naming
the harness as the place that guarantees the input shape. This is the comment that stops the next
harness from re-introducing the same defect.

**Beyond the plan:** `unified_stats` also became hunk-aware. Its `!line.starts_with("+++")` guard
ran over the whole body, so inside a hunk it dropped any changed line whose own text begins `++` or
`--` — which a translated whole-file body produces the moment the file contains a line starting
`+` or `-`, exactly the content this change is about. It now skips `---`/`+++` only before the
first hunk, where they really are headers, and counts by the marker column inside one.
`diffStats` in `app.js` was given the same rule so the overlay header agrees with the descriptor.

### 3. `giskard-server/static/app.js` — refuse to mis-render

- In `openCapturedDiff` (`:8001`), before calling `openDiffOverlay`, apply the same
  `looks_like_unified_diff` rule to a `unified` body. If the descriptor's `change` is `created` or
  `deleted` **and** the body does not look like a patch, open it as a whole-file listing instead:
  every line rendered with that change's colour (`created` → add, `deleted` → del), real file line
  numbers in the appropriate gutter, and a header that counts lines rather than claiming `+N −M`.
  Requiring both conditions is what makes the guard inert once step 1 ships — a post-fix `created`
  body does look like a patch and takes the normal path — so the listing is reached only by a body
  written before the fix. A pre-fix raw body that is itself a patch file stays ambiguous and renders
  as a diff; there is no information left in the record to tell those apart, and it is the rarer
  case.
- `renderDiffRows` (`:8823`) already takes rows with per-side line numbers, so the listing is a row
  builder, not a new renderer. `parseUnifiedDiff` itself is unchanged: its pre-hunk colouring is
  still right for the headerless-patch case it was written for.
- Relabel **Copy diff** to **Copy file** for that path (`setCodeCopyDiff`, `:8882`) — copying raw
  content under a "Copy diff" label is the same lie in miniature, and the content is still worth
  copying. The label is view state (`state.diffOverlayCopyLabel`) so the button restores its own
  wording after the "Copied" flash rather than snapping back to "Copy diff".

Add the new function names to the served-script assertions in
`crates/giskard-server/tests/ui.rs` alongside the existing `parseUnifiedDiff` check (`:1213`).

### 4. End-to-end coverage

Seed a created file whose content contains lines starting with `+` and `-`, and assert in
`tests/e2e/tests/lazy-diffs.spec.ts` that the overlay shows no `diff-del` row for it. This is the
test that would have caught the defect, so it is the one that must exist.

**Beyond the plan:** rather than extending the existing lazy-diff turn, this is its own trigger
(`SCRIPTED_WHOLE_FILE_TRIGGER`). Several existing tests address that turn's entries by `.first()`
and `.last()`, and a second file-change item in the same turn merges into the same transcript row,
so adding entries there would have moved the targets of assertions that have nothing to do with
this change. The new turn carries both shapes at once — one entry with the translated body, one
with the raw body a turn captured before the fix still holds — so the patch view and the listing
are covered side by side. Keep `SCRIPTED_*` in `tests/e2e/tests/helpers.ts` in sync, per
`AGENTS.md`.

The replay harness feeds `FileChangeEntry` directly and never goes through the Codex mapper, so the
translated body is seeded as a literal; the mapper's unit tests pin the same bytes, so a change to
the translation fails both.

## Already-persisted history

Turns written before this change keep raw content inline in `threads/<id>/turns/<turn_id>.jsonl`, and
`captured_diff_contents` (`crates/giskard-persist/src/history.rs:361`) will keep re-deriving
`CapturedDiffContent::Unified` from it with the same wrong counts. That is deliberate: rewriting
persisted turns to fix a rendering defect is not worth a migration, and the client guard (step 3)
makes the old bodies render correctly and honestly anyway. The stale `additions`/`deletions` on
those old descriptors stay stale; nothing reads them today, and a reader added later would be
reading a historical record, not a live one.

If that is judged not good enough, the alternative is a lazy per-turn migration at read time —
`TURN_PAYLOAD_FORMAT` exists per file precisely to allow it (`crates/giskard-persist/src/layout.rs:19-23`)
— but it should be a separate change with its own plan, not folded into this one.

## Documentation to update in the same change

- `crates/giskard-harness-codex/README.md` — a paragraph in the file-change section (near `:640`)
  stating that `add`/`delete` bodies are raw content, that the adapter translates them to unified
  diffs before they leave the crate, that the shape check guards the translation, and that previews
  carry no bodies. `AGENTS.md` requires this README to move with protocol-routing changes.
- `specs/giskard-specification.md` — the lazy-captured-diffs amendment (`:156-165`) says bodies are
  extracted, not that one harness normalises them first. Add a sentence. While there, correct §S6
  (`:1376`, `:3282`) which describes an approval diff preview built from the raw diff string; no such
  preview is implemented.
- No README, `config.example.toml`, or `docs/api-endpoints.md` change: no config key, endpoint,
  request shape, or response shape moves.
- No screenshot regeneration: the change is visible only inside the diff overlay, which the
  README screenshots do not show.

## What this plan does not do

- It does not change the wire types, the payload format, or any endpoint.
- It does not implement option C's whole-file listing for *correct* `add`/`delete` bodies. After
  this change they render as an all-`+` or all-`-` patch, which is accurate. C is a follow-up.
- It does not migrate persisted history.
- It does not touch `TurnDiffUpdated`, the structured diff path, or `/api/projects/{id}/git/diff`,
  all of which carry real diffs already.
- It does not address an adjacent gap found while reading this code: `map_patch_change_kind`
  (`crates/giskard-harness-codex/src/mapping.rs:3242`) discards `PatchChangeKind::Update`'s
  `move_path`, so a renamed file is shown as a plain modification of its new path and the old path
  is never named. `FileChangeEntry` has nowhere to put it, so fixing it is a wire-type change and
  wants its own plan.
