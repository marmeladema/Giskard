# Ideas under consideration

These are possible future directions, not planned features or commitments.

- Consider an offline `giskard-admin import-codex-thread` command. It could hold the existing
  data-directory lock, inspect or resume Codex metadata outside the live registry and owner
  lifecycle, and atomically create the Giskard binding and metadata. History, parentage, workspace,
  and override semantics would require a separate design.
- Explore support for Claude Code through a dedicated harness adapter. Its protocol capabilities,
  identity mapping, lifecycle behavior, and restart semantics would need to be investigated before
  defining a design.
