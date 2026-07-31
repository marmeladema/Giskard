//! Parsers for the machine-readable output of the `git` command line.
//!
//! Giskard reads a project's working tree by running `git` and parsing what it prints, so this
//! crate is where that text becomes typed data. It is deliberately pure — no process spawning, no
//! I/O, no logging — which is what lets every format quirk below be covered by a unit test against
//! bytes captured from real `git` output.
//!
//! The formats parsed here are `git status --porcelain=v2 -z` and `git diff --numstat -z`. Both are
//! documented stability contracts, and `-z` makes them unambiguous: records are NUL-terminated and
//! paths are emitted verbatim, so a filename containing a space, quote or newline survives
//! round-tripping intact.

use std::collections::HashMap;

use giskard_proto::{GitFileStatus, GitStatusResponse};

/// Parse `git status --porcelain=v2 -z` output.
///
/// v2 is the labelled form of the status output: header lines carry named fields, and each entry
/// is prefixed by its own record type — `1` ordinary, `2` rename or copy, `u` unmerged, `?`
/// untracked. That prefix is what makes the states unambiguous; v1 encoded them all in a
/// two-character code, where an add/add conflict is indistinguishable from a staged add, a
/// detached HEAD carries no revision, and an unborn branch is prose.
///
/// Records are NUL-separated. A rename entry is followed by its original path as its own record.
pub fn parse_git_status(output: &[u8]) -> GitStatusResponse {
    let mut branch = None;
    let mut head = None;
    let mut detached = false;
    let mut ahead = 0;
    let mut behind = 0;
    let mut files = Vec::new();

    let mut records = output
        .split(|byte| *byte == 0)
        .filter(|record| !record.is_empty());
    while let Some(record) = records.next() {
        let text = String::from_utf8_lossy(record);
        if let Some(header) = text.strip_prefix("# ") {
            let (key, value) = header.split_once(' ').unwrap_or((header, ""));
            match key {
                // `(initial)` on an unborn branch, where the branch name is still reported normally.
                "branch.oid" => {
                    head = (value != "(initial)").then(|| git_short_oid(value));
                }
                "branch.head" => {
                    if value == "(detached)" {
                        detached = true;
                    } else if !value.is_empty() {
                        branch = Some(value.to_string());
                    }
                }
                "branch.ab" => (ahead, behind) = parse_git_ahead_behind(value),
                _ => {}
            }
            continue;
        }

        let Some(kind) = text.chars().next() else {
            continue;
        };
        let file = match kind {
            // `1 <XY> <sub> <mH> <mI> <mW> <hH> <hI> <path>`
            '1' => parse_git_entry_fields(&text, 9).map(|(xy, path)| GitFileStatus {
                kind: git_file_kind(xy.0, xy.1).into(),
                path,
                old_path: None,
                index_status: git_status_name(xy.0).into(),
                worktree_status: git_status_name(xy.1).into(),
                staged_added: None,
                staged_deleted: None,
                unstaged_added: None,
                unstaged_deleted: None,
            }),
            // `2 <XY> <sub> <mH> <mI> <mW> <hH> <hI> <X><score> <path>` then the original path as
            // its own record.
            '2' => parse_git_entry_fields(&text, 10).and_then(|(xy, path)| {
                let old_path = records.next()?;
                Some(GitFileStatus {
                    kind: git_file_kind(xy.0, xy.1).into(),
                    path,
                    old_path: Some(String::from_utf8_lossy(old_path).to_string()),
                    index_status: git_status_name(xy.0).into(),
                    worktree_status: git_status_name(xy.1).into(),
                    staged_added: None,
                    staged_deleted: None,
                    unstaged_added: None,
                    unstaged_deleted: None,
                })
            }),
            // `u <XY> <sub> <m1> <m2> <m3> <mW> <h1> <h2> <h3> <path>`. The record type alone says
            // it is a conflict, so the `AA` and `DD` pairs need no special handling.
            'u' => parse_git_entry_fields(&text, 11).map(|(xy, path)| GitFileStatus {
                kind: "unmerged".into(),
                path,
                old_path: None,
                index_status: git_status_name(xy.0).into(),
                worktree_status: git_status_name(xy.1).into(),
                staged_added: None,
                staged_deleted: None,
                unstaged_added: None,
                unstaged_deleted: None,
            }),
            // `? <path>`; a collapsed untracked directory keeps its trailing slash.
            '?' => text.strip_prefix("? ").map(|path| GitFileStatus {
                kind: "untracked".into(),
                path: path.to_string(),
                old_path: None,
                index_status: "untracked".into(),
                worktree_status: "untracked".into(),
                staged_added: None,
                staged_deleted: None,
                unstaged_added: None,
                unstaged_deleted: None,
            }),
            // `!` ignored entries are never requested.
            _ => None,
        };
        if let Some(file) = file {
            files.push(file);
        }
    }

    let conflicted_count = files.iter().filter(|file| file.kind == "unmerged").count();
    let staged_count = files
        .iter()
        .filter(|file| {
            file.kind != "unmerged"
                && file.index_status != "unmodified"
                && file.index_status != "untracked"
        })
        .count();
    let unstaged_count = files
        .iter()
        .filter(|file| {
            file.kind != "unmerged"
                && file.worktree_status != "unmodified"
                && file.worktree_status != "untracked"
        })
        .count();
    let untracked_count = files.iter().filter(|file| file.kind == "untracked").count();
    GitStatusResponse {
        is_repository: true,
        branch,
        head,
        detached,
        ahead,
        behind,
        dirty: !files.is_empty(),
        staged_count,
        unstaged_count,
        untracked_count,
        conflicted_count,
        added_total: 0,
        deleted_total: 0,
        files,
        error: None,
    }
}

/// Split a v2 entry into its `XY` status pair and its path.
///
/// `fields` is the number of space-separated fields before the path, which differs per record
/// type; the path is whatever follows and may itself contain spaces.
fn parse_git_entry_fields(text: &str, fields: usize) -> Option<((char, char), String)> {
    let mut parts = text.splitn(fields, ' ');
    let _record_type = parts.next()?;
    let mut status = parts.next()?.chars();
    let xy = (status.next()?, status.next()?);
    // `splitn` simply yields fewer parts when the record is short, so the count has to be checked:
    // otherwise a truncated record hands back one of the mode or object-id fields as the path.
    let rest: Vec<&str> = parts.collect();
    if rest.len() != fields - 2 {
        return None;
    }
    let path = rest.last()?;
    if path.is_empty() {
        return None;
    }
    Some((xy, (*path).to_string()))
}

/// `# branch.ab +<ahead> -<behind>`, absent when there is no upstream to compare against.
fn parse_git_ahead_behind(value: &str) -> (u32, u32) {
    let mut ahead = 0;
    let mut behind = 0;
    for field in value.split_whitespace() {
        if let Some(count) = field.strip_prefix('+') {
            ahead = count.parse().unwrap_or(0);
        } else if let Some(count) = field.strip_prefix('-') {
            behind = count.parse().unwrap_or(0);
        }
    }
    (ahead, behind)
}

fn git_short_oid(oid: &str) -> String {
    oid.chars().take(7).collect()
}

fn git_status_name(status: char) -> &'static str {
    match status {
        // v2 spells "unchanged on this side" as a dot rather than a space.
        '.' | ' ' => "unmodified",
        'M' => "modified",
        'A' => "added",
        'D' => "deleted",
        'R' => "renamed",
        'C' => "copied",
        // A file swapped for a symlink, say — neither an add nor a plain edit.
        'T' => "typechange",
        'U' => "unmerged",
        '?' => "untracked",
        '!' => "ignored",
        _ => "unknown",
    }
}

/// The overall kind of an ordinary or renamed entry, from the side that carries the change.
/// Unmerged and untracked entries never reach here — their record type already named them.
fn git_file_kind(index: char, worktree: char) -> &'static str {
    if matches!(index, 'R' | 'C' | 'T') {
        git_status_name(index)
    } else if matches!(worktree, 'R' | 'C' | 'T') {
        git_status_name(worktree)
    } else if index == 'D' || worktree == 'D' {
        "deleted"
    } else if index == 'A' || worktree == 'A' {
        "added"
    } else {
        "modified"
    }
}

pub struct GitNumstatEntry {
    pub path: String,
    pub added: Option<u32>,
    pub deleted: Option<u32>,
}

/// Parse `git diff --numstat -z` output.
///
/// Records are NUL-separated. A plain entry is `added\tdeleted\tpath`; a rename or copy emits the
/// counts with an empty path field and follows them with two more records, the old path then the
/// new one. Binary files report `-` for both counts.
pub fn parse_git_numstat(output: &[u8]) -> Vec<GitNumstatEntry> {
    let mut entries = Vec::new();
    let mut records = output
        .split(|byte| *byte == 0)
        .filter(|record| !record.is_empty());
    while let Some(record) = records.next() {
        let text = String::from_utf8_lossy(record);
        let mut fields = text.splitn(3, '\t');
        let (Some(added), Some(deleted), Some(rest)) =
            (fields.next(), fields.next(), fields.next())
        else {
            continue;
        };
        let path = if rest.is_empty() {
            // Rename or copy: the old path is emitted first, then the new one, which is the path
            // `git status` reports for the entry.
            let _old_path = records.next();
            match records.next() {
                Some(new_path) => String::from_utf8_lossy(new_path).to_string(),
                None => continue,
            }
        } else {
            rest.to_string()
        };
        entries.push(GitNumstatEntry {
            path,
            added: parse_numstat_count(added),
            deleted: parse_numstat_count(deleted),
        });
    }
    entries
}

fn parse_numstat_count(field: &str) -> Option<u32> {
    if field == "-" {
        return None;
    }
    field.parse().ok()
}

pub fn index_numstat(entries: Vec<GitNumstatEntry>) -> HashMap<String, GitNumstatEntry> {
    entries
        .into_iter()
        .map(|entry| (entry.path.clone(), entry))
        .collect()
}

pub fn apply_numstat_counts(
    status: &mut GitStatusResponse,
    staged: &HashMap<String, GitNumstatEntry>,
    unstaged: &HashMap<String, GitNumstatEntry>,
) {
    let mut added_total = 0u32;
    let mut deleted_total = 0u32;
    for file in &mut status.files {
        // An unmerged path has no single line count to report: git diffs it against each merge
        // stage, so numstat emits one record per stage for the same path and any one of them is an
        // artifact of the conflict rather than a change someone made. Such a file keeps `None` on
        // both sides and contributes nothing to the totals — the UI shows the conflict itself.
        if file.kind == "unmerged" {
            continue;
        }
        if let Some(entry) = staged.get(&file.path) {
            file.staged_added = entry.added;
            file.staged_deleted = entry.deleted;
        }
        if let Some(entry) = unstaged.get(&file.path) {
            file.unstaged_added = entry.added;
            file.unstaged_deleted = entry.deleted;
        }
        // Binary files contribute nothing to the totals: numstat reports `-` for them, so there is
        // no line count to add.
        for value in [file.staged_added, file.unstaged_added] {
            added_total = added_total.saturating_add(value.unwrap_or(0));
        }
        for value in [file.staged_deleted, file.unstaged_deleted] {
            deleted_total = deleted_total.saturating_add(value.unwrap_or(0));
        }
    }
    status.added_total = added_total;
    status.deleted_total = deleted_total;
}

#[cfg(test)]
mod tests {
    use super::*;

    // Fixtures are bytes captured from real `git` output; see the crate docs for the two formats.
    /// With `--untracked-files=normal` an untracked directory arrives as a single entry with a
    /// trailing slash, standing for everything beneath it. Listing each file instead turns an
    /// unignored `node_modules` into thousands of rows.
    #[test]
    fn keeps_an_untracked_directory_as_one_entry() {
        let status = parse_git_status(b"# branch.head main\x00? node_modules/\x00? loose.txt\x00");

        assert_eq!(status.untracked_count, 2);
        assert_eq!(status.files[0].path, "node_modules/");
        assert_eq!(status.files[0].kind, "untracked");
        assert_eq!(status.files[1].path, "loose.txt");
    }

    #[test]
    fn parses_git_status_v2_records() {
        let status = parse_git_status(
            b"# branch.oid 1a2b3c4d5e6f7a8b9c0d\x00# branch.head main\x00\
              # branch.upstream origin/main\x00# branch.ab +2 -1\x00\
              1 .M N... 100644 100644 100644 aaa bbb src/lib.rs\x00\
              1 A. N... 000000 100644 100644 ccc ddd src/new.rs\x00\
              ? notes.txt\x00",
        );

        assert!(status.is_repository);
        assert_eq!(status.branch.as_deref(), Some("main"));
        // The revision comes straight from the header, shortened for display.
        assert_eq!(status.head.as_deref(), Some("1a2b3c4"));
        assert!(!status.detached);
        assert_eq!(status.ahead, 2);
        assert_eq!(status.behind, 1);
        assert!(status.dirty);
        assert_eq!(status.staged_count, 1);
        assert_eq!(status.unstaged_count, 1);
        assert_eq!(status.untracked_count, 1);
        assert_eq!(status.files[0].path, "src/lib.rs");
        assert_eq!(status.files[0].index_status, "unmodified");
        assert_eq!(status.files[0].worktree_status, "modified");
        assert_eq!(status.files[1].kind, "added");
        assert_eq!(status.files[2].kind, "untracked");
    }

    /// A rename entry is followed by its original path as its own record.
    #[test]
    fn parses_a_rename_entry_and_its_original_path() {
        let status = parse_git_status(
            b"# branch.head main\x00\
              2 R. N... 100644 100644 100644 aaa bbb R100 new name.txt\x00old name.txt\x00",
        );

        assert_eq!(status.files.len(), 1);
        assert_eq!(status.files[0].kind, "renamed");
        assert_eq!(status.files[0].path, "new name.txt");
        assert_eq!(status.files[0].old_path.as_deref(), Some("old name.txt"));
    }

    /// v2 names the detached state and carries the revision, so nothing has to be inferred from
    /// prose or fetched with a second command.
    #[test]
    fn reads_the_detached_head_revision_from_the_header() {
        let status =
            parse_git_status(b"# branch.oid fdfba2c961ff9b32418e\x00# branch.head (detached)\x00");

        assert!(status.detached);
        assert_eq!(status.branch, None);
        assert_eq!(status.head.as_deref(), Some("fdfba2c"));
    }

    /// An unborn branch reports its name normally; only the revision is missing.
    #[test]
    fn reads_an_unborn_branch_normally() {
        let status =
            parse_git_status(b"# branch.oid (initial)\x00# branch.head main\x00? first.txt\x00");

        assert_eq!(status.branch.as_deref(), Some("main"));
        assert_eq!(status.head, None);
        assert!(!status.detached);
        assert_eq!(status.untracked_count, 1);
    }

    /// Conflicts are their own record type, so the `AA` and `DD` pairs that carry no `U` need no
    /// special handling — the case that made an add/add conflict read as a staged add.
    #[test]
    fn treats_every_unmerged_record_as_a_conflict() {
        let status = parse_git_status(
            b"# branch.head main\x00\
              u AA N... 100644 100644 100644 100644 a b c both-added.txt\x00\
              u DD N... 100644 100644 100644 100644 a b c both-deleted.txt\x00\
              u AU N... 100644 100644 100644 100644 a b c ours.txt\x00\
              1 M. N... 100644 100644 100644 aaa bbb staged.txt\x00",
        );

        assert_eq!(status.conflicted_count, 3);
        assert_eq!(status.staged_count, 1);
        assert_eq!(status.unstaged_count, 0);
        for file in status.files.iter().take(3) {
            assert_eq!(file.kind, "unmerged", "{} misclassified", file.path);
        }
    }

    #[test]
    fn keeps_plain_adds_and_deletes_out_of_the_conflict_bucket() {
        let status = parse_git_status(
            b"# branch.head main\x00\
              1 A. N... 000000 100644 100644 aaa bbb added.txt\x00\
              1 .D N... 100644 100644 000000 aaa bbb deleted.txt\x00\
              1 AM N... 000000 100644 100644 aaa bbb mixed.txt\x00",
        );

        assert_eq!(status.conflicted_count, 0);
        assert_eq!(status.files[0].kind, "added");
        assert_eq!(status.files[1].kind, "deleted");
        assert_eq!(status.files[2].kind, "added");
    }

    /// A file swapped for a symlink is neither an add nor an edit.
    #[test]
    fn names_the_typechange_status() {
        let status = parse_git_status(
            b"# branch.head main\x00\
              1 T. N... 100644 120000 120000 aaa bbb swapped.txt\x00\
              1 .T N... 100644 100644 120000 aaa bbb other.txt\x00",
        );

        assert_eq!(status.files[0].kind, "typechange");
        assert_eq!(status.files[0].index_status, "typechange");
        assert_eq!(status.files[1].kind, "typechange");
        assert_eq!(status.files[1].worktree_status, "typechange");
    }

    /// Paths may contain spaces, so the path is whatever follows the fixed field count.
    #[test]
    fn keeps_spaces_in_entry_paths() {
        let status = parse_git_status(
            b"# branch.head main\x001 .M N... 100644 100644 100644 aaa bbb some dir/a file.txt\x00",
        );

        assert_eq!(status.files[0].path, "some dir/a file.txt");
    }

    #[test]
    fn parses_numstat_records_including_renames_and_binaries() {
        // NULs are written as `\x00`: a `\0` sitting directly before a digit reads as an octal
        // escape, which Rust does not support.
        let entries = parse_git_numstat(
            b"12\t3\tsrc/lib.rs\x00-\t-\tlogo.png\x004\t0\t\x00old.rs\x00new.rs\x00",
        );

        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].path, "src/lib.rs");
        assert_eq!(entries[0].added, Some(12));
        assert_eq!(entries[0].deleted, Some(3));
        assert_eq!(entries[1].path, "logo.png");
        assert_eq!(entries[1].added, None);
        assert_eq!(entries[1].deleted, None);
        // A rename reports the counts once, then the old and new paths; the new path is the one
        // `git status` uses for the same entry.
        assert_eq!(entries[2].path, "new.rs");
        assert_eq!(entries[2].added, Some(4));
    }

    /// git diffs an unmerged path against each merge stage, so numstat emits several records for
    /// the same path. Reporting any one of them would put a number on the row that nobody wrote.
    #[test]
    fn conflicted_files_carry_no_line_counts() {
        let mut status = parse_git_status(
            b"# branch.head main\x00u UU N... 100644 100644 100644 100644 a b c c.txt\x001 M. N... 100644 100644 100644 aaa bbb clean.txt\x00",
        );
        let counts = numstat_map([("c.txt", Some(8), Some(0)), ("clean.txt", Some(3), Some(1))]);

        apply_numstat_counts(&mut status, &counts, &HashMap::new());

        let conflicted = &status.files[0];
        assert_eq!(conflicted.kind, "unmerged");
        assert_eq!(conflicted.staged_added, None);
        assert_eq!(conflicted.unstaged_added, None);
        assert_eq!(status.files[1].staged_added, Some(3));
        // Totals cover the real change only, not the conflict's per-stage artifact.
        assert_eq!(status.added_total, 3);
        assert_eq!(status.deleted_total, 1);
    }

    /// The point of keeping the two numstats apart: a path staged and then modified again is two
    /// changes, and each side has to report its own count rather than one combined figure twice.
    #[test]
    fn keeps_index_and_worktree_line_counts_apart() {
        let mut status = parse_git_status(
            b"# branch.head main\x001 MM N... 100644 100644 100644 aaa bbb both.txt\x00",
        );
        let staged = numstat_map([("both.txt", Some(2), Some(0))]);
        let unstaged = numstat_map([("both.txt", Some(3), Some(1))]);

        apply_numstat_counts(&mut status, &staged, &unstaged);

        let file = &status.files[0];
        assert_eq!(file.staged_added, Some(2));
        assert_eq!(file.staged_deleted, Some(0));
        assert_eq!(file.unstaged_added, Some(3));
        assert_eq!(file.unstaged_deleted, Some(1));
        // The totals still cover the whole working tree, so both sides count once.
        assert_eq!(status.added_total, 5);
        assert_eq!(status.deleted_total, 1);
    }

    /// A binary file reports `-` on the side that changed; the other side is simply absent. Neither
    /// may be reported as zero, which would claim the file has no changed lines.
    #[test]
    fn binary_files_report_no_line_counts() {
        let mut status = parse_git_status(
            b"# branch.head main\x001 M. N... 100644 100644 100644 aaa bbb logo.png\x00",
        );
        let staged = numstat_map([("logo.png", None, None)]);

        apply_numstat_counts(&mut status, &staged, &HashMap::new());

        assert_eq!(status.files[0].staged_added, None);
        assert_eq!(status.files[0].staged_deleted, None);
        assert_eq!(status.added_total, 0);
    }

    fn numstat_map<const N: usize>(
        entries: [(&str, Option<u32>, Option<u32>); N],
    ) -> HashMap<String, GitNumstatEntry> {
        entries
            .into_iter()
            .map(|(path, added, deleted)| {
                (
                    path.to_string(),
                    GitNumstatEntry {
                        path: path.to_string(),
                        added,
                        deleted,
                    },
                )
            })
            .collect()
    }

    // ---- header fields -------------------------------------------------------------------

    /// `# branch.ab` is absent when there is no upstream to compare against.
    #[test]
    fn defaults_ahead_behind_to_zero_without_an_upstream() {
        let status = parse_git_status(b"# branch.oid abc123def456\x00# branch.head main\x00");

        assert_eq!(status.ahead, 0);
        assert_eq!(status.behind, 0);
        assert!(!status.dirty);
        assert!(status.files.is_empty());
    }

    #[test]
    fn parses_ahead_behind_in_either_order_and_ignores_junk() {
        assert_eq!(parse_git_ahead_behind("+2 -1"), (2, 1));
        assert_eq!(parse_git_ahead_behind("-4 +3"), (3, 4));
        assert_eq!(parse_git_ahead_behind("+0 -0"), (0, 0));
        // Nothing parseable must not panic or half-apply.
        assert_eq!(parse_git_ahead_behind(""), (0, 0));
        assert_eq!(parse_git_ahead_behind("garbage"), (0, 0));
        assert_eq!(parse_git_ahead_behind("+x -y"), (0, 0));
    }

    #[test]
    fn ignores_unknown_header_fields() {
        let status = parse_git_status(
            b"# branch.oid abc123def456\x00# branch.head main\x00# branch.upstream origin/main\x00\
              # something.new whatever\x00",
        );

        assert_eq!(status.branch.as_deref(), Some("main"));
        assert_eq!(status.head.as_deref(), Some("abc123d"));
    }

    #[test]
    fn shortens_the_object_id_for_display() {
        let status = parse_git_status(
            b"# branch.oid 0123456789abcdef0123456789abcdef01234567\x00# branch.head (detached)\x00",
        );

        assert_eq!(status.head.as_deref(), Some("0123456"));
    }

    // ---- entry records -------------------------------------------------------------------

    /// Ignored entries are never requested, but a `!` record must not be mistaken for a file.
    #[test]
    fn skips_ignored_records() {
        let status = parse_git_status(b"# branch.head main\x00! build/\x00? real.txt\x00");

        assert_eq!(status.files.len(), 1);
        assert_eq!(status.files[0].path, "real.txt");
    }

    #[test]
    fn reports_a_copy_as_its_own_kind() {
        let status = parse_git_status(
            b"# branch.head main\x00\
              2 C. N... 100644 100644 100644 aaa bbb C75 copy.txt\x00source.txt\x00",
        );

        assert_eq!(status.files[0].kind, "copied");
        assert_eq!(status.files[0].old_path.as_deref(), Some("source.txt"));
    }

    /// A rename whose original path record is missing must drop the entry rather than swallow the
    /// following record as if it were one.
    #[test]
    fn drops_a_rename_missing_its_original_path() {
        let status = parse_git_status(
            b"# branch.head main\x002 R. N... 100644 100644 100644 aaa bbb R100 new.txt\x00",
        );

        assert!(status.files.is_empty());
    }

    #[test]
    fn counts_a_file_changed_on_both_sides_once() {
        let status = parse_git_status(
            b"# branch.head main\x001 MM N... 100644 100644 100644 aaa bbb both.txt\x00",
        );

        // One path, but it is genuinely staged and modified, so it counts on both sides.
        assert_eq!(status.files.len(), 1);
        assert_eq!(status.staged_count, 1);
        assert_eq!(status.unstaged_count, 1);
    }

    #[test]
    fn treats_a_deleted_worktree_file_as_unstaged() {
        let status = parse_git_status(
            b"# branch.head main\x001 .D N... 100644 100644 000000 aaa bbb gone.txt\x00",
        );

        assert_eq!(status.staged_count, 0);
        assert_eq!(status.unstaged_count, 1);
        assert_eq!(status.files[0].kind, "deleted");
    }

    #[test]
    fn keeps_untracked_out_of_the_staged_and_unstaged_counts() {
        let status = parse_git_status(b"# branch.head main\x00? a.txt\x00? b/\x00");

        assert_eq!(status.untracked_count, 2);
        assert_eq!(status.staged_count, 0);
        assert_eq!(status.unstaged_count, 0);
        assert!(status.dirty);
    }

    // ---- malformed input -----------------------------------------------------------------

    /// Truncated or unrecognized records are skipped rather than panicking: this is the output of
    /// another program, and a parser that unwraps on it takes the whole request down.
    #[test]
    fn survives_truncated_and_unknown_records() {
        let status = parse_git_status(
            b"# branch.head main\x001\x001 M\x001 M. N...\x00x nonsense\x00\x00\
              1 .M N... 100644 100644 100644 aaa bbb good.txt\x00",
        );

        assert_eq!(status.files.len(), 1);
        assert_eq!(status.files[0].path, "good.txt");
    }

    #[test]
    fn survives_empty_output() {
        let status = parse_git_status(b"");

        assert!(status.is_repository);
        assert!(!status.dirty);
        assert_eq!(status.branch, None);
        assert_eq!(status.head, None);
        assert!(status.files.is_empty());
    }

    /// `-z` emits paths verbatim, so a filename with a newline in it must survive intact — the
    /// case that makes the NUL-terminated form worth using over the line-oriented one.
    #[test]
    fn keeps_a_newline_inside_a_path() {
        let status = parse_git_status(
            b"# branch.head main\x001 .M N... 100644 100644 100644 aaa bbb we\nird.txt\x00",
        );

        assert_eq!(status.files[0].path, "we\nird.txt");
    }

    #[test]
    fn keeps_non_utf8_paths_lossily_rather_than_dropping_them() {
        // A path that is not valid UTF-8 still has to appear; the replacement character is a fair
        // trade against losing the entry entirely.
        let status = parse_git_status(
            b"# branch.head main\x001 .M N... 100644 100644 100644 aaa bbb b\xffad.txt\x00",
        );

        assert_eq!(status.files.len(), 1);
        assert!(status.files[0].path.contains("ad.txt"));
    }

    // ---- numstat -------------------------------------------------------------------------

    #[test]
    fn parses_numstat_counts_and_binaries() {
        assert_eq!(parse_numstat_count("12"), Some(12));
        assert_eq!(parse_numstat_count("0"), Some(0));
        // Binary files report `-` on both sides.
        assert_eq!(parse_numstat_count("-"), None);
        assert_eq!(parse_numstat_count("nonsense"), None);
    }

    #[test]
    fn survives_malformed_numstat_records() {
        let entries = parse_git_numstat(b"nofields\x0012\x0012\t3\tok.rs\x00");

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].path, "ok.rs");
    }

    #[test]
    fn indexes_numstat_entries_by_path() {
        let indexed = index_numstat(parse_git_numstat(b"1\t2\ta.rs\x003\t4\tb.rs\x00"));

        assert_eq!(indexed.len(), 2);
        assert_eq!(indexed["a.rs"].added, Some(1));
        assert_eq!(indexed["b.rs"].deleted, Some(4));
    }

    #[test]
    fn leaves_counts_unset_for_files_numstat_never_mentioned() {
        let mut status = parse_git_status(
            b"# branch.head main\x001 .M N... 100644 100644 100644 aaa bbb absent.txt\x00",
        );

        apply_numstat_counts(&mut status, &HashMap::new(), &HashMap::new());

        assert_eq!(status.files[0].staged_added, None);
        assert_eq!(status.files[0].unstaged_added, None);
        assert_eq!(status.added_total, 0);
    }

    #[test]
    fn sums_totals_across_both_sides_of_every_file() {
        let mut status = parse_git_status(
            b"# branch.head main\x00\
              1 MM N... 100644 100644 100644 aaa bbb both.txt\x00\
              1 .M N... 100644 100644 100644 aaa bbb work.txt\x00",
        );
        let staged = numstat_map([("both.txt", Some(2), Some(1))]);
        let unstaged = numstat_map([
            ("both.txt", Some(3), Some(0)),
            ("work.txt", Some(5), Some(4)),
        ]);

        apply_numstat_counts(&mut status, &staged, &unstaged);

        // both.txt contributes from each side (2 + 3 added, 1 + 0 deleted), work.txt from one.
        assert_eq!(status.added_total, 10);
        assert_eq!(status.deleted_total, 5);
    }
}
