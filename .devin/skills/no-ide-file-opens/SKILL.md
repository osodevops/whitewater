---
name: no-ide-file-opens
description: Never open repository files in the IDE — perform all file reads and writes through shell/Python only
triggers:
  - user
  - model
---

# Never open files in the IDE

The user has explicitly required: NEVER OPEN A FILE IN THE IDE — EVER.

The `read`, `edit`, and `write` tools surface touched files inside the user's IDE. The IDE then holds buffers that can overwrite on-disk content (stale-buffer saves have repeatedly reverted committed work in this repository, including `docs/tasks.md`, `src/api.rs`, `src/control.rs`, and `src/active_range/merge.rs`).

## Rules

1. Do not use the `read`, `edit`, or `write` tools on files inside this workspace. View files with `cat`, `sed -n 'A,Bp'`, or `python` via the `exec` tool.
2. Modify files only through `exec` with content-anchored Python scripts or heredocs. Anchor replacements on exact surrounding text and verify the result on disk immediately (`git diff`, `sed`, `python`).
3. If a file operation fails or disk content does not match what was just applied, STOP and report to the user immediately — do not silently retry or re-apply. Surface the failure, show the observed disk state, and wait for direction.
4. Jupyter notebooks are exempt only if `notebook_read`/`notebook_edit` do not surface IDE buffers; prefer `python -m json.tool` inspection via `exec` when possible.
5. `grep`, `find_file_by_name`, and `code_search` are acceptable for locating content (read-only, no buffers opened), but prefer shell tools for reading file contents.

## Commit-time check

Before committing, run `git status` and `git diff` to confirm the disk state matches intent — never trust a tool's reported view of file contents without an on-disk verification.
