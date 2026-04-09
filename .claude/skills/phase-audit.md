---
name: phase-audit
description: Audit the libsql-graph codebase after completing all tasks in a phase. Checks for gaps, untested code paths, and architectural issues.
---

# Phase Audit

## When to use
When all tasks in a phase are marked `done` in `tasks/STATUS.md` and no `pending` tasks remain.

## Checklist

1. **Run all tests**: `cargo test -p libsql-graph` — all must pass
2. **Run clippy**: `cargo clippy -p libsql-graph -- -D warnings` — zero warnings
3. **Dispatch an Opus audit agent** to read every file in `libsql-graph/src/` and check:
   - Dead code, stubs, TODOs, `unimplemented!()`, `todo!()`
   - Public APIs without tests (grep for `pub fn`)
   - Panics on valid/corrupt input (unwrap on user data)
   - Header format consistency
   - Missing error handling
4. **End-to-end CRUD**: Verify create nodes, rels, properties, traverse, delete, integrity check
5. **Full Cypher**: Verify CREATE, MATCH, WHERE, RETURN, DELETE, SET, MERGE all work
6. **Persistence**: Verify close+reopen preserves all data (nodes, rels, properties, stats)
7. **Known gaps from reviewers**: Check handoff notes in STATUS.md for deferred issues

## Output
- **Overall health**: green / yellow / red
- **Gaps found**: list with severity (blocking / medium / low / informational)
- **Recommendations**: what to fix or build next
- If gaps found: create new task files in `tasks/`, add to STATUS.md, and start implementing
- If all clear: update STATUS.md with "Phase Complete" note

## After audit:
If gaps are found, run a build-review-incorporate cycle (use the `build-review-cycle` skill) for the gap-fix task.
