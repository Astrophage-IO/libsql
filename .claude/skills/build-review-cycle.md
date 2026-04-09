---
name: build-review-cycle
description: Execute a build-review-incorporate cycle for a libsql-graph task. Reads the task spec, dispatches a builder agent, then 3 reviewer agents, then an incorporator if needed.
---

# Build-Review-Incorporate Cycle

## When to use
When executing a task from `tasks/STATUS.md` in the libsql-graph project.

## Steps

### Phase 1: BUILD
1. Read `tasks/STATUS.md` to find the first `pending` task
2. Read that task's spec file from `tasks/`
3. Run `cargo test -p libsql-graph` to verify baseline
4. Dispatch an Opus agent with `bypassPermissions` mode to implement the task
5. Verify: all tests pass, clippy clean

### Phase 2: REVIEW
Dispatch 3 Opus agents in parallel (all background):
- **Reviewer 1 (Correctness)**: Verify all spec requirements met, all required tests present, run `cargo test` and `cargo clippy`
- **Reviewer 2 (Code Quality)**: Check code isolation in `libsql-graph/`, no dead code, no panics on user data, no unnecessary comments, clippy clean
- **Reviewer 3 (Architecture)**: Check design alignment, future task composability, crash safety, rollback correctness, backward compatibility

### Phase 3: INCORPORATE
If any reviewer found blocking issues:
1. Dispatch an Opus agent with `bypassPermissions` to fix all reported issues
2. Re-run `cargo test` and `cargo clippy`
3. Verify fixes address each reported issue

If no blocking issues: skip this phase.

### Finalize
1. Update `tasks/STATUS.md`: mark task `done`, update test counts, add handoff notes
2. Commit with conventional commit message: `feat|fix(scope): description`
3. Report summary: files changed, tests added, reviewer verdicts, issues found/fixed

## Key Rules
- Every task must be FULLY implemented before marking done
- ALL tests must pass — not "most tests", ALL tests
- `cargo clippy -p libsql-graph -- -D warnings` must be clean
- All graph code stays in `libsql-graph/` — no modifications to other crates
- No `git push` — only `git commit`
