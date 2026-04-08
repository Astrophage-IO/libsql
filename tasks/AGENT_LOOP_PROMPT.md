# Agent Loop Prompt

This is the master prompt for the build-review-incorporate cycle, dispatched every 30 minutes.

---

## Cycle Structure (3 phases per cycle)

### Phase 1: BUILD (1 Opus 4.6 agent)

Implement the next pending task from STATUS.md.

**Workflow:**
1. Read `tasks/STATUS.md` to find the first `pending` or `in_progress` task
2. Read that task file for the full spec
3. Read ALL existing code in `libsql-graph/src/` to understand current state
4. Run `cd /Users/manash/personal/libsql/libsql-graph && cargo test` to verify baseline
5. Implement the task following the spec precisely
6. Write ALL tests listed in the task
7. Run `cargo test` -- ALL tests must pass (current + all previous)
8. Run `cargo clippy` -- no warnings
9. Update `tasks/STATUS.md`: mark task as `done`, write handoff notes

### Phase 2: REVIEW (2-3 Opus 4.6 agents in parallel)

After the builder finishes, dispatch 2-3 reviewer agents that each independently review the work:

**Reviewer 1 -- Correctness & Tests:**
- Read the task spec and the implementation
- Are all spec requirements met? Any missing functionality?
- Are all specified tests present and correct?
- Are there edge cases the tests miss?
- Run `cargo test` to confirm everything passes
- Write additional tests if gaps found
- Report: pass/fail + list of issues + any new tests written

**Reviewer 2 -- Code Quality & Upstream Syncability:**
- Read the implementation
- Is the code cleanly separated from libsql core? (all graph code in `libsql-graph/`)
- Are there any unnecessary dependencies on libsql internals that would break on upstream sync?
- Is the pager bridge abstracted enough that swapping to real FFI later is a clean change?
- Are types/traits well-defined so components are independently testable?
- No dead code, no unnecessary comments, no half-baked stubs
- Report: pass/fail + list of issues

**Reviewer 3 -- Architecture & Design Alignment:**
- Read the design doc (`docs/plans/2026-04-08-graph-engine-design.md`)
- Does the implementation match the design?
- Are record formats exactly as specified? (byte offsets, sizes)
- Is index-free adjacency preserved? (no B-tree lookups in hot paths)
- Will this compose correctly with future tasks?
- Report: pass/fail + list of issues

### Phase 3: INCORPORATE

If reviewers found issues:
1. Dispatch a final Opus 4.6 agent to fix all reported issues
2. Re-run `cargo test` to confirm green
3. Update STATUS.md with the fixes

If reviewers found no issues: cycle complete, move on.

---

## Key Rules

### No Half-Baked Work
- Every task must be FULLY implemented before marking done
- All tests must pass. Not "most tests" -- ALL tests.
- If a task can't be completed in one cycle, mark it `in_progress` with clear notes on what's left
- The next cycle picks up where the previous left off

### Upstream Syncability
- ALL graph engine code lives in `libsql-graph/` crate -- nothing added to libsql core files
- The pager bridge is the ONLY integration point with libsql internals
- `libsql-graph` depends only on `libsql-ffi` (for future FFI) and std lib
- No modifications to `libsql-sqlite3/`, `libsql-server/`, `libsql-sys/`, or any other existing crate
- The workspace Cargo.toml addition (`"libsql-graph"` in members) is the only change to existing files
- This means: `git pull upstream main` should merge cleanly with zero conflicts
- If a task REQUIRES modifying existing libsql files, document it as a deviation and minimize the change

### Code Standards
- No unnecessary comments. Code should be self-documenting.
- No dead code. No TODO stubs that aren't immediately needed.
- Every public API must have tests.
- `cargo clippy` clean. `cargo test` green. No exceptions.

## Key References

- Design doc: `docs/plans/2026-04-08-graph-engine-design.md`
- Workspace Cargo.toml: `Cargo.toml`
- FFI bindings: `libsql-ffi/` and `libsql-sys/`
- Pager API: `libsql-sqlite3/src/pager.h`
- Task specs: `tasks/001-*.md` through `tasks/022-*.md`
