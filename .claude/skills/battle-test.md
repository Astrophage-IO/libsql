---
name: battle-test
description: Dispatch 10 parallel Opus agents to battle test the libsql-graph engine across different subsystems. Each agent writes a Rust test file and reports pass/fail.
---

# Battle Test

## When to use
After completing a major feature or phase, to stress-test the engine.

## Agents to dispatch (all in parallel, all Opus, all background)

1. **Stress Nodes** — 500 nodes, 10 labels, properties, persistence (use page_size=65536 to avoid page collision bug)
2. **Rel Chains** — 100-node chain, supernode hub, chain traversal, delete breaks chain
3. **Transactions** — commit/rollback, counter restore, batch atomicity, persistence
4. **Cypher Queries** — 12+ diverse query patterns (ORDER BY, WHERE, LIMIT, SKIP, DISTINCT, aggregation, string predicates, error handling, empty results)
5. **WAL Crash Recovery** — persistence, checkpoint, corruption recovery, rapid open/close cycles
6. **Properties** — all value types (String, Int32, Int64, Float64, Bool), overflow strings, 20 props/node, boundary values (i64::MAX), empty strings
7. **Delete Ops** — cycle deletion, star pattern, Cypher DETACH DELETE, double-delete no-op, integrity check after deletes
8. **Stats & Optimizer** — label counts, rel_type counts, avg_degree, EXPLAIN/PROFILE output, optimizer transparency
9. **Dense Nodes** — supernode conversion, mixed rel types, Cypher after conversion, properties persist, double-convert no-op, persistence
10. **MemPager** — full stack on in-memory backend: nodes, rels, Cypher, transactions, rollback, schema

## Each agent should:
- Write a Rust test file in `libsql-graph/tests/battle/`
- Add the module to `tests/battle.rs`
- Run `cargo test --test battle::<module> -- --nocapture`
- Report pass/fail with details on each sub-test

## Known issues to work around:
- **Page collision bug**: `address_for_id()` overflows store pages at >63 records with 4096-byte pages. Use `page_size=65536` for tests with >60 nodes.
- **Unknown label scan**: `MATCH (n:NonExistentLabel)` returns all nodes instead of 0.

## After all agents complete:
- Compile results into a summary table (agent / tests / pass/fail / bugs found)
- Note any new bugs discovered
- Note any workarounds needed
