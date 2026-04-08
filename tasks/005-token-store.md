# Task 005: Token Store (Label/Type Name <-> ID)

**Status**: pending
**Depends on**: 002 (GraphDatabase)
**Design doc**: Section 4.4 "Token Record -- 32 bytes"

## Objective

Implement the token store: maps label names (e.g., "Person") and relationship type names (e.g., "KNOWS") to integer token IDs. Used by node records (label_token_id) and rel records (type_token_id).

## Record Format (32 bytes)

```
0       1     flags              [inUse:1][kind:1 (0=label, 1=rel_type)][reserved:6]
1       3     token_id           24-bit
4       1     name_length
5       27    name               UTF-8, up to 27 bytes
```

## Deliverables

- `TokenRecord` struct with from_bytes/to_bytes
- `TokenStore` with:
  - `get_or_create(name, kind) -> token_id` -- idempotent, returns existing ID or creates new
  - `get_by_id(token_id) -> Option<(String, TokenKind)>`
  - `get_by_name(name, kind) -> Option<token_id>`
  - `list_all(kind) -> Vec<(token_id, String)>`
- In-memory cache (HashMap) for fast name->id lookups (loaded on open)

## Tests

1. `test_create_and_lookup_label` -- create "Person", look up by name and by ID
2. `test_idempotent_create` -- create "Person" twice, get same token_id
3. `test_separate_namespaces` -- label "KNOWS" and rel_type "KNOWS" get different token_ids
4. `test_many_tokens` -- create 500 tokens, verify all retrievable
5. `test_persistence` -- create tokens, close, reopen, verify cache rebuilt correctly
6. `test_long_name` -- name exactly 27 bytes, verify no truncation
7. `test_name_too_long` -- name > 27 bytes should error (or use overflow, TBD)

## Handoff

Update STATUS.md. Next: `006-freespace-manager.md`
