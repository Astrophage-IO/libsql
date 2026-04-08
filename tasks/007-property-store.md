# Task 007: Property Store (Inline + Overflow)

**Status**: pending
**Depends on**: 005 (token store for property key names), 006 (freespace)

## Objective

Implement property storage: small properties inline in node/rel records (40B / 18B), larger properties in overflow chain (64-byte property records with 4 property blocks each).

## Record Format (64 bytes)

```
0       1     flags              [inUse:1]
1       1     block_count        (1-4)
2       4     next_prop_page     chain to next property record
6       2     next_prop_slot
8       56    blocks             4 x 14-byte property blocks:
                                   [2B key_token_id][1B type][1B size][10B value_or_ptr]
```

## Deliverables

- `PropertyValue` enum: Null, Bool, Int32, Int64, Float64, String(up to 10 bytes), StringRef(overflow)
- `PropertyBlock` struct: key_token_id, value
- `PropertyRecord` struct: up to 4 blocks + next chain pointer
- `PropertyStore`:
  - `set_properties(target_addr: RecordAddress, props: &[(u32, PropertyValue)])` -- write inline + overflow
  - `get_properties(target_addr: RecordAddress, inline_data: &[u8]) -> Vec<(u32, PropertyValue)>`
  - `get_property(target_addr, key_token_id) -> Option<PropertyValue>`
- Helper to pack/unpack inline properties in node/rel record's inline_properties field

## Tests

1. `test_single_inline_property` -- set name="Alice" on node, read back
2. `test_multiple_inline_properties` -- set 3 small props that fit in 40 bytes
3. `test_overflow_to_chain` -- set 10 properties (exceeds inline), verify chain created
4. `test_property_types` -- write/read each PropertyValue variant
5. `test_update_property` -- set, then overwrite, verify new value
6. `test_delete_properties` -- clear all properties, verify chain freed
7. `test_persistence` -- set props, commit, reopen, read back

## Handoff

Update STATUS.md. Next: `008-graph-api.md`
