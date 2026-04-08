# Task 006: Free-Space Bitmap Manager

**Status**: pending
**Depends on**: 002 (GraphDatabase)

## Objective

Track which slots in each store are free vs occupied using bitmap pages. Enables O(1) amortized slot allocation and O(1) deallocation.

## Design

Each store (node, rel, prop, token) gets a bitmap. Each bit = one slot. 1 = occupied, 0 = free.

Bitmap page layout (4KB, 8-byte header):
- 4088 bytes = 32,704 bits per bitmap page
- For node store (63 slots/page): one bitmap page covers 32,704/63 = ~519 store pages

## Deliverables

- `FreeSpaceManager` per store
  - `alloc_slot() -> RecordAddress` -- find first free bit, set to 1, return address
  - `free_slot(addr: RecordAddress)` -- set bit to 0
  - `is_free(addr: RecordAddress) -> bool`
- Grows bitmap pages automatically when store expands
- Caches "current scan position" for fast sequential allocation

## Tests

1. `test_alloc_sequential` -- alloc 200 slots, verify all unique
2. `test_free_and_realloc` -- alloc, free, alloc again, verify reused
3. `test_bitmap_persistence` -- alloc slots, commit, reopen, verify bitmap correct
4. `test_bitmap_crosses_pages` -- alloc enough to span multiple bitmap pages
5. `test_alloc_performance` -- alloc 10K slots, verify < 100ms

## Handoff

Update STATUS.md. Next: `007-property-store.md`
