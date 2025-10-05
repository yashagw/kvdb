# B+ Tree Implementation

A B+ tree implementation in Go, following the principles taught in [CS 186 Berkeley's Database course](https://cs186berkeley.net/resources/static/notes/n04-B+Trees.pdf).

## Overview

B+ trees are balanced tree data structures that optimize database operations by:
- Maintaining sorted data for efficient range queries
- Self-balancing to ensure O(log n) operations
- Linking leaf nodes for sequential access

## Implementation Details

### Current Features
- Insert with node splitting
- Get operations
- Simple delete (without handling underflow)
- Parent pointers for easier tree navigation

### Design Decisions

Following Berkeley CS 186's guidance:
> "To delete a value, just find the appropriate leaf and delete the unwanted value from that leaf. That's all there is to it. (Yes, technically we could end up violating some of the invariants of a B+ tree. That's okay because in practice we get way more insertions than deletions so something will quickly replace whatever we delete.)"

This implementation uses this simplified deletion approach as:
1. Insertions are more common than deletions in practice
2. Any underflow will likely be temporary
3. Simplifies the implementation significantly

## Future Enhancements
- [ ] Range queries using leaf node links
- [ ] Full deletion with rebalancing

## References
- [CS 186 Berkeley B+ Tree Notes](https://cs186berkeley.net/resources/static/notes/n04-B+Trees.pdf)
