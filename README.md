# Database Internals Learning Project

This repository documents my journey learning database internals by implementing core components from scratch.

## 📚 Implementations

### 1. Bitcask Storage Engine
- Located in `/internal/bitcask`
- An append-only log-structured storage engine
- Features: fast writes, O(1) lookups, crash recovery

### 2. B+ Tree Index
- Located in `/internal/bplustree`
- A B+ tree implementation for efficient indexing
