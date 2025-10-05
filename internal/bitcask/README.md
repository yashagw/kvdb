# Bitcask Storage Engine

A simple, fast, and persistent key-value database implementation in Go, inspired by the Bitcask storage model.

## What is this?

This implements the Bitcask storage engine - the same one used by Riak. The core idea is beautifully simple: append-only log files for writes, and an in-memory hash table for fast lookups.

## Features

- **Fast writes**: Append-only log structure (~420K writes/sec)
- **Super fast reads**: In-memory index for O(1) lookups
- **Persistent**: Data survives restarts
- **Crash recovery**: Rebuilds index from log files on startup
- **File rotation**: Automatically creates new files when they get too big
- **Compaction**: Removes old/deleted data to reclaim space
- **Thread-safe**: Concurrent reads and writes using RWMutex

## How it works

### Storage Model
- **Write path**: New entries are appended to the active log file
- **Read path**: Look up key in in-memory index, then read value from file
- **Delete**: Write a "tombstone" entry (zero-length value)
- **File rotation**: When active file gets too big, make it read-only and create a new one

### File Format
Each log entry contains:
```
[timestamp:4][key_size:4][value_size:4][key][value]
```

## Performance

Benchmarked on Apple M3 Pro:

```
BenchmarkPut-11           422,000 ops/sec   (2.4μs per write)
BenchmarkGet-11         1,800,000 ops/sec   (0.56μs per read)
BenchmarkConcurrentReads 1,200,000 ops/sec  (concurrent)
```

## Limitations

- All keys must fit in RAM (the values don't)
- Not optimized for range queries
- Single writer (though multiple concurrent readers work fine)

## Future improvements
- [ ] Background compaction worker
