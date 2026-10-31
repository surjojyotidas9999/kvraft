# kvraft

A key-value database storage engine built from scratch in C++17, with no external libraries.
It uses an LSM-tree design, the same architecture behind LevelDB, RocksDB and Cassandra.
Distributed replication using the Raft consensus algorithm is in progress.

## Features

- **Write-ahead log (WAL)**: every write is appended to disk first, with a CRC32 checksum per record, so the database recovers after a crash. Half-written records at the end of the log are detected and discarded.
- **Memtable**: sorted in-memory store that serves recent writes.
- **SSTables**: when the memtable fills up, it is flushed to an immutable sorted file. Writes to these files are atomic (write to a temp file, then rename).
- **Tombstones**: deletes are recorded as markers so they correctly hide older values in older files.
- **Compaction**: SSTables are automatically merged, so reads stay fast and overwritten data is dropped. It is crash-safe: the merged file replaces the newest file atomically, and the older files are deleted only afterwards.
- **Configurable durability**: each write can either `fsync` immediately or be batched and synced later.

## Benchmarks

Measured on WSL2 (Ubuntu), 100-byte values, compiled with `-O2`.

| Operation | Throughput |
|---|---|
| Writes, fsync every write | 222 ops/sec |
| Writes, fsync every 100 writes | 9,617 ops/sec |
| Writes, fsync once at the end | 18,261 ops/sec |
| Reads (key exists) | ~190,000 ops/sec |
| Reads (key missing) | ~2,300,000 ops/sec |

Batching fsyncs gives a ~43x write speedup over syncing every write, at the cost of possibly losing the last batch on power failure. The caller chooses.

## Build and run

```
g++ -std=c++17 -Wall src/main.cpp src/storage/wal.cpp src/storage/sstable.cpp -o kvraft
./kvraft
```

Shell commands: `put <key> <value>`, `get <key>`, `del <key>`, `flush`, `compact`, `stats`, `fill <n>`, `quit`.

Run the benchmark:

```
g++ -std=c++17 -O2 bench/bench.cpp src/storage/wal.cpp src/storage/sstable.cpp -o bench_run
./bench_run
```

## Design notes

- Tombstones are kept during compaction on purpose. If the process crashes after the merged file is written but before the old files are deleted, a deleted key can never reappear.
- Reads check the memtable first, then SSTables from newest to oldest, so the newest value always wins.

## Roadmap

- [x] Write-ahead log with checksums and crash recovery
- [x] Memtable and SSTable flush
- [x] Compaction
- [x] Benchmark harness
- [ ] Bloom filters and sparse index for datasets larger than RAM
- [ ] TCP server and client protocol
- [ ] Raft: leader election and log replication
- [ ] Fault-injection tests (killed nodes, network partitions)
- [ ] Docker Compose cluster demo