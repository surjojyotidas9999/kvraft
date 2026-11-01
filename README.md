# kvraft

A distributed, fault-tolerant key-value database built from scratch in C++17, with no external libraries.

- **Storage engine**: an LSM tree (the design behind LevelDB, RocksDB and Cassandra) with a write-ahead log, memtable, SSTables and compaction.
- **Replication**: the Raft consensus algorithm (leader election, log replication, crash recovery) keeps a 3-node cluster consistent.
- **Tested with fault injection**: a script hard-kills nodes while writes are in flight, then checks that no acknowledged write was lost.

## Architecture

```
  client ──► any node ──► leader
                            │  1. append command to its Raft log (fsync)
                            │  2. replicate to followers (AppendEntries)
                            │  3. wait for a majority to confirm
                            ▼
                  committed ──► applied to the storage engine

  Storage engine (on each node)
  ┌────────────┐  flush   ┌──────────────┐  compaction  ┌──────────┐
  │  memtable  │ ───────► │  SSTables    │ ───────────► │ 1 merged │
  │  + WAL     │          │  (sorted)    │              │ SSTable  │
  └────────────┘          └──────────────┘              └──────────┘
```

## Try it (Docker)

```
docker compose up --build -d
./client.sh "PUT name hello"
./client.sh "GET name"
docker compose logs | grep "became LEADER"     # find the leader
docker compose stop node1                       # kill it (use the leader's name)
./client.sh "PUT still works"                   # the cluster elects a new leader
docker compose down
```

`client.sh` finds the leader by trying each node. A follower answers `NOT_LEADER <id>`.

## Raft implementation

- **Leader election**: randomized 500 to 1000 ms election timeouts, 50 ms heartbeats, one vote per term. Term and vote are saved to disk (fsync) before replying, so a restarted node can never vote twice in a term.
- **Election safety**: a node only votes for a candidate whose log is at least as up to date as its own.
- **Log replication**: the leader sends entries with a consistency check on the previous entry. A follower with a conflicting log truncates it and the leader steps back until the logs match.
- **Commit rule**: an entry is committed only when a majority has stored it. A new leader first commits an empty no-op entry, so entries from earlier terms are only committed indirectly, as the Raft paper requires.
- **Durability**: the Raft log is fsynced before a node acknowledges it. The key-value database on each node is a state machine rebuilt from the log at startup.
- **Reads** go through the log too, so a deposed leader can never return stale data.

## Fault-injection test

`chaos.sh` starts a 3-node cluster, then:

1. Runs a background writer. Only writes answered `OK` count as acknowledged.
2. Repeatedly `kill -9`s random nodes (the leader half the time) and restarts them while writes continue.
3. Waits for the cluster to converge, then reads back **every acknowledged write** through the leader and from each node's own copy.
4. Hard-kills **all** nodes at once, restarts them, and checks everything again.

Result: **153 acknowledged writes, 0 lost.**

Run it with `./chaos.sh` (build `raft_node` first, see below).

## Storage engine

- **Write-ahead log**: each record is appended with a CRC32 checksum. A half-written record from a crash is detected and discarded on recovery.
- **Memtable and SSTables**: the memtable is flushed to an immutable sorted file when full. Files are written to a temp name and renamed, so they are never half-written.
- **Tombstones**: deletes are markers that hide older values in older files.
- **Compaction**: SSTables are merged automatically. The merged file atomically replaces the newest file, and older files are deleted only afterwards. Tombstones are deliberately kept, so a crash in the middle can never bring a deleted key back.
- **Configurable durability**: each write can fsync immediately or be batched.

### Benchmarks

Single node, WSL2 Ubuntu, 100-byte values, `-O2`:

| Operation | Throughput |
|---|---|
| Writes, fsync every write | 222 ops/sec |
| Writes, fsync every 100 writes | 9,617 ops/sec |
| Writes, fsync once at the end | 18,261 ops/sec |
| Reads (key exists) | ~190,000 ops/sec |
| Reads (key missing) | ~2,300,000 ops/sec |

Batching fsyncs gives a ~43x write speedup over syncing every write, at the cost of possibly losing the last batch on power failure.

## Build without Docker

```
g++ -std=c++17 -O2 -pthread src/raft_main.cpp src/storage/wal.cpp src/storage/sstable.cpp -o raft_node
./raft_node 1 8001 8002 8003      # in three terminals, with ids 1, 2 and 3
```

Single-node shell: `g++ -std=c++17 -O2 src/main.cpp src/storage/wal.cpp src/storage/sstable.cpp -o kvraft && ./kvraft`

Single-node benchmark: `g++ -std=c++17 -O2 bench/bench.cpp src/storage/wal.cpp src/storage/sstable.cpp -o bench_run && ./bench_run`

## Known limitations

- The cluster has a fixed size of 3 nodes (no membership changes).
- The Raft log is never trimmed (no snapshots), so restarts replay the whole log.
- Reads go through the log, which is correct but slower than lease-based reads.
- Tombstones are never dropped from SSTables.
- Each SSTable keeps its full key index in memory (a sparse index and bloom filters would remove that limit).

## Roadmap

- [x] Write-ahead log, memtable, SSTables, compaction
- [x] Benchmarks
- [x] Raft leader election
- [x] Raft log replication
- [x] Fault-injection tests
- [x] Docker Compose cluster
- [ ] Raft snapshots (log compaction)
- [ ] Sparse index and bloom filters
- [ ] Faster reads (read leases)