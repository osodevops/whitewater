# Index engine benchmark — Fjall vs redb vs RocksDB

Reference workload for the M6 engine-selection task. Harness lives in
[`tools/index-bench`](../tools/index-bench) — a standalone crate (own
workspace) so `cargo clippy --all-features` and the standard test suite
never compile the RocksDB C++ toolchain.

```sh
cd tools/index-bench
cargo build --release
IB_ROWS=10000 IB_OPS=8000 IB_BURST=10000 ./target/release/index-bench
# or a subset: index-bench fjall redb
```

## Method

One deterministic logical workload replayed identically against each
engine, using the real `finnstream::index` key codecs — `primary_key`,
`posting_key`, `posting_prefix`, `unique_claim_key`. Three storage
namespaces per engine (primary rows, shared secondary/unique-claim
entries, checkpoints), matching `FjallIndexStore`'s layout:

- **fjall**: `SingleWriterTxDatabase` + 3 keyspaces, `write_tx` with
  `PersistMode::SyncAll`.
- **redb**: `Database` + 3 tables, `WriteTransaction` (Immediate
  durability — fsync per commit).
- **rocksdb**: `DB` + 3 column families, `WriteBatch` with
  `WriteOptions::set_sync(true)`.

Phases:

1. **Bulk upsert** — 10,000 new primary rows; each commit is a
   read-modify-write (get primary, swap 4 posting entries across 32
   logical Indexes, one unique claim, one checkpoint write).
2. **Mixed** — 8,000 ops at 70% upsert (with stale-entry removal), 20%
   bounded prefix lookups resolving each posting back to its primary row,
   10% deletes.
3. **Compaction tail** — 10,000-op burst with no reads, then one lookup
   samples the tail latency the engine left behind.
4. **Hard kill** — a child process writes 500 sync commits then
   `exit()`s without destructors; the parent measures reopen (WAL
   recovery) time and verifies the last committed row survived.

## Results (Docker Desktop, container-local ext4, release build)

| engine | bulk op/s | commit p50 | commit p99 | mixed op/s | upsert p99 |
|---|---|---|---|---|---|
| redb | **253** | **3.07 ms** | **10.06 ms** | **377** | **7.01 ms** |
| fjall | 185 | 5.00 ms | 16.35 ms | 232 | 16.25 ms |
| rocksdb | 179 | 5.03 ms | 16.31 ms | 226 | 16.30 ms |

| engine | lookup p50 | lookup p99 | delete p50 | tail@burst | reopen | survived |
|---|---|---|---|---|---|---|
| redb | **13.1 µs** | **74.3 µs** | **3.00 ms** | **11.6 µs** | **33.6 ms** | yes |
| fjall | 14.9 µs | 86.6 µs | 5.00 ms | 25.9 µs | 648.8 ms | yes |
| rocksdb | 22.7 µs | 82.4 µs | 5.02 ms | 39.5 µs | 59.3 ms | yes |

Resolved posting→primary rows were identical across engines (2,041) —
the workload is deterministic, so matching counts double as a
cross-engine correctness check.

## Reading the numbers

- **redb leads every axis**: ~1.6× commit throughput, the fastest reopen
  (20× faster than fjall), and the cleanest tail. Its single-file
  B-tree+WAL model pays less bookkeeping per fsync-commit than LSM
  flush/compaction paths, and a single file is operationally attractive
  for checkpoint transfer.
- **fjall ≈ rocksdb on writes** — both hit the same ~5 ms fsync wall;
  fjall's prefix lookups are slightly faster, but its 649 ms reopen is
  the worst of the three (rebuilds in-memory index state on open).
- Crash durability is correct everywhere at sync-commit parity, as
  expected — the interesting axis was reopen cost, which redb wins.

## Caveats

- Docker Desktop's virtiofs puts a ~3–5 ms floor under every fsync;
  absolute numbers will improve on bare metal, but the relative ordering
  is the signal.
- redb is a B-tree store, not an LSM — "many user Indexes sharing a
  bounded number of trees" maps to one file rather than N keyspaces,
  which satisfies the bounded-tree requirement even more tightly. Its
  write txns are serialized with immediate durability — equivalent to
  `SingleWriterTxDatabase` for the read-modify-write contract.
- This measures single-Node storage cost; replication/checkpoint
  transfer cost under the RF3 path still needs the wire-level milestone
  to price honestly.

## Recommendation input

redb is the leading candidate for the Index Engine selection task:
fastest on every measured axis, pure Rust (no C++ toolchain, no
bindgen), ACID multi-table transactions, and a single-file layout that
simplifies checkpoint transfer. Before committing, validate:

- crash-consistency behaviour under the replicated Index path (kill
  mid-checkpoint, mid-catch-up),
- copy-on-write space amplification under sustained churn,
- MVCC reader cost during long-running index scans.
