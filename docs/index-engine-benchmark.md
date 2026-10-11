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

Phases (defaults: `IB_ROWS=20000`, `IB_OPS=10000`, `IB_BURST=20000`,
`IB_CHURN_ROUNDS=10`, `IB_CHURN_KEYS=2000`):

1. **Bulk upsert** — 20,000 new primary rows; each commit is a
   read-modify-write (get primary, swap 4 posting entries across 32
   logical Indexes, one unique claim, one checkpoint write).
2. **Mixed** — 10,000 ops at 70% upsert (with stale-entry removal), 20%
   bounded prefix lookups resolving each posting back to its primary row,
   10% deletes.
3. **Compaction tail** — 20,000-op burst with no reads, then one lookup
   samples the tail latency the engine left behind.
4. **Churn** — 10 rounds x 2,000 in-place updates over a fixed key set;
   every commit removes the previous generation's postings and inserts
   new ones. Measures commit-p99 drift and on-disk growth under
   sustained index churn — the copy-on-write vs LSM-compaction axis.
5. **Clean reopen** — drop and re-open measures recovery cost without a
   crash (bounded replay expected under the production journal caps).
6. **Hard kill** — a child process writes 500 sync commits then
   `exit()`s without destructors; the parent measures reopen (WAL
   recovery) time and verifies the last committed row survived.

## Results (Docker Desktop, container-local ext4, release build)

Final run — fjall with the production bounds (64 MiB journal rotation,
16 MiB memtable flush):

| engine | bulk op/s | commit p50 | commit p99 | mixed op/s | upsert p99 |
|---|---|---|---|---|---|
| redb | **193** | **3.17 ms** | 27.10 ms | 165 | 91.4 ms |
| fjall | 180 | **5.01 ms** | **17.82 ms** | **229** | **17.5 ms** |
| rocksdb | 84 | 10.77 ms | 59.69 ms | 89 | 116.2 ms |

| engine | lookup p50 | lookup p99 | delete p50 | tail@burst | survived |
|---|---|---|---|---|---|
| redb | **27.4 µs** | **133.8 µs** | 4.46 ms | **28.6 µs** | yes |
| fjall | 40.0 µs | 160.0 µs | 4.96 ms | 136.6 µs | yes |
| rocksdb | 37.5 µs | 156.3 µs | 10.91 ms | 43.8 µs | yes |

| engine | reopen after kill | reopen clean | churn p99 | on-disk size |
|---|---|---|---|---|
| redb | **53.6 ms** | **6.5 ms** | **18.2 ms** | **48.7 MB** |
| fjall | 251.7 ms | 305.5 ms | 30.5 ms | 101.8 MB |
| rocksdb | 105.0 ms | 1,867.0 ms | 86.1 ms | 70.7 MB |

Resolved posting→primary rows were identical across engines (2,585) —
the workload is deterministic, so matching counts double as a
cross-engine correctness check. All three survived the hard kill with
the last committed row intact.

The earlier unbounded run (pre-selection, fjall defaults) for
comparison: redb 253 op/s / 3.07 ms p50 / 33.6 ms reopen; fjall
185 op/s / 5.00 ms p50 / 648.8 ms reopen; rocksdb 179 op/s / 5.03 ms
p50 / 59.3 ms reopen.

## Reading the numbers

- **Bounded replay works**: the 64 MiB journal cap plus 16 MiB memtable
  flush cut fjall's post-kill reopen from 649 ms to 252 ms — recovery
  cost is now proportional to the bounded tail, not total history. redb
  still reopens fastest (its single-file B-tree+WAL pays less replay
  bookkeeping), but fjall is inside an operationally acceptable bound.
- **Churn did not produce the feared cliff**: redb's file stayed smaller
  (48.7 MB vs fjall's 101.8 MB) at this scale — 20k churn commits are
  still comfortably inside redb's free-page and CoW headroom, and
  fjall's larger footprint reflects unflushed journal plus not-yet-
  compacted churn garbage. redb's churn p99 (18.2 ms) stayed ahead of
  fjall (30.5 ms) and well ahead of rocksdb (86.1 ms).
- **rocksdb collapsed under sustained load** in this environment
  (84 op/s bulk, 1.87 s clean reopen, 116 ms upsert p99) — background
  compaction competing with the virtiofs fsync floor. It remains the
  control group only.
- **redb leads most raw numbers** on this workload — commit p50 (3.2 ms
  vs 5.0 ms), reopen cost, and tail latency — while fjall's p99 commit
  tail and mixed-phase throughput are tighter. Neither ordering is
  decisive at the ~200 ms quorum-commit path this engine sits under.
- Crash durability is correct everywhere at sync-commit parity, as
  expected.

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

## Decision

**Fjall is the selected Index Engine.** The selection weighs the
benchmark honestly but does not follow its headline ordering, because
the measured term is not the binding one:

- Local commit latency is ~3-5 ms while quorum append commit is ~200
  ms — engine commit time is roughly 1% of the user-visible path, so
  redb's commit-latency lead does not decide the architecture.
- The Index workload is small-value, write-dominated churn (every
  logical update becomes posting inserts plus stale-entry deletes):
  the access pattern LSM engines are built for. redb is a
  copy-on-write B-tree whose file growth and deferred free-page
  reclamation under exactly this churn remained unproven, while
  Fjall's compaction behaviour is the understood cost model.
- Fjall is the incumbent across all internal durable state —
  subscription progress journals, effect journals, and the Index
  prototype — so one engine family means one tuning surface, one
  checkpoint format, and one failure-mode playbook.
- Both are pure Rust; RocksDB was the control group and its C++
  toolchain requirement already disqualified it for the normal crate
  build.

redb remains a documented fallback behind the `IndexStore` trait
(`src/index.rs`): the engine is an implementation detail of replica
code, so evidence that reverses the workload-shape assumption can
re-open the decision without rewriting consumers.

### Follow-up evidence (bounded-replay configuration)

`FjallIndexStore` opens with a 64 MiB journal rotation cap and
16 MiB memtable flush (`JOURNAL_REPLAY_BUDGET_BYTES` /
`MEMTABLE_FLUSH_BYTES` in `src/index.rs`), bounding crash-replay tail
length — the same configuration the benchmark's fjall engine uses, so
the measured 252 ms post-kill reopen is the production path's honest
number, not a tuned-for-bench artifact.

Remaining honest gaps, kept as fallback-evaluation notes rather than
selection blockers: redb's MVCC reader cost during hour-long scans and
its behaviour when killed mid-checkpoint were not measured; neither was
a multi-GB churn run where CoW pressure actually bites. If replicated-
path evidence ever favours redb, the `IndexStore` trait is the seam.

