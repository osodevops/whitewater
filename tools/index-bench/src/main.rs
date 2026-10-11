//! Index-engine reference benchmark: Fjall vs redb vs RocksDB.
//!
//! Runs one deterministic workload against each candidate engine — many
//! logical user Indexes sharing a bounded number of physical trees, the
//! same three-keyspace layout `src/index.rs` uses (primary rows, shared
//! secondary/unique-claim entries, checkpoints). Measures sync-commit
//! throughput and tail latency under mixed upsert/lookup/delete, reopen
//! time and last-write survival after a hard kill, and tail latency while
//! the engine absorbs a compaction-pressure burst.
//!
//!   cargo run --release --manifest-path tools/index-bench/Cargo.toml
//!
//! Tuning: IB_ROWS (default 20000), IB_OPS (10000), IB_INDEXES (32),
//! IB_PAYLOAD (256 bytes), IB_BURST (rows), IB_CHURN_ROUNDS (10),
//! IB_CHURN_KEYS (2000).

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{Duration, Instant};

use finnstream::index::{
    posting_key, posting_prefix, primary_key, unique_claim_key, IndexEntry, IndexId, IndexValue,
    PrimaryRef,
};
use fjall::Readable as _;
use redb::ReadableTable as _;
use uuid::Uuid;

// Deterministic PRNG (xorshift64*) so every engine sees an identical
// logical workload.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }
    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }
}

const PRIMARY: &str = "index_primary";
const ENTRIES: &str = "index_entries";
const CHECKPOINTS: &str = "index_checkpoints";

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

// ---- workload generation -------------------------------------------------

enum Op {
    /// read-modify-write: fetch old row, swap posting set, move checkpoint.
    Upsert {
        pk: Vec<u8>,
        row: Vec<u8>,
        new_postings: Vec<Vec<u8>>,
        old_postings: Vec<Vec<u8>>,
        claim: Vec<u8>,
        cp_key: Vec<u8>,
        cp_val: Vec<u8>,
    },
    /// prefix scan of postings, resolving each back to its primary row.
    Lookup { prefix: Vec<u8>, limit: usize },
    Delete {
        pk: Vec<u8>,
        old_postings: Vec<Vec<u8>>,
        cp_key: Vec<u8>,
        cp_val: Vec<u8>,
    },
}

struct Workload {
    feed_id: Uuid,
    indexes: Vec<IndexId>,
    unique_index: IndexId,
    rng: Rng,
    generation: BTreeMap<u64, u64>,
    payload_len: usize,
}

impl Workload {
    fn new(index_count: usize, payload_len: usize) -> Self {
        let indexes = (0..index_count)
            .map(|i| IndexId(Uuid::from_u128(0x1000 + i as u128)))
            .collect();
        Self {
            feed_id: Uuid::from_u128(0xFEED),
            indexes,
            unique_index: IndexId(Uuid::from_u128(0x9999)),
            rng: Rng(0x9E3779B97F4A7C15),
            generation: BTreeMap::new(),
            payload_len,
        }
    }
    fn reference(&self, row: u64) -> PrimaryRef {
        PrimaryRef {
            feed_id: self.feed_id,
            application_key: format!("order{:010}", row).into_bytes(),
        }
    }
    fn entries_for(&self, row: u64, generation: u64) -> Vec<IndexEntry> {
        let tenant = format!("tenant{:05}", row % 512);
        (0..4)
            .map(|slot| {
                let idx = ((row + generation).wrapping_add(slot as u64) % self.indexes.len() as u64)
                    as usize;
                IndexEntry {
                    id: self.indexes[idx],
                    values: vec![
                        IndexValue::Text(tenant.clone()),
                        IndexValue::I64((row ^ generation) as i64),
                    ],
                    unique: false,
                }
            })
            .collect()
    }
    fn postings(&self, row: u64, generation: u64) -> Vec<Vec<u8>> {
        let reference = self.reference(row);
        self.entries_for(row, generation)
            .iter()
            .map(|entry| posting_key(entry, &reference).unwrap())
            .collect()
    }
    fn claim_for(&self, row: u64) -> Vec<u8> {
        unique_claim_key(&IndexEntry {
            id: self.unique_index,
            values: vec![IndexValue::Bytes(format!("sku{:010}", row).into_bytes())],
            unique: true,
        })
        .unwrap()
    }
    fn row_payload(&self, row: u64, generation: u64) -> Vec<u8> {
        let mut value = Vec::with_capacity(self.payload_len);
        let mut seed = row ^ (generation << 32);
        while value.len() < self.payload_len {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            value.extend_from_slice(&seed.to_le_bytes());
        }
        value.truncate(self.payload_len);
        value
    }
    fn upsert(&mut self, row: u64) -> Op {
        let generation = self.generation.entry(row).or_insert(0);
        *generation += 1;
        let g = *generation;
        let reference = self.reference(row);
        Op::Upsert {
            pk: primary_key(&reference).unwrap(),
            row: self.row_payload(row, g),
            new_postings: self.postings(row, g),
            old_postings: if g > 1 {
                self.postings(row, g - 1)
            } else {
                Vec::new()
            },
            claim: self.claim_for(row),
            cp_key: self.indexes[row as usize % self.indexes.len()]
                .0
                .as_bytes()
                .to_vec(),
            cp_val: format!("cursor-{:016x}", row ^ g).into_bytes(),
        }
    }
    fn delete(&mut self, row: u64) -> Op {
        let g = self.generation.get(&row).copied().unwrap_or(1);
        Op::Delete {
            pk: primary_key(&self.reference(row)).unwrap(),
            old_postings: self.postings(row, g),
            cp_key: self.indexes[row as usize % self.indexes.len()]
                .0
                .as_bytes()
                .to_vec(),
            cp_val: format!("cursor-{:016x}", row).into_bytes(),
        }
    }
    fn lookup(&mut self) -> Op {
        let idx = self.rng.below(self.indexes.len() as u64) as usize;
        let tenant = format!("tenant{:05}", self.rng.below(512));
        Op::Lookup {
            prefix: posting_prefix(self.indexes[idx], &[IndexValue::Text(tenant)]).unwrap(),
            limit: 8,
        }
    }
}

// ---- engine abstraction --------------------------------------------------

trait BenchEngine {
    fn open(dir: &Path) -> Result<Self, String>
    where
        Self: Sized;
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, String>;
    /// One synchronous commit: primary upsert + posting swap + claim +
    /// checkpoint, mirroring `FjallIndexStore::upsert`.
    fn upsert_sync(&mut self, op: &Op) -> Result<(), String>;
    fn delete_sync(&mut self, op: &Op) -> Result<(), String>;
    /// Prefix scan over entries, resolving each to a primary get; returns
    /// resolved row count.
    fn lookup(&mut self, prefix: &[u8], limit: usize) -> Result<usize, String>;
    fn name(&self) -> &'static str;
}

// ---- fjall ----------------------------------------------------------------

struct FjallBench {
    db: fjall::SingleWriterTxDatabase,
    primary: fjall::SingleWriterTxKeyspace,
    entries: fjall::SingleWriterTxKeyspace,
    checkpoints: fjall::SingleWriterTxKeyspace,
}

impl BenchEngine for FjallBench {
    fn open(dir: &Path) -> Result<Self, String> {
        // Same bounds as FjallIndexStore::open: a 64 MiB journal rotation
        // cap plus 16 MiB memtable flush keeps the crash-replay tail
        // bounded rather than proportional to history.
        let db = fjall::SingleWriterTxDatabase::builder(dir)
            .max_journaling_size(64 * 1024 * 1024)
            .open()
            .map_err(|e| e.to_string())?;
        let opts = || fjall::KeyspaceCreateOptions::default().max_memtable_size(16 * 1024 * 1024);
        Ok(Self {
            primary: db.keyspace(PRIMARY, opts).map_err(|e| e.to_string())?,
            entries: db.keyspace(ENTRIES, opts).map_err(|e| e.to_string())?,
            checkpoints: db.keyspace(CHECKPOINTS, opts).map_err(|e| e.to_string())?,
            db,
        })
    }
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        self.db
            .read_tx()
            .get(&self.primary, key)
            .map(|opt| opt.map(|s| s.to_vec()))
            .map_err(|e| e.to_string())
    }
    fn upsert_sync(&mut self, op: &Op) -> Result<(), String> {
        let Op::Upsert {
            pk,
            row,
            new_postings,
            old_postings,
            claim,
            cp_key,
            cp_val,
        } = op
        else {
            unreachable!()
        };
        let mut tx = self
            .db
            .write_tx()
            .durability(Some(fjall::PersistMode::SyncAll));
        tx.get(&self.primary, pk.as_slice())
            .map_err(|e| e.to_string())?;
        for key in old_postings {
            tx.remove(&self.entries, key.as_slice());
        }
        for key in new_postings {
            tx.insert(&self.entries, key.as_slice(), pk.as_slice());
        }
        tx.insert(&self.entries, claim.as_slice(), pk.as_slice());
        tx.insert(&self.primary, pk.as_slice(), row.as_slice());
        tx.insert(&self.checkpoints, cp_key.as_slice(), cp_val.as_slice());
        tx.commit().map_err(|e| e.to_string())
    }
    fn delete_sync(&mut self, op: &Op) -> Result<(), String> {
        let Op::Delete {
            pk,
            old_postings,
            cp_key,
            cp_val,
        } = op
        else {
            unreachable!()
        };
        let mut tx = self
            .db
            .write_tx()
            .durability(Some(fjall::PersistMode::SyncAll));
        for key in old_postings {
            tx.remove(&self.entries, key.as_slice());
        }
        tx.remove(&self.primary, pk.as_slice());
        tx.insert(&self.checkpoints, cp_key.as_slice(), cp_val.as_slice());
        tx.commit().map_err(|e| e.to_string())
    }
    fn lookup(&mut self, prefix: &[u8], limit: usize) -> Result<usize, String> {
        let snap = self.db.read_tx();
        let mut resolved = 0;
        for item in snap.prefix(&self.entries, prefix.to_vec()).take(limit) {
            let (_, pk) = item.into_inner().map_err(|e| e.to_string())?;
            if snap
                .get(&self.primary, &*pk)
                .map_err(|e| e.to_string())?
                .is_none()
            {
                return Err("dangling posting".into());
            }
            resolved += 1;
        }
        Ok(resolved)
    }
    fn name(&self) -> &'static str {
        "fjall"
    }
}

// ---- redb ------------------------------------------------------------------

use redb::{Database, TableDefinition};

const R_PRIMARY: TableDefinition<&[u8], &[u8]> = TableDefinition::new("index_primary");
const R_ENTRIES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("index_entries");
const R_CHECKPOINTS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("index_checkpoints");

struct RedbBench {
    db: Database,
}

impl BenchEngine for RedbBench {
    fn open(dir: &Path) -> Result<Self, String> {
        let db = Database::create(dir.join("index.redb")).map_err(|e| e.to_string())?;
        // Create the three tables up front so read transactions can open them.
        let tx = db.begin_write().map_err(|e| e.to_string())?;
        {
            let _ = tx.open_table(R_PRIMARY).map_err(|e| e.to_string())?;
            let _ = tx.open_table(R_ENTRIES).map_err(|e| e.to_string())?;
            let _ = tx.open_table(R_CHECKPOINTS).map_err(|e| e.to_string())?;
        }
        tx.commit().map_err(|e| e.to_string())?;
        Ok(Self { db })
    }
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        let tx = self.db.begin_read().map_err(|e| e.to_string())?;
        let table = tx.open_table(R_PRIMARY).map_err(|e| e.to_string())?;
        table
            .get(key)
            .map(|opt| opt.map(|g| g.value().to_vec()))
            .map_err(|e| e.to_string())
    }
    fn upsert_sync(&mut self, op: &Op) -> Result<(), String> {
        let Op::Upsert {
            pk,
            row,
            new_postings,
            old_postings,
            claim,
            cp_key,
            cp_val,
        } = op
        else {
            unreachable!()
        };
        let tx = self.db.begin_write().map_err(|e| e.to_string())?;
        {
            let mut primary = tx.open_table(R_PRIMARY).map_err(|e| e.to_string())?;
            let mut entries = tx.open_table(R_ENTRIES).map_err(|e| e.to_string())?;
            let mut checkpoints = tx.open_table(R_CHECKPOINTS).map_err(|e| e.to_string())?;
            primary.get(pk.as_slice()).map_err(|e| e.to_string())?;
            for key in old_postings {
                entries.remove(key.as_slice()).map_err(|e| e.to_string())?;
            }
            for key in new_postings {
                entries
                    .insert(key.as_slice(), pk.as_slice())
                    .map_err(|e| e.to_string())?;
            }
            entries
                .insert(claim.as_slice(), pk.as_slice())
                .map_err(|e| e.to_string())?;
            primary
                .insert(pk.as_slice(), row.as_slice())
                .map_err(|e| e.to_string())?;
            checkpoints
                .insert(cp_key.as_slice(), cp_val.as_slice())
                .map_err(|e| e.to_string())?;
        }
        tx.commit().map_err(|e| e.to_string())
    }
    fn delete_sync(&mut self, op: &Op) -> Result<(), String> {
        let Op::Delete {
            pk,
            old_postings,
            cp_key,
            cp_val,
        } = op
        else {
            unreachable!()
        };
        let tx = self.db.begin_write().map_err(|e| e.to_string())?;
        {
            let mut primary = tx.open_table(R_PRIMARY).map_err(|e| e.to_string())?;
            let mut entries = tx.open_table(R_ENTRIES).map_err(|e| e.to_string())?;
            let mut checkpoints = tx.open_table(R_CHECKPOINTS).map_err(|e| e.to_string())?;
            for key in old_postings {
                entries.remove(key.as_slice()).map_err(|e| e.to_string())?;
            }
            primary.remove(pk.as_slice()).map_err(|e| e.to_string())?;
            checkpoints
                .insert(cp_key.as_slice(), cp_val.as_slice())
                .map_err(|e| e.to_string())?;
        }
        tx.commit().map_err(|e| e.to_string())
    }
    fn lookup(&mut self, prefix: &[u8], limit: usize) -> Result<usize, String> {
        let tx = self.db.begin_read().map_err(|e| e.to_string())?;
        let entries = tx.open_table(R_ENTRIES).map_err(|e| e.to_string())?;
        let primary = tx.open_table(R_PRIMARY).map_err(|e| e.to_string())?;
        let mut resolved = 0;
        for item in entries
            .range(prefix..)
            .map_err(|e| e.to_string())?
            .take(limit)
        {
            let (key, pk) = item.map_err(|e| e.to_string())?;
            if !key.value().starts_with(prefix) {
                break;
            }
            if primary
                .get(pk.value())
                .map_err(|e| e.to_string())?
                .is_none()
            {
                return Err("dangling posting".into());
            }
            resolved += 1;
        }
        Ok(resolved)
    }
    fn name(&self) -> &'static str {
        "redb"
    }
}

// ---- rocksdb ---------------------------------------------------------------

use rocksdb::{Direction, IteratorMode, Options, WriteBatch, WriteOptions, DB};

struct RocksBench {
    db: DB,
}

impl BenchEngine for RocksBench {
    fn open(dir: &Path) -> Result<Self, String> {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        let db = DB::open_cf(&options, dir, [PRIMARY, ENTRIES, CHECKPOINTS])
            .map_err(|e| e.to_string())?;
        Ok(Self { db })
    }
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        let cf = self.db.cf_handle(PRIMARY).unwrap();
        self.db.get_cf(cf, key).map_err(|e| e.to_string())
    }
    fn upsert_sync(&mut self, op: &Op) -> Result<(), String> {
        let Op::Upsert {
            pk,
            row,
            new_postings,
            old_postings,
            claim,
            cp_key,
            cp_val,
        } = op
        else {
            unreachable!()
        };
        let primary = self.db.cf_handle(PRIMARY).unwrap();
        let entries = self.db.cf_handle(ENTRIES).unwrap();
        let checkpoints = self.db.cf_handle(CHECKPOINTS).unwrap();
        self.db.get_cf(primary, pk).map_err(|e| e.to_string())?;
        let mut batch = WriteBatch::default();
        for key in old_postings {
            batch.delete_cf(entries, key);
        }
        for key in new_postings {
            batch.put_cf(entries, key, pk);
        }
        batch.put_cf(entries, claim, pk);
        batch.put_cf(primary, pk, row);
        batch.put_cf(checkpoints, cp_key, cp_val);
        let mut write = WriteOptions::default();
        write.set_sync(true);
        self.db.write_opt(batch, &write).map_err(|e| e.to_string())
    }
    fn delete_sync(&mut self, op: &Op) -> Result<(), String> {
        let Op::Delete {
            pk,
            old_postings,
            cp_key,
            cp_val,
        } = op
        else {
            unreachable!()
        };
        let primary = self.db.cf_handle(PRIMARY).unwrap();
        let entries = self.db.cf_handle(ENTRIES).unwrap();
        let checkpoints = self.db.cf_handle(CHECKPOINTS).unwrap();
        let mut batch = WriteBatch::default();
        for key in old_postings {
            batch.delete_cf(entries, key);
        }
        batch.delete_cf(primary, pk);
        batch.put_cf(checkpoints, cp_key, cp_val);
        let mut write = WriteOptions::default();
        write.set_sync(true);
        self.db.write_opt(batch, &write).map_err(|e| e.to_string())
    }
    fn lookup(&mut self, prefix: &[u8], limit: usize) -> Result<usize, String> {
        let entries = self.db.cf_handle(ENTRIES).unwrap();
        let primary = self.db.cf_handle(PRIMARY).unwrap();
        let mut resolved = 0;
        for item in self
            .db
            .iterator_cf(entries, IteratorMode::From(prefix, Direction::Forward))
            .take(limit)
        {
            let (key, pk) = item.map_err(|e| e.to_string())?;
            if !key.starts_with(prefix) {
                break;
            }
            if self
                .db
                .get_cf(primary, &*pk)
                .map_err(|e| e.to_string())?
                .is_none()
            {
                return Err("dangling posting".into());
            }
            resolved += 1;
        }
        Ok(resolved)
    }
    fn name(&self) -> &'static str {
        "rocksdb"
    }
}

// ---- measurement -----------------------------------------------------------

struct Stats {
    samples: Vec<u64>,
}
impl Stats {
    fn new() -> Self {
        Self {
            samples: Vec::with_capacity(64_000),
        }
    }
    fn record(&mut self, elapsed: Duration) {
        self.samples.push(elapsed.as_nanos() as u64);
    }
    fn percentile(&mut self, q: f64) -> Duration {
        self.samples.sort_unstable();
        let idx = ((self.samples.len() as f64) * q) as usize;
        Duration::from_nanos(*self.samples.get(idx).unwrap_or(&0))
    }
}

struct EngineReport {
    engine: &'static str,
    bulk_ops_per_s: f64,
    bulk_p50: Duration,
    bulk_p99: Duration,
    mixed_ops_per_s: f64,
    upsert_p50: Duration,
    upsert_p99: Duration,
    lookup_p50: Duration,
    lookup_p99: Duration,
    delete_p50: Duration,
    tail_after_burst: Duration,
    reopen_ms: f64,
    reopen_clean_ms: f64,
    churn_p99: Duration,
    db_bytes: u64,
    crash_survived: bool,
    resolved_rows: usize,
}

fn dir(name: &str) -> PathBuf {
    let root = std::env::temp_dir().join("whitewater-index-bench");
    let dir = root.join(name);
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::create_dir_all(&dir);
    dir
}

fn dir_size(path: &Path) -> u64 {
    let mut total = 0u64;
    let mut stack = vec![path.to_path_buf()];
    while let Some(d) = stack.pop() {
        if let Ok(entries) = std::fs::read_dir(d) {
            for entry in entries.flatten() {
                let p = entry.path();
                if p.is_dir() {
                    stack.push(p);
                } else if let Ok(m) = entry.metadata() {
                    total += m.len();
                }
            }
        }
    }
    total
}

fn run_engine<E: BenchEngine>(
    engine_dir: &Path,
    rows: usize,
    ops: usize,
    burst: usize,
) -> Result<EngineReport, String> {
    let mut wl = Workload::new(env_usize("IB_INDEXES", 32), env_usize("IB_PAYLOAD", 256));

    // Phase 1: bulk upsert, all rows new.
    let mut engine = E::open(engine_dir)?;
    let mut bulk = Stats::new();
    let started = Instant::now();
    for row in 0..rows as u64 {
        let op = wl.upsert(row);
        let t = Instant::now();
        engine.upsert_sync(&op)?;
        bulk.record(t.elapsed());
    }
    let bulk_elapsed = started.elapsed();

    // Phase 2: mixed — 70% upsert (half updates), 20% lookup, 10% delete.
    let mut upsert = Stats::new();
    let mut lookup = Stats::new();
    let mut delete = Stats::new();
    let mut resolved_rows = 0;
    let started = Instant::now();
    for _ in 0..ops {
        let roll = wl.rng.below(10);
        if roll < 7 {
            let row = wl.rng.below(rows as u64);
            let op = wl.upsert(row);
            let t = Instant::now();
            engine.upsert_sync(&op)?;
            upsert.record(t.elapsed());
        } else if roll < 9 {
            let op = wl.lookup();
            let Op::Lookup { prefix, limit } = &op else {
                unreachable!()
            };
            let t = Instant::now();
            resolved_rows += engine.lookup(prefix, *limit)?;
            lookup.record(t.elapsed());
        } else {
            let row = wl.rng.below(rows as u64);
            let op = wl.delete(row);
            let t = Instant::now();
            engine.delete_sync(&op)?;
            delete.record(t.elapsed());
        }
    }
    let mixed_elapsed = started.elapsed();

    // Phase 3: compaction-pressure burst (pure upserts, no reads), then a
    // single small lookup to sample the tail the engine leaves behind.
    for _ in 0..burst {
        let row = wl.rng.below(rows as u64);
        let op = wl.upsert(row);
        engine.upsert_sync(&op)?;
    }
    let op = wl.lookup();
    let Op::Lookup { prefix, limit } = &op else {
        unreachable!()
    };
    let t = Instant::now();
    engine.lookup(prefix, *limit)?;
    let tail_after_burst = t.elapsed();

    // Phase 4: churn — repeated in-place updates over a fixed key set.
    // Each upsert bumps the row generation so old postings are removed
    // and new ones written: the workload that exposes copy-on-write
    // amplification (B-tree) vs compaction behaviour (LSM) under
    // sustained index churn.
    let mut churn = Stats::new();
    for _ in 0..env_usize("IB_CHURN_ROUNDS", 10) {
        for i in 0..env_usize("IB_CHURN_KEYS", 2_000) as u64 {
            let op = wl.upsert(i);
            let t = Instant::now();
            engine.upsert_sync(&op)?;
            churn.record(t.elapsed());
        }
    }
    let db_bytes = dir_size(engine_dir);

    // Phase 5: clean reopen — bounded replay expected even without a
    // crash, since the journal/memtable bounds cap residual replay.
    drop(engine);
    let t = Instant::now();
    let clean = E::open(engine_dir)?;
    let reopen_clean_ms = t.elapsed().as_secs_f64() * 1000.0;
    drop(clean);

    // Phase 6: hard-kill durability — child writes then _exit()s without
    // destructors; parent reopens and checks the last committed row.
    let child_status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--crash-child",
            std::any::type_name::<E>(),
            engine_dir.to_str().unwrap(),
        ])
        .status()
        .map_err(|e| e.to_string())?;
    if child_status.code() != Some(42) {
        return Err(format!("crash child exited with {child_status}"));
    }
    let t = Instant::now();
    let mut engine = E::open(engine_dir)?;
    let reopen_ms = t.elapsed().as_secs_f64() * 1000.0;
    let probe = primary_key(&PrimaryRef {
        feed_id: wl.feed_id,
        application_key: b"__crash_probe__".to_vec(),
    })
    .unwrap();
    let crash_survived = engine.get(&probe)?.is_some();

    Ok(EngineReport {
        engine: engine.name(),
        bulk_ops_per_s: rows as f64 / bulk_elapsed.as_secs_f64(),
        bulk_p50: bulk.percentile(0.50),
        bulk_p99: bulk.percentile(0.99),
        mixed_ops_per_s: ops as f64 / mixed_elapsed.as_secs_f64(),
        upsert_p50: upsert.percentile(0.50),
        upsert_p99: upsert.percentile(0.99),
        lookup_p50: lookup.percentile(0.50),
        lookup_p99: lookup.percentile(0.99),
        delete_p50: delete.percentile(0.50),
        tail_after_burst,
        reopen_ms,
        reopen_clean_ms,
        churn_p99: churn.percentile(0.99),
        db_bytes,
        crash_survived,
        resolved_rows,
    })
}

/// Crash child: write upserts with sync commits, then hard-exit so the
/// parent reopens through WAL recovery, not a clean close.
fn crash_child_for<E: BenchEngine>(dir: &Path) -> ExitCode {
    let mut engine = E::open(dir).expect("crash child open");
    let mut wl = Workload::new(8, 256);
    for _ in 0..500 {
        let mut op = wl.upsert(0);
        if let Op::Upsert { pk, .. } = &mut op {
            *pk = primary_key(&PrimaryRef {
                feed_id: wl.feed_id,
                application_key: b"__crash_probe__".to_vec(),
            })
            .unwrap();
        }
        engine.upsert_sync(&op).expect("crash child upsert");
    }
    std::process::exit(42);
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    if args.len() >= 3 && args[1] == "--crash-child" {
        let dir = PathBuf::from(&args[3]);
        return match args[2].as_str() {
            s if s.ends_with("FjallBench") => crash_child_for::<FjallBench>(&dir),
            s if s.ends_with("RedbBench") => crash_child_for::<RedbBench>(&dir),
            s if s.ends_with("RocksBench") => crash_child_for::<RocksBench>(&dir),
            other => {
                eprintln!("unknown engine {other}");
                ExitCode::from(2)
            }
        };
    }

    let rows = env_usize("IB_ROWS", 20_000);
    let ops = env_usize("IB_OPS", 10_000);
    let burst = env_usize("IB_BURST", rows);
    println!(
        "index-engine benchmark: rows={rows} mixed_ops={ops} indexes={} payload={}B burst={burst}",
        env_usize("IB_INDEXES", 32),
        env_usize("IB_PAYLOAD", 256)
    );

    let engines: Vec<String> = args
        .get(1..)
        .map(|rest| rest.to_vec())
        .filter(|rest| !rest.is_empty())
        .unwrap_or_else(|| vec!["fjall".into(), "redb".into(), "rocksdb".into()]);

    let mut reports = Vec::new();
    for engine in &engines {
        let d = dir(engine);
        let result = match engine.as_str() {
            "fjall" => run_engine::<FjallBench>(&d, rows, ops, burst),
            "redb" => run_engine::<RedbBench>(&d, rows, ops, burst),
            "rocksdb" => run_engine::<RocksBench>(&d, rows, ops, burst),
            other => Err(format!("unknown engine {other}")),
        };
        match result {
            Ok(report) => reports.push(report),
            Err(error) => eprintln!("{engine}: FAILED: {error}"),
        }
    }

    let mut out = String::new();
    writeln!(
        out,
        "\n{:<9} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10}",
        "engine", "bulk op/s", "bulk p50", "bulk p99", "mix op/s", "up p50", "up p99"
    )
    .unwrap();
    for r in &reports {
        writeln!(
            out,
            "{:<9} {:>10.0} {:>9?} {:>9?} {:>10.0} {:>9?} {:>9?}",
            r.engine,
            r.bulk_ops_per_s,
            r.bulk_p50,
            r.bulk_p99,
            r.mixed_ops_per_s,
            r.upsert_p50,
            r.upsert_p99
        )
        .unwrap();
    }
    writeln!(
        out,
        "\n{:<9} {:>10} {:>10} {:>10} {:>14} {:>10} {:>12}",
        "engine", "lk p50", "lk p99", "del p50", "tail@burst", "survived", "resolved"
    )
    .unwrap();
    for r in &reports {
        writeln!(
            out,
            "{:<9} {:>9?} {:>9?} {:>9?} {:>13?} {:>10} {:>12}",
            r.engine,
            r.lookup_p50,
            r.lookup_p99,
            r.delete_p50,
            r.tail_after_burst,
            r.crash_survived,
            r.resolved_rows
        )
        .unwrap();
    }
    writeln!(
        out,
        "\n{:<9} {:>11} {:>12} {:>10} {:>10}",
        "engine", "reopen kill", "reopen clean", "churn p99", "db MB"
    )
    .unwrap();
    for r in &reports {
        writeln!(
            out,
            "{:<9} {:>11.1} {:>12.1} {:>9?} {:>10.1}",
            r.engine,
            r.reopen_ms,
            r.reopen_clean_ms,
            r.churn_p99,
            r.db_bytes as f64 / (1024.0 * 1024.0)
        )
        .unwrap();
    }
    println!("{out}");
    ExitCode::SUCCESS
}
