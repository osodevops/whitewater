# Parallelizing the live acceptance suite — implementation plan

Handoff document for an implementation session. Goal: cut wall-clock time of
`python scripts/run-acceptance.py` (and the `live-acceptance` CI job) by running
independent scripts concurrently, without weakening coverage or breaking the
fixture invariants the suite relies on.

## Current architecture (verified facts)

`scripts/run-acceptance.py` is a sequential runner. It:

1. Refuses to start if `BASE_PORT..BASE_PORT+2` are bound (`ports_free`, line ~122).
2. `docker compose -p whitewater-verification -f compose.verify.yml up -d --build`,
   then `wait_healthy()` (health + control-plane convergence, lines ~131-184).
3. Runs every `scripts/test-*.py` serially with `subprocess.run` (lines ~233-266),
   re-gating `wait_healthy(timeout_s=120)` before each script (a predecessor may
   have restarted fixture Nodes mid-run).
4. On failure: `collect_diagnostics(script, env)` → `acceptance-logs/`.
5. `finally`: `sweep(env)` downs every known test project (`compose_file_projects`).
6. `test-m111-fault-suite.py` always runs **last** — it restarts the whole
   Riverbed and re-runs three suites internally (line ~44).

Two environment hooks already exist and matter:

- `WHITEWATER_TEST_BASE_PORT` (default 7371): every shared-fixture script
  derives its endpoints from it — `f"http://localhost:{BASE_PORT + offset}"`.
- `COMPOSE_FILE` (set to `compose.verify.yml` by the runner): fixture-mutating
  scripts issue **bare `docker compose`** calls with no `-f`, so they target
  whatever `COMPOSE_FILE` says. `scripts/chaos_driver.py` also honours it.

## Resource conflict map

Compose file → project name → host ports:

| Compose file | Project | Host ports | Used by |
|---|---|---|---|
| compose.verify.yml | whitewater-verification | 7371-7373 HTTP | all shared-fixture scripts (below) |
| compose.pipe.yml | whitewater-pipe | 7471-7473 | test-enrich-pipe, test-pipe-forward |
| compose.readers.yml | whitewater-readers | 7571-7573 | test-many-readers |
| compose.hotkeys.yml | whitewater-hotkeys | 7971-7973 | test-hot-keys |
| compose.mtls.yml | whitewater-mtls-test (`-p whitewater-mtls` override) | 7171-7174 HTTP + 7571-7574 TLS + cert env 7271 | test-mtls-plane |
| compose.storage-join.yml | whitewater-storage-join | 7171-7174 HTTP + 7571-7574 TLS + storage 7175 + cert env 7271 | test-storage-node-join, test-window-pipe-4n |
| compose.m4-move.yml | whitewater-m4-move-test | 7171-7175 (4 nodes) | test-m4-cross-node-read, test-m4-follower-move, test-storage-drain |
| compose.m4-owner.yml | whitewater-m4-owner-test | 7271-7273 | test-m4-owner-move |

**Shared-fixture scripts** (endpoints from `WHITEWATER_TEST_BASE_PORT`, no own
compose file). Split into *read-only* and *fixture-mutating*:

Read-only over HTTP/CLI (safe to share a fixture concurrently, subject to
load-induced timing sensitivity):
test-concurrent-append, test-m17-topology-free-append, test-m2-writer-session,
test-m3-reader-session, test-m4-auto-split, test-m4-live-split,
test-m4-split-merge-cycle, test-sdk-conformance, test-sdk-rust,
test-wcl-cli-write.

Fixture-mutating (stop/start/restart Nodes — must own the fixture exclusively
while running):
- test-m4-split-rollback — raw `docker stop/start` on `WHITEWATER_TEST_NODE3`
  (default container `whitewater-verification-node3-1`, env-overridable)
- test-m19-owner-recovery — `docker compose stop/start` via COMPOSE_FILE
- test-m7-subscription-member — `docker compose stop/start` via COMPOSE_FILE
- test-m110-replica-catchup — `docker compose stop/start` via COMPOSE_FILE
- test-m111-fault-suite — `docker compose restart` (all nodes) + inner reruns

**Port-collision groups among isolated projects** — these cannot overlap today:
- 7171-7175 + 7571-7574 + 7271 zone: mtls, storage-join/window-pipe-4n,
  all three m4-move scripts, m4-owner (7271-7273 collides with the cert port),
  many-readers (7571-7573 collides with the TLS range). One serial group of 8.
- pipe pair: serialize with each other (same project), free of everything else.
- hotkeys: completely free.
- verify lane: free of everything else.

## Plan — two phases

### Phase 1: lane parallelism with zero port work (cheap, safe)

Run four lanes concurrently; serialise inside each:

- **Lane A — verify fixture** (7371-7373): all shared-fixture scripts in
  today's order, fault suite last. This lane is the long pole.
- **Lane B — pipe** (7471-7473): test-enrich-pipe → test-pipe-forward.
- **Lane C — hotkeys** (7971-7973): test-hot-keys.
- **Lane D — isolated zone** (7171-7175/7271/7571-7574): mtls-plane,
  storage-node-join, window-pipe-4n, m4-cross-node-read, m4-follower-move,
  storage-drain, m4-owner-move, many-readers — in any order, serial.

Implementation notes:

- Spawn lanes with `concurrent.futures.ThreadPoolExecutor` or `asyncio`; each
  lane is just a sequential loop reusing the existing `subprocess.run` block.
- Lane A still needs the runner-owned `compose.verify.yml up -d --build` +
  `wait_healthy()` before it starts; other lanes self-provision inside their
  scripts (`down -v` → `up -d --build` per script — unchanged).
- Keep the per-script `wait_healthy(120)` re-gate on lane A only.
- `sweep`/`collect_diagnostics`/`preflight` stay global and run in `finally` —
  already project-scoped per file, no change needed.
- Results merge into the same `results` dict; keep the summary format.
- Expected saving: `sum → max(A,B,C,D)`. If lane A dominates (it will), the
  win is limited — phase 2 is where the real time goes.

### Phase 2: multiple verify lanes (the real win)

The shared-fixture scripts are the bulk of the suite. `WHITEWATER_TEST_BASE_PORT`
is already honoured everywhere, so N verify riverbeds can run on disjoint base
ports — **but** three things block it today:

1. **Project name is hardcoded** (`name: whitewater-verification` in
   compose.verify.yml, line 1) → identical container names across lanes, and
   `test-m4-split-rollback`'s `NODE3` default resolves per name. Parametrize:
   `name: whitewater-verification${WHITEWATER_VERIFY_LANE:-}` — empty default
   keeps today's names exactly; lane B sets `WHITEWATER_VERIFY_LANE=-b` giving
   `whitewater-verification-b-node3-1`. Mutating scripts that take container
   names already have env overrides (`WHITEWATER_TEST_NODE3`); lane B passes
   the `-b`-suffixed name.

2. **Subnet collides** — `compose.verify.yml` line ~66:
   `${WHITEWATER_VERIFY_SUBNET:-10.250.247.0/24}`. Assign each lane its own:
   lane B → `10.250.248.0/24`, lane C → `10.250.249.0/24`. (AGENTS.md already
   requires this env for CI.)

3. **Concurrent `up --build` races** on the same Dockerfile/tag. Pre-build
   once before lanes start (`docker compose -f compose.verify.yml build`), then
   lanes `up -d` without `--build`. The isolated projects still self-build;
   `docker build` layer-caches make concurrent builds correct but slower —
   acceptable, or serialize a build phase upfront for every compose file.

Lane split suggestion (weight by measured per-script times from CI logs —
each `--- script: ok (Ns)` line; rebalance freely):

- **Lane A** (7371, `WHITEWATER_VERIFY_LANE` unset — keeps names): mutating +
  slow scripts. e.g. m110-replica-catchup, m4-split-rollback, m19-owner-recovery,
  m7-subscription-member, then fault suite last.
- **Lane B** (7871, `-b`): read-only group A — concurrent-append, m17, m2, m3,
  sdk-conformance.
- **Lane C** (7891, `-c`): read-only group B — m4-auto-split, m4-live-split,
  m4-split-merge-cycle, sdk-rust, wcl-cli-write.

Verify ports chosen (7871/7891) are free of every mapped range; check again
before coding. Mutating scripts stay serialized *within* their lane but each
lane's fixtures are fully independent — a `stop` on lane A cannot disturb
lane B's nodes.

Runner changes:

- `ports_free()` → per-lane check.
- `wait_healthy()`/`wait_control_plane()` → take the lane's endpoints/subnet env.
- Lanes each `down -v --remove-orphans && up -d && wait_healthy` their own
  `compose.verify.yml` instance (project name + ports + subnet from env).
- Lane count env: `WHITEWATER_ACCEPTANCE_LANES` (default 1 = today's exact
  behaviour; CI sets 2-3 — hosted ubuntu-latest is ~4 vCPU/16 GB; measure
  before going higher since node CPU during builds is the real constraint).

### Phase 3 (optional): deconflict the isolated zone

If lane D also becomes critical-path, parametrize the hardcoded port blocks
in compose.m4-move/storage-join/mtls/readers (each compose file gets its own
env-substituted base, e.g. `${WW_M4_PORT_BASE:-7171}`) so m4-move and
storage-join scripts can overlap. Larger blast radius — only if measurements
say the zone dominates.

## Invariants that must not break

- A script's failure must produce `acceptance-logs/<script>-<project>-*.log`
  plus host `df`/`docker system df` (already per-label; parallel-safe).
- `sweep` must down every project from every lane (extend
  `compose_file_projects` or sweep the lane-specific project names too —
  `whitewater-verification-b` will not appear in the file map!).
- Fault suite stays last on its lane — and must not run concurrently with
  *anything* on its own riverbed.
- `wait_control_plane` convergence gating stays per-script on each lane.
- Never touch the development riverbed (`compose.yml`/`compose.cluster.yml`,
  ports 7071-7073) — the sweep exclusion list is unchanged.
- `chaos_driver.py`/`WHITEWATER_CHAOS_CMD` already honours `COMPOSE_FILE` —
  works per-lane once env-propagated.
- The `whitewater-certgen-target` named volume is intentionally shared across
  scripts (build cache) — do not lane-duplicate it.

## Verification before merging

1. `python -m unittest discover -s tests -p "test_*acceptance.py"` — extend
   `AcceptanceDiagnosticTests` to cover lane scheduling (mock subprocess;
   assert lane envs carry distinct base ports/subnets and fault suite ordering).
2. `python scripts/run-acceptance.py` with `WHITEWATER_ACCEPTANCE_LANES=1`
   must reproduce today's behaviour exactly (regression gate).
3. Full parallel run locally with lanes=3 on a clean Docker host; then on CI.
   Compare wall-clock and confirm no port/subnet/name collisions in the sweep
   step's `docker ps`/`docker volume ls` residue check (verify.yml already
   asserts a clean host at the end).
4. Report measured per-script times so lane weights can be rebalanced.

## Rollback

Everything sits behind `WHITEWATER_ACCEPTANCE_LANES` (default 1 = current
serial behaviour). The CI workflow opts in via env; reverting the env var
restores serial execution without a code change.
