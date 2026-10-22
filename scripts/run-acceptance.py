"""Run the live acceptance suite against an isolated disposable Riverbed.

The runner owns the shared verification topology (`compose.verify.yml`,
project `whitewater-verification`, ports 7371-7373) like a Testcontainers
fixture: it is created fresh for the run and always removed in `finally`,
containers and volumes included. Scenario scripts that declare their own
Compose file (M4 move/owner, mTLS, pipe, readers, hot-keys, storage
join/drain) keep their dedicated projects and tear themselves down; the
runner still sweeps every known test project at the end so a hard-killed
script cannot leak stopped containers or volumes onto the host.

Run from the repository root:

    python scripts/run-acceptance.py                     # every scripts/test-*.py, serial
    python scripts/run-acceptance.py test-m2-writer-session.py ...
    python scripts/run-acceptance.py --lane verify-reads-a   # one lane only
    python scripts/run-acceptance.py --lanes 3               # all lanes, concurrent

Lanes are the suite's parallel decomposition (see
docs/parallel-acceptance-testing.md). `--lane`/`WHITEWATER_ACCEPTANCE_LANE`
selects exactly one lane — CI runs each lane as a matrix leg on its own VM.
`--lanes`/`WHITEWATER_ACCEPTANCE_LANES` (values >1) runs every lane
concurrently on one Docker host: verify lanes past the first get their own
base port (`7871 + 20*i`), Compose project suffix (`whitewater-verification-a`,
`-b`, ...) through `COMPOSE_PROJECT_NAME`, and `/24` subnet derived from
`WHITEWATER_VERIFY_SUBNET`, so fixtures never collide. With no selector the
runner is serial and byte-for-byte compatible with the legacy suite.

`WHITEWATER_VERIFY_SUBNET` is honoured through `compose.verify.yml`; set it
to an unused subnet when Docker's default pools are exhausted. The runner
refuses to start when the verification ports are already bound, so it can
never disrupt the development Riverbed on 7071-7073.
"""

import concurrent.futures
import ipaddress
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import time
import urllib.request
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
COMPOSE_FILE = "compose.verify.yml"
PROJECT = "whitewater-verification"
BASE_PORT = int(os.environ.get("WHITEWATER_TEST_BASE_PORT", "7371"))
SUBNET = os.environ.get("WHITEWATER_VERIFY_SUBNET", "10.250.247.0/24")
NODES = [f"http://localhost:{BASE_PORT + offset}" for offset in range(3)]
HEALTH_TIMEOUT_S = int(os.environ.get("WHITEWATER_ACCEPTANCE_HEALTH_TIMEOUT_S", "600"))
SCRIPT_TIMEOUT_S = int(os.environ.get("WHITEWATER_ACCEPTANCE_SCRIPT_TIMEOUT_S", "1800"))

# The fault suite re-runs three suites internally and restarts the whole
# Riverbed; keep it last on its lane so its disruption cannot degrade
# earlier evidence.
FAULT_SUITE = "test-m111-fault-suite.py"

# Shared-fixture scripts split into fixture-mutating (pinned to the primary
# lane so they keep the default project/container names their docker
# stop/start targets derive from) and read-only (chunked across the
# remaining verify lanes).
VERIFY_MUTATING = (
    "test-m110-replica-catchup.py",
    "test-m19-owner-recovery.py",
    "test-m4-split-rollback.py",
    "test-m7-subscription-member.py",
)
VERIFY_READS = (
    "test-concurrent-append.py",
    "test-m17-topology-free-append.py",
    "test-m2-writer-session.py",
    "test-m3-reader-session.py",
    "test-sdk-conformance.py",
    "test-m4-auto-split.py",
    "test-m4-live-split.py",
    "test-m4-split-merge-cycle.py",
    "test-m4-large-history.py",
    "test-sdk-rust.py",
    "test-wcl-cli-write.py",
)

# Scripts whose dedicated Compose projects share the hardcoded 7171-7175 /
# 7271-7273 / 7571-7575 port blocks: they must serialize on one lane.
ISOLATED_ZONE = (
    "test-mtls-plane.py",
    "test-storage-node-join.py",
    "test-window-pipe-4n.py",
    "test-m4-cross-node-read.py",
    "test-m4-follower-move.py",
    "test-storage-drain.py",
    "test-m4-owner-move.py",
    "test-many-readers.py",
)
# All three pipe scripts drive the same `whitewater-pipe` project — serial.
PIPE = ("test-enrich-pipe.py", "test-pipe-forward.py", "test-window-pipe.py")
HOTKEYS = ("test-hot-keys.py",)
# The disk-full fault scenario owns the 7671-7673 port block and its
# `whitewater-disk-full` project — disjoint from every other lane.
DISK_FULL = ("test-disk-full.py",)

# Verify lanes past the first publish on their own port base and subnet so a
# single Docker host can run several fixtures; these ranges are disjoint
# from every hardcoded compose.*.yml port block.
EXTRA_VERIFY_BASE_PORT = 7871
VERIFY_LANE_PORT_STRIDE = 20
# The canonical decomposition the CI matrix and `--lane` selector use: one
# mutating lane plus two read lanes.
DEFAULT_VERIFY_LANES = 3


@dataclass(frozen=True)
class Lane:
    """A serial queue of scripts. `fixture` lanes own a private
    compose.verify.yml Riverbed; other lanes' scripts provision their own
    dedicated Compose projects inside the script."""

    name: str
    scripts: tuple[str, ...]
    fixture: bool = False
    base_port: int = BASE_PORT
    project_suffix: str = ""
    # Lane i runs on the i-th /24 after SUBNET; the shift is only computed
    # when a lane consumes it so a malformed base subnet cannot break lanes
    # that never touch compose.verify.yml.
    subnet_index: int = 0

    @property
    def project(self) -> str:
        return f"{PROJECT}{self.project_suffix}"

    @property
    def subnet(self) -> str:
        return SUBNET if self.subnet_index == 0 else shifted_subnet(SUBNET, self.subnet_index)

    @property
    def nodes(self) -> list[str]:
        return [f"http://localhost:{self.base_port + offset}" for offset in range(3)]

    def env(self, base: dict) -> dict:
        """Per-lane child env: the base port steers every shared-fixture
        script's endpoint derivation, the WHITEWATER_VERIFY_* values steer
        compose.verify.yml, and COMPOSE_PROJECT_NAME retargets the bare
        `docker compose` calls mutating scripts make."""
        env = dict(base)
        env["WHITEWATER_TEST_BASE_PORT"] = str(self.base_port)
        if not self.fixture:
            return env
        env["WHITEWATER_VERIFY_PORT_1"] = str(self.base_port)
        env["WHITEWATER_VERIFY_PORT_2"] = str(self.base_port + 1)
        env["WHITEWATER_VERIFY_PORT_3"] = str(self.base_port + 2)
        env["WHITEWATER_VERIFY_SUBNET"] = self.subnet
        if self.project_suffix:
            env["COMPOSE_PROJECT_NAME"] = self.project
            env["WHITEWATER_TEST_NODE3"] = f"{self.project}-node3-1"
        return env


def shifted_subnet(subnet: str, index: int) -> str:
    """The lane subnet `index` blocks after `subnet` (index 1 → next /24)."""
    network = ipaddress.ip_network(subnet)
    if network.version != 4 or network.prefixlen != 24:
        raise ValueError(
            "WHITEWATER_VERIFY_SUBNET must be an IPv4 /24 so per-lane "
            f"subnets can be derived, got {subnet!r}"
        )
    try:
        address = ipaddress.ip_address(int(network.network_address) + index * 256)
        return str(ipaddress.ip_network(f"{address}/24"))
    except ValueError as error:
        raise ValueError(
            f"WHITEWATER_VERIFY_SUBNET {subnet!r} has no room for lane subnet {index}"
        ) from error


def _chunks(scripts: tuple[str, ...], count: int) -> list[tuple[str, ...]]:
    """Split `scripts` into `count` contiguous, near-equal chunks."""
    count = max(1, min(count, len(scripts)))
    per, extra = divmod(len(scripts), count)
    out, start = [], 0
    for _ in range(count):
        stop = start + per + (1 if extra > 0 else 0)
        extra -= 1 if extra > 0 else 0
        out.append(scripts[start:stop])
        start = stop
    return out


def lane_catalog(verify_count: int = DEFAULT_VERIFY_LANES) -> list[Lane]:
    """The full parallel decomposition: `verify_count` verify-fixture lanes
    (mutating scripts plus the fault suite on the first, read-only scripts
    chunked across the rest) followed by the fixed isolated-project lanes."""
    lanes = [
        Lane(
            "verify-mutating",
            VERIFY_MUTATING + (FAULT_SUITE,),
            fixture=True,
        )
    ]
    for index, chunk in enumerate(_chunks(VERIFY_READS, verify_count - 1), start=1):
        if not chunk:
            continue
        lanes.append(
            Lane(
                f"verify-reads-{chr(ord('a') + index - 1)}",
                chunk,
                fixture=True,
                base_port=EXTRA_VERIFY_BASE_PORT + VERIFY_LANE_PORT_STRIDE * (index - 1),
                project_suffix=f"-{chr(ord('a') + index - 1)}",
                subnet_index=index,
            )
        )
    return lanes + [
        Lane("isolated-zone", ISOLATED_ZONE),
        Lane("pipe", PIPE),
        Lane("hotkeys", HOTKEYS),
        Lane("disk-full", DISK_FULL),
    ]


def parse_invocation(argv: list[str], environ) -> tuple[list[Lane] | None, list[str]]:
    """Resolve the run mode into (lanes, positional args). `None` lanes is
    the serial legacy mode; flags override env and env overrides nothing
    (flags win)."""
    lane_name = None
    lane_count = None
    positionals = []
    index = 0
    while index < len(argv):
        arg = argv[index]
        if arg == "--lane" or arg.startswith("--lane="):
            lane_name = arg.split("=", 1)[1] if "=" in arg else (argv[index + 1] if index + 1 < len(argv) else "")
            index += 1 if "=" in arg else 2
            if not lane_name:
                raise ValueError("--lane requires a lane name")
        elif arg == "--lanes" or arg.startswith("--lanes="):
            lane_count = arg.split("=", 1)[1] if "=" in arg else (argv[index + 1] if index + 1 < len(argv) else "")
            index += 1 if "=" in arg else 2
            if not lane_count:
                raise ValueError("--lanes requires a lane count")
        else:
            positionals.append(arg)
            index += 1
    lane_name = lane_name or environ.get("WHITEWATER_ACCEPTANCE_LANE")
    lane_count = lane_count or environ.get("WHITEWATER_ACCEPTANCE_LANES")
    if lane_name:
        if positionals or lane_count:
            raise ValueError("--lane cannot be combined with script names or --lanes")
        known = lane_catalog()
        lane = next((item for item in known if item.name == lane_name), None)
        if lane is None:
            names = ", ".join(item.name for item in known)
            raise ValueError(f"unknown lane {lane_name!r}; known lanes: {names}")
        return [lane], positionals
    if lane_count:
        if positionals:
            raise ValueError("--lanes cannot be combined with script names")
        if int(lane_count) <= 1:
            return None, positionals
        return lane_catalog(int(lane_count)), positionals
    return None, positionals


def test_scripts(requested: list[str] | None = None) -> list[str]:
    requested = sys.argv[1:] if requested is None else requested
    if requested:
        names = []
        for arg in requested:
            name = arg.rsplit("/", 1)[-1].rsplit("\\", 1)[-1]
            names.append(name if name.endswith(".py") else f"{name}.py")
        return names
    scripts = sorted(path.name for path in (ROOT / "scripts").glob("test-*.py"))
    return [name for name in scripts if name != FAULT_SUITE] + [FAULT_SUITE]


def compose_file_projects() -> list[tuple[str, str]]:
    """Every test Compose file mapped to its project name; the manual
    development files (`compose.yml`, `compose.cluster.yml`) are never
    swept — they belong to the user's Riverbed, not the test fixture."""
    projects = []
    for path in sorted(ROOT.glob("compose.*.yml")):
        if path.name == "compose.cluster.yml":
            continue
        match = re.search(r"^name:\s*(\S+)", path.read_text(encoding="utf-8"), re.MULTILINE)
        if match:
            projects.append((path.name, match.group(1)))
    # The mTLS, pipe and storage-join scripts override their file's `name:`
    # with an explicit `-p`; sweep those project names as well.
    projects.append(("compose.mtls.yml", "whitewater-mtls"))
    projects.append(("compose.pipe.yml", "whitewater-pipe"))
    projects.append(("compose.storage-join.yml", "whitewater-join"))
    return projects


def project_map(lanes: list[Lane] | None) -> list[tuple[str, str]]:
    """Sweep/diagnostics targets: every static test project plus each
    fixture lane's project — a suffixed lane name never appears in a
    compose file's `name:` field."""
    projects = compose_file_projects()
    for lane in lanes or []:
        entry = (COMPOSE_FILE, lane.project)
        if lane.fixture and entry not in projects:
            projects.append(entry)
    return projects


def compose(file: str, *args: str, env: dict, project: str = PROJECT, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["docker", "compose", "-p", project, "-f", file, *args],
        cwd=ROOT,
        env=env,
        check=check,
    )


def collect_diagnostics(label: str, env: dict, projects: list[tuple[str, str]] | None = None) -> None:
    directory = ROOT / "acceptance-logs"
    directory.mkdir(exist_ok=True)
    label = re.sub(r"[^a-zA-Z0-9_.-]", "_", label)
    for file, project in (compose_file_projects() if projects is None else projects):
        for kind, args in (("ps", ["ps", "--all"]), ("logs", ["logs", "--no-color", "--tail", "300"])):
            command = ["docker", "compose", "-p", project, "-f", file, *args]
            try:
                result = subprocess.run(command, cwd=ROOT, env=env, check=False, capture_output=True, text=True, timeout=15)
                output = result.stdout + result.stderr
            except (OSError, subprocess.TimeoutExpired) as error:
                output = f"Diagnostic collection failed: {error}\n"
            (directory / f"{label}-{project}-{kind}.log").write_text(output, encoding="utf-8")
    for kind, command in (("df", ["df", "-h", str(ROOT)]), ("docker-df", ["docker", "system", "df"])):
        try:
            result = subprocess.run(command, cwd=ROOT, check=False, capture_output=True, text=True, timeout=15)
            output = result.stdout + result.stderr
        except (OSError, subprocess.TimeoutExpired) as error:
            output = f"Diagnostic collection failed: {error}\n"
        (directory / f"{label}-host-{kind}.log").write_text(output, encoding="utf-8")
    print(f"--- diagnostics saved to {directory}", flush=True)


def sweep(env: dict, projects: list[tuple[str, str]] | None = None) -> None:
    """Remove every test project's containers and volumes, ignoring errors —
    teardown must be best-effort so one wedged project cannot mask others."""
    for file, project in (compose_file_projects() if projects is None else projects):
        subprocess.run(
            ["docker", "compose", "-p", project, "-f", file, "down", "-v", "--remove-orphans"],
            cwd=ROOT,
            env=env,
            check=False,
            capture_output=True,
        )


def ports_free(base_port: int = BASE_PORT) -> list[int]:
    busy = []
    for port in range(base_port, base_port + 3):
        with socket.socket() as probe:
            if probe.connect_ex(("127.0.0.1", port)) == 0:
                busy.append(port)
    return busy


def wait_healthy(timeout_s: int = HEALTH_TIMEOUT_S, nodes: list[str] | None = None) -> None:
    nodes = NODES if nodes is None else nodes
    deadline = time.monotonic() + timeout_s
    pending = set(nodes)
    while time.monotonic() < deadline:
        for endpoint in list(pending):
            try:
                with urllib.request.urlopen(f"{endpoint}/health", timeout=3) as response:
                    if response.status == 200:
                        pending.remove(endpoint)
            except Exception:
                pass
        if not pending:
            break
        time.sleep(1)
    else:
        raise AssertionError(f"verification Riverbed did not become healthy: {sorted(pending)}")
    wait_control_plane(deadline, nodes)


def wait_control_plane(deadline: float, nodes: list[str] | None = None) -> None:
    # `/health` reports the HTTP listener only; WCL calls fail with "leader
    # election is in progress" until every voter observes a leader. A Node that
    # restarted mid-suite must also finish replaying its catalog backlog before
    # scripts run — otherwise read-your-writes requests hit a half-applied
    # state (a Reader exists but its session open has not landed). Gate on
    # `/v1/admin/control-plane` until every Node reports a leader AND each
    # Node's applied index has converged with the log tail. The catalog is
    # quiescent between scripts, so the indexes converge.
    nodes = NODES if nodes is None else nodes
    key = os.environ.get("WHITEWATER_ADMIN_API_KEY", "whitewater-local-development-admin-key")
    while time.monotonic() < deadline:
        statuses = []
        for endpoint in nodes:
            try:
                request = urllib.request.Request(
                    f"{endpoint}/v1/admin/control-plane",
                    headers={"authorization": f"Bearer {key}"},
                )
                with urllib.request.urlopen(request, timeout=3) as response:
                    statuses.append(json.loads(response.read()))
            except Exception:
                statuses = []
                break
        if len(statuses) == len(nodes):
            tails = {status.get("last_log_index") for status in statuses}
            leaders_ok = all(status.get("leader_id") is not None for status in statuses)
            applied_ok = all(
                status.get("last_applied_index") is not None
                and status["last_applied_index"] >= (status.get("last_log_index") or 0)
                for status in statuses
            )
            if leaders_ok and applied_ok and len(tails) == 1:
                return
        time.sleep(1)
    raise AssertionError("verification Control Plane did not converge: leader or applied index missing")


def preflight(env: dict) -> None:
    """Log host disk state; on CI runners prune unused Docker objects when the
    suite would otherwise run out of disk mid-run. The prune is gated on CI —
    `docker system prune` is destructive on a developer machine."""
    for command in (["df", "-h", str(ROOT)], ["docker", "system", "df"]):
        try:
            subprocess.run(command, check=False)
        except OSError:
            pass
    if os.environ.get("CI") != "true":
        return
    free_bytes = shutil.disk_usage(ROOT).free
    if free_bytes < 8 * 1024**3:
        print("--- low disk on CI runner; pruning unused Docker objects", flush=True)
        try:
            subprocess.run(["docker", "system", "prune", "-af"], cwd=ROOT, env=env, check=False)
        except OSError:
            pass


def run_script(script: str, env: dict, gate_nodes: list[str] | None, projects: list[tuple[str, str]], prefix: str = "") -> str:
    """Run one script and return its summary outcome. `gate_nodes` re-gates
    on fixture health first — a predecessor may have restarted Nodes and
    left a fresh Control Plane election in flight."""
    path = ROOT / "scripts" / script
    if not path.exists():
        print(f"--- {prefix}{script}: file not found, skipped", flush=True)
        return "missing"
    print(f"--- {prefix}{script}", flush=True)
    started = time.monotonic()
    if gate_nodes is not None:
        try:
            wait_healthy(timeout_s=120, nodes=gate_nodes)
        except AssertionError as error:
            outcome = f"fixture not ready: {error}"
            print(f"--- {prefix}{script}: {outcome}", flush=True)
            collect_diagnostics(script, env, projects)
            return outcome
    try:
        subprocess.run(
            [sys.executable, str(path)],
            cwd=ROOT,
            env=env,
            check=True,
            timeout=SCRIPT_TIMEOUT_S,
        )
        outcome = "ok"
    except subprocess.TimeoutExpired:
        outcome = f"timeout after {SCRIPT_TIMEOUT_S}s"
    except subprocess.CalledProcessError as error:
        outcome = f"exit {error.returncode}"
    print(f"--- {prefix}{script}: {outcome} ({time.monotonic() - started:.0f}s)", flush=True)
    if outcome != "ok":
        collect_diagnostics(script, env, projects)
    return outcome


def run_lane(lane: Lane, base_env: dict, projects: list[tuple[str, str]], prefix: str = "") -> dict[str, str]:
    env = lane.env(base_env)
    if lane.fixture:
        try:
            compose(COMPOSE_FILE, "down", "-v", "--remove-orphans", env=env, project=lane.project, check=False)
            compose(COMPOSE_FILE, "up", "-d", env=env, project=lane.project)
            wait_healthy(nodes=lane.nodes)
        except (AssertionError, subprocess.CalledProcessError) as error:
            collect_diagnostics(lane.name, env, projects)
            return {script: f"fixture not ready: {error}" for script in lane.scripts}
    return {
        script: run_script(script, env, lane.nodes if lane.fixture else None, projects, prefix)
        for script in lane.scripts
    }


def summarize(results: dict[str, str]) -> int:
    failures = {script: outcome for script, outcome in results.items() if outcome != "ok"}
    for script in sorted(results):
        print(f"{results[script]:>24}  {script}")
    if failures:
        print(f"acceptance suite failed: {failures}", file=sys.stderr)
        return 1
    print(f"acceptance suite ok: {len(results)} scripts, all test projects removed")
    return 0


def main() -> int:
    try:
        lanes, positionals = parse_invocation(sys.argv[1:], os.environ)
    except ValueError as error:
        print(f"invalid acceptance invocation: {error}", file=sys.stderr)
        return 2

    env = dict(
        os.environ,
        COMPOSE_FILE=COMPOSE_FILE,
        PYTHONUNBUFFERED="1",
    )

    if lanes is None:
        busy = ports_free()
        if busy:
            print(
                f"ports {busy} are already bound; stop the Riverbed using them "
                "(scripts/cluster.ps1 down for the development project) or pick "
                "a free base via WHITEWATER_TEST_BASE_PORT",
                file=sys.stderr,
            )
            return 2
        env["WHITEWATER_TEST_BASE_PORT"] = str(BASE_PORT)
        preflight(env)
        projects = project_map(None)
        results: dict[str, str] = {}
        try:
            # A previous crashed run may have left the shared fixture behind;
            # down -v guarantees the suite starts from an empty Riverbed.
            compose(COMPOSE_FILE, "down", "-v", "--remove-orphans", env=env, check=False)
            compose(COMPOSE_FILE, "up", "-d", "--build", env=env)
            wait_healthy()
            for script in test_scripts(positionals):
                results[script] = run_script(script, env, NODES, projects)
        except Exception:
            collect_diagnostics("runner", env, projects)
            raise
        finally:
            sweep(env, projects)
        return summarize(results)

    fixture_lanes = [lane for lane in lanes if lane.fixture]
    busy = [port for lane in fixture_lanes for port in ports_free(lane.base_port)]
    if busy:
        print(
            f"ports {busy} are already bound; stop the Riverbed using them "
            "or pick a free base via WHITEWATER_TEST_BASE_PORT",
            file=sys.stderr,
        )
        return 2
    try:
        for lane in fixture_lanes:
            lane.env(env)
    except ValueError as error:
        print(f"invalid lane configuration: {error}", file=sys.stderr)
        return 2

    preflight(env)
    projects = project_map(lanes)
    results = {}
    try:
        if fixture_lanes:
            # Build the shared Node image once up front: every fixture lane's
            # `up -d` reuses the whitewater-verify-node tag instead of racing
            # concurrent builds of the same Dockerfile.
            compose(COMPOSE_FILE, "build", env=env)
        if len(lanes) == 1:
            results.update(run_lane(lanes[0], env, projects))
        else:
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(lanes)) as pool:
                futures = {pool.submit(run_lane, lane, env, projects, f"[{lane.name}] "): lane for lane in lanes}
                for future in concurrent.futures.as_completed(futures):
                    lane = futures[future]
                    try:
                        results.update(future.result())
                    except Exception as error:
                        results.update({script: f"lane error: {error}" for script in lane.scripts})
                        collect_diagnostics(lane.name, lane.env(env), projects)
    except Exception:
        collect_diagnostics("runner", env, projects)
        raise
    finally:
        sweep(env, projects)
    return summarize(results)


if __name__ == "__main__":
    sys.exit(main())
