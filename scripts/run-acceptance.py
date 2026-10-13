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

    python scripts/run-acceptance.py                     # every scripts/test-*.py
    python scripts/run-acceptance.py test-m2-writer-session.py ...

`WHITEWATER_VERIFY_SUBNET` is honoured through `compose.verify.yml`; set it
to an unused subnet when Docker's default pools are exhausted. The runner
refuses to start when the verification ports are already bound, so it can
never disrupt the development Riverbed on 7071-7073.
"""

import os
import re
import socket
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
COMPOSE_FILE = "compose.verify.yml"
PROJECT = "whitewater-verification"
BASE_PORT = int(os.environ.get("WHITEWATER_TEST_BASE_PORT", "7371"))
NODES = [f"http://localhost:{BASE_PORT + offset}" for offset in range(3)]
HEALTH_TIMEOUT_S = int(os.environ.get("WHITEWATER_ACCEPTANCE_HEALTH_TIMEOUT_S", "600"))
SCRIPT_TIMEOUT_S = int(os.environ.get("WHITEWATER_ACCEPTANCE_SCRIPT_TIMEOUT_S", "1800"))

# The fault suite re-runs three suites internally and restarts the whole
# Riverbed; keep it last so its disruption cannot degrade earlier evidence.
FAULT_SUITE = "test-m111-fault-suite.py"


def test_scripts() -> list[str]:
    requested = sys.argv[1:]
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
    # The mTLS and pipe scripts override their file's `name:` with an
    # explicit `-p`; sweep those project names as well.
    projects.append(("compose.mtls.yml", "whitewater-mtls"))
    projects.append(("compose.pipe.yml", "whitewater-pipe"))
    return projects


def compose(file: str, *args: str, env: dict, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["docker", "compose", "-p", PROJECT, "-f", file, *args],
        cwd=ROOT,
        env=env,
        check=check,
    )


def sweep(env: dict) -> None:
    """Remove every test project's containers and volumes, ignoring errors —
    teardown must be best-effort so one wedged project cannot mask others."""
    for file, project in compose_file_projects():
        subprocess.run(
            ["docker", "compose", "-p", project, "-f", file, "down", "-v", "--remove-orphans"],
            cwd=ROOT,
            env=env,
            check=False,
            capture_output=True,
        )


def ports_free() -> list[int]:
    busy = []
    for port in range(BASE_PORT, BASE_PORT + 3):
        with socket.socket() as probe:
            if probe.connect_ex(("127.0.0.1", port)) == 0:
                busy.append(port)
    return busy


def wait_healthy() -> None:
    deadline = time.monotonic() + HEALTH_TIMEOUT_S
    pending = set(NODES)
    while time.monotonic() < deadline:
        for endpoint in list(pending):
            try:
                with urllib.request.urlopen(f"{endpoint}/health", timeout=3) as response:
                    if response.status == 200:
                        pending.remove(endpoint)
            except Exception:
                pass
        if not pending:
            return
        time.sleep(1)
    raise AssertionError(f"verification Riverbed did not become healthy: {sorted(pending)}")


def main() -> int:
    busy = ports_free()
    if busy:
        print(
            f"ports {busy} are already bound; stop the Riverbed using them "
            "(scripts/cluster.ps1 down for the development project) or pick "
            "a free base via WHITEWATER_TEST_BASE_PORT",
            file=sys.stderr,
        )
        return 2

    env = dict(
        os.environ,
        COMPOSE_FILE=COMPOSE_FILE,
        WHITEWATER_TEST_BASE_PORT=str(BASE_PORT),
        PYTHONUNBUFFERED="1",
    )
    scripts = test_scripts()
    results: dict[str, str] = {}
    try:
        # A previous crashed run may have left the shared fixture behind;
        # down -v guarantees the suite starts from an empty Riverbed.
        compose(COMPOSE_FILE, "down", "-v", "--remove-orphans", env=env, check=False)
        compose(COMPOSE_FILE, "up", "-d", "--build", env=env)
        wait_healthy()
        for script in scripts:
            path = ROOT / "scripts" / script
            if not path.exists():
                results[script] = "missing"
                print(f"--- {script}: file not found, skipped")
                continue
            print(f"--- {script}")
            started = time.monotonic()
            try:
                subprocess.run(
                    [sys.executable, str(path)],
                    cwd=ROOT,
                    env=env,
                    check=True,
                    timeout=SCRIPT_TIMEOUT_S,
                )
                results[script] = "ok"
            except subprocess.TimeoutExpired:
                results[script] = f"timeout after {SCRIPT_TIMEOUT_S}s"
            except subprocess.CalledProcessError as error:
                results[script] = f"exit {error.returncode}"
            print(f"--- {script}: {results[script]} ({time.monotonic() - started:.0f}s)")
    finally:
        sweep(env)

    failures = {script: outcome for script, outcome in results.items() if outcome != "ok"}
    for script, outcome in results.items():
        print(f"{outcome:>24}  {script}")
    if failures:
        print(f"acceptance suite failed: {failures}", file=sys.stderr)
        return 1
    print(f"acceptance suite ok: {len(results)} scripts, all test projects removed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
