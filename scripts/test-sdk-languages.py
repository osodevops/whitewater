"""Cross-language SDK conformance gate.

Runs every SDK's shared-corpus runner (`sdk/spec/conformance/*.json`)
against this lane's compose.verify.yml Riverbed — the same fixture set
test-sdk-conformance.py drives through the Rust runner, executed here by
Go, Python, Java, Node.js/TypeScript, and .NET as well. This is the
AGENTS.md "one cross-language three-Node conformance suite" gate: an SDK
that cannot express a corpus op fails explicitly rather than skipping.

Each sibling repository is discovered next to this checkout
(`../whitewater-go`, etc.) or via `WHITEWATER_<LANG>_DIR`. A missing repo
or toolchain reports `absent`/`no-toolchain` and fails the gate — the
lane exists to prove all six, so a silent skip would be a false green.
`WHITEWATER_SDK_LANGUAGES=go,python` narrows the expectation explicitly
for local iteration.

Environment each runner receives:

  WHITEWATER_ENDPOINTS   comma-separated lane Node base URLs
  WHITEWATER_SPEC_DIR    this checkout's sdk/spec/conformance
  WHITEWATER_API_KEY / WHITEWATER_ADMIN_API_KEY / FINNSTREAM_ADMIN_API_KEY
  WHITEWATER_CONFORMANCE_URLS / WHITEWATER_FIXTURE_DIR  (legacy aliases)
  WHITEWATER_CHAOS_CMD   this checkout's chaos driver
  WHITEWATER_TEST_BASE_PORT + COMPOSE_FILE  inherited from the lane so the
                         chaos driver steers this Riverbed, not the default
"""

import os
import shutil
import subprocess
import sys
import tempfile
import time

KEY = "whitewater-local-development-admin-key"
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SPEC_DIR = os.path.join(ROOT, "sdk", "spec", "conformance")
WINDOWS = os.name == "nt"

# Per-runner wall clock: Java's `verify` runs the module's unit suite
# before the integration corpus; chaos fixtures drive multi-Node outages
# and re-gates that alone take several minutes.
RUNNER_TIMEOUT_S = {"rust": 900, "go": 900, "python": 900,
                    "java": 1800, "node": 900, "dotnet": 900}


def wait_ready(endpoint):
    """Gate on a majority-committed catalog, not just the listener —
    fixtures issue WCL immediately."""
    for _ in range(120):
        try:
            import json
            import urllib.request

            with urllib.request.urlopen(f"{endpoint}/health", timeout=3) as response:
                if response.status != 200:
                    continue
            request = urllib.request.Request(
                f"{endpoint}/v1/admin/wcl",
                data=json.dumps({"script": "SHOW DOMAINS;"}).encode(),
                headers={
                    "content-type": "application/json",
                    "authorization": f"Bearer {KEY}",
                },
                method="POST",
            )
            with urllib.request.urlopen(request, timeout=3) as wcl_reply:
                if wcl_reply.status == 200:
                    return
        except Exception:
            pass
        time.sleep(0.5)
    raise AssertionError(f"{endpoint} never answered /health")


def run_steps(steps, cwd, env, timeout_s):
    """Run a runner's command list serially inside its repo; first
    failure wins. Returns the failing exit code or 0."""
    for command in steps:
        argv = command[0] + command[1:] if command and isinstance(command[0], list) else command
        started = time.monotonic()
        try:
            run = subprocess.run(argv, cwd=cwd, env=env, timeout=timeout_s)
            code = run.returncode
        except subprocess.TimeoutExpired:
            code = -9
        elapsed = time.monotonic() - started
        print(f"    $ {' '.join(argv)} -> {code} ({elapsed:.0f}s)", flush=True)
        if code != 0:
            return code
    return 0


GO_IMAGE = "golang:1.23-bookworm"


def go_leg(directory):
    """The Go runner's steps. Preferred path is a host `go` toolchain; on
    a host without Go the pure-Go test binary cross-compiles inside the
    golang image and then runs NATIVE steps — chaos fixtures need the
    host's Python + Docker daemon, which a Linux container cannot reach
    through Docker Desktop's npipe."""
    spec = {"dir": directory}
    if shutil.which("go"):
        spec["toolchain"] = ["go"]
        spec["steps"] = [["go", "test", "-v", "-count=1",
                          "-run", "TestSharedConformanceCorpus", "."]]
    elif shutil.which("docker") and directory:
        # Compile inside the image, but -o must land on a mounted host
        # path: the container's own filesystem dies with --rm. The host
        # tempdir mounts as /ww-out so the binary survives to run
        # natively (chaos needs the host's Python + Docker daemon).
        binary = os.path.join(tempfile.gettempdir(), "ww-go-conformance")
        goos = "windows" if WINDOWS else "linux"
        if WINDOWS:
            binary += ".exe"
        spec["toolchain"] = ["docker"]
        spec["artifact"] = binary
        spec["steps"] = [
            ["docker", "run", "--rm",
             "-v", f"{directory}:/ws",
             "-v", f"{os.path.dirname(binary)}:/ww-out",
             "-w", "/ws",
             "-e", f"GOOS={goos}", "-e", "GOARCH=amd64",
             "-e", "CGO_ENABLED=0",
             GO_IMAGE, "go", "test", "-c",
             "-o", f"/ww-out/{os.path.basename(binary)}", "."],
            [binary, "-test.v", "-test.run", "TestSharedConformanceCorpus"],
        ]
    else:
        spec["toolchain"] = ["go"]
        spec["steps"] = []
    return spec


def java_leg(directory):
    """mvnw resolves against the SDK checkout, not PATH — a bare
    `mvnw.cmd` argv[0] cannot be located by CreateProcess under a
    foreign cwd, so the step names the wrapper by absolute path."""
    wrapper = None
    if directory:
        wrapper = os.path.join(directory, "mvnw.cmd" if WINDOWS else "mvnw")
    return {
        "dir": directory,
        "toolchain": ["java"],
        "steps": [[wrapper or "mvnw", "-q", "verify",
                   "-Dit.test=ConformanceRunnerIT",
                   "-DfailIfNoTests=false"]],
    }


def plan(env=os.environ):
    """Build the runner table + shared environment.

    Separate from `main` so the gate's expectations are unit-testable
    without a live Riverbed. `env` mirrors os.environ (lane exports like
    WHITEWATER_TEST_BASE_PORT / COMPOSE_FILE / COMPOSE_PROJECT_NAME must
    pass through so the chaos driver steers this lane's Riverbed)."""

    base_port = int(env.get("WHITEWATER_TEST_BASE_PORT", "7071"))
    endpoints = [f"http://localhost:{base_port + offset}" for offset in range(3)]
    chaos_cmd = f"{sys.executable} {os.path.join(ROOT, 'scripts', 'chaos_driver.py')}"
    # chaos_driver invokes `docker compose -f $COMPOSE_FILE` under the
    # sibling repo's cwd — a relative path only resolves inside ROOT.
    compose_file = env.get("COMPOSE_FILE", "compose.verify.yml")
    if not os.path.isabs(compose_file):
        compose_file = os.path.join(ROOT, compose_file)
    base_env = dict(
        env,
        WHITEWATER_ENDPOINTS=",".join(endpoints),
        WHITEWATER_SPEC_DIR=SPEC_DIR,
        WHITEWATER_API_KEY=KEY,
        WHITEWATER_ADMIN_API_KEY=KEY,
        WHITEWATER_CHAOS_CMD=chaos_cmd,
        WHITEWATER_TEST_BASE_PORT=str(base_port),
        COMPOSE_FILE=compose_file,
        # Legacy aliases the .NET/Go runners also accept.
        WHITEWATER_CONFORMANCE_URLS=",".join(endpoints),
        WHITEWATER_FIXTURE_DIR=SPEC_DIR,
        FINNSTREAM_ADMIN_API_KEY=KEY,
    )

    def locate(name, repo):
        override = env.get(f"WHITEWATER_{name.upper()}_DIR")
        candidate = override or os.path.join(os.path.dirname(ROOT), repo)
        return candidate if os.path.isdir(candidate) else None

    runners = {
        "rust": {
            "dir": ROOT,
            "toolchain": ["cargo"],
            "env": {"WHITEWATER_SDK_NODES": ",".join(endpoints)},
            "steps": [["cargo", "test", "--test", "sdk_conformance", "--", "--nocapture"]],
        },
        "go": go_leg(locate("go", "whitewater-go")),
        "python": {
            "dir": locate("python", "whitewater-python"),
            "toolchain": [sys.executable],
            "steps": [
                [sys.executable, "-m", "pip", "install", "-q", "-e", "."],
                [sys.executable, "-m", "pytest",
                 "tests/conformance/test_conformance.py", "-q"],
            ],
        },
        "java": java_leg(locate("java", "whitewater-java")),
        "node": {
            "dir": locate("node", "whitewater-node"),
            "toolchain": ["npm"],
            "steps": [
                [["npm.cmd" if WINDOWS else "npm"], "ci"],
                [["npx.cmd" if WINDOWS else "npx"], "vitest", "run",
                 "test/conformance.test.ts"],
            ],
        },
        "dotnet": {
            "dir": locate("dotnet", "whitewater-dotnet"),
            "toolchain": ["dotnet"],
            "steps": [["dotnet", "test",
                       os.path.join("tests", "Whitewater.Tests",
                                    "Whitewater.Tests.csproj"),
                       "--filter", "FullyQualifiedName~ConformanceRunner"]],
        },
    }

    expected = env.get("WHITEWATER_SDK_LANGUAGES")
    if expected:
        keep = {name.strip() for name in expected.split(",") if name.strip()}
        runners = {name: spec for name, spec in runners.items() if name in keep}
        unknown = keep - {
            "rust", "go", "python", "java", "node", "dotnet"}
        if unknown:
            raise AssertionError(
                f"WHITEWATER_SDK_LANGUAGES names unknown SDKs: {sorted(unknown)}")
    return endpoints, base_env, runners


def main():
    endpoints, base_env, runners = plan()
    for endpoint in endpoints:
        wait_ready(endpoint)

    results = {}
    for name, spec in runners.items():
        directory = spec["dir"]
        if directory is None:
            results[name] = "absent"
            print(f"--- sdk-language {name}: absent (no checkout beside {ROOT})",
                  flush=True)
            continue
        missing = [tool for tool in spec["toolchain"] if shutil.which(tool) is None]
        if missing:
            results[name] = "no-toolchain"
            print(f"--- sdk-language {name}: no-toolchain {missing}", flush=True)
            continue
        print(f"--- sdk-language {name}: running {len(spec['steps'])} step(s) "
              f"in {directory}", flush=True)
        env = dict(base_env)
        env.update(spec.get("env", {}))
        code = run_steps(spec["steps"], directory, env, RUNNER_TIMEOUT_S[name])
        artifact = spec.get("artifact")
        if artifact:
            try:
                os.remove(artifact)
            except OSError:
                pass
        results[name] = "failed" if code != 0 else "ok"
        print(f"--- sdk-language {name}: {results[name]}", flush=True)

    width = max(len(name) for name in results) if results else 1
    for name in sorted(results):
        print(f"{name:>{width}}  {results[name]}")
    bad = {name: outcome for name, outcome in results.items() if outcome != "ok"}
    if bad:
        raise AssertionError(f"sdk-language gate failed: {bad}")
    print(f"sdk-language gate green ({len(results)} runners)")


if __name__ == "__main__":
    main()
