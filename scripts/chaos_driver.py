"""Conformance chaos-hook driver for the verification topology.

Invoked by an SDK's conformance runner for `chaos.*` steps (the runner
executes `<WHITEWATER_CHAOS_CMD> <op>`). The driver owns the action's
meaning and does not return until the Riverbed answers majority-
committed WCL again, so fixtures never carry timing waits.

Actions:
  chaos.restart         restart one storage Node (node2) and re-gate on health
  chaos.restart_owner   INSPECT PLACEMENT for the named Feed, restart the
                        Node that currently owns its range, and re-gate
"""

import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request

BASE_PORT = int(os.environ.get("WHITEWATER_TEST_BASE_PORT", "7071"))
ENDPOINTS = [f"http://localhost:{BASE_PORT + offset}" for offset in range(3)]
KEY = "whitewater-local-development-admin-key"
COMPOSE_FILE = os.environ.get("COMPOSE_FILE", "compose.verify.yml")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def healthy(endpoint):
    try:
        with urllib.request.urlopen(f"{endpoint}/health", timeout=3) as response:
            if response.status != 200:
                return False
        request = urllib.request.Request(
            f"{endpoint}/v1/admin/wcl",
            data=json.dumps({"script": "SHOW DOMAINS;"}).encode(),
            headers={"authorization": f"Bearer {KEY}", "content-type": "application/json"},
            method="POST",
        )
        # /health answers before Control Plane authority exists — the next
        # fixture step issues WCL immediately, so usability means a
        # majority-committed catalog, not just the listener.
        with urllib.request.urlopen(request, timeout=3) as wcl_reply:
            return wcl_reply.status == 200
    except Exception:
        return False


def wait_riverbed(timeout_s=120):
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if all(healthy(endpoint) for endpoint in ENDPOINTS):
            return
        time.sleep(0.5)
    raise AssertionError("Riverbed did not become healthy after the chaos action")


def restart():
    subprocess.run(
        ["docker", "compose", "-f", COMPOSE_FILE, "restart", "node2"],
        cwd=ROOT, check=True, timeout=120,
    )
    wait_riverbed()
    # Majority-committed WCL answers before a restarted Node finishes
    # re-electing range owners and rebuilding session registries; a short
    # bounded settle keeps the next fixture step off the re-election edge.
    time.sleep(3)


def placement(feed):
    # Retry through a still-recovering control plane; the action runs
    # immediately after the previous step committed WCL.
    deadline = time.monotonic() + 30
    last = None
    while time.monotonic() < deadline:
        for endpoint in ENDPOINTS:
            try:
                request = urllib.request.Request(
                    f"{endpoint}/v1/admin/wcl",
                    data=json.dumps({"script": f"INSPECT PLACEMENT FOR FEED {feed};"}).encode(),
                    headers={"authorization": f"Bearer {KEY}", "content-type": "application/json"},
                    method="POST",
                )
                with urllib.request.urlopen(request, timeout=5) as reply:
                    body = json.load(reply)
                data = body["results"][0]["data"]
                if data.get("owner"):
                    return data
            except Exception as error:
                last = error
        time.sleep(0.5)
    raise AssertionError(f"placement for {feed} never answered: {last}")


def restart_owner(feed):
    # The named Feed's current range owner is the fault target, so the
    # scenario exercises loss of the serving owner rather than a replica
    # chosen at random.
    owner = placement(feed)["owner"]
    service = f"node{owner.removeprefix('control-')}"
    subprocess.run(
        ["docker", "compose", "-f", COMPOSE_FILE, "restart", service],
        cwd=ROOT, check=True, timeout=120,
    )
    wait_riverbed()
    time.sleep(3)


ACTIONS = {
    "chaos.restart": restart,
    "chaos.restart_owner": restart_owner,
}


def main():
    if len(sys.argv) < 2 or sys.argv[1] not in ACTIONS:
        known = ", ".join(sorted(ACTIONS))
        print(f"usage: chaos_driver.py <action> [args]; known: {known}", file=sys.stderr)
        return 2
    ACTIONS[sys.argv[1]](*sys.argv[2:])


if __name__ == "__main__":
    try:
        main()
    except (AssertionError, subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
        print(f"chaos action failed: {error}", file=sys.stderr)
        sys.exit(1)
