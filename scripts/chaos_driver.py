"""Conformance chaos-hook driver for the verification topology.

Invoked by an SDK's conformance runner for `chaos.*` steps (the runner
executes `<WHITEWATER_CHAOS_CMD> <op>`). The driver owns the action's
meaning and does not return until the Riverbed answers majority-
committed WCL again, so fixtures never carry timing waits.

Actions:
  chaos.restart         restart one storage Node (node2) and re-gate on health
  chaos.restart_owner   INSPECT PLACEMENT for the named Feed, restart the
                        Node that currently owns its range, and re-gate
  chaos.stop_two        stop two of three Nodes, prove the survivor fails
                        every write closed during the outage (a stale
                        self-leader still cannot commit without a voter
                        majority), restart both, and re-gate
  chaos.partition       disconnect node3 from the Riverbed network, prove
                        the container stays Up but unreachable while the
                        majority side keeps committing, reconnect with its
                        service aliases restored (plain `network connect`
                        drops them, leaving the Node unresolvable), re-gate
"""

import base64
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid

BASE_PORT = int(os.environ.get("WHITEWATER_TEST_BASE_PORT", "7071"))
ENDPOINTS = [f"http://localhost:{BASE_PORT + offset}" for offset in range(3)]
KEY = "whitewater-local-development-admin-key"
COMPOSE_FILE = os.environ.get("COMPOSE_FILE", "compose.verify.yml")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def control_plane(endpoint):
    try:
        request = urllib.request.Request(
            f"{endpoint}/v1/admin/control-plane",
            headers={"authorization": f"Bearer {KEY}"},
        )
        with urllib.request.urlopen(request, timeout=3) as response:
            return json.loads(response.read())
    except Exception:
        return None


def wait_riverbed(timeout_s=120):
    # `/health` reports the HTTP listener only and a stale local catalog can
    # still answer reads, so usability means every Node reports a leader AND
    # each applied index has converged with the replicated log tail — the
    # same convergence the acceptance runner requires between scripts.
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        statuses = [control_plane(endpoint) for endpoint in ENDPOINTS]
        if all(status is not None for status in statuses):
            tails = {status.get("last_log_index") for status in statuses}
            leaders_ok = all(status.get("leader_id") is not None for status in statuses)
            applied_ok = all(
                status.get("last_applied_index") is not None
                and status["last_applied_index"] >= (status.get("last_log_index") or 0)
                for status in statuses
            )
            if leaders_ok and applied_ok and len(tails) == 1:
                return
        time.sleep(0.5)
    raise AssertionError("Riverbed did not converge after the chaos action")


def wait_writable(timeout_s=45):
    # `SHOW DOMAINS` can answer from a stale local catalog; only a committed
    # write proves a leader holding a real quorum. Chaos actions that can
    # leave the Control Plane mid-election (an isolated Node returns with an
    # inflated term and forces one) re-gate here so the next fixture never
    # meets a still-electing writer path.
    deadline = time.monotonic() + timeout_s
    attempt = 0
    while time.monotonic() < deadline:
        attempt += 1
        try:
            request = urllib.request.Request(
                f"{ENDPOINTS[0]}/v1/admin/wcl",
                data=json.dumps({"script": f"CREATE DOMAIN chaosgate{int(time.time() * 1000)}{attempt};"}).encode(),
                headers={"authorization": f"Bearer {KEY}", "content-type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(request, timeout=8) as reply:
                if reply.status == 200:
                    return
        except Exception:
            pass
        time.sleep(0.5)
    raise AssertionError("Riverbed did not commit a write after the chaos action")


def probe_writer():
    # Self-provisioned append-path probe: chaos actions must prove the
    # append plane — owner routing plus follower commit corroboration —
    # recovered, not just the WCL catalog. A dedicated probe Feed keeps
    # fixture record assertions uncontaminated.
    stamp = int(time.time() * 1000)
    domain, writer = f"chaosap{stamp}", f"chaosapw{stamp}"
    request = urllib.request.Request(
        f"{ENDPOINTS[0]}/v1/admin/wcl",
        data=json.dumps({"script": f"CREATE DOMAIN {domain}; CREATE FEED {domain}.events; CREATE WRITER {writer} TO {domain}.events; OPEN WRITER SESSION {writer};"}).encode(),
        headers={"authorization": f"Bearer {KEY}", "content-type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=15) as reply:
        body = json.load(reply)
    epoch = body["results"][3]["data"]["session_epoch"]
    return writer, epoch


def wait_appends(writer, epoch, timeout_s=90):
    # The append path converges slower than WCL after a heal: range owners
    # re-register and lagging replicas must persist fresh commit evidence
    # before the owner can corroborate. Poll a real append until it commits.
    last = "no attempt completed"
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            probe = {
                "request_id": str(uuid.uuid4()),
                "writer": writer,
                "session_epoch": epoch,
                "event_time_ns": "1",
                "key_base64": base64.b64encode(b"chaos-probe").decode(),
                "payload_base64": base64.b64encode(b"chaos-probe").decode(),
                "metadata_base64": {},
            }
            request = urllib.request.Request(
                f"{ENDPOINTS[0]}/v1/writers/append",
                data=json.dumps(probe).encode(),
                headers={"authorization": f"Bearer {KEY}", "content-type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(request, timeout=10) as reply:
                if reply.status == 200:
                    return
                last = f"status {reply.status}"
        except urllib.error.HTTPError as error:
            last = f"{error.code}: {error.read()[:160]!r}"
        except Exception as error:
            last = str(error)
        time.sleep(1)
    raise AssertionError(f"append plane did not recover after the chaos action ({last})")


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


def stop_two():
    # Losing two of three Nodes removes every majority: the survivor must
    # fail writes closed rather than acknowledge work it cannot commit. A
    # WCL write is the cleanest probe — even an ambiguous timeout cannot
    # fabricate a commit without a majority, and a probe that lands late
    # only creates an inert uniquely-named Domain.
    writer, epoch = probe_writer()
    subprocess.run(
        ["docker", "compose", "-f", COMPOSE_FILE, "stop", "node2", "node3"],
        cwd=ROOT, check=True, timeout=120,
    )
    # A lone survivor may keep reporting a stale self-leader — it cannot
    # commit without a voter majority either way — so the evidence probes
    # writes directly: every WCL write during the outage must be refused
    # or drop unacknowledged, never return success.
    failure = None
    try:
        for attempt in range(6):
            status_code = None
            try:
                probe = urllib.request.Request(
                    f"{ENDPOINTS[0]}/v1/admin/wcl",
                    data=json.dumps({"script": f"CREATE DOMAIN chaostwo{int(time.time() * 1000)}{attempt};"}).encode(),
                    headers={"authorization": f"Bearer {KEY}", "content-type": "application/json"},
                    method="POST",
                )
                with urllib.request.urlopen(probe, timeout=8) as reply:
                    status_code = reply.status
            except urllib.error.HTTPError as error:
                status_code = error.code
            except Exception:
                pass  # timeout/reset — no majority existed to commit it
            if status_code == 200:
                raise AssertionError("a write committed while two of three Nodes were down")
            time.sleep(1)
    except AssertionError as error:
        failure = error
    finally:
        subprocess.run(
            ["docker", "compose", "-f", COMPOSE_FILE, "start", "node2", "node3"],
            cwd=ROOT, check=True, timeout=120,
        )
    wait_riverbed()
    wait_writable()
    wait_appends(writer, epoch)
    # The probe writes advanced the log: re-gate on applied convergence so
    # the next fixture reads a fully settled catalog on every Node.
    wait_riverbed()
    time.sleep(3)
    if failure is not None:
        raise failure


def partition():
    # `docker network disconnect` severs a running container's interface:
    # peers cannot reach it and the published port dies, but the process
    # keeps running — a network fault, not a crash. The majority side must
    # keep committing writes (re-electing if the partitioned Node led),
    # and on reconnect the partitioned replica rejoins and catches up.
    writer, epoch = probe_writer()
    container = subprocess.run(
        ["docker", "compose", "-f", COMPOSE_FILE, "ps", "-q", "node3"],
        cwd=ROOT, check=True, timeout=30, capture_output=True, text=True,
    ).stdout.strip()
    networks = json.loads(subprocess.run(
        ["docker", "inspect", container, "--format", "{{json .NetworkSettings.Networks}}"],
        check=True, timeout=30, capture_output=True, text=True,
    ).stdout)
    candidates = [name for name in networks if name != "bridge"]
    network = next((name for name in candidates if "whitewater" in name), candidates[0])
    # Compose publishes the service name as a network alias (`node3`).
    # `docker network connect` does not restore it, and without the alias
    # peers can never address the healed Node: it stays a permanent Raft
    # candidate with an inflated term and never receives AppendEntries.
    aliases = networks[network].get("Aliases") or []
    subprocess.run(
        ["docker", "network", "disconnect", network, container],
        check=True, timeout=30,
    )
    failure = None
    try:
        # Partition proof: the host-published port must be unreachable.
        reachable = True
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            try:
                with urllib.request.urlopen(f"{ENDPOINTS[2]}/health", timeout=3):
                    pass
            except Exception:
                reachable = False
                break
            time.sleep(0.5)
        if reachable:
            raise AssertionError("partitioned Node stayed reachable from the host")
        # The process keeps running: a network fault, not a crash.
        state = subprocess.run(
            ["docker", "inspect", container, "--format", "{{.State.Running}}"],
            check=True, timeout=30, capture_output=True, text=True,
        ).stdout.strip()
        if state != "true":
            raise AssertionError("partitioned Node exited instead of staying isolated")
        # Majority continuity: the surviving two Nodes hold quorum, so a
        # write must commit once the election settles (bounded, because a
        # partitioned leader takes a moment to be replaced).
        committed = False
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline and not committed:
            try:
                probe = urllib.request.Request(
                    f"{ENDPOINTS[0]}/v1/admin/wcl",
                    data=json.dumps({"script": f"CREATE DOMAIN chaospart{int(time.time() * 1000)};"}).encode(),
                    headers={"authorization": f"Bearer {KEY}", "content-type": "application/json"},
                    method="POST",
                )
                with urllib.request.urlopen(probe, timeout=8) as reply:
                    committed = reply.status == 200
            except Exception:
                time.sleep(1)
        if not committed:
            raise AssertionError("the majority side did not keep serving writes during the partition")
    except AssertionError as error:
        failure = error
    finally:
        connect = ["docker", "network", "connect"]
        for alias in aliases:
            connect += ["--alias", alias]
        subprocess.run(connect + [network, container], check=True, timeout=30)
    wait_riverbed()
    wait_writable()
    wait_appends(writer, epoch)
    # The probe writes advanced the log: re-gate on applied convergence so
    # the next fixture reads a fully settled catalog on every Node.
    wait_riverbed()
    time.sleep(3)
    if failure is not None:
        raise failure


ACTIONS = {
    "chaos.restart": restart,
    "chaos.restart_owner": restart_owner,
    "chaos.stop_two": stop_two,
    "chaos.partition": partition,
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
