import base64
import http.client
import json
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

COMPOSE = ["docker", "compose", "-f", "compose.m4-move.yml"]
NODES = [f"http://localhost:{port}" for port in range(7171, 7175)]
KEY = "whitewater-local-development-admin-key"


def post(node, path, payload):
    request = urllib.request.Request(
        f"{node}{path}",
        data=json.dumps(payload).encode(),
        headers={"authorization": f"Bearer {KEY}", "content-type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        return error.code, json.load(error)


def read(node, feed, after=None):
    query = urllib.parse.urlencode({"feed": feed, "limit": 100, **({"after": after} if after else {})})
    request = urllib.request.Request(
        f"{node}/v1/feeds/records?{query}",
        headers={"authorization": f"Bearer {KEY}"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def placement(node, feed):
    status, response = post(node, "/v1/admin/wcl", {"script": f"INSPECT PLACEMENT FOR FEED {feed};"})
    if status != 200:
        raise AssertionError(response)
    return response["results"][0]["data"]


def wait_for_fabric():
    for _ in range(90):
        try:
            if all(urllib.request.urlopen(f"{node}/health", timeout=2).status == 200 for node in NODES):
                return
        except (urllib.error.URLError, TimeoutError, ConnectionError, http.client.RemoteDisconnected):
            pass
        time.sleep(1)
    raise AssertionError("four-Node Riverbed did not become healthy")


def main():
    subprocess.run([*COMPOSE, "down", "-v"], check=True)
    subprocess.run([*COMPOSE, "up", "--build", "-d"], check=True)
    try:
        wait_for_fabric()
        suffix = f"mv{uuid.uuid4().hex[:12]}"
        feed, writer = f"{suffix}.events", f"writer{suffix}"
        script = f"CREATE SPACE {suffix}; CREATE FEED {feed}; CREATE WRITER {writer} TO {feed}; OPEN WRITER SESSION {writer};"
        for _ in range(30):
            status, result = post(NODES[0], "/v1/admin/wcl", {"script": script})
            if status == 200:
                break
            time.sleep(1)
        if status != 200:
            raise AssertionError(result)
        epoch = result["results"][-1]["data"]["session_epoch"]
        cursors = []

        def append(index, node=None):
            payload = {
                "request_id": str(uuid.uuid4()),
                "writer": writer,
                "session_epoch": epoch,
                "event_time_ns": str(index),
                "key_base64": base64.b64encode(b"order-123").decode(),
                "payload_base64": base64.b64encode(f"event-{index}".encode()).decode(),
                "metadata_base64": {},
            }
            status, response = post(node or NODES[index % len(NODES)], "/v1/writers/append", payload)
            if status != 200:
                raise AssertionError(response)
            cursors.append(response["cursor"])

        for index in range(1, 7):
            append(index)
        before = placement(NODES[1], feed)
        if len(before["replicas"]) != 3 or before["owner"] not in before["replicas"]:
            raise AssertionError(before)
        # Rendezvous placement picks any three Nodes; the follower move
        # exercises whichever non-owner replica was placed and whichever
        # fourth Node stayed unassigned.
        removed_replica = next(replica for replica in before["replicas"] if replica != before["owner"])
        spares = [f"control-{index}" for index in range(1, 5) if f"control-{index}" not in before["replicas"]]
        if len(spares) != 1:
            raise AssertionError(before)
        replacement_replica = spares[0]
        target_service = f"node{replacement_replica.removeprefix('control-')}"
        expected_replicas = sorted(
            replacement_replica if replica == removed_replica else replica
            for replica in before["replicas"]
        )
        move = {
            "request_id": str(uuid.uuid4()),
            "feed": feed,
            "range_id": before["range_id"],
            "removed_replica": removed_replica,
            "replacement_replica": replacement_replica,
        }
        status, invalid = post(NODES[1], "/v1/admin/ranges/move-follower", {**move, "request_id": str(uuid.uuid4()), "replacement_replica": "control-5"})
        if status == 200 or placement(NODES[0], feed)["replicas"] != before["replicas"]:
            raise AssertionError(invalid)
        subprocess.run([*COMPOSE, "stop", target_service], check=True)
        # The stopped spare can be any Node, including a Control Plane voter;
        # probe through one that stays up and ride out any re-election before
        # expecting the refused move and unchanged placement.
        live = next(
            node
            for index, node in enumerate(NODES)
            if f"control-{index + 1}" != replacement_replica
        )
        deadline = time.time() + 45
        while True:
            try:
                status, unavailable = post(live, "/v1/admin/ranges/move-follower", move)
                probe_status, probe = post(
                    live, "/v1/admin/wcl", {"script": f"INSPECT PLACEMENT FOR FEED {feed};"}
                )
                if status == 200:
                    raise AssertionError(unavailable)
                if probe_status == 200:
                    if probe["results"][0]["data"]["replicas"] != before["replicas"]:
                        raise AssertionError(unavailable)
                    break
            except (ConnectionError, urllib.error.URLError, TimeoutError, http.client.RemoteDisconnected):
                pass
            if time.time() > deadline:
                raise AssertionError("Control Plane did not recover after stopping the spare Node")
            time.sleep(0.5)
        for _ in range(60):
            try:
                append(7, live)
                break
            except (AssertionError, ConnectionError, urllib.error.URLError, TimeoutError, http.client.RemoteDisconnected):
                time.sleep(0.5)
        else:
            raise AssertionError("append did not recover after stopping the spare Node")
        subprocess.run([*COMPOSE, "start", target_service], check=True)
        wait_for_fabric()
        for _ in range(20):
            status, moved = post(NODES[1], "/v1/admin/ranges/move-follower", move)
            if status == 200:
                break
            time.sleep(0.5)
        if status != 200 or moved.get("status") != "activated":
            raise AssertionError(moved)
        if sorted(moved["assignment"]["replicas"]) != expected_replicas:
            raise AssertionError(moved)
        status, repeated = post(NODES[2], "/v1/admin/ranges/move-follower", move)
        if status != 200 or repeated.get("status") != "activated":
            raise AssertionError(repeated)
        for index in range(8, 12):
            append(index)
        records = read(NODES[3], feed)
        if len(records) != 11 or [record["cursor"] for record in records] != cursors:
            raise AssertionError(records)
        after = read(NODES[3], feed, cursors[5])
        if [record["cursor"] for record in after] != cursors[6:]:
            raise AssertionError(after)
        subprocess.run([*COMPOSE, "restart", target_service], check=True)
        wait_for_fabric()
        resumed = None
        last_error = None
        for _ in range(30):
            try:
                resumed = read(NODES[3], feed, cursors[5])
                break
            except urllib.error.HTTPError as error:
                if error.code != 503:
                    raise
                last_error = error.read().decode()
                time.sleep(0.5)
        if resumed is None or [record["cursor"] for record in resumed] != cursors[6:]:
            raise AssertionError(f"replacement lost committed Cursor history after restart: {last_error}")
        print(json.dumps({"status": "ok", "feed": feed, "replicas": moved["assignment"]["replicas"], "records": len(records)}, indent=2))
    finally:
        subprocess.run([*COMPOSE, "down", "-v"], check=True)


if __name__ == "__main__":
    main()
