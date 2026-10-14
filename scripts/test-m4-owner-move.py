import base64
import http.client
import json
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

COMPOSE = ["docker", "compose", "-f", "compose.m4-owner.yml"]
NODES = [f"http://localhost:{port}" for port in range(7271, 7274)]
KEY = "whitewater-local-development-admin-key"


# Transient connection errors between the host and Docker-published ports are
# retried; HTTP statuses and assertion failures still propagate unchanged.
def _retried(operation):
    def wrapped(*args, **kwargs):
        last = None
        for _ in range(60):
            try:
                return operation(*args, **kwargs)
            except urllib.error.HTTPError:
                raise
            except (ConnectionError, urllib.error.URLError, TimeoutError) as error:
                last = error
                time.sleep(0.5)
        raise last

    return wrapped

@_retried
def post(node, path, payload):
    request = urllib.request.Request(
        node + path,
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


def get_json(node, path):
    request = urllib.request.Request(
        f"{node}{path}",
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
    raise AssertionError("isolated owner-movement Riverbed did not become healthy")


def main():
    subprocess.run([*COMPOSE, "down", "-v"], check=True)
    subprocess.run([*COMPOSE, "up", "--build", "-d"], check=True)
    try:
        wait_for_fabric()
        suffix = f"own{uuid.uuid4().hex[:12]}"
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

        def append(index, node):
            status, response = post(node, "/v1/writers/append", {
                "request_id": str(uuid.uuid4()),
                "writer": writer,
                "session_epoch": epoch,
                "event_time_ns": str(index),
                "key_base64": base64.b64encode(b"order-123").decode(),
                "payload_base64": base64.b64encode(f"event-{index}".encode()).decode(),
                "metadata_base64": {},
            })
            if status != 200:
                raise AssertionError(response)
            cursors.append(response["cursor"])

        for index in range(1, 7):
            append(index, NODES[index % 3])
        before = placement(NODES[1], feed)
        if len(before["replicas"]) != 3 or before["owner"] not in before["replicas"]:
            raise AssertionError(before)
        # Rendezvous placement picks any replica as owner; drive the move
        # relative to whichever one won instead of assuming control-1.
        old_owner = before["owner"]
        new_owner = next(replica for replica in before["replicas"] if replica != old_owner)
        target_service = f"node{new_owner.removeprefix('control-')}"
        move = {
            "request_id": str(uuid.uuid4()),
            "feed": feed,
            "range_id": before["range_id"],
            "new_owner": new_owner,
        }
        status, invalid = post(NODES[2], "/v1/admin/ranges/move-owner", {**move, "request_id": str(uuid.uuid4()), "new_owner": "control-4"})
        if status == 200 or placement(NODES[0], feed)["owner"] != old_owner:
            raise AssertionError(invalid)
        subprocess.run([*COMPOSE, "stop", target_service], check=True)
        # The stopped target can be any replica, including a Control Plane
        # voter; probe through one that stays up and ride out any re-election
        # before expecting the refused move and unchanged owner.
        live = next(
            node
            for index, node in enumerate(NODES)
            if f"control-{index + 1}" != new_owner
        )
        deadline = time.time() + 45
        while True:
            try:
                status, unavailable = post(live, "/v1/admin/ranges/move-owner", move)
                probe_status, probe = post(
                    live, "/v1/admin/wcl", {"script": f"INSPECT PLACEMENT FOR FEED {feed};"}
                )
                if status == 200:
                    raise AssertionError(unavailable)
                if probe_status == 200:
                    if probe["results"][0]["data"]["owner"] != old_owner:
                        raise AssertionError(unavailable)
                    break
            except (ConnectionError, urllib.error.URLError, TimeoutError, http.client.RemoteDisconnected):
                pass
            if time.time() > deadline:
                raise AssertionError("Control Plane did not recover after stopping the target Node")
            time.sleep(0.5)
        for _ in range(60):
            try:
                append(7, live)
                break
            except (AssertionError, ConnectionError, urllib.error.URLError, TimeoutError, http.client.RemoteDisconnected):
                time.sleep(0.5)
        else:
            raise AssertionError("append did not recover after stopping the target Node")
        subprocess.run([*COMPOSE, "start", target_service], check=True)
        wait_for_fabric()
        for _ in range(35):
            status, moved = post(NODES[2], "/v1/admin/ranges/move-owner", move)
            if status == 200:
                break
            time.sleep(0.5)
        if status != 200 or moved.get("status") != "activated":
            raise AssertionError(moved)
        if moved["assignment"]["owner"] != new_owner or moved["assignment"]["replicas"] != before["replicas"]:
            raise AssertionError(moved)
        status, repeated = post(NODES[0], "/v1/admin/ranges/move-owner", move)
        if status != 200 or repeated.get("status") != "activated":
            raise AssertionError(repeated)
        # Node-local move telemetry: an owner move verifies the staged
        # replica instead of copying, so the event records on its serving
        # Node; merge the Feed demand rows across all Nodes.
        merged = {}
        for node in NODES:
            body = get_json(node, "/v1/admin/demand/feeds")
            for row in body.get("feeds", []):
                entry = merged.setdefault(row["feed"], {})
                for field, value in row.items():
                    if field in ("feed", "domain"):
                        continue
                    entry[field] = entry.get(field, 0) + value
        if merged.get(feed, {}).get("moves", 0) < 1:
            raise AssertionError(f"moves {merged.get(feed, {})}")
        for index in range(8, 12):
            append(index, NODES[index % 3])
        for node in NODES:
            rows = read(node, feed)
            if [row["cursor"] for row in rows] != cursors:
                raise AssertionError(f"committed history differs on {node}: {rows}")
            if [row["cursor"] for row in read(node, feed, cursors[5])] != cursors[6:]:
                raise AssertionError(f"Cursor continuation differs on {node}")
        subprocess.run([*COMPOSE, "restart", target_service], check=True)
        wait_for_fabric()
        last_error = None
        for _ in range(30):
            try:
                if [row["cursor"] for row in read(NODES[1], feed, cursors[5])] != cursors[6:]:
                    raise AssertionError("committed Cursors changed after new owner restart")
                break
            except urllib.error.HTTPError as error:
                if error.code != 503:
                    raise
                last_error = error.read().decode()
                time.sleep(0.5)
        else:
            raise AssertionError(f"new owner did not recover committed history: {last_error}")
        print(json.dumps({"status": "ok", "feed": feed, "owner": new_owner, "records": len(cursors)}, indent=2))
    finally:
        subprocess.run([*COMPOSE, "down", "-v"], check=True)


if __name__ == "__main__":
    main()
