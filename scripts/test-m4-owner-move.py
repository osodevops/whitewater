import base64
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
        except (urllib.error.URLError, TimeoutError):
            pass
        time.sleep(1)
    raise AssertionError("isolated owner-movement Fabric did not become healthy")


def main():
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
        if before["owner"] != "control-1" or before["replicas"] != ["control-1", "control-2", "control-3"]:
            raise AssertionError(before)
        move = {
            "request_id": str(uuid.uuid4()),
            "feed": feed,
            "range_id": before["range_id"],
            "new_owner": "control-2",
        }
        status, invalid = post(NODES[2], "/v1/admin/ranges/move-owner", {**move, "request_id": str(uuid.uuid4()), "new_owner": "control-4"})
        if status == 200 or placement(NODES[0], feed)["owner"] != "control-1":
            raise AssertionError(invalid)
        subprocess.run([*COMPOSE, "stop", "node2"], check=True)
        status, unavailable = post(NODES[2], "/v1/admin/ranges/move-owner", move)
        if status == 200 or placement(NODES[0], feed)["owner"] != "control-1":
            raise AssertionError(unavailable)
        append(7, NODES[0])
        subprocess.run([*COMPOSE, "start", "node2"], check=True)
        wait_for_fabric()
        for _ in range(35):
            status, moved = post(NODES[2], "/v1/admin/ranges/move-owner", move)
            if status == 200:
                break
            time.sleep(0.5)
        if status != 200 or moved.get("status") != "activated":
            raise AssertionError(moved)
        if moved["assignment"]["owner"] != "control-2" or moved["assignment"]["replicas"] != before["replicas"]:
            raise AssertionError(moved)
        status, repeated = post(NODES[0], "/v1/admin/ranges/move-owner", move)
        if status != 200 or repeated.get("status") != "activated":
            raise AssertionError(repeated)
        for index in range(8, 12):
            append(index, NODES[index % 3])
        for node in NODES:
            rows = read(node, feed)
            if [row["cursor"] for row in rows] != cursors:
                raise AssertionError(f"committed history differs on {node}: {rows}")
            if [row["cursor"] for row in read(node, feed, cursors[5])] != cursors[6:]:
                raise AssertionError(f"Cursor continuation differs on {node}")
        subprocess.run([*COMPOSE, "restart", "node2"], check=True)
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
        print(json.dumps({"status": "ok", "feed": feed, "owner": "control-2", "records": len(cursors)}, indent=2))
    finally:
        subprocess.run([*COMPOSE, "stop"], check=True)


if __name__ == "__main__":
    main()
