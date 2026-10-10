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
DRAINED = "control-4"


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


def wcl(node, script):
    status, response = post(node, "/v1/admin/wcl", {"script": script})
    if status != 200:
        raise AssertionError({"script": script, "response": response})
    return [result["data"] for result in response["results"]]


def wcl_retry(node, script, attempts=30, delay=1):
    last = None
    for _ in range(attempts):
        try:
            return wcl(node, script)
        except AssertionError as error:
            last = error
            time.sleep(delay)
    raise last


def placement(node, feed):
    return wcl_retry(node, f"INSPECT PLACEMENT FOR FEED {feed};")[0]


def read(node, feed):
    query = urllib.parse.urlencode({"feed": feed, "limit": 100})
    request = urllib.request.Request(
        f"{node}/v1/feeds/records?{query}",
        headers={"authorization": f"Bearer {KEY}"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def wait_for_fabric():
    for _ in range(90):
        try:
            if all(urllib.request.urlopen(f"{node}/health", timeout=2).status == 200 for node in NODES):
                return
        except (urllib.error.URLError, TimeoutError, http.client.RemoteDisconnected):
            pass
        time.sleep(1)
    raise AssertionError("four-Node Riverbed did not become healthy")


def main():
    # Lifecycle flags persist in catalog volumes; a prior aborted run could
    # leave Nodes marked draining, so start from clean volumes.
    subprocess.run([*COMPOSE, "down", "-v"], check=True)
    subprocess.run([*COMPOSE, "up", "--build", "-d"], check=True)
    try:
        wait_for_fabric()
        suffix = f"dr{uuid.uuid4().hex[:12]}"
        feeds = [f"{suffix}.events{index}" for index in range(5)]
        subscription = f"{suffix}.billing"
        script = "; ".join(
            [f"CREATE SPACE {suffix}"]
            + [f"CREATE FEED {feed}" for feed in feeds]
            + [f"CREATE SUBSCRIPTION {subscription} FROM {feeds[0]}"]
        ) + ";"
        for _ in range(30):
            status, _ = post(NODES[0], "/v1/admin/wcl", {"script": script})
            if status == 200:
                break
            time.sleep(1)
        if status != 200:
            raise AssertionError("Riverbed setup failed")

        # Seed a committed record so copied replicas carry real data.
        for _ in range(30):
            status, appended = post(
                NODES[0],
                "/v1/feeds/append",
                {
                    "request_id": str(uuid.uuid4()),
                    "feed": feeds[0],
                    "writer_session_id": str(uuid.uuid4()),
                    "writer_epoch": 1,
                    "sequence": 1,
                    "event_time_ns": "1",
                    "key_base64": "b3JkZXItMQ==",
                    "payload_base64": "ZXZlbnQtMQ==",
                },
            )
            if status == 200:
                break
            time.sleep(1)
        if status != 200:
            raise AssertionError(appended)

        # The drained Node must actually hold something, and a survivor must
        # remain for every reference; with five Feeds and RF3 over four Nodes
        # every Node is almost surely placed somewhere.
        held = {
            feed: placement(NODES[0], feed)["replicas"]
            for feed in feeds
        }
        if not any(DRAINED in replicas for replicas in held.values()):
            raise AssertionError(f"{DRAINED} holds no Feed replicas: {held}")

        wcl_retry(NODES[0], f"DRAIN STORAGE NODE {DRAINED};")

        # The leader-side supervisor ticks every 5s and drives each planned
        # move through the admin move endpoints; poll the explainable plan.
        deadline = time.time() + 300
        plan = None
        while time.time() < deadline:
            try:
                plan = wcl(NODES[0], f"INSPECT DRAIN FOR STORAGE NODE {DRAINED};")[0]
            except AssertionError:
                plan = None
            if plan and plan["ready_to_retire"] and not plan["moves"]:
                break
            time.sleep(5)
        else:
            raise AssertionError(f"drain did not complete: {plan}")
        if plan["unplannable"]:
            raise AssertionError(f"drain reported unplannable references: {plan}")

        for feed, replicas in held.items():
            current = placement(NODES[0], feed)
            if DRAINED in current["replicas"] or current["owner"] == DRAINED:
                raise AssertionError({"feed": feed, "was": replicas, "now": current})

        wcl_retry(NODES[0], f"RETIRE STORAGE NODE {DRAINED};")
        nodes = wcl_retry(NODES[0], "SHOW STORAGE NODES;")[0]
        retired = next(item for item in nodes if item["node"] == DRAINED)
        if retired["eligible"] or retired["draining"]:
            raise AssertionError(retired)

        # Writes and reads keep flowing with the retired Node gone.
        status, appended = post(
            NODES[0],
            "/v1/feeds/append",
            {
                "request_id": str(uuid.uuid4()),
                "feed": feeds[0],
                "writer_session_id": str(uuid.uuid4()),
                "writer_epoch": 1,
                "sequence": 1,
                "event_time_ns": "2",
                "key_base64": "b3JkZXItMg==",
                "payload_base64": "ZXZlbnQtMg==",
            },
        )
        if status != 200:
            raise AssertionError(appended)
        records = read(NODES[0], feeds[0])
        if len(records) != 2:
            raise AssertionError(records)

        print(json.dumps({"status": "ok", "drained": DRAINED, "feeds": held, "plan": plan}, indent=2))
    finally:
        subprocess.run([*COMPOSE, "down", "-v"], check=True)


if __name__ == "__main__":
    main()
