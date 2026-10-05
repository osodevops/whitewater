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
NODES = {f"control-{index}": f"http://localhost:{7170 + index}" for index in range(1, 5)}
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
        f"{node}/v1/feeds/records?{query}", headers={"authorization": f"Bearer {KEY}"},
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        return error.code, json.load(error)


def placement(node, feed):
    status, response = post(node, "/v1/admin/wcl", {"script": f"INSPECT PLACEMENT FOR FEED {feed};"})
    if status != 200:
        raise AssertionError(response)
    return response["results"][0]["data"]


def wait_for_fabric():
    for _ in range(90):
        try:
            if all(urllib.request.urlopen(f"{node}/health", timeout=2).status == 200 for node in NODES.values()):
                return
        except (urllib.error.URLError, TimeoutError, http.client.RemoteDisconnected):
            pass
        time.sleep(1)
    raise AssertionError("isolated four-Node Fabric did not become healthy")


def main():
    subprocess.run([*COMPOSE, "up", "--build", "-d"], check=True)
    stopped = None
    try:
        wait_for_fabric()
        suffix = f"read{uuid.uuid4().hex[:12]}"
        feed, writer = f"{suffix}.events", f"writer{suffix}"
        script = f"CREATE SPACE {suffix}; CREATE FEED {feed}; CREATE WRITER {writer} TO {feed}; OPEN WRITER SESSION {writer};"
        for _ in range(30):
            status, created = post(NODES["control-1"], "/v1/admin/wcl", {"script": script})
            if status == 200:
                break
            time.sleep(1)
        if status != 200:
            raise AssertionError(created)
        epoch = created["results"][-1]["data"]["session_epoch"]
        cursors = []
        for index in range(1, 33):
            status, result = post(NODES[f"control-{index % 3 + 1}"], "/v1/writers/append", {
                "request_id": str(uuid.uuid4()), "writer": writer, "session_epoch": epoch,
                "event_time_ns": str(index),
                "key_base64": base64.b64encode(f"key-{index % 11}".encode()).decode(),
                "payload_base64": base64.b64encode(f"event-{index}".encode()).decode(),
                "metadata_base64": {},
            })
            if status != 200:
                raise AssertionError(result)
            cursors.append(result["cursor"])
        status, split = post(NODES["control-1"], "/v1/admin/ranges/split", {
            "request_id": str(uuid.uuid4()), "feed": feed,
            "split_at": "80000000000000000000000000000000", "batch_size": 2,
        })
        if status != 200 or split.get("status") != "activated":
            raise AssertionError(split)
        ranges = placement(NODES["control-2"], feed)["range_assignments"]
        if len(ranges) != 2 or any("control-4" in item["replicas"] for item in ranges):
            raise AssertionError(ranges)
        source, other = ranges
        removed = next((node for node in source["replicas"] if node != source["owner"] and node in other["replicas"]), None)
        if removed is None:
            raise AssertionError(ranges)
        status, moved = post(NODES["control-2"], "/v1/admin/ranges/move-follower", {
            "request_id": str(uuid.uuid4()), "feed": feed,
            "range_id": source["range_id"], "removed_replica": removed,
            "replacement_replica": "control-4",
        })
        if status != 200 or moved.get("status") != "activated":
            raise AssertionError(moved)
        ingress = NODES[removed]
        status, records = read(ingress, feed)
        if status != 200 or [row["cursor"] for row in records] != cursors:
            raise AssertionError(f"incomplete cross-Node Feed read on {removed}: {records}")
        status, resumed = read(ingress, feed, cursors[10])
        if status != 200 or [row["cursor"] for row in resumed] != cursors[11:]:
            raise AssertionError(f"cross-Node Cursor continuation failed: {resumed}")
        status, unknown = read(ingress, feed, "not-a-feed-cursor")
        if status != 400:
            raise AssertionError(f"unknown Cursor was not rejected: {unknown}")
        status, reader = post(ingress, "/v1/admin/wcl", {
            "script": f"CREATE READER reader{suffix} FROM {feed} START AT BEGINNING;"
        })
        if status != 200:
            raise AssertionError(reader)
        status, opened = post(ingress, "/v1/readers/open", {
            "request_id": str(uuid.uuid4()), "reader": f"reader{suffix}", "capacity": 7,
        })
        if status != 200:
            raise AssertionError(opened)
        read_cursors = []
        delivered = None
        for _ in range(6):
            status, fetched = post(ingress, "/v1/readers/fetch", {
                "request_id": str(uuid.uuid4()), "reader": f"reader{suffix}",
                "session_epoch": opened["session_epoch"], "limit": 7,
            })
            if status != 200 or not fetched["records"]:
                raise AssertionError(f"Reader stopped before complete Feed: {fetched}")
            read_cursors.extend(row["cursor"] for row in fetched["records"])
            delivered = fetched["delivered_cursor"]
            if not delivered.startswith("rf1_") or delivered == fetched["records"][-1]["cursor"]:
                raise AssertionError(f"Reader progress is not an opaque frontier: {fetched}")
            status, ack = post(ingress, "/v1/readers/ack", {
                "request_id": str(uuid.uuid4()), "reader": f"reader{suffix}",
                "session_epoch": opened["session_epoch"], "cursor": delivered,
            })
            if status != 200 or ack["acknowledged_cursor"] != delivered:
                raise AssertionError(f"Reader frontier acknowledgement failed: {ack}")
            if len(read_cursors) >= len(cursors):
                break
        if read_cursors != cursors:
            raise AssertionError(f"Reader omitted or repeated a range: {read_cursors}")
        reopened_ingress = NODES["control-3" if removed != "control-3" else "control-2"]
        status, reopened = post(reopened_ingress, "/v1/readers/open", {
            "request_id": str(uuid.uuid4()), "reader": f"reader{suffix}", "capacity": 7,
        })
        if status != 200 or reopened["delivered_cursor"] != delivered:
            raise AssertionError(f"Reader lost acknowledged frontier on reopen: {reopened}")
        status, empty = post(reopened_ingress, "/v1/readers/fetch", {
            "request_id": str(uuid.uuid4()), "reader": f"reader{suffix}",
            "session_epoch": reopened["session_epoch"], "limit": 7,
        })
        if status != 200 or empty["records"] or empty["delivered_cursor"] != delivered:
            raise AssertionError(f"Reader redelivered acknowledged history: {empty}")
        second_name = f"readersecond{suffix}"
        status, second_created = post(ingress, "/v1/admin/wcl", {
            "script": f"CREATE READER {second_name} FROM {feed} START AT BEGINNING;"
        })
        if status != 200:
            raise AssertionError(second_created)
        status, second_open = post(ingress, "/v1/readers/open", {
            "request_id": str(uuid.uuid4()), "reader": second_name, "capacity": 5,
        })
        if status != 200:
            raise AssertionError(second_open)
        status, second_first = post(ingress, "/v1/readers/fetch", {
            "request_id": str(uuid.uuid4()), "reader": second_name,
            "session_epoch": second_open["session_epoch"], "limit": 5,
        })
        if status != 200 or [row["cursor"] for row in second_first["records"]] != cursors[:5]:
            raise AssertionError(f"independent Reader missed initial history: {second_first}")
        if second_first["delivered_cursor"] == delivered:
            raise AssertionError("independent Readers shared a progress token")
        status, second_ack = post(ingress, "/v1/readers/ack", {
            "request_id": str(uuid.uuid4()), "reader": second_name,
            "session_epoch": second_open["session_epoch"], "cursor": second_first["delivered_cursor"],
        })
        if status != 200 or second_ack["acknowledged_cursor"] != second_first["delivered_cursor"]:
            raise AssertionError(f"second Reader acknowledgement affected another Reader: {second_ack}")
        status, second_next = post(reopened_ingress, "/v1/readers/fetch", {
            "request_id": str(uuid.uuid4()), "reader": second_name,
            "session_epoch": second_open["session_epoch"], "limit": 5,
        })
        if status != 200 or [row["cursor"] for row in second_next["records"]] != cursors[5:10]:
            raise AssertionError(f"second Reader did not continue independently: {second_next}")
        status, temporary = post(ingress, "/v1/readers/temporary/fetch", {
            "feed": feed, "after": cursors[10], "limit": 100,
        })
        if status != 200 or [row["cursor"] for row in temporary["records"]] != cursors[11:]:
            raise AssertionError(f"temporary Reader lost cross-Node Cursor: {temporary}")
        status, new_only = post(ingress, "/v1/readers/temporary/fetch", {
            "feed": feed, "new_only": True,
        })
        if status != 200 or new_only["next_cursor"] != cursors[-1] or new_only["records"]:
            raise AssertionError(f"temporary Reader found an incomplete tail: {new_only}")
        stopped = f"node{source['owner'].removeprefix('control-')}"
        subprocess.run([*COMPOSE, "stop", stopped], check=True)
        status, degraded = read(ingress, feed)
        if status == 200 and [row["cursor"] for row in degraded] != cursors:
            raise AssertionError(f"read returned partial Feed during owner failure: {degraded}")
        if status not in (200, 503):
            raise AssertionError(f"owner failure was not retriable: {degraded}")
        print(json.dumps({"status": "ok", "feed": feed, "partial_ingress": removed,
                          "remote_range_owner": source["owner"], "records": len(cursors)}, indent=2))
    finally:
        if stopped:
            subprocess.run([*COMPOSE, "start", stopped], check=True)
        subprocess.run([*COMPOSE, "stop"], check=True)


if __name__ == "__main__":
    main()
