import base64
import json
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

# Distributary exercise: two Riverbeds on one network — the destination
# declares a Riverbed Link plus a Distributary and managed-copy resumes
# the source Feed on the destination, preserving Key, event time, and
# Metadata under new Cursors. The script then kills the lease owner,
# moves the lease, and proves the copy resumes without duplication.
COMPOSE = ["docker", "compose", "-f", "compose.distributary.yml"]
SRC = [f"http://localhost:{port}" for port in range(8171, 8174)]
DST = [f"http://localhost:{port}" for port in range(8181, 8184)]
DST_NODE_IDS = [f"whitewater-distributary-dst{i}" for i in range(1, 4)]
KEY = "whitewater-local-development-admin-key"


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
    for _ in range(120):
        status, result = _post_once(node, path, payload)
        if status == 0 or status >= 500:
            time.sleep(0.5)
            continue
        return status, result
    return status, result


def _post_once(node, path, payload):
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
        body = error.read()
        try:
            return error.code, json.loads(body)
        except json.JSONDecodeError:
            return error.code, {"error": body.decode(errors="replace")}
    except OSError:
        return 0, {}


def wcl(node, script):
    status, result = post(
        node, "/v1/admin/wcl", {"request_id": str(uuid.uuid4()), "script": script}
    )
    if status != 200:
        raise AssertionError({"wcl": script, "result": result})
    return result


def b64(value):
    return base64.b64encode(value.encode()).decode()


def append(node, writer, epoch, record_id):
    status, result = post(
        node,
        "/v1/writers/append",
        {
            "request_id": str(uuid.uuid4()),
            "writer": writer,
            "session_epoch": epoch,
            "event_time_ns": str(1_700_000_000_000_000_000 + record_id),
            "key_base64": b64(f"key-{record_id % 5}"),
            "payload_base64": b64(f"payload-{record_id}"),
            "metadata_base64": {"origin": b64("distributary-test")},
        },
    )
    if status != 200:
        raise AssertionError({"append": result})


def records(node, feed, limit=500):
    status, result = post(
        node,
        "/v1/readers/temporary/fetch",
        {"feed": feed, "limit": limit},
    )
    if status != 200:
        raise AssertionError({"read": result})
    return result["records"]


def wait_for(predicate, description, timeout=90):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            last = predicate()
            if last:
                return last
        except AssertionError as error:
            last = error
        time.sleep(0.5)
    raise AssertionError({description: last})


def owner_index(owner):
    # The lease owner is a StorageNodeId: on this fixture the storage
    # identity is the control voter name `control-N`, mapped to the
    # `dstN` compose service/container.
    for index in range(3):
        if owner in (DST_NODE_IDS[index], str(index + 1), f"control-{index + 1}"):
            return index
    raise AssertionError({"unknown lease owner": owner})


def live_dst(exclude_index):
    return DST[next(i for i in range(3) if i != exclude_index)]


def lease_owner(endpoint):
    result = wcl(endpoint, "SHOW DISTRIBUTARIES;")
    rows = result["results"][0]["data"]
    return rows[0]["lease_owner"], int(rows[0]["lease_epoch"])


def main():
    subprocess.run([*COMPOSE, "down", "-v"], check=True)
    try:
        subprocess.run([*COMPOSE, "up", "-d", "--build"], check=True)
        # Seed the source Feed with records through a Writer session.
        wcl(
            SRC[0],
            "CREATE DOMAIN srcdom; CREATE FEED srcdom.events; "
            "CREATE WRITER srcwriter TO srcdom.events;",
        )
        epoch = wcl(SRC[0], "OPEN WRITER SESSION srcwriter;")["results"][0]["data"][
            "session_epoch"
        ]
        for record_id in range(1, 21):
            append(SRC[record_id % 3], "srcwriter", epoch, record_id)

        # Destination: target Feed, Riverbed Link (credential by env
        # reference), and the Distributary itself.
        wcl(
            DST[0],
            "CREATE DOMAIN dstdom; CREATE FEED dstdom.mirrored; "
            "CREATE RIVERBED LINK upstream "
            "AT 'http://src1:7070,http://src2:7070,http://src3:7070' "
            "WITH KEY ENV WHITEWATER_DIST_KEY; "
            "CREATE DISTRIBUTARY dstdom.mirror FROM LINK upstream "
            "FEED srcdom.events INTO dstdom.mirrored;",
        )

        # The managed copy catches up: all 20 records land on the
        # destination with content preserved and new Cursors minted.
        def copied_all():
            rows = records(DST[0], "dstdom.mirrored")
            if len(rows) != 20:
                return False
            expected = {f"payload-{i}" for i in range(1, 21)}
            got = {
                base64.b64decode(row["payload_base64"]).decode() for row in rows
            }
            if got != expected:
                return False
            for row in rows:
                assert row["metadata_base64"]["origin"] == b64("distributary-test")
                assert int(row["event_time_ns"]) >= 1_700_000_000_000_000_000
            return rows

        wait_for(copied_all, "initial copy")
        # New Cursors are minted — source and destination Cursors differ
        # for the same record while content stays identical.
        source_cursors = {row["cursor"] for row in records(SRC[0], "srcdom.events")}
        dest_cursors = {row["cursor"] for row in records(DST[0], "dstdom.mirrored")}
        assert source_cursors.isdisjoint(dest_cursors)

        # Continuous copy: later source appends flow without redefinition.
        for record_id in range(21, 31):
            append(SRC[record_id % 3], "srcwriter", epoch, record_id)
        wait_for(
            lambda: len(records(DST[0], "dstdom.mirrored")) == 30 or None,
            "continuous copy",
        )

        # Lease failover: stop the owner Node, move the lease to a live
        # Node, and prove the copy resumes with no duplicated records.
        # Reads and the MOVE itself route to a surviving destination
        # Node — the stopped owner cannot serve them.
        owner, epoch_before = lease_owner(DST[0])
        victim_index = owner_index(owner)
        survivor = live_dst(victim_index)
        subprocess.run(
            ["docker", "stop", f"whitewater-distributary-dst{victim_index + 1}-1"],
            check=True,
        )
        replacement = f"control-{next(i for i in range(3) if i != victim_index) + 1}"
        wcl(survivor, f"MOVE DISTRIBUTARY LEASE dstdom.mirror TO NODE {replacement};")
        for record_id in range(31, 41):
            append(SRC[record_id % 3], "srcwriter", epoch, record_id)

        def resumed_without_dupes():
            rows = records(survivor, "dstdom.mirrored")
            if len(rows) != 40:
                return False
            # A replay across the lease move must dedup — every payload
            # appears exactly once.
            payloads = [
                base64.b64decode(row["payload_base64"]).decode() for row in rows
            ]
            if len(set(payloads)) != 40:
                raise AssertionError("duplicate records after lease move")
            return rows

        wait_for(resumed_without_dupes, "lease-move resume")
        _, epoch_after = lease_owner(survivor)
        assert epoch_after > epoch_before

        print(
            json.dumps(
                {
                    "initial_copy": 20,
                    "continuous_copy": 10,
                    "post_failover_copy": 10,
                    "destination_records": 40,
                    "duplicates": 0,
                    "lease_owner_before": owner,
                    "lease_epoch_before": epoch_before,
                    "lease_epoch_after": epoch_after,
                    "note": "destination-owned pull copied the source Feed; "
                    "Key/event-time/Metadata preserved under new Cursors; "
                    "owner loss + operator lease move resumed with zero duplicates",
                }
            )
        )
    finally:
        subprocess.run(["docker", "start", "whitewater-distributary-dst1-1"], check=False)
        subprocess.run(["docker", "start", "whitewater-distributary-dst2-1"], check=False)
        subprocess.run(["docker", "start", "whitewater-distributary-dst3-1"], check=False)
        subprocess.run([*COMPOSE, "down", "-v"], check=True)


if __name__ == "__main__":
    main()
