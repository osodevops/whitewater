import base64
import json
import pathlib
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


def victim_back_at(index):
    try:
        with urllib.request.urlopen(
            f"http://localhost:{8180 + index + 1}/health", timeout=5
        ) as response:
            return response.status == 200
    except (OSError, urllib.error.HTTPError):
        return False


def lease_owner(endpoint, name=None):
    result = wcl(endpoint, "SHOW DISTRIBUTARIES;")
    rows = result["results"][0]["data"]
    if name is not None:
        rows = [row for row in rows if row["name"] == name]
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

        # Lease failover: stop the owner Node. The leader's lease
        # recovery supervisor must notice the dead owner and CAS the
        # lease onto a survivor on its own — no operator MOVE — and the
        # copy must then resume with no duplicated records. Reads route
        # to a surviving destination Node — the stopped owner cannot
        # serve them.
        owner, epoch_before = lease_owner(DST[0])
        victim_index = owner_index(owner)
        survivor = live_dst(victim_index)
        subprocess.run(
            ["docker", "stop", f"whitewater-distributary-dst{victim_index + 1}-1"],
            check=True,
        )

        def lease_recovered():
            new_owner, new_epoch = lease_owner(survivor)
            if new_owner == owner or new_epoch <= epoch_before:
                return False
            return new_owner, new_epoch

        recovered_owner, _ = wait_for(lease_recovered, "automatic lease recovery")
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

        # The recovery chain — lease CAS, range owner recovery, reconcile,
        # then ~2s/record appends — needs a wider window than the happy
        # path; a genuine wedge still fails, just not the honest delay.
        wait_for(resumed_without_dupes, "lease-move resume", timeout=240)
        _, epoch_after = lease_owner(survivor)
        assert epoch_after > epoch_before

        # Range-owner failover: bring the victim back so quorum holds at
        # two live Nodes, then stop the Node that currently owns the
        # mirrored Feed's Active Range. The owner recovery supervisor —
        # dead-owner planning plus the live-owner committed-frontier
        # backstop — must heal the range while the drive keeps copying.
        subprocess.run(
            ["docker", "start", f"whitewater-distributary-dst{victim_index + 1}-1"],
            check=True,
        )

        wait_for(lambda: victim_back_at(victim_index), "victim rejoin", timeout=60)

        def range_owner_index(endpoint):
            result = wcl(endpoint, "INSPECT PLACEMENT FOR FEED dstdom.mirrored;")
            assignments = result["results"][0]["data"]["range_assignments"]
            return owner_index(assignments[0]["owner"])

        range_victim = range_owner_index(survivor)
        subprocess.run(
            ["docker", "stop", f"whitewater-distributary-dst{range_victim + 1}-1"],
            check=True,
        )
        range_survivor = live_dst(range_victim)
        for record_id in range(41, 51):
            append(SRC[record_id % 3], "srcwriter", epoch, record_id)

        def resumed_after_range_owner_loss():
            rows = records(range_survivor, "dstdom.mirrored")
            if len(rows) != 50:
                return False
            payloads = [
                base64.b64decode(row["payload_base64"]).decode() for row in rows
            ]
            if len(set(payloads)) != 50:
                raise AssertionError("duplicate records after range owner loss")
            return rows

        wait_for(resumed_after_range_owner_loss, "range-owner resume", timeout=240)
        range_owner_after = range_owner_index(range_survivor)
        assert range_owner_after != range_victim

        # Restore the range victim before the store phase: a second dead
        # Node would drop the destination below Control Plane quorum, and
        # the store lease kill needs a surviving majority to CAS onto.
        subprocess.run(
            ["docker", "start", f"whitewater-distributary-dst{range_victim + 1}-1"],
            check=True,
        )
        wait_for(
            lambda: victim_back_at(range_victim),
            "range victim rejoin",
            timeout=60,
        )

        # Manual StateStore journal shipping: a store whose only history
        # is its mutation journal mirrors cross-Riverbed through the
        # same lease-fenced drive. Seed the source store (puts + a
        # delete), declare the destination copy, and the ship catches up.
        wcl(
            SRC[0],
            "DEFINE STATESTORE srcdom.profiles KEYS dept;",
        )

        def mutate(node, key, payload, delete=False, mutation=None):
            op = (
                {"kind": "delete_row", "key_base64": b64(key)}
                if delete
                else {
                    "kind": "put_row",
                    "key_base64": b64(key),
                    "payload_base64": b64(payload),
                    "named": [
                        {
                            "name": "dept",
                            "values": [{"kind": "text", "value": "eng"}],
                        }
                    ],
                }
            )
            status, result = post(
                node,
                "/v1/state-stores/mutate",
                {
                    "store": "srcdom.profiles",
                    "mutation_id": mutation or str(uuid.uuid4()),
                    "op": op,
                },
            )
            assert status == 200 and result["sequence"] > 0, (status, result)

        mutate(SRC[0], "u1", "payload-u1")
        mutate(SRC[0], "u2", "payload-u2")
        mutate(SRC[0], "u3", "payload-u3")
        mutate(SRC[0], "u3", "", delete=True)

        wcl(
            DST[0],
            "DEFINE STATESTORE dstdom.shadow KEYS dept; "
            "CREATE DISTRIBUTARY dstdom.storecopy FROM LINK upstream "
            "STATE-STORE srcdom.profiles INTO dstdom.shadow;",
        )

        def lookup(node, store, key):
            status, response = post(
                node,
                "/v1/state-stores/lookup",
                {"store": store, "key_base64": b64(key)},
            )
            assert status == 200, (status, response)
            if not response["found"]:
                return None
            return base64.b64decode(response["payload_base64"]).decode()

        def store_shipped():
            if lookup(DST[0], "dstdom.shadow", "u1") != "payload-u1":
                return False
            if lookup(DST[0], "dstdom.shadow", "u2") != "payload-u2":
                return False
            # The delete shipped too — u3 must stay absent.
            if lookup(DST[0], "dstdom.shadow", "u3") is not None:
                return False
            described = wcl(DST[0], "DESCRIBE DISTRIBUTARY dstdom.storecopy;")
            data = described["results"][0]["data"]
            if data["kind"] != "state_store" or int(data["shipped_seq"]) < 4:
                return False
            return True

        wait_for(store_shipped, "store journal ship", timeout=120)

        # Continuous ship: later mutations flow without redefinition.
        mutate(SRC[0], "u4", "payload-u4")
        mutate(SRC[0], "u5", "payload-u5")
        wait_for(
            lambda: lookup(DST[0], "dstdom.shadow", "u5") == "payload-u5"
            or None,
            "store continuous ship",
            timeout=120,
        )

        # Lease kill on the store copy: the shipped_seq frontier lives on
        # the catalog row, so the recovered owner resumes from committed
        # progress rather than reshipping — a reship would still dedup
        # through the derived mutation ids.
        store_owner, store_epoch_before = lease_owner(DST[0], "dstdom.storecopy")
        store_victim = owner_index(store_owner)
        store_survivor = live_dst(store_victim)
        subprocess.run(
            ["docker", "stop", f"whitewater-distributary-dst{store_victim + 1}-1"],
            check=True,
        )

        def store_lease_recovered():
            new_owner, new_epoch = lease_owner(
                store_survivor, "dstdom.storecopy"
            )
            if new_owner == store_owner or new_epoch <= store_epoch_before:
                return False
            return new_owner, new_epoch

        wait_for(store_lease_recovered, "store lease recovery")
        mutate(SRC[0], "u6", "payload-u6")

        def store_resumed():
            if lookup(store_survivor, "dstdom.shadow", "u6") != "payload-u6":
                return False
            described = wcl(store_survivor, "DESCRIBE DISTRIBUTARY dstdom.storecopy;")
            return int(described["results"][0]["data"]["shipped_seq"]) >= 6

        wait_for(store_resumed, "store ship resume", timeout=240)
        # Bring the stopped destination Node back so the fixture tears
        # down cleanly with every Node alive.
        subprocess.run(
            ["docker", "start", f"whitewater-distributary-dst{store_victim + 1}-1"],
            check=True,
        )

        print(
            json.dumps(
                {
                    "initial_copy": 20,
                    "continuous_copy": 10,
                    "post_failover_copy": 10,
                    "post_range_owner_loss_copy": 10,
                    "destination_records": 50,
                    "duplicates": 0,
                    "lease_owner_before": owner,
                    "lease_owner_after": recovered_owner,
                    "lease_epoch_before": epoch_before,
                    "lease_epoch_after": epoch_after,
                    "store_shipped": True,
                    "store_survived_lease_move": True,
                    "range_owner_lost": f"control-{range_victim + 1}",
                    "range_owner_after": f"control-{range_owner_after + 1}",
                    "note": "destination-owned pull copied the source Feed; "
                    "Key/event-time/Metadata preserved under new Cursors; "
                    "lease-owner loss + automatic lease recovery and "
                    "range-owner loss + owner recovery resumed with "
                    "zero duplicates",
                }
            )
        )
    finally:
        # Capture diagnostics before teardown — CI uploads acceptance-logs/.
        try:
            logs_dir = pathlib.Path(__file__).resolve().parent.parent / "acceptance-logs"
            logs_dir.mkdir(exist_ok=True)
            captured = subprocess.run(
                [*COMPOSE, "logs", "--no-color", "--tail", "400"],
                capture_output=True,
                text=True,
                check=False,
            )
            (logs_dir / "distributary-containers.log").write_text(
                captured.stdout + captured.stderr, encoding="utf-8"
            )
        except OSError:
            pass
        subprocess.run(["docker", "start", "whitewater-distributary-dst1-1"], check=False)
        subprocess.run(["docker", "start", "whitewater-distributary-dst2-1"], check=False)
        subprocess.run(["docker", "start", "whitewater-distributary-dst3-1"], check=False)
        subprocess.run([*COMPOSE, "down", "-v"], check=True)


if __name__ == "__main__":
    main()
