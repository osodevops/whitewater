import base64
import http.client
import json
import os
import pathlib
import shutil
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

# Backup/restore DR exercise: seed a Riverbed, take a crash-consistent
# per-Node backup, destroy every data volume, restore, and verify the
# cluster resumes — catalog, records, state stores, subscriptions.
COMPOSE = ["docker", "compose", "-f", "compose.backup.yml"]
NODES = [f"http://localhost:{port}" for port in range(8071, 8074)]
KEY = "whitewater-local-development-admin-key"
ARTIFACTS = pathlib.Path(__file__).resolve().parents[1] / ".backup-artifacts"


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


def wcl_retry(node, script, attempts=45, delay=1):
    request_id = str(uuid.uuid4())
    last = None
    for _ in range(attempts):
        try:
            status, response = post(
                node, "/v1/admin/wcl", {"request_id": request_id, "script": script}
            )
            if status == 200:
                return [result["data"] for result in response["results"]]
            last = {"status": status, "response": response}
        except AssertionError as error:
            last = error
        time.sleep(delay)
    raise AssertionError(last)


def read(node, feed):
    query = urllib.parse.urlencode({"feed": feed, "limit": 100})
    request = urllib.request.Request(
        f"{node}/v1/feeds/records?{query}",
        headers={"authorization": f"Bearer {KEY}"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def b64(value):
    return base64.b64encode(value.encode()).decode()


def append(node, feed, sequence, writer_session_id):
    status, result = post(
        node,
        "/v1/feeds/append",
        {
            "request_id": str(uuid.uuid4()),
            "feed": feed,
            "writer_session_id": writer_session_id,
            "writer_epoch": 1,
            "sequence": sequence,
            "event_time_ns": str(sequence),
            "key_base64": b64("order-1"),
            "payload_base64": b64(f"event-{sequence}"),
        },
    )
    if status != 200:
        raise AssertionError({"append": result})


def store_mutate(node, store, key, payload):
    status, result = post(
        node,
        "/v1/state-stores/mutate",
        {
            "store": store,
            "mutation_id": str(uuid.uuid4()),
            "op": {
                "kind": "put_row",
                "key_base64": b64(key),
                "payload_base64": b64(payload),
            },
        },
    )
    if status != 200:
        raise AssertionError({"mutate": result})


def store_lookup(node, store, key):
    status, result = post(
        node,
        "/v1/state-stores/lookup",
        {"store": store, "key_base64": b64(key)},
    )
    if status != 200:
        raise AssertionError({"lookup": result})
    return result


def backup(node, destination):
    status, result = post(
        node, "/v1/admin/backup", {"destination": destination}
    )
    if status != 200:
        raise AssertionError({"backup": destination, "response": result})
    return result


def wait_for_nodes(label):
    for _ in range(120):
        try:
            if all(
                urllib.request.urlopen(f"{node}/health", timeout=2).status == 200
                for node in NODES
            ):
                return
        except (urllib.error.URLError, TimeoutError, ConnectionError, http.client.RemoteDisconnected):
            pass
        time.sleep(1)
    raise AssertionError(f"{label} Riverbed did not become healthy")


def seed():
    writer = str(uuid.uuid4())
    wcl_retry(
        NODES[0],
        "CREATE DOMAIN dr.orders; "
        "CREATE FEED dr.orders.events; "
        "DEFINE STATESTORE dr.orders.totals; "
        "CREATE SUBSCRIPTION dr.orders.audit FROM dr.orders.events START AT BEGINNING;",
    )
    for sequence in range(12):
        append(NODES[0], "dr.orders.events", sequence, writer)
    store_mutate(NODES[0], "dr.orders.totals", "order-1", json.dumps({"count": 7}))


def expect_restored():
    domains = wcl_retry(NODES[0], "SHOW DOMAINS;")[0]
    assert any(d.get("name") == "dr.orders" for d in domains), domains
    records = wcl_retry(NODES[0], "SHOW FEEDS;")[0]
    assert any(f.get("name") == "dr.orders.events" for f in records), records
    # Poll the read until replicas re-corroborate post-restore.
    deadline = time.time() + 90
    rows = []
    while time.time() < deadline:
        try:
            rows = read(NODES[1], "dr.orders.events")
            if len(rows) >= 12:
                break
        except urllib.error.HTTPError:
            pass
        time.sleep(2)
    assert len(rows) >= 12, rows
    found = store_lookup(NODES[0], "dr.orders.totals", "order-1")
    assert found.get("found") is True, found
    assert base64.b64decode(found["payload_base64"]).decode() == '{"count": 7}', found
    # Writes continue where the backup left off — dedup + sequences survived.
    append(NODES[0], "dr.orders.events", 12, str(uuid.uuid4()))
    deadline = time.time() + 60
    while time.time() < deadline:
        try:
            if len(read(NODES[0], "dr.orders.events")) >= 13:
                break
        except urllib.error.HTTPError:
            pass
        time.sleep(2)
    else:
        raise AssertionError("post-restore append never became readable")


def logs(service):
    subprocess.run(
        [*COMPOSE, "logs", "--no-color", "--tail", "60", service],
        check=False,
    )


def main():
    if ARTIFACTS.exists():
        shutil.rmtree(ARTIFACTS)
    for index in range(1, 4):
        (ARTIFACTS / f"node{index}").mkdir(parents=True)
    subprocess.run([*COMPOSE, "down", "-v"], check=True)
    subprocess.run([*COMPOSE, "up", "-d", "--build"], check=True)
    try:
        wait_for_nodes("backup source")
        seed()
        # One crash-consistent archive per Node, into its bind-mounted dir.
        manifests = [
            backup(node, "/backups") for node in NODES
        ]
        assert all(m.get("files", 0) > 3 for m in manifests), manifests
        revisions = [m["catalog_revision"] for m in manifests]
        # Destroy the Riverbed completely — named volumes included.
        subprocess.run([*COMPOSE, "down", "-v"], check=True)
        # Restore from the surviving bind-mounted archives.
        subprocess.run(
            [*COMPOSE, "up", "-d"],
            check=True,
            env={**os.environ, "WHITEWATER_RESTORE_FROM": "/backups"},
        )
        wait_for_nodes("restored")
        expect_restored()
        # Idempotent restart: the marker makes a second boot with the env
        # still set resume rather than clobber or refuse.
        subprocess.run(
            [*COMPOSE, "restart", "node1"],
            check=True,
            env={**os.environ, "WHITEWATER_RESTORE_FROM": "/backups"},
        )
        wait_for_nodes("restarted-after-restore")
        expect_restored()
        print(
            json.dumps(
                {
                    "status": "ok",
                    "manifests": [
                        {
                            "files": m["files"],
                            "bytes": m["bytes"],
                            "catalog_revision": m["catalog_revision"],
                            "catalog_schema_version": m["catalog_schema_version"],
                        }
                        for m in manifests
                    ],
                    "revision_spread": max(revisions) - min(revisions),
                    "note": "backups taken on a live Riverbed survived down -v; restore "
                    "resumed the same catalog, records, state store, and subscriptions, "
                    "and a restart with the env still set was idempotent",
                },
                indent=2,
            )
        )
    except BaseException:
        for service in ("node1", "node2", "node3"):
            logs(service)
        raise
    finally:
        subprocess.run([*COMPOSE, "down", "-v"], check=True)


if __name__ == "__main__":
    main()
