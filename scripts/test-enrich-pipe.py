import base64
import http.client
import json
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

COMPOSE = ["docker", "compose", "-f", "compose.pipe.yml", "-p", "whitewater-pipe"]
NODES = [f"http://localhost:{port}" for port in range(7471, 7474)]
SERVICES = ["control-1", "control-2", "control-3"]
KEY = "whitewater-local-development-admin-key"
CONTROL_KEY = "whitewater-local-development-control-key"


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


def post_once(node, path, payload, control=False):
    headers = {"content-type": "application/json"}
    if control:
        headers["x-whitewater-control-key"] = CONTROL_KEY
    else:
        headers["authorization"] = f"Bearer {KEY}"
    request = urllib.request.Request(
        f"{node}{path}",
        data=json.dumps(payload).encode(),
        headers=headers,
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        return error.code, json.load(error)
    except OSError:
        return 0, {}
_post_transport = _retried(post_once)


def post(node, path, payload, control=False):
    for _ in range(120):
        status, result = _post_transport(node, path, payload, control)
        if status == 0 or status >= 500:
            time.sleep(0.5)
            continue
        return status, result
    return status, result


def wcl(node, script, request_id=None):
    status, response = post(node, "/v1/admin/wcl", {"request_id": request_id or str(uuid.uuid4()), "script": script})
    if status != 200:
        raise AssertionError({"script": script, "response": response})
    return [result["data"] for result in response["results"]]


def wcl_retry(node, script, attempts=30, delay=1):
    request_id = str(uuid.uuid4())
    last = None
    for _ in range(attempts):
        try:
            return wcl(node, script, request_id)
        except AssertionError as error:
            last = error
            time.sleep(delay)
    raise last


def read(node, feed):
    query = urllib.parse.urlencode({"feed": feed, "limit": 100})
    request = urllib.request.Request(
        f"{node}/v1/feeds/records?{query}",
        headers={"authorization": f"Bearer {KEY}"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def b64(text):
    return base64.b64encode(text.encode()).decode()


def unb64(value):
    return base64.b64decode(value).decode()


def append_batch(node, writer, epoch, events, retries=1):
    records = [
        {
            "request_id": str(uuid.uuid4()),
            "writer": writer,
            "session_epoch": epoch,
            "event_time_ns": str(1_700_000_000_000_000_000 + index),
            "key_base64": b64(key),
            "payload_base64": b64(f"payload-{key}-{index}"),
            "metadata_base64": {"trace": b64(f"trace-{index}")},
        }
        for index, key in events
    ]
    body = {"records": records}
    last = None
    for _ in range(retries):
        status, result = post(node, "/v1/writers/append-batch", body)
        if status == 200:
            return body
        last = result
        time.sleep(2)
    raise AssertionError({"append": last})


def repost_batch(node, body, retries=45):
    """Replay the identical request identities: transient retries must dedupe
    rather than land a second record."""
    last = None
    for _ in range(retries):
        status, result = post(node, "/v1/writers/append-batch", body)
        if status == 200:
            return
        last = result
        time.sleep(2)
    raise AssertionError({"repost": last})


def wait_for_fabric():
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
    raise AssertionError("pipe Riverbed did not become healthy")


def wait_records(feed, expected, deadline=90):
    """Poll until the Feed holds at least `expected` records."""
    last = None
    end = time.time() + deadline
    while time.time() < end:
        try:
            records = read(NODES[2], feed)
            if len(records) >= expected:
                return records
            last = {"feed": feed, "records": len(records), "expected": expected}
        except (urllib.error.URLError, urllib.error.HTTPError) as error:
            last = repr(error)
        time.sleep(1)
    raise AssertionError(f"Feed never reached {expected} records: {last}")


def settle(feed, seconds=6):
    """Give any stray duplicate a chance to surface, then return the Feed."""
    time.sleep(seconds)
    return read(NODES[1], feed)


def mutate_row(node, store, key, payload, mutation_id=None):
    status, result = post(
        node,
        "/v1/state-stores/mutate",
        {
            "store": store,
            "mutation_id": mutation_id or str(uuid.uuid4()),
            "op": {
                "kind": "put_row",
                "key_base64": b64(key),
                "payload_base64": b64(payload),
            },
        },
    )
    if status != 200:
        raise AssertionError({"mutate": result})
    return result


def wait_rows(node, store, keys, deadline=60):
    """A row must be applied before the Pipe drive resolves it; otherwise the
    miss is a durable journaled decision rather than a transient."""
    end = time.time() + deadline
    pending = set(keys)
    while pending and time.time() < end:
        for key in list(pending):
            status, body = post(
                node,
                "/v1/state-stores/lookup",
                {"store": store, "key_base64": b64(key)},
            )
            if status == 200:
                pending.discard(key)
        time.sleep(0.5)
    if pending:
        raise AssertionError(f"StateStore rows never applied: {sorted(pending)}")


def drive_probe_once(node, pipe_id, subscription_id, storage_node, epoch):
    return post_once(
        node,
        "/internal/pipe/drive",
        {
            "pipe_id": pipe_id,
            "subscription_id": subscription_id,
            "owner": storage_node,
            "receiver": storage_node,
            "ownership_epoch": epoch,
            "limit": 64,
        },
        control=True,
    )


def find_progress_owner(pipe_id, subscription_id):
    """Probe each storage Node with owner=receiver=<that Node>. Only the
    current progress owner accepts the drive; a stale epoch or wrong owner
    is fenced, so a 200 is an authoritative answer."""
    for _ in range(90):
        for index, node in enumerate(NODES):
            for epoch in range(1, 12):
                try:
                    status, body = drive_probe_once(
                        node, pipe_id, subscription_id, SERVICES[index], epoch
                    )
                except (ConnectionError, urllib.error.URLError, TimeoutError):
                    continue
                if status == 200:
                    return SERVICES[index], epoch
        time.sleep(1)
    raise AssertionError("no Node accepted the Pipe drive")


def event_time(index):
    return str(1_700_000_000_000_000_000 + index)


def assert_enriched(record, key, event_index):
    metadata = record["metadata_base64"]
    if record["key_base64"] != b64(key):
        raise AssertionError({"key": record, "expected": key})
    if record["payload_base64"] != b64(f"payload-{key}-{event_index}"):
        raise AssertionError({"payload": record})
    if record["event_time_ns"] != event_time(event_index):
        raise AssertionError({"event_time": record})
    if unb64(metadata.get("enrich.matched", "")) != "true":
        raise AssertionError({"enrich.matched": record})
    if unb64(metadata.get("enrich.payload", "")) != f'{{"tier":"{key}"}}':
        raise AssertionError({"enrich.payload": record})
    if not unb64(metadata.get("enrich.seq", "")).isdigit():
        raise AssertionError({"enrich.seq": record})
    if unb64(metadata.get("trace", "")) != f"trace-{event_index}":
        raise AssertionError({"trace": record})


def assert_quarantined(record, key, event_index):
    metadata = record["metadata_base64"]
    if record["key_base64"] != b64(key):
        raise AssertionError({"key": record, "expected": key})
    if record["payload_base64"] != b64(f"payload-{key}-{event_index}"):
        raise AssertionError({"payload": record})
    if record["event_time_ns"] != event_time(event_index):
        raise AssertionError({"event_time": record})
    if unb64(metadata.get("enrich.matched", "")) != "false":
        raise AssertionError({"enrich.matched": record})
    if unb64(metadata.get("enrich.quarantine", "")) != "true":
        raise AssertionError({"enrich.quarantine": record})


def record_at(records, event_index):
    return next(
        (row for row in records if row["event_time_ns"] == event_time(event_index)),
        None,
    )


def assert_exactly_once(enriched, quarantined):
    """Every delivered output is unique by (key, event_time): re-delivery and
    re-drive must replay identically rather than double-apply an effect."""
    seen = set()
    for record in enriched + quarantined:
        identity = (record["key_base64"], record["event_time_ns"])
        if identity in seen:
            raise AssertionError(f"duplicate committed output: {record}")
        seen.add(identity)


def main():
    subprocess.run([*COMPOSE, "down", "-v"], check=True)
    subprocess.run([*COMPOSE, "up", "-d", "--build"], check=True)
    try:
        wait_for_fabric()

        suffix = uuid.uuid4().hex[:8]
        domain = f"enr{suffix}"
        source, enriched, quarantine = (
            f"{domain}.source",
            f"{domain}.enriched",
            f"{domain}.quarantine",
        )
        store = f"{domain}.profiles"
        writer, subscription, pipe = (
            f"writer{suffix}",
            f"{domain}.flow",
            f"{domain}.enrich",
        )
        results = wcl_retry(
            NODES[0],
            f"CREATE DOMAIN {domain}; "
            f"CREATE FEED {source}; "
            f"CREATE FEED {enriched}; "
            f"CREATE FEED {quarantine}; "
            f"DEFINE STATESTORE {store}; "
            f"CREATE WRITER {writer} TO {source}; "
            f"OPEN WRITER SESSION {writer}; "
            f"CREATE SUBSCRIPTION {subscription} FROM {source} START AT BEGINNING; "
            f"CREATE PIPE {pipe} FROM SUBSCRIPTION {subscription} "
            f"TO FEED {enriched} ENRICH STATESTORE {store} BY KEY "
            f"MISS QUARANTINE {quarantine};",
        )
        writer_epoch = results[6]["session_epoch"]
        described = wcl(NODES[1], f"DESCRIBE PIPE {pipe};")[0]
        pipe_id = described["pipe_id"]
        subscription_id = described["subscription_id"]

        for key in ("user-1", "user-2", "user-3"):
            mutate_row(NODES[0], store, key, f'{{"tier":"{key}"}}')
        wait_rows(NODES[1], store, ["user-1", "user-2", "user-3"])

        # Three lookups hit; the miss routes to the declared quarantine Feed.
        batch = append_batch(
            NODES[1],
            writer,
            writer_epoch,
            [(1, "user-1"), (2, "user-2"), (3, "user-3"), (4, "user-miss")],
        )
        out = wait_records(enriched, 3)
        quar = wait_records(quarantine, 1)
        for index, key in ((1, "user-1"), (2, "user-2"), (3, "user-3")):
            match = record_at(out, index)
            if match is None:
                raise AssertionError(f"enriched output is missing {key}: {out}")
            assert_enriched(match, key, index)
        assert_quarantined(quar[0], "user-miss", 4)
        assert_exactly_once(out, quar)

        # Same-request retry: identical request_ids must dedupe rather than
        # land a second input that the Pipe would deliver again.
        repost_batch(NODES[0], batch)
        out = settle(enriched)
        quar = settle(quarantine)
        if len(out) != 3 or len(quar) != 1:
            raise AssertionError(
                f"request retry double-applied: enriched={len(out)} quarantine={len(quar)}"
            )
        assert_exactly_once(out, quar)

        # Owner change: drain the progress owner so the Control Plane moves
        # it to a surviving replica at a higher epoch; the supervisor on the
        # new owner picks the Pipe up automatically.
        owner, epoch = find_progress_owner(pipe_id, subscription_id)
        wcl(NODES[0], f"DRAIN STORAGE NODE {owner};")
        new_owner, new_epoch = None, None
        deadline = time.time() + 120
        while time.time() < deadline:
            found = find_progress_owner(pipe_id, subscription_id)
            if found[0] != owner and found[1] > epoch:
                new_owner, new_epoch = found
                break
            time.sleep(2)
        if new_owner is None:
            raise AssertionError(
                f"drain did not move the Subscription progress owner off {owner}"
            )
        wcl(NODES[0], f"UNDRAIN STORAGE NODE {owner};")
    
        append_batch(
            NODES[0],
            writer,
            writer_epoch,
            [(5, "user-1"), (6, "user-miss-2")],
            retries=45,
        )
        out = wait_records(enriched, 4)
        quar = wait_records(quarantine, 2)
        assert_enriched(record_at(out, 5), "user-1", 5)
        assert_quarantined(record_at(quar, 6), "user-miss-2", 6)
        assert_exactly_once(out, quar)

        # Hard restart of the progress owner: a member join recovers the
        # lost owner through the placement authority, and the Pipe resumes
        # on the surviving owner without redelivering acknowledged work.
        subprocess.run([*COMPOSE, "stop", new_owner], check=True)
        survivor = next(
            node
            for service, node in zip(SERVICES, NODES)
            if service != new_owner
        )
        joined = None
        join_identity = {"request_id": str(uuid.uuid4()), "member_id": str(uuid.uuid4())}
        for _ in range(90):
            status, joined = post(
                survivor,
                "/v1/subscriptions/members/join",
                {
                    "subscription": subscription,
                    "request_id": join_identity["request_id"],
                    "member_id": join_identity["member_id"],
                },
            )
            if status == 200:
                break
            time.sleep(1)
        else:
            raise AssertionError({"member_join_for_recovery": joined})
        recovered_owner, recovered_epoch = None, None
        deadline = time.time() + 120
        while time.time() < deadline:
            found = find_progress_owner(pipe_id, subscription_id)
            if found[0] != new_owner and found[1] > new_epoch:
                recovered_owner, recovered_epoch = found
                break
            time.sleep(2)
        if recovered_owner is None:
            raise AssertionError(
                f"progress owner was not recovered off stopped Node {new_owner}"
            )
        subprocess.run([*COMPOSE, "start", new_owner], check=True)
        wait_for_fabric()

        append_batch(
            NODES[2],
            writer,
            writer_epoch,
            [(7, "user-3")],
            retries=45,
        )
        out = wait_records(enriched, 5)
        quar = wait_records(quarantine, 2)
        assert_enriched(record_at(out, 7), "user-3", 7)

        # Re-drive idempotency is the supervisor's own behavior: at a 200ms
        # drive interval each settle window is dozens of re-drives over a
        # caught-up frontier, and replay rides the committed Declare rather
        # than re-resolving rows. Any double-apply would surface here.
        out = settle(enriched)
        quar = settle(quarantine)
        if len(out) != 5 or len(quar) != 2:
            raise AssertionError(
                f"re-drive emitted extra outputs: enriched={len(out)} quarantine={len(quar)}"
            )
        assert_exactly_once(out, quar)

        print(
            json.dumps(
                {
                    "status": "ok",
                    "enriched": len(out),
                    "quarantined": len(quar),
                    "owner_before_drain": owner,
                    "owner_after_drain": new_owner,
                    "owner_after_restart": recovered_owner,
                    "note": (
                        "enrich Pipe resolved StateStore rows into output "
                        "Metadata, quarantined the declared miss, deduplicated "
                        "the same-request retry, and stayed exactly-once "
                        "across drain and a hard progress-owner restart"
                    ),
                },
                indent=2,
            )
        )
    finally:
        # `down -v` discards the drained marker together with every volume.
        subprocess.run([*COMPOSE, "down", "-v"], check=True)


if __name__ == "__main__":
    main()
