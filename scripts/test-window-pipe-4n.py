import base64
import http.client
import json
import pathlib
import struct
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

# The project name comes from the compose file's `name:` field so the
# acceptance runner's sweep covers it alongside the sibling join test.
COMPOSE = ["docker", "compose", "-f", "compose.storage-join.yml"]
VOTERS = [f"http://localhost:{port}" for port in range(7171, 7174)]
STORAGE = "http://localhost:7175"
VOTER_SERVICES = ["control-1", "control-2", "control-3"]
STORAGE_NODE = "storage-1"
NODES = VOTERS + [STORAGE]
KEY = "whitewater-local-development-admin-key"
CERTS_DIR = ".mtls-join-certs"
CERT_NODES = VOTER_SERVICES + [STORAGE_NODE]

WINDOW_NS = 8_000_000_000
PANE_NS = WINDOW_NS // 16  # 500ms — 16 panes per window
MAX_LATE_NS = 6_000_000_000


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


def post_once(node, path, payload):
    headers = {
        "authorization": f"Bearer {KEY}",
        "content-type": "application/json",
    }
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


post = _retried(post_once)


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


def append_json(node, writer, epoch, events, retries=1):
    """`events`: (customer, amount, event_time_ns). The payload is the JSON
    document the `BY JSON 'customer'` / `SUM JSON 'amount'` specs project."""
    records = [
        {
            "request_id": str(uuid.uuid4()),
            "writer": writer,
            "session_epoch": epoch,
            "event_time_ns": str(event_ns),
            "key_base64": b64(customer),
            "payload_base64": b64(
                json.dumps({"customer": customer, "amount": amount})
            ),
        }
        for customer, amount, event_ns in events
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


def wait_for_nodes(nodes, label):
    for _ in range(120):
        try:
            if all(
                urllib.request.urlopen(f"{node}/health", timeout=2).status == 200
                for node in nodes
            ):
                return
        except (urllib.error.URLError, TimeoutError, ConnectionError, http.client.RemoteDisconnected):
            pass
        time.sleep(1)
    raise AssertionError(f"{label} Riverbed did not become healthy")


def wait_records(feed, expected, deadline=90):
    """Poll until the Feed holds at least `expected` records."""
    last = None
    end = time.time() + deadline
    while time.time() < end:
        try:
            records = read(VOTERS[2], feed)
            if len(records) >= expected:
                return records
            last = {"feed": feed, "records": len(records), "expected": expected}
        except (urllib.error.URLError, urllib.error.HTTPError) as error:
            last = repr(error)
        time.sleep(1)
    raise AssertionError(f"Feed never reached {expected} records: {last}")


def get_by(node, store, named_key, values=None, lower=None):
    body = {"store": store, "named_key": named_key}
    if values is not None:
        body["values"] = values
    if lower is not None:
        body["lower"] = lower
    status, result = post(node, "/v1/state-stores/get-by", body)
    if status != 200:
        raise AssertionError({"get_by": result})
    return result["rows"]


def sentinel_rows(node, store):
    return get_by(node, store, "pane", values=[{"kind": "i64", "value": -1}])


def parse_row_key(key_base64):
    """Inverse of `window_pane_row_key`: (tagged group bytes, u64 pane)."""
    raw = base64.b64decode(key_base64)
    pane = struct.unpack(">Q", raw[-8:])[0]
    group_len = struct.unpack(">I", raw[:4])[0]
    group_tagged = raw[4 : 4 + group_len]
    if len(group_tagged) != group_len or not group_tagged:
        raise AssertionError(f"malformed pane row key: {key_base64}")
    return group_tagged, pane


def decode_group(group_tagged):
    tag = group_tagged[0]
    if tag in (2, 3):
        return group_tagged[1:].decode()
    raise AssertionError(f"unexpected group tag {tag}")


def decode_accumulator(payload_base64):
    raw = base64.b64decode(payload_base64)
    if len(raw) != 24:
        raise AssertionError(f"accumulator payload is not 24 bytes: {len(raw)}")
    count = struct.unpack("<Q", raw[:8])[0]
    sum_value = int.from_bytes(raw[8:], "little", signed=True)
    return count, sum_value


def member_join(node, subscription):
    return post(
        node,
        "/v1/subscriptions/members/join",
        {
            "subscription": subscription,
            "request_id": str(uuid.uuid4()),
            "member_id": str(uuid.uuid4()),
        },
    )


def emissions(records):
    """Decode emitted window results; keyed by (group, pane)."""
    out = {}
    for record in records:
        if unb64(record["metadata_base64"].get("window.emitted", "")) != "true":
            raise AssertionError({"not an emission": record})
        body = json.loads(unb64(record["payload_base64"]))
        identity = (body["group"], body["pane"])
        if identity in out:
            raise AssertionError(f"duplicate window emission: {body}")
        expected_event = (body["pane"] + 1) * PANE_NS - 1
        if record["event_time_ns"] != str(expected_event):
            raise AssertionError({"emission event_time": record})
        if int(body["pane_end_ns"]) - int(body["pane_start_ns"]) != PANE_NS:
            raise AssertionError({"emission bounds": record})
        if int(body["store_seq"]) <= 0:
            raise AssertionError({"emission store_seq": record})
        out[identity] = body
    return out


def pane_of(event_ns):
    return max(event_ns, 0) // PANE_NS


def assert_subset(out, expected):
    for (group, pane), (count, sum_value) in expected.items():
        body = out.get((group, pane))
        if body is None:
            raise AssertionError({"missing emission": (group, pane), "emitted": out})
        if body["count"] != count or int(body["sum"]) != sum_value:
            raise AssertionError({"emission mismatch": body, "expected": (count, sum_value)})


def generate_certificates():
    subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "-v",
            f"{pathlib.Path(__file__).resolve().parents[1].as_posix()}:/workspace",
            "-w",
            "/workspace",
            "-e",
            "WW_CERTS_DIR=/workspace/.mtls-join-certs",
            "-e",
            "WW_CERTS_NODES=" + ",".join(CERT_NODES),
            "-e",
            "WW_CERTS_PORT=7271",
            # Cargo runs as the container's root user; keep its build cache in
            # a Docker-managed volume so it never leaves a root-owned target/
            # in the host workspace (which breaks the runner's own cargo).
            "-e",
            "CARGO_TARGET_DIR=/tmp/ww-certgen-target",
            "-v",
            "whitewater-certgen-target:/tmp/ww-certgen-target",
            "rust:1.90-bookworm",
            "bash",
            "-c",
            "cargo test --release --test dev_cert_gen && chmod -R a+rw /workspace/.mtls-join-certs",
        ],
        check=True,
    )


def env_path(node):
    return pathlib.Path(CERTS_DIR).joinpath(f"{node}.env")


def storage_pin():
    text = env_path(STORAGE_NODE).read_text()
    line = next(
        line
        for line in text.splitlines()
        if line.startswith("WHITEWATER_SUBSCRIPTION_MTLS_PEER_PINS=")
    )
    entry = next(
        entry
        for entry in line.split("=", 1)[1].split(",")
        if entry.startswith(f"{STORAGE_NODE}@")
    )
    return entry.split("@", 1)[1]


def strip_voter_pins():
    # Remove the storage Node's pin from every voter's static peer set so the
    # voters can only trust its certificate after the catalog registration
    # distributes the pin — the mechanism this acceptance exists to prove.
    for voter in CERT_NODES:
        if voter == STORAGE_NODE:
            continue
        path = env_path(voter)
        lines = path.read_text().splitlines(keepends=True)
        rewritten = []
        for line in lines:
            if line.startswith("WHITEWATER_SUBSCRIPTION_MTLS_PEER_PINS="):
                entries = [
                    entry
                    for entry in line.strip().split("=", 1)[1].split(",")
                    if entry and not entry.startswith(f"{STORAGE_NODE}@")
                ]
                line = (
                    "WHITEWATER_SUBSCRIPTION_MTLS_PEER_PINS="
                    + ",".join(entries)
                    + "\n"
                )
            rewritten.append(line)
        path.write_text("".join(rewritten))


def placement(node, feed):
    return wcl_retry(node, f"INSPECT PLACEMENT FOR FEED {feed};")[0]


def show_pipe(node, pipe_name):
    rows = wcl_retry(node, "SHOW PIPES;")[0]
    for row in rows:
        if row.get("name") == pipe_name:
            return row
    raise AssertionError({"pipes": rows})


def admin_progress(node, subscription):
    query = urllib.parse.urlencode({"subscription": subscription})
    request = urllib.request.Request(
        f"{node}/v1/admin/subscriptions/progress?{query}",
        headers={"authorization": f"Bearer {KEY}"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)["assignment"]


def storage_serves_read(feed, expected):
    """Poll until the storage Node serves at least `expected` committed
    records — its local replica must catch up to the moved prefix."""
    deadline = time.time() + 150
    last = None
    while time.time() < deadline:
        try:
            records = read(STORAGE, feed)
            if len(records) >= expected:
                return records
            last = {"records": len(records), "expected": expected}
        except (urllib.error.URLError, urllib.error.HTTPError) as error:
            last = repr(error)
        time.sleep(2)
    detail = {}
    for index, node in enumerate(VOTERS + [STORAGE]):
        try:
            detail[f"node{index}"] = [
                record.get("sequence") for record in read(node, feed)
            ]
        except Exception as error:
            detail[f"node{index}"] = repr(error)
    raise AssertionError(
        f"storage Node did not serve a synced read: {last} per-node={detail}"
    )


def main():
    certs = pathlib.Path(CERTS_DIR)
    if not certs.joinpath("ca.pem").exists() or any(
        not certs.joinpath(f"{node}.pem").exists() for node in CERT_NODES
    ):
        generate_certificates()
    strip_voter_pins()
    subprocess.run([*COMPOSE, "down", "-v"], check=True)
    # Three voters only — the storage Node joins later, turning the run into
    # a live three-to-four-Node topology change.
    subprocess.run([*COMPOSE, "up", "-d", "--build", *VOTER_SERVICES], check=True)
    try:
        wait_for_nodes(VOTERS, "three-Node voter")

        suffix = uuid.uuid4().hex[:8]
        domain = f"w4n{suffix}"
        source, results_feed = f"{domain}.source", f"{domain}.results"
        panes_store = f"{domain}.panes"
        writer = f"writer{suffix}"
        subscription = f"{domain}.flow"
        pipe = f"{domain}.totals"
        results = wcl_retry(
            VOTERS[0],
            f"CREATE DOMAIN {domain}; "
            f"CREATE FEED {source}; "
            f"CREATE FEED {results_feed}; "
            f"DEFINE STATESTORE {panes_store} KEYS key, pane; "
            f"CREATE WRITER {writer} TO {source}; "
            f"OPEN WRITER SESSION {writer}; "
            f"CREATE SUBSCRIPTION {subscription} FROM {source} START AT BEGINNING; "
            f"CREATE PIPE {pipe} FROM SUBSCRIPTION {subscription} "
            f"TO STATESTORE {panes_store} WINDOW 8s EVENT MAX_LATE 6s "
            f"BY JSON 'customer' SUM JSON 'amount' EMIT TO FEED {results_feed};",
        )
        writer_epoch = results[5]["session_epoch"]

        # Pane P0 anchored ~2s ahead so batch placement is jitter-proof.
        t0 = time.time_ns()
        p0 = (t0 + 2_000_000_000) // PANE_NS
        p0_start = p0 * PANE_NS

        append_json(
            VOTERS[1],
            writer,
            writer_epoch,
            [
                ("acme", 10, p0_start + 100_000_000),
                ("acme", 20, p0_start + 200_000_000),
                ("globex", 5, p0_start + 300_000_000),
            ],
        )
        time.sleep(3)
        append_json(
            VOTERS[0],
            writer,
            writer_epoch,
            [
                ("acme", 30, p0_start + 400_000_000),    # still P0
                ("acme", 40, p0_start + 600_000_000),    # P0+1
                ("globex", 7, p0_start + 1_100_000_000), # P0+2
                ("acme", 100, p0_start + 1_600_000_000), # P0+3
            ],
        )

        # Three-Node evidence: the first panes close and emit while the
        # Riverbed is still only the three voters. The source Feed holds
        # seven records so far (3 + 4).
        close_at = p0_start + 2_000_000_000 + MAX_LATE_NS + 1_000_000_000
        wait_ns = close_at - time.time_ns()
        if wait_ns > 0:
            time.sleep(wait_ns / 1e9)
        source_records = 7
        expected = {
            ("acme", p0): (3, 60),
            ("globex", p0): (1, 5),
            ("acme", p0 + 1): (1, 40),
            ("globex", p0 + 2): (1, 7),
            ("acme", p0 + 3): (1, 100),
        }
        emitted = emissions(wait_records(results_feed, 5))
        assert_subset(emitted, expected)

        # Compute-lease evidence: the Pipe's drive authority is a catalog
        # lease seeded co-located with the Subscription progress owner —
        # but it moves independently.
        progress = admin_progress(VOTERS[0], subscription)
        row = show_pipe(VOTERS[0], pipe)
        if row["compute_owner"] != progress["owner"] or row["compute_epoch"] != 1:
            raise AssertionError(
                {"seeded_lease": row, "progress_owner": progress}
            )
        lease_target = next(
            node
            for node in VOTER_SERVICES
            if node != progress["owner"]
        )
        wcl_retry(
            VOTERS[0],
            f"MOVE PIPE COMPUTE {pipe} TO NODE {lease_target};",
        )
        moved = show_pipe(VOTERS[0], pipe)
        if (
            moved["compute_owner"] != lease_target
            or moved["compute_epoch"] != 2
        ):
            raise AssertionError({"lease_after_move": moved})
        progress_after = admin_progress(VOTERS[0], subscription)
        if (
            progress_after["owner"] != progress["owner"]
            or progress_after["ownership_epoch"]
            != progress["ownership_epoch"]
        ):
            raise AssertionError(
                "compute-lease move touched the progress placement: "
                f"{progress} -> {progress_after}"
            )

        # Fresh input while the compute lease and the progress owner live
        # on different Nodes proves the remote drive path end-to-end.
        tm = time.time_ns()
        pm = pane_of(tm)
        append_json(
            VOTERS[2],
            writer,
            writer_epoch,
            [("acme", 9, tm)],
            retries=30,
        )
        source_records += 1
        expected[("acme", pm)] = (1, 9)
        emitted = emissions(wait_records(results_feed, 6, deadline=90))
        assert_subset(emitted, expected)

        # Park the lease on the voter about to drain (it may already be
        # there from the divergence move) so the drain must migrate it
        # alongside ranges and progress.
        if show_pipe(VOTERS[0], pipe)["compute_owner"] != "control-1":
            wcl_retry(
                VOTERS[0],
                f"MOVE PIPE COMPUTE {pipe} TO NODE control-1;",
            )

        # Three-to-four: start and register the storage Node. Its pin is not
        # in the voters' static peer set — only the catalog registration
        # makes it a trusted internal peer.
        subprocess.run([*COMPOSE, "up", "-d", STORAGE_NODE], check=True)
        wait_for_nodes(NODES, "four-Node")
        pin = storage_pin()
        wcl_retry(
            VOTERS[0],
            f"REGISTER STORAGE NODE {STORAGE_NODE} "
            f"AT https://{STORAGE_NODE}:7271 WITH CERT PIN {pin};",
        )
        shown = wcl_retry(VOTERS[0], "SHOW STORAGE NODES;")[0]
        rows = shown["nodes"] if isinstance(shown, dict) else shown
        registered = [row for row in rows if row.get("node") == STORAGE_NODE]
        if not registered or not registered[0].get("eligible", True):
            raise AssertionError({"storage_nodes": shown})

        # Drain the first voter: every drained replica — Feed ranges AND
        # Subscription progress — has {storage-1} as its only eligible
        # replacement, so control-1's responsibilities land on the joined
        # Node deterministically. The drain supervisor retires each planned
        # move as it lands, so the move evidence is the union over polls.
        wcl(VOTERS[0], "DRAIN STORAGE NODE control-1;")
        plan, moves_seen = None, []
        for _ in range(120):
            plan = wcl(VOTERS[0], "INSPECT DRAIN FOR STORAGE NODE control-1;")[0]
            moves_seen.extend(plan.get("moves", []))
            if plan.get("ready_to_retire"):
                break
            time.sleep(1)
        else:
            raise AssertionError(f"control-1 drain did not complete: {plan}")
        moves_json = json.dumps(moves_seen)
        if STORAGE_NODE not in moves_json or "subscription_id" not in moves_json:
            raise AssertionError(
                f"drain never moved data/progress to {STORAGE_NODE}: {moves_seen}"
            )
        # The parked compute lease had to migrate off the drained voter
        # too; it lands on any storage-eligible survivor.
        post_drain = show_pipe(VOTERS[0], pipe)
        if post_drain["compute_owner"] == "control-1":
            raise AssertionError({"lease_still_on_drained_node": post_drain})

        placed = placement(VOTERS[0], source)
        if STORAGE_NODE not in placed["replicas"]:
            raise AssertionError({"post-drain placement": placed})

        # The joined Node serves the Feed data it now holds, and member
        # traffic on the same Subscription keeps working across the moved
        # progress placement.
        storage_serves_read(source, source_records)
        status, joined = member_join(VOTERS[1], subscription)
        if status != 200:
            raise AssertionError({"member_join_after_drain": joined})

        # Fresh input after the topology change aggregates and emits on the
        # four-Node placement; a deliberately late record still quarantines.
        t2 = time.time_ns()
        p2 = pane_of(t2)
        append_json(
            VOTERS[1],
            writer,
            writer_epoch,
            [
                ("globex", 3, t2),
                ("acme", 2, t2 + 600_000_000),
                ("lateco", 999, p0_start + 10_000_000),
            ],
            retries=45,
        )
        source_records += 3
        expected[("globex", p2)] = (1, 3)
        # t2 + 600ms can cross two pane boundaries when t2 sits in the
        # last 100ms of its pane — compute the pane, don't assume p2 + 1.
        expected[("acme", pane_of(t2 + 600_000_000))] = (1, 2)
        deadline = time.time() + 150
        emitted = None
        last_error = None
        while time.time() < deadline:
            try:
                emitted = emissions(wait_records(results_feed, 7, deadline=30))
                assert_subset(emitted, expected)
                break
            except AssertionError as error:
                last_error = error
                time.sleep(2)
        else:
            raise AssertionError(
                {
                    "post_drain_emissions": last_error,
                    "feed_records": len(read(VOTERS[2], results_feed)),
                    "lease": show_pipe(VOTERS[0], pipe),
                    "progress": admin_progress(VOTERS[0], subscription),
                }
            )

        # Hard restart of the joined Node: replicas re-sync, drives keep
        # committing through the quorum that includes it, and no pane
        # re-emits.
        subprocess.run([*COMPOSE, "restart", STORAGE_NODE], check=True)
        wait_for_nodes(NODES, "post-restart four-Node")
        storage_serves_read(source, source_records)
        t3 = time.time_ns()
        p3 = pane_of(t3)
        append_json(
            VOTERS[0],
            writer,
            writer_epoch,
            [("acme", 4, t3)],
            retries=45,
        )
        source_records += 1
        expected[("acme", p3)] = (1, 4)
        deadline = time.time() + 150
        emitted = None
        last_error = None
        while time.time() < deadline:
            try:
                emitted = emissions(wait_records(results_feed, 8, deadline=30))
                assert_subset(emitted, expected)
                break
            except AssertionError as error:
                last_error = error
                time.sleep(2)
        else:
            raise AssertionError(
                {
                    "post_restart_emissions": last_error,
                    "feed_records": len(read(VOTERS[2], results_feed)),
                    "lease": show_pipe(VOTERS[0], pipe),
                    "progress": admin_progress(VOTERS[0], subscription),
                }
            )

        # Exactly-once and quarantine evidence across the whole lifecycle.
        sentinels = sentinel_rows(VOTERS[1], panes_store)
        if len(sentinels) != 1:
            raise AssertionError({"sentinel rows": sentinels})
        group_tagged, pane = parse_row_key(sentinels[0]["key_base64"])
        if pane != 2**64 - 1 or decode_group(group_tagged) != "lateco":
            raise AssertionError({"sentinel row": sentinels[0]})
        if decode_accumulator(sentinels[0]["payload_base64"]) != (1, 0):
            raise AssertionError({"sentinel accumulator": sentinels[0]})

        progress_moves = sum(
            1 for move in moves_seen if "subscription_id" in json.dumps(move)
        )
        print(
            json.dumps(
                {
                    "status": "ok",
                    "emissions": len(emitted),
                    "placement_after_drain": placed["replicas"],
                    "progress_moves_to_storage": progress_moves,
                    "compute_lease_moves": sum(
                        1
                        for move in moves_seen
                        if "move_pipe_compute_lease" in json.dumps(move)
                    ),
                    "compute_owner_after_drain": post_drain["compute_owner"],
                    "compute_epoch_after_drain": post_drain["compute_epoch"],
                    "storage_served_records": len(
                        storage_serves_read(source, source_records)
                    ),
                    "note": (
                        "window Pipe emitted closed panes on three voters, "
                        "the storage Node joined and registered at runtime, "
                        "a voter drain moved Feed replicas and Subscription "
                        "progress onto it, and emissions stayed exactly-once "
                        "through the topology change and a storage-Node restart"
                    ),
                },
                indent=2,
            )
        )
    finally:
        subprocess.run([*COMPOSE, "down", "-v"], check=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        import traceback

        traceback.print_exc()
        for service in VOTER_SERVICES + [STORAGE_NODE]:
            try:
                logs = subprocess.run(
                    [*COMPOSE, "logs", "--no-color", "--tail", "60", service],
                    capture_output=True,
                    text=True,
                    timeout=15,
                )
                print(f"--- logs {service} ---\n{logs.stdout[-3000:]}", flush=True)
            except Exception:
                pass
        print(f"window Pipe three-to-four acceptance failed: {error}", flush=True)
        raise SystemExit(1)
