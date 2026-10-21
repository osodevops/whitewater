import base64
import http.client
import json
import struct
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

WINDOW_NS = 8_000_000_000
PANE_NS = WINDOW_NS // 16  # 500ms â€” 16 panes per window
MAX_LATE_NS = 20_000_000_000
I64_MAX = 9_223_372_036_854_775_807


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


def get_json(node, path):
    request = urllib.request.Request(
        f"{node}{path}",
        headers={"authorization": f"Bearer {KEY}"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


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


def repost_batch(node, body, retries=45):
    """Replay the identical request identities: transient retries must dedupe
    rather than land a second record that would re-accumulate panes."""
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


def pane_rows(node, store):
    """Every real (non-sentinel) pane row: the named `pane` posting of any
    pane index >= 0."""
    return get_by(
        node,
        store,
        "pane",
        lower={"value": {"kind": "i64", "value": 0}, "inclusive": True},
    )


def pipe_demand():
    """Merged `/v1/admin/demand/pipes` across all Nodes.

    Telemetry is Node-local since process start â€” a compute-lease move
    restarts the row on the new owner â€” so run-total cost is the sum of
    per-Node rows. `last_*`/`*_last` fields merge by max (most-recent
    estimates, not costs); `pipes` in a Domain rollup merges by max for
    the same reason (every Node reports the same live Pipe set).
    """
    merged, domains = {}, {}
    for node in NODES:
        try:
            candidate = get_json(node, "/v1/admin/demand/pipes")
        except Exception:
            continue
        for row in candidate.get("pipes", []):
            entry = merged.setdefault(
                row["pipe"],
                {
                    "domain": row["domain"],
                    "operation": row["operation"],
                    "backfilling": False,
                    "telemetry": {},
                },
            )
            entry["backfilling"] = entry["backfilling"] or row.get(
                "backfilling", False
            )
            for field, value in (row.get("telemetry") or {}).items():
                if not isinstance(value, (int, float)):
                    continue
                prior = entry["telemetry"].get(field, 0)
                entry["telemetry"][field] = (
                    max(prior, value)
                    if field.startswith("last_") or field.endswith("_last")
                    else prior + value
                )
        for name, rollup in candidate.get("domains", {}).items():
            target = domains.setdefault(name, {})
            for field, value in rollup.items():
                if isinstance(value, (int, float)):
                    target[field] = (
                        max(target.get(field, 0), value)
                        if field == "pipes"
                        else target.get(field, 0) + value
                    )
    return merged, domains


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
    """Tag 2 is Text; 3 is Bytes (BY KEY)."""
    tag = group_tagged[0]
    if tag == 2:
        return group_tagged[1:].decode()
    if tag == 3:
        return group_tagged[1:].decode()
    raise AssertionError(f"unexpected group tag {tag}")


def decode_accumulator(payload_base64):
    raw = base64.b64decode(payload_base64)
    if len(raw) != 24:
        raise AssertionError(f"accumulator payload is not 24 bytes: {len(raw)}")
    count = struct.unpack("<Q", raw[:8])[0]
    sum_value = int.from_bytes(raw[8:], "little", signed=True)
    return count, sum_value


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
                    status, _ = drive_probe_once(
                        node, pipe_id, subscription_id, SERVICES[index], epoch
                    )
                except (ConnectionError, urllib.error.URLError, TimeoutError):
                    continue
                if status == 200:
                    return SERVICES[index], epoch
        time.sleep(1)
    raise AssertionError("no Node accepted the Pipe drive")


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
        # The emitted record carries the last nanosecond of its pane.
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
    """Every expected (group, pane) -> (count, sum) appears in `out`; extra
    emissions are tolerated here because later panes keep closing."""
    for (group, pane), (count, sum_value) in expected.items():
        body = out.get((group, pane))
        if body is None:
            raise AssertionError({"missing emission": (group, pane), "emitted": out})
        if body["count"] != count or int(body["sum"]) != sum_value:
            raise AssertionError({"emission mismatch": body, "expected": (count, sum_value)})


def main():
    subprocess.run([*COMPOSE, "down", "-v"], check=True)
    subprocess.run([*COMPOSE, "up", "-d", "--build"], check=True)
    try:
        wait_for_fabric()

        suffix = uuid.uuid4().hex[:8]
        domain = f"win{suffix}"
        source, results_feed = f"{domain}.source", f"{domain}.results"
        panes_store, retired_store = f"{domain}.panes", f"{domain}.retired"
        capped_store = f"{domain}.capped"
        byte_store = f"{domain}.bytecap"
        writer = f"writer{suffix}"
        # A Subscription is one shared durable frontier â€” members and
        # declared Pipes compete for the same pages. Each Pipe therefore
        # consumes its own Subscription over the shared source Feed.
        subscription, subscription_b = f"{domain}.flow", f"{domain}.flowb"
        subscription_c = f"{domain}.flowc"
        subscription_d = f"{domain}.flowd"
        totals_pipe, retire_pipe = f"{domain}.totals", f"{domain}.retire"
        capped_pipe = f"{domain}.capped"
        byte_pipe = f"{domain}.bytecap"
        results = wcl_retry(
            NODES[0],
            f"CREATE DOMAIN {domain}; "
            f"CREATE FEED {source}; "
            f"CREATE FEED {results_feed}; "
            f"DEFINE STATESTORE {panes_store} KEYS key, pane; "
            f"DEFINE STATESTORE {retired_store} KEYS key, pane; "
            f"CREATE WRITER {writer} TO {source}; "
            f"OPEN WRITER SESSION {writer}; "
            f"CREATE SUBSCRIPTION {subscription} FROM {source} START AT BEGINNING; "
            f"CREATE SUBSCRIPTION {subscription_b} FROM {source} START AT BEGINNING; "
            # Emitting pipe: closed panes publish to the results Feed and
            # their rows delete atomically with the emission.
            f"CREATE PIPE {totals_pipe} FROM SUBSCRIPTION {subscription} "
            f"TO STATESTORE {panes_store} WINDOW 8s EVENT MAX_LATE 20s "
            f"BY JSON 'customer' SUM JSON 'amount' EMIT TO FEED {results_feed}; "
            # Non-emitting pipe: closed panes never publish; EXPIRE is the
            # retention contract that reclaims them without further input.
            f"CREATE PIPE {retire_pipe} FROM SUBSCRIPTION {subscription_b} "
            f"TO STATESTORE {retired_store} WINDOW 8s EVENT MAX_LATE 20s "
            f"EXPIRE 40s BY JSON 'customer' COUNT; "
            # Capped pipe: LIMIT KEYS 1 admits only the smallest tagged
            # group to pane state; every other Key's records accumulate
            # the shared __keycap__ sentinel â€” bounded refusal evidence.
            f"DEFINE STATESTORE {capped_store} KEYS key, pane; "
            f"CREATE SUBSCRIPTION {subscription_c} FROM {source} START AT BEGINNING; "
            f"CREATE PIPE {capped_pipe} FROM SUBSCRIPTION {subscription_c} "
            f"TO STATESTORE {capped_store} WINDOW 8s EVENT MAX_LATE 20s "
            f"LIMIT KEYS 1 BY JSON 'customer' COUNT; "
            # Byte-capped pipe: LIMIT BYTES 130 admits three pane rows
            # (41-43 accounted bytes each) then refuses every further new
            # (group, pane) row into the shared __bytecap__ sentinel.
            f"DEFINE STATESTORE {byte_store} KEYS key, pane; "
            f"CREATE SUBSCRIPTION {subscription_d} FROM {source} START AT BEGINNING; "
            f"CREATE PIPE {byte_pipe} FROM SUBSCRIPTION {subscription_d} "
            f"TO STATESTORE {byte_store} WINDOW 8s EVENT MAX_LATE 20s "
            f"LIMIT BYTES 130 BY JSON 'customer' COUNT;",
        )
        writer_epoch = results[6]["session_epoch"]
        described = wcl(NODES[1], f"DESCRIBE PIPE {totals_pipe};")[0]
        pipe_id = described["pipe_id"]
        subscription_id = described["subscription_id"]

        # Anchor pane P0 ~1.5-2s in the future so batch-A records land in it
        # deterministically regardless of scheduling jitter: the pane index
        # derives from event_time_ns, never the arrival time.
        t0 = time.time_ns()
        # Margin: ingest_time is server-stamped at commit; under host load a
        # commit can trail the request by many seconds. Lateness is measured
        # ingest - event, so events need this much future headroom to keep
        # on-time records out of the lateness sentinel.
        p0 = (t0 + 30_000_000_000) // PANE_NS
        p0_start = p0 * PANE_NS

        # Batch A â€” three records inside pane P0.
        append_json(
            NODES[1],
            writer,
            writer_epoch,
            [
                ("acme", 10, p0_start + 100_000_000),
                ("acme", 20, p0_start + 200_000_000),
                ("globex", 5, p0_start + 300_000_000),
            ],
        )

        # Batch B ~3s later: an out-of-order straggler still inside P0
        # (lateness ~1s < MAX_LATE), plus events crossing three further pane
        # boundaries â€” every adjacent pane is a distinct row.
        time.sleep(3)
        batch_b = append_json(
            NODES[0],
            writer,
            writer_epoch,
            [
                ("acme", 30, p0_start + 400_000_000),    # still P0
                ("acme", 40, p0_start + 600_000_000),    # P0+1
                ("globex", 7, p0_start + 1_100_000_000), # P0+2
                ("acme", 100, p0_start + 1_600_000_000), # P0+3
            ],
        )

        # Wait until pane P0+3 provably closes: now > pane_end + MAX_LATE.
        close_at = p0_start + 2_000_000_000 + MAX_LATE_NS + 1_000_000_000
        wait_ns = close_at - time.time_ns()
        if wait_ns > 0:
            time.sleep(wait_ns / 1e9)

        # Batch C: a late record (~45s behind its event time, > MAX_LATE)
        # quarantines into the sentinel pane; a fresh event lands in a pane
        # that stays open until its own close; an i64::MAX event lands in a
        # pane whose close-boundary arithmetic overflows â€” fail-closed Keep,
        # it can never emit.
        t_c = time.time_ns()
        pfresh = pane_of(t_c + 30_000_000_000)
        append_json(
            NODES[2],
            writer,
            writer_epoch,
            [
                # ~45s behind its event time — comfortably past the widened
                # 20s MAX_LATE even when commits stall under host load.
                ("lateco", 999, t_c - 45_000_000_000),
                ("acme", 1, t_c + 30_000_000_000),
                ("edge", 1, I64_MAX),
            ],
        )

        expected = {
            ("acme", p0): (3, 60),
            ("globex", p0): (1, 5),
            ("acme", p0 + 1): (1, 40),
            ("globex", p0 + 2): (1, 7),
            ("acme", p0 + 3): (1, 100),
        }
        emitted = emissions(wait_records(results_feed, 5))
        assert_subset(emitted, expected)

        # The sentinel pane never emits; neither does the overflow pane.
        if any(pane < 0 or pane >= 2**63 for (_, pane) in emitted):
            raise AssertionError({"emitted closed/invalid pane": emitted})

        # Sentinel evidence: one late row for group lateco, count 1, sum 0.
        # The closed-pane emissions can land before batch C is consumed â€”
        # idle drives emit them during the close wait â€” so the sentinel is
        # awaited, not assumed present.
        sentinels = []
        deadline = time.time() + 60
        while time.time() < deadline:
            sentinels = sentinel_rows(NODES[0], panes_store)
            if len(sentinels) == 1:
                break
            time.sleep(0.5)
        if len(sentinels) != 1:
            raise AssertionError({"sentinel rows": sentinels})
        group_tagged, pane = parse_row_key(sentinels[0]["key_base64"])
        if pane != 2**64 - 1 or decode_group(group_tagged) != "lateco":
            raise AssertionError({"sentinel row": sentinels[0]})
        if decode_accumulator(sentinels[0]["payload_base64"]) != (1, 0):
            raise AssertionError({"sentinel accumulator": sentinels[0]})

        # Same-request retry: identical request_ids dedupe at the Feed, so
        # the Pipe never re-accumulates them â€” a failed dedup would land the
        # reposted records as lateness > MAX_LATE and grow the sentinel set.
        repost_batch(NODES[1], batch_b)
        settle(results_feed)
        if len(sentinel_rows(NODES[0], panes_store)) != 1:
            raise AssertionError("request retry re-accumulated into the sentinel")

        # The fresh batch-C pane closes on its own boundary and emits too.
        expected[("acme", pfresh)] = (1, 1)
        emitted = emissions(wait_records(results_feed, 6))
        assert_subset(emitted, expected)

        # Bounded state: emitted panes deleted; only the overflow pane row
        # (and possibly the not-yet-closed fresh pane) remains alongside the
        # sentinel â€” never a re-materialized closed pane. The read may land
        # on a replica still applying the emission deletes, so converge.
        remaining_groups = None
        for _ in range(30):
            open_rows = pane_rows(NODES[1], panes_store)
            remaining_groups = sorted(
                decode_group(parse_row_key(row["key_base64"])[0])
                for row in open_rows
            )
            if remaining_groups in (["edge"], ["acme", "edge"]):
                break
            time.sleep(1)
        if remaining_groups not in (["edge"], ["acme", "edge"]):
            raise AssertionError({"unbounded pane rows": open_rows})

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

        # Post-move window: fresh input still aggregates and closes through
        # the moved owner.
        t2 = time.time_ns()
        pmoved = pane_of(t2 + 30_000_000_000)
        append_json(
            NODES[0],
            writer,
            writer_epoch,
            [("globex", 3, t2 + 30_000_000_000)],
            retries=45,
        )
        expected[("globex", pmoved)] = (1, 3)
        emitted = emissions(wait_records(results_feed, 7, deadline=90))
        assert_subset(emitted, expected)

        # Per-Domain Pipe cost attribution, read while every drive Node is
        # still up: telemetry is Node-local since process start, so the
        # hard restart below would wipe the rows this proves. The merge
        # sums per-Node counters and max-merges *_last estimates.
        merged, domains = pipe_demand()
        assert totals_pipe in merged, "no Node reported Pipe drive telemetry"
        totals_row = merged[totals_pipe]
        assert totals_row["domain"] == domain
        assert totals_row["operation"] == "window"
        telemetry = totals_row["telemetry"]
        assert telemetry["drives"] > 0
        assert telemetry["applied"] > 0
        # 11 records appended so far (batches A-C plus the post-move one).
        assert telemetry["records_fetched"] >= 11, {"totals": telemetry}
        assert telemetry["emissions"] >= 5
        assert telemetry["outputs"] >= 5
        assert telemetry["state_mutations"] >= 5
        assert telemetry["drive_wall_ns_total"] > 0
        assert not totals_row["backfilling"]
        # Both capped Pipes divert refused records into sentinel rows; the
        # applied mutations carry the refused-Record totals: LIMIT KEYS
        # refused every non-acme record, LIMIT BYTES refused every new
        # (group, pane) past its three admitted rows.
        keycap_row = merged[capped_pipe]
        assert keycap_row["telemetry"]["records_refused"] >= 5, {
            "keycap": keycap_row["telemetry"]
        }
        byte_row = merged[byte_pipe]
        assert byte_row["telemetry"]["records_refused"] >= 5, {
            "bytecap": byte_row["telemetry"]
        }
        assert byte_row["telemetry"]["pane_rows_last"] >= 3
        assert byte_row["telemetry"]["pane_bytes_last"] >= 125
        rollup = domains[domain]
        assert rollup["pipes"] >= 3
        # Four Pipes each fetch the same 11 records through their own
        # Subscriptions, so the Domain rollup exceeds the per-Pipe count.
        assert rollup["records_fetched"] >= 11
        assert rollup["drive_wall_ns_total"] > 0

        # Hard restart of the progress owner: a member join recovers the
        # lost owner through the placement authority, and the Pipe resumes
        # on the surviving owner without re-emitting acknowledged panes.
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

        t3 = time.time_ns()
        pdone = pane_of(t3 + 30_000_000_000)
        append_json(
            NODES[2],
            writer,
            writer_epoch,
            [("acme", 9, t3 + 30_000_000_000)],
            retries=45,
        )
        expected[("acme", pdone)] = (1, 9)
        emitted = emissions(wait_records(results_feed, 8, deadline=90))
        assert_subset(emitted, expected)
        if len(emitted) != 8:
            raise AssertionError({"unexpected extra emissions": emitted})

        # Re-drive idempotency is the supervisor's own behavior: at a 200ms
        # drive interval each settle window is dozens of re-drives over a
        # caught-up frontier, and replay rides the committed Declare rather
        # than re-resolving panes. Any double-apply would surface here.
        if len(settle(results_feed)) != 8:
            raise AssertionError("re-drive emitted extra outputs")

        # No-input expiry: the non-emitting Pipe's pane rows (fed by the
        # same source) delete at pane_end + EXPIRE on idle drives â€” the
        # sentinel and the overflow pane are kept as designed.
        expiry_deadline = (pdone + 1) * PANE_NS + 40_000_000_000 + 30_000_000_000
        while time.time_ns() < expiry_deadline:
            rows = pane_rows(NODES[2], retired_store)
            if len(rows) == 1:
                group_tagged, _ = parse_row_key(rows[0]["key_base64"])
                if decode_group(group_tagged) == "edge":
                    break
            time.sleep(1)
        else:
            raise AssertionError(
                {"expired rows never deleted": pane_rows(NODES[2], retired_store)}
            )
        if len(sentinel_rows(NODES[0], retired_store)) != 1:
            raise AssertionError("sentinel row expired or duplicated")

        # LIMIT KEYS: the capped Pipe folds the same 12 source records but
        # admits only "acme" (the smallest tagged group) to pane state.
        # The five non-acme records accumulate the single shared
        # `__keycap__` sentinel row â€” countable refusal evidence, and the
        # bound holds no matter how many distinct Keys arrive.
        capped_sentinels, capped_groups = [], None
        deadline = time.time() + 120
        while time.time() < deadline:
            capped_sentinels = sentinel_rows(NODES[0], capped_store)
            capped_groups = {
                decode_group(parse_row_key(row["key_base64"])[0])
                for row in pane_rows(NODES[0], capped_store)
            }
            if (
                len(capped_sentinels) == 1
                and capped_groups == {"acme"}
                and decode_accumulator(capped_sentinels[0]["payload_base64"])
                == (5, 0)
            ):
                break
            time.sleep(1)
        else:
            raise AssertionError(
                {
                    "capped pane rows": pane_rows(NODES[0], capped_store),
                    "capped sentinels": capped_sentinels,
                }
            )
        keycap_tagged, keycap_pane = parse_row_key(
            capped_sentinels[0]["key_base64"]
        )
        if keycap_pane != 2**64 - 1 or decode_group(keycap_tagged) != "__keycap__":
            raise AssertionError({"keycap sentinel": capped_sentinels[0]})

        # LIMIT BYTES: the byte-capped Pipe admits the first three pane
        # rows it encounters â€” (acme,P0), (globex,P0), (acme,P0+1) at
        # 41+43+41=125 accounted bytes â€” then every later new (group,
        # pane) row refuses into the one shared `__bytecap__` sentinel:
        # globex P0+2, acme P0+3, lateco's late row, acme pfresh, edge's
        # overflow pane, and the post-move/restart records â€” 7 refused
        # records total. Merges into admitted rows never grow the budget.
        byte_sentinels, byte_panes = [], None
        deadline = time.time() + 120
        while time.time() < deadline:
            byte_sentinels = sentinel_rows(NODES[0], byte_store)
            byte_panes = pane_rows(NODES[0], byte_store)
            if (
                len(byte_sentinels) == 1
                and len(byte_panes) == 3
                and decode_accumulator(byte_sentinels[0]["payload_base64"])
                == (7, 0)
            ):
                break
            time.sleep(1)
        else:
            raise AssertionError(
                {
                    "byte-capped pane rows": byte_panes,
                    "byte-capped sentinels": byte_sentinels,
                }
            )
        byte_groups = sorted(
            decode_group(parse_row_key(row["key_base64"])[0])
            for row in byte_panes
        )
        if byte_groups != ["acme", "acme", "globex"]:
            raise AssertionError({"admitted rows": byte_groups})
        bytecap_tagged, bytecap_pane = parse_row_key(
            byte_sentinels[0]["key_base64"]
        )
        if bytecap_pane != 2**64 - 1 or decode_group(bytecap_tagged) != "__bytecap__":
            raise AssertionError({"bytecap sentinel": byte_sentinels[0]})

        # Post-restart attribution: the recovered compute-lease owner
        # records fresh drives for the same Pipes â€” the restarted Node's
        # wiped row is honest, not a loss, and the recovered owner's row
        # proves attribution keeps following the lease.
        merged, _ = pipe_demand()
        assert totals_pipe in merged, "Pipe telemetry vanished post-restart"
        telemetry = merged[totals_pipe]["telemetry"]
        assert telemetry["drives"] > 0

        print(
            json.dumps(
                {
                    "status": "ok",
                    "emissions": len(emitted),
                    "late_quarantined": 1,
                    "bounded_open_rows": remaining_groups,
                    "owner_before_drain": owner,
                    "owner_after_drain": new_owner,
                    "owner_after_restart": recovered_owner,
                    "keycap_refused": decode_accumulator(
                        capped_sentinels[0]["payload_base64"]
                    )[0],
                    "bytecap_refused": decode_accumulator(
                        byte_sentinels[0]["payload_base64"]
                    )[0],
                    "note": (
                        "window Pipe folded multi-pane event-time input into "
                        "RF3 StateStore rows, emitted five closed panes with "
                        "count/sum/store_seq evidence, quarantined the late "
                        "record into the sentinel pane, kept the overflow "
                        "pane fail-closed, deduplicated a same-request "
                        "repost, stayed exactly-once across drain and a hard "
                        "progress-owner restart, and expired non-emitting "
                        "pane rows without further input"
                    ),
                },
                indent=2,
            )
        )
    except BaseException:
        # Diagnostics before teardown: the whole observable state of both
        # stores and both feeds, so a failure is diagnosable from logs.
        # Each probe is individually guarded â€” a wedged endpoint must not
        # mask the rest of the evidence.
        def probe(fn):
            try:
                return fn()
            except Exception as inner:
                return repr(inner)

        diag = {
            "source_records": probe(lambda: len(read(NODES[0], source))),
            "result_records": probe(lambda: read(NODES[1], results_feed)),
            "panes_store_real": probe(lambda: pane_rows(NODES[0], panes_store)),
            "panes_store_sentinel": probe(
                lambda: sentinel_rows(NODES[0], panes_store)
            ),
            "retired_store_real": probe(lambda: pane_rows(NODES[0], retired_store)),
            "retired_store_sentinel": probe(
                lambda: sentinel_rows(NODES[0], retired_store)
            ),
            "capped_store_real": probe(lambda: pane_rows(NODES[0], capped_store)),
            "capped_store_sentinel": probe(
                lambda: sentinel_rows(NODES[0], capped_store)
            ),
        }
        print(f"DIAGNOSTICS {json.dumps(diag, indent=2, default=str)}")
        raise
    finally:
        subprocess.run([*COMPOSE, "down", "-v"], check=True)


if __name__ == "__main__":
    main()
