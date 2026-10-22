import base64, json, os, sys, time, urllib.error, urllib.parse, urllib.request, uuid

BASE_PORT = int(os.environ.get("WHITEWATER_TEST_BASE_PORT", "7071"))
# WHITEWATER_TEST_ENDPOINTS overrides the default localhost base-port
# derivation so the script can run inside a Compose network; it is separate
# from the SDK's WHITEWATER_ENDPOINTS so a developer environment can never
# redirect this fault test at a real Riverbed.
_endpoints_env = os.environ.get("WHITEWATER_TEST_ENDPOINTS")
ENDPOINTS = [entry if "://" in entry else f"http://{entry}" for entry in _endpoints_env.split(",")] if _endpoints_env else [f"http://localhost:{BASE_PORT + offset}" for offset in range(3)]
KEY = "whitewater-local-development-admin-key"
MEMBERS = "/v1/subscriptions/members"
TRANSIENT = {"no_quorum", "unavailable", "ambiguous_commit", "invalid_assignment"}

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
def post(endpoint, path, body):
    request = urllib.request.Request(endpoint + path, data=json.dumps(body).encode(), headers={"authorization": f"Bearer {KEY}", "content-type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=60) as response: return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error: return error.code, json.loads(error.read() or b"{}")
    except OSError:
        return 0, {}
def main():
    # Fresh cutovers can take a moment to propagate progress placements and
    # session registration; identical request_id retries stay idempotent.
    # Connection failures and 5xx unavailability retry alongside the typed
    # transient codes; other refusals return at once for the caller to assert.
    def retried_post(endpoint, path, body):
        for _ in range(120):
            status, result = post(endpoint, path, body)
            if status == 200 or (
                status
                and status < 500
                and result.get("code") not in TRANSIENT
            ):
                return status, result
            time.sleep(0.5)
        return status, result

    suffix = f"{int(time.time())}{uuid.uuid4().hex[:6]}"
    domain, feed, writer, reader = f"cyc{suffix}", f"cyc{suffix}.events", f"cycw{suffix}", f"cyr{suffix}"
    subscription = f"{domain}.drain"
    status, setup = retried_post(ENDPOINTS[0], "/v1/admin/wcl", {"request_id": str(uuid.uuid4()), "script": f"CREATE DOMAIN {domain}; CREATE FEED {feed}; CREATE WRITER {writer} TO {feed}; OPEN WRITER SESSION {writer}; CREATE READER {reader} FROM {feed} START AT BEGINNING; CREATE SUBSCRIPTION {subscription} FROM {feed} START AT BEGINNING;"})
    if status != 200: raise AssertionError(setup)
    epoch = setup["results"][3]["data"]["session_epoch"]

    cursors = []
    def append(tag, endpoint):
        # Three shared Keys per cycle so the merged committed order proves
        # per-Key ordering survives every split/merge transition, not just
        # aggregate feed order.
        key = f"{tag}-key-{len(cursors) % 3}"
        status, result = retried_post(endpoint, "/v1/writers/append", {"request_id": str(uuid.uuid4()), "writer": writer, "session_epoch": epoch, "event_time_ns": str(len(cursors)), "key_base64": base64.b64encode(key.encode()).decode(), "payload_base64": base64.b64encode(f"value-{len(cursors)}".encode()).decode(), "metadata_base64": {}})
        if status != 200: raise AssertionError(result)
        cursors.append(result["cursor"])

    def placement_routes():
        status, placement = retried_post(ENDPOINTS[2], "/v1/admin/wcl", {"request_id": str(uuid.uuid4()), "script": f"INSPECT PLACEMENT FOR FEED {feed};"})
        if status != 200: raise AssertionError(placement)
        return placement["results"][0]["data"]["range_map"]["routes"]

    reader_epoch = None
    reader_acked = 0
    def reader_drain(limit=64):
        nonlocal reader_epoch, reader_acked
        status, opened = retried_post(ENDPOINTS[0], "/v1/readers/open", {"request_id": str(uuid.uuid4()), "reader": reader, "capacity": limit})
        if status != 200: raise AssertionError(opened)
        reader_epoch = opened["session_epoch"]
        status, page = retried_post(ENDPOINTS[1], "/v1/readers/fetch", {"request_id": str(uuid.uuid4()), "reader": reader, "session_epoch": reader_epoch, "limit": limit})
        if status != 200: raise AssertionError(page)
        expected = cursors[reader_acked:reader_acked + limit]
        if [row["cursor"] for row in page["records"]] != expected: raise AssertionError(f"Reader diverged from committed history: {page}")
        if page["records"]:
            status, ack = retried_post(ENDPOINTS[2], "/v1/readers/ack", {"request_id": str(uuid.uuid4()), "reader": reader, "session_epoch": reader_epoch, "cursor": page["delivered_cursor"]})
            if status != 200: raise AssertionError(ack)
            reader_acked += len(page["records"])
        return len(page["records"])

    member = str(uuid.uuid4())
    member_epoch = None
    member_delivered = []
    def member_drain():
        nonlocal member_epoch
        status, joined = retried_post(ENDPOINTS[1], f"{MEMBERS}/join", {"subscription": subscription, "request_id": str(uuid.uuid4()), "member_id": member})
        if status != 200: raise AssertionError(joined)
        member_epoch = joined["member_epoch"]
        for _ in range(12):
            work = str(uuid.uuid4())
            status, claimed = retried_post(ENDPOINTS[2], f"{MEMBERS}/claim", {"subscription": subscription, "request_id": str(uuid.uuid4()), "member_id": member, "member_epoch": member_epoch, "work_id": work, "lease_ticks": 60})
            if status != 200: raise AssertionError(claimed)
            lease = claimed["lease"]["lease_epoch"]
            status, page = retried_post(ENDPOINTS[0], f"{MEMBERS}/fetch", {"subscription": subscription, "request_id": str(uuid.uuid4()), "member_id": member, "member_epoch": member_epoch, "work_id": work, "lease_epoch": lease, "limit": 64})
            if status != 200: raise AssertionError(page)
            if not page["records"]:
                status, _ = post(ENDPOINTS[1], f"{MEMBERS}/release", {"subscription": subscription, "request_id": str(uuid.uuid4()), "member_id": member, "member_epoch": member_epoch, "work_id": work, "lease_epoch": lease})
                break
            status, acked = retried_post(ENDPOINTS[1], f"{MEMBERS}/ack", {"subscription": subscription, "request_id": str(uuid.uuid4()), "member_id": member, "member_epoch": member_epoch, "work_id": work, "lease_epoch": lease, "cursor": page["cursor"], "positions": page["positions"]})
            if status != 200: raise AssertionError(acked)
            member_delivered.extend(row["cursor"] for row in page["records"])
            if member_delivered != cursors[:len(member_delivered)]: raise AssertionError("Subscription reordered or skipped committed history")

    # Three split/merge cycles at rotating token boundaries, each under
    # continuing write traffic, with both a named Reader and a Subscription
    # member draining across every transition.
    boundaries = ["80000000000000000000000000000000", "40000000000000000000000000000000", "c0000000000000000000000000000000"]
    stale_fences = 0
    for cycle, boundary in enumerate(boundaries):
        for index in range(12):
            append(f"c{cycle}", ENDPOINTS[index % 3])
        reader_drain()
        member_drain()
        status, split = retried_post(ENDPOINTS[1], "/v1/admin/ranges/split", {"request_id": str(uuid.uuid4()), "feed": feed, "split_at": boundary, "batch_size": 4})
        if status != 200 or split.get("status") != "activated": raise AssertionError(f"cycle {cycle} split: {split}")
        # The translated frontier rode inside the activation command as
        # durable pending evidence; the post-activation apply must have
        # committed it to the progress shard.
        statuses = [entry.get("status") for entry in split.get("subscription_frontiers", []) if entry.get("subscription") == subscription]
        if split.get("subscription_reconcile_failed") or statuses not in (["reconciled"], ["current"], []): raise AssertionError(f"cycle {cycle} split did not apply the pending Subscription translation: {split}")
        if len(placement_routes()) != 2: raise AssertionError(f"cycle {cycle} did not leave two Active Ranges")
        # The pre-transition Reader epoch is fenced â€” a stale ack must refuse.
        stale = {"request_id": str(uuid.uuid4()), "reader": reader, "session_epoch": reader_epoch, "cursor": "AAAA"}
        status, refused = post(ENDPOINTS[0], "/v1/readers/ack", stale)
        if status == 200: raise AssertionError(f"cycle {cycle} accepted a stale-epoch Reader ack: {refused}")
        stale_fences += 1
        reader_drain()
        member_drain()
        for index in range(8):
            append(f"c{cycle}s", ENDPOINTS[index % 3])
        reader_drain()
        routes = placement_routes()
        status, merged = retried_post(ENDPOINTS[2], "/v1/admin/ranges/merge", {"request_id": str(uuid.uuid4()), "feed": feed, "left_range_id": routes[0]["range_id"], "right_range_id": routes[1]["range_id"]})
        if status != 200 or merged.get("status") != "activated": raise AssertionError(f"cycle {cycle} merge: {merged}")
        statuses = [entry.get("status") for entry in merged.get("subscription_frontiers", []) if entry.get("subscription") == subscription]
        if merged.get("subscription_reconcile_failed") or statuses not in (["reconciled"], ["current"], []): raise AssertionError(f"cycle {cycle} merge did not apply the pending Subscription translation: {merged}")
        if len(placement_routes()) != 1: raise AssertionError(f"cycle {cycle} did not restore one Active Range")
        reader_drain()
        member_drain()

    encoded = urllib.parse.quote(feed, safe="")
    def read_history():
        for _ in range(60):
            try:
                with urllib.request.urlopen(urllib.request.Request(f"{ENDPOINTS[0]}/v1/feeds/records?feed={encoded}&limit=200", headers={"authorization": f"Bearer {KEY}"}), timeout=30) as response:
                    return json.loads(response.read())
            except urllib.error.HTTPError as error:
                if error.code < 500:
                    raise
            except (ConnectionError, urllib.error.URLError, TimeoutError, OSError):
                pass
            time.sleep(0.5)
        raise AssertionError("Feed history stayed unavailable through the transient-retry window")
    records = read_history()
    if [record["cursor"] for record in records] != cursors: raise AssertionError("repeated split/merge cycles lost or reordered immutable Feed history")
    if member_delivered != cursors: raise AssertionError("the Subscription frontier did not drain every record exactly once across three transitions")
    print(json.dumps({
        "status": "ok",
        "feed": feed,
        "cycles": len(boundaries),
        "records": len(cursors),
        "reader_acked": reader_acked,
        "member_delivered": len(member_delivered),
        "stale_epoch_fences": stale_fences,
        "note": "one Feed survived three split/merge cycles at rotating token boundaries under continuing writes; the named Reader resumed unacknowledged history in order after every transition, stale epochs fenced, and the Subscription member drained every committed record exactly once in committed order",
    }, indent=2))

if __name__ == "__main__":
    try: main()
    except Exception as error: print(f"M4 repeated split/merge cycle failed: {error}", file=sys.stderr); sys.exit(1)
