import json, os, subprocess, sys, time, urllib.error, urllib.request, uuid

BASE_PORT = int(os.environ.get("WHITEWATER_TEST_BASE_PORT", "7071"))
ENDPOINTS = [f"http://localhost:{BASE_PORT + offset}" for offset in range(3)]
KEY = "whitewater-local-development-admin-key"

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
    # Transient unavailability (election windows, quorum stalls, restart
    # gaps) retries with the same body â€” stable request_id values in the
    # payload keep the retry idempotent. Refusals (4xx) return at once
    # for the caller to assert.
    for _ in range(120):
        status, result = _post_once(endpoint, path, body)
        if status == 0 or status >= 500:
            time.sleep(0.5)
            continue
        return status, result
    return status, result

def _post_once(endpoint, path, body):
    request = urllib.request.Request(endpoint + path, data=json.dumps(body).encode(), headers={"authorization": f"Bearer {KEY}", "content-type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=30) as response: return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error: return error.code, json.loads(error.read() or b"{}")
    except OSError:
        return 0, {}
@_retried
def get(endpoint, path):
    for _ in range(120):
        status, result = _get_once(endpoint, path)
        if status == 0 or status >= 500:
            time.sleep(0.5)
            continue
        return status, result
    return status, result

def _get_once(endpoint, path):
    request = urllib.request.Request(endpoint + path, headers={"authorization": f"Bearer {KEY}"})
    try:
        with urllib.request.urlopen(request, timeout=30) as response: return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error: return error.code, json.loads(error.read() or b"{}")
    except OSError:
        return 0, {}
def wait_ready():
    for _ in range(60):
        try:
            status, _ = post(ENDPOINTS[0], "/v1/admin/wcl", {"request_id": str(uuid.uuid4()), "script": "SHOW DOMAINS;"})
            if status == 200:
                return
        except (ConnectionError, urllib.error.URLError, TimeoutError):
            pass
        time.sleep(1)
    raise AssertionError("verification Riverbed did not answer WCL")


def member_ready(endpoint, path, body):
    # Stopping a voter can drop the Control Plane leader; the survivor must
    # finish re-electing before member mutations succeed again. Ride out the
    # election window â€” every retry replays the same request_id, so repeats
    # are idempotent.
    delay = 0.25
    for attempt in range(45):
        status, response = post(endpoint, path, body)
        if status == 200:
            return response
        if status != 503:
            raise AssertionError(f"{path} returned HTTP {status}: {response}")
        if attempt < 44:
            time.sleep(delay)
            delay = min(delay * 2, 1.0)
    raise AssertionError(f"member call still failing after re-election window: {response}")


def phase(name):
    print(f"[m7] {name}", file=sys.stderr, flush=True)


def main():
    wait_ready()
    phase("fixture ready")
    suffix = f"{int(time.time())}{uuid.uuid4().hex[:6]}"
    domain, feed, writer = f"m7{suffix}", f"m7{suffix}.events", f"writer{suffix}"
    subscription = f"{domain}.billing"
    status, setup = post(ENDPOINTS[0], "/v1/admin/wcl", {"request_id": str(uuid.uuid4()), "script": f"CREATE DOMAIN {domain}; CREATE FEED {feed}; CREATE WRITER {writer} TO {feed}; OPEN WRITER SESSION {writer}; CREATE SUBSCRIPTION {subscription} FROM {feed} START AT BEGINNING;"})
    if status != 200: raise AssertionError(setup)
    writer_epoch = setup["results"][3]["data"]["session_epoch"]
    records = [{"request_id": str(uuid.uuid4()), "writer": writer, "session_epoch": writer_epoch, "event_time_ns": str(index), "key_base64": "aw==", "payload_base64": "dg==", "metadata_base64": {}} for index in range(1, 6)]
    status, batch = post(ENDPOINTS[1], "/v1/writers/append-batch", {"records": records})
    if status != 200 or len(batch["results"]) != 5: raise AssertionError(batch)

    member = str(uuid.uuid4())
    work = str(uuid.uuid4())
    members = "/v1/subscriptions/members"

    # Unknown Subscription is rejected before placement work begins.
    status, missing = post(ENDPOINTS[0], f"{members}/join", {"subscription": f"{domain}.missing", "request_id": str(uuid.uuid4()), "member_id": member})
    if status != 400: raise AssertionError(missing)

    # The first join atomically establishes the beginning frontier and epoch 1.
    join_request = str(uuid.uuid4())
    joined = member_ready(ENDPOINTS[1], f"{members}/join", {"subscription": subscription, "request_id": join_request, "member_id": member})
    if joined["member_epoch"] != 1:  raise AssertionError(joined)
    replay = member_ready(ENDPOINTS[2], f"{members}/join", {"subscription": subscription, "request_id": join_request, "member_id": member})
    if replay["member_epoch"] != 1:  raise AssertionError(f"join replay returned a different epoch: {replay}")

    # A fresh join request fences the earlier member epoch.
    rejoined = member_ready(ENDPOINTS[0], f"{members}/join", {"subscription": subscription, "request_id": str(uuid.uuid4()), "member_id": member})
    if rejoined["member_epoch"] != 2:  raise AssertionError(rejoined)

    # The fenced epoch cannot claim work.
    status, stale = post(ENDPOINTS[1], f"{members}/claim", {"subscription": subscription, "request_id": str(uuid.uuid4()), "member_id": member, "member_epoch": 1, "work_id": work})
    if status != 409: raise AssertionError(stale)

    # The live member claims work and fetches a bounded page after the frontier.
    claimed = member_ready(ENDPOINTS[2], f"{members}/claim", {"subscription": subscription, "request_id": str(uuid.uuid4()), "member_id": member, "member_epoch": 2, "work_id": work, "lease_ticks": 60})
    if not claimed["lease"]:  raise AssertionError(claimed)
    lease_epoch = claimed["lease"]["lease_epoch"]
    page = member_ready(ENDPOINTS[0], f"{members}/fetch", {"subscription": subscription, "request_id": str(uuid.uuid4()), "member_id": member, "member_epoch": 2, "work_id": work, "lease_epoch": lease_epoch, "limit": 3})
    if len(page["records"]) != 3 or not page["cursor"]:  raise AssertionError(page)

    # A stale lease epoch cannot fetch or acknowledge.
    status, refused = post(ENDPOINTS[1], f"{members}/fetch", {"subscription": subscription, "request_id": str(uuid.uuid4()), "member_id": member, "member_epoch": 2, "work_id": work, "lease_epoch": lease_epoch + 99})
    if status != 409: raise AssertionError(refused)

    # The valid grant acknowledges the delivered page atomically.
    acked = member_ready(ENDPOINTS[2], f"{members}/ack", {"subscription": subscription, "request_id": str(uuid.uuid4()), "member_id": member, "member_epoch": 2, "work_id": work, "lease_epoch": lease_epoch, "cursor": page["cursor"], "positions": page["positions"]})

    phase("join/claim/fetch/ack verified")

    # A lease transfer hands claimed work to another member and fences the
    # source holder for both fetch and acknowledge. The remaining two
    # records are delivered under the source lease, then acknowledged only
    # by the transfer target.
    member_b = str(uuid.uuid4())
    joined_b = member_ready(ENDPOINTS[1], f"{members}/join", {"subscription": subscription, "request_id": str(uuid.uuid4()), "member_id": member_b})
    if joined_b["member_epoch"] != 1:  raise AssertionError(joined_b)
    work_b = str(uuid.uuid4())
    claimed_b = member_ready(ENDPOINTS[0], f"{members}/claim", {"subscription": subscription, "request_id": str(uuid.uuid4()), "member_id": member, "member_epoch": 2, "work_id": work_b, "lease_ticks": 60})
    if not claimed_b["lease"]:  raise AssertionError(claimed_b)
    lease_b = claimed_b["lease"]["lease_epoch"]
    page_b = member_ready(ENDPOINTS[2], f"{members}/fetch", {"subscription": subscription, "request_id": str(uuid.uuid4()), "member_id": member, "member_epoch": 2, "work_id": work_b, "lease_epoch": lease_b})
    if len(page_b["records"]) != 2:  raise AssertionError(page_b)
    moved = member_ready(ENDPOINTS[1], f"{members}/transfer", {"subscription": subscription, "request_id": str(uuid.uuid4()), "work_id": work_b, "member_id": member, "lease_epoch": lease_b, "to_member_id": member_b, "to_member_epoch": joined_b["member_epoch"], "lease_ticks": 60})
    if not moved["lease"]:  raise AssertionError(moved)
    # The fenced holder can neither fetch nor acknowledge with the stale lease.
    status, refused = post(ENDPOINTS[0], f"{members}/fetch", {"subscription": subscription, "request_id": str(uuid.uuid4()), "member_id": member, "member_epoch": 2, "work_id": work_b, "lease_epoch": lease_b})
    if status != 409: raise AssertionError(refused)
    status, refused = post(ENDPOINTS[1], f"{members}/ack", {"subscription": subscription, "request_id": str(uuid.uuid4()), "member_id": member, "member_epoch": 2, "work_id": work_b, "lease_epoch": lease_b, "cursor": page_b["cursor"], "positions": page_b["positions"]})
    if status != 409: raise AssertionError(refused)
    # The new holder re-delivers and acknowledges the same unacked page; the
    # released transferred lease cannot fetch again.
    new_lease_b = moved["lease"]["lease_epoch"]
    redelivered = member_ready(ENDPOINTS[2], f"{members}/fetch", {"subscription": subscription, "request_id": str(uuid.uuid4()), "member_id": member_b, "member_epoch": joined_b["member_epoch"], "work_id": work_b, "lease_epoch": new_lease_b})
    if len(redelivered["records"]) != 2:  raise AssertionError(redelivered)
    acked = member_ready(ENDPOINTS[0], f"{members}/ack", {"subscription": subscription, "request_id": str(uuid.uuid4()), "member_id": member_b, "member_epoch": joined_b["member_epoch"], "work_id": work_b, "lease_epoch": new_lease_b, "cursor": redelivered["cursor"], "positions": redelivered["positions"]})
    status, released = post(ENDPOINTS[2], f"{members}/fetch", {"subscription": subscription, "request_id": str(uuid.uuid4()), "member_id": member_b, "member_epoch": joined_b["member_epoch"], "work_id": work_b, "lease_epoch": new_lease_b})
    if status != 409: raise AssertionError(released)
    # A fresh claim reaches the acknowledged end of the Feed.
    tailclaim = member_ready(ENDPOINTS[0], f"{members}/claim", {"subscription": subscription, "request_id": str(uuid.uuid4()), "member_id": member, "member_epoch": 2, "work_id": str(uuid.uuid4())})
    tail = member_ready(ENDPOINTS[2], f"{members}/fetch", {"subscription": subscription, "request_id": str(uuid.uuid4()), "member_id": member, "member_epoch": 2, "work_id": tailclaim["lease"]["work_id"], "lease_epoch": tailclaim["lease"]["lease_epoch"]})
    if tail["records"]:  raise AssertionError(tail)

    phase("lease transfer fencing verified")

    # A `now` Subscription pins the committed tail at first join: earlier
    # records are skipped and only post-join appends are delivered.
    now_sub = f"{domain}.realtime"
    status, declared = post(ENDPOINTS[0], "/v1/admin/wcl", {"request_id": str(uuid.uuid4()), "script": f"CREATE SUBSCRIPTION {now_sub} FROM {feed} START AT NOW;"})
    if status != 200: raise AssertionError(declared)
    joined = member_ready(ENDPOINTS[1], f"{members}/join", {"subscription": now_sub, "request_id": str(uuid.uuid4()), "member_id": member})
    if joined["member_epoch"] != 1:  raise AssertionError(joined)
    claimed = member_ready(ENDPOINTS[2], f"{members}/claim", {"subscription": now_sub, "request_id": str(uuid.uuid4()), "member_id": member, "member_epoch": 1, "work_id": work})
    page = member_ready(ENDPOINTS[0], f"{members}/fetch", {"subscription": now_sub, "request_id": str(uuid.uuid4()), "member_id": member, "member_epoch": 1, "work_id": work, "lease_epoch": claimed["lease"]["lease_epoch"]})
    if page["records"]:  raise AssertionError(f"now Subscription delivered pre-join records: {page}")
    status, appended = post(ENDPOINTS[1], "/v1/writers/append", {"request_id": str(uuid.uuid4()), "writer": writer, "session_epoch": writer_epoch, "event_time_ns": "6", "key_base64": "aw==", "payload_base64": "dg==", "metadata_base64": {}})
    if status != 200: raise AssertionError(appended)
    page = member_ready(ENDPOINTS[2], f"{members}/fetch", {"subscription": now_sub, "request_id": str(uuid.uuid4()), "member_id": member, "member_epoch": 1, "work_id": work, "lease_epoch": claimed["lease"]["lease_epoch"], "wait_ms": 2000})
    if len(page["records"]) != 1:  raise AssertionError(page)
    phase("now start verified")

    # A standalone Reader exposes each record's opaque Cursor and ingest
    # timestamp; explicit-Cursor and timestamp Subscription starts resolve
    # them into internal per-Range positions at first join.
    timeline_reader = f"m7timeline{suffix}"
    status, declared = post(ENDPOINTS[0], "/v1/admin/wcl", {"request_id": str(uuid.uuid4()), "script": f"CREATE READER {timeline_reader} FROM {feed} START AT BEGINNING;"})
    if status != 200: raise AssertionError(declared)
    status, opened = post(ENDPOINTS[0], "/v1/readers/open", {"request_id": str(uuid.uuid4()), "reader": timeline_reader, "capacity": 10})
    if status != 200: raise AssertionError(opened)
    status, timeline = post(ENDPOINTS[0], "/v1/readers/fetch", {"request_id": str(uuid.uuid4()), "reader": timeline_reader, "session_epoch": opened["session_epoch"], "limit": 10})
    if status != 200 or len(timeline["records"]) != 6: raise AssertionError(timeline)
    history = timeline["records"]

    # A timestamp start pins every internal Range to the last frame ingested
    # before the threshold; the first delivery is the first record
    # at-or-after it on the ingest clock.
    threshold = int(history[3]["ingest_time_ns"])
    ts_sub = f"{domain}.timeshifted"
    status, declared = post(ENDPOINTS[0], "/v1/admin/wcl", {"request_id": str(uuid.uuid4()), "script": f"CREATE SUBSCRIPTION {ts_sub} FROM {feed} START AT TIMESTAMP {threshold};"})
    if status != 200: raise AssertionError(declared)
    joined = member_ready(ENDPOINTS[1], f"{members}/join", {"subscription": ts_sub, "request_id": str(uuid.uuid4()), "member_id": member})
    if joined["member_epoch"] != 1:  raise AssertionError(joined)
    claimed = member_ready(ENDPOINTS[2], f"{members}/claim", {"subscription": ts_sub, "request_id": str(uuid.uuid4()), "member_id": member, "member_epoch": 1, "work_id": work})
    page = member_ready(ENDPOINTS[0], f"{members}/fetch", {"subscription": ts_sub, "request_id": str(uuid.uuid4()), "member_id": member, "member_epoch": 1, "work_id": work, "lease_epoch": claimed["lease"]["lease_epoch"], "limit": 10})
    expected = [row["event_time_ns"] for row in history if int(row["ingest_time_ns"]) >= threshold]
    if [row["event_time_ns"] for row in page["records"]] != expected: raise AssertionError(page)
    phase("timestamp start verified")

    # An explicit-Cursor start resumes each internal Range after the last
    # frame it contributed to the merged-order prefix at-or-before the
    # Cursor â€” the same skip a Reader `after` read applies.
    boundary = history[2]["cursor"]
    cursor_sub = f"{domain}.replaying"
    status, declared = post(ENDPOINTS[0], "/v1/admin/wcl", {"request_id": str(uuid.uuid4()), "script": f"CREATE SUBSCRIPTION {cursor_sub} FROM {feed} START AT CURSOR '{boundary}';"})
    if status != 200: raise AssertionError(declared)
    joined = member_ready(ENDPOINTS[1], f"{members}/join", {"subscription": cursor_sub, "request_id": str(uuid.uuid4()), "member_id": member})
    if joined["member_epoch"] != 1:  raise AssertionError(joined)
    claimed = member_ready(ENDPOINTS[2], f"{members}/claim", {"subscription": cursor_sub, "request_id": str(uuid.uuid4()), "member_id": member, "member_epoch": 1, "work_id": work})
    page = member_ready(ENDPOINTS[0], f"{members}/fetch", {"subscription": cursor_sub, "request_id": str(uuid.uuid4()), "member_id": member, "member_epoch": 1, "work_id": work, "lease_epoch": claimed["lease"]["lease_epoch"], "limit": 10})
    boundary_index = next(index for index, row in enumerate(history) if row["cursor"] == boundary)
    expected = [row["event_time_ns"] for row in history[boundary_index + 1:]]
    if [row["event_time_ns"] for row in page["records"]] != expected: raise AssertionError(page)

    # An unknown or foreign-Feed Cursor fails closed instead of guessing a
    # starting position.
    bogus_sub = f"{domain}.bogus"
    status, declared = post(ENDPOINTS[0], "/v1/admin/wcl", {"request_id": str(uuid.uuid4()), "script": f"CREATE SUBSCRIPTION {bogus_sub} FROM {feed} START AT CURSOR 'not-a-feed-cursor';"})
    if status != 200: raise AssertionError(declared)
    status, refused = post(ENDPOINTS[1], f"{members}/join", {"subscription": bogus_sub, "request_id": str(uuid.uuid4()), "member_id": member})
    if status != 400: raise AssertionError(refused)
    phase("cursor start verified")

    # Member state reports the fenced epoch and no live leases after acks.
    status, state = get(ENDPOINTS[1], f"{members}/state?subscription={subscription}")
    if status != 200 or state["member_epochs"][member] != 2: raise AssertionError(state)

    # Member-traffic demand attribution is Node-local: merge every Node's
    # rows so the round-robin calls above all count, and the per-Domain
    # rollup covers both Subscriptions in the Domain.
    merged = {}
    rollups = {}
    for endpoint in ENDPOINTS:
        status, demand = get(endpoint, "/v1/admin/demand/subscriptions")
        if status != 200: raise AssertionError(demand)
        for row in demand["subscriptions"]:
            telemetry = row.get("telemetry") or {}
            slot = merged.setdefault(row["subscription"], {})
            for field, value in telemetry.items():
                if isinstance(value, int):
                    if field.startswith("last_") or field.endswith("_last") or field == "members_active":
                        slot[field] = max(slot.get(field, 0), value)
                    else:
                        slot[field] = slot.get(field, 0) + value
        for domain_name, rollup in demand["domains"].items():
            slot = rollups.setdefault(domain_name, {})
            for field, value in rollup.items():
                if isinstance(value, int):
                    if field == "subscriptions":
                        slot[field] = max(slot.get(field, 0), value)
                    else:
                        slot[field] = slot.get(field, 0) + value
    telemetry = merged.get(subscription, {})
    if telemetry.get("joins", 0) < 4: raise AssertionError(f"joins {telemetry}")
    if telemetry.get("claims", 0) < 3: raise AssertionError(f"claims {telemetry}")
    if telemetry.get("fetches", 0) < 4: raise AssertionError(f"fetches {telemetry}")
    if telemetry.get("fetch_records", 0) < 5: raise AssertionError(f"fetch_records {telemetry}")
    if telemetry.get("acks", 0) < 2: raise AssertionError(f"acks {telemetry}")
    if telemetry.get("transfers", 0) < 1: raise AssertionError(f"transfers {telemetry}")
    rollup = rollups.get(domain, {})
    if rollup.get("subscriptions", 0) < 2: raise AssertionError(f"domain rollup {rollup}")
    phase("subscription demand attribution verified")

    # A Domain-source Subscription tracks one durable frontier per member
    # Feed: join establishes them all, and every member call carries an
    # explicit `feed` selector resolved to the immutable FeedId.
    dsub = f"{domain}.cover"
    status, declared = post(ENDPOINTS[0], "/v1/admin/wcl", {"request_id": str(uuid.uuid4()), "script": f"CREATE SUBSCRIPTION {dsub} FROM DOMAIN {domain} START AT BEGINNING;"})
    if status != 200: raise AssertionError(declared)
    dmember = str(uuid.uuid4())
    djoin = member_ready(ENDPOINTS[1], f"{members}/join", {"subscription": dsub, "request_id": str(uuid.uuid4()), "member_id": dmember})
    if djoin["member_epoch"] != 1: raise AssertionError(djoin)
    dwork = str(uuid.uuid4())
    dclaim = member_ready(ENDPOINTS[2], f"{members}/claim", {"subscription": dsub, "request_id": str(uuid.uuid4()), "member_id": dmember, "member_epoch": 1, "work_id": dwork, "lease_ticks": 60})
    dlease = dclaim["lease"]["lease_epoch"]

    # The Feed selector is mandatory on a Domain source; an unattached Feed
    # conflicts rather than guessing a frontier.
    status, refused = post(ENDPOINTS[0], f"{members}/fetch", {"subscription": dsub, "request_id": str(uuid.uuid4()), "member_id": dmember, "member_epoch": 1, "work_id": dwork, "lease_epoch": dlease})
    if status != 400: raise AssertionError(refused)
    status, refused = post(ENDPOINTS[0], f"{members}/fetch", {"subscription": dsub, "request_id": str(uuid.uuid4()), "member_id": dmember, "member_epoch": 1, "work_id": dwork, "lease_epoch": dlease, "feed": "nosuch.feed"})
    if status == 200: raise AssertionError(refused)

    # The Domain frontier is independent of the exact-Feed Subscription's
    # progress on the same history: all six records deliver again here.
    dpage = member_ready(ENDPOINTS[1], f"{members}/fetch", {"subscription": dsub, "request_id": str(uuid.uuid4()), "member_id": dmember, "member_epoch": 1, "work_id": dwork, "lease_epoch": dlease, "feed": feed, "limit": 10})
    if len(dpage["records"]) != 6: raise AssertionError(dpage)
    member_ready(ENDPOINTS[2], f"{members}/ack", {"subscription": dsub, "request_id": str(uuid.uuid4()), "member_id": dmember, "member_epoch": 1, "work_id": dwork, "lease_epoch": dlease, "feed": feed, "cursor": dpage["cursor"], "positions": dpage["positions"]})

    # The ack completes dwork's lease, so the empty-tail probe claims fresh
    # work — the member Feed's frontier stays acked under any later claim.
    dclaim2 = member_ready(ENDPOINTS[0], f"{members}/claim", {"subscription": dsub, "request_id": str(uuid.uuid4()), "member_id": dmember, "member_epoch": 1, "work_id": str(uuid.uuid4()), "lease_ticks": 60})
    dlease2, dwork2 = dclaim2["lease"]["lease_epoch"], dclaim2["lease"]["work_id"]
    dempty = member_ready(ENDPOINTS[1], f"{members}/fetch", {"subscription": dsub, "request_id": str(uuid.uuid4()), "member_id": dmember, "member_epoch": 1, "work_id": dwork2, "lease_epoch": dlease2, "feed": feed})
    if dempty["records"]: raise AssertionError(dempty)

    # A Feed created after the Subscription attaches at BEGINNING; the
    # first fetch naming it commits its frontier entry durably.
    feed2, writer2 = f"{domain}.late", f"writer2{suffix}"
    status, setup = post(ENDPOINTS[0], "/v1/admin/wcl", {"request_id": str(uuid.uuid4()), "script": f"CREATE FEED {feed2}; CREATE WRITER {writer2} TO {feed2}; OPEN WRITER SESSION {writer2};"})
    if status != 200: raise AssertionError(setup)
    we2 = setup["results"][2]["data"]["session_epoch"]
    for index in (7, 8):
        status, appended = post(ENDPOINTS[1], "/v1/writers/append", {"request_id": str(uuid.uuid4()), "writer": writer2, "session_epoch": we2, "event_time_ns": str(index), "key_base64": "aw==", "payload_base64": "dg==", "metadata_base64": {}})
        if status != 200: raise AssertionError(appended)
    dpage2 = member_ready(ENDPOINTS[2], f"{members}/fetch", {"subscription": dsub, "request_id": str(uuid.uuid4()), "member_id": dmember, "member_epoch": 1, "work_id": dwork2, "lease_epoch": dlease2, "feed": feed2, "limit": 10})
    if len(dpage2["records"]) != 2: raise AssertionError(dpage2)

    # Seeks refuse while members hold leases; release first, then a seek on
    # one member Feed leaves the other's frontier untouched.
    member_ready(ENDPOINTS[0], f"{members}/release", {"subscription": dsub, "request_id": str(uuid.uuid4()), "member_id": dmember, "member_epoch": 1, "work_id": dwork2, "lease_epoch": dlease2})
    status, seeked = post(ENDPOINTS[0], "/v1/subscriptions/seek", {"subscription": dsub, "request_id": str(uuid.uuid4()), "position": "beginning", "feed": feed2})
    if status != 200: raise AssertionError(seeked)
    dclaim3 = member_ready(ENDPOINTS[1], f"{members}/claim", {"subscription": dsub, "request_id": str(uuid.uuid4()), "member_id": dmember, "member_epoch": 1, "work_id": str(uuid.uuid4()), "lease_ticks": 60})
    dlease3, dwork3 = dclaim3["lease"]["lease_epoch"], dclaim3["lease"]["work_id"]
    dpage3 = member_ready(ENDPOINTS[2], f"{members}/fetch", {"subscription": dsub, "request_id": str(uuid.uuid4()), "member_id": dmember, "member_epoch": 1, "work_id": dwork3, "lease_epoch": dlease3, "feed": feed2, "limit": 10})
    if len(dpage3["records"]) != 2: raise AssertionError(dpage3)
    dstill = member_ready(ENDPOINTS[0], f"{members}/fetch", {"subscription": dsub, "request_id": str(uuid.uuid4()), "member_id": dmember, "member_epoch": 1, "work_id": dwork3, "lease_epoch": dlease3, "feed": feed})
    if dstill["records"]: raise AssertionError(dstill)

    # A range split on one member Feed stages a durable cutover that the
    # reconcile applies to that Feed's frontier entry only: the Domain row
    # keeps serving, the acknowledged history stays acknowledged, and new
    # appends land on the post-split ranges.
    status, split = post(ENDPOINTS[1], "/v1/admin/ranges/split", {"request_id": str(uuid.uuid4()), "feed": feed, "split_at": "80000000000000000000000000000000", "batch_size": 4})
    if status != 200 or split.get("status") != "activated": raise AssertionError({"split": split})
    if split.get("subscription_reconcile_failed"): raise AssertionError({"split reconcile": split})
    status, appended = post(ENDPOINTS[1], "/v1/writers/append", {"request_id": str(uuid.uuid4()), "writer": writer, "session_epoch": writer_epoch, "event_time_ns": "9", "key_base64": "eg==", "payload_base64": "eQ==", "metadata_base64": {}})
    if status != 200: raise AssertionError(appended)
    dpost = member_ready(ENDPOINTS[2], f"{members}/fetch", {"subscription": dsub, "request_id": str(uuid.uuid4()), "member_id": dmember, "member_epoch": 1, "work_id": dwork3, "lease_epoch": dlease3, "feed": feed, "limit": 10})
    if len(dpost["records"]) != 1: raise AssertionError(dpost)
    member_ready(ENDPOINTS[0], f"{members}/ack", {"subscription": dsub, "request_id": str(uuid.uuid4()), "member_id": dmember, "member_epoch": 1, "work_id": dwork3, "lease_epoch": dlease3, "feed": feed, "cursor": dpost["cursor"], "positions": dpost["positions"]})
    dclaim4 = member_ready(ENDPOINTS[1], f"{members}/claim", {"subscription": dsub, "request_id": str(uuid.uuid4()), "member_id": dmember, "member_epoch": 1, "work_id": str(uuid.uuid4()), "lease_ticks": 60})
    dsibling = member_ready(ENDPOINTS[2], f"{members}/fetch", {"subscription": dsub, "request_id": str(uuid.uuid4()), "member_id": dmember, "member_epoch": 1, "work_id": dclaim4["lease"]["work_id"], "lease_epoch": dclaim4["lease"]["lease_epoch"], "feed": feed2, "limit": 10})
    if len(dsibling["records"]) != 2: raise AssertionError(dsibling)
    phase("domain member frontiers verified")

    # Killing Nodes one at a time: a replica loss never fails member calls,
    # and the owner kill is detected by the ownership_epoch bump the
    # endpoint's transparent recovery performs before serving the mutation.
    epoch = state["ownership_epoch"]
    recovered = False
    for index in range(3):
        service = f"node{index + 1}"
        phase(f"stopping {service}")
        subprocess.run(["docker", "compose", "stop", service], check=True)
        survivor = ENDPOINTS[(index + 1) % 3]
        try:
            member_ready(survivor, f"{members}/join", {"subscription": subscription, "request_id": str(uuid.uuid4()), "member_id": str(uuid.uuid4())})
            for _ in range(45):
                status, after = get(survivor, f"{members}/state?subscription={subscription}")
                if status == 200:
                    break
                time.sleep(1)
            else:
                raise AssertionError(after)
            if after["ownership_epoch"] > epoch:
                epoch, recovered = after["ownership_epoch"], True
        finally:
            phase(f"starting {service}")
            subprocess.run(["docker", "compose", "start", service], check=True)
            for _ in range(60):
                try:
                    status, _ = post(ENDPOINTS[index], "/v1/admin/wcl", {"request_id": str(uuid.uuid4()), "script": "SHOW DOMAINS;"})
                    if status == 200: break
                except (urllib.error.URLError, TimeoutError, ConnectionError): pass
                time.sleep(1)
            else: raise AssertionError(f"{service} did not come back after restart")
    if not recovered: raise AssertionError("member mutations never recovered a lost progress owner")

    print(json.dumps({"status": "ok", "subscription": subscription, "member_epoch": 2, "first_page": 3, "transferred_page": len(redelivered["records"]), "final_fetch": len(tail["records"])}, indent=2))

if __name__ == "__main__":
    try: main()
    except Exception as error:
        import traceback
        traceback.print_exc()
        print(f"M7 Subscription member acceptance failed: {error}", file=sys.stderr); sys.exit(1)
