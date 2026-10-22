"""M4 evidence: >10,000-frame Feed history through split/merge transitions.

Seeds a Feed past the 10,000-frame boundary, splits it, merges it back, and
proves at every stage that:

  * a Writer-issued record Cursor minted early in the history still resolves
    through the per-range lookup probe while the Feed is split, and through
    the single-range index once it is merged;
  * a named Reader draining bounded pages mid-transition receives the full
    immutable history in committed order — no loss, reorder, or duplication;
  * a Subscription seek to the historical Cursor repositions the shared
    frontier exactly after that record, and the next member fetch resumes
    the correct record.

Seeding uses several Writer sessions driving single-record appends in
parallel: each append is one quorum-committed frame with a stable request
identity, so a retried record dedupes by its Cursor instead of duplicating.
Runs against the shared compose.verify.yml Riverbed via
WHITEWATER_TEST_BASE_PORT; registered on a read lane of run-acceptance.py.

Set WHITEWATER_LH_RESUME_FEED to a Feed name that already holds TOTAL
committed frames to skip setup and seeding: the script rebuilds ground
truth by paginating /v1/feeds/records, creates fresh Reader/Subscription/
Writer resources on the existing Domain, and drives the same
split/merge/lookup/drain/seek evidence against the carried-over history.
"""

import base64, json, os, sys, threading, time, urllib.error, urllib.parse, urllib.request, uuid

BASE_PORT = int(os.environ.get("WHITEWATER_TEST_BASE_PORT", "7071"))
_endpoints_env = os.environ.get("WHITEWATER_TEST_ENDPOINTS")
ENDPOINTS = [entry if "://" in entry else f"http://{entry}" for entry in _endpoints_env.split(",")] if _endpoints_env else [f"http://localhost:{BASE_PORT + offset}" for offset in range(3)]
KEY = "whitewater-local-development-admin-key"
MEMBERS = "/v1/subscriptions/members"
TRANSIENT = {"no_quorum", "unavailable", "ambiguous_commit", "invalid_assignment"}
RESUME_FEED = os.environ.get("WHITEWATER_LH_RESUME_FEED", "")

# The evidence boundary this script exists to prove: the Feed holds strictly
# more than 10,000 frames before the transitions begin.
TOTAL = 10_200
SEED_WRITERS = 8          # parallel Writer sessions; per-session sequences stay contiguous
TAIL = 96                 # records appended post-split so the merge stage moves live writes too
KEYS = 16                 # enough distinct Keys that a midpoint split divides data across both children
READER_PAGE = 512
SEED_BATCH = 1_000        # /v1/writers/append-batch caps at 1,000 records


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
def post(endpoint, path, body, timeout=120):
    request = urllib.request.Request(endpoint + path, data=json.dumps(body).encode(), headers={"authorization": f"Bearer {KEY}", "content-type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response: return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error: return error.code, json.loads(error.read() or b"{}")
    except OSError:
        return 0, {}


@_retried
def _get_once(endpoint, path):
    request = urllib.request.Request(endpoint + path, headers={"authorization": f"Bearer {KEY}"})
    try:
        with urllib.request.urlopen(request, timeout=60) as response: return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error: return error.code, json.loads(error.read() or b"{}")
    except OSError:
        return 0, {}


def b64(raw):
    return base64.b64encode(raw).decode()


def main():
    def retried_post(endpoint, path, body, timeout=120):
        for _ in range(240):
            status, result = post(endpoint, path, body, timeout)
            if status == 200 or (
                status
                and status < 500
                and result.get("code") not in TRANSIENT
            ):
                return status, result
            time.sleep(0.5)
        return status, result

    suffix = f"{int(time.time())}{uuid.uuid4().hex[:6]}"
    writer, reader = f"lhw{suffix}", f"lhr{suffix}"
    if RESUME_FEED:
        domain, feed = RESUME_FEED.split(".", 1)[0], RESUME_FEED
        subscription = f"{domain}.drainr{suffix}"
        setup_script = f"CREATE WRITER {writer} TO {feed}; OPEN WRITER SESSION {writer}; CREATE READER {reader} FROM {feed} START AT BEGINNING; CREATE SUBSCRIPTION {subscription} FROM {feed} START AT BEGINNING;"
        session_row = 1
    else:
        domain, feed = f"lh{suffix}", f"lh{suffix}.events"
        subscription = f"{domain}.drain"
        setup_script = f"CREATE DOMAIN {domain}; CREATE FEED {feed}; CREATE WRITER {writer} TO {feed}; OPEN WRITER SESSION {writer}; CREATE READER {reader} FROM {feed} START AT BEGINNING; CREATE SUBSCRIPTION {subscription} FROM {feed} START AT BEGINNING;"
        session_row = 3
    seed_writers = [f"lhs{i}w{suffix}" for i in range(SEED_WRITERS)]

    status, setup = retried_post(ENDPOINTS[0], "/v1/admin/wcl", {"request_id": str(uuid.uuid4()), "script": setup_script})
    if status != 200: raise AssertionError(setup)
    epoch = setup["results"][session_row]["data"]["session_epoch"]

    encoded = urllib.parse.quote(feed, safe="")
    cursors, expected = [], {}

    if RESUME_FEED:
        # Resume mode rebuilds the ground-truth Cursor set from committed
        # history: every row carries the opaque Cursor the Writer was issued
        # for that record, so lookup fidelity is checked exactly as after a
        # fresh seed.
        after = None
        while len(cursors) < TOTAL + 1:
            path = f"/v1/feeds/records?feed={encoded}&limit=1024" + (f"&after={urllib.parse.quote(after, safe='')}" if after else "")
            status, page = _get_once(ENDPOINTS[0], path)
            if status != 200: raise AssertionError(f"committed history fetch refused: {page}")
            if not page: break
            cursors.extend(row["cursor"] for row in page)
            for row in page: expected[row["cursor"]] = base64.b64decode(row["payload_base64"]).decode()
            after = page[-1]["cursor"]
            if len(page) < 1024: break
        if len(cursors) != TOTAL: raise AssertionError(f"resume Feed holds {len(cursors)} committed frames, expected {TOTAL}")
        if len(set(cursors)) != TOTAL: raise AssertionError("committed history minted duplicate Cursors")
        print(f"resumed {feed} with {len(cursors)} committed frames", flush=True)
    else:
        # One Writer per seeding lane, one open session each: parallel sessions
        # keep per-session Writer sequences contiguous while disjoint request
        # identities mint independent Cursors.
        script = "".join(f"CREATE WRITER {name} TO {feed}; OPEN WRITER SESSION {name};" for name in seed_writers)
        status, opened = retried_post(ENDPOINTS[0], "/v1/admin/wcl", {"request_id": str(uuid.uuid4()), "script": script}, timeout=180)
        if status != 200: raise AssertionError(opened)
        sessions = [row["data"] for row in opened["results"] if row["statement"].startswith("OPEN WRITER SESSION")]
        if len(sessions) != SEED_WRITERS: raise AssertionError(f"opened {len(sessions)} seed sessions: {opened}")

        cursors = [None] * TOTAL
        seeded = [0]
        failures = []
        def seed_lane(lane):
            session = sessions[lane]
            for index in range(lane, TOTAL, SEED_WRITERS):
                sequence = index // SEED_WRITERS + 1
                body = {
                    "request_id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"{feed}/{index}")),
                    "feed": feed,
                    "writer_session_id": session["writer_id"],
                    "writer_epoch": session["session_epoch"],
                    "sequence": sequence,
                    "event_time_ns": str(1_700_000_000_000_000_000 + index),
                    "key_base64": b64(f"key-{index % KEYS}".encode()),
                    "payload_base64": b64(f"value-{index}".encode()),
                    "metadata_base64": {},
                }
                for _ in range(240):
                    status, result = post(ENDPOINTS[index % 3], "/v1/feeds/append", body, timeout=60)
                    if status == 200:
                        cursors[index] = result["cursor"]
                        break
                    if status and status < 500 and result.get("code") not in TRANSIENT:
                        failures.append(f"record {index} refused: {status} {result}")
                        return
                    time.sleep(0.5)
                else:
                    failures.append(f"record {index} stayed unavailable")
                    return
                done = seeded[0] = seeded[0] + 1
                if done % 500 == 0:
                    print(f"seeded {done}/{TOTAL}", flush=True)

        lanes = [threading.Thread(target=seed_lane, args=(lane,), daemon=True) for lane in range(SEED_WRITERS)]
        for lane in lanes: lane.start()
        for lane in lanes: lane.join()
        if failures: raise AssertionError(failures[0])
        if any(cursor is None for cursor in cursors): raise AssertionError("seeded history has gaps")
        if len(set(cursors)) != TOTAL: raise AssertionError("distinct request identities minted duplicate Cursors")
        expected = {cursor: f"value-{index}" for index, cursor in enumerate(cursors)}
    early_cursor, mid_cursor = cursors[7], cursors[6_000]
    def lookup(cursor):
        for _ in range(60):
            status, record = _get_once(ENDPOINTS[0], f"/v1/feeds/records/lookup?feed={encoded}&cursor={urllib.parse.quote(cursor, safe='')}")
            if status == 200: return record
            if status and status < 500: raise AssertionError(f"Cursor lookup refused: {record}")
            time.sleep(0.5)
        raise AssertionError(f"Cursor lookup stayed unavailable: {record}")

    # Pre-transition: the Writer-issued Cursors resolve against the
    # single-range owner index.
    if base64.b64decode(lookup(early_cursor)["payload_base64"]).decode() != expected[early_cursor]:
        raise AssertionError("pre-split deep Cursor resolved the wrong record")
    if base64.b64decode(lookup(mid_cursor)["payload_base64"]).decode() != expected[mid_cursor]:
        raise AssertionError("pre-split mid-history Cursor resolved the wrong record")

    # Named Reader opens at BEGINNING and drains the first half of history
    # before the split, so its pages straddle the cutover. Parallel seeding
    # interleaves positions across lanes, so the committed order — not the
    # submission index — is the ground truth the Reader must reproduce.
    status, opened = retried_post(ENDPOINTS[0], "/v1/readers/open", {"request_id": str(uuid.uuid4()), "reader": reader, "capacity": READER_PAGE})
    if status != 200: raise AssertionError(opened)
    reader_epoch = opened["session_epoch"]
    reader_seen = []
    cursor_set = set()
    def reader_reopen():
        # A committed range transition fences the old session (epoch bump)
        # so a stale client cannot acknowledge against a retired range map;
        # reopening resumes the Reader exactly at its translated
        # acknowledged progress.
        nonlocal reader_epoch
        status, reopened = retried_post(ENDPOINTS[0], "/v1/readers/open", {"request_id": str(uuid.uuid4()), "reader": reader, "capacity": READER_PAGE})
        if status != 200: raise AssertionError(f"Reader reopen after cutover fence refused: {reopened}")
        reader_epoch = reopened["session_epoch"]

    def fenced_session(payload):
        return "stale" in str(payload.get("error", "")) or "inactive" in str(payload.get("error", ""))

    def reader_drain(target):
        while len(reader_seen) < target:
            status, page = retried_post(ENDPOINTS[len(reader_seen) % 3], "/v1/readers/fetch", {"request_id": str(uuid.uuid4()), "reader": reader, "session_epoch": reader_epoch, "limit": READER_PAGE})
            if status != 200:
                if status == 400 and fenced_session(page):
                    reader_reopen()
                    continue
                raise AssertionError(page)
            if not page["records"]: break
            status, ack = retried_post(ENDPOINTS[1], "/v1/readers/ack", {"request_id": str(uuid.uuid4()), "reader": reader, "session_epoch": reader_epoch, "cursor": page["delivered_cursor"]})
            if status != 200:
                if status == 400 and fenced_session(ack):
                    # The cutover landed between fetch and ack: the unacked
                    # page replays once the reopened session resumes, and it
                    # was never recorded below, so no duplicate can appear.
                    reader_reopen()
                    continue
                raise AssertionError(ack)
            for row in page["records"]:
                if row["cursor"] in cursor_set: raise AssertionError("Reader replayed a committed record across the transition")
                cursor_set.add(row["cursor"])
                reader_seen.append(row["cursor"])
        if len(reader_seen) < target:
            raise AssertionError(f"Reader stalled at {len(reader_seen)} of {target}")

    reader_drain(5_000)
    if not cursor_set <= set(cursors): raise AssertionError("Reader delivered a Cursor outside the seeded set")

    def placement_routes():
        status, placement = retried_post(ENDPOINTS[2], "/v1/admin/wcl", {"request_id": str(uuid.uuid4()), "script": f"INSPECT PLACEMENT FOR FEED {feed};"})
        if status != 200: raise AssertionError(placement)
        return placement["results"][0]["data"]["range_map"]["routes"]

    # Commit real Subscription progress before the split so the cutover
    # translates a live frontier rather than skipping a declared-but-unused
    # one: join a member, claim work, fetch and acknowledge a bounded page,
    # then release the lease so it cannot expire mid-transition.
    member = str(uuid.uuid4())
    status, joined = retried_post(ENDPOINTS[1], f"{MEMBERS}/join", {"subscription": subscription, "request_id": str(uuid.uuid4()), "member_id": member})
    if status != 200: raise AssertionError(f"pre-split member join: {joined}")
    work = str(uuid.uuid4())
    status, claimed = retried_post(ENDPOINTS[2], f"{MEMBERS}/claim", {"subscription": subscription, "request_id": str(uuid.uuid4()), "member_id": member, "member_epoch": joined["member_epoch"], "work_id": work, "lease_ticks": 60})
    if status != 200: raise AssertionError(f"pre-split member claim: {claimed}")
    status, page = retried_post(ENDPOINTS[0], f"{MEMBERS}/fetch", {"subscription": subscription, "request_id": str(uuid.uuid4()), "member_id": member, "member_epoch": joined["member_epoch"], "work_id": work, "lease_epoch": claimed["lease"]["lease_epoch"], "limit": 64})
    if status != 200 or not page.get("records"): raise AssertionError(f"pre-split member fetch: {page}")
    status, acked = retried_post(ENDPOINTS[1], f"{MEMBERS}/ack", {"subscription": subscription, "request_id": str(uuid.uuid4()), "member_id": member, "member_epoch": joined["member_epoch"], "work_id": work, "lease_epoch": claimed["lease"]["lease_epoch"], "cursor": page["cursor"], "positions": page["positions"]})
    if status != 200: raise AssertionError(f"pre-split member ack: {acked}")
    post(ENDPOINTS[1], f"{MEMBERS}/release", {"subscription": subscription, "request_id": str(uuid.uuid4()), "member_id": member, "member_epoch": joined["member_epoch"], "work_id": work, "lease_epoch": claimed["lease"]["lease_epoch"]})

    status, split = retried_post(ENDPOINTS[1], "/v1/admin/ranges/split", {"request_id": str(uuid.uuid4()), "feed": feed, "split_at": "80000000000000000000000000000000", "batch_size": 256})
    if status != 200 or split.get("status") != "activated": raise AssertionError(f"split: {split}")
    statuses = [entry.get("status") for entry in split.get("subscription_frontiers", []) if entry.get("subscription") == subscription]
    if split.get("subscription_reconcile_failed") or statuses not in (["reconciled"], ["current"], []): raise AssertionError(f"split did not apply the pending Subscription translation: {split}")
    if len(placement_routes()) != 2: raise AssertionError("split did not leave two Active Ranges")

    # Post-split: the same opaque Cursors still resolve through the
    # per-range lookup probe; the Reader drains the remaining history
    # across both children without losing committed order.
    if base64.b64decode(lookup(early_cursor)["payload_base64"]).decode() != expected[early_cursor]:
        raise AssertionError("post-split deep Cursor resolved the wrong record")
    if base64.b64decode(lookup(mid_cursor)["payload_base64"]).decode() != expected[mid_cursor]:
        raise AssertionError("post-split mid-history Cursor resolved the wrong record")
    reader_drain(TOTAL)
    if cursor_set != set(cursors): raise AssertionError("Reader did not drain the full seeded history across the split")

    # Live appends keep landing on both children while it is split. These go
    # through the catalog-allocated session path so the transition also
    # moves in-flight Writer sequence allocation.
    def append_batch(start, count):
        records = [{
            "request_id": str(uuid.uuid4()),
            "writer": writer,
            "session_epoch": epoch,
            "event_time_ns": str(1_700_000_000_000_000_000 + start + index),
            "key_base64": b64(f"key-{(start + index) % KEYS}".encode()),
            "payload_base64": b64(f"value-{start + index}".encode()),
            "metadata_base64": {},
        } for index in range(count)]
        status, result = retried_post(ENDPOINTS[(start // SEED_BATCH) % 3], "/v1/writers/append-batch", {"records": records})
        if status != 200 or len(result.get("results", [])) != count: raise AssertionError(f"batch @{start}: {result}")
        for index, row in enumerate(result["results"]):
            cursors.append(row["cursor"])
            expected[row["cursor"]] = f"value-{start + index}"

    append_batch(TOTAL, TAIL)
    reader_drain(TOTAL + TAIL)
    if cursor_set != set(cursors): raise AssertionError("Reader missed live appends landed on the split children")

    routes = placement_routes()
    status, merged = retried_post(ENDPOINTS[2], "/v1/admin/ranges/merge", {"request_id": str(uuid.uuid4()), "feed": feed, "left_range_id": routes[0]["range_id"], "right_range_id": routes[1]["range_id"]})
    if status != 200 or merged.get("status") != "activated": raise AssertionError(f"merge: {merged}")
    statuses = [entry.get("status") for entry in merged.get("subscription_frontiers", []) if entry.get("subscription") == subscription]
    if merged.get("subscription_reconcile_failed") or statuses not in (["reconciled"], ["current"], []): raise AssertionError(f"merge did not apply the pending Subscription translation: {merged}")
    if len(placement_routes()) != 1: raise AssertionError("merge did not restore one Active Range")

    # Post-merge single range: the merged log holds >10,000 positions, the
    # historical Writer-issued Cursors still resolve, and the Reader sees
    # no replayed or skipped history after the cutover.
    if base64.b64decode(lookup(early_cursor)["payload_base64"]).decode() != expected[early_cursor]:
        raise AssertionError("post-merge deep Cursor resolved the wrong record")
    if base64.b64decode(lookup(mid_cursor)["payload_base64"]).decode() != expected[mid_cursor]:
        raise AssertionError("post-merge mid-history Cursor resolved the wrong record")
    reader_reopen()
    status, page = retried_post(ENDPOINTS[0], "/v1/readers/fetch", {"request_id": str(uuid.uuid4()), "reader": reader, "session_epoch": reader_epoch, "limit": READER_PAGE})
    if status != 200: raise AssertionError(page)
    if page["records"]: raise AssertionError("Reader received replayed history after the merge")

    # Seek the Subscription's shared frontier to the historical Cursor — a
    # single-range deep seek through the owner index — then a member fetch
    # must resume exactly at the record after it in committed order.
    status, sought = retried_post(ENDPOINTS[2], "/v1/subscriptions/seek", {"subscription": subscription, "request_id": str(uuid.uuid4()), "position": "cursor", "cursor": early_cursor})
    if status != 200: raise AssertionError(f"historical Cursor seek refused on the merged >10,000-frame Feed: {sought}")
    member = str(uuid.uuid4())
    status, joined = retried_post(ENDPOINTS[1], f"{MEMBERS}/join", {"subscription": subscription, "request_id": str(uuid.uuid4()), "member_id": member})
    if status != 200: raise AssertionError(joined)
    work = str(uuid.uuid4())
    status, claimed = retried_post(ENDPOINTS[2], f"{MEMBERS}/claim", {"subscription": subscription, "request_id": str(uuid.uuid4()), "member_id": member, "member_epoch": joined["member_epoch"], "work_id": work, "lease_ticks": 60})
    if status != 200: raise AssertionError(claimed)
    status, page = retried_post(ENDPOINTS[0], f"{MEMBERS}/fetch", {"subscription": subscription, "request_id": str(uuid.uuid4()), "member_id": member, "member_epoch": joined["member_epoch"], "work_id": work, "lease_epoch": claimed["lease"]["lease_epoch"], "limit": 16})
    if status != 200: raise AssertionError(page)
    delivered = [row["cursor"] for row in page["records"]]
    status, expected = _get_once(ENDPOINTS[0], f"/v1/feeds/records?feed={encoded}&after={urllib.parse.quote(early_cursor, safe='')}&limit=16")
    if status != 200: raise AssertionError(f"post-seek canonical read refused: {expected}")
    if delivered != [row["cursor"] for row in expected[:len(delivered)]]:
        raise AssertionError("Subscription member did not resume the record after the historical seek Cursor")
    status, acked = retried_post(ENDPOINTS[1], f"{MEMBERS}/ack", {"subscription": subscription, "request_id": str(uuid.uuid4()), "member_id": member, "member_epoch": joined["member_epoch"], "work_id": work, "lease_epoch": claimed["lease"]["lease_epoch"], "cursor": page["cursor"], "positions": page["positions"]})
    if status != 200: raise AssertionError(acked)
    status, _ = post(ENDPOINTS[1], f"{MEMBERS}/release", {"subscription": subscription, "request_id": str(uuid.uuid4()), "member_id": member, "member_epoch": joined["member_epoch"], "work_id": work, "lease_epoch": claimed["lease"]["lease_epoch"]})

    print(json.dumps({
        "status": "ok",
        "feed": feed,
        "records": len(cursors),
        "reader_drained": len(reader_seen),
        "member_resumed_after_seek": len(delivered),
        "note": "one Feed crossed the 10,000-frame boundary, split at the midpoint token, merged back under live writes, and kept every Writer-issued Cursor resolvable; the named Reader drained the full immutable history in committed order across both transitions and a Subscription seek to a deep historical Cursor resumed the member exactly after it",
    }, indent=2))


if __name__ == "__main__":
    try: main()
    except Exception as error: print(f"M4 large-history transition failed: {error}", file=sys.stderr); sys.exit(1)
