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
    request = urllib.request.Request(endpoint + path, data=json.dumps(body).encode(), headers={"authorization": f"Bearer {KEY}", "content-type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=30) as response: return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error: return error.code, json.loads(error.read() or b"{}")

@_retried
def get(endpoint, path):
    request = urllib.request.Request(endpoint + path, headers={"authorization": f"Bearer {KEY}"})
    try:
        with urllib.request.urlopen(request, timeout=30) as response: return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error: return error.code, json.loads(error.read() or b"{}")

def wait_ready():
    for _ in range(60):
        try:
            status, _ = post(ENDPOINTS[0], "/v1/admin/wcl", {"script": "SHOW DOMAINS;"})
            if status == 200:
                return
        except (ConnectionError, urllib.error.URLError, TimeoutError):
            pass
        time.sleep(1)
    raise AssertionError("verification Riverbed did not answer WCL")


def member_ready(endpoint, path, body):
    # Stopping a voter can drop the Control Plane leader; the survivor must
    # finish re-electing before member mutations succeed again. Ride out the
    # election window — every retry replays the same request_id, so repeats
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
    status, setup = post(ENDPOINTS[0], "/v1/admin/wcl", {"script": f"CREATE DOMAIN {domain}; CREATE FEED {feed}; CREATE WRITER {writer} TO {feed}; OPEN WRITER SESSION {writer}; CREATE SUBSCRIPTION {subscription} FROM {feed} START AT BEGINNING;"})
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
    status, joined = post(ENDPOINTS[1], f"{members}/join", {"subscription": subscription, "request_id": join_request, "member_id": member})
    if status != 200 or joined["member_epoch"] != 1: raise AssertionError(joined)
    status, replay = post(ENDPOINTS[2], f"{members}/join", {"subscription": subscription, "request_id": join_request, "member_id": member})
    if status != 200 or replay["member_epoch"] != 1: raise AssertionError(f"join replay returned a different epoch: {replay}")

    # A fresh join request fences the earlier member epoch.
    status, rejoined = post(ENDPOINTS[0], f"{members}/join", {"subscription": subscription, "request_id": str(uuid.uuid4()), "member_id": member})
    if status != 200 or rejoined["member_epoch"] != 2: raise AssertionError(rejoined)

    # The fenced epoch cannot claim work.
    status, stale = post(ENDPOINTS[1], f"{members}/claim", {"subscription": subscription, "request_id": str(uuid.uuid4()), "member_id": member, "member_epoch": 1, "work_id": work})
    if status != 409: raise AssertionError(stale)

    # The live member claims work and fetches a bounded page after the frontier.
    status, claimed = post(ENDPOINTS[2], f"{members}/claim", {"subscription": subscription, "request_id": str(uuid.uuid4()), "member_id": member, "member_epoch": 2, "work_id": work, "lease_ticks": 60})
    if status != 200 or not claimed["lease"]: raise AssertionError(claimed)
    lease_epoch = claimed["lease"]["lease_epoch"]
    status, page = post(ENDPOINTS[0], f"{members}/fetch", {"subscription": subscription, "request_id": str(uuid.uuid4()), "member_id": member, "member_epoch": 2, "work_id": work, "lease_epoch": lease_epoch, "limit": 3})
    if status != 200 or len(page["records"]) != 3 or not page["cursor"]: raise AssertionError(page)

    # A stale lease epoch cannot fetch or acknowledge.
    status, refused = post(ENDPOINTS[1], f"{members}/fetch", {"subscription": subscription, "request_id": str(uuid.uuid4()), "member_id": member, "member_epoch": 2, "work_id": work, "lease_epoch": lease_epoch + 99})
    if status != 409: raise AssertionError(refused)

    # The valid grant acknowledges the delivered page atomically.
    status, acked = post(ENDPOINTS[2], f"{members}/ack", {"subscription": subscription, "request_id": str(uuid.uuid4()), "member_id": member, "member_epoch": 2, "work_id": work, "lease_epoch": lease_epoch, "cursor": page["cursor"], "positions": page["positions"]})
    if status != 200: raise AssertionError(acked)

    phase("join/claim/fetch/ack verified")

    # A lease transfer hands claimed work to another member and fences the
    # source holder for both fetch and acknowledge. The remaining two
    # records are delivered under the source lease, then acknowledged only
    # by the transfer target.
    member_b = str(uuid.uuid4())
    status, joined_b = post(ENDPOINTS[1], f"{members}/join", {"subscription": subscription, "request_id": str(uuid.uuid4()), "member_id": member_b})
    if status != 200 or joined_b["member_epoch"] != 1: raise AssertionError(joined_b)
    work_b = str(uuid.uuid4())
    status, claimed_b = post(ENDPOINTS[0], f"{members}/claim", {"subscription": subscription, "request_id": str(uuid.uuid4()), "member_id": member, "member_epoch": 2, "work_id": work_b, "lease_ticks": 60})
    if status != 200 or not claimed_b["lease"]: raise AssertionError(claimed_b)
    lease_b = claimed_b["lease"]["lease_epoch"]
    status, page_b = post(ENDPOINTS[2], f"{members}/fetch", {"subscription": subscription, "request_id": str(uuid.uuid4()), "member_id": member, "member_epoch": 2, "work_id": work_b, "lease_epoch": lease_b})
    if status != 200 or len(page_b["records"]) != 2: raise AssertionError(page_b)
    status, moved = post(ENDPOINTS[1], f"{members}/transfer", {"subscription": subscription, "request_id": str(uuid.uuid4()), "work_id": work_b, "member_id": member, "lease_epoch": lease_b, "to_member_id": member_b, "to_member_epoch": joined_b["member_epoch"], "lease_ticks": 60})
    if status != 200 or not moved["lease"]: raise AssertionError(moved)
    # The fenced holder can neither fetch nor acknowledge with the stale lease.
    status, refused = post(ENDPOINTS[0], f"{members}/fetch", {"subscription": subscription, "request_id": str(uuid.uuid4()), "member_id": member, "member_epoch": 2, "work_id": work_b, "lease_epoch": lease_b})
    if status != 409: raise AssertionError(refused)
    status, refused = post(ENDPOINTS[1], f"{members}/ack", {"subscription": subscription, "request_id": str(uuid.uuid4()), "member_id": member, "member_epoch": 2, "work_id": work_b, "lease_epoch": lease_b, "cursor": page_b["cursor"], "positions": page_b["positions"]})
    if status != 409: raise AssertionError(refused)
    # The new holder re-delivers and acknowledges the same unacked page; the
    # released transferred lease cannot fetch again.
    new_lease_b = moved["lease"]["lease_epoch"]
    status, redelivered = post(ENDPOINTS[2], f"{members}/fetch", {"subscription": subscription, "request_id": str(uuid.uuid4()), "member_id": member_b, "member_epoch": joined_b["member_epoch"], "work_id": work_b, "lease_epoch": new_lease_b})
    if status != 200 or len(redelivered["records"]) != 2: raise AssertionError(redelivered)
    status, acked = post(ENDPOINTS[0], f"{members}/ack", {"subscription": subscription, "request_id": str(uuid.uuid4()), "member_id": member_b, "member_epoch": joined_b["member_epoch"], "work_id": work_b, "lease_epoch": new_lease_b, "cursor": redelivered["cursor"], "positions": redelivered["positions"]})
    if status != 200: raise AssertionError(acked)
    status, released = post(ENDPOINTS[2], f"{members}/fetch", {"subscription": subscription, "request_id": str(uuid.uuid4()), "member_id": member_b, "member_epoch": joined_b["member_epoch"], "work_id": work_b, "lease_epoch": new_lease_b})
    if status != 409: raise AssertionError(released)
    # A fresh claim reaches the acknowledged end of the Feed.
    status, tailclaim = post(ENDPOINTS[0], f"{members}/claim", {"subscription": subscription, "request_id": str(uuid.uuid4()), "member_id": member, "member_epoch": 2, "work_id": str(uuid.uuid4())})
    if status != 200: raise AssertionError(tailclaim)
    status, tail = post(ENDPOINTS[2], f"{members}/fetch", {"subscription": subscription, "request_id": str(uuid.uuid4()), "member_id": member, "member_epoch": 2, "work_id": tailclaim["lease"]["work_id"], "lease_epoch": tailclaim["lease"]["lease_epoch"]})
    if status != 200 or tail["records"]: raise AssertionError(tail)

    phase("lease transfer fencing verified")

    # A `now` Subscription pins the committed tail at first join: earlier
    # records are skipped and only post-join appends are delivered.
    now_sub = f"{domain}.realtime"
    status, declared = post(ENDPOINTS[0], "/v1/admin/wcl", {"script": f"CREATE SUBSCRIPTION {now_sub} FROM {feed} START AT NOW;"})
    if status != 200: raise AssertionError(declared)
    status, joined = post(ENDPOINTS[1], f"{members}/join", {"subscription": now_sub, "request_id": str(uuid.uuid4()), "member_id": member})
    if status != 200 or joined["member_epoch"] != 1: raise AssertionError(joined)
    status, claimed = post(ENDPOINTS[2], f"{members}/claim", {"subscription": now_sub, "request_id": str(uuid.uuid4()), "member_id": member, "member_epoch": 1, "work_id": work})
    if status != 200: raise AssertionError(claimed)
    status, page = post(ENDPOINTS[0], f"{members}/fetch", {"subscription": now_sub, "request_id": str(uuid.uuid4()), "member_id": member, "member_epoch": 1, "work_id": work, "lease_epoch": claimed["lease"]["lease_epoch"]})
    if status != 200 or page["records"]: raise AssertionError(f"now Subscription delivered pre-join records: {page}")
    status, appended = post(ENDPOINTS[1], "/v1/writers/append", {"request_id": str(uuid.uuid4()), "writer": writer, "session_epoch": writer_epoch, "event_time_ns": "6", "key_base64": "aw==", "payload_base64": "dg==", "metadata_base64": {}})
    if status != 200: raise AssertionError(appended)
    status, page = post(ENDPOINTS[2], f"{members}/fetch", {"subscription": now_sub, "request_id": str(uuid.uuid4()), "member_id": member, "member_epoch": 1, "work_id": work, "lease_epoch": claimed["lease"]["lease_epoch"], "wait_ms": 2000})
    if status != 200 or len(page["records"]) != 1: raise AssertionError(page)

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
                    status, _ = post(ENDPOINTS[index], "/v1/admin/wcl", {"script": "SHOW DOMAINS;"})
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
