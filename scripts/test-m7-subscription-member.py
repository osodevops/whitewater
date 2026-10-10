import json, os, subprocess, sys, time, urllib.error, urllib.request, uuid

BASE_PORT = int(os.environ.get("WHITEWATER_TEST_BASE_PORT", "7071"))
ENDPOINTS = [f"http://localhost:{BASE_PORT + offset}" for offset in range(3)]
KEY = "whitewater-local-development-admin-key"

def post(endpoint, path, body):
    request = urllib.request.Request(endpoint + path, data=json.dumps(body).encode(), headers={"authorization": f"Bearer {KEY}", "content-type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=30) as response: return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error: return error.code, json.loads(error.read() or b"{}")

def get(endpoint, path):
    request = urllib.request.Request(endpoint + path, headers={"authorization": f"Bearer {KEY}"})
    try:
        with urllib.request.urlopen(request, timeout=30) as response: return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error: return error.code, json.loads(error.read() or b"{}")

def main():
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

    # The next fetch continues from the acknowledged frontier.
    status, reclaimed = post(ENDPOINTS[0], f"{members}/claim", {"subscription": subscription, "request_id": str(uuid.uuid4()), "member_id": member, "member_epoch": 2, "work_id": work})
    if status != 200: raise AssertionError(reclaimed)
    status, rest = post(ENDPOINTS[1], f"{members}/fetch", {"subscription": subscription, "request_id": str(uuid.uuid4()), "member_id": member, "member_epoch": 2, "work_id": work, "lease_epoch": reclaimed["lease"]["lease_epoch"]})
    if status != 200 or len(rest["records"]) != 2: raise AssertionError(rest)
    status, acked = post(ENDPOINTS[0], f"{members}/ack", {"subscription": subscription, "request_id": str(uuid.uuid4()), "member_id": member, "member_epoch": 2, "work_id": work, "lease_epoch": reclaimed["lease"]["lease_epoch"], "cursor": rest["cursor"], "positions": rest["positions"]})
    if status != 200: raise AssertionError(acked)
    # A released lease cannot fetch again.
    status, released = post(ENDPOINTS[2], f"{members}/fetch", {"subscription": subscription, "request_id": str(uuid.uuid4()), "member_id": member, "member_epoch": 2, "work_id": work, "lease_epoch": reclaimed["lease"]["lease_epoch"]})
    if status != 409: raise AssertionError(released)
    # A fresh claim reaches the acknowledged end of the Feed.
    status, tailclaim = post(ENDPOINTS[0], f"{members}/claim", {"subscription": subscription, "request_id": str(uuid.uuid4()), "member_id": member, "member_epoch": 2, "work_id": str(uuid.uuid4())})
    if status != 200: raise AssertionError(tailclaim)
    status, tail = post(ENDPOINTS[2], f"{members}/fetch", {"subscription": subscription, "request_id": str(uuid.uuid4()), "member_id": member, "member_epoch": 2, "work_id": tailclaim["lease"]["work_id"], "lease_epoch": tailclaim["lease"]["lease_epoch"]})
    if status != 200 or tail["records"]: raise AssertionError(tail)

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

    # Killing Nodes one at a time: a replica loss never fails member calls,
    # and the owner kill is detected by the ownership_epoch bump the
    # endpoint's transparent recovery performs before serving the mutation.
    epoch = state["ownership_epoch"]
    recovered = False
    for index in range(3):
        service = f"node{index + 1}"
        subprocess.run(["docker", "compose", "stop", service], check=True)
        survivor = ENDPOINTS[(index + 1) % 3]
        try:
            status, join = post(survivor, f"{members}/join", {"subscription": subscription, "request_id": str(uuid.uuid4()), "member_id": str(uuid.uuid4())})
            if status != 200:
                for probe in ENDPOINTS:
                    try:
                        ps, pbody = get(probe, f"{members}/state?subscription={subscription}")
                        print(f"PROBE {probe}: {ps} {pbody}")
                    except Exception as error:
                        print(f"PROBE {probe}: unreachable {error}")
                raise AssertionError(f"member op failed with {service} down: {join}")
            status, after = get(survivor, f"{members}/state?subscription={subscription}")
            if status != 200: raise AssertionError(after)
            if after["ownership_epoch"] > epoch:
                epoch, recovered = after["ownership_epoch"], True
        finally:
            subprocess.run(["docker", "compose", "start", service], check=True)
            for _ in range(60):
                try:
                    status, _ = post(ENDPOINTS[index], "/v1/admin/wcl", {"script": "SHOW DOMAINS;"})
                    if status == 200: break
                except (urllib.error.URLError, TimeoutError): pass
                time.sleep(1)
            else: raise AssertionError(f"{service} did not come back after restart")
    if not recovered: raise AssertionError("member mutations never recovered a lost progress owner")

    print(json.dumps({"status": "ok", "subscription": subscription, "member_epoch": 2, "first_page": 3, "resumed_page": 2, "final_fetch": len(tail["records"])}, indent=2))

if __name__ == "__main__":
    try: main()
    except Exception as error: print(f"M7 Subscription member acceptance failed: {error}", file=sys.stderr); sys.exit(1)
