import base64, json, os, subprocess, sys, time, urllib.error, urllib.parse, urllib.request, uuid

BASE_PORT = int(os.environ.get("WHITEWATER_TEST_BASE_PORT", "7071"))
# WHITEWATER_TEST_ENDPOINTS overrides the default localhost base-port
# derivation so the script can run inside a Compose network; it is separate
# from the SDK's WHITEWATER_ENDPOINTS so a developer environment can never
# redirect this fault test at a real Riverbed.
_endpoints_env = os.environ.get("WHITEWATER_TEST_ENDPOINTS")
ENDPOINTS = [entry if "://" in entry else f"http://{entry}" for entry in _endpoints_env.split(",")] if _endpoints_env else [f"http://localhost:{BASE_PORT + offset}" for offset in range(3)]
KEY = "whitewater-local-development-admin-key"
NODE3 = os.environ.get("WHITEWATER_TEST_NODE3", "whitewater-verification-node3-1")

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

# The stopped Node may hold raft leadership when it dies: Prepare then fails
# before staging anything and read-only WCL forward to the dead leader until
# the survivors elect a new one. Wait for the quorum to answer before
# asserting on staged state.
def wait_control_plane(endpoint, timeout=90):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        status, result = post(endpoint, "/v1/admin/wcl", {"script": "SHOW STORAGE NODES;"})
        if status == 200: return
        last = result
        time.sleep(1)
    raise AssertionError(f"Control Plane never recovered quorum after the stop: {last}")

def main():
    suffix = f"{int(time.time())}{uuid.uuid4().hex[:6]}"
    domain, feed, writer = f"rbk{suffix}", f"rbk{suffix}.events", f"rbkw{suffix}"
    status, setup = post(ENDPOINTS[0], "/v1/admin/wcl", {"script": f"CREATE DOMAIN {domain}; CREATE FEED {feed}; CREATE WRITER {writer} TO {feed}; OPEN WRITER SESSION {writer};"})
    if status != 200: raise AssertionError(setup)
    epoch = setup["results"][3]["data"]["session_epoch"]
    for position in range(1, 9):
        status, result = post(ENDPOINTS[0], "/v1/writers/append", {"request_id": str(uuid.uuid4()), "writer": writer, "session_epoch": epoch, "event_time_ns": str(position), "key_base64": base64.b64encode(f"key-{position}".encode()).decode(), "payload_base64": base64.b64encode(f"value-{position}".encode()).decode(), "metadata_base64": {}})
        if status != 200: raise AssertionError(result)

    split_request_id = str(uuid.uuid4())
    def attempt_split(tag, request_id=None, boundary="80000000000000000000000000000000"):
        body = {"request_id": request_id or str(uuid.uuid4()), "feed": feed, "split_at": boundary}
        status, result = post(ENDPOINTS[1], "/v1/admin/ranges/split", body)
        print(f"split {tag}: status={status} error={result.get('error', '')[:140]}", flush=True)
        return status, result

    def staged_split_plan():
        status, placed = post(ENDPOINTS[0], "/v1/admin/wcl", {"script": f"INSPECT PLACEMENT FOR FEED {feed};"})
        if status != 200: raise AssertionError(placed)
        return placed["results"][0]["data"].get("range_split_plan")

    # Fail the cutover mid-staging: a replica is unreachable. The staged plan
    # must be retained as resumable state (the same request ID resumes it) and
    # the frozen replicas released — never silently deleted, which would break
    # the dedup replay contract.
    subprocess.run(["docker", "stop", NODE3], check=True, capture_output=True)
    try:
        # If the stopped Node was leader, Prepare cannot commit until the
        # survivors elect a replacement; wait for that so the failure lands
        # mid-cutover rather than at Prepare.
        wait_control_plane(ENDPOINTS[0])
        status, result = attempt_split("during-outage", request_id=split_request_id)
        if status == 200: raise AssertionError("split unexpectedly activated while a replica was down")

        plan = staged_split_plan()
        if not plan: raise AssertionError("staged split plan was deleted on failure instead of retained for resume")

        # A retry of the same request ID resumes the staged plan and fails the
        # same way — never a ghost cutover, never a pending fence against itself.
        status, result = attempt_split("same-id-during-outage", request_id=split_request_id)
        if status == 200: raise AssertionError("split unexpectedly activated on same-ID retry")
        error = result.get("error", "")
        if "operation" in error.lower() and "for feed" in error.lower():
            raise AssertionError(f"same-ID retry fenced against its own staged plan: {error}")

        # A fresh retry of the same logical split resumes the staged plan too;
        # only a conflicting operation fences.
        status, result = attempt_split("same-boundary-fresh-id")
        if status == 200: raise AssertionError("split unexpectedly activated on fresh-ID retry")
        error = result.get("error", "")
        if "operation" in error.lower() and "for feed" in error.lower():
            raise AssertionError(f"fresh same-boundary retry fenced against the resumable plan: {error}")

        status, result = attempt_split("different-boundary", boundary="40000000000000000000000000000000")
        if status == 200: raise AssertionError("conflicting split activated while a plan is staged")
        error = result.get("error", "")
        if "operation" not in error.lower():
            raise AssertionError(f"a conflicting split must fence while a plan is staged: {error}")
    finally:
        subprocess.run(["docker", "start", NODE3], check=True, capture_output=True)

    # The source range must be unfrozen: writes keep flowing after rollback.
    time.sleep(3)
    status, result = post(ENDPOINTS[0], "/v1/writers/append", {"request_id": str(uuid.uuid4()), "writer": writer, "session_epoch": epoch, "event_time_ns": "9", "key_base64": base64.b64encode(b"key-9").decode(), "payload_base64": base64.b64encode("post-rollback".encode()).decode(), "metadata_base64": {}})
    if status != 200: raise AssertionError(f"feed stayed frozen after refused split: {result}")

    # With the replica back, replaying the original request ID resumes the
    # staged plan and activates it — the durable resume contract.
    for _ in range(30):
        status, result = attempt_split("same-id-after-recovery", request_id=split_request_id)
        if status == 200: break
        time.sleep(2)
    if status != 200: raise AssertionError(f"split never resumed after recovery: {result}")
    routes = (result.get("range_map") or {}).get("routes") or []
    if len(routes) != 2: raise AssertionError(f"expected two routes after resumed split: {result}")

    # Owner-move leg: same retain-and-resume contract, plus the explicit
    # operator abandon for a permanently wedged plan and honest stale replay.
    move_feed = f"rbk{suffix}.moves"
    status, setup = post(ENDPOINTS[0], "/v1/admin/wcl", {"script": f"CREATE FEED {move_feed};"})
    if status != 200: raise AssertionError(setup)
    NODE3_ID = os.environ.get("WHITEWATER_TEST_NODE3_ID", "control-3")
    status, placed = post(ENDPOINTS[0], "/v1/admin/wcl", {"script": f"INSPECT PLACEMENT FOR FEED {move_feed};"})
    if status != 200: raise AssertionError(placed)
    assignments = placed["results"][0]["data"]["range_assignments"]
    if len(assignments) != 1: raise AssertionError(f"expected one range for the move leg: {assignments}")
    move_range = assignments[0]
    if NODE3_ID not in move_range["replicas"]:
        raise AssertionError(f"node3 is not a replica of the move-leg range: {move_range}")
    if move_range["owner"] == NODE3_ID:
        # node3 owns the range: stopping it fails the source freeze.
        new_owner = next(replica for replica in move_range["replicas"] if replica != NODE3_ID)
    else:
        # node3 follows the range: stopping it fails the target verify.
        new_owner = NODE3_ID
    move_request_id = str(uuid.uuid4())

    def attempt_move(tag, request_id=None):
        body = {"request_id": request_id or str(uuid.uuid4()), "feed": move_feed, "range_id": move_range["range_id"], "new_owner": new_owner}
        status, result = post(ENDPOINTS[1], "/v1/admin/ranges/move-owner", body)
        print(f"owner-move {tag}: status={status} error={result.get('error', '')[:140]}", flush=True)
        return status, result

    def staged_move_plans():
        status, placed = post(ENDPOINTS[0], "/v1/admin/wcl", {"script": f"INSPECT PLACEMENT FOR FEED {move_feed};"})
        if status != 200: raise AssertionError(placed)
        return placed["results"][0]["data"].get("owner_move_plans") or []

    subprocess.run(["docker", "stop", NODE3], check=True, capture_output=True)
    try:
        wait_control_plane(ENDPOINTS[0])
        status, result = attempt_move("during-outage", request_id=move_request_id)
        if status == 200: raise AssertionError("owner move unexpectedly activated while a participant was down")

        plans = staged_move_plans()
        if len(plans) != 1: raise AssertionError(f"staged owner-move plan was deleted on failure instead of retained: {plans}")
        staged_plan_id = plans[0]["plan_id"]

        # Same-request-ID retry resumes and fails the same way during the outage.
        status, result = attempt_move("same-id-during-outage", request_id=move_request_id)
        if status == 200: raise AssertionError("owner move unexpectedly activated on same-ID retry")
        error = result.get("error", "")
        if "operation" in error.lower() and "for feed" in error.lower():
            raise AssertionError(f"same-ID retry fenced against its own staged plan: {error}")

        # The operator remedy: fence-checked abandon clears the wedged plan.
        status, result = post(ENDPOINTS[0], "/v1/admin/ranges/abandon", {"request_id": str(uuid.uuid4()), "feed": move_feed, "plan_id": staged_plan_id})
        if status != 200: raise AssertionError(f"fenced abandon rejected the inspected plan: {result}")
        if staged_move_plans(): raise AssertionError("owner-move plan row survived a committed abandon")

        # Replaying the abandoned request ID must now fail honestly — the
        # staged row it names is gone.
        status, result = attempt_move("stale-replay-after-abandon", request_id=move_request_id)
        if status == 200: raise AssertionError("abandoned move unexpectedly activated on stale replay")
    finally:
        subprocess.run(["docker", "start", NODE3], check=True, capture_output=True)

    # With the participant back and the wedged plan abandoned, a fresh move
    # activates end-to-end.
    for _ in range(30):
        status, result = attempt_move("fresh-after-recovery")
        if status == 200: break
        time.sleep(2)
    if status != 200: raise AssertionError(f"owner move never recovered: {result}")
    if result.get("assignment", {}).get("owner") != new_owner:
        raise AssertionError(f"owner move did not land on the requested replica: {result}")

    print("OK: failed split retained its resumable plan (unfrozen, same-ID retry resumed and activated); failed owner move retained then abandoned via the fenced remedy, stale replay refused, fresh move activated", flush=True)

if __name__ == "__main__":
    main()
