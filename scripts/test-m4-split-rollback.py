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

def main():
    suffix = f"{int(time.time())}{uuid.uuid4().hex[:6]}"
    domain, feed, writer = f"rbk{suffix}", f"rbk{suffix}.events", f"rbkw{suffix}"
    status, setup = post(ENDPOINTS[0], "/v1/admin/wcl", {"script": f"CREATE DOMAIN {domain}; CREATE FEED {feed}; CREATE WRITER {writer} TO {feed}; OPEN WRITER SESSION {writer};"})
    if status != 200: raise AssertionError(setup)
    epoch = setup["results"][3]["data"]["session_epoch"]
    for position in range(1, 9):
        status, result = post(ENDPOINTS[0], "/v1/writers/append", {"request_id": str(uuid.uuid4()), "writer": writer, "session_epoch": epoch, "event_time_ns": str(position), "key_base64": base64.b64encode(f"key-{position}".encode()).decode(), "payload_base64": base64.b64encode(f"value-{position}".encode()).decode(), "metadata_base64": {}})
        if status != 200: raise AssertionError(result)

    def attempt_split(tag):
        status, result = post(ENDPOINTS[1], "/v1/admin/ranges/split", {"request_id": str(uuid.uuid4()), "feed": feed, "split_at": "80000000000000000000000000000000"})
        print(f"split {tag}: status={status} error={result.get('error', '')[:140]}", flush=True)
        return status, result

    # Fail the cutover mid-staging: a replica is unreachable, so the staged
    # plan must roll back (unfreeze + abandon) instead of leaving a row in
    # range_split_plans that blocks every later range operation on this Feed.
    subprocess.run(["docker", "stop", NODE3], check=True, capture_output=True)
    try:
        status, result = attempt_split("during-outage")
        if status == 200: raise AssertionError("split unexpectedly activated while a replica was down")
        error = result.get("error", "")
        if "pending" in error.lower() and "operation" in error.lower():
            raise AssertionError(f"split wedged behind a stale plan: {error}")

        # A second attempt during the outage must refuse the same way rather
        # than report "a range operation is already pending".
        status, result = attempt_split("retry-during-outage")
        if status == 200: raise AssertionError("split unexpectedly activated on retry")
        error = result.get("error", "")
        if "pending" in error.lower() and "operation" in error.lower():
            raise AssertionError(f"stale prepared plan still blocks the Feed: {error}")
    finally:
        subprocess.run(["docker", "start", NODE3], check=True, capture_output=True)

    # The source range must be unfrozen: writes keep flowing after rollback.
    time.sleep(3)
    status, result = post(ENDPOINTS[0], "/v1/writers/append", {"request_id": str(uuid.uuid4()), "writer": writer, "session_epoch": epoch, "event_time_ns": "9", "key_base64": base64.b64encode(b"key-9").decode(), "payload_base64": base64.b64encode("post-rollback".encode()).decode(), "metadata_base64": {}})
    if status != 200: raise AssertionError(f"feed stayed frozen after refused split: {result}")

    # With the replica back, the same split must now succeed end-to-end —
    # proof that the failed attempt left no catalog or freeze residue.
    for _ in range(30):
        status, result = attempt_split("after-recovery")
        if status == 200: break
        time.sleep(2)
    if status != 200: raise AssertionError(f"split never recovered: {result}")
    routes = (result.get("range_map") or {}).get("routes") or []
    if len(routes) != 2: raise AssertionError(f"expected two routes after split: {result}")

    print("OK: failed split rolled back (unfrozen + plan abandoned), feed writable, retry activated", flush=True)

if __name__ == "__main__":
    main()
