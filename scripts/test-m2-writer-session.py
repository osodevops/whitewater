import base64, json, os, sys, time, urllib.error, urllib.request, uuid

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
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read() or b"{}")
    except OSError:
        return 0, {}
def main():
    suffix = f"{int(time.time())}{uuid.uuid4().hex[:6]}"
    space, feed, writer = f"m2{suffix}", f"m2{suffix}.events", f"writer{suffix}"
    status, setup = post(ENDPOINTS[0], "/v1/admin/wcl", {"request_id": str(uuid.uuid4()), "script": f"CREATE SPACE {space}; CREATE FEED {feed}; CREATE WRITER {writer} TO {feed}; OPEN WRITER SESSION {writer};"})
    if status != 200: raise AssertionError(setup)
    epoch = setup["results"][-1]["data"]["session_epoch"]
    append = {"request_id": str(uuid.uuid4()), "writer": writer, "session_epoch": epoch, "event_time_ns": "1", "key_base64": "aw==", "payload_base64": "dg==", "metadata_base64": {}}
    results = []
    for endpoint in ENDPOINTS:
        status, result = post(endpoint, "/v1/writers/append", append)
        if status != 200: raise AssertionError(result)
        results.append(result)
    if results[0]["deduplicated"] or not all(result["deduplicated"] for result in results[1:]): raise AssertionError("cross-Node Writer retry was not deduplicated")
    if len({result["message_id"] for result in results}) != 1: raise AssertionError("MessageId changed across retry")
    if results[0]["feedback"]["max_frame_bytes"] <= 0: raise AssertionError("server batching feedback missing")
    batch = {"records": [
        {"request_id": str(uuid.uuid4()), "writer": writer, "session_epoch": epoch, "event_time_ns": str(index + 2), "key_base64": "aw==", "payload_base64": "dmFsdWU=", "metadata_base64": {}}
        for index in range(3)
    ]}
    status, batch_result = post(ENDPOINTS[0], "/v1/writers/append-batch", batch)
    if status != 200 or len(batch_result["results"]) != 3: raise AssertionError("Writer batch did not commit all records")
    if batch_result["feedback"]["recommended_batch_count"] <= 0: raise AssertionError("batch feedback missing")
    payload = base64.b64encode(b'{"device":"meter","bytesUp":123456}' * 1000).decode()
    for index, enabled in enumerate([False, True, False, True]):
        record = dict(append, request_id=str(uuid.uuid4()), payload_base64=payload, use_compression=enabled)
        status, written = post(ENDPOINTS[index % 3], "/v1/writers/append", record)
        if status != 200: raise AssertionError(written)
        retry = dict(record, use_compression=not enabled)
        status, replay = post(ENDPOINTS[(index + 1) % 3], "/v1/writers/append", retry)
        if status != 200 or not replay["deduplicated"] or replay["cursor"] != written["cursor"]: raise AssertionError(replay)
    status, page = post(ENDPOINTS[2], "/v1/readers/temporary/fetch", {"feed": feed, "limit": 20})
    if status != 200 or len(page["records"]) != 8: raise AssertionError(page)
    if [record["payload_base64"] for record in page["records"]][-4:] != [payload] * 4: raise AssertionError("compression changed application payloads")
    status, opened = post(ENDPOINTS[0], "/v1/admin/wcl", {"request_id": str(uuid.uuid4()), "script": f"OPEN WRITER SESSION {writer};"})
    new_epoch = opened["results"][0]["data"]["session_epoch"]
    stale = dict(append); stale["request_id"] = str(uuid.uuid4())
    status, _ = post(ENDPOINTS[1], "/v1/writers/append", stale)
    if status == 200: raise AssertionError("stale Writer epoch was accepted")
    post(ENDPOINTS[0], "/v1/admin/wcl", {"request_id": str(uuid.uuid4()), "script": f"REVOKE WRITER SESSION {writer} EPOCH {new_epoch};"})
    # Session revocation is a catalog change that propagates to each
    # Node's session registry asynchronously; probe ENDPOINTS[2] until
    # the revoked epoch is rejected rather than racing that applier.
    deadline = time.time() + 15
    while True:
        revoked = dict(append); revoked.update({"request_id": str(uuid.uuid4()), "session_epoch": new_epoch})
        status, _ = post(ENDPOINTS[2], "/v1/writers/append", revoked)
        if status != 200:
            break
        if time.time() >= deadline:
            raise AssertionError("revoked Writer session was accepted")
        time.sleep(0.5)
    print(json.dumps({"status": "ok", "writer": writer, "epoch": epoch, "new_epoch": new_epoch, "message_id": results[0]["message_id"], "feedback": results[0]["feedback"]}, indent=2))

if __name__ == "__main__":
    try: main()
    except Exception as error:
        print(f"M2 Writer session acceptance failed: {error}", file=sys.stderr); sys.exit(1)
