import json, sys, time, urllib.error, urllib.request, uuid

ENDPOINTS = ["http://localhost:7071", "http://localhost:7072", "http://localhost:7073"]
KEY = "whitewater-local-development-admin-key"

def post(endpoint, path, body):
    request = urllib.request.Request(endpoint + path, data=json.dumps(body).encode(), headers={"authorization": f"Bearer {KEY}", "content-type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=10) as response: return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error: return error.code, json.loads(error.read() or b"{}")

def main():
    suffix = f"{int(time.time())}{uuid.uuid4().hex[:6]}"
    space, feed, writer, reader = f"m3{suffix}", f"m3{suffix}.events", f"writer{suffix}", f"reader{suffix}"
    status, setup = post(ENDPOINTS[0], "/v1/admin/wcl", {"script": f"CREATE SPACE {space}; CREATE FEED {feed}; CREATE WRITER {writer} TO {feed}; OPEN WRITER SESSION {writer}; CREATE READER {reader} FROM {feed} START AT BEGINNING;"})
    if status != 200: raise AssertionError(setup)
    writer_epoch = setup["results"][3]["data"]["session_epoch"]
    records = [{"request_id": str(uuid.uuid4()), "writer": writer, "session_epoch": writer_epoch, "event_time_ns": str(index), "key_base64": "aw==", "payload_base64": "dg==", "metadata_base64": {}} for index in range(1, 4)]
    status, batch = post(ENDPOINTS[1], "/v1/writers/append-batch", {"records": records})
    if status != 200 or len(batch["results"]) != 3: raise AssertionError(batch)
    status, opened = post(ENDPOINTS[2], "/v1/readers/open", {"request_id": str(uuid.uuid4()), "reader": reader, "capacity": 2})
    epoch = opened["session_epoch"]
    status, first = post(ENDPOINTS[2], "/v1/readers/fetch", {"request_id": str(uuid.uuid4()), "reader": reader, "session_epoch": epoch})
    if status != 200 or len(first["records"]) != 2: raise AssertionError(first)
    delivered = first["delivered_cursor"]
    status, acknowledged = post(ENDPOINTS[0], "/v1/readers/ack", {"request_id": str(uuid.uuid4()), "reader": reader, "session_epoch": epoch, "cursor": delivered})
    if status != 200 or acknowledged["acknowledged_cursor"] != delivered: raise AssertionError(acknowledged)
    status, reopened = post(ENDPOINTS[1], "/v1/readers/open", {"request_id": str(uuid.uuid4()), "reader": reader, "capacity": 2})
    if reopened["session_epoch"] != epoch + 1 or reopened["delivered_cursor"] != delivered: raise AssertionError(reopened)
    status, second = post(ENDPOINTS[1], "/v1/readers/fetch", {"request_id": str(uuid.uuid4()), "reader": reader, "session_epoch": reopened["session_epoch"]})
    if status != 200 or len(second["records"]) != 1: raise AssertionError(second)
    stale_status, _ = post(ENDPOINTS[0], "/v1/readers/fetch", {"request_id": str(uuid.uuid4()), "reader": reader, "session_epoch": epoch})
    if stale_status == 200: raise AssertionError("stale Reader epoch was accepted")
    status, baseline = post(ENDPOINTS[2], "/v1/readers/temporary/fetch", {"feed": feed, "new_only": True, "limit": 10})
    if status != 200 or baseline["records"] or not baseline["next_cursor"]: raise AssertionError(baseline)
    status, appended = post(ENDPOINTS[0], "/v1/writers/append", {"request_id": str(uuid.uuid4()), "writer": writer, "session_epoch": writer_epoch, "event_time_ns": "4", "key_base64": "aw==", "payload_base64": "dg==", "metadata_base64": {}})
    if status != 200: raise AssertionError(appended)
    status, waited = post(ENDPOINTS[1], "/v1/readers/temporary/fetch", {"feed": feed, "after": baseline["next_cursor"], "limit": 10, "wait_ms": 2000})
    if status != 200 or len(waited["records"]) != 1: raise AssertionError(waited)
    status, tail = post(ENDPOINTS[2], "/v1/readers/temporary/fetch", {"feed": feed, "tail": True, "limit": 2})
    if status != 200 or len(tail["records"]) != 2: raise AssertionError(tail)
    print(json.dumps({"status": "ok", "reader": reader, "first_delivery": 2, "resumed_delivery": 1, "temporary_new_only_delivery": 1, "tail_records": 2, "acknowledged_cursor": delivered}, indent=2))

if __name__ == "__main__":
    try: main()
    except Exception as error: print(f"M3 Reader acceptance failed: {error}", file=sys.stderr); sys.exit(1)
