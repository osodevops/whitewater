import base64, json, os, sys, time, urllib.error, urllib.parse, urllib.request, uuid

BASE_PORT = int(os.environ.get("WHITEWATER_TEST_BASE_PORT", "7071"))
ENDPOINTS = [f"http://localhost:{BASE_PORT + offset}" for offset in range(3)]
KEY = "whitewater-local-development-admin-key"

def post(endpoint, path, body):
    request = urllib.request.Request(endpoint + path, data=json.dumps(body).encode(), headers={"authorization": f"Bearer {KEY}", "content-type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=60) as response: return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error: return error.code, json.loads(error.read() or b"{}")

def main():
    suffix = f"{int(time.time())}{uuid.uuid4().hex[:6]}"
    space, feed, writer = f"m4{suffix}", f"m4{suffix}.events", f"writer{suffix}"
    status, setup = post(ENDPOINTS[0], "/v1/admin/wcl", {"script": f"CREATE SPACE {space}; CREATE FEED {feed}; CREATE WRITER {writer} TO {feed}; OPEN WRITER SESSION {writer};"})
    if status != 200: raise AssertionError(setup)
    epoch = setup["results"][-1]["data"]["session_epoch"]
    def append(index, endpoint):
        return post(endpoint, "/v1/writers/append", {"request_id": str(uuid.uuid4()), "writer": writer, "session_epoch": epoch, "event_time_ns": str(index), "key_base64": base64.b64encode(f"key-{index}".encode()).decode(), "payload_base64": base64.b64encode(f"value-{index}".encode()).decode(), "metadata_base64": {}})
    for index in range(1, 9):
        status, result = append(index, ENDPOINTS[index % 3])
        if status != 200: raise AssertionError(result)
    status, split = post(ENDPOINTS[1], "/v1/admin/ranges/split", {"request_id": str(uuid.uuid4()), "feed": feed, "split_at": "80000000000000000000000000000000", "batch_size": 2})
    if status != 200 or split.get("status") != "activated": raise AssertionError(split)
    status, placement = post(ENDPOINTS[2], "/v1/admin/wcl", {"script": f"INSPECT PLACEMENT FOR FEED {feed};"})
    data = placement["results"][0]["data"]
    routes = data["range_map"]["routes"]
    owners = {assignment["owner"] for assignment in data["range_assignments"]}
    if len(routes) != 2 or len(owners) != 2: raise AssertionError(data)
    for index in range(9, 17):
        status, result = append(index, ENDPOINTS[index % 3])
        if status != 200: raise AssertionError(result)
    encoded = urllib.parse.quote(feed, safe="")
    with urllib.request.urlopen(urllib.request.Request(f"{ENDPOINTS[0]}/v1/feeds/records?feed={encoded}&limit=100", headers={"authorization": f"Bearer {KEY}"}), timeout=30) as response:
        records = json.loads(response.read())
    if len(records) != 16: raise AssertionError(f"expected 16 records, got {len(records)}")
    print(json.dumps({"status": "ok", "feed": feed, "ranges": len(routes), "owners": sorted(owners), "records": len(records)}, indent=2))

if __name__ == "__main__":
    try: main()
    except Exception as error: print(f"M4 live split failed: {error}", file=sys.stderr); sys.exit(1)
