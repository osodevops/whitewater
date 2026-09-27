import base64, json, subprocess, sys, time, urllib.error, urllib.parse, urllib.request, uuid

ENDPOINTS = {"control-1": "http://localhost:7071", "control-2": "http://localhost:7072", "control-3": "http://localhost:7073"}
KEY = "whitewater-local-development-admin-key"

def call(endpoint, path, body=None):
    headers = {"authorization": f"Bearer {KEY}"}
    data = None
    method = "GET"
    if body is not None:
        headers["content-type"] = "application/json"
        data = json.dumps(body).encode()
        method = "POST"
    request = urllib.request.Request(endpoint + path, data=data, headers=headers, method=method)
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.loads(response.read())

def main():
    suffix = f"{int(time.time())}{uuid.uuid4().hex[:6]}"
    space, feed = f"m110{suffix}", f"m110{suffix}.events"
    created = call(ENDPOINTS["control-1"], "/v1/admin/wcl", {"script": f"CREATE SPACE {space}; CREATE FEED {feed}; INSPECT PLACEMENT FOR FEED {feed};"})
    owner = created["results"][-1]["data"]["owner"]
    target = next(node for node in ENDPOINTS if node != owner)
    target_service = f"node{target.removeprefix('control-')}"
    subprocess.run(["docker", "compose", "stop", target_service], check=True)
    try:
        writer = str(uuid.uuid4())
        ingress = ENDPOINTS[owner]
        for sequence in range(1, 4):
            result = call(ingress, "/v1/feeds/append", {"request_id": str(uuid.uuid4()), "feed": feed, "writer_session_id": writer, "writer_epoch": 1, "sequence": sequence, "event_time_ns": str(sequence), "key_base64": "aw==", "payload_base64": base64.b64encode(f"value-{sequence}".encode()).decode(), "metadata_base64": {}})
            if result["durability"] != "majority_committed":
                raise AssertionError("append was not majority committed")
    finally:
        subprocess.run(["docker", "compose", "start", target_service], check=True)
    encoded = urllib.parse.quote(feed, safe="")
    records = None
    for _ in range(30):
        try:
            records = call(ENDPOINTS[target], f"/v1/feeds/records?feed={encoded}&limit=10")
            if len(records) == 3:
                break
        except (urllib.error.URLError, TimeoutError):
            pass
        time.sleep(2)
    if records is None or len(records) != 3:
        raise AssertionError(f"restarted replica did not catch up: {records}")
    print(json.dumps({"status": "ok", "feed": feed, "owner": owner, "repaired_replica": target, "records": len(records)}, indent=2))

if __name__ == "__main__":
    try: main()
    except Exception as error:
        print(f"M1.10 acceptance failed: {error}", file=sys.stderr)
        sys.exit(1)
