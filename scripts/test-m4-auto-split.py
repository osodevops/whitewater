import base64, json, os, subprocess, sys, time, urllib.error, urllib.request, uuid

BASE_PORT = int(os.environ.get("WHITEWATER_TEST_BASE_PORT", "7071"))
ENDPOINTS = [f"http://localhost:{BASE_PORT + offset}" for offset in range(3)]
KEY = "whitewater-local-development-admin-key"

def post(endpoint, path, body):
    request = urllib.request.Request(endpoint + path, data=json.dumps(body).encode(), headers={"authorization": f"Bearer {KEY}", "content-type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=60) as response: return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error: return error.code, json.loads(error.read() or b"{}")

def main():
    env = os.environ.copy()
    env.update({"WHITEWATER_AUTO_SPLIT_INTERVAL_MS": "500", "WHITEWATER_AUTO_SPLIT_APPEND_RATE": "2", "WHITEWATER_AUTO_SPLIT_SUSTAINED_SAMPLES": "1", "WHITEWATER_AUTO_SPLIT_COOLDOWN_SAMPLES": "20"})
    subprocess.run(["docker", "compose", "up", "-d", "--force-recreate"], check=True, env=env)
    try:
        time.sleep(5)
        suffix = f"auto{int(time.time())}{uuid.uuid4().hex[:6]}"
        space, feed, writer = suffix, f"{suffix}.events", f"writer{suffix}"
        status, setup = post(ENDPOINTS[0], "/v1/admin/wcl", {"script": f"CREATE SPACE {space}; CREATE FEED {feed}; CREATE WRITER {writer} TO {feed}; OPEN WRITER SESSION {writer};"})
        if status != 200: raise AssertionError(setup)
        epoch = setup["results"][-1]["data"]["session_epoch"]
        split_started = False
        for value in range(30):
            request = {"request_id": str(uuid.uuid4()), "writer": writer, "session_epoch": epoch, "event_time_ns": str(value), "key_base64": base64.b64encode(f"key-{value}".encode()).decode(), "payload_base64": "dg==", "metadata_base64": {}}
            for _ in range(20):
                status, result = post(ENDPOINTS[value % 3], "/v1/writers/append", request)
                if status == 200:
                    break
                if "frozen for split cutover" in result.get("error", ""):
                    split_started = True
                    break
                if status != 503:
                    raise AssertionError(result)
                time.sleep(0.1)
            else:
                raise AssertionError(f"stable Writer request did not resolve after retry: {result}")
            if split_started:
                break
            time.sleep(0.1)
        placement = None
        for _ in range(40):
            status, response = post(ENDPOINTS[0], "/v1/admin/wcl", {"script": f"INSPECT PLACEMENT FOR FEED {feed};"})
            if status == 200:
                placement = response["results"][0]["data"]
                if len(placement["range_map"]["routes"]) >= 2: break
            time.sleep(0.5)
        if placement is None or len(placement["range_map"]["routes"]) < 2:
            logs = subprocess.run(["docker", "compose", "logs", "--tail=200"], capture_output=True, text=True)
            raise AssertionError(f"sustained pressure did not trigger a split\n{logs.stdout}")
        print(json.dumps({"status": "ok", "feed": feed, "ranges": len(placement["range_map"]["routes"]), "owners": sorted({item["owner"] for item in placement["range_assignments"]})}, indent=2))
    finally:
        subprocess.run(["docker", "compose", "up", "-d", "--force-recreate"], check=True)

if __name__ == "__main__":
    try: main()
    except Exception as error: print(f"M4 automatic split failed: {error}", file=sys.stderr); sys.exit(1)
