import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid

BASE_PORT = int(os.environ.get("WHITEWATER_TEST_BASE_PORT", "7071"))
ENDPOINTS = {f"control-{index}": f"http://localhost:{BASE_PORT + index - 1}" for index in range(1, 4)}
KEY = "whitewater-local-development-admin-key"


def post(endpoint, path, body):
    request = urllib.request.Request(
        endpoint + path,
        data=json.dumps(body).encode(),
        headers={"authorization": f"Bearer {KEY}", "content-type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.loads(response.read())


def inspect(endpoint, feed):
    return post(endpoint, "/v1/admin/wcl", {"script": f"INSPECT PLACEMENT FOR FEED {feed};"})["results"][0]["data"]


def main():
    suffix = f"{int(time.time())}{uuid.uuid4().hex[:6]}"
    space = f"m19{suffix}"
    feed = f"{space}.events"
    created = post(ENDPOINTS["control-1"], "/v1/admin/wcl", {"script": f"CREATE SPACE {space}; CREATE FEED {feed}; INSPECT PLACEMENT FOR FEED {feed};"})
    original = created["results"][-1]["data"]
    old_owner = original["owner"]
    service = f"node{old_owner.removeprefix('control-')}"
    subprocess.run(["docker", "compose", "stop", service], check=True)
    try:
        survivors = [endpoint for node, endpoint in ENDPOINTS.items() if node != old_owner]
        recovered = None
        for _ in range(30):
            for endpoint in survivors:
                try:
                    placement = inspect(endpoint, feed)
                    if placement["owner"] != old_owner:
                        recovered = placement
                        break
                except (urllib.error.URLError, TimeoutError):
                    pass
            if recovered:
                break
            time.sleep(2)
        if recovered is None:
            raise AssertionError("ownership did not transfer after sustained owner failure")
        if recovered["ownership_epoch"] != original["ownership_epoch"] + 1:
            raise AssertionError("ownership epoch did not increase exactly once")
        result = post(survivors[0], "/v1/feeds/append", {
            "request_id": str(uuid.uuid4()), "feed": feed,
            "writer_session_id": str(uuid.uuid4()), "writer_epoch": 1, "sequence": 1,
            "event_time_ns": "1", "key_base64": "aw==", "payload_base64": "dg==", "metadata_base64": {},
        })
        if result["durability"] != "majority_committed":
            raise AssertionError("append did not resume with majority durability")
        print(json.dumps({"status": "ok", "feed": feed, "old_owner": old_owner, "new_owner": recovered["owner"], "ownership_epoch": recovered["ownership_epoch"], "message_id": result["message_id"]}, indent=2))
    finally:
        subprocess.run(["docker", "compose", "start", service], check=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"M1.9 acceptance failed: {error}", file=sys.stderr)
        sys.exit(1)
