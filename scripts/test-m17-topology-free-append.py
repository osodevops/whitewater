import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

ENDPOINTS = {
    "control-1": "http://localhost:7071",
    "control-2": "http://localhost:7072",
    "control-3": "http://localhost:7073",
}
ADMIN_KEY = os.environ.get(
    "FINNSTREAM_ADMIN_API_KEY", "whitewater-local-development-admin-key"
)


def request(endpoint, path, body, authenticated=True):
    headers = {"content-type": "application/json"}
    if authenticated:
        headers["authorization"] = f"Bearer {ADMIN_KEY}"
    request_value = urllib.request.Request(
        f"{endpoint}{path}",
        data=json.dumps(body).encode(),
        headers=headers,
        method="POST",
    )
    try:
        with urllib.request.urlopen(request_value, timeout=10) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error:
        payload = json.loads(error.read() or b"{}")
        return error.code, payload


def get(endpoint, path):
    request_value = urllib.request.Request(
        f"{endpoint}{path}",
        headers={"authorization": f"Bearer {ADMIN_KEY}"},
        method="GET",
    )
    with urllib.request.urlopen(request_value, timeout=10) as response:
        return response.status, json.loads(response.read())


def assert_equal(actual, expected, message):
    if actual != expected:
        raise AssertionError(f"{message}: expected {expected!r}, got {actual!r}")


def main():
    for endpoint in ENDPOINTS.values():
        with urllib.request.urlopen(f"{endpoint}/health", timeout=5) as response:
            assert_equal(response.status, 200, f"health check for {endpoint}")

    suffix = f"{int(time.time())}{uuid.uuid4().hex[:6]}"
    space = f"m17{suffix}"
    feed = f"{space}.events"
    status, created = request(
        ENDPOINTS["control-1"],
        "/v1/admin/wcl",
        {"script": f"CREATE SPACE {space}; CREATE FEED {feed}; INSPECT PLACEMENT FOR FEED {feed};"},
    )
    assert_equal(status, 200, "Feed creation")
    placement = created["results"][-1]["data"]
    owner = placement["owner"]
    if owner not in ENDPOINTS:
        raise AssertionError(f"unknown owner endpoint: {owner}")

    append = {
        "request_id": str(uuid.uuid4()),
        "feed": feed,
        "writer_session_id": str(uuid.uuid4()),
        "writer_epoch": 1,
        "sequence": 1,
        "event_time_ns": "1700000000123456789",
        "key_base64": "Y3VzdG9tZXItMQ==",
        "payload_base64": "eyJvcmRlcklkIjoiQTEwMCJ9",
        "metadata_base64": {"trace-id": "bTE3LXRyYWNl"},
    }

    status, unauthorized = request(
        ENDPOINTS[owner], "/v1/feeds/append", append, authenticated=False
    )
    assert_equal(status, 401, "unauthenticated append")
    if "error" not in unauthorized:
        raise AssertionError("unauthenticated append did not return an actionable error")

    ingress_order = [owner] + [node for node in ENDPOINTS if node != owner]
    results = []
    for index, node in enumerate(ingress_order):
        status, result = request(ENDPOINTS[node], "/v1/feeds/append", append)
        assert_equal(status, 200, f"append through {node}")
        assert_equal(result["durability"], "majority_committed", f"durability through {node}")
        assert_equal(result["deduplicated"], index > 0, f"deduplication through {node}")
        results.append(result)

    original = results[0]
    for node, result in zip(ingress_order[1:], results[1:]):
        assert_equal(result["message_id"], original["message_id"], f"MessageId through {node}")
        assert_equal(result["cursor"], original["cursor"], f"Cursor through {node}")

    encoded_feed = urllib.parse.quote(feed, safe="")
    for node in ingress_order:
        status, records = get(
            ENDPOINTS[node], f"/v1/feeds/records?feed={encoded_feed}&limit=10"
        )
        assert_equal(status, 200, f"committed read through {node}")
        assert_equal(len(records), 1, f"committed record count through {node}")
        assert_equal(records[0]["message_id"], original["message_id"], f"read MessageId through {node}")
        assert_equal(records[0]["cursor"], original["cursor"], f"read Cursor through {node}")

    encoded_cursor = urllib.parse.quote(original["cursor"], safe="")
    status, after = get(
        ENDPOINTS[owner],
        f"/v1/feeds/records?feed={encoded_feed}&after={encoded_cursor}&limit=10",
    )
    assert_equal(status, 200, "read after Cursor")
    assert_equal(after, [], "read after latest Cursor")

    missing = dict(append)
    missing["request_id"] = str(uuid.uuid4())
    missing["feed"] = f"{space}.missing"
    status, error = request(ENDPOINTS[owner], "/v1/feeds/append", missing)
    assert_equal(status, 400, "unknown Feed append")
    if "error" not in error:
        raise AssertionError("unknown Feed append did not return an actionable error")

    print(
        json.dumps(
            {
                "status": "ok",
                "feed": feed,
                "owner": owner,
                "ingress_nodes": ingress_order,
                "message_id": original["message_id"],
                "cursor": original["cursor"],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"M1.7 acceptance failed: {error}", file=sys.stderr)
        sys.exit(1)
