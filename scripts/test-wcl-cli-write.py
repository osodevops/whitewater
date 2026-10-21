import base64, json, os, shutil, subprocess, sys, time, urllib.error, urllib.request, uuid

BASE_PORT = int(os.environ.get("WHITEWATER_TEST_BASE_PORT", "7071"))
ENDPOINTS = [f"http://localhost:{BASE_PORT + offset}" for offset in range(3)]
KEY = "whitewater-local-development-admin-key"
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WCL_CLI = os.path.join(ROOT, "scripts", "wcl-cli.ps1")

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
def wait_ready(endpoint):
    for _ in range(120):
        try:
            with urllib.request.urlopen(f"{endpoint}/health", timeout=3) as response:
                if response.status != 200:
                    raise ConnectionError("unhealthy")
            status, _ = post(endpoint, "/v1/admin/wcl", {"request_id": str(uuid.uuid4()), "script": "SHOW DOMAINS;"})
            if status == 200:
                return
        except Exception:
            pass
        time.sleep(0.5)
    raise AssertionError(f"{endpoint} never became ready")

def wcl_cli(*arguments):
    return subprocess.run(
        ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", WCL_CLI, *arguments],
        capture_output=True, text=True, timeout=120,
    )

def main():
    powershell = shutil.which("powershell")
    if not powershell:
        print(json.dumps({"status": "skipped", "reason": "powershell not on PATH â€” wcl-cli is a Windows PowerShell host tool"}))
        return
    for endpoint in ENDPOINTS:
        wait_ready(endpoint)

    suffix = f"{int(time.time())}{uuid.uuid4().hex[:6]}"
    domain, feed, writer = f"pscli{suffix}", f"pscli{suffix}.events", f"writer{suffix}"
    status, setup = post(ENDPOINTS[0], "/v1/admin/wcl", {"request_id": str(uuid.uuid4()), "script": f"CREATE DOMAIN {domain}; CREATE FEED {feed}; CREATE WRITER {writer} TO {feed}; OPEN WRITER SESSION {writer};"})
    if status != 200:
        raise AssertionError(setup)
    epoch = setup["results"][3]["data"]["session_epoch"]

    request_id = str(uuid.uuid4())
    key, payload, trace = b"order-42", b"total=99", b"hi"
    first = wcl_cli(
        "write", "-Endpoints", f"127.0.0.1:{BASE_PORT}", "-ApiKey", KEY,
        "-Writer", writer, "-SessionEpoch", str(epoch),
        "-Key", key.decode(), "-Payload", payload.decode(),
        "-EventTimeNs", "1700000000000000042",
        "-Metadata", f"trace={base64.b64encode(trace).decode()}",
        "-RequestId", request_id,
    )
    if first.returncode != 0:
        raise AssertionError(f"wcl-cli write failed: {first.stderr or first.stdout}")
    appended = json.loads(first.stdout)
    if not appended.get("cursor"):
        raise AssertionError(f"append response lacks a cursor: {appended}")

    # Identical request_id replays deduplicate at the data API â€” the same
    # ambiguity contract the SDKs rely on after an unknown outcome.
    replay = wcl_cli(
        "write", "-Endpoints", f"127.0.0.1:{BASE_PORT + 1}", "-ApiKey", KEY,
        "-Writer", writer, "-SessionEpoch", str(epoch),
        "-Key", key.decode(), "-Payload", payload.decode(),
        "-EventTimeNs", "1700000000000000042",
        "-Metadata", f"trace={base64.b64encode(trace).decode()}",
        "-RequestId", request_id,
    )
    if replay.returncode != 0:
        raise AssertionError(f"wcl-cli write replay failed: {replay.stderr or replay.stdout}")
    deduplicated = json.loads(replay.stdout)
    if not deduplicated.get("deduplicated") or deduplicated.get("cursor") != appended["cursor"]:
        raise AssertionError(f"identical request_id did not deduplicate: {deduplicated}")

    # The appended record is visible through the data read path with exact
    # bytes â€” the CLI never invents payloads or drops Metadata.
    status, fetched = post(ENDPOINTS[2], "/v1/readers/temporary/fetch", {"feed": feed, "limit": 10})
    if status != 200:
        raise AssertionError(fetched)
    matches = [
        record for record in fetched["records"]
        if record.get("key_base64") == base64.b64encode(key).decode()
        and record.get("payload_base64") == base64.b64encode(payload).decode()
        and record.get("metadata_base64", {}).get("trace") == base64.b64encode(trace).decode()
    ]
    if len(matches) != 1:
        raise AssertionError(f"appended record not visible exactly once: {fetched}")

    # Validation refuses before any wire traffic: a second payload source.
    refused = wcl_cli(
        "write", "-Endpoints", f"127.0.0.1:{BASE_PORT}", "-ApiKey", KEY,
        "-Writer", writer, "-SessionEpoch", str(epoch),
        "-Key", "k", "-Payload", "a", "-PayloadBase64", "dg==",
    )
    if refused.returncode == 0 or "exactly one payload source" not in (refused.stderr or ""):
        raise AssertionError(f"conflicting payload sources were accepted: {refused.stdout}")

    print(json.dumps({
        "status": "ok",
        "writer": writer,
        "cursor": appended["cursor"],
        "deduplicated_replay": True,
        "records_visible": len(matches),
    }, indent=2))

if __name__ == "__main__":
    try:
        main()
    except (AssertionError, subprocess.TimeoutExpired) as error:
        print(f"FAILED: {error}", file=sys.stderr)
        sys.exit(1)
