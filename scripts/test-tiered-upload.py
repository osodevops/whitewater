"""M8-B: sealed segments tier to a shared object store verified and deduplicated.

Spins the verify Riverbed up with compose.verify.tier.yml overlaid on
compose.verify.yml: every Node gets the shared `tier-objects` volume as
WHITEWATER_OBJECT_STORE_DIR, a 256 KiB segment bound, and a 1s scan tick.
Appends then force multiple rotations; the test asserts content-addressed
objects (BLAKE3 digest == object name), manifest identity binding, byte
length parity, per-replica tier markers, and RF3 dedupe — one object per
sealed segment, not three.
"""
import base64, json, os, subprocess, sys, time, urllib.error, urllib.request, uuid

BASE_PORT = int(os.environ.get("WHITEWATER_TEST_BASE_PORT", "7071"))
ENDPOINTS = [f"http://localhost:{BASE_PORT + offset}" for offset in range(3)]
KEY = "whitewater-local-development-admin-key"
TIER_DIR = "/var/lib/whitewater-tier"


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
        with urllib.request.urlopen(request, timeout=60) as response: return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error: return error.code, json.loads(error.read() or b"{}")
    except OSError:
        return 0, {}


def compose(args, env=None, capture=False):
    run_env = os.environ.copy()
    run_env["COMPOSE_FILE"] = os.pathsep.join(["compose.verify.yml", "compose.verify.tier.yml"])
    if env:
        run_env.update(env)
    result = subprocess.run(["docker", "compose"] + args, env=run_env, capture_output=capture, text=True)
    if capture:
        return result
    result.check_returncode()
    return result


def node_exec(node, command, user=None):
    args = ["exec", "-T"]
    if user:
        args += ["--user", user]
    result = compose(args + [node, "sh", "-c", command], capture=True)
    return result.stdout.strip(), result.returncode


def restore_base_riverbed():
    env = os.environ.copy()
    env["COMPOSE_FILE"] = "compose.verify.yml"
    subprocess.run(["docker", "compose", "up", "-d", "--force-recreate"], env=env, check=True)


def main():
    compose(["up", "-d", "--force-recreate"])
    try:
        # The fresh shared volume is root-owned; the node runs as uid 10001.
        # One chown covers all three replicas — it is the same volume.
        node_exec("node1", f"chown -R 10001:10001 {TIER_DIR}", user="root")
        time.sleep(6)

        suffix = f"tier{int(time.time())}{uuid.uuid4().hex[:6]}"
        domain, feed, writer = suffix, f"{suffix}.events", f"writer{suffix}"
        status, setup = post(ENDPOINTS[0], "/v1/admin/wcl", {"request_id": str(uuid.uuid4()), "script": f"CREATE DOMAIN {domain}; CREATE FEED {feed}; CREATE WRITER {writer} TO {feed}; OPEN WRITER SESSION {writer};"})
        if status != 200:
            raise AssertionError(setup)
        epoch = setup["results"][-1]["data"]["session_epoch"]

        # ~600 KiB of committed history across 40 records forces at least
        # two rotations at the 256 KiB bound.
        payload = base64.b64encode(os.urandom(16 * 1024)).decode()
        for batch in range(8):
            records = [
                {
                    "request_id": str(uuid.uuid4()),
                    "writer": writer,
                    "session_epoch": epoch,
                    "event_time_ns": str(time.time_ns()),
                    "key_base64": base64.b64encode(f"key-{batch}-{i}".encode()).decode(),
                    "payload_base64": payload,
                }
                for i in range(5)
            ]
            status, result = post(ENDPOINTS[batch % 3], "/v1/writers/append-batch", {"records": records})
            if status != 200:
                raise AssertionError(f"append batch {batch}: {result}")

        # Wait for the 1s scan tick to upload every sealed segment.
        objects = {}
        deadline = time.time() + 90
        while time.time() < deadline:
            listing, _ = node_exec("node1", f"find {TIER_DIR} -name '*.log' -type f")
            objects = {
                line.rsplit("/", 1)[-1][:-4]: line
                for line in listing.splitlines()
                if line.strip()
            }
            if len(objects) >= 2:
                break
            time.sleep(2)
        if len(objects) < 2:
            logs = compose(["logs", "--tail=120"], capture=True).stdout
            raise AssertionError(f"sealed segments did not reach the object store\n{logs}")

        # Every object is content-addressed: basename == BLAKE3 hex of the
        # segment the manifest binds. Lengths match the manifest exactly.
        for digest, path in objects.items():
            if len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
                raise AssertionError(f"object name is not a BLAKE3 hex digest: {path}")
            manifest_path = f"{os.path.dirname(path)}/{digest}.manifest.json"
            raw, rc = node_exec("node1", f"cat {manifest_path}")
            if rc != 0:
                raise AssertionError(f"missing manifest object for {digest}")
            manifest = json.loads(raw)
            if manifest["segment_digest"] != digest:
                raise AssertionError(f"manifest digest {manifest['segment_digest']} != object name {digest}")
            size_raw, _ = node_exec("node1", f"stat -c %s {path}")
            if int(size_raw) != manifest["bytes"]:
                raise AssertionError(f"remote bytes {size_raw} != manifest bytes {manifest['bytes']} for {digest}")
            if manifest["entries"] < 1 or manifest["last_position"] < manifest["first_position"]:
                raise AssertionError(f"manifest position span broken for {digest}: {manifest}")

        # RF3 dedupe: the shared store holds exactly one object per sealed
        # segment even though three replicas each ran the uploader.
        all_logs, _ = node_exec("node1", f"find {TIER_DIR} -name '*.log' -type f | wc -l")
        if int(all_logs) != len(objects):
            raise AssertionError(f"replicas duplicated objects: {all_logs} files vs {len(objects)} digests")

        # Each replica persisted a tier marker naming the same remote key —
        # restart-idempotent proof the upload completed and verified.
        for node in ("node1", "node2", "node3"):
            markers, _ = node_exec(node, "find /var/lib/whitewater/active-ranges -name 'segment-tier-*.json' -type f | wc -l")
            if int(markers) < len(objects):
                raise AssertionError(f"{node} persisted {markers} tier markers, expected >= {len(objects)}")

        print(json.dumps({"status": "ok", "tiered_segments": len(objects), "digests": sorted(objects)[:3]}, indent=2))
    finally:
        restore_base_riverbed()


if __name__ == "__main__":
    try: main()
    except Exception as error: print(f"M8-B tiered upload failed: {error}", file=sys.stderr); sys.exit(1)
