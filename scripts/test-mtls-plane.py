import http.client
import json
import pathlib
import ssl
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

COMPOSE = ["docker", "compose", "-f", "compose.mtls.yml", "-p", "whitewater-mtls"]
NODES = [f"http://localhost:{port}" for port in range(7171, 7175)]
TLS_PORTS = range(7571, 7575)
KEY = "whitewater-local-development-admin-key"
DRAINED = "control-4"
CERTS_DIR = ".mtls-certs"


def post(node, path, payload):
    request = urllib.request.Request(
        f"{node}{path}",
        data=json.dumps(payload).encode(),
        headers={"authorization": f"Bearer {KEY}", "content-type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        return error.code, json.load(error)


def wcl(node, script):
    status, response = post(node, "/v1/admin/wcl", {"script": script})
    if status != 200:
        raise AssertionError({"script": script, "response": response})
    return [result["data"] for result in response["results"]]


def wcl_retry(node, script, attempts=30, delay=1):
    last = None
    for _ in range(attempts):
        try:
            return wcl(node, script)
        except AssertionError as error:
            last = error
            time.sleep(delay)
    raise last


def placement(node, feed):
    return wcl_retry(node, f"INSPECT PLACEMENT FOR FEED {feed};")[0]


def read(node, feed):
    query = urllib.parse.urlencode({"feed": feed, "limit": 100})
    request = urllib.request.Request(
        f"{node}/v1/feeds/records?{query}",
        headers={"authorization": f"Bearer {KEY}"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def wait_for_fabric():
    for _ in range(120):
        try:
            if all(
                urllib.request.urlopen(f"{node}/health", timeout=2).status == 200
                for node in NODES
            ):
                return
        except (urllib.error.URLError, TimeoutError, http.client.RemoteDisconnected):
            pass
        time.sleep(1)
    raise AssertionError("four-Node mTLS Riverbed did not become healthy")


def generate_certificates():
    subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "-v",
            f"{pathlib.Path(__file__).resolve().parents[1].as_posix()}:/workspace",
            "-w",
            "/workspace",
            "-e",
            "WW_CERTS_DIR=/workspace/.mtls-certs",
            "-e",
            "WW_CERTS_NODES=control-1,control-2,control-3,control-4",
            "-e",
            "WW_CERTS_PORT=7271",
            "rust:1.90-bookworm",
            "cargo",
            "test",
            "--release",
            "--test",
            "dev_cert_gen",
        ],
        check=True,
    )


def internal_plane_refuses_downgrade():
    # The public listener must not serve any internal route once mTLS is on.
    for port in (7171, 7173):
        try:
            urllib.request.urlopen(
                f"http://localhost:{port}/internal/active-range/pressure",
                timeout=3,
            )
            raise AssertionError("public listener served an internal route")
        except urllib.error.HTTPError as error:
            if error.code != 404:
                raise AssertionError({"expected": 404, "got": error.code})
    # The mTLS listener refuses clients without a pinned Node certificate:
    # the TLS handshake fails before any HTTP status is produced.
    unverified = ssl._create_unverified_context()
    try:
        urllib.request.urlopen(
            "https://localhost:7571/internal/active-range/pressure",
            timeout=5,
            context=unverified,
        )
        raise AssertionError("mTLS listener accepted an uncertified client")
    except (urllib.error.URLError, ssl.SSLError, ConnectionError, OSError):
        pass


def main():
    if not pathlib.Path(CERTS_DIR).joinpath("ca.pem").exists():
        generate_certificates()
    subprocess.run([*COMPOSE, "down", "-v"], check=True)
    subprocess.run([*COMPOSE, "up", "-d", "--build"], check=True)
    try:
        wait_for_fabric()
        internal_plane_refuses_downgrade()

        suffix = f"mt{uuid.uuid4().hex[:12]}"
        feeds = [f"{suffix}.events{index}" for index in range(5)]
        subscription = f"{suffix}.billing"
        script = "; ".join(
            [f"CREATE SPACE {suffix}"]
            + [f"CREATE FEED {feed}" for feed in feeds]
            + [f"CREATE SUBSCRIPTION {subscription} FROM {feeds[0]}"]
        ) + ";"
        for _ in range(30):
            status, _ = post(NODES[0], "/v1/admin/wcl", {"script": script})
            if status == 200:
                break
            time.sleep(1)
        if status != 200:
            raise AssertionError("mTLS Riverbed setup failed")

        # Append through a non-owner Node so the request forwards over the
        # authenticated internal plane; RF3 commit then uses mTLS replica
        # append/commit.
        for target in NODES:
            status, appended = post(
                target,
                "/v1/feeds/append",
                {
                    "request_id": str(uuid.uuid4()),
                    "feed": feeds[0],
                    "writer_session_id": str(uuid.uuid4()),
                    "writer_epoch": 1,
                    "sequence": 1,
                    "event_time_ns": "1",
                    "key_base64": "b3JkZXItMQ==",
                    "payload_base64": "ZXZlbnQtMQ==",
                },
            )
            if status == 200:
                break
        if status != 200:
            raise AssertionError(appended)

        # A cooperating member join exercises Subscription progress
        # prepare/commit over the pinned-certificate path.
        member_id = str(uuid.uuid4())
        status, joined = post(
            NODES[1],
            "/v1/subscriptions/members/join",
            {
                "subscription": subscription,
                "request_id": str(uuid.uuid4()),
                "member_id": member_id,
            },
        )
        if status != 200 or member_id not in joined.get("member_epochs", {}):
            raise AssertionError(joined)

        held = {feed: placement(NODES[0], feed)["replicas"] for feed in feeds}
        if not any(DRAINED in replicas for replicas in held.values()):
            raise AssertionError(f"{DRAINED} holds no Feed replicas: {held}")

        wcl_retry(NODES[0], f"DRAIN STORAGE NODE {DRAINED};")

        deadline = time.time() + 300
        plan = None
        while time.time() < deadline:
            try:
                plan = wcl(NODES[0], f"INSPECT DRAIN FOR STORAGE NODE {DRAINED};")[0]
            except AssertionError:
                plan = None
            if plan and plan["ready_to_retire"] and not plan["moves"]:
                break
            time.sleep(5)
        else:
            raise AssertionError(f"mTLS drain did not complete: {plan}")
        if plan["unplannable"]:
            raise AssertionError(f"drain reported unplannable references: {plan}")

        for feed, replicas in held.items():
            current = placement(NODES[0], feed)
            if DRAINED in current["replicas"] or current["owner"] == DRAINED:
                raise AssertionError({"feed": feed, "was": replicas, "now": current})

        wcl_retry(NODES[0], f"RETIRE STORAGE NODE {DRAINED};")

        status, appended = post(
            NODES[0],
            "/v1/feeds/append",
            {
                "request_id": str(uuid.uuid4()),
                "feed": feeds[0],
                "writer_session_id": str(uuid.uuid4()),
                "writer_epoch": 1,
                "sequence": 1,
                "event_time_ns": "2",
                "key_base64": "b3JkZXItMg==",
                "payload_base64": "ZXZlbnQtMg==",
            },
        )
        if status != 200:
            raise AssertionError(appended)
        records = read(NODES[0], feeds[0])
        if len(records) != 2:
            raise AssertionError(records)

        print(
            json.dumps(
                {
                    "status": "ok",
                    "mtls": "internal plane served only over pinned certificates",
                    "drained": DRAINED,
                    "plan": plan,
                },
                indent=2,
            )
        )
    finally:
        subprocess.run([*COMPOSE, "down", "-v"], check=True)


if __name__ == "__main__":
    main()
