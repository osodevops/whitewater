import http.client
import json
import pathlib
import re
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

COMPOSE = [
    "docker",
    "compose",
    "-f",
    "compose.storage-join.yml",
    "-p",
    "whitewater-join",
]
VOTERS = [f"http://localhost:{port}" for port in range(7171, 7174)]
STORAGE = "http://localhost:7175"
KEY = "whitewater-local-development-admin-key"
CERTS_DIR = ".mtls-join-certs"
STORAGE_NODE = "storage-1"
NODES = [f"control-{index}" for index in range(1, 4)] + [STORAGE_NODE]


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


def append(node, feed, sequence):
    return post(
        node,
        "/v1/feeds/append",
        {
            "request_id": str(uuid.uuid4()),
            "feed": feed,
            "writer_session_id": str(uuid.uuid4()),
            "writer_epoch": 1,
            "sequence": sequence,
            "event_time_ns": str(sequence),
            "key_base64": "b3JkZXItMQ==",
            "payload_base64": "ZXZlbnQtMQ==",
        },
    )


def wait_for_fabric():
    endpoints = VOTERS + [STORAGE]
    for _ in range(120):
        try:
            if all(
                urllib.request.urlopen(f"{node}/health", timeout=2).status == 200
                for node in endpoints
            ):
                return
        except (urllib.error.URLError, TimeoutError, ConnectionError, http.client.RemoteDisconnected):
            pass
        time.sleep(1)
    raise AssertionError("storage-join Riverbed did not become healthy")


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
            "WW_CERTS_DIR=/workspace/.mtls-join-certs",
            "-e",
            "WW_CERTS_NODES=" + ",".join(NODES),
            "-e",
            "WW_CERTS_PORT=7271",
            # Cargo runs as the container's root user; keep its build cache in
            # a Docker-managed volume so it never leaves a root-owned target/
            # in the host workspace (which breaks the runner's own cargo).
            "-e",
            "CARGO_TARGET_DIR=/tmp/ww-certgen-target",
            "-v",
            "whitewater-certgen-target:/tmp/ww-certgen-target",
            "rust:1.90-bookworm",
            "bash",
            "-c",
            "cargo test --release --test dev_cert_gen && chmod -R a+rw /workspace/.mtls-join-certs",
        ],
        check=True,
    )


def env_path(node):
    return pathlib.Path(CERTS_DIR).joinpath(f"{node}.env")


def storage_pin():
    # The generated env lists every peer pin as `<node>@<blake3-hex>`; the
    # storage Node's own file retains all of them.
    text = env_path(STORAGE_NODE).read_text()
    line = next(
        line for line in text.splitlines() if line.startswith(
            "WHITEWATER_SUBSCRIPTION_MTLS_PEER_PINS="
        )
    )
    entry = next(
        entry
        for entry in line.split("=", 1)[1].split(",")
        if entry.startswith(f"{STORAGE_NODE}@")
    )
    return entry.split("@", 1)[1]


def strip_voter_pins():
    # Remove the storage Node's pin from every voter's static peer set so the
    # voters can only trust its certificate after the catalog registration
    # distributes the pin — the mechanism this acceptance exists to prove.
    for voter in NODES:
        if voter == STORAGE_NODE:
            continue
        path = env_path(voter)
        lines = path.read_text().splitlines(keepends=True)
        rewritten = []
        for line in lines:
            if line.startswith("WHITEWATER_SUBSCRIPTION_MTLS_PEER_PINS="):
                entries = [
                    entry
                    for entry in line.strip().split("=", 1)[1].split(",")
                    if entry and not entry.startswith(f"{STORAGE_NODE}@")
                ]
                line = (
                    "WHITEWATER_SUBSCRIPTION_MTLS_PEER_PINS="
                    + ",".join(entries)
                    + "\n"
                )
            rewritten.append(line)
        path.write_text("".join(rewritten))


def storage_serves_read(feed, expected):
    # Reads through the storage Node's public API resolve the Feed through its
    # synced catalog and pull pages over the authenticated internal plane.
    deadline = time.time() + 60
    last = None
    while time.time() < deadline:
        try:
            records = read(STORAGE, feed)
            if len(records) == expected:
                return records
            last = {"feed": feed, "records": len(records), "expected": expected}
        except (urllib.error.URLError, urllib.error.HTTPError) as error:
            last = repr(error)
        time.sleep(2)
    raise AssertionError(f"storage Node did not serve a synced read: {last}")


def main():
    certs = pathlib.Path(CERTS_DIR)
    if not certs.joinpath("ca.pem").exists() or any(
        not certs.joinpath(f"{node}.pem").exists() for node in NODES
    ):
        generate_certificates()
    strip_voter_pins()
    subprocess.run([*COMPOSE, "down", "-v"], check=True)
    subprocess.run([*COMPOSE, "up", "-d", "--build"], check=True)
    try:
        wait_for_fabric()

        # Register the storage-only Node with its certificate pin. The voters
        # do not carry its pin in static configuration, so every subsequent
        # internal call from storage-1 proves catalog-distributed pin auth.
        pin = storage_pin()
        wcl_retry(
            VOTERS[0],
            f"REGISTER STORAGE NODE {STORAGE_NODE} "
            f"AT https://{STORAGE_NODE}:7271 WITH CERT PIN {pin};",
        )
        shown = wcl_retry(VOTERS[0], "SHOW STORAGE NODES;")[0]
        rows = shown["nodes"] if isinstance(shown, dict) else shown
        registered = [row for row in rows if row.get("node") == STORAGE_NODE]
        if not registered or not registered[0].get("eligible", True):
            raise AssertionError({"storage_nodes": shown})

        # Capacity evidence: the catalog accepts an explicit report for
        # the registered Node and SHOW exposes the committed measurement.
        wcl_retry(
            VOTERS[0],
            f"REPORT STORAGE NODE {STORAGE_NODE} CAPACITY cpu_cores=4, "
            f"memory_bytes=8589934592, disk_total_bytes=107374182400, "
            f"disk_free_bytes=53687091200, network_mbps=10000, "
            f"failure_domain='join-a';",
        )
        shown = wcl_retry(VOTERS[0], "SHOW STORAGE NODES;")[0]
        rows = shown["nodes"] if isinstance(shown, dict) else shown
        reported = [row for row in rows if row.get("node") == STORAGE_NODE]
        # storage-1's own CapacityReporter may legitimately overwrite the
        # explicit report on its next tick, so assert the committed
        # measurement exists rather than pinning its values.
        if not reported or not isinstance(reported[0].get("capacity"), dict):
            raise AssertionError({"capacity_report": shown})

        # Voters run the CapacityReporter on an interval: their committed
        # self-reports appear without any operator command.
        deadline = time.time() + 60
        while time.time() < deadline:
            shown = wcl_retry(VOTERS[0], "SHOW STORAGE NODES;")[0]
            rows = shown["nodes"] if isinstance(shown, dict) else shown
            voter_rows = [
                row for row in rows if row.get("node", "").startswith("control-")
            ]
            if voter_rows and all(
                isinstance(row.get("capacity"), dict)
                and row["capacity"].get("cpu_cores", 0) > 0
                for row in voter_rows
            ):
                break
            time.sleep(3)
        else:
            raise AssertionError({"self_reported_capacity": shown})

        suffix = f"sj{uuid.uuid4().hex[:12]}"
        feeds = [f"{suffix}.events{index}" for index in range(8)]
        script = "; ".join(
            [f"CREATE SPACE {suffix}"]
            + [f"CREATE FEED {feed}" for feed in feeds]
        ) + ";"
        for _ in range(30):
            status, _ = post(VOTERS[0], "/v1/admin/wcl", {"script": script})
            if status == 200:
                break
            time.sleep(1)
        if status != 200:
            raise AssertionError("storage-join Riverbed setup failed")

        held = {feed: placement(VOTERS[0], feed)["replicas"] for feed in feeds}
        hosted = [
            feed for feed, replicas in held.items() if STORAGE_NODE in replicas
        ]
        if not hosted:
            raise AssertionError(f"{STORAGE_NODE} holds no Feed replicas: {held}")

        # Appends to a storage-1-held Feed commit only once its replica accepts
        # frames over mTLS — catalog-distributed pin + synced catalog fencing.
        feed = hosted[0]
        last = None
        deadline = time.time() + 90
        while time.time() < deadline:
            status, body = append(VOTERS[1], feed, 1)
            if status == 200:
                break
            last = body
            time.sleep(2)
        else:
            raise AssertionError(f"append to {STORAGE_NODE}-held Feed failed: {last}")

        storage_serves_read(feed, 1)

        # Restart proves replica bytes are durable on the non-voter and that a
        # re-joining Node re-syncs the catalog without voter reconfiguration.
        subprocess.run([*COMPOSE, "restart", STORAGE_NODE], check=True)
        wait_for_fabric()
        storage_serves_read(feed, 1)

        status, body = append(VOTERS[0], feed, 2)
        if status != 200:
            raise AssertionError(body)
        records = storage_serves_read(feed, 2)

        print(
            json.dumps(
                {
                    "status": "ok",
                    "registered": STORAGE_NODE,
                    "hosted_feeds": len(hosted),
                    "replicas": held[feed],
                    "records_after_restart": len(records),
                    "note": (
                        "voters trust the storage Node certificate only "
                        "through its catalog-registered pin"
                    ),
                },
                indent=2,
            )
        )
    finally:
        subprocess.run([*COMPOSE, "down", "-v"], check=True)


if __name__ == "__main__":
    main()
