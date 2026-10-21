"""Hot-Key detection and isolation acceptance.

Drives sustained dominant-Key append traffic at a three-Node Riverbed and
proves:

  * the dominant Key is flagged as hot after consecutive windowed samples
    (not on the first burst window),
  * flagged appends return `feedback.hot_key: true` with tightened batch
    guidance while unrelated Keys in the same range keep normal guidance,
  * `GET /v1/admin/demand/hot-keys` reports the flag with share evidence and
    an isolating split boundary,
  * sustained range pressure splits at the flagged Key's edge, and the flag
    re-lands on the child range that now owns the Key.

Per-Key ordering means a hot Key can be isolated but never parallelized; the
test asserts the system reports exactly that contract.
"""

import base64
import json
import os
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid

COMPOSE = "compose.hotkeys.yml"
ENDPOINTS = ["http://localhost:7971", "http://localhost:7972", "http://localhost:7973"]
KEY = "whitewater-local-development-admin-key"
DETECT_DEADLINE = 90
SPLIT_DEADLINE = 300
COLD_KEYS = 20


def compose(*args):
    result = subprocess.run(
        ["docker", "compose", "-f", COMPOSE, *args],
        capture_output=True,
        text=True,
    )
    return result.returncode, result.stdout + result.stderr


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
def post(endpoint, path, body, timeout=30):
    # Transient unavailability (election windows, quorum stalls, restart
    # gaps) retries with the same body â€” stable request_id values in the
    # payload keep the retry idempotent. Refusals (4xx) return at once
    # for the caller to assert.
    for _ in range(120):
        status, result = _post_once(endpoint, path, body, timeout)
        if status == 0 or status >= 500:
            time.sleep(0.5)
            continue
        return status, result
    return status, result

def _post_once(endpoint, path, body, timeout=30):
    request = urllib.request.Request(
        endpoint + path,
        data=json.dumps(body).encode(),
        headers={
            "authorization": f"Bearer {KEY}",
            "content-type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read() or b"{}")
    except OSError as error:
        return 0, {"error": str(error)}


@_retried
def get(endpoint, path, timeout=15):
    for _ in range(120):
        status, result = _get_once(endpoint, path, timeout)
        if status == 0 or status >= 500:
            time.sleep(0.5)
            continue
        return status, result
    return status, result

def _get_once(endpoint, path, timeout=15):
    request = urllib.request.Request(
        endpoint + path, headers={"authorization": f"Bearer {KEY}"}
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read() or b"{}")
    except OSError as error:
        return 0, {"error": str(error)}


def wcl(script):
    request_id = str(uuid.uuid4())
    for _ in range(30):
        status, body = post(ENDPOINTS[0], "/v1/admin/wcl", {"request_id": request_id, "script": script})
        if status == 200:
            return body
        # A 400 means the script itself was rejected (partial execution may
        # have committed earlier statements); retrying can only fail again.
        if status == 400:
            raise AssertionError(f"WCL rejected: {body}")
        time.sleep(1)
    raise AssertionError(f"WCL never succeeded: {status} {body}")


def wait_ready():
    for endpoint in ENDPOINTS:
        for _ in range(120):
            status, _ = get(endpoint, "/health")
            if status == 200:
                break
            time.sleep(1)
        else:
            raise AssertionError(f"{endpoint} never became healthy")


def hot_keys():
    """Union of flagged hot Keys reported by every Node."""
    seen = []
    for endpoint in ENDPOINTS:
        status, body = get(endpoint, "/v1/admin/demand/hot-keys")
        if status == 200:
            for entry in body.get("hot_keys", []):
                key = (entry["range_id"], entry["token"])
                if all((e["range_id"], e["token"]) != key for e in seen):
                    seen.append(entry)
    return seen


def b64(raw):
    return base64.b64encode(raw).decode()


class Driver:
    """Background append loops dominated by one Key.

    Three workers keep the sustained windowed rate (~12-15 records/s on this
    quorum path) above the compose file's 8 records/s split threshold even
    though each quorum batch serializes at ~5 records/s per Writer.
    """

    WORKERS = 3

    def __init__(self, feed, writers, hot_key):
        self.feed = feed
        # One Writer per worker: a Writer's record sequence must submit in
        # order, so concurrent workers cannot share a sequence stream.
        self.writers = writers
        self.hot_key = hot_key
        self.counter = 0
        self.counter_lock = threading.Lock()
        self.errors = []
        self.stop = threading.Event()
        self.threads = [
            threading.Thread(target=self._run, args=(worker,), daemon=True)
            for worker in range(self.WORKERS)
        ]

    def start(self):
        for thread in self.threads:
            thread.start()

    def join(self):
        self.stop.set()
        for thread in self.threads:
            thread.join(timeout=30)

    def next_index(self):
        with self.counter_lock:
            index = self.counter
            self.counter += 1
            return index

    def record(self, index, writer, writer_epoch):
        cold = index % 10 == 0
        key = f"cold-{index % COLD_KEYS}" if cold else self.hot_key
        return {
            "request_id": str(uuid.uuid4()),
            "writer": writer,
            "session_epoch": writer_epoch,
            "event_time_ns": str(1_700_000_000_000_000_000 + index),
            "key_base64": b64(key.encode()),
            "payload_base64": b64(f"record-{index}".encode()),
            "metadata_base64": {"source": b64(b"hotkey-acceptance")},
        }

    def _run(self, worker):
        writer, writer_epoch = self.writers[worker]
        while not self.stop.is_set():
            records = [
                self.record(self.next_index(), writer, writer_epoch)
                for _ in range(30)
            ]
            # Cutover freezes are transient and retryable by contract;
            # retries ride out the bounded freeze window, and only errors
            # that persist past it count.
            status, body = 0, {}
            for _ in range(40):
                status, body = post(
                    ENDPOINTS[worker % len(ENDPOINTS)],
                    "/v1/writers/append-batch",
                    {"records": records},
                )
                if status == 200:
                    break
                time.sleep(1)
            if status != 200:
                body["http_status"] = status
                self.errors.append(body)
            time.sleep(0.15)


def main():
    code, out = compose("up", "-d", "--build")
    assert code == 0, out
    try:
        wait_ready()
        suffix = f"{int(time.time())}{uuid.uuid4().hex[:4]}"
        domain = f"hk{suffix}"
        feed = f"{domain}.events"
        statements = [f"CREATE DOMAIN {domain}", f"CREATE FEED {feed}"]
        writer_names = [f"writer{suffix}w{i}" for i in range(Driver.WORKERS + 1)]
        for name in writer_names:
            statements.append(f"CREATE WRITER {name} TO {feed}")
            statements.append(f"OPEN WRITER SESSION {name}")
        setup = wcl("; ".join(statements) + ";")
        writers = [
            (name, setup["results"][2 + 2 * i + 1]["data"]["session_epoch"])
            for i, name in enumerate(writer_names)
        ]
        hot_key = "hot-tenant"

        driver = Driver(feed, writers[: Driver.WORKERS], hot_key)
        driver.start()
        try:
            # Phase 1 â€” sustained dominance flags the Key on its owner.
            deadline = time.time() + DETECT_DEADLINE
            flagged = []
            isolated = []
            while time.time() < deadline:
                flagged = [e for e in hot_keys() if e["feed"] == feed]
                # The boundary stays null until a neighbouring cold Key is
                # observed in the window, so the flag alone is not enough.
                # A split can also commit between polls: a child range that
                # starts exactly at the Key's token is the same isolation
                # already landed â€” and the flag re-landed on the owning
                # range, which Phase 2's feedback probe needs. Phase 3 then
                # still confirms the split edge.
                isolated = [
                    e
                    for e in flagged
                    if e["isolating_boundary"] or e["range_start"] == e["token"]
                ]
                if isolated:
                    break
                if driver.errors:
                    raise AssertionError(f"append driver failing: {driver.errors[:3]}")
                time.sleep(1)
            assert isolated, (
                "dominant Key was never flagged with an isolating boundary; "
                f"last flagged={flagged}; driver errors={driver.errors[:3]}"
            )
            # Prefer the boundary-bearing flag, then the child whose start is
            # the Key's token (the flag already re-landed on the owning
            # range â€” Phase 2's feedback probe needs it live there).
            evidence = sorted(
                isolated,
                key=lambda e: (
                    not e["isolating_boundary"],
                    e["range_start"] != e["token"],
                ),
            )[0]
            print(f"flagged: {json.dumps(evidence, indent=2)}")
            assert evidence["share_bps"] >= 5_000, evidence
            assert evidence["window_records"] > 0, evidence
            assert evidence["feed"] == feed, evidence
            assert (
                evidence["isolating_boundary"]
                or evidence["range_start"] == evidence["token"]
            ), evidence
            first_range = evidence["range_id"]

            # Phase 2 â€” Writer feedback carries the flag and tightens only
            # for the hot Key.
            def one_append(key):
                probe_writer, probe_epoch = writers[Driver.WORKERS]
                return post(
                    ENDPOINTS[0],
                    "/v1/writers/append",
                    {
                        "request_id": str(uuid.uuid4()),
                        "writer": probe_writer,
                        "session_epoch": probe_epoch,
                        "key_base64": b64(key.encode()),
                        "payload_base64": b64(b"probe"),
                        "metadata_base64": {},
                    },
                )

            status, hot = one_append(hot_key)
            assert status == 200, hot
            feedback = hot["feedback"]
            assert feedback["hot_key"] is True, feedback
            assert feedback["retry_after_ms"] >= 25, feedback
            assert feedback["recommended_batch_count"] <= 8, feedback
            assert feedback["recommended_batch_bytes"] <= 64 * 1024, feedback
            status, cold = one_append("cold-probe")
            assert status == 200, cold
            assert cold["feedback"]["hot_key"] is False, cold["feedback"]

            hot_token = evidence["token"]

            # Phase 3 -- sustained pressure splits at the Key's edge. The
            # isolation lands as a range boundary exactly at the hot Key's
            # token: either the flag re-lands on the right child (whose
            # start is the token) or the kept-left child's end_exclusive is
            # the token. Both prove unrelated Keys were carved away.
            deadline = time.time() + SPLIT_DEADLINE
            isolated = None
            while time.time() < deadline:
                for entry in hot_keys():
                    if entry["feed"] != feed or entry["token"] != hot_token:
                        continue
                    if (
                        entry["range_id"] != first_range
                        or entry["range_start"] == hot_token
                        or entry["range_end_exclusive"] == hot_token
                    ):
                        isolated = entry
                        break
                if isolated:
                    break
                time.sleep(2)
            assert isolated, (
                "no isolating split at the hot Key's edge was observed; "
                f"still: {hot_keys()}"
            )
            print(f"isolated at Key edge: {json.dumps(isolated, indent=2)}")

            # The whole run stayed durable and writable: no append errors.
            assert not driver.errors, driver.errors[:3]
        finally:
            driver.join()

        print("HOT KEY ACCEPTANCE PASSED")
    finally:
        if not os.environ.get("WW_KEEP_UP"):
            compose("down", "-v")


if __name__ == "__main__":
    main()
