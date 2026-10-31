"""Sustained mixed-workload soak benchmark for a disposable Riverbed.

The per-mode benches (bench-feed.py / bench-kafka.py) measure write or
read in isolation. Production load is simultaneous: replication traffic,
segment writes, and read serving contend for the same disk and NIC on
every Node, and degradations only appear over minutes. This driver runs
writers AND tail-following readers concurrently for a fixed duration and
reports the throughput *curve*, not just the average — a system that
holds 25k rec/s for 60s and collapses at minute 3 fails this bench.

  * writers append batches continuously until BENCH_SOAK_SECONDS elapse
  * readers chase the tail through binary temporary fetches, polling like
    a real consumer (wait_ms) instead of draining a fixed history once
  * a monitor snapshots both counters every BENCH_SOAK_INTERVAL and
    emits per-interval rates; the summary carries min/last ratios so a
    degrading tail is visible without plotting

Results print as `BENCH_RESULT {json}` lines sharing the contract with
bench-feed.py / bench-kafka.py: one `soak-write`, one `soak-read`, and
one `soak` summary row carrying the interval series.

    python scripts/bench-soak.py
    BENCH_SOAK_SECONDS=300 BENCH_WRITERS=8 BENCH_READERS=4 python scripts/bench-soak.py

Environment knobs: BENCH_SOAK_SECONDS, BENCH_SOAK_INTERVAL, BENCH_BYTES,
BENCH_WRITERS, BENCH_BATCH, BENCH_READERS, BENCH_FEEDS, BENCH_BASE_PORT,
WW_KEEP_UP. This never runs under run-acceptance.py (bench-* prefix).
"""

import base64
import http.client
import json
import os
import statistics
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid

COMPOSE = "compose.verify.yml"
PROJECT = "whitewater-bench"
BASE_PORT = int(os.environ.get("BENCH_BASE_PORT", "7771"))
SUBNET = os.environ.get("BENCH_SUBNET", "10.250.245.0/24")
ENDPOINTS = [f"http://localhost:{BASE_PORT + offset}" for offset in range(3)]
KEY = os.environ.get("WHITEWATER_ADMIN_API_KEY", "whitewater-local-development-admin-key")

SOAK_SECONDS = float(os.environ.get("BENCH_SOAK_SECONDS", "120"))
INTERVAL = float(os.environ.get("BENCH_SOAK_INTERVAL", "5"))
BYTES = int(os.environ.get("BENCH_BYTES", "1024"))
WRITERS = int(os.environ.get("BENCH_WRITERS", "8"))
BATCH = int(os.environ.get("BENCH_BATCH", "256"))
READERS = int(os.environ.get("BENCH_READERS", "4"))
FEEDS = int(os.environ.get("BENCH_FEEDS", "4"))
PAGE_LIMIT = int(os.environ.get("BENCH_PAGE_LIMIT", "4096"))
READ_WAIT_MS = int(os.environ.get("BENCH_READ_WAIT_MS", "100"))
BATCH_ATTEMPTS = int(os.environ.get("BENCH_BATCH_ATTEMPTS", "8"))

FRAMES_MEDIA = "application/vnd.whitewater.frames"


def compose(*args):
    env = dict(os.environ)
    env["COMPOSE_PROJECT_NAME"] = PROJECT
    env["WHITEWATER_VERIFY_PORT_1"] = str(BASE_PORT)
    env["WHITEWATER_VERIFY_PORT_2"] = str(BASE_PORT + 1)
    env["WHITEWATER_VERIFY_PORT_3"] = str(BASE_PORT + 2)
    env["WHITEWATER_VERIFY_SUBNET"] = SUBNET
    result = subprocess.run(
        ["docker", "compose", "-f", COMPOSE, *args],
        capture_output=True,
        text=True,
        env=env,
    )
    return result.returncode, result.stdout + result.stderr


def post(endpoint, path, body, timeout=60):
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


def get(endpoint, path, timeout=15):
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
    for _ in range(30):
        status, body = post(ENDPOINTS[0], "/v1/admin/wcl", {"script": script})
        if status == 200:
            return body
        if status == 400:
            raise AssertionError(f"WCL rejected: {body}")
        time.sleep(1)
    raise AssertionError(f"WCL never succeeded: {status} {body}")


def wait_ready():
    for endpoint in ENDPOINTS:
        for _ in range(300):
            try:
                with urllib.request.urlopen(
                    urllib.request.Request(
                        f"{endpoint}/health",
                        headers={"authorization": f"Bearer {KEY}"},
                    ),
                    timeout=3,
                ) as response:
                    if response.status == 200:
                        break
            except Exception:
                pass
            time.sleep(1)
        else:
            raise AssertionError(f"{endpoint} never became healthy")
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        statuses = [get(endpoint, "/v1/admin/control-plane")[1] for endpoint in ENDPOINTS]
        if all(
            status.get("leader_id") is not None
            and status.get("last_applied_index") is not None
            and status["last_applied_index"] >= (status.get("last_log_index") or 0)
            for status in statuses
        ):
            return
        time.sleep(1)
    raise AssertionError(f"Control Plane did not converge: {statuses}")


def percentile_stats(samples):
    if not samples:
        return {}
    ordered = sorted(samples)

    def pct(quantile):
        index = min(len(ordered) - 1, max(0, round(quantile * (len(ordered) - 1))))
        return ordered[index]

    return {
        "count": len(ordered),
        "min_ms": round(ordered[0] * 1000, 3),
        "p50_ms": round(pct(0.50) * 1000, 3),
        "p95_ms": round(pct(0.95) * 1000, 3),
        "p99_ms": round(pct(0.99) * 1000, 3),
        "max_ms": round(ordered[-1] * 1000, 3),
        "mean_ms": round(statistics.fmean(ordered) * 1000, 3),
    }


def bench_result(system, mode, records, payload_bytes, seconds, latencies, extra=None):
    result = {
        "system": system,
        "mode": mode,
        "records": records,
        "bytes_per_record": payload_bytes,
        "seconds": round(seconds, 3),
        "records_per_sec": round(records / seconds, 1) if seconds > 0 else 0,
        "mib_per_sec": round(records * payload_bytes / seconds / 1024 / 1024, 2)
        if seconds > 0
        else 0,
    }
    result.update(percentile_stats(latencies))
    if extra:
        result.update(extra)
    return result


def report(result):
    print(f"BENCH_RESULT {json.dumps(result)}")
    print(
        f"  {result['system']} {result['mode']}: "
        f"{result['records_per_sec']:,.0f} records/s, "
        f"{result['mib_per_sec']:,.2f} MiB/s"
    )


def b64(raw):
    return base64.b64encode(raw).decode()


class Session:
    """Keep-alive POST channel — one persistent connection per worker,
    matching an SDK client's socket reuse."""

    def __init__(self, endpoint, binary=False):
        self.hostport = endpoint.split("://", 1)[1]
        self.binary = binary
        self.conn = http.client.HTTPConnection(self.hostport, timeout=60)

    def post(self, path, body):
        payload = json.dumps(body)
        last_error = None
        for _ in range(2):
            try:
                request_headers = {
                    "authorization": f"Bearer {KEY}",
                    "content-type": "application/json",
                }
                if self.binary:
                    request_headers["accept"] = FRAMES_MEDIA
                self.conn.request("POST", path, payload, request_headers)
                response = self.conn.getresponse()
                data = response.read()
                if self.binary and response.status == 200:
                    return response.status, data
                return response.status, json.loads(data or b"{}")
            except (http.client.HTTPException, OSError) as error:
                self.conn.close()
                self.conn = http.client.HTTPConnection(self.hostport, timeout=60)
                last_error = error
        return 0, {"error": str(last_error)}


class Counters:
    """Monotonic counters shared by all workers; the monitor reads deltas."""

    def __init__(self):
        self.lock = threading.Lock()
        self.written = 0
        self.read = 0
        self.read_bytes = 0
        self.errors = 0
        self.latencies = []

    def add_written(self, count, seconds):
        with self.lock:
            self.written += count
            self.latencies.append(seconds)

    def add_errors(self, count=1):
        with self.lock:
            self.errors += count

    def add_read(self, count, payload_bytes):
        with self.lock:
            self.read += count
            self.read_bytes += payload_bytes

    def snapshot(self):
        with self.lock:
            return self.written, self.read, self.errors


class SoakWriter:
    """Appends batches continuously until the deadline. Each worker owns a
    disjoint keyspace; retries after ambiguous failures dedupe server-side
    through stable request_ids (same contract as bench-feed)."""

    def __init__(self, writers, deadline, counters):
        self.writers = writers
        self.deadline = deadline
        self.counters = counters
        self.payload = b64(bytes(((index % 26) + 97) for index in range(BYTES)))

    def _run(self, worker):
        writer, epoch, feed = self.writers[worker]
        session = Session(ENDPOINTS[worker % len(ENDPOINTS)])
        index = 0
        while time.monotonic() < self.deadline:
            records = [
                {
                    "request_id": str(uuid.uuid5(
                        uuid.NAMESPACE_URL, f"soak/{writer}/{index + i}"
                    )),
                    "writer": writer,
                    "session_epoch": epoch,
                    "event_time_ns": str(1_700_000_000_000_000_000 + index + i),
                    "key_base64": b64(f"w{worker}-{index + i:012d}".encode()),
                    "payload_base64": self.payload,
                    "metadata_base64": {"source": b64(b"bench-soak")},
                }
                for i in range(BATCH)
            ]
            index += BATCH
            for _ in range(BATCH_ATTEMPTS):
                started = time.monotonic()
                status, body = session.post(
                    "/v1/writers/append-batch", {"records": records}
                )
                elapsed = time.monotonic() - started
                if status == 200:
                    self.counters.add_written(BATCH, elapsed)
                    break
                time.sleep(0.5)
            else:
                self.counters.add_errors(len(records))
                if body:
                    print(f"  append refused mid-soak: {body}", file=sys.stderr)


def parse_frame_fetch(data):
    """Binary fetch page: u32 count, u32 next_cursor_len + cursor, then per
    record u32 cursor_len + cursor + u32 frame_len + stored frame bytes.
    V2 body lays payload_len as a big-endian u32 at body offset 64."""
    pos = 0
    count = int.from_bytes(data[pos:pos + 4], "little"); pos += 4
    nc_len = int.from_bytes(data[pos:pos + 4], "little"); pos += 4
    next_cursor = data[pos:pos + nc_len].decode(); pos += nc_len
    payload_bytes = 0
    for _ in range(count):
        c_len = int.from_bytes(data[pos:pos + 4], "little"); pos += 4 + c_len
        f_len = int.from_bytes(data[pos:pos + 4], "little"); pos += 4
        payload_bytes += int.from_bytes(data[pos + 68:pos + 72], "big")
        pos += f_len
    return count, payload_bytes, next_cursor


def soak_reader(feed, index, deadline, counters):
    """Tail-following consumer: fetch pages until the deadline, long-polling
    at the tail exactly like a real attached consumer (wait_ms) — never a
    one-shot drain of fixed history."""
    session = Session(ENDPOINTS[index % len(ENDPOINTS)], binary=True)
    cursor = ""
    stall_deadline = deadline + 30  # drain grace after writers stop
    while True:
        status, page = session.post(
            "/v1/readers/temporary/fetch",
            {
                "feed": feed,
                "after": cursor or None,
                "limit": PAGE_LIMIT,
                "wait_ms": READ_WAIT_MS,
            },
        )
        if status != 200:
            counters.add_errors()
            time.sleep(0.5)
            continue
        count, payload_bytes, next_cursor = parse_frame_fetch(page)
        if count:
            counters.add_read(count, payload_bytes)
            if next_cursor and next_cursor != cursor:
                cursor = next_cursor
            continue
        # Empty page: at the tail. Stop once writers are done and the
        # drain grace has passed — anything read until then is still
        # counting sustained consumer work.
        if time.monotonic() > stall_deadline:
            return


def main():
    code, out = compose("up", "-d", "--build")
    assert code == 0, out
    try:
        wait_ready()
        suffix = f"{int(time.time())}{uuid.uuid4().hex[:4]}"
        domain = f"soak{suffix}"
        feeds = [f"{domain}.events{i}" for i in range(FEEDS)]
        statements = [f"CREATE DOMAIN {domain}"]
        for feed in feeds:
            statements.append(f"CREATE FEED {feed}")
        writer_names = [f"soakw{suffix}w{i}" for i in range(WRITERS)]
        for i, name in enumerate(writer_names):
            statements.append(f"CREATE WRITER {name} TO {feeds[i % len(feeds)]}")
            statements.append(f"OPEN WRITER SESSION {name}")
        setup = wcl("; ".join(statements) + ";")
        first_writer_result = 1 + len(feeds)
        writers = [
            (
                name,
                setup["results"][first_writer_result + 2 * i + 1]["data"]["session_epoch"],
                feeds[i % len(feeds)],
            )
            for i, name in enumerate(writer_names)
        ]

        counters = Counters()
        deadline = time.monotonic() + SOAK_SECONDS
        stop_monitor = threading.Event()
        series = []

        def monitor():
            prev = (time.monotonic(), 0, 0)
            while not stop_monitor.wait(INTERVAL):
                now = time.monotonic()
                written, read, errors = counters.snapshot()
                elapsed = now - prev[0]
                series.append({
                    "t_s": round(now - (deadline - SOAK_SECONDS), 1),
                    "write_rps": round((written - prev[1]) / elapsed, 1),
                    "read_rps": round((read - prev[2]) / elapsed, 1),
                    "written_total": written,
                    "read_total": read,
                    "errors": errors,
                })
                prev = (now, written, read)

        threads = [threading.Thread(target=monitor, daemon=True)]
        for worker in range(len(writers)):
            threads.append(threading.Thread(
                target=SoakWriter(writers, deadline, counters)._run,
                args=(worker,), daemon=True))
        for reader_index in range(READERS):
            feed = feeds[reader_index % len(feeds)]
            threads.append(threading.Thread(
                target=soak_reader,
                args=(feed, reader_index, deadline, counters), daemon=True))

        started = time.monotonic()
        for thread in threads:
            thread.start()
        # Readers keep draining for the 30s grace after the write deadline;
        # the run ends when every worker exits.
        for thread in threads[1:]:
            thread.join()
        stop_monitor.set()
        seconds = time.monotonic() - started
        written, read, errors = counters.snapshot()
        read_bytes = counters.read_bytes

        report(bench_result(
            "whitewater", "soak-write", written, BYTES, seconds,
            counters.latencies,
            {"writers": WRITERS, "batch": BATCH, "errors": errors},
        ))
        report(bench_result(
            "whitewater", "soak-read", read,
            read_bytes // read if read else BYTES, seconds, [],
            {"readers": READERS, "feeds": FEEDS},
        ))
        write_rates = [point["write_rps"] for point in series if point["write_rps"] > 0]
        read_rates = [point["read_rps"] for point in series if point["read_rps"] > 0]
        report(bench_result(
            "whitewater", "soak", read, BYTES, seconds, [],
            {
                "soak_seconds": SOAK_SECONDS,
                "interval_s": INTERVAL,
                "write_rps_min": min(write_rates) if write_rates else 0,
                "write_rps_last": write_rates[-1] if write_rates else 0,
                "read_rps_min": min(read_rates) if read_rates else 0,
                "read_rps_last": read_rates[-1] if read_rates else 0,
                "curve": series,
            },
        ))
        assert errors == 0, f"{errors} records/reads errored during soak"
        assert read > 0, "readers never observed a record"
        print("BENCH PASSED")
    finally:
        if not os.environ.get("WW_KEEP_UP"):
            compose("down", "-v")


if __name__ == "__main__":
    main()
