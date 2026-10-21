import base64, json, os, sys, threading, time, urllib.error, urllib.request, uuid

BASE_PORT = int(os.environ.get("WHITEWATER_TEST_BASE_PORT", "7071"))
ENDPOINTS = [f"http://localhost:{BASE_PORT + offset}" for offset in range(3)]
KEY = "whitewater-local-development-admin-key"
WRITER_COUNT = 4
RECORDS_PER_WRITER = 25

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
    request = urllib.request.Request(endpoint + path, data=json.dumps(body).encode(), headers={"authorization": f"Bearer {KEY}", "content-type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read() or b"{}")
    except OSError:
        return 0, {}
    except (urllib.error.URLError, TimeoutError, ConnectionError, OSError):
        return 0, {}


def committed_post(endpoint, path, body):
    # Transient quorum/commit stalls are retryable: the stable request_id in
    # each body keeps a retried append idempotent, and refusal statuses return
    # at once so assertion checks still observe real rejections.
    for _ in range(120):
        status, result = post(endpoint, path, body)
        if status == 0 or status >= 500:
            time.sleep(0.5)
            continue
        return status, result
    return status, result


@_retried
def get(endpoint, path):
    request = urllib.request.Request(endpoint + path, headers={"authorization": f"Bearer {KEY}"})
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read() or b"{}")
    except OSError:
        return 0, {}
def b64(raw):
    return base64.b64encode(raw).decode()


def append_body(writer, epoch, key, payload, event_time_ns):
    return {
        "request_id": str(uuid.uuid4()),
        "writer": writer,
        "session_epoch": epoch,
        "event_time_ns": str(event_time_ns),
        "key_base64": b64(key),
        "payload_base64": b64(payload),
        "metadata_base64": {},
    }


def main():
    suffix = f"{int(time.time())}{uuid.uuid4().hex[:6]}"
    domain, feed = f"conc{suffix}", f"conc{suffix}.events"
    writers = [f"conw{index}{suffix}" for index in range(WRITER_COUNT)]
    script = f"CREATE DOMAIN {domain}; CREATE FEED {feed};" + "".join(
        f" CREATE WRITER {writer} TO {feed}; OPEN WRITER SESSION {writer};" for writer in writers
    )
    status, setup = committed_post(ENDPOINTS[0], "/v1/admin/wcl", {"script": script, "request_id": str(uuid.uuid4())})
    if status != 200:
        raise AssertionError(setup)
    epochs = [
        setup["results"][2 + index * 2 + 1]["data"]["session_epoch"] for index in range(WRITER_COUNT)
    ]

    # Phase 1: WRITER_COUNT sessions append concurrently across all three
    # ingresses. Each Writer sends sequentially so its Key's committed order
    # must equal submission order; Writers race each other.
    failures = []
    results = [[] for _ in range(WRITER_COUNT)]

    def writer_worker(index):
        writer, epoch = writers[index], epochs[index]
        for seq in range(RECORDS_PER_WRITER):
            body = append_body(writer, epoch, f"key-{index}".encode(), f"w{index}-{seq:04d}".encode(), seq)
            status, result = committed_post(ENDPOINTS[(index + seq) % 3], "/v1/writers/append", body)
            # A retried append can legitimately answer deduplicated: the
            # earlier attempt committed but its response was lost to a
            # transient 5xx â€” exactly-once held, and the committed-order
            # checks below still catch any record that never landed.
            if status != 200:
                failures.append((index, seq, status, result))
                return
            results[index].append(seq)

    threads = [threading.Thread(target=writer_worker, args=(index,)) for index in range(WRITER_COUNT)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    if failures:
        raise AssertionError(f"concurrent appends failed: {failures[:3]}")
    if any(len(result) != RECORDS_PER_WRITER for result in results):
        raise AssertionError("a Writer lost records under concurrency")

    # Phase 2: the same request_id raced on two ingresses must still commit
    # exactly one record â€” dedup under a real race, not a sequential retry.
    race_body = append_body(writers[0], epochs[0], b"race", b"raced-request", 999)
    barrier = threading.Barrier(3)
    race_replies = []

    def race_worker(endpoint):
        barrier.wait()
        race_replies.append(committed_post(endpoint, "/v1/writers/append", race_body))

    racers = [threading.Thread(target=race_worker, args=(endpoint,)) for endpoint in (ENDPOINTS[0], ENDPOINTS[2])]
    for thread in racers:
        thread.start()
    barrier.wait()
    for thread in racers:
        thread.join()
    if len(race_replies) != 2 or any(status != 200 for status, _ in race_replies):
        raise AssertionError(f"raced duplicate request was not idempotent: {race_replies}")
    cursors = {reply["cursor"] for _, reply in race_replies}
    if len(cursors) != 1:
        raise AssertionError(f"raced duplicate committed twice: {race_replies}")

    # Phase 3: client clock skew. Records on one Key carry scrambled
    # event_time_ns (distant past, far future, epoch zero, negative, now).
    # Committed order must follow append order â€” declared event time never
    # reorders the immutable log â€” and ingest_time_ns stays non-decreasing.
    now_ns = time.time_ns()
    skewed_event_times = [now_ns - 86_400_000_000_000, now_ns + 86_400_000_000_000, 0, -1_000_000_000, now_ns]
    for seq, event_time in enumerate(skewed_event_times):
        body = append_body(writers[0], epochs[0], b"skew", f"skew-{seq}".encode(), event_time)
        status, result = committed_post(ENDPOINTS[seq % 3], "/v1/writers/append", body)
        if status != 200:
            raise AssertionError(f"skewed-event append refused: {result}")

    # Read the whole Feed and verify ordering, completeness, and skew immunity.
    status, page = committed_post(ENDPOINTS[1], "/v1/readers/temporary/fetch", {"feed": feed, "limit": 200})
    if status != 200:
        raise AssertionError(page)
    records = page["records"]
    expected = WRITER_COUNT * RECORDS_PER_WRITER + 1 + len(skewed_event_times)
    if len(records) != expected:
        raise AssertionError(f"committed {len(records)} records, expected {expected}")
    payloads = [base64.b64decode(record["payload_base64"]).decode() for record in records]
    if payloads.count("raced-request") != 1:
        raise AssertionError("the raced request_id produced a duplicate record")

    # Per-Key committed order must equal each Writer's submission order.
    by_writer = {index: [] for index in range(WRITER_COUNT)}
    for record in records:
        key = base64.b64decode(record["key_base64"]).decode()
        payload = base64.b64decode(record["payload_base64"]).decode()
        if key.startswith("key-"):
            by_writer[int(key.removeprefix("key-"))].append(payload)
    for index, seen in by_writer.items():
        expected_order = [f"w{index}-{seq:04d}" for seq in range(RECORDS_PER_WRITER)]
        if seen != expected_order:
            raise AssertionError(f"Key key-{index} committed order diverged from submission order")

    # Skewed event times never reorder the log: the skew Key delivers in
    # submission order and ingest timestamps never move backwards in it.
    skew_records = [
        record for record in records if base64.b64decode(record["key_base64"]).decode() == "skew"
    ]
    if [base64.b64decode(record["payload_base64"]).decode() for record in skew_records] != [f"skew-{seq}" for seq in range(len(skewed_event_times))]:
        raise AssertionError("client-declared event_time skew reordered committed delivery")
    ingest_times = [int(record["ingest_time_ns"]) for record in skew_records]
    if ingest_times != sorted(ingest_times):
        raise AssertionError("ingest_time_ns moved backwards under event_time skew")

    # Skewed event times still resolve through the declared event-time filter:
    # the far-future record is found by its own bound, not by ingest position.
    future = skewed_event_times[1]
    status, found = get(ENDPOINTS[0], f"/v1/feeds/records/search?feed={feed}&from_event_time_ns={future}")
    if status != 200 or not any(
        base64.b64decode(record["payload_base64"]).decode() == "skew-1" for record in found["records"]
    ):
        raise AssertionError(f"event-time search missed the far-future record: {found}")

    print(json.dumps({
        "status": "ok",
        "feed": feed,
        "writers": WRITER_COUNT,
        "records": expected,
        "raced_duplicate_committed_once": True,
        "skewed_event_times": [str(value) for value in skewed_event_times],
        "note": "concurrent Writers across all ingresses kept per-Key submission order with no loss or duplication, an identical request_id raced on two ingresses committed exactly once, and scrambled client event times never reordered the committed log while event-time search still resolved them",
    }, indent=2))


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"concurrent append and clock-skew acceptance failed: {error}", file=sys.stderr)
        sys.exit(1)
