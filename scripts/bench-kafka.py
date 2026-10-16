"""Ad-hoc Kafka comparison benchmark (producer + consumer perf tools).

Provisions `compose.kafka.yml` — three combined KRaft brokers on
apache/kafka:3.8.0 — as project `whitewater-kafka-bench`, creates an RF3
topic, then runs the stock `kafka-producer-perf-test.sh` and
`kafka-consumer-perf-test.sh` tools inside a broker container. Results
print as `BENCH_RESULT {json}` lines sharing the metric contract with
`bench-feed.py` and the Java `BenchFeed` example:

    python scripts/bench-kafka.py                    # write + read
    BENCH_MODE=write python scripts/bench-kafka.py   # producer only

Durability parity note: the producer defaults to `acks=all` so both
systems are measured at their strongest acknowledged-durability setting;
Whitewater appends always commit through the RF3 quorum. Batching is not
identical — Kafka batches inside the client (`linger.ms`/`batch.size`),
Whitewater batches records per call on `/v1/writers/append-batch` — the
BENCH_BATCH/BENCH_PRODUCER_PROPS knobs keep both sides tunable.

Environment knobs: BENCH_RECORDS, BENCH_BYTES, BENCH_PARTITIONS,
BENCH_MODE, BENCH_TOPIC, BENCH_PRODUCER_PROPS, WW_KEEP_UP.
"""

import json
import os
import re
import subprocess
import time

COMPOSE = "compose.kafka.yml"
PROJECT = "whitewater-kafka-bench"
BOOTSTRAP = "broker1:29092,broker2:29092,broker3:29092"

RECORDS = int(os.environ.get("BENCH_RECORDS", "20000"))
BYTES = int(os.environ.get("BENCH_BYTES", "1024"))
PARTITIONS = int(os.environ.get("BENCH_PARTITIONS", "6"))
MODE = os.environ.get("BENCH_MODE", "all")
TOPIC = os.environ.get("BENCH_TOPIC", "bench.events")
PRODUCER_PROPS = os.environ.get(
    "BENCH_PRODUCER_PROPS", "acks=all linger.ms=5"
)


def compose(*args, timeout=120):
    result = subprocess.run(
        ["docker", "compose", "-f", COMPOSE, *args],
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    return result.returncode, result.stdout + result.stderr


def exec_broker(command, timeout=600):
    """Run a bundled Kafka tool inside broker1 over the internal listener.
    The apache/kafka image ships tools under /opt/kafka/bin, not PATH."""
    code, out = compose(
        "exec", "-T", "broker1", f"/opt/kafka/bin/{command[0]}", *command[1:],
        timeout=timeout,
    )
    return code, out


def wait_ready():
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        code, out = exec_broker(
            ["kafka-broker-api-versions.sh", "--bootstrap-server", "broker1:29092"],
            timeout=30,
        )
        if code == 0 and len(re.findall(r"broker\d:\d+", out)) >= 3:
            return
        time.sleep(2)
    raise AssertionError(f"kafka brokers never formed a quorum: {out}")


def create_topic():
    code, out = exec_broker(
        [
            "kafka-topics.sh",
            "--bootstrap-server", BOOTSTRAP,
            "--create",
            "--topic", TOPIC,
            "--partitions", str(PARTITIONS),
            "--replication-factor", "3",
        ],
        timeout=60,
    )
    assert code == 0 or "already exists" in out, out


PRODUCER_RE = re.compile(
    r"(\d+) records sent, ([\d.]+) records/sec \(([\d.]+) MB/sec\), "
    r"([\d.]+) ms avg latency, ([\d.]+) ms max latency, "
    r"(\d+) ms 50th, (\d+) ms 95th, (\d+) ms 99th, (\d+) ms 99\.9th"
)


def parse_producer_output(output):
    """Map the perf-tool summary line onto the shared result contract."""
    match = PRODUCER_RE.search(output)
    assert match, f"producer-perf summary not found: {output[-800:]}"
    (
        records,
        records_per_sec,
        mb_per_sec,
        avg_ms,
        max_ms,
        p50,
        p95,
        p99,
        p999,
    ) = match.groups()
    return {
        "system": "kafka",
        "mode": "write",
        "records": int(records),
        "bytes_per_record": BYTES,
        "records_per_sec": round(float(records_per_sec), 1),
        "mib_per_sec": round(float(mb_per_sec) / 1.048576, 2),
        "mean_ms": float(avg_ms),
        "max_ms": float(max_ms),
        "p50_ms": float(p50),
        "p95_ms": float(p95),
        "p99_ms": float(p99),
        "p999_ms": float(p999),
    }


def run_write():
    code, out = exec_broker(
        [
            "kafka-producer-perf-test.sh",
            "--topic", TOPIC,
            "--num-records", str(RECORDS),
            "--record-size", str(BYTES),
            "--throughput", "-1",
            "--producer-props",
            f"bootstrap.servers={BOOTSTRAP}",
            *PRODUCER_PROPS.split(),
        ],
        timeout=RECORDS // 100 + 300,
    )
    assert code == 0, out[-800:]
    return parse_producer_output(out)


def parse_consumer_output(output):
    """The consumer perf tool prints a header row then one CSV data row."""
    lines = [line.strip() for line in output.splitlines() if line.strip()]
    for index, line in enumerate(lines):
        if "data.consumed.in.nMsg" in line and index + 1 < len(lines):
            header = [column.strip() for column in line.split(",")]
            row = [column.strip() for column in lines[index + 1].split(",")]
            values = dict(zip(header, row))
            try:
                records = int(float(values["data.consumed.in.nMsg"]))
                mib = float(values["data.consumed.in.MB"])
                seconds = float(values["fetch.time.ms"]) / 1000
                per_sec = float(values["fetch.nMsg.sec"])
            except (KeyError, ValueError) as error:
                raise AssertionError(
                    f"consumer-perf row unparseable: {values}"
                ) from error
            return {
                "system": "kafka",
                "mode": "read",
                "records": records,
                "bytes_per_record": BYTES,
                "seconds": round(seconds, 3),
                "records_per_sec": round(per_sec, 1),
                "mib_per_sec": round(mib / seconds, 2) if seconds > 0 else 0,
            }
    raise AssertionError(f"consumer-perf summary not found: {output[-800:]}")


def run_read():
    code, out = exec_broker(
        [
            "kafka-consumer-perf-test.sh",
            "--bootstrap-server", BOOTSTRAP,
            "--topic", TOPIC,
            "--messages", str(RECORDS),
            "--timeout", "180000",
        ],
        timeout=360,
    )
    assert code == 0, out[-800:]
    return parse_consumer_output(out)


def report(result):
    print(f"BENCH_RESULT {json.dumps(result)}")
    latency = " ".join(
        f"{key}={result[key]}ms"
        for key in ("p50_ms", "p95_ms", "p99_ms")
        if key in result
    )
    print(
        f"  {result['system']} {result['mode']}: "
        f"{result['records_per_sec']:,.0f} records/s, "
        f"{result['mib_per_sec']:,.2f} MiB/s {latency}"
    )


def main():
    code, out = compose("up", "-d")
    assert code == 0, out
    try:
        wait_ready()
        create_topic()
        if MODE in ("write", "all"):
            report(run_write())
        if MODE in ("read", "all"):
            report(run_read())
        print("KAFKA BENCH PASSED")
    finally:
        if not os.environ.get("WW_KEEP_UP"):
            compose("down", "-v")


if __name__ == "__main__":
    main()
