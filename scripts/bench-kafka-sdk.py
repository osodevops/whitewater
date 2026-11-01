"""Ad-hoc Kafka Java-SDK comparison benchmark.

Provisions `compose.kafka.yml` — three combined KRaft brokers on
apache/kafka:3.8.0, project `whitewater-kafka-bench` — creates an RF3
topic, then builds and runs `benchmarks/kafka-sdk` (a plain
`kafka-clients` producer/consumer app; Spring Kafka delegates to the
same client) in a Maven container on the bench network. Results print
as `BENCH_RESULT {json}` lines sharing the metric contract with
`scripts/bench-feed.py` and `examples/bench_feed.rs` (whitewater-rust):

    python scripts/bench-kafka-sdk.py                    # write + read
    BENCH_MODE=write python scripts/bench-kafka-sdk.py   # producer only

Durability parity: the app defaults to `acks=all` +
`enable.idempotence=true` against RF3/min-ISR-2 — the strongest
acknowledged-durability Kafka shape, matching Whitewater's RF3 quorum
commit. Maven dependencies cache in the `kafka-sdk-m2` volume.

Environment knobs: BENCH_RECORDS, BENCH_BYTES, BENCH_PARTITIONS,
BENCH_WRITERS, BENCH_READ_THREADS, BENCH_MODE, BENCH_TOPIC,
BENCH_PRODUCER_ACKS, BENCH_PRODUCER_LINGER_MS, BENCH_PRODUCER_BATCH_SIZE,
BENCH_CONSUMER_*, WW_KEEP_UP.
"""

import os
import re
import subprocess
import time

COMPOSE = "compose.kafka.yml"
PROJECT = "whitewater-kafka-bench"
BOOTSTRAP = "broker1:29092,broker2:29092,broker3:29092"
MAVEN_IMAGE = "maven:3.9-eclipse-temurin-17"
M2_VOLUME = "kafka-sdk-m2"

TOPIC = os.environ.get("BENCH_TOPIC", "bench.events")
PARTITIONS = int(os.environ.get("BENCH_PARTITIONS", "1"))


def compose(*args, timeout=120):
    env = dict(os.environ)
    env["COMPOSE_PROJECT_NAME"] = PROJECT
    result = subprocess.run(
        ["docker", "compose", "-f", COMPOSE, *args],
        capture_output=True,
        text=True,
        timeout=timeout,
        env=env,
    )
    return result.returncode, result.stdout + result.stderr


def exec_broker(command, timeout=600):
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


def java_bench_command(env=None):
    """The docker run invocation: BENCH_* knobs pass through so the Java
    side and this driver never disagree on the workload shape."""
    env = os.environ if env is None else env
    env_args = ["-e", f"BOOTSTRAP_SERVERS={BOOTSTRAP}", "-e", f"BENCH_TOPIC={TOPIC}"]
    for name, value in env.items():
        if name.startswith("BENCH_") and name != "BENCH_TOPIC":
            env_args += ["-e", f"{name}={value}"]
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    source = os.path.join(root, "benchmarks", "kafka-sdk")
    return [
        "docker", "run", "--rm",
        "--network", f"{PROJECT}_default",
        "-v", f"{source}:/src",
        "-v", f"{M2_VOLUME}:/root/.m2",
        "-w", "/src",
        *env_args,
        MAVEN_IMAGE,
        "sh", "-c",
        "mvn -q -DskipTests package && java -jar target/kafka-sdk-bench.jar",
    ]


def run_java_bench():
    """Build once (cached m2 volume), then run on the bench network."""
    result = subprocess.run(
        java_bench_command(), capture_output=True, text=True, timeout=1800
    )
    sys_out = result.stdout + result.stderr
    results = re.findall(r"BENCH_RESULT .*", sys_out)
    for line in results:
        print(line)
    assert result.returncode == 0, "java bench failed: " + sys_out[-3000:]
    assert results, "no BENCH_RESULT emitted: " + sys_out[-3000:]


def main():
    if not os.environ.get("WW_KEEP_UP"):
        compose("down", "-v")
    code, out = compose("up", "-d", timeout=300)
    assert code == 0, out
    try:
        wait_ready()
        create_topic()
        run_java_bench()
    finally:
        if not os.environ.get("WW_KEEP_UP"):
            compose("down", "-v")


if __name__ == "__main__":
    main()
