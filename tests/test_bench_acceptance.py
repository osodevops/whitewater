import importlib.util
import json
from pathlib import Path
import unittest
from unittest.mock import patch


scripts = Path(__file__).resolve().parents[1] / "scripts"


def load(name):
    spec = importlib.util.spec_from_file_location(name.replace("-", "_"), scripts / name)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


bench_feed = load("bench-feed.py")
bench_kafka = load("bench-kafka.py")
disk_full = load("test-disk-full.py")
runner = load("run-acceptance.py")


class PercentileTests(unittest.TestCase):
    def test_empty_samples_report_no_latency(self):
        self.assertEqual(bench_feed.percentile_stats([]), {})

    def test_single_sample_reports_all_quantiles(self):
        stats = bench_feed.percentile_stats([0.002])
        self.assertEqual(stats["count"], 1)
        for key in ("min_ms", "p50_ms", "p95_ms", "p99_ms", "max_ms", "mean_ms"):
            self.assertAlmostEqual(stats[key], 2.0)

    def test_percentiles_rank_over_sorted_samples(self):
        stats = bench_feed.percentile_stats([i / 1000 for i in range(1, 101)])
        self.assertEqual(stats["count"], 100)
        self.assertAlmostEqual(stats["p50_ms"], 51, delta=1)
        self.assertAlmostEqual(stats["p99_ms"], 100, delta=1)
        self.assertAlmostEqual(stats["max_ms"], 100.0)


class BenchResultTests(unittest.TestCase):
    def test_result_shape_matches_shared_contract(self):
        result = bench_feed.bench_result(
            "whitewater", "write", 1000, 512, 2.0, [0.001], {"writers": 4}
        )
        parsed = json.loads(json.dumps(result))
        self.assertEqual(parsed["system"], "whitewater")
        self.assertEqual(parsed["mode"], "write")
        self.assertEqual(parsed["records"], 1000)
        self.assertEqual(parsed["records_per_sec"], 500.0)
        self.assertAlmostEqual(parsed["mib_per_sec"], 0.244, places=2)
        self.assertEqual(parsed["writers"], 4)
        self.assertIn("p50_ms", parsed)

    def test_write_driver_splits_records_into_disjoint_shares(self):
        writers = [(f"w{i}", 1) for i in range(3)]
        driver = bench_feed.WriteDriver(writers, 10, 64, 4)
        self.assertEqual(driver.shares, [4, 3, 3])
        self.assertEqual(sum(driver.shares), 10)

    def test_record_wire_shape(self):
        record = bench_feed.WriteDriver(
            [("w", 7)], 1, 16, 1
        ).record("w", 7, 42)
        self.assertEqual(record["writer"], "w")
        self.assertEqual(record["session_epoch"], 7)
        self.assertIn("request_id", record)
        self.assertIn("key_base64", record)
        self.assertIn("payload_base64", record)


class KafkaParserTests(unittest.TestCase):
    SAMPLE = (
        "20000 records sent, 20533.918823 records/sec (20.05 MB/sec), "
        "4.17 ms avg latency, 65.00 ms max latency, 3 ms 50th, 8 ms 95th, "
        "24 ms 99th, 41 ms 99.9th."
    )

    def test_producer_summary_parses_into_contract(self):
        result = bench_kafka.parse_producer_output("noise\n" + self.SAMPLE + "\ntrailer")
        self.assertEqual(result["system"], "kafka")
        self.assertEqual(result["mode"], "write")
        self.assertEqual(result["records"], 20000)
        self.assertEqual(result["records_per_sec"], 20533.9)
        self.assertAlmostEqual(result["mib_per_sec"], 19.12, places=2)
        self.assertEqual(result["p50_ms"], 3.0)
        self.assertEqual(result["p95_ms"], 8.0)
        self.assertEqual(result["p99_ms"], 24.0)
        self.assertEqual(result["mean_ms"], 4.17)

    def test_producer_summary_missing_raises(self):
        with self.assertRaises(AssertionError):
            bench_kafka.parse_producer_output("no summary here")

    def test_consumer_csv_parses_into_contract(self):
        header = (
            "start.time, end.time, data.consumed.in.MB, MB.sec, "
            "data.consumed.in.nMsg, nMsg.sec, rebalance.time.ms, "
            "fetch.time.ms, fetch.MB.sec, fetch.nMsg.sec"
        )
        row = "2025-01-01T00:00:00, 2025-01-01T00:00:02, 20.05, 10.0, 20000, 9990, 200, 2000, 10.0, 10000"
        result = bench_kafka.parse_consumer_output(header + "\n" + row)
        self.assertEqual(result["mode"], "read")
        self.assertEqual(result["records"], 20000)
        self.assertEqual(result["seconds"], 2.0)
        self.assertEqual(result["records_per_sec"], 10000.0)
        self.assertAlmostEqual(result["mib_per_sec"], 10.025, places=2)

    def test_consumer_without_rows_raises(self):
        with self.assertRaises(AssertionError):
            bench_kafka.parse_consumer_output("empty")


class DiskFullLaneTests(unittest.TestCase):
    def test_disk_full_lane_exists_with_the_fault_script(self):
        lane = next(
            item for item in runner.lane_catalog() if item.name == "disk-full"
        )
        self.assertEqual(lane.scripts, ("test-disk-full.py",))
        self.assertFalse(lane.fixture)

    def test_disk_full_scripts_stay_disjoint_from_other_lanes(self):
        catalog = runner.lane_catalog()
        seen = {}
        for lane in catalog:
            for script in lane.scripts:
                self.assertNotIn(script, seen, f"{script} on two lanes")
                seen[script] = lane.name
        self.assertEqual(seen["test-disk-full.py"], "disk-full")

    def test_disk_full_project_is_swept(self):
        projects = dict(runner.project_map(runner.lane_catalog()))
        self.assertEqual(projects.get("compose.disk-full.yml"), "whitewater-disk-full")
        self.assertEqual(projects.get("compose.kafka.yml"), "whitewater-kafka-bench")


class DiskFullScriptTests(unittest.TestCase):
    def test_record_wire_shape(self):
        record = disk_full.record("w", 3, 7)
        self.assertEqual(record["writer"], "w")
        self.assertEqual(record["session_epoch"], 3)
        self.assertIn("request_id", record)
        decoded_key = __import__("base64").b64decode(record["key_base64"]).decode()
        self.assertEqual(decoded_key, "diskfull-000007")

    def test_refused_batch_reports_none(self):
        with patch.object(disk_full, "post", return_value=(503, {"error": "full"})), \
                patch.object(disk_full.time, "sleep"):
            self.assertIsNone(disk_full.append_batch("w", 1, 0, 3, attempts=3))

    def test_acknowledged_batch_returns_sent_keys(self):
        response = {"results": [{"cursor": f"c{i}"} for i in range(3)]}
        with patch.object(disk_full, "post", return_value=(200, response)):
            self.assertEqual(
                disk_full.append_batch("w", 1, 10, 3, attempts=1),
                {"diskfull-000010", "diskfull-000011", "diskfull-000012"},
            )


if __name__ == "__main__":
    unittest.main()
