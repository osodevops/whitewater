import importlib.util
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import call, patch


path = Path(__file__).resolve().parents[1] / "scripts" / "test-m7-subscription-member.py"
spec = importlib.util.spec_from_file_location("subscription_acceptance", path)
acceptance = importlib.util.module_from_spec(spec)
spec.loader.exec_module(acceptance)


class SubscriptionMemberAcceptanceTests(unittest.TestCase):
    def test_replica_unavailable_retries_the_exact_request_identity(self):
        body = {"request_id": "same-request", "member_id": "same-member"}
        unavailable = {"error": "Subscription progress replica is unavailable; retry the same request identity"}
        expected = {"member_epoch": 1}
        with patch.object(acceptance, "post", side_effect=[(503, unavailable), (200, expected)]) as post, patch.object(acceptance.time, "sleep"):
            self.assertEqual(acceptance.member_ready("http://node", "/join", body), expected)
            self.assertEqual(post.call_args_list, [call("http://node", "/join", body)] * 2)

    def test_non_retryable_fencing_error_fails_immediately_even_with_election_text(self):
        with patch.object(acceptance, "post", return_value=(409, {"error": "stale epoch after leader election"})) as post, patch.object(acceptance.time, "sleep") as sleep:
            with self.assertRaises(AssertionError):
                acceptance.member_ready("http://node", "/join", {})
            self.assertEqual(post.call_count, 1)
            sleep.assert_not_called()

    def test_persistent_unavailability_exhausts_a_bounded_retry_budget(self):
        with patch.object(acceptance, "post", return_value=(503, {"error": "replica unavailable"})) as post, patch.object(acceptance.time, "sleep") as sleep:
            with self.assertRaisesRegex(AssertionError, "re-election window"):
                acceptance.member_ready("http://node", "/join", {})
            self.assertEqual(post.call_count, 45)
            self.assertEqual(sleep.call_count, 44)
            self.assertTrue(all(0 < item.args[0] <= 1 for item in sleep.call_args_list))


runner_path = path.with_name("run-acceptance.py")
runner_spec = importlib.util.spec_from_file_location("acceptance_runner", runner_path)
runner = importlib.util.module_from_spec(runner_spec)
runner_spec.loader.exec_module(runner)


class AcceptanceDiagnosticTests(unittest.TestCase):
    def test_diagnostics_are_scoped_to_test_projects_and_include_logs(self):
        result = subprocess.CompletedProcess([], 0, stdout="node logs", stderr="diagnostic warning")
        with tempfile.TemporaryDirectory() as directory, patch.object(runner, "ROOT", Path(directory)), patch.object(runner, "compose_file_projects", return_value=[("compose.verify.yml", "whitewater-verification")]), patch.object(runner.subprocess, "run", return_value=result) as run:
            runner.collect_diagnostics("failed/test", {})
            files = sorted((Path(directory) / "acceptance-logs").glob("*.log"))
            self.assertEqual(len(files), 4)
            self.assertTrue(all(file.read_text() == "node logsdiagnostic warning" for file in files))
            compose_calls = [item for item in run.call_args_list if "compose" in item.args[0]]
            self.assertEqual(len(compose_calls), 2)
            self.assertTrue(all("whitewater-verification" in item.args[0] for item in compose_calls))
            self.assertTrue(all("down" not in item.args[0] for item in run.call_args_list))

    def test_failed_script_collects_diagnostics_before_cleanup(self):
        order = []
        with patch.object(runner, "ports_free", return_value=[]), patch.object(runner, "preflight"), patch.object(runner, "test_scripts", return_value=["test-m7-subscription-member.py"]), patch.object(runner, "compose"), patch.object(runner, "wait_healthy"), patch.object(runner.subprocess, "run", side_effect=subprocess.CalledProcessError(1, ["python"])), patch.object(runner, "collect_diagnostics", side_effect=lambda *args: order.append("diagnostics")), patch.object(runner, "sweep", side_effect=lambda *args: order.append("cleanup")):
            self.assertEqual(runner.main(), 1)
        self.assertEqual(order, ["diagnostics", "cleanup"])


many_path = path.with_name("test-many-readers.py")
many_spec = importlib.util.spec_from_file_location("many_reader_acceptance", many_path)
many = importlib.util.module_from_spec(many_spec)
many_spec.loader.exec_module(many)


class ManyReaderIdentityTests(unittest.TestCase):
    def test_claim_keeps_request_identity_across_ambiguous_results(self):
        import threading
        stats = many.Stats()
        stop = threading.Event()
        responses = [(503, {"code": "ambiguous_commit"}), (200, {"lease": {"lease_epoch": 1}})] + [(200, {"records": []})] * 3
        with patch.object(many, "post", side_effect=responses) as post, patch.object(many.time, "sleep"), patch.object(stop, "wait", return_value=False):
            many.member_worker(0, "orders.sub", "member", "work", stats, stop)
        claims = [item.args[2] for item in post.call_args_list if item.args[1].endswith("/claim")]
        self.assertEqual(claims[0]["request_id"], claims[1]["request_id"])
        self.assertNotEqual(post.call_args_list[0].args[0], post.call_args_list[1].args[0])
        self.assertEqual(stats.done, {0})

    def test_ack_keeps_identity_until_commit_is_known_then_reclaims_released_lease(self):
        import threading
        stats = many.Stats()
        stop = threading.Event()
        responses = [
            (200, {"lease": {"lease_epoch": 1}}),
            (200, {"records": [{"cursor": "c"}], "cursor": "c", "positions": {}}),
            (503, {"code": "ambiguous_commit"}), (200, {}),
            (200, {"lease": {"lease_epoch": 2}}),
        ] + [(200, {"records": []})] * 3
        with patch.object(many, "post", side_effect=responses) as post, patch.object(many.time, "sleep"), patch.object(stop, "wait", return_value=False):
            many.member_worker(0, "orders.sub", "member", "work", stats, stop)
        acks = [item.args[2] for item in post.call_args_list if item.args[1].endswith("/ack")]
        self.assertEqual(acks[0], acks[1])
        self.assertEqual(stats.acks, 1)
        self.assertEqual(stats.done, {0})

    def test_non_retryable_lease_fencing_is_returned_without_retry(self):
        import threading
        body = {"request_id": "same-request"}
        response = (409, {"code": "lease.stale_lease"})
        with patch.object(many, "post", return_value=response) as post:
            self.assertEqual(many.member_post("http://node", "/ack", body, many.Stats(), threading.Event()), response)
        self.assertEqual(post.call_count, 1)

    def test_ambiguous_retry_stops_with_the_global_deadline(self):
        import threading
        stats = many.Stats()
        stop = threading.Event()
        response = (503, {"code": "ambiguous_commit"})
        def end_retry(delay):
            stop.set()
            return True
        with patch.object(many, "post", return_value=response) as post, patch.object(stop, "wait", side_effect=end_retry):
            self.assertEqual(many.member_post("http://node", "/ack", {}, stats, stop), response)
        self.assertEqual(post.call_count, 1)

    def test_stopped_worker_is_not_reported_as_drained(self):
        import threading
        stats = many.Stats()
        stop = threading.Event()
        stop.set()
        many.member_worker(0, "orders.sub", "member", "work", stats, stop)
        self.assertEqual(stats.done, set())


if __name__ == "__main__":
    unittest.main()
