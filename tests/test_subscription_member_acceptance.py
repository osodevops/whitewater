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


class AcceptanceLaneTests(unittest.TestCase):
    def test_lane_union_covers_every_test_script_exactly_once(self):
        all_scripts = sorted(item.name for item in (runner.ROOT / "scripts").glob("test-*.py"))
        self.assertEqual(sorted(runner.test_scripts([])), all_scripts)
        for count in (2, 3, 4, 5):
            lanes = runner.lane_catalog(count)
            scripts = [script for lane in lanes for script in lane.scripts]
            self.assertEqual(sorted(scripts), all_scripts)
            self.assertEqual(len(scripts), len(set(scripts)))

    def test_fault_suite_is_last_on_the_mutating_lane_and_only_there(self):
        for lane in runner.lane_catalog():
            if lane.name == "verify-mutating":
                self.assertEqual(lane.scripts[:-1], runner.VERIFY_MUTATING)
                self.assertEqual(lane.scripts[-1], runner.FAULT_SUITE)
            else:
                self.assertNotIn(runner.FAULT_SUITE, lane.scripts)

    def test_verify_lanes_have_disjoint_ports_subnets_and_projects(self):
        lanes = [lane for lane in runner.lane_catalog() if lane.fixture]
        for values in (
            [lane.base_port for lane in lanes],
            [lane.subnet for lane in lanes],
            [lane.project for lane in lanes],
        ):
            self.assertEqual(len(values), len(set(values)), values)
        # Lane port blocks must stay clear of every hardcoded compose.*.yml
        # range (7171-7175, 7271-7273, 7471-7473, 7571-7575, 7971-7973).
        reserved = set().union(*[range(7171, 7176), range(7271, 7274), range(7471, 7474), range(7571, 7576), range(7971, 7974)])
        for lane in lanes:
            self.assertFalse(reserved.intersection(range(lane.base_port, lane.base_port + 3)), lane.name)

    def test_extra_verify_lane_env_carries_lane_identity(self):
        lane = next(lane for lane in runner.lane_catalog() if lane.name == "verify-reads-a")
        env = lane.env({})
        self.assertEqual(lane.nodes, ["http://localhost:7871", "http://localhost:7872", "http://localhost:7873"])
        self.assertEqual(env["WHITEWATER_TEST_BASE_PORT"], "7871")
        self.assertEqual(env["WHITEWATER_VERIFY_PORT_1"], "7871")
        self.assertEqual(env["WHITEWATER_VERIFY_PORT_2"], "7872")
        self.assertEqual(env["WHITEWATER_VERIFY_PORT_3"], "7873")
        self.assertEqual(env["WHITEWATER_VERIFY_SUBNET"], "10.250.248.0/24")
        self.assertEqual(env["COMPOSE_PROJECT_NAME"], "whitewater-verification-a")
        self.assertEqual(env["WHITEWATER_TEST_NODE3"], "whitewater-verification-a-node3-1")

    def test_primary_verify_lane_keeps_legacy_names(self):
        lane = next(lane for lane in runner.lane_catalog() if lane.name == "verify-mutating")
        env = lane.env({})
        self.assertEqual(lane.project, "whitewater-verification")
        self.assertEqual(env["WHITEWATER_TEST_BASE_PORT"], str(runner.BASE_PORT))
        self.assertNotIn("COMPOSE_PROJECT_NAME", env)
        self.assertNotIn("WHITEWATER_TEST_NODE3", env)

    def test_shifted_subnet_derives_next_24_and_refuses_wider_networks(self):
        self.assertEqual(runner.shifted_subnet("10.250.247.0/24", 1), "10.250.248.0/24")
        self.assertEqual(runner.shifted_subnet("10.250.247.0/24", 2), "10.250.249.0/24")
        with self.assertRaises(ValueError):
            runner.shifted_subnet("10.250.0.0/16", 1)

    def test_lane_env_selection_picks_exactly_its_scripts(self):
        lanes, _ = runner.parse_invocation([], {"WHITEWATER_ACCEPTANCE_LANE": "hotkeys"})
        self.assertEqual([lane.name for lane in lanes], ["hotkeys"])
        self.assertEqual(lanes[0].scripts, runner.HOTKEYS)
        lanes, _ = runner.parse_invocation(["--lane", "pipe"], {})
        self.assertEqual([lane.name for lane in lanes], ["pipe"])
        self.assertEqual(lanes[0].scripts, runner.PIPE)

    def test_unknown_lane_error_names_every_candidate(self):
        with self.assertRaisesRegex(ValueError, "isolated-zone"):
            runner.parse_invocation(["--lane", "bogus"], {})

    def test_lanes_count_splits_verify_scripts_across_verify_lanes(self):
        lanes, _ = runner.parse_invocation(["--lanes", "2"], {})
        self.assertEqual(
            [lane.name for lane in lanes],
            ["verify-mutating", "verify-reads-a", "sdk-languages",
             "isolated-zone", "pipe", "hotkeys", "disk-full"],
        )
        self.assertEqual(lanes[1].scripts, runner.VERIFY_READS)
        lanes, _ = runner.parse_invocation([], {"WHITEWATER_ACCEPTANCE_LANES": "3"})
        self.assertEqual(len(lanes), 8)
        self.assertEqual(lanes[1].scripts + lanes[2].scripts, runner.VERIFY_READS)

    def test_no_lane_selector_is_the_serial_legacy_mode(self):
        lanes, positionals = runner.parse_invocation([], {})
        self.assertIsNone(lanes)
        lanes, _ = runner.parse_invocation([], {"WHITEWATER_ACCEPTANCE_LANES": "1"})
        self.assertIsNone(lanes)
        lanes, positionals = runner.parse_invocation(["test-x.py"], {})
        self.assertIsNone(lanes)
        self.assertEqual(positionals, ["test-x.py"])

    def test_lane_selectors_reject_mixed_invocations(self):
        with self.assertRaises(ValueError):
            runner.parse_invocation(["--lane", "pipe", "test-x.py"], {})
        with self.assertRaises(ValueError):
            runner.parse_invocation(["--lanes", "3", "test-x.py"], {})
        with self.assertRaises(ValueError):
            runner.parse_invocation(["--lane"], {})

    def test_project_map_adds_lane_projects_to_the_static_sweep(self):
        verify_projects = [
            project for file, project in runner.project_map(runner.lane_catalog())
            if file == runner.COMPOSE_FILE
        ]
        self.assertEqual(
            verify_projects,
            [
                "whitewater-verification",
                "whitewater-verification-a",
                "whitewater-verification-b",
                "whitewater-verification-sdk",
            ],
        )

    def test_fixture_lane_provisions_before_running_scripts(self):
        lane = runner.Lane("verify-mutating", ("test-x.py",), fixture=True)
        order = []
        with patch.object(runner, "compose", side_effect=lambda *args, **kwargs: order.append("compose")), \
            patch.object(runner, "wait_healthy", side_effect=lambda **kwargs: order.append("healthy")), \
            patch.object(runner, "run_script", side_effect=lambda *args, **kwargs: order.append("script") or "ok"):
            results = runner.run_lane(lane, {}, [])
        self.assertEqual(order, ["compose", "compose", "healthy", "script"])
        self.assertEqual(results, {"test-x.py": "ok"})

    def test_fixture_failure_marks_every_lane_script_and_collects_diagnostics(self):
        lane = runner.Lane("verify-reads-a", ("test-x.py", "test-y.py"), fixture=True, project_suffix="-a")
        with patch.object(runner, "compose", side_effect=subprocess.CalledProcessError(1, ["docker"])), \
            patch.object(runner, "collect_diagnostics") as diagnostics, \
            patch.object(runner, "run_script") as run_script:
            results = runner.run_lane(lane, {}, [])
        self.assertEqual(sorted(results), ["test-x.py", "test-y.py"])
        self.assertTrue(all(outcome.startswith("fixture not ready") for outcome in results.values()))
        diagnostics.assert_called_once()
        run_script.assert_not_called()


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
