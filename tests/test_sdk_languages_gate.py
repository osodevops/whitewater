import importlib.util
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


path = Path(__file__).resolve().parents[1] / "scripts" / "test-sdk-languages.py"
spec = importlib.util.spec_from_file_location("sdk_languages_gate", path)
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)


class SdkLanguagesGatePlanTests(unittest.TestCase):
    def base_env(self, **overrides):
        env = {"PATH": os.environ.get("PATH", "")}
        env.update(overrides)
        return env

    def test_endpoints_derive_from_the_lane_base_port(self):
        endpoints, _, _ = gate.plan(self.base_env(WHITEWATER_TEST_BASE_PORT="7371"))
        self.assertEqual(
            endpoints,
            ["http://localhost:7371", "http://localhost:7372", "http://localhost:7373"],
        )

    def test_runner_env_carries_the_shared_contract(self):
        _, env, _ = gate.plan(self.base_env(WHITEWATER_TEST_BASE_PORT="7371"))
        self.assertEqual(
            env["WHITEWATER_ENDPOINTS"],
            "http://localhost:7371,http://localhost:7372,http://localhost:7373",
        )
        self.assertEqual(env["WHITEWATER_SPEC_DIR"], gate.SPEC_DIR)
        self.assertEqual(env["WHITEWATER_API_KEY"], gate.KEY)
        self.assertEqual(env["WHITEWATER_TEST_BASE_PORT"], "7371")
        self.assertIn("chaos_driver.py", env["WHITEWATER_CHAOS_CMD"])
        # Legacy aliases stay populated for the .NET/Go runners.
        self.assertEqual(env["WHITEWATER_CONFORMANCE_URLS"], env["WHITEWATER_ENDPOINTS"])
        self.assertEqual(env["WHITEWATER_FIXTURE_DIR"], gate.SPEC_DIR)

    def test_relative_compose_file_is_absolutized_against_the_repo_root(self):
        # chaos_driver runs `docker compose -f $COMPOSE_FILE` under each
        # sibling repo's cwd — a relative path would not resolve there.
        _, env, _ = gate.plan(self.base_env(COMPOSE_FILE="compose.verify.yml"))
        self.assertTrue(os.path.isabs(env["COMPOSE_FILE"]))
        self.assertEqual(
            env["COMPOSE_FILE"], os.path.join(gate.ROOT, "compose.verify.yml"))

    def test_absolute_compose_file_passes_through(self):
        _, env, _ = gate.plan(
            self.base_env(COMPOSE_FILE=os.path.join("x", "custom", "f.yml")))
        self.assertTrue(env["COMPOSE_FILE"].endswith(os.path.join("custom", "f.yml")))

    def test_all_six_runners_are_expected_by_default(self):
        _, _, runners = gate.plan(self.base_env())
        self.assertEqual(
            set(runners), {"rust", "go", "python", "java", "node", "dotnet"})

    def test_language_narrowing_keeps_named_runners(self):
        _, _, runners = gate.plan(
            self.base_env(WHITEWATER_SDK_LANGUAGES="python, node"))
        self.assertEqual(set(runners), {"python", "node"})

    def test_language_narrowing_refuses_unknown_names(self):
        with self.assertRaises(AssertionError):
            gate.plan(self.base_env(WHITEWATER_SDK_LANGUAGES="rust,cobol"))

    def test_sdk_dir_override_beats_the_sibling_default(self):
        with tempfile.TemporaryDirectory() as override:
            _, _, runners = gate.plan(
                self.base_env(WHITEWATER_GO_DIR=override))
            self.assertEqual(runners["go"]["dir"], override)

    def test_missing_checkout_reports_absent_not_silent(self):
        _, _, runners = gate.plan(
            self.base_env(WHITEWATER_GO_DIR=os.path.join("x", "not", "checked", "out")))
        # plan leaves a None dir; main() reports it as `absent` — the
        # gate fails rather than silently skipping the SDK.
        self.assertIsNone(runners["go"]["dir"])

    def test_rust_runner_uses_the_canonical_conformance_invocation(self):
        _, _, runners = gate.plan(self.base_env())
        self.assertEqual(
            runners["rust"]["steps"],
            [["cargo", "test", "--test", "sdk_conformance", "--", "--nocapture"]],
        )
        self.assertIn("WHITEWATER_SDK_NODES", runners["rust"]["env"])

    def test_go_prefers_a_host_toolchain(self):
        def which(tool):
            return "/usr/bin/go" if tool == "go" else None
        with patch.object(gate.shutil, "which", side_effect=which):
            spec = gate.go_leg("/tmp/whitewater-go")
        self.assertEqual(spec["toolchain"], ["go"])
        self.assertEqual(spec["steps"][0][0], "go")

    def test_go_falls_back_to_a_cross_compiled_native_binary(self):
        # The chaos fixtures need the host's Python + Docker daemon — a
        # golang container cannot reach Docker Desktop's npipe — so the
        # fallback compiles inside Docker and RUNS natively.
        def which(tool):
            return "/usr/bin/docker" if tool == "docker" else None
        with patch.object(gate.shutil, "which", side_effect=which):
            spec = gate.go_leg("/tmp/whitewater-go")
        self.assertEqual(spec["toolchain"], ["docker"])
        compile_step, run_step = spec["steps"]
        self.assertEqual(compile_step[0], "docker")
        self.assertIn("GOOS=", " ".join(compile_step))
        self.assertIn("test", compile_step)
        self.assertNotEqual(run_step[0], "docker")

    def test_go_without_either_toolchain_reports_missing(self):
        with patch.object(gate.shutil, "which", return_value=None):
            spec = gate.go_leg("/tmp/whitewater-go")
        self.assertEqual(spec["toolchain"], ["go"])
        self.assertEqual(spec["steps"], [])

    def test_python_install_pulls_the_dev_test_extra(self):
        # `pip install -e .` alone leaves the hosted runner without
        # pytest — the SDK declares it under the `dev` extra.
        _, _, runners = gate.plan(self.base_env())
        install = runners["python"]["steps"][0]
        self.assertIn(".[dev]", install)

    def test_java_leg_runs_mvnw_through_sh_off_windows(self):
        # whitewater-java's mvnw may arrive without the +x bit (mode
        # 100644 committed on Windows); `sh mvnw` runs it regardless.
        spec = gate.java_leg("/tmp/whitewater-java")
        argv = spec["steps"][0]
        if gate.WINDOWS:
            self.assertEqual(
                argv[0], os.path.join("/tmp/whitewater-java", "mvnw.cmd"))
        else:
            self.assertEqual(argv[0], "sh")
            self.assertIn("mvnw", argv[1])

    def test_run_steps_turns_a_launch_error_into_a_leg_failure(self):
        # A PermissionError on the step binary used to crash the gate
        # with a raw traceback; it now reports a failed leg like any
        # other nonzero exit.
        def deny(*args, **kwargs):
            raise PermissionError(13, "Permission denied", "mvnw")
        with patch.object(gate.subprocess, "run", side_effect=deny):
            code = gate.run_steps([["./mvnw", "verify"]], "/tmp", {}, 60)
        self.assertEqual(code, 1)


if __name__ == "__main__":
    unittest.main()
