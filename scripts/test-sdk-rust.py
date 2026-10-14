import os
import subprocess
import sys
import time
import urllib.error
import urllib.request

BASE_PORT = int(os.environ.get("WHITEWATER_TEST_BASE_PORT", "7071"))
ENDPOINTS = [f"http://localhost:{BASE_PORT + offset}" for offset in range(3)]
KEY = "whitewater-local-development-admin-key"
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def wait_ready(endpoint):
    for _ in range(120):
        try:
            with urllib.request.urlopen(f"{endpoint}/health", timeout=3) as response:
                if response.status == 200:
                    return
        except Exception:
            pass
        time.sleep(0.5)
    raise AssertionError(f"{endpoint} never answered /health")


def main():
    for endpoint in ENDPOINTS:
        wait_ready(endpoint)

    env = dict(os.environ)
    env["WHITEWATER_SDK_NODES"] = ",".join(ENDPOINTS)
    env["WHITEWATER_ADMIN_API_KEY"] = KEY
    run = subprocess.run(
        ["cargo", "test", "--test", "sdk_live", "--", "--nocapture"],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=600,
    )
    sys.stdout.write(run.stdout)
    sys.stderr.write(run.stderr)
    if run.returncode != 0:
        raise AssertionError(f"cargo test --test sdk_live exited {run.returncode}")
    for test in (
        "sdk_roundtrip_against_a_live_riverbed",
        "state_store_index_and_temporary_facades_live",
        "stale_member_epoch_fences_as_typed_errors",
    ):
        if f"test {test} ... ok" not in run.stdout:
            raise AssertionError(f"{test} did not report ok; output above")
    if "skipping live SDK acceptance" in run.stdout + run.stderr:
        raise AssertionError("the live tests skipped — env wiring failed")
    print("sdk-rust live acceptance green")


if __name__ == "__main__":
    main()
