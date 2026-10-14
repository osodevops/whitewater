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
        ["cargo", "test", "--test", "sdk_conformance", "--", "--nocapture"],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=600,
    )
    sys.stdout.write(run.stdout)
    sys.stderr.write(run.stderr)
    if run.returncode != 0:
        raise AssertionError(f"cargo test --test sdk_conformance exited {run.returncode}")
    if "skipping conformance corpus" in run.stdout + run.stderr:
        raise AssertionError("the conformance corpus skipped — env wiring failed")
    passed = (run.stdout + run.stderr).count("conformance ok")
    if passed < 9:
        raise AssertionError(f"only {passed}/9 fixtures reported ok; output above")
    print(f"sdk conformance corpus green ({passed}/9 fixtures)")


if __name__ == "__main__":
    main()
