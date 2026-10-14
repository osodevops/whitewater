import json
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
                if response.status != 200:
                    continue
            request = urllib.request.Request(
                f"{endpoint}/v1/admin/wcl",
                data=json.dumps({"script": "SHOW DOMAINS;"}).encode(),
                headers={
                    "content-type": "application/json",
                    "authorization": f"Bearer {KEY}",
                },
                method="POST",
            )
            # /health answers before Control Plane authority exists —
            # fixtures issue WCL immediately, so readiness means a
            # majority-committed catalog answers, not just the listener.
            with urllib.request.urlopen(request, timeout=3) as wcl_reply:
                if wcl_reply.status == 200:
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
    fixtures = len(
        [
            name
            for name in os.listdir(os.path.join(ROOT, "sdk", "spec", "conformance"))
            if name.endswith(".json")
        ]
    )
    if passed < fixtures:
        raise AssertionError(
            f"only {passed}/{fixtures} fixtures reported ok; output above"
        )
    print(f"sdk conformance corpus green ({passed}/{fixtures} fixtures)")


if __name__ == "__main__":
    main()
