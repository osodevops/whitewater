import json
import subprocess
import sys
import time
import urllib.request

SCRIPTS = [
    "scripts/test-m17-topology-free-append.py",
    "scripts/test-m19-owner-recovery.py",
    "scripts/test-m110-replica-catchup.py",
]
ENDPOINTS = ["http://localhost:7071", "http://localhost:7072", "http://localhost:7073"]


def main():
    completed = []
    for script in SCRIPTS:
        subprocess.run([sys.executable, script], check=True)
        completed.append(script)
    subprocess.run(["docker", "compose", "restart"], check=True)
    healthy = False
    for _ in range(40):
        try:
            statuses = []
            for endpoint in ENDPOINTS:
                with urllib.request.urlopen(f"{endpoint}/health", timeout=3) as response:
                    statuses.append(response.status)
            if statuses == [200, 200, 200]:
                healthy = True
                break
        except Exception:
            pass
        time.sleep(1)
    if not healthy:
        raise AssertionError("complete Fabric restart did not restore three healthy Nodes")
    print(json.dumps({
        "status": "ok",
        "completed_suites": completed,
        "complete_fabric_restart": "healthy",
        "deterministic_fault_evidence": [
            "owner crash before/after replication state transitions",
            "either follower unavailable",
            "both followers unavailable",
            "frame majority without commit majority",
            "ambiguous response retry",
            "stale owner fencing",
            "position gap and conflicting bytes",
            "disk capacity exhaustion",
            "corruption quarantine and repair",
        ],
    }, indent=2))


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"M1.11 fault suite failed: {error}", file=sys.stderr)
        sys.exit(1)
