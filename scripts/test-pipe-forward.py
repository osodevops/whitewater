import base64
import http.client
import json
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

COMPOSE = ["docker", "compose", "-f", "compose.pipe.yml", "-p", "whitewater-pipe"]
NODES = [f"http://localhost:{port}" for port in range(7471, 7474)]
SERVICES = ["control-1", "control-2", "control-3"]
KEY = "whitewater-local-development-admin-key"
CONTROL_KEY = "whitewater-local-development-control-key"


# Transient connection errors between the host and Docker-published ports are
# retried; HTTP statuses and assertion failures still propagate unchanged.
def _retried(operation):
    def wrapped(*args, **kwargs):
        last = None
        for _ in range(60):
            try:
                return operation(*args, **kwargs)
            except urllib.error.HTTPError:
                raise
            except (ConnectionError, urllib.error.URLError, TimeoutError) as error:
                last = error
                time.sleep(0.5)
        raise last

    return wrapped

@_retried
def post(node, path, payload, control=False):
    # Same retry posture as the other acceptance scripts: 5xx/transport
    # transients retry with the same request identity; refusals return.
    for _ in range(120):
        status, result = _post_once(node, path, payload, control)
        if status == 0 or status >= 500:
            time.sleep(0.5)
            continue
        return status, result
    return status, result

def _post_once(node, path, payload, control=False):
    headers = {"content-type": "application/json"}
    if control:
        headers["x-whitewater-control-key"] = CONTROL_KEY
    else:
        headers["authorization"] = f"Bearer {KEY}"
    request = urllib.request.Request(
        f"{node}{path}",
        data=json.dumps(payload).encode(),
        headers=headers,
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        return error.code, json.load(error)
    except OSError:
        return 0, {}
def wcl(node, script, request_id=None):
    status, response = post(node, "/v1/admin/wcl", {"request_id": request_id or str(uuid.uuid4()), "script": script})
    if status != 200:
        raise AssertionError({"script": script, "response": response})
    return [result["data"] for result in response["results"]]


def wcl_retry(node, script, attempts=30, delay=1):
    request_id = str(uuid.uuid4())
    last = None
    for _ in range(attempts):
        try:
            return wcl(node, script, request_id)
        except AssertionError as error:
            last = error
            time.sleep(delay)
    raise last


def read(node, feed):
    query = urllib.parse.urlencode({"feed": feed, "limit": 100})
    request = urllib.request.Request(
        f"{node}/v1/feeds/records?{query}",
        headers={"authorization": f"Bearer {KEY}"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def b64(text):
    return base64.b64encode(text.encode()).decode()


def append_batch(node, writer, epoch, events, retries=1):
    records = [
        {
            "request_id": str(uuid.uuid4()),
            "writer": writer,
            "session_epoch": epoch,
            "event_time_ns": str(1_700_000_000_000_000_000 + index),
            "key_base64": b64(f"order-{index}"),
            "payload_base64": b64(f"payload-{index}"),
            "metadata_base64": {"trace": b64(f"trace-{index}")},
        }
        for index in events
    ]
    last = None
    for _ in range(retries):
        status, body = post(node, "/v1/writers/append-batch", {"records": records})
        if status == 200:
            return records
        last = body
        time.sleep(2)
    raise AssertionError({"append": last})


def wait_for_fabric():
    for _ in range(120):
        try:
            if all(
                urllib.request.urlopen(f"{node}/health", timeout=2).status == 200
                for node in NODES
            ):
                return
        except (urllib.error.URLError, TimeoutError, ConnectionError, http.client.RemoteDisconnected):
            pass
        time.sleep(1)
    raise AssertionError("pipe Riverbed did not become healthy")


def wait_forwarded(feed, expected, deadline=90):
    """The Pipe supervisor forwards without any trigger; poll until the
    output Feed holds every appended record."""
    last = None
    end = time.time() + deadline
    while time.time() < end:
        try:
            records = read(NODES[2], feed)
            if len(records) >= expected:
                return records
            last = {"feed": feed, "records": len(records), "expected": expected}
        except (urllib.error.URLError, urllib.error.HTTPError) as error:
            last = repr(error)
        time.sleep(1)
    raise AssertionError(f"Pipe did not forward {expected} records: {last}")


def drive_probe(node, pipe_id, subscription_id, storage_node, epoch):
    return post(
        node,
        "/internal/pipe/drive",
        {
            "pipe_id": pipe_id,
            "subscription_id": subscription_id,
            "owner": storage_node,
            "receiver": storage_node,
            "ownership_epoch": epoch,
            "limit": 64,
        },
        control=True,
    )


def find_progress_owner(pipe_id, subscription_id, sweeps=60):
    """Probe each storage Node with owner=receiver=<that Node>. Only the
    current progress owner accepts the drive; a stale epoch or wrong owner
    is fenced, so a 200 is an authoritative answer. The last statuses each
    Node returned ride the AssertionError: 409s mean placement never moved,
    5xx mean the owner answered but the drive stayed degraded, and 0s mean
    the Node was unreachable — three different root causes."""
    seen = {}
    for _ in range(sweeps):
        for index, node in enumerate(NODES):
            statuses = seen.setdefault(SERVICES[index], [])
            for epoch in range(1, 21):
                status, body = drive_probe(
                    node, pipe_id, subscription_id, SERVICES[index], epoch
                )
                statuses.append((epoch, status))
                del statuses[:-8]
                if status == 200:
                    return SERVICES[index], epoch
        time.sleep(1)
    raise AssertionError({"no Node accepted the Pipe drive": seen})


def main():
    subprocess.run([*COMPOSE, "down", "-v"], check=True)
    subprocess.run([*COMPOSE, "up", "-d", "--build"], check=True)
    try:
        wait_for_fabric()

        suffix = uuid.uuid4().hex[:8]
        domain, source, output = (
            f"pipe{suffix}",
            f"pipe{suffix}.source",
            f"pipe{suffix}.forwarded",
        )
        writer, subscription, pipe = (
            f"writer{suffix}",
            f"{domain}.forward",
            f"{domain}.copy",
        )
        results = wcl_retry(
            NODES[0],
            f"CREATE DOMAIN {domain}; "
            f"CREATE FEED {source}; "
            f"CREATE FEED {output}; "
            f"CREATE WRITER {writer} TO {source}; "
            f"OPEN WRITER SESSION {writer}; "
            f"CREATE SUBSCRIPTION {subscription} FROM {source} START AT BEGINNING; "
            f"CREATE PIPE {pipe} FROM SUBSCRIPTION {subscription} TO FEED {output};",
        )
        writer_epoch = results[4]["session_epoch"]
        described = wcl(NODES[1], f"DESCRIBE PIPE {pipe};")[0]
        pipe_id = described["pipe_id"]
        subscription_id = described["subscription_id"]

        # No member ever joins the Subscription: the driver must bootstrap
        # the declared-start frontier itself, then forward through the
        # effect journal on every supervisor tick.
        sent = append_batch(NODES[1], writer, writer_epoch, range(1, 6))
        forwarded = wait_forwarded(output, 5)
        by_key = {record["key_base64"]: record for record in forwarded}
        for record in sent:
            seen = by_key.get(record["key_base64"])
            if seen is None:
                raise AssertionError(f"forwarded output is missing {record}")
            for field in ("payload_base64", "event_time_ns", "metadata_base64"):
                if seen[field] != record[field]:
                    raise AssertionError(
                        {
                            "field": field,
                            "sent": record[field],
                            "forwarded": seen[field],
                        }
                    )

        # Ownership handoff: DRAIN the progress owner so the Control Plane
        # recovers ownership onto a surviving replica at a higher epoch.
        # Driving follows the placement automatically - the old owner is
        # fenced by the epoch, the new owner's supervisor picks the Pipe up.
        owner, epoch = find_progress_owner(pipe_id, subscription_id)
        wcl(NODES[0], f"DRAIN STORAGE NODE {owner};")
        new_owner, new_epoch = None, None
        deadline = time.time() + 120
        last_probe = None
        while time.time() < deadline:
            try:
                found = find_progress_owner(pipe_id, subscription_id, sweeps=10)
            except AssertionError as error:
                last_probe = error
            else:
                if found[0] != owner and found[1] > epoch:
                    new_owner, new_epoch = found
                    break
            time.sleep(2)
        if new_owner is None:
            raise AssertionError(
                {"drain did not move the Subscription progress owner off " + owner: last_probe}
            )

        # Feed range moves during the drain can briefly freeze appends;
        # retry inside the same deterministic request identities.
        append_batch(NODES[0], writer, writer_epoch, range(6, 9), retries=45)
        wait_forwarded(output, 8)

        wcl(NODES[0], f"UNDRAIN STORAGE NODE {owner};")

        # Range transitions: the Pipe's consumed Subscription frontier
        # translates through split and merge cutovers with the same frozen
        # lineage evidence Reader sessions use; forwarding must continue
        # without operator intervention and never skip or duplicate.
        status, split = post(
            NODES[1],
            "/v1/admin/ranges/split",
            {
                "request_id": str(uuid.uuid4()),
                "feed": source,
                "split_at": "80000000000000000000000000000000",
                "batch_size": 4,
            },
        )
        if status != 200 or split.get("status") != "activated":
            raise AssertionError({"split": split})
        if split.get("subscription_reconcile_failed"):
            raise AssertionError({"split reconcile": split})
        append_batch(NODES[0], writer, writer_epoch, range(9, 12), retries=45)
        wait_forwarded(output, 11)

        routes = wcl(NODES[0], f"INSPECT PLACEMENT FOR FEED {source};")[0][
            "range_map"
        ]["routes"]
        status, merged = post(
            NODES[2],
            "/v1/admin/ranges/merge",
            {
                "request_id": str(uuid.uuid4()),
                "feed": source,
                "left_range_id": routes[0]["range_id"],
                "right_range_id": routes[1]["range_id"],
            },
        )
        if status != 200 or merged.get("status") != "activated":
            raise AssertionError({"merge": merged})
        if merged.get("subscription_reconcile_failed"):
            raise AssertionError({"merge reconcile": merged})
        append_batch(NODES[1], writer, writer_epoch, range(12, 15), retries=45)
        forwarded = wait_forwarded(output, 14)
        if len({record["key_base64"] for record in forwarded}) != 14:
            raise AssertionError(
                "the Pipe lost or duplicated records across range transitions"
            )

        print(
            json.dumps(
                {
                    "status": "ok",
                    "forwarded": len(forwarded),
                    "owner_before_drain": owner,
                    "owner_after_drain": new_owner,
                    "epoch_after_drain": new_epoch,
                    "note": (
                        "the Pipe supervisor bootstrapped the frontier, "
                        "forwarded with payload/key/event-time/Metadata "
                        "intact, followed the progress owner after drain, and "
                        "kept forwarding exactly-once through a split/merge "
                        "cutover that translated the consumed Subscription "
                        "frontier onto the new range map"
                    ),
                },
                indent=2,
            )
        )
    finally:
        subprocess.run([*COMPOSE, "down", "-v"], check=True)


if __name__ == "__main__":
    main()
