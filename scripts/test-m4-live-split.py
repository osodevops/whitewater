import base64, json, os, sys, time, urllib.error, urllib.parse, urllib.request, uuid

BASE_PORT = int(os.environ.get("WHITEWATER_TEST_BASE_PORT", "7071"))
ENDPOINTS = [f"http://localhost:{BASE_PORT + offset}" for offset in range(3)]
KEY = "whitewater-local-development-admin-key"

def post(endpoint, path, body):
    request = urllib.request.Request(endpoint + path, data=json.dumps(body).encode(), headers={"authorization": f"Bearer {KEY}", "content-type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=60) as response: return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error: return error.code, json.loads(error.read() or b"{}")

def main():
    suffix = f"{int(time.time())}{uuid.uuid4().hex[:6]}"
    space, feed, writer, reader = f"m4{suffix}", f"m4{suffix}.events", f"writer{suffix}", f"reader{suffix}"
    second_reader = f"secondreader{suffix}"
    status, setup = post(ENDPOINTS[0], "/v1/admin/wcl", {"script": f"CREATE DOMAIN {space}; CREATE FEED {feed}; CREATE WRITER {writer} TO {feed}; OPEN WRITER SESSION {writer}; CREATE READER {reader} FROM {feed} START AT BEGINNING; CREATE READER {second_reader} FROM {feed} START AT BEGINNING;"})
    if status != 200: raise AssertionError(setup)
    epoch = setup["results"][3]["data"]["session_epoch"]
    def append(index, endpoint):
        return post(endpoint, "/v1/writers/append", {"request_id": str(uuid.uuid4()), "writer": writer, "session_epoch": epoch, "event_time_ns": str(index), "key_base64": base64.b64encode(f"key-{index}".encode()).decode(), "payload_base64": base64.b64encode(f"value-{index}".encode()).decode(), "metadata_base64": {}})
    cursors = []
    for index in range(1, 9):
        status, result = append(index, ENDPOINTS[index % 3])
        if status != 200: raise AssertionError(result)
        cursors.append(result["cursor"])
    status, opened = post(ENDPOINTS[0], "/v1/readers/open", {"request_id": str(uuid.uuid4()), "reader": reader, "capacity": 2})
    if status != 200: raise AssertionError(opened)
    reader_epoch = opened["session_epoch"]
    status, first = post(ENDPOINTS[1], "/v1/readers/fetch", {"request_id": str(uuid.uuid4()), "reader": reader, "session_epoch": reader_epoch})
    if status != 200 or [row["cursor"] for row in first["records"]] != cursors[:2]: raise AssertionError(first)
    status, ack = post(ENDPOINTS[0], "/v1/readers/ack", {"request_id": str(uuid.uuid4()), "reader": reader, "session_epoch": reader_epoch, "cursor": first["delivered_cursor"]})
    if status != 200: raise AssertionError(ack)
    status, unacknowledged = post(ENDPOINTS[2], "/v1/readers/fetch", {"request_id": str(uuid.uuid4()), "reader": reader, "session_epoch": reader_epoch})
    if status != 200 or [row["cursor"] for row in unacknowledged["records"]] != cursors[2:4]: raise AssertionError(unacknowledged)
    status, other_open = post(ENDPOINTS[2], "/v1/readers/open", {"request_id": str(uuid.uuid4()), "reader": second_reader, "capacity": 1})
    if status != 200: raise AssertionError(other_open)
    status, other_first = post(ENDPOINTS[1], "/v1/readers/fetch", {"request_id": str(uuid.uuid4()), "reader": second_reader, "session_epoch": other_open["session_epoch"]})
    if status != 200 or [row["cursor"] for row in other_first["records"]] != cursors[:1]: raise AssertionError(other_first)
    status, other_ack = post(ENDPOINTS[0], "/v1/readers/ack", {"request_id": str(uuid.uuid4()), "reader": second_reader, "session_epoch": other_open["session_epoch"], "cursor": other_first["delivered_cursor"]})
    if status != 200: raise AssertionError(other_ack)
    status, split = post(ENDPOINTS[1], "/v1/admin/ranges/split", {"request_id": str(uuid.uuid4()), "feed": feed, "split_at": "80000000000000000000000000000000", "batch_size": 2})
    if status != 200 or split.get("status") != "activated": raise AssertionError(split)
    status, placement = post(ENDPOINTS[2], "/v1/admin/wcl", {"script": f"INSPECT PLACEMENT FOR FEED {feed};"})
    data = placement["results"][0]["data"]
    routes = data["range_map"]["routes"]
    owners = {assignment["owner"] for assignment in data["range_assignments"]}
    if len(routes) != 2 or len(owners) != 2: raise AssertionError(data)
    status, stale = post(ENDPOINTS[0], "/v1/readers/ack", {"request_id": str(uuid.uuid4()), "reader": reader, "session_epoch": reader_epoch, "cursor": unacknowledged["delivered_cursor"]})
    if status == 200: raise AssertionError(f"split accepted stale Reader acknowledgement: {stale}")
    status, reopened = post(ENDPOINTS[2], "/v1/readers/open", {"request_id": str(uuid.uuid4()), "reader": reader, "capacity": 16})
    if status != 200 or reopened["session_epoch"] <= reader_epoch: raise AssertionError(reopened)
    split_epoch = reopened["session_epoch"]
    status, resumed = post(ENDPOINTS[1], "/v1/readers/fetch", {"request_id": str(uuid.uuid4()), "reader": reader, "session_epoch": split_epoch, "limit": 16})
    if status != 200 or [row["cursor"] for row in resumed["records"]] != cursors[2:8]: raise AssertionError(f"split skipped or reordered Reader history: {resumed}")
    status, second_open = post(ENDPOINTS[0], "/v1/readers/open", {"request_id": str(uuid.uuid4()), "reader": second_reader, "capacity": 16})
    if status != 200 or second_open["session_epoch"] <= other_open["session_epoch"]: raise AssertionError(second_open)
    status, second_resumed = post(ENDPOINTS[2], "/v1/readers/fetch", {"request_id": str(uuid.uuid4()), "reader": second_reader, "session_epoch": second_open["session_epoch"], "limit": 16})
    if status != 200 or [row["cursor"] for row in second_resumed["records"]] != cursors[1:8]: raise AssertionError(f"independent Reader skipped split history: {second_resumed}")
    status, ack = post(ENDPOINTS[0], "/v1/readers/ack", {"request_id": str(uuid.uuid4()), "reader": reader, "session_epoch": split_epoch, "cursor": resumed["delivered_cursor"]})
    if status != 200: raise AssertionError(ack)
    for index in range(9, 17):
        status, result = append(index, ENDPOINTS[index % 3])
        if status != 200: raise AssertionError(result)
        cursors.append(result["cursor"])
    status, pending = post(ENDPOINTS[2], "/v1/readers/fetch", {"request_id": str(uuid.uuid4()), "reader": reader, "session_epoch": split_epoch, "limit": 4})
    if status != 200 or [row["cursor"] for row in pending["records"]] != cursors[8:12]: raise AssertionError(pending)
    status, merged = post(ENDPOINTS[1], "/v1/admin/ranges/merge", {"request_id": str(uuid.uuid4()), "feed": feed, "left_range_id": routes[0]["range_id"], "right_range_id": routes[1]["range_id"]})
    if status == 200 and merged.get("status") == "activated":
        status, stale = post(ENDPOINTS[0], "/v1/readers/ack", {"request_id": str(uuid.uuid4()), "reader": reader, "session_epoch": split_epoch, "cursor": pending["delivered_cursor"]})
        if status == 200: raise AssertionError(f"merge accepted stale Reader acknowledgement: {stale}")
        merge_status = "activated"
    elif status in (409, 503) and "Writer sequence" in merged.get("error", ""):
        status, unchanged = post(ENDPOINTS[0], "/v1/admin/wcl", {"script": f"INSPECT PLACEMENT FOR FEED {feed};"})
        if status != 200 or len(unchanged["results"][0]["data"]["range_map"]["routes"]) != 2: raise AssertionError(f"failed merge changed placement: {unchanged}")
        merge_status = "refused_overlapping_writer_sequences"
    else:
        raise AssertionError(merged)
    status, reopened = post(ENDPOINTS[2], "/v1/readers/open", {"request_id": str(uuid.uuid4()), "reader": reader, "capacity": 16})
    if status != 200 or reopened["session_epoch"] <= split_epoch: raise AssertionError(reopened)
    status, after_merge = post(ENDPOINTS[1], "/v1/readers/fetch", {"request_id": str(uuid.uuid4()), "reader": reader, "session_epoch": reopened["session_epoch"], "limit": 16})
    if status != 200 or [row["cursor"] for row in after_merge["records"]] != cursors[8:]: raise AssertionError(f"merge failure or cutover skipped unacknowledged Reader history: {after_merge}")
    if merge_status != "activated":
        status, restored_append = append(17, ENDPOINTS[2])
        if status != 200: raise AssertionError(f"failed merge left a source generation frozen: {restored_append}")
        cursors.append(restored_append["cursor"])
    encoded = urllib.parse.quote(feed, safe="")
    with urllib.request.urlopen(urllib.request.Request(f"{ENDPOINTS[0]}/v1/feeds/records?feed={encoded}&limit=100", headers={"authorization": f"Bearer {KEY}"}), timeout=30) as response:
        records = json.loads(response.read())
    if [record["cursor"] for record in records] != cursors: raise AssertionError(f"split/merge lost immutable Feed history: {records}")
    merge_feed, merge_reader, bootstrap = f"{space}.merge", f"mergereader{suffix}", f"bootstrap{suffix}"
    status, prepared = post(ENDPOINTS[0], "/v1/admin/wcl", {"script": f"CREATE FEED {merge_feed}; CREATE WRITER {bootstrap} TO {merge_feed}; OPEN WRITER SESSION {bootstrap}; CREATE READER {merge_reader} FROM {merge_feed} START AT BEGINNING;"})
    if status != 200: raise AssertionError(prepared)
    bootstrap_epoch = prepared["results"][2]["data"]["session_epoch"]
    def merge_append(name, session_epoch, index):
        return post(ENDPOINTS[index % 3], "/v1/writers/append", {"request_id": str(uuid.uuid4()), "writer": name, "session_epoch": session_epoch, "event_time_ns": str(index), "key_base64": base64.b64encode(f"merge-key-{index}".encode()).decode(), "payload_base64": base64.b64encode(f"merge-value-{index}".encode()).decode(), "metadata_base64": {}})
    status, first_record = merge_append(bootstrap, bootstrap_epoch, 1)
    if status != 200: raise AssertionError(first_record)
    merge_cursors = [first_record["cursor"]]
    status, second_split = post(ENDPOINTS[1], "/v1/admin/ranges/split", {"request_id": str(uuid.uuid4()), "feed": merge_feed, "split_at": "80000000000000000000000000000000", "batch_size": 2})
    if status != 200 or second_split.get("status") != "activated": raise AssertionError(second_split)
    writers = []
    for index in range(2, 6):
        name = f"mergewriter{index}{suffix}"
        status, opened_writer = post(ENDPOINTS[0], "/v1/admin/wcl", {"script": f"CREATE WRITER {name} TO {merge_feed}; OPEN WRITER SESSION {name};"})
        if status != 200: raise AssertionError(opened_writer)
        writer_epoch = opened_writer["results"][1]["data"]["session_epoch"]
        writers.append((name, writer_epoch))
        status, appended = merge_append(name, writer_epoch, index)
        if status != 200: raise AssertionError(appended)
        merge_cursors.append(appended["cursor"])
    status, merge_open = post(ENDPOINTS[2], "/v1/readers/open", {"request_id": str(uuid.uuid4()), "reader": merge_reader, "capacity": 2})
    if status != 200: raise AssertionError(merge_open)
    epoch_before_merge = merge_open["session_epoch"]
    status, merge_first = post(ENDPOINTS[0], "/v1/readers/fetch", {"request_id": str(uuid.uuid4()), "reader": merge_reader, "session_epoch": epoch_before_merge})
    if status != 200 or [row["cursor"] for row in merge_first["records"]] != merge_cursors[:2]: raise AssertionError(merge_first)
    status, merge_ack = post(ENDPOINTS[1], "/v1/readers/ack", {"request_id": str(uuid.uuid4()), "reader": merge_reader, "session_epoch": epoch_before_merge, "cursor": merge_first["delivered_cursor"]})
    if status != 200: raise AssertionError(merge_ack)
    status, merge_pending = post(ENDPOINTS[2], "/v1/readers/fetch", {"request_id": str(uuid.uuid4()), "reader": merge_reader, "session_epoch": epoch_before_merge})
    if status != 200 or [row["cursor"] for row in merge_pending["records"]] != merge_cursors[2:4]: raise AssertionError(merge_pending)
    status, placement = post(ENDPOINTS[0], "/v1/admin/wcl", {"script": f"INSPECT PLACEMENT FOR FEED {merge_feed};"})
    if status != 200: raise AssertionError(placement)
    merge_routes = placement["results"][0]["data"]["range_map"]["routes"]
    status, merged_cleanly = post(ENDPOINTS[1], "/v1/admin/ranges/merge", {"request_id": str(uuid.uuid4()), "feed": merge_feed, "left_range_id": merge_routes[0]["range_id"], "right_range_id": merge_routes[1]["range_id"]})
    if status != 200 or merged_cleanly.get("status") != "activated": raise AssertionError(f"disjoint Writer identities could not merge: {merged_cleanly}")
    status, stale = post(ENDPOINTS[2], "/v1/readers/ack", {"request_id": str(uuid.uuid4()), "reader": merge_reader, "session_epoch": epoch_before_merge, "cursor": merge_pending["delivered_cursor"]})
    if status == 200: raise AssertionError(f"merge accepted stale Reader acknowledgement: {stale}")
    status, merge_reopen = post(ENDPOINTS[0], "/v1/readers/open", {"request_id": str(uuid.uuid4()), "reader": merge_reader, "capacity": 8})
    if status != 200 or merge_reopen["session_epoch"] <= epoch_before_merge: raise AssertionError(merge_reopen)
    status, merge_resumed = post(ENDPOINTS[2], "/v1/readers/fetch", {"request_id": str(uuid.uuid4()), "reader": merge_reader, "session_epoch": merge_reopen["session_epoch"], "limit": 8})
    if status != 200 or [row["cursor"] for row in merge_resumed["records"]] != merge_cursors[2:]: raise AssertionError(f"merge skipped Reader history: {merge_resumed}")
    status, post_merge_append = merge_append(bootstrap, bootstrap_epoch, 6)
    if status != 200: raise AssertionError(post_merge_append)
    print(json.dumps({"status": "ok", "feed": feed, "ranges": len(routes), "owners": sorted(owners), "records": len(records), "reader_split_replayed": len(resumed["records"]), "reader_after_merge_attempt": len(after_merge["records"]), "merge_status": merge_status, "reader_merge_replayed": len(merge_resumed["records"])}, indent=2))

if __name__ == "__main__":
    try: main()
    except Exception as error: print(f"M4 live split failed: {error}", file=sys.stderr); sys.exit(1)
