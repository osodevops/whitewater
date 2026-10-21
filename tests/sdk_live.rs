//! Live acceptance for the typed SDK surface against a real Riverbed.
//!
//! Skips unless `WHITEWATER_SDK_NODES` names comma-separated Node base
//! URLs and `WHITEWATER_ADMIN_API_KEY` carries the admin key —
//! `scripts/test-sdk-rust.py` sets both against the disposable
//! verification topology. The assertions exercise the real wire through
//! `HttpTransport`: session binding, idempotent append, delivered-cursor
//! ack semantics, member claim/fetch/ack, and opaque-Cursor reads.

use std::collections::BTreeMap;
use std::sync::Arc;

use whitewater::sdk::{Client, HttpTransport, SdkError};

/// Drive an SDK call to settlement against a live Riverbed: `retryable()`
/// conditions (transient quorum/commit windows, election churn) retry the
/// identical request — every call site passes a stable identity so a replay
/// dedupes or returns the original outcome — while refusal/stale/fencing
/// errors panic at once so semantic assertions still observe real rejections.
macro_rules! settle {
    ($call:expr, $context:expr) => {{
        let mut attempts = 0_u32;
        loop {
            match { $call }.await {
                Ok(value) => break value,
                Err(error) if error.retryable() => {
                    attempts += 1;
                    if attempts > 120 {
                        panic!(
                            "{} stayed retryable through 120 attempts: {error:?}",
                            $context
                        );
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
                Err(error) => panic!("{}: {error:?}", $context),
            }
        }
    }};
}

fn live_client() -> Option<Client> {
    let nodes = std::env::var("WHITEWATER_SDK_NODES").ok()?;
    let bases: Vec<String> = nodes
        .split(',')
        .map(str::trim)
        .filter(|base| !base.is_empty())
        .map(str::to_owned)
        .collect();
    if bases.is_empty() {
        return None;
    }
    let key = std::env::var("WHITEWATER_ADMIN_API_KEY")
        .unwrap_or_else(|_| "whitewater-local-development-admin-key".to_owned());
    let transport = Arc::new(HttpTransport::new().expect("reqwest client"));
    Some(Client::new(transport, bases, key))
}

fn uniq() -> String {
    uuid::Uuid::new_v4().simple().to_string()[..8].to_owned()
}

#[tokio::test]
async fn sdk_roundtrip_against_a_live_riverbed() {
    let Some(client) = live_client() else {
        eprintln!("WHITEWATER_SDK_NODES unset — skipping live SDK acceptance");
        return;
    };
    settle!(client.check_compatible(), "protocol major");
    let limits = settle!(client.transport_limits(), "advertised transport bounds");
    assert_eq!(limits.max_frame_bytes, 64 * 1024 * 1024);
    assert!(limits.member_fetch_max_limit >= 1);

    let run = uniq();
    let (domain, feed) = (format!("sdk{run}"), format!("sdk{run}.events"));
    let (writer, reader, subscription) = (
        format!("wr{run}"),
        format!("{domain}.rd"),
        format!("{domain}.sub"),
    );
    let results = settle!(
        client.wcl(&format!(
            "CREATE DOMAIN {domain}; \
             CREATE FEED {feed}; \
             CREATE WRITER {writer} TO {feed}; \
             CREATE READER {reader} FROM {feed} START AT BEGINNING; \
             CREATE SUBSCRIPTION {subscription} FROM {feed} START AT BEGINNING;"
        )),
        "setup WCL"
    );
    assert_eq!(results.len(), 5, "every statement reported: {results:?}");

    // Writer: bind a session, append, then retry the identical request —
    // the durable record must dedupe rather than commit twice.
    let mut writer = settle!(client.writer(&writer), "writer session");
    assert!(writer.session_epoch() >= 1);
    let request_id = uuid::Uuid::new_v4();
    let outcome = settle!(
        writer.append(
            request_id,
            b"order-42",
            b"{\"total\":17}",
            1_700_000_000_000_000_001,
            BTreeMap::from([("source".to_owned(), b"sdk-live".to_vec())]),
        ),
        "append"
    );
    assert!(!outcome.deduplicated);
    assert!(!outcome.cursor.is_empty(), "opaque Cursor returned");
    let retry = settle!(
        writer.append(
            request_id,
            b"order-42",
            b"{\"total\":17}",
            1_700_000_000_000_000_001,
            BTreeMap::from([("source".to_owned(), b"sdk-live".to_vec())]),
        ),
        "identical retry settles"
    );
    assert!(retry.deduplicated, "same request_id dedupes server-side");
    assert_eq!(retry.message_id, outcome.message_id);
    settle!(writer.reopen(), "session reopens to a new epoch");

    // Reader: fetch the record, ack the page's delivered cursor.
    let mut reader = settle!(client.reader(&reader, 16), "reader open");
    let page = settle!(reader.fetch(16), "reader fetch");
    assert_eq!(page.records.len(), 1, "page: {page:?}");
    assert_eq!(page.records[0].key, b"order-42");
    assert_eq!(page.records[0].event_time_ns, 1_700_000_000_000_000_001);
    assert_eq!(
        page.records[0].metadata.get("source").map(Vec::as_slice),
        Some(b"sdk-live".as_slice())
    );
    settle!(reader.ack(), "ack delivered cursor");
    assert_eq!(
        reader.acknowledged_cursor(),
        page.delivered_cursor.as_deref(),
        "the page's delivered cursor is the durable restart point"
    );
    reader.close().await.expect("close");

    // Subscription member: claim work, fetch its page, ack progress.
    let member = settle!(client.member(&subscription), "member join");
    let work = settle!(member.claim(), "claim");
    assert!(work.lease_epoch >= 1);
    let work_page = settle!(member.fetch(&work, 16), "member fetch");
    assert_eq!(work_page.records.len(), 1, "member page: {work_page:?}");
    // The page reports its delivering claim's attempt number — the
    // durable per-work delivery counter retry budgets count against.
    assert_eq!(work_page.delivery_attempt, work.lease_epoch);
    settle!(member.ack(&work, &work_page), "member ack");
    // The ack consumes the lease: releasing it now must fence stale, not
    // silently succeed — the durable frontier already moved.
    assert_eq!(member.release(&work).await, Err(SdkError::LeaseStale));

    // Opaque-Cursor read straight off the Feed.
    let records = settle!(client.records(&feed, None, 10), "feed records");
    assert!(
        records.iter().any(|record| record.key == b"order-42"),
        "feed read surfaces the appended record"
    );

    // Pipe facade: declare a forward Pipe, then drive its lifecycle —
    // describe reports the live catalog row, pause/resume hold and
    // restart drives, and drop retires the declaration.
    let (out_feed, relay) = (format!("{domain}.out"), format!("{domain}.relay"));
    settle!(
        client.wcl(&format!(
            "CREATE FEED {out_feed}; \
             CREATE PIPE {relay} FROM SUBSCRIPTION {subscription} TO FEED {out_feed};"
        )),
        "pipe setup WCL"
    );
    let described = settle!(client.pipe(&relay).describe(), "pipe describe");
    assert_eq!(described.name, relay);
    assert_eq!(described.stage, "declared");
    assert_eq!(
        described.subscription_id,
        settle!(
            client.wcl(&format!("DESCRIBE SUBSCRIPTION {subscription};")),
            "subscription describe"
        )[0]
        .pointer("/data/subscription_id")
        .and_then(|v| v.as_str())
        .expect("subscription id")
        .parse::<uuid::Uuid>()
        .expect("uuid")
    );
    assert_eq!(described.operation["operation"], "forward");
    assert!(described.output_feed_id.is_some());
    assert!(settle!(client.pipes(), "list pipes")
        .iter()
        .any(|pipe| pipe.name == relay));
    settle!(client.pipe(&relay).pause(), "pause");
    let paused = settle!(client.pipe(&relay).describe(), "paused describe");
    assert_eq!(paused.stage, "paused");
    settle!(client.pipe(&relay).resume(), "resume");
    let resumed = settle!(client.pipe(&relay).describe(), "resumed describe");
    assert_eq!(resumed.stage, "declared");
    settle!(client.pipe(&relay).drop(), "drop");
    assert!(
        client.pipe(&relay).describe().await.is_err(),
        "dropped pipe refuses describe"
    );
}

#[tokio::test]
async fn state_store_index_and_temporary_facades_live() {
    use whitewater::sdk::{IndexBound, IndexQuery, TypedValue};

    let Some(client) = live_client() else {
        eprintln!("WHITEWATER_SDK_NODES unset — skipping live SDK acceptance");
        return;
    };
    let run = uniq();
    let (domain, feed, writer, store, index) = (
        format!("sdk{run}"),
        format!("sdk{run}.events"),
        format!("wr{run}"),
        format!("sdk{run}.kv"),
        format!("sdk{run}.bykey"),
    );
    settle!(
        client.wcl(&format!(
            "CREATE DOMAIN {domain}; \
             CREATE FEED {feed}; \
             CREATE WRITER {writer} TO {feed}; \
             OPEN WRITER SESSION {writer}; \
             DEFINE STATESTORE {store} KEYS region; \
             CREATE INDEX {index} ON {feed} FIELDS KEY, METADATA 'tenant';"
        )),
        "setup WCL"
    );

    // Manual StateStore: put -> strict lookup -> named get_by -> iterate
    // -> delete -> clear, with an identical mutation replay deduped.
    let store_api = client.state_store(&store);
    let mutation = uuid::Uuid::new_v4();
    let outcome = settle!(
        store_api.put_row(
            mutation,
            b"acct-1",
            b"{\"tier\":\"gold\"}",
            &[(
                "region".to_owned(),
                vec![TypedValue::Text("emea".to_owned())],
            )],
        ),
        "put_row"
    );
    assert!(!outcome.deduplicated);
    let replay = settle!(
        store_api.put_row(
            mutation,
            b"acct-1",
            b"{\"tier\":\"gold\"}",
            &[(
                "region".to_owned(),
                vec![TypedValue::Text("emea".to_owned())],
            )],
        ),
        "identical retry"
    );
    assert!(replay.deduplicated, "same mutation_id dedupes");

    let lookup = settle!(store_api.lookup(b"acct-1", true), "lookup");
    assert_eq!(
        lookup.row.as_ref().map(|row| row.payload.as_slice()),
        Some(b"{\"tier\":\"gold\"}".as_slice())
    );
    let page = settle!(
        store_api.get_by(
            "region",
            &[TypedValue::Text("emea".to_owned())],
            Some(10),
            false,
        ),
        "get_by"
    );
    assert_eq!(page.rows.len(), 1);
    let walk = settle!(store_api.iterate(None, Some(10)), "iterate");
    assert_eq!(walk.rows.len(), 1);
    assert_eq!(walk.next_key, None);
    let delete_mutation = uuid::Uuid::new_v4();
    settle!(
        store_api.delete_row(delete_mutation, b"acct-1"),
        "delete_row"
    );
    assert!(
        settle!(store_api.lookup(b"acct-1", true), "lookup after delete")
            .row
            .is_none()
    );
    let clear_mutation = uuid::Uuid::new_v4();
    settle!(store_api.clear(clear_mutation), "clear");

    // Writer append lands in retained history, then REBUILD INDEX runs
    // the shadow build; bounded-poll the query until activation posts
    // the row (builds are asynchronous by design).
    let writer_api = settle!(client.writer(&writer), "writer session");
    let index_append_id = uuid::Uuid::new_v4();
    settle!(
        writer_api.append(
            index_append_id,
            b"order-7",
            b"created",
            1_700_000_000_000_000_010,
            BTreeMap::from([("tenant".to_owned(), b"acme".to_vec())]),
        ),
        "append"
    );
    settle!(
        client.wcl(&format!("REBUILD INDEX {index};")),
        "rebuild index"
    );

    // The shadow build activates asynchronously — wait on the catalog
    // lifecycle (`stage=active`) rather than racing query errors.
    let mut activated = false;
    for _ in 0..240 {
        if let Ok(results) = client.wcl(&format!("DESCRIBE INDEX {index};")).await {
            let stage = results
                .first()
                .and_then(|result| result.get("data"))
                .and_then(|data| data.get("stage"));
            if stage.map(|value| value.get("active").is_some()) == Some(true) {
                activated = true;
                break;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    assert!(activated, "index build activated within the bounded wait");

    // `strict` bounded-waits for the serving checkpoint to reach the
    // activation frontier — the posting must be visible now.
    let page = settle!(
        client.query_index(
            &index,
            IndexQuery::Exact(vec![
                TypedValue::Bytes(b"order-7".to_vec()),
                // METADATA extractors post raw bytes — never Text.
                TypedValue::Bytes(b"acme".to_vec()),
            ]),
            Some(10),
            true,
        ),
        "index query after activation"
    );
    assert_eq!(page.rows.len(), 1, "exact posting: {page:?}");

    // Temporary fetch: bounded read off the same Feed.
    let page = settle!(
        client.temporary_fetch(
            &feed,
            whitewater::sdk::TemporaryFetch {
                limit: 10,
                wait_ms: Some(500),
                ..whitewater::sdk::TemporaryFetch::default()
            },
        ),
        "temporary fetch"
    );
    assert!(page.records.iter().any(|record| record.key == b"order-7"));

    // Range mode wire shape end-to-end (empty results are valid evidence).
    let _ = settle!(
        client.query_index(
            &index,
            IndexQuery::Range {
                leading: vec![],
                lower: IndexBound {
                    value: TypedValue::Bytes(vec![]),
                    inclusive: true,
                },
                upper: IndexBound {
                    value: TypedValue::Bytes(vec![0xff]),
                    inclusive: true,
                },
            },
            Some(10),
            false,
        ),
        "range query"
    );

    // Index demand attribution: every served query lands on the answering
    // replica's Node-local row — merge across the endpoint pool since the
    // query could have been served by any of them.
    let mut queries = 0_u64;
    let mut rows_returned = 0_u64;
    for _ in 0..3 {
        let reply = settle!(
            client.wire("GET", "/v1/admin/demand/indexes", None),
            "index demand telemetry"
        );
        for row in reply.body["indexes"].as_array().into_iter().flatten() {
            if row["index"] == index {
                let telemetry = &row["telemetry"];
                queries += telemetry["queries"].as_u64().unwrap_or(0);
                rows_returned += telemetry["rows_returned"].as_u64().unwrap_or(0);
            }
        }
    }
    assert_eq!(queries, 2, "served index queries attributed");
    // The exact lookup returned one posting; the unbounded range scan
    // covers it too, so both queries returned one row each.
    assert_eq!(rows_returned, 2, "returned postings attributed");
}

#[tokio::test]
async fn stale_member_epoch_fences_as_typed_errors() {
    let Some(client) = live_client() else {
        eprintln!("WHITEWATER_SDK_NODES unset — skipping live SDK acceptance");
        return;
    };
    let run = uniq();
    let (domain, feed, writer, subscription) = (
        format!("sdk{run}"),
        format!("sdk{run}.events"),
        format!("wr{run}"),
        format!("sdk{run}.sub"),
    );
    settle!(
        client.wcl(&format!(
            "CREATE DOMAIN {domain}; \
             CREATE FEED {feed}; \
             CREATE WRITER {writer} TO {feed}; \
             OPEN WRITER SESSION {writer}; \
             CREATE SUBSCRIPTION {subscription} FROM {feed} START AT BEGINNING;"
        )),
        "setup WCL"
    );

    // A member whose epoch was never joined (or went stale) must be
    // fenced by the wire contract, not silently admitted.
    let stale = whitewater::sdk::Member::for_testing(
        client.clone(),
        &subscription,
        uuid::Uuid::new_v4(),
        0,
    );
    let error = stale.claim().await.unwrap_err();
    assert!(
        matches!(
            error,
            SdkError::StaleEpoch | SdkError::Conflict(_) | SdkError::Refused(_)
        ),
        "unjoined member claim must fence, got {error:?}"
    );
}
