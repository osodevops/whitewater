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
    client.check_compatible().await.expect("protocol major");

    let run = uniq();
    let (domain, feed) = (format!("sdk{run}"), format!("sdk{run}.events"));
    let (writer, reader, subscription) = (
        format!("wr{run}"),
        format!("{domain}.rd"),
        format!("{domain}.sub"),
    );
    let results = client
        .wcl(&format!(
            "CREATE DOMAIN {domain}; \
             CREATE FEED {feed}; \
             CREATE WRITER {writer} TO {feed}; \
             CREATE READER {reader} FROM {feed} START AT BEGINNING; \
             CREATE SUBSCRIPTION {subscription} FROM {feed} START AT BEGINNING;"
        ))
        .await
        .expect("setup WCL");
    assert_eq!(results.len(), 5, "every statement reported: {results:?}");

    // Writer: bind a session, append, then retry the identical request —
    // the durable record must dedupe rather than commit twice.
    let mut writer = client.writer(&writer).await.expect("writer session");
    assert!(writer.session_epoch() >= 1);
    let request_id = uuid::Uuid::new_v4();
    let outcome = writer
        .append(
            request_id,
            b"order-42",
            b"{\"total\":17}",
            1_700_000_000_000_000_001,
            BTreeMap::from([("source".to_owned(), b"sdk-live".to_vec())]),
        )
        .await
        .expect("append");
    assert!(!outcome.deduplicated);
    assert!(!outcome.cursor.is_empty(), "opaque Cursor returned");
    let retry = writer
        .append(
            request_id,
            b"order-42",
            b"{\"total\":17}",
            1_700_000_000_000_000_001,
            BTreeMap::from([("source".to_owned(), b"sdk-live".to_vec())]),
        )
        .await
        .expect("identical retry settles");
    assert!(retry.deduplicated, "same request_id dedupes server-side");
    assert_eq!(retry.message_id, outcome.message_id);
    writer
        .reopen()
        .await
        .expect("session reopens to a new epoch");

    // Reader: fetch the record, ack the page's delivered cursor.
    let mut reader = client.reader(&reader, 16).await.expect("reader open");
    let page = reader.fetch(16).await.expect("reader fetch");
    assert_eq!(page.records.len(), 1, "page: {page:?}");
    assert_eq!(page.records[0].key, b"order-42");
    assert_eq!(page.records[0].event_time_ns, 1_700_000_000_000_000_001);
    assert_eq!(
        page.records[0].metadata.get("source").map(Vec::as_slice),
        Some(b"sdk-live".as_slice())
    );
    reader.ack().await.expect("ack delivered cursor");
    assert_eq!(
        reader.acknowledged_cursor(),
        page.delivered_cursor.as_deref(),
        "the page's delivered cursor is the durable restart point"
    );
    reader.close().await.expect("close");

    // Subscription member: claim work, fetch its page, ack progress.
    let member = client.member(&subscription).await.expect("member join");
    let work = member.claim().await.expect("claim");
    assert!(work.lease_epoch >= 1);
    let work_page = member.fetch(&work, 16).await.expect("member fetch");
    assert_eq!(work_page.records.len(), 1, "member page: {work_page:?}");
    // The page reports its delivering claim's attempt number — the
    // durable per-work delivery counter retry budgets count against.
    assert_eq!(work_page.delivery_attempt, work.lease_epoch);
    member.ack(&work, &work_page).await.expect("member ack");
    // The ack consumes the lease: releasing it now must fence stale, not
    // silently succeed — the durable frontier already moved.
    assert_eq!(member.release(&work).await, Err(SdkError::LeaseStale));

    // Opaque-Cursor read straight off the Feed.
    let records = client.records(&feed, None, 10).await.expect("feed records");
    assert!(
        records.iter().any(|record| record.key == b"order-42"),
        "feed read surfaces the appended record"
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
    client
        .wcl(&format!(
            "CREATE DOMAIN {domain}; \
             CREATE FEED {feed}; \
             CREATE WRITER {writer} TO {feed}; \
             OPEN WRITER SESSION {writer}; \
             DEFINE STATESTORE {store} KEYS region; \
             CREATE INDEX {index} ON {feed} FIELDS KEY, METADATA 'tenant';"
        ))
        .await
        .expect("setup WCL");

    // Manual StateStore: put -> strict lookup -> named get_by -> iterate
    // -> delete -> clear, with an identical mutation replay deduped.
    let store_api = client.state_store(&store);
    let mutation = uuid::Uuid::new_v4();
    let outcome = store_api
        .put_row(
            mutation,
            b"acct-1",
            b"{\"tier\":\"gold\"}",
            &[(
                "region".to_owned(),
                vec![TypedValue::Text("emea".to_owned())],
            )],
        )
        .await
        .expect("put_row");
    assert!(!outcome.deduplicated);
    let replay = store_api
        .put_row(
            mutation,
            b"acct-1",
            b"{\"tier\":\"gold\"}",
            &[(
                "region".to_owned(),
                vec![TypedValue::Text("emea".to_owned())],
            )],
        )
        .await
        .expect("identical retry");
    assert!(replay.deduplicated, "same mutation_id dedupes");

    let lookup = store_api.lookup(b"acct-1", true).await.expect("lookup");
    assert_eq!(
        lookup.row.as_ref().map(|row| row.payload.as_slice()),
        Some(b"{\"tier\":\"gold\"}".as_slice())
    );
    let page = store_api
        .get_by(
            "region",
            &[TypedValue::Text("emea".to_owned())],
            Some(10),
            false,
        )
        .await
        .expect("get_by");
    assert_eq!(page.rows.len(), 1);
    let walk = store_api.iterate(None, Some(10)).await.expect("iterate");
    assert_eq!(walk.rows.len(), 1);
    assert_eq!(walk.next_key, None);
    store_api
        .delete_row(uuid::Uuid::new_v4(), b"acct-1")
        .await
        .expect("delete_row");
    assert!(store_api
        .lookup(b"acct-1", true)
        .await
        .expect("lookup after delete")
        .row
        .is_none());
    store_api.clear(uuid::Uuid::new_v4()).await.expect("clear");

    // Writer append lands in retained history, then REBUILD INDEX runs
    // the shadow build; bounded-poll the query until activation posts
    // the row (builds are asynchronous by design).
    let writer_api = client.writer(&writer).await.expect("writer session");
    writer_api
        .append(
            uuid::Uuid::new_v4(),
            b"order-7",
            b"created",
            1_700_000_000_000_000_010,
            BTreeMap::from([("tenant".to_owned(), b"acme".to_vec())]),
        )
        .await
        .expect("append");
    client
        .wcl(&format!("REBUILD INDEX {index};"))
        .await
        .expect("rebuild index");

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
    let page = client
        .query_index(
            &index,
            IndexQuery::Exact(vec![
                TypedValue::Bytes(b"order-7".to_vec()),
                // METADATA extractors post raw bytes — never Text.
                TypedValue::Bytes(b"acme".to_vec()),
            ]),
            Some(10),
            true,
        )
        .await
        .expect("index query after activation");
    assert_eq!(page.rows.len(), 1, "exact posting: {page:?}");

    // Temporary fetch: bounded read off the same Feed.
    let page = client
        .temporary_fetch(
            &feed,
            whitewater::sdk::TemporaryFetch {
                limit: 10,
                wait_ms: Some(500),
                ..whitewater::sdk::TemporaryFetch::default()
            },
        )
        .await
        .expect("temporary fetch");
    assert!(page.records.iter().any(|record| record.key == b"order-7"));

    // Range mode wire shape end-to-end (empty results are valid evidence).
    let _ = client
        .query_index(
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
        )
        .await
        .expect("range query");
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
    client
        .wcl(&format!(
            "CREATE DOMAIN {domain}; \
             CREATE FEED {feed}; \
             CREATE WRITER {writer} TO {feed}; \
             OPEN WRITER SESSION {writer}; \
             CREATE SUBSCRIPTION {subscription} FROM {feed} START AT BEGINNING;"
        ))
        .await
        .expect("setup WCL");

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
