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
