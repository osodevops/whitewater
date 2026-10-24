//! Declares the three Pipe kinds against a live Riverbed — a forward
//! copy Pipe, an enrich lookup Pipe, and a rolling-window aggregation
//! Pipe — then drives their shared lifecycle surface
//! (describe/pause/resume).
//!
//! Run against a disposable three-Node Riverbed, e.g. the
//! `compose.verify.yml` development topology:
//!
//! ```sh
//! WHITEWATER_ENDPOINTS="http://localhost:7371,http://localhost:7372,http://localhost:7373" \
//! WHITEWATER_API_KEY="whitewater-local-development-admin-key" \
//! cargo run --example pipes
//! ```
//!
//! `WHITEWATER_ENDPOINTS` is the comma-separated Node base-URL list the
//! conformance suite uses; `WHITEWATER_API_KEY` falls back to the local
//! development key. Each run declares a fresh Domain so repeat runs
//! never collide.

use std::sync::Arc;

use whitewater::sdk::{
    Client, EnrichFreshness, EnrichMiss, Extractor, HttpTransport, PipeOperation, SdkError,
    WindowAggregation, WindowTime,
};

#[tokio::main]
async fn main() -> Result<(), SdkError> {
    let endpoints = std::env::var("WHITEWATER_ENDPOINTS")
        .unwrap_or_else(|_| "http://localhost:7371".to_owned());
    let bases: Vec<String> = endpoints
        .split(',')
        .map(|endpoint| endpoint.trim().to_owned())
        .filter(|endpoint| !endpoint.is_empty())
        .collect();
    let api_key = std::env::var("WHITEWATER_API_KEY")
        .unwrap_or_else(|_| "whitewater-local-development-admin-key".to_owned());
    let client = Client::new(Arc::new(HttpTransport::new()?), bases, api_key);
    client.check_compatible().await?;

    // A unique Domain per run keeps repeated runs collision-free.
    let run = &uuid::Uuid::new_v4().simple().to_string()[..8];
    let domain = format!("pipe{run}");
    let events = format!("{domain}.events");
    let relayed = format!("{domain}.relayed");
    let enriched = format!("{domain}.enriched");
    let quarantine = format!("{domain}.quarantine");
    let closed = format!("{domain}.closedpanes");
    let profiles = format!("{domain}.profiles");
    let stats = format!("{domain}.stats");

    // Fixture resources ride WCL — the same typed commands underneath.
    // The window store declares the non-unique `key`/`pane` named keys
    // the driver posts group-key and pane-index rows under.
    client
        .wcl(&format!(
            "CREATE DOMAIN {domain}; \
             CREATE FEED {events}; \
             CREATE FEED {relayed}; \
             CREATE FEED {enriched}; \
             CREATE FEED {quarantine}; \
             CREATE FEED {closed}; \
             DEFINE STATESTORE {profiles}; \
             DEFINE STATESTORE {stats} KEYS key, pane; \
             CREATE SUBSCRIPTION {domain}.relay FROM {events}; \
             CREATE SUBSCRIPTION {domain}.enriching FROM {events}; \
             CREATE SUBSCRIPTION {domain}.windowing FROM {events};"
        ))
        .await?;

    // Forward: copy every delivered record onto `relayed` unchanged.
    client
        .define_forward_pipe(
            &format!("{domain}.copy"),
            &format!("{domain}.relay"),
            &relayed,
        )
        .await?;

    // Enrich: resolve each record's Key against `profiles`; misses route
    // to the quarantine Feed instead of the output.
    client
        .define_enrich_pipe(
            &format!("{domain}.enrich"),
            &format!("{domain}.enriching"),
            &enriched,
            &profiles,
            PipeOperation::enrich(
                Extractor::record_key(),
                EnrichFreshness::Latest,
                None,
                EnrichMiss::Quarantine,
            ),
            Some(&quarantine),
        )
        .await?;

    // Window: rolling one-hour count per record Key into `stats`,
    // emitting closed panes onto `closed` once the watermark horizon
    // passes — `emit_feed` needs a lateness bound, so the watermark
    // matters.
    let hourly = PipeOperation::window(
        3_600_000_000_000,
        Extractor::record_key(),
        WindowAggregation::count(),
    )?
    .time(WindowTime::Event)
    .watermark_ns(60_000_000_000)
    .expire_ns(86_400_000_000_000);
    client
        .define_window_pipe(
            &format!("{domain}.hourly"),
            &format!("{domain}.windowing"),
            &stats,
            hourly,
            Some(&closed),
        )
        .await?;

    // Shared lifecycle: describe reports the catalog row; pause halts
    // drives cooperatively and resume restarts from the durable
    // frontier — both idempotent.
    for name in [
        format!("{domain}.copy"),
        format!("{domain}.enrich"),
        format!("{domain}.hourly"),
    ] {
        let pipe = client.pipe(&name);
        let described = pipe.describe().await?;
        println!(
            "{}: stage={} status={}",
            described.name, described.stage, described.status
        );
        pipe.pause().await?;
        pipe.resume().await?;
    }
    Ok(())
}
