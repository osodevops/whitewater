//! Runs the shared `sdk/spec/conformance/*.json` corpus against a live
//! Riverbed through the SDK-owned [`ConformanceRunner`] — the same
//! op/arg/expect contract every language SDK's conformance runner
//! implements (`sdk/spec/conformance/README.md`).
//!
//! Skips unless `WHITEWATER_SDK_NODES` names comma-separated base URLs;
//! `scripts/test-sdk-conformance.py` sets it against the disposable
//! verification topology. Every fixture's `requires` ops are checked
//! against the runner's dispatch table — an unmapped op reports
//! `unsupported` explicitly rather than skipping silently.

use std::path::PathBuf;
use std::sync::Arc;

use whitewater::conformance::ConformanceRunner;
use whitewater::sdk::{Client, HttpTransport};

const ADMIN_KEY: &str = "whitewater-local-development-admin-key";

fn nodes() -> Option<Vec<String>> {
    let nodes = std::env::var("WHITEWATER_SDK_NODES").ok()?;
    let bases: Vec<String> = nodes
        .split(',')
        .map(str::trim)
        .filter(|base| !base.is_empty())
        .map(str::to_owned)
        .collect();
    (!bases.is_empty()).then_some(bases)
}

#[tokio::test]
async fn shared_conformance_corpus() {
    let Some(bases) = nodes() else {
        eprintln!("WHITEWATER_SDK_NODES unset — skipping conformance corpus");
        return;
    };
    let directory = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("sdk/spec/conformance");
    let client = Client::new(
        Arc::new(HttpTransport::new().expect("reqwest client")),
        bases,
        ADMIN_KEY,
    );
    let runner = ConformanceRunner::new(client);

    let reports = runner.run_directory(&directory).await;
    assert!(
        !reports.is_empty(),
        "no conformance fixtures found in {}",
        directory.display()
    );
    let mut failures = Vec::new();
    for report in &reports {
        match &report.failure {
            Some(failure) => failures.push(format!(
                "{}: step {} ({}){}: {}",
                report.name,
                failure.step,
                failure.op,
                failure
                    .status
                    .map(|status| format!(" [{status}]"))
                    .unwrap_or_default(),
                failure.message
            )),
            None => eprintln!("conformance ok  {}", report.name),
        }
    }
    assert!(
        failures.is_empty(),
        "conformance failures:\n{}",
        failures.join("\n")
    );
}
