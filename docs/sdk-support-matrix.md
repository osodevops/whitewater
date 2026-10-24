# SDK support matrix

One capability set, six SDKs, measured parity. A capability is listed as
supported only where the shared conformance corpus or a named acceptance
script exercises it on a live three-Node Riverbed. `scripts/test-sdk-languages.py`
is the gate: it runs each SDK's runner against one isolated Riverbed and
fails loudly (`absent`, `no-toolchain`, `failed`) instead of silently
skipping a language.

## Packages and runtimes

| SDK | Package | Version | Runtime floor | Repository |
|---|---|---|---|---|
| Rust | `whitewater` (crate, in this repo) | 0.1.0 | Rust 1.90 | `whitewater` |
| Go | `github.com/osodevops/whitewater-go` | go.mod, unreleased | Go 1.22 | `whitewater-go` |
| Python | `whitewater-client` (PyPI name) | 0.1.0.dev0 | Python ≥ 3.10 | `whitewater-python` |
| Java | `dev.whitewater:whitewater-client` | 0.1.0-SNAPSHOT | Java 17 | `whitewater-java` |
| Node.js/TypeScript | `whitewater-client` (npm name) | 0.1.0 | Node ≥ 20, ESM | `whitewater-node` |
| C#/.NET | `Whitewater.Client` (NuGet id) | 0.1.0 | .NET 8.0 | `whitewater-dotnet` |

All six are pre-release packages consumed from source checkouts today; no
registry publish workflow exists yet — treat every coordinate above as the
declaration of intent for the release pipeline, not a published artifact.

## Capability parity

Every row passed all six runners through `scripts/test-sdk-languages.py`
against a live three-Node Riverbed (22/22 scenarios each, including the four
`zz-chaos-*` fixtures). There are no per-language gaps to mark unsupported
on this surface.

| Capability | Evidence | Rust | Go | Python | Java | Node | .NET |
|---|---|---|---|---|---|---|---|
| Typed admin commands + WCL | `admin-wcl-resources` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Writer session append + batch | `writer-idempotent-append` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Request-id dedup on retry | `writer-idempotent-append` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Ambiguous-commit reporting | `ambiguous-commit` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Writer epoch/sequence fencing | `writer-sequence-fencing` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Reader open/fetch/ack/close/resume | `reader-fetch-ack-resume` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Feed `records` + `temporary_fetch` replay | `feed-access` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Nanosecond `event_time_ns`/`ingest_time_ns` (decimal strings on the wire; Node `bigint`) | `nanoseconds-and-keys` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Record compression (optional wire codec) | `optional-compression` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Auth refusal + API-key surface | `auth-refused` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Health + capability check | `health-capabilities` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Subscription member join/claim/renew/release/transfer | `subscription-member-lease`, `subscription-member-drain` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Subscription seek (beginning/timestamp/cursor) | `subscription-seek` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| StateStore mutate/lookup/get_by/iterate/clear | `statestore-manual-lifecycle` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Declared Index define/rebuild/exact/prefix/range | `index-query` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Forward Pipe | `pipe-forward` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Enrich Pipe (Reader→StateStore→Writer + miss→quarantine) | `enrich-pipe` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Window Pipe (watermark/grace/max_late/expire/limits) | `window-pipe` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Pipe declaration surface (`define_*_pipe`) | SDK unit suites + fixture legs | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Supervised consumer + value-level store lookups | per-SDK consumer unit suites | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Demand telemetry + hot-Key reports | per-SDK operations unit suites | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Node restart / owner loss / two-Node outage / partition | `zz-chaos-restart`, `zz-chaos-owner-loss`, `zz-chaos-two-node-outage`, `zz-chaos-partition` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| In-memory unit-test double (`testing` module) | per-SDK double unit suites | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |

## Explicit non-goals on this surface

- No SDK exposes partition counts, Active Ranges, owners, replicas, or
  co-partitioning — their absence is the product contract, not a gap.
- A browser JavaScript client is not the Node SDK; no browser artifact is
  published or supported.
- No SDK claims 12/24-Node live capacity; the conformance topology is the
  supported three-Node development Riverbed.
- Guides list per-scenario runnable evidence in
  [guides/README.md](guides/README.md); operations outside the table above
  fail explicitly rather than silently degrading.
