# Whitewater SDK spec

The versioned contract every supported SDK implements. Owned by the
server repository because the server defines it — spec changes ride the
same PR as the feature they describe.

| Path | Purpose |
|---|---|
| `PROTOCOL.md` | Semantic contract: scalar encodings, error envelope, idempotency/ambiguity rules, ordering, delivery, capability negotiation. |
| `endpoints.json` | Machine-readable route/field/error table for every public operation. |
| `conformance/` | Named scenario fixtures + runner vocabulary. Every SDK executes these against a real three-Node Riverbed in CI. |
| `VERSION` | Single-line spec version (`1.0.0`); bump on every contract change. |

## How an SDK repo consumes this

1. Vendor the spec: git submodule at `spec/` or a pinned
   `sdk-spec-<version>.tar.gz` download in CI. Pin a concrete version —
   never `main`.
2. Declare `whitewater-protocol` major `1` and the pinned spec version in
   the package metadata and on `GET /health` capability checks.
3. Implement the runner contract in `conformance/README.md`: `{{uniq}}`,
   `{{uuid}}`, `{{uuid:name}}`, `bind`/`{{bind.field}}` substitution,
   `expect` operators. The runner reports `pass` / `fail` /
   `unsupported` per scenario; `unsupported` blocks a parity claim.
4. The shared fixtures assert semantics, not wire bytes — byte-level wire
   checks live in each SDK's unit tests against `endpoints.json`.

## Repo layout

Each supported language is its own repository under `osodevops`:

`whitewater-python` · `whitewater-java` · `whitewater-dotnet` ·
`whitewater-node` · `whitewater-go`

The Rust SDK remains the reference implementation inside this repository
(it already backs `wwctl`); it exercises the same conformance corpus from
`tests/`.

Independent per-SDK semver releases; the shared spec version is the only
cross-repo lockstep.
