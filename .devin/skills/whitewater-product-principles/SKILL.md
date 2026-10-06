---
name: whitewater-product-principles
description: Apply Whitewater's Kafka pain points and DevOps/developer experience goals to product, architecture, and implementation work
triggers:
  - user
  - model
---

# Whitewater product principles

Use this skill whenever planning, reviewing, designing, or implementing Whitewater behavior, APIs, operations, SDKs, documentation, or user experience.

## Read the sources of truth

Before proposing or changing behavior, read the relevant sections of:

1. `docs/kafka-pain-points.md` for the product problems Whitewater must solve.
2. `docs/kafka-successor-architecture.md` for architecture and correctness contracts.
3. `docs/why-whitewater.md` for product terminology and Kafka equivalents.
4. `docs/operational-experience.md` for day-two behavior and acceptance criteria.
5. `AGENTS.md` for current repository-wide engineering rules.

Do not rely on this skill's summary when a source document defines the behavior in more detail.

## Product objective

Whitewater is a powerful DevOps- and developer-friendly alternative to Kafka. Make partitions, balancing, recovery, schemas, retries, state, observability, and scaling internal implementation details rather than expertise every operator and application developer must acquire.

Merely hiding or renaming complexity is not sufficient. Whitewater must safely automate it, remove it, or expose it through a stable and explainable contract.

## Required evaluation for every change

Identify and state:

- The specific operator or developer pain the change addresses.
- Whether the change eliminates responsibility, safely automates it, or only moves it elsewhere.
- The stable public contract and safe default.
- Failure, degraded-mode, recovery, and rollback behavior.
- How users can understand what happened, why it happened, and what to do next.
- Resource, capacity, cost, security, compatibility, and disaster-recovery consequences.
- Unit tests plus the relevant integration, restart, fault, or acceptance evidence.

If a proposal does not reduce a documented pain, justify why it belongs in Whitewater. If it recreates a Kafka pain under Whitewater terminology, redesign it.

## Non-negotiable experience principles

- Preserve `riverbed -> space -> feed -> key -> cursor -> subscription`; never expose physical partitions.
- Feed creation never asks for partition counts or physical placement.
- Keys define ordering; physical ranges may split, merge, and move without changing the client contract.
- Use opaque Cursors rather than public physical offsets.
- Prefer incremental leases and capacity-aware delivery over global consumer-group rebalances.
- Treat retries, delayed delivery, poison-event handling, and final disposition as coherent first-class workflows.
- Keep immutable Feed history separate from explicit persisted replicated Indexes.
- Provide queryable state without requiring application-managed local stores, changelogs, or routing.
- Keep schemas optional for storage but first-class in identity, compatibility, validation, and tooling.
- Use secure defaults: TLS for clients and authenticated encryption with verified Node identity between Nodes. Native mTLS is the default; a trusted service mesh or equivalent orchestrator transport is acceptable only when identity, rotation, audit, and downgrade prevention are verified. Keep scoped API-key identities, explainable authorization, and safe rotation.
- Use safe durability invariants rather than configuration combinations that silently weaken guarantees.
- Scale and rebalance gradually with sustained thresholds, hysteresis, disruption budgets, and safe drain gates.
- Never claim that adding capacity can parallelize one strictly ordered hot key.
- Make routine replacement, upgrade, replay, credential rotation, restore, and failover boring and explainable.
- Correlate metrics, traces, logs, events, and control decisions so users can move from symptom to cause.
- Errors explain cause, impact, retry safety, and the next safe action.
- Keep behavior and semantics consistent across supported client languages; Java is not the privileged public contract.
- Attribute storage, replication, movement, egress, Pipe, Subscription, and Index costs to logical owners.
- Distinguish implemented behavior from roadmap intent; never market an unverified guarantee.

## Engineering approach

- Prefer SOLID boundaries and cohesive domain types without abstracting ahead of demonstrated need.
- Prioritize correctness contracts before throughput optimization.
- Add unit tests for every behavior change and broader tests when persistence, distribution, or failure semantics are involved.
- Test unhappy paths, restart behavior, bounded resource use, and observability—not only the happy path.
- Preserve simple application APIs while keeping operational consequences visible and explainable.
- Convert pain points into measurable acceptance criteria rather than subjective claims.
- Require reproducible evidence for performance or cost claims.

## Rust engineering practices

- Use ownership and borrowing to make lifecycle and mutation boundaries explicit; clone only when ownership transfer or isolation justifies it.
- Model identities, positions, epochs, and validated values with domain newtypes rather than interchangeable primitives.
- Keep traits focused on stable subsystem boundaries and prefer concrete types inside an implementation.
- Make invalid states difficult to represent through constructors and private fields.
- Use typed errors with actionable context; do not use `unwrap`, `expect`, or panics in production paths.
- Use checked arithmetic and bounded allocations for all persisted or network-controlled lengths and counters.
- Keep blocking filesystem work off Tokio executor threads and make lock scope small, explicit, and free of `.await` points.
- Persist state with write, flush, atomic replace, and directory synchronization where required by the platform durability contract.
- Treat checksums as corruption detection, not authentication, and validate data before mutating durable state.
- Recover from expected torn tails, but fail closed on corruption inside an acknowledged or sealed prefix.
- Keep unsafe Rust out of the implementation unless no safe alternative exists; every unsafe block requires a documented invariant and focused tests.
- Follow standard naming, formatting, Clippy, and rustdoc conventions; optimize only from measured evidence.
- Prefer deterministic tests with temporary directories and explicit fault boundaries over sleeps or timing assumptions.
- Use targeted tests for iteration, but never treat them as final verification.

## Mandatory final verification

Run the full gate **after the last edit**, not just earlier in development, before marking a Whitewater task complete or committing:

1. Run `cargo fmt --check` and `cargo clippy --all-targets --all-features -- -D warnings`.
2. Run `cargo test --all-targets`; this covers the repository's Rust unit and integration test targets. Run with the supported Rust Docker image if the host lacks Rust.
3. Run every repository-maintained Python/Docker integration, fault, and live acceptance script in `scripts/test-*.py` sequentially against an isolated disposable test Riverbed. The M1.11 fault suite invokes other scripts and restarts Compose; the M4 auto-split test `--force-recreate`s the default Riverbed; other scripts stop/restart Nodes. Inspect exact side effects, project name, ports, data volumes, and credentials first. Never run them against the user's live Riverbed or destroy/recreate persistent resources without specific approval. Do not let repeated suites share mutable state unsafely.
4. If any final verification fails, fix the cause and rerun the affected tests **and the full gate** after the change. Report each command and outcome; if Docker, isolation, authentication, permissions, or another safety condition blocks a suite, state exactly what was not run and why. Never say "all tests passed" or check off live acceptance if any required suite was skipped.

This final gate applies to implementation work even if focused tests passed earlier. For documentation-only changes also verify links and `git diff --check`, and do not silently claim that unrun integration scripts passed.

## Completion check

Before declaring work complete, verify that:

1. The relevant pain point is linked or named in the design reasoning.
2. No physical implementation detail leaked into the public model unnecessarily.
3. Safe defaults work without expert configuration.
4. Failure and recovery behavior is documented and tested.
5. Diagnostics answer what, scope, cause, automatic action, current risk, and next safe action.
6. Unit tests and all applicable repository checks pass.
7. Documentation states current limitations honestly.