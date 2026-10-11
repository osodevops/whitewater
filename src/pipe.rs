//! The Pipe driver: turns a declared `PipeDefinition` into committed,
//! applied effects. Each cycle reads the consumed Subscription's committed
//! frontier, fetches a bounded page of input records, commits a `Declare`
//! carrying the frontier evidence and deterministic output identities, then
//! drives that declaration to `Applied`.
//!
//! Deterministic identities make a crashed or restarted driver safe: the
//! same frontier yields the same `effect_id`, `request_id`, and per-record
//! writer identities, so journal replays and Feed append dedupe collapse
//! repeats instead of duplicating work. The driver never claims atomicity
//! beyond what the journal's apply step provides.

use std::collections::BTreeMap;
use std::sync::Arc;

use uuid::Uuid;

use crate::active_range::RangeId;
use crate::control::PipeDefinition;
use crate::effect::{
    EffectConsume, EffectCoordinator, EffectJournalError, EffectMutation, EffectOutput,
    EffectTransition,
};
use crate::reader::{SubscriptionProgressCoordinator, SubscriptionProgressMutation};

/// One input record fetched after the Subscription frontier.
#[derive(Clone, Debug)]
pub struct FetchedRecord {
    pub message_id: Uuid,
    pub key_base64: String,
    pub payload_base64: String,
    pub metadata_base64: BTreeMap<String, String>,
    pub event_time_ns: i64,
}

/// A bounded page fetched after a committed frontier: records in delivery
/// order plus the resulting frontier cursor and per-range positions.
#[derive(Clone, Debug)]
pub struct FetchedPage {
    pub records: Vec<FetchedRecord>,
    pub cursor: String,
    pub positions: BTreeMap<RangeId, String>,
}

/// Deterministic identity for the effect one Pipe cycle declares over a
/// given frontier: the same frontier re-driven after a crash produces the
/// same identity, and the journal replays it idempotently.
pub fn pipe_effect_id(pipe_id: Uuid, subscription_id: Uuid, expected_cursor: &str) -> Uuid {
    derived_uuid(
        b"pipe-effect",
        &[
            pipe_id.as_bytes().as_slice(),
            subscription_id.as_bytes().as_slice(),
            expected_cursor.as_bytes(),
        ],
    )
}

/// The per-record writer session a Pipe output uses; identical input records
/// map to identical writer identities so owner-side dedupe collapses replays.
pub fn pipe_output_writer_session(pipe_id: Uuid, message_id: Uuid) -> Uuid {
    derived_uuid(
        b"pipe-writer",
        &[
            pipe_id.as_bytes().as_slice(),
            message_id.as_bytes().as_slice(),
        ],
    )
}

/// The Declare request identity for one effect.
pub fn pipe_declare_request_id(effect_id: Uuid) -> Uuid {
    derived_uuid(b"pipe-declare", &[effect_id.as_bytes().as_slice()])
}

fn derived_uuid(tag: &[u8], parts: &[&[u8]]) -> Uuid {
    let mut material = tag.to_vec();
    for part in parts {
        material.extend_from_slice(&(part.len() as u64).to_le_bytes());
        material.extend_from_slice(part);
    }
    let digest = blake3::hash(&material);
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    Uuid::from_bytes(bytes)
}

/// Maps a progress-journal failure into the effect surface: transient
/// quorum/availability failures stay retryable, genuine fencing conflicts
/// surface as conflicts.
fn progress_apply_error(error: crate::reader::SubscriptionProgressError) -> EffectJournalError {
    match error {
        crate::reader::SubscriptionProgressError::Unavailable
        | crate::reader::SubscriptionProgressError::AmbiguousCommit
        | crate::reader::SubscriptionProgressError::NoQuorum
        | crate::reader::SubscriptionProgressError::Engine(_)
        | crate::reader::SubscriptionProgressError::Serialization(_) => {
            EffectJournalError::Unavailable
        }
        _ => EffectJournalError::Conflict,
    }
}

/// Drives one declared Pipe over a Subscription's progress assignment: the
/// same assignment fences both the consumed frontier and the effect journal.
pub struct PipeDriver {
    pipe: PipeDefinition,
    journal: Arc<EffectCoordinator>,
    progress: Arc<SubscriptionProgressCoordinator>,
}

impl PipeDriver {
    pub fn new(
        pipe: PipeDefinition,
        journal: Arc<EffectCoordinator>,
        progress: Arc<SubscriptionProgressCoordinator>,
    ) -> Self {
        Self {
            pipe,
            journal,
            progress,
        }
    }

    pub fn pipe(&self) -> &PipeDefinition {
        &self.pipe
    }

    /// Run one bounded cycle: read the committed frontier, fetch up to
    /// `limit` records, commit the Declare, then apply it. Returns the
    /// committed `Applied` mutation, or `None` when the input is caught up.
    ///
    /// The cycle is safe to call concurrently across Nodes and across
    /// retries: the second caller re-reads the moved frontier and either
    /// builds the next page or replays the identical journal mutation, while
    /// a stale `expected_cursor` fences a consumer that raced ahead.
    pub async fn drive_once<F, Fut, A>(
        &self,
        limit: usize,
        fetch: F,
        apply: &A,
    ) -> Result<Option<EffectMutation>, EffectJournalError>
    where
        F: FnOnce(&SubscriptionProgressMutation, usize) -> Fut,
        Fut: std::future::Future<Output = Result<FetchedPage, EffectJournalError>>,
        A: crate::effect::EffectApply + ?Sized,
    {
        let Some(frontier) = self
            .progress
            .read_committed()
            .await
            .map_err(progress_apply_error)?
        else {
            return Err(EffectJournalError::Unavailable);
        };
        let limit = limit.clamp(1, crate::effect::EFFECT_MAX_OUTPUTS);
        let page = fetch(&frontier, limit).await?;
        if page.records.is_empty() {
            return Ok(None);
        }
        if page.records.len() > crate::effect::EFFECT_MAX_OUTPUTS
            || page.positions.len() > crate::effect::EFFECT_MAX_POSITIONS
        {
            return Err(EffectJournalError::TooLarge);
        }
        if page.cursor.is_empty() || page.positions.is_empty() {
            return Err(EffectJournalError::Conflict);
        }
        let effect_id = pipe_effect_id(
            self.pipe.pipe_id,
            frontier.subscription_id,
            &frontier.cursor,
        );
        let outputs = page
            .records
            .iter()
            .map(|record| EffectOutput {
                feed_id: self.pipe.output_feed_id,
                key_base64: record.key_base64.clone(),
                payload_base64: record.payload_base64.clone(),
                metadata_base64: record.metadata_base64.clone(),
                event_time_ns: record.event_time_ns,
                writer_session_id: pipe_output_writer_session(self.pipe.pipe_id, record.message_id),
                writer_epoch: 1,
                sequence: 1,
            })
            .collect();
        let declare = EffectMutation {
            effect_id,
            subscription_id: frontier.subscription_id,
            ownership_epoch: frontier.ownership_epoch,
            sequence: 1,
            request_id: pipe_declare_request_id(effect_id),
            transition: EffectTransition::Declare {
                consume: Some(EffectConsume {
                    feed_id: frontier.feed_id,
                    expected_cursor: Some(frontier.cursor.clone()),
                    cursor: page.cursor,
                    positions: page.positions,
                }),
                outputs,
            },
        };
        match self.journal.apply(declare.clone()).await {
            Ok(_) => {}
            Err(EffectJournalError::AmbiguousCommit | EffectJournalError::NoQuorum) => {
                if self.journal.read_committed(effect_id).await?.is_none() {
                    // Nothing committed: the Declare may be half-prepared, so
                    // replay it to settle the quorum before applying.
                    self.journal.reconcile_retry(declare).await?;
                }
            }
            Err(error) => return Err(error),
        }
        self.journal
            .apply_committed(effect_id, apply)
            .await
            .map(Some)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::{PipeOperation, PipeStage, ResourceStatus};

    #[test]
    fn derived_identities_are_stable_and_scoped() {
        let pipe = Uuid::from_u128(1);
        let subscription = Uuid::from_u128(2);
        let first = pipe_effect_id(pipe, subscription, "cursor-a");
        assert_eq!(first, pipe_effect_id(pipe, subscription, "cursor-a"));
        assert_ne!(first, pipe_effect_id(pipe, subscription, "cursor-b"));
        assert_ne!(
            first,
            pipe_effect_id(Uuid::from_u128(9), subscription, "cursor-a")
        );
        let session = pipe_output_writer_session(pipe, Uuid::from_u128(50));
        assert_eq!(
            session,
            pipe_output_writer_session(pipe, Uuid::from_u128(50))
        );
        assert_ne!(
            session,
            pipe_output_writer_session(pipe, Uuid::from_u128(51))
        );
        assert_ne!(
            session,
            pipe_output_writer_session(Uuid::from_u128(9), Uuid::from_u128(50))
        );
        assert_eq!(
            pipe_declare_request_id(first),
            pipe_declare_request_id(first)
        );
    }

    #[test]
    fn forward_pipe_definition_carries_driver_inputs() {
        // The declared shape is all drive_once needs: consuming Subscription,
        // output Feed, and an operation that maps records 1:1.
        let pipe = PipeDefinition {
            pipe_id: Uuid::from_u128(7),
            name: "orders.forward".to_owned(),
            space_id: Uuid::from_u128(8),
            subscription_id: Uuid::from_u128(9),
            operation: PipeOperation::Forward,
            output_feed_id: Uuid::from_u128(10),
            stage: PipeStage::Declared,
            status: ResourceStatus::Active,
            created_at_ns: 0,
        };
        assert_eq!(pipe.operation, PipeOperation::Forward);
    }
}
