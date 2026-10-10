path = 'src/reader.rs'
src = open(path, encoding='utf-8').read()

anchor = """        Ok(SubscriptionRecoveryOutcome {
            assignment: self.assignment.clone(),
            committed,
            members: member_state,
            adopted,
        })
    }
}"""
assert src.count(anchor) == 1

methods = """        Ok(SubscriptionRecoveryOutcome {
            assignment: self.assignment.clone(),
            committed,
            members: member_state,
            adopted,
        })
    }

    pub async fn member_state(&self) -> Result<SubscriptionMemberState, SubscriptionProgressError> {
        let evidence = self.inspect_evidence().await?;
        let (_, members) = Self::recovered_committed(&evidence)?;
        Ok(members)
    }

    pub async fn can_member_ack(
        &self,
        grant: &SubscriptionWorkLease,
        now_tick: u64,
    ) -> Result<bool, SubscriptionProgressError> {
        let members = self.member_state().await?;
        let tracker =
            SubscriptionLeaseTracker::from_state(members, SUBSCRIPTION_LEASE_MAX_WORK);
        Ok(tracker.can_ack(grant, now_tick))
    }

    pub async fn apply_member_ops(
        &self,
        request_id: Uuid,
        tick: u64,
        lease_ops: Vec<SubscriptionLeaseOp>,
    ) -> Result<SubscriptionProgressMutation, SubscriptionProgressError> {
        let current = self
            .read_committed()
            .await?
            .ok_or(SubscriptionProgressError::Conflict)?;
        let mut mutation = current.clone();
        mutation.sequence = current
            .sequence
            .checked_add(1)
            .ok_or(SubscriptionProgressError::Sequence)?;
        mutation.request_id = request_id;
        mutation.expected_cursor = Some(current.cursor.clone());
        mutation.tick = tick;
        mutation.lease_ops = lease_ops;
        self.apply(mutation.clone()).await?;
        Ok(mutation)
    }

    pub async fn acknowledge(
        &self,
        grant: &SubscriptionWorkLease,
        tick: u64,
        request_id: Uuid,
        cursor: String,
        positions: BTreeMap<RangeId, String>,
    ) -> Result<SubscriptionProgressMutation, SubscriptionProgressError> {
        let current = self
            .read_committed()
            .await?
            .ok_or(SubscriptionProgressError::Conflict)?;
        let mut mutation = SubscriptionProgressMutation {
            subscription_id: current.subscription_id,
            feed_id: current.feed_id,
            ownership_epoch: current.ownership_epoch,
            sequence: current
                .sequence
                .checked_add(1)
                .ok_or(SubscriptionProgressError::Sequence)?,
            request_id,
            expected_cursor: Some(current.cursor.clone()),
            cursor,
            positions,
            tick,
            lease_ops: vec![SubscriptionLeaseOp::Release {
                work_id: grant.work_id,
                member_id: grant.member_id,
                member_epoch: grant.member_epoch,
                lease_epoch: grant.lease_epoch,
            }],
        };
        self.apply(mutation.clone()).await?;
        mutation.lease_ops.clear();
        Ok(mutation)
    }
}"""
src = src.replace(anchor, methods)
open(path, 'w', encoding='utf-8', newline='\n').write(src)
print("member api added")
