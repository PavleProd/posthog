use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use common_kafka_consumer::{
    GroupCompletion, Offset, Partition, TopicOffsetLedger, TopicPartition,
};
use tokio::time::Instant;

use super::collector::tests::message;
use super::{Parts, PollLimits, PollingStrategy, Step};
use crate::partition_budget::BudgetLimits;
use crate::settlement::{GroupSettlement, Settlement};

pub(super) const BATCH_TIMEOUT: Duration = Duration::from_millis(500);

pub(super) struct Harness<P> {
    pub(super) poller: P,
    pub(super) settlement: Arc<GroupSettlement>,
    pub(super) ledger: Arc<TopicOffsetLedger>,
    pub(super) now: Instant,
    batched: Vec<Vec<(i32, i64)>>,
}

impl<P: PollingStrategy> Harness<P> {
    pub(super) fn new(make: impl FnOnce(Parts) -> P) -> Self {
        let ledger = Arc::new(TopicOffsetLedger::new());
        let settlement = Arc::new(GroupSettlement::new(
            Arc::clone(&ledger),
            Duration::ZERO,
            BudgetLimits::unset(),
        ));
        let poller = make(Parts {
            limits: PollLimits {
                batch_size: 100,
                batch_size_bytes: 0,
                batch_timeout: BATCH_TIMEOUT,
            },
            settlement: Arc::clone(&settlement),
            group_id: "test".to_string(),
        });
        Self {
            poller,
            settlement,
            ledger,
            now: Instant::now(),
            batched: Vec::new(),
        }
    }

    pub(super) fn receive(&mut self, partition: i32, offsets: &[i64]) {
        for &offset in offsets {
            self.poller
                .on_received(&message(partition, offset, 10), self.now);
        }
    }

    pub(super) fn after_the_batch_timeout(&mut self) {
        self.now += BATCH_TIMEOUT;
    }

    pub(super) fn step(&mut self) -> Option<Step> {
        self.poller.next_step(self.now)
    }

    pub(super) fn submit(&mut self) -> HashMap<TopicPartition, (i64, i64)> {
        let Some(Step::Submit(collected)) = self.step() else {
            panic!("expected a poll to submit");
        };
        let charged = collected
            .charge(&self.ledger, &*self.settlement)
            .expect("the ledger accepts the charge");
        let spans = charged
            .poll
            .partitions
            .iter()
            .map(|(topic_partition, deliveries)| {
                (
                    topic_partition.clone(),
                    (deliveries.span.first, deliveries.span.last),
                )
            })
            .collect();
        self.batched.push(
            charged
                .accumulator
                .into_groups()
                .into_iter()
                .flat_map(|group| {
                    group
                        .messages
                        .into_iter()
                        .map(move |message| (group.partition.0, message.offset.0))
                })
                .collect(),
        );
        self.poller.on_submitted(charged.poll);
        spans
    }

    pub(super) fn batched(&self) -> &[Vec<(i32, i64)>] {
        &self.batched
    }

    pub(super) fn complete(&mut self, partition: i32, offsets: &[i64]) {
        let generation = self.ledger.generation(&p(partition));
        self.complete_under(generation, partition, offsets);
    }

    pub(super) fn complete_under(&mut self, generation: u64, partition: i32, offsets: &[i64]) {
        self.poller
            .on_completed(&completion(generation, partition, offsets))
            .expect("the completion applies");
    }

    pub(super) fn revoke(&mut self, partition: i32) {
        self.settlement.on_revoked(&[p(partition)]);
        self.poller.on_revoked(&[p(partition)]);
    }

    pub(super) fn finish_drain(&mut self, partition: i32) -> HashMap<TopicPartition, Offset> {
        let committed = self
            .poller
            .on_drain_finished(&[p(partition)])
            .map(|commit| commit.offsets)
            .unwrap_or_default();
        self.settlement.on_revoked(&[p(partition)]);
        committed
    }

    pub(super) fn commit(&mut self) -> HashMap<TopicPartition, Offset> {
        match self.step() {
            Some(Step::Commit(commit)) => commit.offsets,
            _ => panic!("expected a commit"),
        }
    }
}

pub(super) fn completion(generation: u64, partition: i32, offsets: &[i64]) -> GroupCompletion {
    GroupCompletion {
        partition: Partition(partition),
        assignment_epoch: 1,
        generation,
        offsets: offsets.iter().map(|&offset| Offset(offset)).collect(),
        accepted: offsets.len() as u32,
    }
}

pub(super) fn p(partition: i32) -> TopicPartition {
    TopicPartition::new("events", partition)
}
