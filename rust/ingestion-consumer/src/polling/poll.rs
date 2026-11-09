use std::collections::HashMap;

use common_kafka_consumer::{Charge, Offset, Partition, TopicOffsetLedger, TopicPartition};

use crate::debug_recorder::{PartitionOffset, PartitionOffsets};
use crate::ledger_rejection::{report_rejection, RejectedSlice};
use crate::order_sentinel::OffsetSpan;
use crate::settlement::Settlement;
use crate::types::Accumulator;

#[derive(Default)]
pub(crate) struct BatchStats {
    /// One (partition, lag in ms) pair per message with a parseable `now` header.
    pub(crate) message_lags_ms: Vec<(i32, i64)>,
    /// Sum of message payload sizes, without keys or headers.
    pub(crate) total_bytes: usize,
}

pub(crate) struct Delivery {
    pub(crate) offset: i64,
    pub(crate) charge: Charge,
    pub(crate) payload_bytes: usize,
    /// Kafka message timestamp (ms).
    pub(crate) kafka_ts: i64,
    /// Ingestion lag (ms); `None` without a parseable `now` header.
    pub(crate) lag_ms: Option<i64>,
}

pub(crate) struct PartitionDeliveries {
    /// The first and last delivered offsets. A compacted or transactional topic
    /// can skip offsets between them.
    pub(crate) span: OffsetSpan,
    /// The ledger generation, set when the poll is charged.
    pub(crate) generation: u64,
    pub(crate) charges: Vec<(Offset, Charge)>,
    pub(crate) payload_bytes: usize,
    /// Max Kafka message timestamp (ms).
    pub(crate) latest_kafka_ts: i64,
    pub(crate) max_lag_ms: Option<i64>,
    pub(crate) delivered: u32,
}

impl PartitionDeliveries {
    pub(crate) fn new(delivery: &Delivery) -> Self {
        Self {
            span: OffsetSpan::new(delivery.offset),
            generation: 0,
            charges: vec![(Offset(delivery.offset), delivery.charge)],
            payload_bytes: delivery.payload_bytes,
            latest_kafka_ts: delivery.kafka_ts,
            max_lag_ms: delivery.lag_ms,
            delivered: 1,
        }
    }

    pub(crate) fn record(&mut self, delivery: &Delivery) {
        self.span.extend(delivery.offset);
        self.delivered += 1;
        self.payload_bytes += delivery.payload_bytes;
        self.latest_kafka_ts = self.latest_kafka_ts.max(delivery.kafka_ts);
        if let Some(lag_ms) = delivery.lag_ms {
            self.max_lag_ms = Some(self.max_lag_ms.map_or(lag_ms, |max| max.max(lag_ms)));
        }
        self.charges
            .push((Offset(delivery.offset), delivery.charge));
    }
}

pub(crate) struct CollectedBatch {
    pub(crate) accumulator: Accumulator,
    pub(crate) partitions: HashMap<TopicPartition, PartitionDeliveries>,
    pub(crate) stats: BatchStats,
}

pub(crate) struct InFlightPoll {
    pub(crate) partitions: HashMap<TopicPartition, PartitionDeliveries>,
    pub(crate) uncommitted: HashMap<TopicPartition, Charge>,
}

pub(crate) struct ChargedPoll {
    /// Holds no message the ledger dropped as redelivered.
    pub(crate) accumulator: Accumulator,
    pub(crate) poll: InFlightPoll,
    pub(crate) stats: BatchStats,
    pub(crate) generations: HashMap<Partition, u64>,
    pub(crate) to_pause: Vec<TopicPartition>,
}

impl CollectedBatch {
    /// Fails on a ledger violation.
    pub(crate) fn charge(
        self,
        ledger: &TopicOffsetLedger,
        settlement: &impl Settlement,
    ) -> anyhow::Result<ChargedPoll> {
        let CollectedBatch {
            mut accumulator,
            mut partitions,
            stats,
        } = self;
        let mut uncommitted = HashMap::with_capacity(partitions.len());
        let mut generations = HashMap::with_capacity(partitions.len());
        let mut to_pause = Vec::new();
        // One ledger call per partition keeps the lock and the gauge labels
        // off the per-message path.
        for (topic_partition, partition) in &mut partitions {
            partition.generation = ledger.generation(topic_partition);
            generations.insert(Partition(topic_partition.partition), partition.generation);
            match settlement.charge(topic_partition, partition.generation, &partition.charges) {
                Ok(charged) => {
                    for &offset in &charged.redelivered {
                        accumulator.remove_last(Partition(topic_partition.partition), offset);
                    }
                    if charged.pause.is_some() {
                        to_pause.push(topic_partition.clone());
                    }
                    uncommitted.insert(topic_partition.clone(), charged.uncommitted);
                }
                Err(rejection) => report_rejection(
                    "charge",
                    topic_partition,
                    rejection,
                    RejectedSlice::charged(&partition.charges),
                )?,
            }
        }
        Ok(ChargedPoll {
            accumulator,
            poll: InFlightPoll {
                partitions,
                uncommitted,
            },
            stats,
            generations,
            to_pause,
        })
    }
}

/// Per-partition max offset + observed lag for the debug UI's batch events.
pub(crate) fn debug_partition_offsets(
    partitions: &HashMap<TopicPartition, PartitionDeliveries>,
) -> Vec<PartitionOffset> {
    partitions
        .iter()
        .map(|(topic_partition, partition)| PartitionOffset {
            topic: topic_partition.topic.clone(),
            partition: topic_partition.partition,
            offset: partition.span.last,
            lag_ms: partition.max_lag_ms.unwrap_or(0),
        })
        .collect()
}

/// Per-partition offset ranges, so the debug UI can match requests to polls.
pub(crate) fn debug_partition_spans(
    partitions: &HashMap<TopicPartition, PartitionDeliveries>,
) -> Vec<PartitionOffsets> {
    partitions
        .iter()
        .map(|(topic_partition, partition)| PartitionOffsets {
            topic: topic_partition.topic.clone(),
            partition: topic_partition.partition,
            ranges: vec![(partition.span.first, partition.span.last)],
        })
        .collect()
}
