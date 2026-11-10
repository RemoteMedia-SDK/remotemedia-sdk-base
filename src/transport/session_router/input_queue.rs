//! Admit owners before they enter a node's input/filter/control queues.
use super::{RuntimeData, SessionControl};
use crate::transport::origin::Routed;
use crate::{
    telemetry::{approx_payload_bytes, EdgeCell, SessionTelemetry},
    Error, Result,
};
use remotemedia_types::BufferBudget;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::mpsc;

pub(super) fn channels<'a>(
    node_ids: impl Iterator<Item = &'a String>,
    allocators: &HashMap<String, Arc<remotemedia_types::BoundOutputAllocator>>,
) -> (
    HashMap<String, InputQueue>,
    HashMap<String, mpsc::Receiver<Routed>>,
) {
    let mut senders = HashMap::new();
    let mut receivers = HashMap::new();
    for node_id in node_ids {
        let (sender, receiver) = mpsc::channel(super::NODE_INPUT_CAPACITY);
        senders.insert(
            node_id.clone(),
            InputQueue {
                sender,
                node_id: node_id.clone(),
                budget: allocators
                    .get(node_id)
                    .and_then(|allocator| allocator.budget())
                    .cloned(),
            },
        );
        receivers.insert(node_id.clone(), receiver);
    }
    (senders, receivers)
}

#[derive(Clone)]
pub(super) struct InputQueue {
    pub(super) sender: mpsc::Sender<Routed>,
    pub(super) budget: Option<BufferBudget>,
    pub(super) node_id: String,
}

impl InputQueue {
    // Receipts survive dequeue, filtering, execution and downstream retention.
    // A cancelled send drops its receipt; it never waits for budget capacity.
    async fn send_observed(&self, routed: Routed, edge: Option<&EdgeCell>) -> Result<bool> {
        let Routed { data, origin } = routed;
        let data = match &self.budget {
            Some(budget) => budget.admit(data).map_err(|error| {
                Error::Execution(format!(
                    "node '{}' input queue buffer retention budget: {error}",
                    self.node_id
                ))
            })?,
            // Unbudgeted queues promote too: dispatch clones the dequeued
            // input, which must never deep-copy legacy Vec audio.
            None => data.into_shared(),
        };
        match self.sender.try_send(Routed::new(data, origin)) {
            Ok(()) => Ok(true),
            Err(mpsc::error::TrySendError::Closed(_)) => Ok(false),
            Err(mpsc::error::TrySendError::Full(data)) => {
                let _stall = edge.map(Stall::new);
                Ok(self.sender.send(data).await.is_ok())
            }
        }
    }

    /// Queue exhaustion is reported like a checked-node execution error. Other
    /// fan-out destinations can still receive the frame, and release permits a retry.
    pub(super) async fn deliver(
        &self,
        data: Routed,
        telemetry: &SessionTelemetry,
        edge_index: Option<usize>,
        control: Option<&SessionControl>,
    ) -> bool {
        let bytes = approx_payload_bytes(&data.data);
        let edge = edge_index.and_then(|index| telemetry.edge(index));
        let delivered = match self.send_observed(data, edge).await {
            Ok(delivered) => delivered,
            Err(error) => {
                crate::telemetry::publisher::record_node_error(
                    telemetry,
                    telemetry.node_index(&self.node_id),
                    control,
                    &self.node_id,
                    &error.to_string(),
                );
                tracing::error!(node_id = %self.node_id, %error, "input queue admission failed");
                false
            }
        };
        if let Some(edge) = edge {
            if delivered {
                edge.record_frame(bytes);
            } else {
                edge.record_drop();
            }
        }
        delivered
    }

    #[cfg(test)]
    pub(super) async fn send(&self, data: RuntimeData) -> Result<bool> {
        self.send_observed(Routed::unattributed(data), None).await
    }
}

struct Stall<'a> {
    edge: &'a EdgeCell,
    start: std::time::Instant,
}
impl<'a> Stall<'a> {
    fn new(edge: &'a EdgeCell) -> Self {
        edge.begin_stall();
        Self {
            edge,
            start: std::time::Instant::now(),
        }
    }
}
impl Drop for Stall<'_> {
    fn drop(&mut self) {
        self.edge.end_stall(self.start.elapsed().as_micros() as u64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use remotemedia_types::{
        BoundOutputAllocator, HeapOutputAllocator, OutputAllocator, SharedBuffer,
    };
    use std::sync::Arc;

    fn queue(owners: u64, bytes: u64) -> (InputQueue, mpsc::Receiver<Routed>, BufferBudget) {
        let (sender, receiver) = mpsc::channel(1);
        let budget = BufferBudget::new(owners, bytes);
        (
            InputQueue {
                sender,
                budget: Some(budget.clone()),
                node_id: "test".into(),
            },
            receiver,
            budget,
        )
    }

    #[tokio::test]
    async fn unbudgeted_queue_promotes_legacy_vec_audio_without_copying() {
        let (sender, mut receiver) = mpsc::channel(1);
        let queue = InputQueue {
            sender,
            budget: None,
            node_id: "test".into(),
        };
        let samples = vec![0.75_f32; 32];
        let original = samples.as_ptr();
        let audio = RuntimeData::Audio {
            samples: crate::data::AudioSamples::Vec(samples),
            sample_rate: 16_000,
            channels: 1,
            stream_id: None,
            timestamp_us: None,
            arrival_ts_us: None,
            metadata: None,
        };
        assert!(queue.send(audio).await.unwrap());
        let RuntimeData::Audio { samples, .. } = receiver.recv().await.unwrap().data else {
            panic!("expected audio")
        };
        assert!(!matches!(samples, crate::data::AudioSamples::Vec(_)));
        assert_eq!(samples.as_ptr(), original);
        // Dispatch clones dequeued input; that clone now shares the samples.
        let dispatched = samples.clone();
        assert_eq!(dispatched.as_ptr(), original);
    }

    #[tokio::test]
    async fn queue_receipts_share_producer_budget_and_outlive_dequeue() {
        let (queue, mut receiver, budget) = queue(1, 8);
        let allocator =
            BoundOutputAllocator::new(Arc::new(HeapOutputAllocator)).with_budget(budget.clone());
        let owner = allocator.allocate_bytes(8).unwrap().freeze();
        let original = owner.as_ptr();
        assert!(queue.send(RuntimeData::Binary(owner)).await.unwrap());
        assert_eq!(budget.usage().owners, 1);
        let RuntimeData::Binary(retained) = receiver.recv().await.unwrap().data else {
            panic!()
        };
        assert_eq!(retained.as_ptr(), original);
        assert!(queue
            .send(RuntimeData::Binary(SharedBuffer::from(vec![1])))
            .await
            .is_err());
        assert!(allocator.allocate_bytes(1).is_err());
        drop(retained);
        assert_eq!(budget.usage().owners, 0);
        assert!(queue
            .send(RuntimeData::Binary(SharedBuffer::from(vec![1])))
            .await
            .unwrap());
        drop(receiver);
        assert_eq!(budget.usage().owners, 0);
    }

    #[tokio::test]
    async fn cancelled_full_queue_send_releases_only_its_reservation() {
        let manifest = super::super::tests::create_test_manifest(vec![("test", "Test")], vec![]);
        let (output, _) = super::super::ClientOutputRouter::new(1);
        let (router, _) = super::super::SessionRouter::new(
            "stall-test".into(),
            Arc::new(manifest),
            Arc::new(crate::nodes::StreamingNodeRegistry::new()),
            output,
        )
        .unwrap();
        let edge = router
            .telemetry
            .edge(router.descriptor.ingress_edge("test").unwrap())
            .unwrap();
        let (queue, mut receiver, budget) = queue(2, 16);
        queue
            .send(RuntimeData::Binary(vec![1; 8].into()))
            .await
            .unwrap();
        let mut pending = Box::pin(queue.send_observed(
            Routed::unattributed(RuntimeData::Binary(vec![2; 8].into())),
            Some(edge),
        ));
        assert!(matches!(
            futures::poll!(pending.as_mut()),
            std::task::Poll::Pending
        ));
        assert_eq!(budget.usage().owners, 2);
        assert_eq!(
            edge.active_stalls
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
        drop(pending);
        assert_eq!(
            edge.active_stalls
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        assert_eq!(budget.usage().owners, 1);
        drop(receiver.recv().await);
        assert_eq!(budget.usage().owners, 0);
    }

    #[tokio::test]
    async fn aliases_empty_views_and_closed_receivers_do_not_leak_credits() {
        let (queue, mut receiver, budget) = queue(1, 8);
        let owner = SharedBuffer::from(vec![3; 8]);
        queue
            .send(RuntimeData::Binary(owner.slice(0..0).unwrap()))
            .await
            .unwrap();
        let retained = receiver.recv().await.unwrap();
        assert_eq!(budget.usage().bytes, 8);
        queue
            .send(RuntimeData::Binary(owner.clone()))
            .await
            .unwrap();
        assert_eq!(budget.usage().owners, 1);
        drop(retained);
        drop(receiver);
        assert_eq!(budget.usage().owners, 0);
        assert!(!queue.send(RuntimeData::Binary(owner)).await.unwrap());
        assert_eq!(budget.usage().owners, 0);
    }
}
