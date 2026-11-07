use crate::nodes::PacingNature;
use futures::{stream::FuturesUnordered, StreamExt};
use tokio::sync::oneshot;

/// Every pacer must explicitly finish; a reactive root keeps external ingress
/// live. Dropping a pacer, exhausting its clock, or a quiet tick is not EOS.
pub(super) struct SourceCompletions {
    pending: FuturesUnordered<oneshot::Receiver<()>>,
    can_finish: bool,
}

pub(super) fn is_paced(nature: PacingNature) -> bool {
    matches!(nature, PacingNature::SourceWall(hz) if hz > 0)
        || matches!(nature, PacingNature::ClockedToOutboundMedia)
}

impl SourceCompletions {
    pub(super) fn for_graph(
        graph: &crate::executor::PipelineGraph,
        nodes: &std::collections::HashMap<String, Box<dyn crate::nodes::StreamingNode>>,
    ) -> Self {
        let autonomous = !graph.sources.is_empty()
            && graph.sources.iter().all(|id| {
                nodes
                    .get(id)
                    .is_some_and(|node| is_paced(node.pacing_nature()))
            });
        Self::new(autonomous)
    }

    pub(super) fn new(can_finish: bool) -> Self {
        Self {
            pending: FuturesUnordered::new(),
            can_finish,
        }
    }

    pub(super) fn register(&mut self, nature: PacingNature) -> Option<oneshot::Sender<()>> {
        if !is_paced(nature) {
            return None;
        }
        let (sender, receiver) = oneshot::channel();
        self.pending.push(receiver);
        Some(sender)
    }

    pub(super) async fn wait(&mut self) {
        while let Some(result) = self.pending.next().await {
            if result.is_err() {
                self.can_finish = false;
            }
        }
        if !self.can_finish {
            std::future::pending::<()>().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn dropped_pacer_is_not_completion_even_after_wait_is_cancelled() {
        let mut group = SourceCompletions::new(true);
        let first = group.register(PacingNature::SourceWall(100)).unwrap();
        let second = group
            .register(PacingNature::ClockedToOutboundMedia)
            .unwrap();
        drop(first);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), group.wait())
                .await
                .is_err()
        );
        second.send(()).unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), group.wait())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn successful_completions_survive_wait_cancellation() {
        let mut group = SourceCompletions::new(true);
        assert!(group.register(PacingNature::Reactive).is_none());
        assert!(group.register(PacingNature::SourceWall(0)).is_none());
        let first = group.register(PacingNature::SourceWall(100)).unwrap();
        let second = group
            .register(PacingNature::ClockedToOutboundMedia)
            .unwrap();
        first.send(()).unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), group.wait())
                .await
                .is_err()
        );
        second.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), group.wait())
            .await
            .unwrap();
    }
}
