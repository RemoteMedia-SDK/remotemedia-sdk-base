//! Generator cancellation in the router (openspec: add-generator-cancellation):
//! the call's token fires with the reason and disposition before the call's
//! future is dropped, `on_cancel` runs, and the disposition decides whether
//! the node's queued inputs survive.

use super::{tests::create_test_manifest, *};
use remotemedia_traits::{
    CancelReason, CancelStrategy, Cancellable, Cancellation, Disposition, DispositionOverride,
    GeneratorRole, Retention, TextGenerator,
};
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::Notify;

#[derive(Default)]
struct Seen {
    /// What `on_cancel` received.
    on_cancel: Mutex<Vec<Cancellation>>,
    /// The token's cancellation when the call's future was dropped.
    at_drop: Mutex<Vec<Option<Cancellation>>>,
}

/// A text generator whose "start" call runs until cancelled.
struct Gen {
    started: Arc<Notify>,
    seen: Arc<Seen>,
    /// A TTS-like default: an interrupt discards queued input.
    discards_queue: bool,
    /// Take a ProtectGuard for the "start" call.
    protect: bool,
}

#[async_trait::async_trait]
impl Cancellable for Gen {
    fn cancel_strategy(&self) -> CancelStrategy {
        CancelStrategy::OnCancel
    }
    fn default_disposition(&self, cancel: &remotemedia_traits::CancelContext) -> Disposition {
        let d = cancel.reason.default_disposition();
        if self.discards_queue {
            Disposition {
                pending_input: Retention::Discard,
                ..d
            }
        } else {
            d
        }
    }
    async fn on_cancel(
        &self,
        _call: &remotemedia_traits::rpc::CallContext,
        cancellation: &Cancellation,
    ) {
        self.seen.on_cancel.lock().unwrap().push(*cancellation);
    }
}
impl TextGenerator for Gen {}

struct RecordAtDrop(remotemedia_traits::CancelToken, Arc<Seen>);
impl Drop for RecordAtDrop {
    fn drop(&mut self) {
        self.1.at_drop.lock().unwrap().push(self.0.cancellation());
    }
}

#[async_trait::async_trait]
impl crate::nodes::AsyncStreamingNode for Gen {
    fn node_type(&self) -> &str {
        "Gen"
    }
    fn generator(&self) -> Option<GeneratorRole<'_>> {
        GeneratorRole::text(self)
    }
    async fn process(&self, data: RuntimeData) -> Result<RuntimeData> {
        Ok(data)
    }
    async fn process_streaming_with_ctx<F>(
        &self,
        data: RuntimeData,
        ctx: &dyn crate::nodes::NodeRuntimeContextRead,
        mut callback: F,
    ) -> Result<usize>
    where
        F: FnMut(RuntimeData) -> Result<()> + Send,
    {
        if !matches!(&data, RuntimeData::Text(t) if t == "start") {
            callback(data)?;
            return Ok(1);
        }
        let _guard = self.protect.then(|| {
            crate::nodes::NodeRuntimeContext::of(ctx)
                .expect("host context")
                .cancel_gate
                .protect()
        });
        let _record = RecordAtDrop(ctx.cancel_token(), self.seen.clone());
        self.started.notify_one();
        std::future::pending().await
    }
}

struct Run {
    pipeline: PipelineTasks,
    received: ClientOutputReceivers,
    seen: Arc<Seen>,
}

async fn start(
    discards_queue: bool,
    protect: bool,
    on_cancel: Option<DispositionOverride>,
) -> Run {
    let mut manifest = create_test_manifest(vec![("gen", "Gen")], vec![]);
    manifest.nodes[0].on_cancel = on_cancel;
    let (output, received) = ClientOutputRouter::new(32);
    let (mut router, _) = SessionRouter::new(
        "gen-cancel".into(),
        Arc::new(manifest),
        Arc::new(StreamingNodeRegistry::new()),
        output,
    )
    .unwrap();
    let started = Arc::new(Notify::new());
    let seen = Arc::new(Seen::default());
    router.cached_nodes.insert(
        "gen".into(),
        Box::new(crate::nodes::AsyncNodeWrapper(Arc::new(Gen {
            started: started.clone(),
            seen: seen.clone(),
            discards_queue,
            protect,
        }))),
    );
    let pipeline = router.spawn_pipeline_tasks().unwrap();
    let input = &pipeline.input_txs["gen"];
    input.send(RuntimeData::Text("start".into())).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), started.notified())
        .await
        .unwrap();
    Run {
        pipeline,
        received,
        seen,
    }
}

impl Run {
    async fn send(&self, data: RuntimeData) {
        self.pipeline.input_txs["gen"].send(data).await.unwrap();
    }

    async fn barge(&self, disposition: Option<DispositionOverride>) {
        self.send(crate::transport::session_control::barge_in_envelope(
            disposition,
        ))
        .await;
    }

    /// Texts the node passes on until `last` arrives.
    async fn texts_until(&mut self, last: &str) -> Vec<String> {
        let mut texts = Vec::new();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let RuntimeData::Text(t) = self.received.data_rx.recv().await.unwrap().data {
                    let done = t == last;
                    texts.push(t);
                    if done {
                        break;
                    }
                }
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out; got {texts:?}"));
        texts
    }

    async fn on_cancel(&self) -> Vec<Cancellation> {
        for _ in 0..100 {
            let seen = self.seen.on_cancel.lock().unwrap().clone();
            if !seen.is_empty() {
                return seen;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Vec::new()
    }

    async fn finish(self) {
        SessionRouter::teardown_pipeline_tasks(self.pipeline, true)
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn an_interrupt_fires_the_token_before_the_drop_and_keeps_queued_input() {
    let mut run = start(false, false, None).await;
    run.send(RuntimeData::Text("said while it spoke".into())).await;
    run.barge(None).await;
    run.send(RuntimeData::Text("said after".into())).await;
    // The audio/text the user produced around the barge reaches the next call.
    assert_eq!(
        run.texts_until("said after").await,
        vec!["said while it spoke", "said after"]
    );
    let interrupt = Cancellation::with_disposition(CancelReason::BargeIn, Disposition::INTERRUPT);
    assert_eq!(*run.seen.at_drop.lock().unwrap(), vec![Some(interrupt)]);
    assert_eq!(run.on_cancel().await, vec![interrupt]);
    run.finish().await;
}

#[tokio::test]
async fn a_node_default_can_discard_queued_input() {
    let mut run = start(true, false, None).await;
    run.send(RuntimeData::Text("stale reply text".into())).await;
    run.barge(None).await;
    run.send(RuntimeData::Text("fresh".into())).await;
    assert_eq!(run.texts_until("fresh").await, vec!["fresh"]);
    let fired = run.on_cancel().await;
    assert_eq!(fired[0].disposition.pending_input, Retention::Discard);
    assert_eq!(fired[0].disposition.partial_output, Retention::Retain);
    run.finish().await;
}

#[tokio::test]
async fn the_canceller_overrides_the_node() {
    let mut run = start(false, false, None).await;
    run.send(RuntimeData::Text("queued".into())).await;
    run.barge(Some(DispositionOverride {
        pending_input: Some(Retention::Discard),
        ..Default::default()
    }))
    .await;
    run.send(RuntimeData::Text("fresh".into())).await;
    assert_eq!(run.texts_until("fresh").await, vec!["fresh"]);
    run.finish().await;
}

#[tokio::test]
async fn the_manifest_overrides_the_node_and_the_canceller_overrides_the_manifest() {
    let manifest_choice = DispositionOverride {
        partial_output: Some(Retention::Discard),
        pending_input: Some(Retention::Discard),
    };
    // Manifest alone: discard both.
    let mut run = start(false, false, Some(manifest_choice)).await;
    run.send(RuntimeData::Text("queued".into())).await;
    run.barge(None).await;
    run.send(RuntimeData::Text("fresh".into())).await;
    assert_eq!(run.texts_until("fresh").await, vec!["fresh"]);
    assert_eq!(run.on_cancel().await[0].disposition, Disposition::ABORT);
    run.finish().await;

    // The canceller keeps the input; the manifest still rolls back output.
    let mut run = start(false, false, Some(manifest_choice)).await;
    run.send(RuntimeData::Text("queued".into())).await;
    run.barge(Some(DispositionOverride {
        pending_input: Some(Retention::Retain),
        ..Default::default()
    }))
    .await;
    run.send(RuntimeData::Text("fresh".into())).await;
    assert_eq!(run.texts_until("fresh").await, vec!["queued", "fresh"]);
    let fired = run.on_cancel().await[0];
    assert_eq!(fired.disposition.partial_output, Retention::Discard);
    assert_eq!(fired.disposition.pending_input, Retention::Retain);
    run.finish().await;
}

#[tokio::test]
async fn a_protected_call_is_not_cancelled_and_its_token_does_not_fire() {
    let run = start(false, true, None).await;
    run.barge(None).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(run.seen.on_cancel.lock().unwrap().is_empty());
    assert!(run.seen.at_drop.lock().unwrap().is_empty(), "the call kept running");
    run.finish().await;
}

