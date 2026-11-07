use super::{tests::create_test_manifest, *};
use crate::nodes::{
    AsyncNodeWrapper, AsyncStreamingNode, NodeRuntimeContext, NodeRuntimeContextRead,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Notify;

struct FiniteSource {
    requested: Notify,
    release: Notify,
    calls: AtomicUsize,
    retained: std::sync::Mutex<Option<NodeRuntimeContext>>,
    inject_ingress: std::sync::Mutex<Option<crate::transport::SessionPacketSender>>,
    bytes: remotemedia_types::SharedBytes,
    fail: bool,
}

#[async_trait::async_trait]
impl AsyncStreamingNode for FiniteSource {
    fn node_type(&self) -> &str {
        "FiniteSource"
    }
    async fn process(&self, data: RuntimeData) -> Result<RuntimeData> {
        Ok(data)
    }
    fn pacing_nature(&self) -> crate::nodes::PacingNature {
        crate::nodes::PacingNature::SourceWall(100)
    }
    async fn tick_with_ctx(
        &self,
        _tick: crate::nodes::Tick,
        ctx: &dyn NodeRuntimeContextRead,
        mut output: Box<dyn FnMut(RuntimeData) -> Result<()> + Send>,
    ) -> Result<()> {
        // A successful quiet tick must not terminate this source.
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            return Ok(());
        }
        ctx.finish_source()?;
        *self.retained.lock().unwrap() = Some(
            ctx.as_any()
                .downcast_ref::<NodeRuntimeContext>()
                .unwrap()
                .clone(),
        );
        output(RuntimeData::Binary(self.bytes.clone()))?;
        self.requested.notify_one();
        self.release.notified().await;
        output(RuntimeData::Binary(self.bytes.clone()))?;
        output(RuntimeData::Binary(self.bytes.clone()))?;
        // No yield between queueing these packets and returning from the final
        // tick: both ingress and completion are ready at the router's next poll.
        if let Some(input) = self.inject_ingress.lock().unwrap().take() {
            for sequence in 0..8 {
                input
                    .try_send(DataPacket {
                        data: RuntimeData::Binary(self.bytes.clone()),
                        from_node: "test-client".into(),
                        to_node: Some("sink".into()),
                        session_id: "finite-source".into(),
                        sequence,
                        sub_sequence: 0,
                        metadata: HashMap::new(),
                    })
                    .unwrap();
            }
        }
        if self.fail {
            return Err(Error::Execution("finite source failure sentinel".into()));
        }
        Ok(())
    }
}

struct InputDriven;

#[async_trait::async_trait]
impl AsyncStreamingNode for InputDriven {
    fn node_type(&self) -> &str {
        "InputDriven"
    }
    async fn process(&self, data: RuntimeData) -> Result<RuntimeData> {
        Ok(data)
    }
}

struct Fixture {
    sources: Vec<Arc<FiniteSource>>,
    input: crate::transport::SessionPacketSender,
    shutdown: mpsc::Sender<()>,
    outputs: mpsc::Receiver<RuntimeData>,
    retained_outputs: Vec<remotemedia_types::SharedBytes>,
    running: JoinHandle<Result<()>>,
}

impl Fixture {
    fn start(count: usize, input_root: bool, fail: bool) -> Self {
        let ids: Vec<_> = (0..count).map(|n| format!("source-{n}")).collect();
        let mut nodes: Vec<_> = ids.iter().map(|id| (id.as_str(), "FiniteSource")).collect();
        if input_root {
            nodes.push(("input", "InputDriven"));
        }
        nodes.push(("sink", "InputDriven"));
        let mut edges: Vec<_> = ids.iter().map(|id| (id.as_str(), "sink")).collect();
        if input_root {
            edges.push(("input", "sink"));
        }
        let (output, outputs) = ClientOutputRouter::new(1);
        let (mut router, shutdown) = SessionRouter::new(
            "finite-source".into(),
            Arc::new(create_test_manifest(nodes, edges)),
            Arc::new(StreamingNodeRegistry::new()),
            output,
        )
        .unwrap();
        let sources = ids
            .into_iter()
            .map(|id| {
                let source = Arc::new(FiniteSource {
                    requested: Notify::new(),
                    release: Notify::new(),
                    calls: AtomicUsize::new(0),
                    retained: std::sync::Mutex::new(None),
                    inject_ingress: std::sync::Mutex::new(None),
                    bytes: vec![3, 1, 4, 1].into(),
                    fail,
                });
                router
                    .cached_nodes
                    .insert(id, Box::new(AsyncNodeWrapper(source.clone())));
                source
            })
            .collect();
        if input_root {
            router.cached_nodes.insert(
                "input".into(),
                Box::new(AsyncNodeWrapper(Arc::new(InputDriven))),
            );
        }
        router.cached_nodes.insert(
            "sink".into(),
            Box::new(AsyncNodeWrapper(Arc::new(InputDriven))),
        );
        let input = router.get_input_sender();
        Self {
            sources,
            input,
            shutdown,
            outputs: outputs.data_rx,
            retained_outputs: Vec::new(),
            running: tokio::spawn(router.run_after_init()),
        }
    }

    async fn requested(&self) {
        for source in &self.sources {
            tokio::time::timeout(Duration::from_secs(2), source.requested.notified())
                .await
                .unwrap();
        }
    }

    async fn receive(&mut self) {
        let RuntimeData::Binary(bytes) =
            tokio::time::timeout(Duration::from_secs(2), self.outputs.recv())
                .await
                .unwrap()
                .unwrap()
        else {
            panic!("expected retained binary owner");
        };
        assert!(self
            .sources
            .iter()
            .any(|source| bytes.as_ptr() == source.bytes.as_ptr()));
        assert_eq!(bytes.as_ref(), &[3, 1, 4, 1]);
        self.retained_outputs.push(bytes);
    }

    async fn still_running(&mut self) {
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut self.running)
                .await
                .is_err()
        );
    }

    async fn join(&mut self) -> Result<()> {
        let result = tokio::time::timeout(Duration::from_secs(2), &mut self.running).await;
        if result.is_err() {
            let _ = self.shutdown.send(()).await;
            self.running.abort();
            let _ = (&mut self.running).await;
            panic!("finite-source session failed to terminate");
        }
        let result = result.unwrap().unwrap();
        for source in &self.sources {
            assert_eq!(source.calls.load(Ordering::SeqCst), 2);
            assert!(source
                .retained
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .finish_source()
                .is_err());
            assert_eq!(source.bytes.as_ref(), &[3, 1, 4, 1]);
        }
        // Release the test's node owners too: only received output owners remain.
        self.sources.clear();
        for bytes in &self.retained_outputs {
            assert_eq!(bytes.as_ref(), &[3, 1, 4, 1]);
        }
        result
    }
}

#[tokio::test]
async fn finite_source_drains_backpressured_tail_with_ingress_still_owned() {
    let mut fixture = Fixture::start(1, false, false);
    fixture.requested().await;
    fixture.receive().await;
    fixture.still_running().await;
    fixture.sources[0].release.notify_one();
    fixture.still_running().await;
    fixture.receive().await;
    fixture.receive().await;
    fixture.join().await.unwrap();
    assert!(fixture.input.is_closed());
    assert!(fixture.outputs.recv().await.is_none());
}

#[tokio::test]
async fn every_paced_source_must_finish_before_session_eof() {
    let mut fixture = Fixture::start(2, false, false);
    fixture.requested().await;
    fixture.receive().await;
    fixture.receive().await;
    fixture.sources[0].release.notify_one();
    fixture.receive().await;
    fixture.receive().await;
    fixture.still_running().await;
    fixture.sources[1].release.notify_one();
    fixture.receive().await;
    fixture.receive().await;
    fixture.join().await.unwrap();
    assert!(fixture.outputs.recv().await.is_none());
}

#[tokio::test]
async fn reactive_root_keeps_ingress_live_after_paced_source_finishes() {
    let mut fixture = Fixture::start(1, true, false);
    fixture.requested().await;
    fixture.receive().await;
    fixture.sources[0].release.notify_one();
    fixture.receive().await;
    fixture.receive().await;
    fixture.still_running().await;
    assert!(!fixture.input.is_closed());
    fixture.shutdown.send(()).await.unwrap();
    fixture.join().await.unwrap();
}

#[tokio::test]
async fn completion_request_does_not_hide_a_tick_failure() {
    let mut fixture = Fixture::start(1, false, true);
    fixture.requested().await;
    fixture.receive().await;
    fixture.sources[0].release.notify_one();
    let error = fixture.join().await.unwrap_err().to_string();
    assert!(error.contains("finite source failure sentinel"), "{error}");
}

#[tokio::test]
async fn cancellation_invalidates_a_pending_completion_request() {
    let mut fixture = Fixture::start(1, false, false);
    fixture.requested().await;
    fixture.receive().await;
    fixture.shutdown.send(()).await.unwrap();
    fixture.join().await.unwrap();
    assert!(fixture.outputs.recv().await.is_none());
}

#[tokio::test]
async fn autonomous_eof_drains_packets_already_accepted_at_ingress() {
    let mut fixture = Fixture::start(1, false, false);
    *fixture.sources[0].inject_ingress.lock().unwrap() = Some(fixture.input.clone());
    fixture.requested().await;
    fixture.receive().await;
    fixture.sources[0].release.notify_one();
    // Two source tail outputs plus eight accepted client packets reach the same
    // downstream sink without changing the shared owner or range.
    for _ in 0..10 {
        fixture.receive().await;
    }
    fixture.join().await.unwrap();
    assert!(fixture.input.is_closed());
    assert!(fixture.outputs.recv().await.is_none());
}
