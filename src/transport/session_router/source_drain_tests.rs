use super::{tests::create_test_manifest, *};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Notify;

struct HeldTick {
    release: Arc<Notify>,
    calls: Arc<AtomicUsize>,
    finished: Arc<AtomicBool>,
    bytes: remotemedia_types::SharedBytes,
}

#[async_trait::async_trait]
impl crate::nodes::AsyncStreamingNode for HeldTick {
    fn node_type(&self) -> &str {
        "HeldTick"
    }
    async fn process(&self, data: RuntimeData) -> Result<RuntimeData> {
        Ok(data)
    }
    fn pacing_nature(&self) -> crate::nodes::PacingNature {
        crate::nodes::PacingNature::SourceWall(100)
    }
    async fn tick(
        &self,
        _tick: crate::nodes::Tick,
        _session: Option<String>,
        mut output: Box<dyn FnMut(RuntimeData) -> Result<()> + Send>,
    ) -> Result<()> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        output(RuntimeData::Binary(self.bytes.clone()))?;
        self.release.notified().await;
        output(RuntimeData::Binary(self.bytes.clone()))?;
        self.finished.store(true, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn graceful_input_completion_drains_the_active_source_tick() {
    exercise_source_drain(false).await;
}

#[tokio::test]
async fn explicit_close_can_interrupt_a_source_already_draining() {
    exercise_source_drain(true).await;
}

async fn exercise_source_drain(cancel: bool) {
    let manifest = create_test_manifest(vec![("source", "HeldTick")], vec![]);
    let (output, mut outputs) = ClientOutputRouter::new(1);
    let (mut router, shutdown) = SessionRouter::new(
        "source-drain".into(),
        Arc::new(manifest),
        Arc::new(StreamingNodeRegistry::new()),
        output,
    )
    .unwrap();
    let release = Arc::new(Notify::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let finished = Arc::new(AtomicBool::new(false));
    let bytes: remotemedia_types::SharedBytes = vec![3, 1, 4, 1].into();
    router.cached_nodes.insert(
        "source".into(),
        Box::new(crate::nodes::AsyncNodeWrapper(Arc::new(HeldTick {
            release: release.clone(),
            calls: calls.clone(),
            finished: finished.clone(),
            bytes: bytes.clone(),
        }))),
    );
    let input = router.get_input_sender();
    let mut running = tokio::spawn(router.run_after_init());
    let first = tokio::time::timeout(Duration::from_secs(2), outputs.data_rx.recv())
        .await
        .unwrap()
        .unwrap();
    drop(input);
    // The final tick is held by the test, so completion here would mean it was
    // aborted. Always release/join before reporting a failed assertion.
    let premature = tokio::time::timeout(Duration::from_millis(50), &mut running).await;
    if premature.is_ok() {
        release.notify_one();
        panic!("input completion aborted the source tick instead of draining it");
    }
    if cancel {
        shutdown.send(()).await.unwrap();
    } else {
        release.notify_one();
    }
    let result = tokio::time::timeout(Duration::from_secs(2), &mut running).await;
    if result.is_err() {
        release.notify_one();
        running.abort();
        let _ = running.await;
        panic!("source drain did not terminate");
    }
    result.unwrap().unwrap().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(finished.load(Ordering::SeqCst), !cancel);
    let RuntimeData::Binary(first) = first else {
        panic!("binary output required")
    };
    assert_eq!(first.as_ptr(), bytes.as_ptr());
    assert_eq!(first.as_ref(), &[3, 1, 4, 1]);
    if !cancel {
        let RuntimeData::Binary(last) = outputs.data_rx.recv().await.unwrap() else {
            panic!("binary tail required")
        };
        assert_eq!(last.as_ptr(), bytes.as_ptr());
        assert_eq!(last.as_ref(), first.as_ref());
    }
    assert!(outputs.data_rx.recv().await.is_none());
}
