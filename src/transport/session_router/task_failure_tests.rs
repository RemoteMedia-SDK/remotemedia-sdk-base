use super::{
    tests::{create_test_manifest, BlockingCloseNode},
    *,
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Notify;

struct TrackedBytes {
    bytes: [u8; 4],
    released: Arc<AtomicBool>,
}
impl AsRef<[u8]> for TrackedBytes {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
impl Drop for TrackedBytes {
    fn drop(&mut self) {
        self.released.store(true, Ordering::SeqCst);
    }
}

struct PanickingNode {
    started: Arc<Notify>,
    release: Arc<Notify>,
}
#[async_trait::async_trait]
impl crate::nodes::AsyncStreamingNode for PanickingNode {
    fn node_type(&self) -> &str {
        "PanickingNode"
    }
    async fn process(&self, _data: RuntimeData) -> Result<RuntimeData> {
        self.started.notify_one();
        self.release.notified().await;
        panic!("active task panic sentinel");
    }
}

fn packet(node: &str, sequence: u64) -> DataPacket {
    DataPacket {
        data: RuntimeData::Text("start".into()),
        from_node: "test".into(),
        to_node: Some(node.into()),
        session_id: "active-failure".into(),
        sequence,
        sub_sequence: 0,
        metadata: HashMap::new(),
    }
}

#[tokio::test]
async fn task_panic_closes_session_while_ingress_is_idle() {
    exercise_failure(false).await;
}

#[tokio::test]
async fn task_panic_interrupts_backpressured_ingress() {
    exercise_failure(true).await;
}

async fn exercise_failure(backpressured: bool) {
    let manifest = create_test_manifest(
        vec![("bad", "PanickingNode"), ("blocked", "BlockingCloseNode")],
        vec![],
    );
    let (output, _unread) = ClientOutputRouter::new(1);
    let (mut router, _keep_shutdown_open) = SessionRouter::new(
        "active-failure".into(),
        Arc::new(manifest),
        Arc::new(StreamingNodeRegistry::new()),
        output,
    )
    .unwrap();
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let blocked_started = Arc::new(Notify::new());
    let stopped = Arc::new(AtomicBool::new(false));
    router.cached_nodes.insert(
        "bad".into(),
        Box::new(crate::nodes::AsyncNodeWrapper(Arc::new(PanickingNode {
            started: started.clone(),
            release: release.clone(),
        }))),
    );
    router.cached_nodes.insert(
        "blocked".into(),
        Box::new(crate::nodes::AsyncNodeWrapper(Arc::new(
            BlockingCloseNode {
                started: blocked_started.clone(),
                stopped: stopped.clone(),
            },
        ))),
    );
    let control = SessionControl::new("active-failure");
    let mut close = control.close_subscriber();
    router.attach_control(control).await;
    let input = router.get_input_sender();
    let running = tokio::spawn(router.run_after_init());
    let released = Arc::new(AtomicBool::new(false));
    let retained = remotemedia_types::SharedBuffer::from_owner(TrackedBytes {
        bytes: [3, 1, 4, 1],
        released: released.clone(),
    });
    let original = retained.as_ptr();
    let mut bad = packet("bad", 0);
    bad.data = RuntimeData::Binary(retained.clone());
    input.send(bad).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), started.notified())
        .await
        .unwrap();
    let producer = if backpressured {
        input.send(packet("blocked", 1)).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), blocked_started.notified())
            .await
            .unwrap();
        let sent = Arc::new(AtomicUsize::new(0));
        let count = sent.clone();
        let sender = input.clone();
        let producer = tokio::spawn(async move {
            for sequence in 2..(NODE_FANOUT_CAPACITY * 4) as u64 {
                if sender.send(packet("blocked", sequence)).await.is_err() {
                    return;
                }
                count.fetch_add(1, Ordering::SeqCst);
            }
            panic!("ingress never backpressured");
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while sent.load(Ordering::SeqCst) < NODE_FANOUT_CAPACITY || input.capacity() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        // No consumer can progress until cancellation. Verify the ingress queue
        // stays full before triggering failure in the independent bad node.
        assert!(
            tokio::time::timeout(Duration::from_millis(20), input.reserve())
                .await
                .is_err()
        );
        Some(producer)
    } else {
        None
    };
    release.notify_one();
    let error = tokio::time::timeout(Duration::from_secs(2), running)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("bad main") && error.contains("active task panic sentinel"),
        "{error}"
    );
    assert!(
        stopped.load(Ordering::SeqCst),
        "healthy sibling was not cleaned up"
    );
    let CloseReason::Error(reason) = close.recv().await.unwrap() else {
        panic!("normal close hid panic")
    };
    assert_eq!(reason, error);
    assert!(input.send(packet("bad", 9999)).await.is_err());
    if let Some(producer) = producer {
        tokio::time::timeout(Duration::from_secs(2), producer)
            .await
            .unwrap()
            .unwrap();
    }
    assert_eq!(retained.as_ptr(), original);
    assert_eq!(retained.as_ref(), &[3, 1, 4, 1]);
    assert!(!released.load(Ordering::SeqCst));
    drop(retained);
    assert!(
        released.load(Ordering::SeqCst),
        "failed task retained a payload owner"
    );
}

/// Every lane holds the node (and therefore this payload) while suspended.
struct ChildOwningNode {
    main: PanickingNode,
    lanes_started: [Arc<Notify>; 3],
    failing_lane: Option<usize>,
    returned_error: bool,
    lane_release: Arc<Notify>,
    payload: remotemedia_types::SharedBuffer<u8>,
    dropped: Arc<AtomicBool>,
}

impl ChildOwningNode {
    async fn hold_lane(&self, lane: usize) -> Result<()> {
        self.lanes_started[lane].notify_one();
        if self.failing_lane == Some(lane) {
            self.lane_release.notified().await;
            if self.returned_error {
                return Err(Error::Execution(
                    "child lane returned error sentinel".into(),
                ));
            }
            panic!("child lane panic sentinel");
        }
        std::future::pending().await
    }
}

impl Drop for ChildOwningNode {
    fn drop(&mut self) {
        assert_eq!(self.payload.as_ref(), &[3, 1, 4, 1]);
        self.dropped.store(true, Ordering::SeqCst);
    }
}

#[async_trait::async_trait]
impl StreamingNode for ChildOwningNode {
    fn node_type(&self) -> &str {
        "ChildOwningNode"
    }

    async fn process_async(
        &self,
        data: RuntimeData,
        _ctx: &dyn remotemedia_traits::runtime_context::NodeRuntimeContextRead,
    ) -> Result<RuntimeData> {
        crate::nodes::AsyncStreamingNode::process(&self.main, data).await
    }

    async fn process_multi_async(
        &self,
        _inputs: HashMap<String, RuntimeData>,
        _ctx: &dyn remotemedia_traits::runtime_context::NodeRuntimeContextRead,
    ) -> Result<RuntimeData> {
        unreachable!("single-input fixture")
    }

    fn is_multi_input(&self) -> bool {
        false
    }

    fn capability_subscriptions(&self) -> Vec<String> {
        vec!["audio:child-ownership".into()]
    }

    async fn on_capability_update(
        &self,
        _addr: &str,
        _caps: crate::capabilities::MediaCapabilities,
        _ctx: &dyn remotemedia_traits::runtime_context::NodeRuntimeContextRead,
    ) -> Result<()> {
        self.hold_lane(0).await
    }

    fn pacing_nature(&self) -> crate::nodes::PacingNature {
        crate::nodes::PacingNature::SourceWall(100)
    }

    async fn tick(
        &self,
        _tick: remotemedia_traits::streaming::Tick,
        _ctx: &dyn remotemedia_traits::runtime_context::NodeRuntimeContextRead,
        _cb: Box<dyn FnMut(RuntimeData) -> Result<()> + Send>,
    ) -> Result<()> {
        self.hold_lane(1).await
    }

    fn rpc_methods(&self) -> Vec<remotemedia_traits::rpc::RpcMethod> {
        use remotemedia_traits::rpc::{RpcMethod, RpcMode};
        vec![RpcMethod::new("hold", RpcMode::Reply)]
    }

    async fn call_rpc(
        &self,
        _request: remotemedia_traits::rpc::RpcRequest,
        _ctx: &dyn remotemedia_traits::runtime_context::NodeRuntimeContextRead,
        _replies: &mut remotemedia_traits::rpc::RpcResponder,
    ) -> Result<()> {
        self.hold_lane(2).await
    }
}

#[tokio::test]
async fn main_panic_joins_capability_pacer_and_nested_rpc_before_returning() {
    exercise_owned_failure(None, true).await;
}

#[tokio::test]
async fn capability_panic_joins_children_with_idle_main() {
    exercise_owned_failure(Some(0), false).await;
}

#[tokio::test]
async fn capability_panic_joins_children_with_suspended_main() {
    exercise_owned_failure(Some(0), true).await;
}

#[tokio::test]
async fn pacer_panic_joins_children_with_idle_main() {
    exercise_owned_failure(Some(1), false).await;
}

#[tokio::test]
async fn pacer_panic_joins_children_with_suspended_main() {
    exercise_owned_failure(Some(1), true).await;
}

async fn exercise_owned_failure(failing_lane: Option<usize>, active_main: bool) {
    exercise_owned_failure_kind(failing_lane, active_main, false).await;
}

#[tokio::test]
async fn pacer_returned_error_joins_children_with_idle_main() {
    exercise_owned_failure_kind(Some(1), false, true).await;
}

#[tokio::test]
async fn pacer_returned_error_joins_children_with_suspended_main() {
    exercise_owned_failure_kind(Some(1), true, true).await;
}

async fn exercise_owned_failure_kind(
    failing_lane: Option<usize>,
    active_main: bool,
    returned_error: bool,
) {
    let manifest = create_test_manifest(vec![("bad", "ChildOwningNode")], vec![]);
    let (output, _unread) = ClientOutputRouter::new(1);
    let (mut router, _keep_shutdown_open) = SessionRouter::new(
        "child-ownership".into(),
        Arc::new(manifest),
        Arc::new(StreamingNodeRegistry::new()),
        output,
    )
    .unwrap();
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let lane_release = Arc::new(Notify::new());
    let lanes_started = std::array::from_fn(|_| Arc::new(Notify::new()));
    let dropped = Arc::new(AtomicBool::new(false));
    let released = Arc::new(AtomicBool::new(false));
    let retained = remotemedia_types::SharedBuffer::from_owner(TrackedBytes {
        bytes: [3, 1, 4, 1],
        released: released.clone(),
    });
    router.cached_nodes.insert(
        "bad".into(),
        Box::new(ChildOwningNode {
            main: PanickingNode {
                started: started.clone(),
                release: release.clone(),
            },
            lanes_started: lanes_started.clone(),
            failing_lane,
            returned_error,
            lane_release: lane_release.clone(),
            payload: retained.clone(),
            dropped: dropped.clone(),
        }),
    );
    let control = SessionControl::new("child-ownership");
    let mut close = control.close_subscriber();
    let original = retained.as_ptr();
    router.attach_control(control.clone()).await;
    let input = router.get_input_sender();
    let mut running = tokio::spawn(router.run_after_init());
    if active_main {
        input.send(packet("bad", 0)).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), started.notified())
            .await
            .unwrap();
    }
    // Pacer startup proves that initialization and child registration completed.
    tokio::time::timeout(Duration::from_secs(2), lanes_started[1].notified())
        .await
        .unwrap();
    // Publish only after startup: the retained capability seed
    // must not block initialization before the child handlers are spawned.
    control.publish_capability("audio:child-ownership", Default::default());
    control
        .publish(
            &crate::transport::session_control::ControlAddress::node_in("bad").with_port("hold"),
            RuntimeData::Json(serde_json::json!({"corr_id": "held-call"})),
        )
        .await
        .unwrap();
    for lane in [&lanes_started[0], &lanes_started[2]] {
        tokio::time::timeout(Duration::from_secs(2), lane.notified())
            .await
            .unwrap();
    }
    assert!(!dropped.load(Ordering::SeqCst));
    if failing_lane.is_some() {
        lane_release.notify_one();
    } else {
        release.notify_one();
    }
    let result = tokio::time::timeout(Duration::from_secs(2), &mut running).await;
    if result.is_err() {
        // A failed regression must still drain the router and its children.
        _keep_shutdown_open.send(()).await.unwrap();
        running.await.unwrap().ok();
        panic!("child failure did not terminate the session");
    }
    let error = result.unwrap().unwrap().unwrap_err().to_string();
    let sentinel = if returned_error {
        "child lane returned error sentinel"
    } else if failing_lane.is_some() {
        "child lane panic sentinel"
    } else {
        "active task panic sentinel"
    };
    assert!(error.contains(sentinel), "{error}");
    if failing_lane == Some(1) {
        assert!(error.contains("pacer"), "{error}");
    }
    if failing_lane == Some(0) {
        assert!(
            error.contains("capability") && error.contains("audio:child-ownership"),
            "{error}"
        );
    }
    let CloseReason::Error(reason) = close.recv().await.unwrap() else {
        panic!("normal close hid child failure")
    };
    assert_eq!(reason, error);
    assert!(input.send(packet("bad", 9999)).await.is_err());
    assert!(
        dropped.load(Ordering::SeqCst),
        "session returned while a child still owned the node"
    );
    assert!(!released.load(Ordering::SeqCst));
    assert_eq!(retained.as_ptr(), original);
    assert_eq!(retained.as_ref(), &[3, 1, 4, 1]);
    drop(retained);
    assert!(released.load(Ordering::SeqCst));
}
