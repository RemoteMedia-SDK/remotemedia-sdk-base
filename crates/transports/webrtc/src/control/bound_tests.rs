//! A control channel bound to one session and one member's access.
use super::*;
use crate::generated::{
    Hello, Intercept, InterceptReply, Publish, SetNodeState, SetTelemetry, Subscribe,
};
use crate::test_support::core_session;
use remotemedia_types::CopyPolicy;
use tokio::time::timeout;

fn out(node: &str) -> PbAddress {
    core_addr_to_pb(&CoreAddress::node_out(node))
}

fn input(node: &str, port: Option<&str>) -> PbAddress {
    let mut addr = CoreAddress::node_in(node);
    if let Some(port) = port {
        addr = addr.with_port(port);
    }
    core_addr_to_pb(&addr)
}

fn binding(grants: Vec<ControlGrant>) -> ControlBinding {
    ControlBinding {
        member: "ana".into(),
        access: ControlAccess::Granted(grants),
    }
}

fn text(value: &str) -> crate::generated::DataBuffer {
    crate::adapters::runtime_data_to_data_buffer(&RuntimeData::Text(value.into()))
}

fn json(value: serde_json::Value) -> crate::generated::DataBuffer {
    crate::adapters::runtime_data_to_data_buffer(&RuntimeData::Json(value))
}

struct Harness {
    control: Arc<SessionControl>,
    tx: mpsc::Sender<Outbound>,
    rx: mpsc::Receiver<Outbound>,
    tasks: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    scope: RpcCallScope,
    binding: ControlBinding,
    _executor: remotemedia_core::transport::PipelineExecutor,
    session: remotemedia_core::transport::SessionHandle,
}

impl Harness {
    async fn new(grants: Vec<ControlGrant>) -> Self {
        let (executor, session, control) = core_session(CopyPolicy::AllowCopies).await;
        let (tx, rx) = mpsc::channel(32);
        Self {
            control,
            tx,
            rx,
            tasks: Arc::new(Mutex::new(Vec::new())),
            scope: RpcCallScope::default(),
            binding: binding(grants),
            _executor: executor,
            session,
        }
    }

    async fn send(&self, op: PbOp) -> Result<(), ErrorEvent> {
        dispatch_frame(
            ControlFrame { op: Some(op) },
            &self.control,
            &self.tx,
            &self.tasks,
            &self.scope,
            Some(&self.binding),
        )
        .await
    }

    async fn refused(&self, op: PbOp) -> String {
        let error = self.send(op).await.expect_err("the operation is not granted");
        assert_eq!(error.code, ControlErrorCode::Unauthorized as i32, "{error:?}");
        error.message
    }

    async fn next_tap_text(&mut self) -> String {
        let outbound = timeout(Duration::from_secs(5), self.rx.recv())
            .await
            .expect("a tap arrives")
            .expect("channel open");
        match outbound {
            Outbound::Payload { data: RuntimeData::Text(text), .. } => text.to_string(),
            _ => panic!("expected a text tap"),
        }
    }

    async fn close(mut self) {
        for task in self.tasks.lock().await.drain(..) {
            task.abort();
        }
        self.session.close().await.unwrap();
    }
}

#[tokio::test]
async fn ungranted_operations_are_refused_with_the_operation_and_node() {
    let harness = Harness::new(vec![ControlGrant::new(ControlOp::Tap, "calc")]).await;

    harness
        .send(PbOp::Subscribe(Subscribe { addr: Some(out("calc")) }))
        .await
        .expect("the granted tap is accepted");

    let message = harness
        .refused(PbOp::Subscribe(Subscribe { addr: Some(out("other")) }))
        .await;
    assert_eq!(message, "not granted: tap on 'other'");

    let message = harness
        .refused(PbOp::Publish(Publish {
            addr: Some(input("calc", None)),
            data: Some(text("hello")),
        }))
        .await;
    assert_eq!(message, "not granted: publish on 'calc'");

    let message = harness
        .refused(PbOp::Intercept(Intercept { addr: Some(out("calc")), deadline_ms: 100 }))
        .await;
    assert_eq!(message, "not granted: intercept on 'calc'");

    let message = harness
        .refused(PbOp::InterceptReply(InterceptReply { correlation_id: 1, decision: None }))
        .await;
    assert_eq!(message, "not granted: intercept");

    let message = harness
        .refused(PbOp::SetNodeState(SetNodeState { node_id: "calc".into(), state: 2 }))
        .await;
    assert_eq!(message, "not granted: node_state on 'calc'");
    assert_eq!(
        harness.control.node_state("calc"),
        NodeState::Enabled,
        "a refused frame changes nothing"
    );

    let message = harness
        .refused(PbOp::SetTelemetry(SetTelemetry { level: "full".into(), interval_ms: 0 }))
        .await;
    assert!(message.contains("telemetry"), "{message}");

    harness.close().await;
}

#[tokio::test]
async fn an_rpc_grant_allows_only_its_listed_methods() {
    let harness = Harness::new(vec![
        ControlGrant::new(ControlOp::Rpc, "calc").methods(["add"]),
        ControlGrant::new(ControlOp::Publish, "chat"),
    ])
    .await;

    let call = |node: &str, method: &str| {
        PbOp::Publish(Publish {
            addr: Some(input(node, Some(method))),
            data: Some(json(serde_json::json!({"corr_id": "c1", "args": []}))),
        })
    };
    let message = harness.refused(call("calc", "reset")).await;
    assert_eq!(message, "not granted: rpc 'reset' on 'calc'");
    // A publish grant is not a grant to call methods.
    let message = harness.refused(call("chat", "reset")).await;
    assert_eq!(message, "not granted: rpc 'reset' on 'chat'");
    // The listed method passes the check (what the node does with it is its own).
    if let Err(error) = harness.send(call("calc", "add")).await {
        assert_ne!(error.code, ControlErrorCode::Unauthorized as i32, "{error:?}");
    }
    // The reply port of the callable node may be tapped; its output may not.
    harness
        .send(PbOp::Subscribe(Subscribe {
            addr: Some(core_addr_to_pb(&CoreAddress::node_out("calc").with_port("__reply__"))),
        }))
        .await
        .expect("reply port");
    harness
        .refused(PbOp::Subscribe(Subscribe { addr: Some(out("calc")) }))
        .await;

    harness.close().await;
}

#[tokio::test]
async fn an_own_tap_delivers_only_the_members_frames() {
    let mut harness = Harness::new(vec![ControlGrant::new(ControlOp::Tap, "calc").own()]).await;
    harness
        .send(PbOp::Subscribe(Subscribe { addr: Some(out("calc")) }))
        .await
        .unwrap();

    let control = Arc::clone(&harness.control);
    for (who, line) in [
        (Some("ben"), "ben speaks"),
        (Some("ana"), "ana speaks"),
        (None, "nobody speaks"),
        (Some("ben"), "ben again"),
        (Some("ana"), "ana again"),
    ] {
        control
            .on_node_output_from("calc", None, RuntimeData::Text(line.into()), who)
            .await;
    }

    assert_eq!(harness.next_tap_text().await, "ana speaks");
    assert_eq!(harness.next_tap_text().await, "ana again");
    assert!(harness.rx.try_recv().is_err(), "nothing of ben's and nothing unattributed");
    harness.close().await;
}

#[tokio::test]
async fn an_unrestricted_tap_delivers_every_members_frames() {
    let mut harness = Harness::new(vec![ControlGrant::new(ControlOp::Tap, "calc")]).await;
    harness
        .send(PbOp::Subscribe(Subscribe { addr: Some(out("calc")) }))
        .await
        .unwrap();
    let control = Arc::clone(&harness.control);
    for (who, line) in [(Some("ben"), "ben speaks"), (Some("ana"), "ana speaks")] {
        control
            .on_node_output_from("calc", None, RuntimeData::Text(line.into()), who)
            .await;
    }
    assert_eq!(harness.next_tap_text().await, "ben speaks");
    assert_eq!(harness.next_tap_text().await, "ana speaks");
    harness.close().await;
}

/// The session a peer names in `Hello` does not select the session of a bound
/// channel: it stays attached to the session it was admitted to.
#[tokio::test]
async fn hello_cannot_select_another_session() {
    let (_executor_a, mut mine, my_control) = core_session(CopyPolicy::AllowCopies).await;
    let (_executor_b, mut theirs, their_control) = core_session(CopyPolicy::AllowCopies).await;
    assert_ne!(mine.session_id, theirs.session_id);

    let offer = crate::test_support::peer().await;
    let answer = crate::test_support::peer().await;
    let sender = offer
        .create_data_channel(CONTROL_CHANNEL_LABEL, None)
        .await
        .unwrap();
    let bound = Arc::clone(&my_control);
    answer.on_data_channel(Box::new(move |dc| {
        let bound = Arc::clone(&bound);
        Box::pin(async move {
            attach_bound_control_channel(
                dc,
                bound,
                binding(vec![ControlGrant::new(ControlOp::Tap, "calc")]),
            )
            .await;
        })
    }));
    let (tx, mut rx) = mpsc::unbounded_channel();
    sender.on_message(Box::new(move |message| {
        let tx = tx.clone();
        Box::pin(async move {
            let _ = tx.send(ControlEvent::decode(message.data).unwrap());
        })
    }));

    let their_id = theirs.session_id.clone();
    let my_id = mine.session_id.clone();
    let work = async {
        crate::test_support::negotiate(&offer, &answer).await;
        while sender.ready_state()
            != webrtc::data_channel::data_channel_state::RTCDataChannelState::Open
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        for op in [
            PbOp::Hello(Hello {
                session_id: their_id.clone(),
                attach_id: "intruder".into(),
                participant: None,
            }),
            PbOp::Subscribe(Subscribe { addr: Some(out("calc")) }),
        ] {
            sender
                .send(&ControlFrame { op: Some(op) }.encode_to_vec().into())
                .await
                .unwrap();
        }
        match rx.recv().await.unwrap().event {
            Some(PbEvent::Attached(attached)) => assert_eq!(attached.session_id, my_id),
            other => panic!("expected Attached, got {other:?}"),
        }
        while my_control.tap_receiver_count("calc", None) == 0 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            their_control.tap_receiver_count("calc", None),
            0,
            "the named session is not tapped"
        );
        // Output of the other session does not arrive; output of its own does.
        their_control.publish_tap("calc", None, RuntimeData::Text("theirs".into()));
        my_control.publish_tap("calc", None, RuntimeData::Text("mine".into()));
        match rx.recv().await.unwrap().event {
            Some(PbEvent::Tap(tap)) => {
                let copies = my_control.transport_copy_context();
                let RuntimeData::Text(text) = pb_to_runtime_data(tap.data.unwrap(), &copies).unwrap()
                else {
                    panic!("text tap")
                };
                assert_eq!(&*text, "mine");
            }
            other => panic!("expected a tap, got {other:?}"),
        }
    };
    let result = timeout(Duration::from_secs(25), work).await;
    sender.close().await.unwrap();
    offer.close().await.unwrap();
    answer.close().await.unwrap();
    mine.close().await.unwrap();
    theirs.close().await.unwrap();
    result.expect("bound control channel timed out");
}
