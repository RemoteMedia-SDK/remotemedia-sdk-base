use super::*;
use futures::FutureExt;
use remotemedia_core::manifest::NodeManifest;
use remotemedia_types::{CopyContext, CopyPolicy, CopyReason, RuntimeData};
use tokio::{
    net::TcpListener,
    time::{timeout, Duration},
};
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

type Client = WebSocketStream<MaybeTlsStream<TcpStream>>;

async fn read_json(client: &mut Client) -> Value {
    loop {
        if let Message::Text(text) = client.next().await.unwrap().unwrap() {
            return serde_json::from_str(&text).unwrap();
        }
    }
}

async fn send_request(client: &mut Client, method: &str, params: Value, id: u64) {
    client
        .send(Message::Text(
            json!({"jsonrpc":"2.0","method":method,"params":params,"id":id}).to_string(),
        ))
        .await
        .unwrap();
}

#[tokio::test]
async fn live_websocket_control_uses_session_policy_for_taps_and_publish() {
    for policy in [CopyPolicy::RequireZeroCopy, CopyPolicy::AllowCopies] {
        websocket_case(policy).await;
    }
}

async fn websocket_case(policy: CopyPolicy) {
    let manifest = Arc::new(Manifest {
        nodes: vec![NodeManifest {
            id: "pass".into(),
            node_type: "PassThrough".into(),
            params: json!({}),
            ..Default::default()
        }],
        ..Default::default()
    });
    let (executor, handle, control) =
        crate::test_support::core_session_with_manifest(policy, manifest.clone()).await;
    let copies = handle.transport_copy_context();
    let executor = Arc::new(executor);
    let core = remotemedia_core::transport::SharedPipelineSession::new("ws-copy", handle).unwrap();
    let shared = crate::peer::SharedPipelineSession::new(core, Some(control.clone()), false)
        .await
        .unwrap();
    let config = Arc::new(WebRtcTransportConfig {
        stun_servers: Vec::new(),
        turn_servers: Vec::new(),
        ..Default::default()
    });
    let state = Arc::new(SharedState::new(
        config.clone(),
        executor.clone(),
        manifest.clone(),
    ));
    let peer = Arc::new(
        ServerPeer::new("ws-copy".into(), &config, executor, manifest)
            .await
            .unwrap(),
    );
    let rtc = crate::test_support::peer().await;
    let _channel = rtc.create_data_channel("data", None).await.unwrap();
    peer.handle_offer_with_shared_session(
        rtc.create_offer(None).await.unwrap().sdp,
        shared.clone(),
    )
    .await
    .unwrap();
    state
        .server_peers
        .write()
        .await
        .insert("ws-copy".into(), peer);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        handle_connection(stream, state).await.unwrap();
    });
    let (mut client, _) = connect_async(format!("ws://{addr}")).await.unwrap();
    let work = async {
        send_request(
            &mut client,
            "peer.announce",
            json!({"peer_id":"ws-copy","capabilities":["data"]}),
            1,
        )
        .await;
        assert_eq!(read_json(&mut client).await["id"], 1);
        send_request(
            &mut client,
            "control.subscribe",
            json!({"topic":"pass.out"}),
            2,
        )
        .await;
        assert_eq!(read_json(&mut client).await["result"]["subscribed"], true);
        control.publish_tap("pass", None, RuntimeData::Binary(vec![42; 64].into()));
        let event = read_json(&mut client).await;
        if policy == CopyPolicy::AllowCopies {
            assert_eq!(event["params"]["payload"]["size"], 64);
            assert_eq!(event["method"], "control.event");
        } else {
            assert!(event["error"]["message"]
                .as_str()
                .unwrap()
                .contains(control.session_id()));
            assert_eq!(event["error"]["data"]["topic"], "pass.out");
        }
        // Use raw escaped base64 to ensure the production request parser does
        // not construct an owned payload Value before consulting the session.
        client.send(Message::Text(r#"{"params":{"payload":{"binary_b64":"\u002fw=="},"topic":"pass.in"},"method":"control.publish","id":3,"jsonrpc":"2.0"}"#.into())).await.unwrap();
        let first = read_json(&mut client).await;
        if policy == CopyPolicy::AllowCopies {
            let second = read_json(&mut client).await;
            let (reply, event) = if first["id"] == 3 {
                (first, second)
            } else {
                (second, first)
            };
            assert_eq!(reply["result"]["published"], true);
            assert_eq!(event["params"]["payload"]["b64"], "/w==");
        } else {
            assert_eq!(first["id"], 3);
            assert!(first["error"]["message"]
                .as_str()
                .unwrap()
                .contains(control.session_id()));
        }
        send_request(
            &mut client,
            "control.subscribe",
            json!({"topic":"__system__.out"}),
            4,
        )
        .await;
        while read_json(&mut client).await["id"] != 4 {}
        control.publish_tap("__system__", None, RuntimeData::Binary(vec![42; 64].into()));
        loop {
            let event = read_json(&mut client).await;
            if policy == CopyPolicy::AllowCopies && event["params"]["kind"] == "binary" {
                assert_eq!(event["params"]["topic"], "__system__.out");
                assert_eq!(event["params"]["payload"]["size"], 64);
                break;
            }
            if policy == CopyPolicy::RequireZeroCopy
                && event["error"]["data"]["topic"] == "__system__.out"
            {
                assert!(event["error"]["message"]
                    .as_str()
                    .unwrap()
                    .contains(control.session_id()));
                break;
            }
        }
        check_counts(&copies, policy);
    };
    let result = std::panic::AssertUnwindSafe(timeout(Duration::from_secs(20), work))
        .catch_unwind()
        .await;
    let _ = client.close(None).await;
    drop(client);
    timeout(Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap();
    shared.close().await.unwrap();
    rtc.close().await.unwrap();
    result.unwrap().expect("WebSocket control test timed out");
    check_counts(&copies, policy); // The ledger survives connection/session shutdown.
}

fn check_counts(copies: &CopyContext, policy: CopyPolicy) {
    let report = copies.report();
    let output = &report
        .entries
        .iter()
        .find(|(r, _)| *r == CopyReason::PortableSerialization)
        .unwrap()
        .1;
    let input = &report
        .entries
        .iter()
        .find(|(r, _)| *r == CopyReason::PortableDeserialization)
        .unwrap()
        .1;
    if policy == CopyPolicy::AllowCopies {
        assert_eq!((output.copied_bytes, input.copied_bytes), (129, 5));
    } else {
        assert_eq!((output.copied_bytes, input.copied_bytes), (0, 0));
        assert_eq!((output.rejected_bytes, input.rejected_bytes), (128, 11));
    }
}

#[tokio::test]
async fn websocket_raw_request_parser_preserves_errors_and_ordinary_signaling() {
    let state = Arc::new(SharedState::new(
        Arc::new(WebRtcTransportConfig::default()),
        Arc::new(PipelineExecutor::with_config(Default::default()).unwrap()),
        Arc::new(Manifest::default()),
    ));
    let peer_id = Arc::new(RwLock::new(None));
    let control = ControlSessionState::new();
    let (tx, mut rx) = mpsc::channel(8);
    for text in [
        "{",
        r#"{"jsonrpc":"2.0","method":"peer.list","params":1e999,"id":7}"#,
    ] {
        handle_message(text, &state, &peer_id, &tx, &control)
            .await
            .unwrap();
        let error: Value = serde_json::from_str(&rx.recv().await.unwrap()).unwrap();
        assert_eq!(error["error"]["code"], error_codes::PARSE_ERROR);
        assert_eq!(error["id"], Value::Null);
    }
    handle_message(
        r#"{"jsonrpc":"2.0","method":"control.publish","params":{},"id":8}"#,
        &state,
        &peer_id,
        &tx,
        &control,
    )
    .await
    .unwrap();
    let error: Value = serde_json::from_str(&rx.recv().await.unwrap()).unwrap();
    assert_eq!(error["error"]["code"], error_codes::INVALID_REQUEST);
    assert_eq!(error["id"], 8);
    handle_message(
        r#"{"jsonrpc":"2.0","method":"peer.list","params":{},"id":9}"#,
        &state,
        &peer_id,
        &tx,
        &control,
    )
    .await
    .unwrap();
    let response: Value = serde_json::from_str(&rx.recv().await.unwrap()).unwrap();
    assert_eq!(response["id"], 9);
    assert!(response.get("result").is_some());
}
