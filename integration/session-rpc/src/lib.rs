#[cfg(test)]
mod tests {
    use remotemedia_sdk_base::{
        data::RuntimeData,
        loadable::factory::{wrap_ffi_factory, LoadableNodeBundle},
        manifest::{Manifest, ManifestMetadata, NodeManifest},
        nodes::StreamingNodeRegistry,
        transport::{
            session_control::{ControlAddress, SessionControl},
            session_router::{ClientOutputRouter, SessionRouter},
        },
    };
    use std::{sync::Arc, time::Duration};
    use tokio::sync::{broadcast, mpsc};

    async fn deadline<T>(future: impl std::future::Future<Output = T>) -> T {
        tokio::time::timeout(Duration::from_secs(5), future)
            .await
            .expect("operation timed out")
    }

    async fn publish(control: &SessionControl, method: &str, id: &str, args: serde_json::Value) {
        deadline(control.publish(
            &ControlAddress::node_in("manager").with_port(method),
            RuntimeData::Json(serde_json::json!({"corr_id":id,"args":args,"kwargs":{}})),
        ))
        .await
        .unwrap();
    }

    async fn reply(rx: &mut broadcast::Receiver<RuntimeData>) -> serde_json::Value {
        match deadline(rx.recv()).await.unwrap() {
            RuntimeData::Json(value) => {
                assert_eq!(value["__aux_port__"], "__reply__");
                value["payload"].clone()
            }
            _ => panic!("expected JSON RPC envelope"),
        }
    }

    async fn start(
        session: &str,
        registry: StreamingNodeRegistry,
    ) -> (
        Arc<SessionControl>,
        broadcast::Receiver<RuntimeData>,
        mpsc::Receiver<remotemedia_sdk_base::transport::TransportData>,
        mpsc::Sender<()>,
        tokio::task::JoinHandle<remotemedia_sdk_base::Result<()>>,
    ) {
        let manifest = Manifest {
            version: "v1".into(),
            metadata: ManifestMetadata {
                name: "rpc-contract".into(),
                ..Default::default()
            },
            nodes: vec![NodeManifest {
                id: "manager".into(),
                node_type: "SessionRpcExample".into(),
                params: serde_json::json!({}),
                ..Default::default()
            }],
            connections: vec![],
            python_env: None,
            plugins: vec![],
        };
        let (output, receivers) = ClientOutputRouter::new(32);
        let (mut router, shutdown) = SessionRouter::new(
            session.into(),
            Arc::new(manifest),
            Arc::new(registry),
            output,
        )
        .unwrap();
        let control = SessionControl::new(session);
        let replies = control
            .subscribe(&ControlAddress::node_out("manager").with_port("__reply__"))
            .unwrap();
        router.attach_control(control.clone()).await;
        let run = tokio::spawn(router.run_public());
        // A media output proves initialization and lane registration have completed.
        control
            .publish(
                &ControlAddress::node_in("manager"),
                RuntimeData::Text("hold_media".into()),
            )
            .await
            .unwrap();
        let mut received = receivers.data_rx;
        let first = deadline(received.recv()).await.unwrap().data;
        assert_eq!(
            first,
            RuntimeData::Json(serde_json::json!({"session":session,"node":"manager"}))
        );
        (control, replies, received, shutdown, run)
    }

    fn registry() -> StreamingNodeRegistry {
        let mut registry = StreamingNodeRegistry::new();
        registry.register(wrap_ffi_factory(session_rpc_example::Factory));
        registry
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rpc_bypasses_blocked_media_and_preserves_wire_protocol() {
        let (control, mut replies, mut media, shutdown, run) =
            start("host-session", registry()).await;
        // Saturate ordinary media ingress. RPC must not wait for its capacity.
        let input = control.input_sender().await.unwrap();
        while input
            .try_send(
                remotemedia_sdk_base::transport::session_router::DataPacket {
                    data: RuntimeData::Text("queued".into()),
                    from_node: "test".into(),
                    to_node: Some("manager".into()),
                    session_id: "host-session".into(),
                    sequence: 0,
                    sub_sequence: 0,
                    metadata: Default::default(),
                },
            )
            .is_ok()
        {}
        publish(&control, "__rpc_meta__", "meta", serde_json::json!([])).await;
        let meta = reply(&mut replies).await;
        assert_eq!(meta["data"]["watch"], "stream");
        assert_eq!(meta["kind"], "value");
        publish(&control, "notify", "ff", serde_json::json!([])).await;
        control
            .publish(
                &ControlAddress::node_in("manager").with_port("identity"),
                RuntimeData::Json(
                    serde_json::json!({"corr_id":"id","args":[],"kwargs":{},"session_id":"forged"}),
                ),
            )
            .await
            .unwrap();
        let identity = reply(&mut replies).await;
        assert_eq!(identity["corr_id"], "id");
        assert_eq!(identity["data"]["session"], "host-session");
        publish(&control, "watch", "stream", serde_json::json!([3])).await;
        for n in 0..3 {
            assert_eq!(reply(&mut replies).await["data"], n);
        }
        assert_eq!(reply(&mut replies).await["kind"], "end");
        publish(&control, "fail", "error", serde_json::json!([])).await;
        assert_eq!(reply(&mut replies).await["kind"], "error");
        publish(&control, "missing", "unknown", serde_json::json!([])).await;
        assert_eq!(reply(&mut replies).await["kind"], "error");
        assert!(
            media.try_recv().is_err(),
            "RPC replies must not enter media output"
        );
        shutdown.send(()).await.unwrap();
        deadline(run).await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancellation_releases_calls_and_sessions_do_not_cross() {
        let (a, mut ar, _, stop_a, run_a) = start("a", registry()).await;
        let (b, mut br, _, stop_b, run_b) = start("b", registry()).await;
        publish(&a, "hold", "same", serde_json::json!([])).await;
        assert_eq!(reply(&mut ar).await["data"], "started");
        publish(&b, "identity", "same", serde_json::json!([])).await;
        assert_eq!(reply(&mut br).await["data"]["session"], "b");
        publish(&a, "hold.__cancel__", "same", serde_json::json!([])).await;
        deadline(async {
            loop {
                publish(&a, "identity", "check", serde_json::json!([])).await;
                if reply(&mut ar).await["data"]["active"] == 0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        publish(&a, "hold", "shutdown", serde_json::json!([])).await;
        assert_eq!(reply(&mut ar).await["data"], "started");
        stop_a.send(()).await.unwrap();
        stop_b.send(()).await.unwrap();
        deadline(run_a).await.unwrap().unwrap();
        deadline(run_b).await.unwrap().unwrap();
        assert!(a
            .publish(
                &ControlAddress::node_in("manager").with_port("identity"),
                RuntimeData::Json(serde_json::json!({"corr_id":"late"}))
            )
            .await
            .is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn overload_and_malformed_arguments_fail_without_blocking_cancellation() {
        let (control, mut replies, _, shutdown, run) = start("limits", registry()).await;
        for i in 0..32 {
            publish(
                &control,
                "hold",
                &format!("hold:{i}"),
                serde_json::json!([]),
            )
            .await;
            assert_eq!(reply(&mut replies).await["data"], "started");
        }
        publish(&control, "identity", "overload", serde_json::json!([])).await;
        let error = reply(&mut replies).await;
        assert_eq!(error["kind"], "error");
        assert!(error["data"]
            .as_str()
            .unwrap()
            .contains("concurrency limit"));
        publish(&control, "hold.__cancel__", "hold:0", serde_json::json!([])).await;
        publish(&control, "watch", "badargs", serde_json::json!(["bad"])).await;
        assert_eq!(reply(&mut replies).await["kind"], "error");
        assert!(control
            .publish(
                &ControlAddress::node_in("manager").with_port("identity"),
                RuntimeData::Json(serde_json::json!({"corr_id":null}))
            )
            .await
            .is_err());
        shutdown.send(()).await.unwrap();
        deadline(run).await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rpc_reply_tap_reports_overflow_explicitly() {
        let (control, mut replies, _, shutdown, run) = start("overflow", registry()).await;
        publish(&control, "watch", "overflow", serde_json::json!([2048])).await;
        deadline(async {
            while replies.len() <= 1024 {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(matches!(
            replies.recv().await,
            Err(broadcast::error::RecvError::Lagged(_))
        ));
        shutdown.send(()).await.unwrap();
        deadline(run).await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires SESSION_RPC_DLL built from the SDK example"]
    async fn real_dll_session_and_rpc_round_trip() {
        let path = std::env::var("SESSION_RPC_DLL").expect("set SESSION_RPC_DLL");
        let bundle = LoadableNodeBundle::load(std::path::Path::new(&path)).unwrap();
        let mut registry = StreamingNodeRegistry::new();
        bundle.register_into(&mut registry);
        let (control, mut replies, _, shutdown, run) = start("dll-session", registry).await;
        publish(&control, "identity", "dll", serde_json::json!([])).await;
        assert_eq!(reply(&mut replies).await["data"]["session"], "dll-session");
        publish(&control, "watch", "stream", serde_json::json!([2])).await;
        assert_eq!(reply(&mut replies).await["data"], 0);
        assert_eq!(reply(&mut replies).await["data"], 1);
        assert_eq!(reply(&mut replies).await["kind"], "end");
        shutdown.send(()).await.unwrap();
        deadline(run).await.unwrap().unwrap();
    }
}
