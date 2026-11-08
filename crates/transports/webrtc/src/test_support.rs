//! Shared ownership barriers, core sessions and loopback-only peer fixtures.
use webrtc::api::{setting_engine::SettingEngine, APIBuilder};
use webrtc::ice::network_type::NetworkType;
use webrtc::peer_connection::RTCPeerConnection;

pub(crate) async fn peer() -> RTCPeerConnection {
    let mut settings = SettingEngine::default();
    settings.set_network_types(vec![NetworkType::Udp4]);
    settings.set_include_loopback_candidate(true);
    settings.set_ip_filter(Box::new(|ip| ip.is_loopback()));
    APIBuilder::new()
        .with_setting_engine(settings)
        .build()
        .new_peer_connection(Default::default())
        .await
        .unwrap()
}

pub(crate) async fn negotiate(offer: &RTCPeerConnection, answer: &RTCPeerConnection) {
    let mut gathered = offer.gathering_complete_promise().await;
    offer
        .set_local_description(offer.create_offer(None).await.unwrap())
        .await
        .unwrap();
    gathered.recv().await;
    answer
        .set_remote_description(offer.local_description().await.unwrap())
        .await
        .unwrap();
    let mut gathered = answer.gathering_complete_promise().await;
    answer
        .set_local_description(answer.create_answer(None).await.unwrap())
        .await
        .unwrap();
    gathered.recv().await;
    offer
        .set_remote_description(answer.local_description().await.unwrap())
        .await
        .unwrap();
}

use remotemedia_core::manifest::{Manifest, NodeManifest};
use remotemedia_core::transport::session_control::SessionControl;
use remotemedia_core::transport::{ExecutorConfig, PipelineExecutor};
use remotemedia_types::CopyPolicy;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

pub(crate) struct Guarded<T> {
    pub(crate) values: Vec<T>,
    pub(crate) deny: Arc<AtomicBool>,
}
impl<T> AsRef<[T]> for Guarded<T> {
    fn as_ref(&self) -> &[T] {
        assert!(
            !self.deny.load(Ordering::SeqCst),
            "payload accessed before authorization"
        );
        &self.values
    }
}

pub(crate) async fn core_session(
    policy: CopyPolicy,
) -> (
    PipelineExecutor,
    remotemedia_core::transport::SessionHandle,
    Arc<SessionControl>,
) {
    let executor = PipelineExecutor::with_config(ExecutorConfig {
        transport_materialization_policy: policy,
        ..Default::default()
    })
    .unwrap();
    let session = executor
        .create_session(Arc::new(Manifest {
            nodes: vec![NodeManifest {
                id: "calc".into(),
                node_type: "CalculatorNode".into(),
                params: serde_json::json!({}),
                ..Default::default()
            }],
            ..Default::default()
        }))
        .await
        .unwrap();
    let control = executor.control_bus().get(&session.session_id).unwrap();
    assert!(control
        .transport_copy_context()
        .same_authority(&session.transport_copy_context()));
    (executor, session, control)
}
