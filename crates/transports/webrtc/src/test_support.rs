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
    let manifest = Arc::new(Manifest {
        nodes: vec![NodeManifest {
            id: "calc".into(),
            node_type: "CalculatorNode".into(),
            params: serde_json::json!({}),
            ..Default::default()
        }],
        ..Default::default()
    });
    core_session_with_manifest(policy, manifest).await
}

pub(crate) async fn core_session_with_manifest(
    policy: CopyPolicy,
    manifest: Arc<Manifest>,
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
    let session = executor.create_session(manifest).await.unwrap();
    let control = executor.control_bus().get(&session.session_id).unwrap();
    assert!(control
        .transport_copy_context()
        .same_authority(&session.transport_copy_context()));
    (executor, session, control)
}

/// Identical external storage barriers for byte and native-sample boundaries.
pub(crate) fn guarded_payloads() -> (
    remotemedia_types::RuntimeData,
    remotemedia_types::RuntimeData,
    Arc<AtomicBool>,
) {
    use remotemedia_types::{RuntimeData, SharedBuffer};
    let deny = Arc::new(AtomicBool::new(false));
    let binary = RuntimeData::Binary(SharedBuffer::from_owner(Guarded {
        values: vec![42u8; 16],
        deny: deny.clone(),
    }));
    let audio = RuntimeData::Audio {
        samples: SharedBuffer::from_owner(Guarded {
            values: vec![0.5f32; 4],
            deny: deny.clone(),
        })
        .into(),
        sample_rate: 48000,
        channels: 2,
        stream_id: None,
        timestamp_us: None,
        arrival_ts_us: None,
        metadata: None,
    };
    (binary, audio, deny)
}
