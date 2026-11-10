use super::*;
use crate::manifest::{Manifest, NodeManifest};
use crate::transport::{ExecutorConfig, PipelineExecutor};
use remotemedia_types::{CopyPolicy, CopyReason};

#[tokio::test]
async fn shared_session_transport_authority_is_isolated_and_survives_join() {
    for policy in [CopyPolicy::AllowCopies, CopyPolicy::RequireZeroCopy] {
        let executor = PipelineExecutor::with_config(ExecutorConfig {
            transport_materialization_policy: policy,
            ..Default::default()
        })
        .unwrap();
        let manifest = Arc::new(Manifest {
            nodes: vec![NodeManifest {
                id: "calc".into(),
                node_type: "CalculatorNode".into(),
                params: serde_json::json!({}),
                ..Default::default()
            }],
            ..Default::default()
        });
        let mut contexts = Vec::new();
        for index in 0..2 {
            let handle = executor.create_session(manifest.clone()).await.unwrap();
            let original = handle.transport_copy_context();
            let node = handle
                .node_materialization_copies()
                .context("calc")
                .unwrap();
            let shared = SharedPipelineSession::new(format!("peer-{index}"), handle).unwrap();
            let attached = shared.transport_copy_context().await;
            let identity = attached.same_authority(&original);
            let node_is_separate = !attached.same_authority(&node);
            let expected_boundary =
                format!("session/{}/transport/materialization", shared.session_id());
            let result = attached.materialize(CopyReason::VecMaterialization, 4, || vec![7u8; 4]);
            shared.close().await.unwrap();
            let shared = Arc::try_unwrap(shared).ok().unwrap();
            let handle = Arc::try_unwrap(shared.session_handle)
                .ok()
                .unwrap()
                .into_inner();
            tokio::time::timeout(std::time::Duration::from_secs(5), handle.wait())
                .await
                .unwrap()
                .unwrap();
            assert!(identity && node_is_separate);
            assert_eq!(attached.policy(), policy);
            assert_eq!(attached.boundary(), expected_boundary);
            assert_eq!(result.is_ok(), policy == CopyPolicy::AllowCopies);
            let report = original.report();
            let counts = report
                .entries
                .iter()
                .find(|(reason, _)| *reason == CopyReason::VecMaterialization)
                .unwrap()
                .1;
            assert_eq!(
                (counts.copied_bytes, counts.rejected_bytes),
                if policy == CopyPolicy::AllowCopies {
                    (4, 0)
                } else {
                    (0, 4)
                }
            );
            contexts.push(original);
        }
        assert!(!contexts[0].same_authority(&contexts[1]));
        assert_ne!(contexts[0].boundary(), contexts[1].boundary());
    }
}

#[tokio::test]
async fn output_drainer_promotes_once_for_every_subscriber() {
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    let (broadcast, _) = tokio::sync::broadcast::channel(4);
    let mut first = broadcast.subscribe();
    let mut second = broadcast.subscribe();
    SharedPipelineSession::spawn_output_drainer(rx, broadcast);
    let samples = vec![0.5_f32; 32];
    let original = samples.as_ptr();
    tx.send(crate::data::RuntimeData::Audio {
        samples: crate::data::AudioSamples::Vec(samples),
        sample_rate: 16_000,
        channels: 1,
        stream_id: None,
        timestamp_us: None,
        arrival_ts_us: None,
        metadata: None,
    })
    .await
    .unwrap();
    for receiver in [&mut first, &mut second] {
        let received = tokio::time::timeout(std::time::Duration::from_secs(2), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        let crate::data::RuntimeData::Audio { samples, .. } = &received.data else {
            panic!("expected audio");
        };
        assert!(!matches!(samples, crate::data::AudioSamples::Vec(_)));
        assert_eq!(
            samples.as_ptr(),
            original,
            "subscriber receive copied the samples"
        );
    }
    // Closing the router side ends the drainer without stranding subscribers.
    drop(tx);
    assert!(matches!(
        tokio::time::timeout(std::time::Duration::from_secs(2), first.recv())
            .await
            .unwrap(),
        Err(tokio::sync::broadcast::error::RecvError::Closed)
    ));
}
