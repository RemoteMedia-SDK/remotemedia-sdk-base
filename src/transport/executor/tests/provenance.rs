//! Participant provenance (openspec: add-multi-participant-sessions,
//! `participant-provenance`): every client output carries the identity of the
//! participant whose input produced it, for every payload type.
use super::*;
use crate::transport::data::participant;
use crate::transport::session_control::NodeState;
use std::collections::HashMap;
use std::time::Duration;

fn manifest(nodes: &[(&str, &str)], connections: &[(&str, &str)]) -> Arc<Manifest> {
    Arc::new(Manifest {
        version: "v1".to_string(),
        metadata: ManifestMetadata {
            name: "provenance-test".to_string(),
            ..Default::default()
        },
        nodes: nodes
            .iter()
            .map(|(id, node_type)| NodeManifest {
                id: id.to_string(),
                node_type: node_type.to_string(),
                params: serde_json::json!({}),
                ..Default::default()
            })
            .collect(),
        connections: connections
            .iter()
            .map(|(from, to)| Connection {
                from: from.to_string(),
                to: to.to_string(),
                ..Default::default()
            })
            .collect(),
        python_env: None,
        plugins: Vec::new(),
    })
}

fn user(id: &str) -> Participant {
    Participant::new(id, participant::role::USER).with_track_id(format!("{id}-mic"))
}

async fn next(session: &mut SessionHandle) -> TransportData {
    tokio::time::timeout(Duration::from_secs(5), session.recv_output())
        .await
        .expect("output in time")
        .unwrap()
        .expect("session open")
}

fn speaker(output: &TransportData) -> Option<&str> {
    output.metadata.get(participant::ID).map(String::as_str)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn text_outputs_carry_their_speaker_through_a_two_node_path() {
    let executor = PipelineExecutor::new().unwrap();
    let mut session = executor
        .create_session(manifest(
            &[("first", "PassThrough"), ("second", "PassThrough")],
            &[("first", "second")],
        ))
        .await
        .unwrap();
    let alice = session.join_participant(user("alice")).unwrap();
    let bob = session.join_participant(user("bob")).unwrap();

    for (handle, text) in [(&alice, "from alice"), (&bob, "from bob"), (&alice, "alice again")] {
        handle
            .send(TransportData::new(RuntimeData::Text(text.into())))
            .await
            .unwrap();
    }
    // Text has no payload metadata: attribution can only come from provenance.
    let mut seen = HashMap::new();
    for _ in 0..3 {
        let output = next(&mut session).await;
        let RuntimeData::Text(text) = &output.data else {
            panic!("expected text, got {:?}", output.data)
        };
        seen.insert(text.clone(), speaker(&output).map(str::to_string));
        assert_eq!(
            output.metadata.get(participant::TRACK_ID).cloned(),
            speaker(&output).map(|id| format!("{id}-mic"))
        );
    }
    assert_eq!(seen["from alice"].as_deref(), Some("alice"));
    assert_eq!(seen["from bob"].as_deref(), Some("bob"));
    assert_eq!(seen["alice again"].as_deref(), Some("alice"));

    // Input without a participant stays unattributed.
    session
        .send_input(TransportData::new(RuntimeData::Text("anonymous".into())))
        .await
        .unwrap();
    assert_eq!(speaker(&next(&mut session).await), None);
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_new_payload_inherits_the_input_that_produced_it() {
    let executor = PipelineExecutor::new().unwrap();
    let mut session = executor
        .create_session(manifest(&[("calc", "CalculatorNode")], &[]))
        .await
        .unwrap();
    let carol = session.join_participant(user("carol")).unwrap();
    carol
        .send(TransportData::new(RuntimeData::Json(
            serde_json::json!({"operation": "add", "operands": [2, 3]}),
        )))
        .await
        .unwrap();
    let output = next(&mut session).await;
    let RuntimeData::Json(value) = &output.data else {
        panic!("expected json")
    };
    assert_eq!(value["result"], 5.0);
    // The calculator builds a fresh object; the speaker comes from provenance.
    assert!(value.get(participant::ID).is_none());
    assert_eq!(speaker(&output), Some("carol"));
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bypassed_node_keeps_the_speaker() {
    let executor = PipelineExecutor::new().unwrap();
    let mut session = executor
        .create_session(manifest(&[("calc", "CalculatorNode")], &[]))
        .await
        .unwrap();
    executor
        .control_bus()
        .get(&session.session_id)
        .expect("session on the control bus")
        .set_node_state("calc", NodeState::Bypass);
    let dave = session.join_participant(user("dave")).unwrap();
    dave.send(TransportData::new(RuntimeData::Text("not a calculation".into())))
        .await
        .unwrap();
    let output = next(&mut session).await;
    assert!(matches!(&output.data, RuntimeData::Text(text) if text == "not a calculation"));
    assert_eq!(speaker(&output), Some("dave"));
    session.close().await.unwrap();
}
