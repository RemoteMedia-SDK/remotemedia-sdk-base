//! [`PeerClient`] against a real signalling server and a model-free pipeline:
//! the plan, the answered sections, a data round trip, decoded audio and
//! video frames of the planned size, and no video for a member that declines.

use std::sync::Arc;
use std::time::{Duration, Instant};

use remotemedia_core::manifest::Manifest;
use remotemedia_core::transport::{ExecutorConfig, PipelineExecutor};
use remotemedia_types::{CopyPolicy, RuntimeData};
use serde_json::json;

use crate::config::WebRtcTransportConfig;
use crate::peer_client::{
    AnsweredSection, PeerClient, PeerClientOptions, PeerEvent, ReceiveVideo, AUDIO_SAMPLE_RATE,
};
use crate::signaling::websocket::{WebSocketServerHandle, WebSocketSignalingServer};

fn manifest() -> Arc<Manifest> {
    Arc::new(
        serde_json::from_value(json!({
            "version": "v1",
            "metadata": { "name": "peer-client-test" },
            "nodes": [
                { "id": "text_in", "node_type": "MediaKindFilterNode",
                  "params": { "allow": ["text", "json"] } },
                { "id": "tts", "node_type": "TestToneTtsNode",
                  "params": { "sample_rate": 48000, "ms_per_char": 50, "frequency": 440.0 } },
                { "id": "echo", "node_type": "PassThrough" },
                { "id": "avatar", "node_type": "TestPatternVideoNode",
                  "params": { "video_stream_id": "avatar", "width": 320, "height": 240,
                              "framerate": 15 } }
            ],
            "connections": [
                { "from": "text_in", "to": "tts" },
                { "from": "text_in", "to": "echo" }
            ]
        }))
        .expect("manifest"),
    )
}

async fn serve() -> (WebSocketServerHandle, String) {
    let executor = Arc::new(
        PipelineExecutor::with_config(ExecutorConfig {
            transport_materialization_policy: CopyPolicy::AllowCopies,
            ..Default::default()
        })
        .unwrap(),
    );
    let config = Arc::new(WebRtcTransportConfig {
        stun_servers: Vec::new(),
        turn_servers: Vec::new(),
        ..Default::default()
    });
    let handle = WebSocketSignalingServer::new(0, config, executor, manifest())
        .start()
        .await
        .expect("server starts");
    let url = format!("ws://127.0.0.1:{}/ws", handle.local_addr().port());
    (handle, url)
}

/// Events until `done` says so, or panic after `within`.
async fn until(
    client: &mut PeerClient,
    within: Duration,
    what: &str,
    mut done: impl FnMut(&PeerEvent) -> bool,
) {
    let deadline = Instant::now() + within;
    loop {
        let left = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_else(|| panic!("timed out waiting for {what}"));
        let event = tokio::time::timeout(left, client.next_event())
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for {what}"))
            .unwrap_or_else(|| panic!("client closed waiting for {what}"));
        if done(&event) {
            return;
        }
    }
}

/// Dominant frequency by counting rising zero crossings.
fn frequency(samples: &[f32], sample_rate: u32) -> f32 {
    let crossings = samples
        .windows(2)
        .filter(|pair| pair[0] <= 0.0 && pair[1] > 0.0)
        .count();
    crossings as f32 * sample_rate as f32 / samples.len() as f32
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_member_gets_the_plan_data_audio_and_video() {
    let (server, url) = serve().await;
    let mut client = PeerClient::connect(&url, PeerClientOptions::default())
        .await
        .expect("joins");

    assert_eq!(
        client.media_plan(),
        Some(&json!({
            "v": 1,
            "audio": [{ "stream": "default" }],
            "video": [{ "stream": "avatar", "width": 320, "height": 240, "framerate": 15 }]
        }))
    );
    let answered = client.answered_media().to_vec();
    assert!(
        answered.iter().any(|s| s.kind == "audio" && s.stream == "default"),
        "{answered:?}"
    );
    let video: Vec<&AnsweredSection> = answered.iter().filter(|s| s.kind == "video").collect();
    assert_eq!(video.len(), 1, "{answered:?}");
    assert_eq!(video[0].stream, "avatar");

    until(&mut client, Duration::from_secs(20), "data channel", |e| {
        matches!(e, PeerEvent::DataChannelOpen)
    })
    .await;

    // Text in: the echo comes back on the data channel, the tone on the
    // audio track. 11 characters at 50 ms each is 550 ms of 440 Hz.
    client
        .send_data(&RuntimeData::Text("hello world".into()))
        .await
        .unwrap();
    let mut echoed = false;
    let mut tone: Vec<f32> = Vec::new();
    until(&mut client, Duration::from_secs(20), "echo and tone", |event| {
        match event {
            PeerEvent::Data {
                data: RuntimeData::Text(text),
                ..
            } if text == "hello world" => echoed = true,
            PeerEvent::Audio { stream, samples, .. } => {
                assert_eq!(stream.as_deref(), Some("default"));
                tone.extend(samples.iter().filter(|s| s.abs() > 0.01));
            }
            _ => {}
        }
        echoed && tone.len() >= AUDIO_SAMPLE_RATE as usize * 4 / 10
    })
    .await;
    let hz = frequency(&tone, AUDIO_SAMPLE_RATE);
    assert!((400.0..480.0).contains(&hz), "tone is {hz} Hz");

    // Microphone audio in: the test pattern answers with video frames.
    let mic: Vec<f32> = (0..AUDIO_SAMPLE_RATE * 2)
        .map(|i| (i as f32 * 2.0 * std::f32::consts::PI * 300.0 / AUDIO_SAMPLE_RATE as f32).sin() * 0.3)
        .collect();
    let (mut frames, mut size) = (0, None);
    client.send_audio(&mic, true).await.unwrap();
    until(&mut client, Duration::from_secs(20), "video frames", |event| {
        if let PeerEvent::VideoFrame {
            stream, dimensions, ..
        } = event
        {
            assert_eq!(stream.as_deref(), Some("avatar"));
            frames += 1;
            if dimensions.is_some() {
                size = *dimensions;
            }
        }
        frames >= 5 && size.is_some()
    })
    .await;
    assert_eq!(size, Some((320, 240)));

    client.close().await.unwrap();
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_member_that_declines_video_gets_none() {
    let (server, url) = serve().await;
    let mut client = PeerClient::connect(
        &url,
        PeerClientOptions {
            receive_video: ReceiveVideo::None,
            ..Default::default()
        },
    )
    .await
    .expect("joins");
    assert!(
        client.answered_media().iter().all(|s| s.kind != "video"),
        "{:?}",
        client.answered_media()
    );
    until(&mut client, Duration::from_secs(20), "data channel", |e| {
        matches!(e, PeerEvent::DataChannelOpen)
    })
    .await;
    let mic = vec![0.2f32; AUDIO_SAMPLE_RATE as usize];
    client.send_audio(&mic, true).await.unwrap();
    client
        .send_data(&RuntimeData::Text("ping".into()))
        .await
        .unwrap();
    // The echo proves the pipeline ran; nothing before it may be video.
    until(&mut client, Duration::from_secs(20), "echo", |event| {
        assert!(
            !matches!(event, PeerEvent::VideoFrame { .. })
                && !matches!(event, PeerEvent::Track { kind, .. } if kind == "video"),
            "video for a member that declined it: {event:?}"
        );
        matches!(event, PeerEvent::Data { data: RuntimeData::Text(t), .. } if t == "ping")
    })
    .await;
    client.close().await.unwrap();
    server.shutdown().await;
}
