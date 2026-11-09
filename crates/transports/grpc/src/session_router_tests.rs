//! gRPC router ownership: fan-out shares one sample allocation, teardown
//! releases owners parked on a full loopback, and a full loopback is reported
//! to streaming callbacks instead of being acknowledged as delivered.
use super::*;
use remotemedia_core::data::AudioSamples;
use remotemedia_core::manifest::{Connection, Manifest, ManifestMetadata, NodeManifest};
use remotemedia_core::nodes::{AsyncNodeWrapper, AsyncStreamingNode};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

const SAMPLES: usize = 48;

fn legacy_audio(samples: Vec<f32>) -> RuntimeData {
    RuntimeData::Audio {
        samples: AudioSamples::Vec(samples),
        sample_rate: 16_000,
        channels: 1,
        stream_id: None,
        timestamp_us: None,
        arrival_ts_us: None,
        metadata: None,
    }
}

fn manifest(nodes: &[(&str, bool)], edges: &[(&str, &str)]) -> Manifest {
    Manifest {
        version: "v1".into(),
        metadata: ManifestMetadata {
            name: "grpc-routing".into(),
            ..Default::default()
        },
        nodes: nodes
            .iter()
            .map(|(id, streaming)| NodeManifest {
                id: id.to_string(),
                node_type: "TestNode".into(),
                params: serde_json::json!({}),
                is_streaming: *streaming,
                ..Default::default()
            })
            .collect(),
        connections: edges
            .iter()
            .map(|(from, to)| Connection {
                from: from.to_string(),
                to: to.to_string(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

struct Passthrough;

#[async_trait::async_trait]
impl AsyncStreamingNode for Passthrough {
    fn node_type(&self) -> &str {
        "TestNode"
    }
    async fn process(&self, data: RuntimeData) -> remotemedia_core::Result<RuntimeData> {
        Ok(data)
    }
}

/// Emits `count` outputs per input and records each callback result.
struct Burst {
    count: usize,
    results: std::sync::Mutex<Vec<Result<(), String>>>,
}

#[async_trait::async_trait]
impl AsyncStreamingNode for Burst {
    fn node_type(&self) -> &str {
        "TestNode"
    }
    async fn process(&self, data: RuntimeData) -> remotemedia_core::Result<RuntimeData> {
        Ok(data)
    }
    async fn process_streaming<F>(
        &self,
        data: RuntimeData,
        _session: Option<String>,
        mut callback: F,
    ) -> remotemedia_core::Result<usize>
    where
        F: FnMut(RuntimeData) -> remotemedia_core::Result<()> + Send,
    {
        for _ in 0..self.count {
            let result = callback(data.clone()).map_err(|error| error.to_string());
            self.results.lock().unwrap().push(result);
        }
        Ok(self.count)
    }
}

/// Byte owner whose release is observable.
struct Tracked {
    bytes: Vec<u8>,
    released: Arc<AtomicUsize>,
}

impl AsRef<[u8]> for Tracked {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Drop for Tracked {
    fn drop(&mut self) {
        self.released.fetch_add(1, Ordering::SeqCst);
    }
}

async fn router(
    manifest: Manifest,
    nodes: Vec<(&str, Box<dyn StreamingNode>)>,
    loopback_capacity: Option<usize>,
) -> SessionRouter {
    let session = StreamSession::new("grpc-routing".into(), manifest, 0, None, None);
    let session = Arc::new(Mutex::new(session));
    {
        let mut guard = session.lock().await;
        for (id, node) in nodes {
            guard.node_cache.insert(id.into(), Arc::new(node));
        }
    }
    let (client_tx, _client_rx) = mpsc::channel(8);
    let (mut router, _shutdown) = SessionRouter::new(
        "grpc-routing".into(),
        Arc::new(StreamingNodeRegistry::new()),
        session,
        client_tx,
    )
    .await
    .unwrap();
    if let Some(capacity) = loopback_capacity {
        let (tx, rx) = mpsc::channel(capacity);
        router.loopback_tx = tx;
        router.loopback_rx = rx;
    }
    router
}

fn packet(data: RuntimeData, from: &str, to: Option<&str>, sequence: u64) -> DataPacket {
    DataPacket {
        data,
        from_node: from.into(),
        to_node: to.map(str::to_owned),
        session_id: "grpc-routing".into(),
        sequence,
        sub_sequence: 0,
    }
}

async fn next_loopback(router: &mut SessionRouter) -> DataPacket {
    tokio::time::timeout(Duration::from_secs(2), router.loopback_rx.recv())
        .await
        .expect("node output reached loopback")
        .expect("loopback open")
}

fn assert_shared_at(data: &RuntimeData, expected: *const f32) {
    let RuntimeData::Audio { samples, .. } = data else {
        panic!("expected audio");
    };
    assert!(
        !matches!(samples, AudioSamples::Vec(_)),
        "legacy Vec reached a node"
    );
    assert_eq!(samples.as_ptr(), expected, "routing copied the samples");
    assert_eq!(samples.len(), SAMPLES);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fan_out_promotes_legacy_audio_without_control_or_copies() {
    let mut router = router(
        manifest(
            &[("emit", false), ("a", false), ("b", false)],
            &[("emit", "a"), ("emit", "b")],
        ),
        vec![
            ("a", Box::new(AsyncNodeWrapper(Arc::new(Passthrough)))),
            ("b", Box::new(AsyncNodeWrapper(Arc::new(Passthrough)))),
        ],
        None,
    )
    .await;
    assert!(router.control.is_none());
    let samples = vec![0.5_f32; SAMPLES];
    let fanned = samples.as_ptr();
    router
        .route_packet(packet(legacy_audio(samples), "emit", None, 1))
        .await
        .unwrap();
    let first = next_loopback(&mut router).await;
    let second = next_loopback(&mut router).await;
    let mut from: Vec<_> = [&first, &second]
        .iter()
        .map(|p| p.from_node.clone())
        .collect();
    from.sort();
    assert_eq!(from, ["a", "b"]);
    assert_shared_at(&first.data, fanned);
    assert_shared_at(&second.data, fanned);

    // Client ingress addressed to one node is promoted too.
    let samples = vec![0.25_f32; SAMPLES];
    let original = samples.as_ptr();
    router
        .route_packet(packet(legacy_audio(samples), "client", Some("a"), 2))
        .await
        .unwrap();
    assert_shared_at(&next_loopback(&mut router).await.data, original);
    router.shutdown_nodes().await;
    // Fanned-out owners survive node teardown at their original address.
    assert_shared_at(&first.data, fanned);
    assert_shared_at(&second.data, fanned);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_releases_owners_parked_on_a_full_loopback() {
    let mut router = router(
        manifest(&[("a", false)], &[]),
        vec![("a", Box::new(AsyncNodeWrapper(Arc::new(Passthrough))))],
        Some(1),
    )
    .await;
    let released = Arc::new(AtomicUsize::new(0));
    for sequence in 0..3 {
        let owner = Tracked {
            bytes: vec![sequence as u8; 16],
            released: released.clone(),
        };
        let data = RuntimeData::Binary(remotemedia_core::data::SharedBytes::from_owner(owner));
        router
            .route_packet(packet(data, "client", Some("a"), sequence))
            .await
            .unwrap();
    }
    // One output fills the loopback; the node task then parks on the next send.
    tokio::time::timeout(Duration::from_secs(2), async {
        while router.loopback_rx.len() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(2), router.shutdown_nodes())
        .await
        .expect("shutdown must not wait behind an undrained loopback");
    assert!(router.node_tasks.is_empty());
    assert_eq!(
        released.load(Ordering::SeqCst),
        3,
        "every queued owner released"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_loopback_is_reported_to_streaming_callbacks() {
    let burst = Arc::new(Burst {
        count: 3,
        results: std::sync::Mutex::new(Vec::new()),
    });
    let mut router = router(
        manifest(&[("burst", true)], &[]),
        vec![("burst", Box::new(AsyncNodeWrapper(burst.clone())))],
        Some(1),
    )
    .await;
    router
        .route_packet(packet(
            RuntimeData::Text("x".into()),
            "client",
            Some("burst"),
            1,
        ))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while burst.results.lock().unwrap().len() < 3 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let results = burst.results.lock().unwrap().clone();
    assert!(results[0].is_ok(), "{results:?}");
    for result in &results[1..] {
        let error = result
            .as_ref()
            .expect_err("a dropped output must not be acknowledged");
        assert!(error.contains("loopback full"), "{error}");
    }
    assert_eq!(
        router.loopback_rx.len(),
        1,
        "only the accepted output is queued"
    );
    router.shutdown_nodes().await;
}
