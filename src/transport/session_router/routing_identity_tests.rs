//! Routing ownership: legacy `AudioSamples::Vec` payloads must be promoted to
//! shared storage before any router clone, so ingress fan-out, node-output
//! fan-out, bypass, taps and client egress all reference one sample allocation.
use super::{tests::create_test_manifest, *};
use crate::data::AudioSamples;
use crate::nodes::{AsyncNodeWrapper, AsyncStreamingNode};
use crate::transport::session_control::{ControlAddress, NodeState, SessionControl};
use std::time::Duration;

const SAMPLES: usize = 64;

fn legacy_audio(samples: Vec<f32>) -> RuntimeData {
    RuntimeData::Audio {
        samples: AudioSamples::Vec(samples),
        sample_rate: 16_000,
        channels: 1,
        stream_id: None,
        timestamp_us: Some(7),
        arrival_ts_us: None,
        metadata: None,
    }
}

/// Shared (not Vec) samples at the expected address with intact contents.
fn shared_samples_at(data: &RuntimeData, expected: *const f32, fill: f32) {
    let RuntimeData::Audio { samples, .. } = data else {
        panic!("expected audio, got {}", data.data_type());
    };
    assert!(
        !matches!(samples, AudioSamples::Vec(_)),
        "routed audio must be shared, not a legacy Vec that deep-copies on clone"
    );
    assert_eq!(samples.as_ptr(), expected, "routing copied the samples");
    assert_eq!(samples.len(), SAMPLES);
    assert!(samples.iter().all(|value| *value == fill));
}

struct Passthrough;

#[async_trait::async_trait]
impl AsyncStreamingNode for Passthrough {
    fn node_type(&self) -> &str {
        "Passthrough"
    }
    async fn process(&self, data: RuntimeData) -> Result<RuntimeData> {
        Ok(data)
    }
}

/// Emits a fresh legacy Vec allocation per input and records its address.
struct VecEmitter {
    emitted: std::sync::Mutex<Vec<usize>>,
}

#[async_trait::async_trait]
impl AsyncStreamingNode for VecEmitter {
    fn node_type(&self) -> &str {
        "VecEmitter"
    }
    async fn process(&self, _data: RuntimeData) -> Result<RuntimeData> {
        let samples = vec![0.5_f32; SAMPLES];
        self.emitted.lock().unwrap().push(samples.as_ptr() as usize);
        Ok(legacy_audio(samples))
    }
}

struct Graph {
    emitter: Arc<VecEmitter>,
    control: Option<Arc<SessionControl>>,
    input: crate::transport::SessionPacketSender,
    audio: mpsc::Receiver<crate::transport::TransportData>,
    shutdown: mpsc::Sender<()>,
    running: JoinHandle<Result<()>>,
}

/// Roots `emit` and `pass` both receive client ingress. `emit` fans out to two
/// sinks; `pass` forwards the client owner to `relay`, the third sink.
async fn start(with_control: bool) -> Graph {
    let manifest = create_test_manifest(
        vec![
            ("emit", "VecEmitter"),
            ("pass", "Passthrough"),
            ("sink-a", "Passthrough"),
            ("sink-b", "Passthrough"),
            ("relay", "Passthrough"),
        ],
        vec![("emit", "sink-a"), ("emit", "sink-b"), ("pass", "relay")],
    );
    let (output, receivers) = ClientOutputRouter::new(16);
    let (mut router, shutdown) = SessionRouter::new(
        "routing-identity".into(),
        Arc::new(manifest),
        Arc::new(StreamingNodeRegistry::new()),
        output,
    )
    .unwrap();
    let control = if with_control {
        let control = SessionControl::new(router.session_id.clone());
        router.attach_control(control.clone()).await;
        Some(control)
    } else {
        None
    };
    let emitter = Arc::new(VecEmitter {
        emitted: std::sync::Mutex::new(Vec::new()),
    });
    router
        .cached_nodes
        .insert("emit".into(), Box::new(AsyncNodeWrapper(emitter.clone())));
    for id in ["pass", "sink-a", "sink-b", "relay"] {
        router
            .cached_nodes
            .insert(id.into(), Box::new(AsyncNodeWrapper(Arc::new(Passthrough))));
    }
    Graph {
        emitter,
        control,
        input: router.get_input_sender(),
        audio: receivers.audio_rx,
        shutdown,
        running: tokio::spawn(router.run_after_init()),
    }
}

impl Graph {
    /// Send one legacy Vec client packet to every root; return its address.
    async fn send_client_vec(&self) -> usize {
        let samples = vec![0.25_f32; SAMPLES];
        let address = samples.as_ptr() as usize;
        self.input
            .send(DataPacket {
                data: legacy_audio(samples),
                from_node: "client".into(),
                to_node: None,
                session_id: "routing-identity".into(),
                sequence: 1,
                sub_sequence: 0,
                metadata: HashMap::new(),
            })
            .await
            .unwrap();
        address
    }

    async fn receive(&mut self, count: usize) -> Vec<RuntimeData> {
        let mut out = Vec::with_capacity(count);
        for _ in 0..count {
            out.push(
                tokio::time::timeout(Duration::from_secs(2), self.audio.recv())
                    .await
                    .expect("routed output")
                    .expect("audio lane open")
                    .data,
            );
        }
        out
    }

    async fn close(self) {
        let _ = self.shutdown.send(()).await;
        tokio::time::timeout(Duration::from_secs(2), self.running)
            .await
            .expect("router joined")
            .unwrap()
            .unwrap();
    }
}

/// Split outputs into node-emitted (0.5) and client-forwarded (0.25) owners.
fn split(outputs: &[RuntimeData]) -> (Vec<&RuntimeData>, Vec<&RuntimeData>) {
    outputs.iter().partition(|data| match data {
        RuntimeData::Audio { samples, .. } => samples.first() == Some(&0.5),
        _ => false,
    })
}

async fn assert_routes_share_owners(with_control: bool) {
    let mut graph = start(with_control).await;
    let client = graph.send_client_vec().await as *const f32;
    let outputs = graph.receive(3).await;
    let node_vec = graph.emitter.emitted.lock().unwrap()[0] as *const f32;
    let check = |outputs: &[RuntimeData]| {
        let (emitted, forwarded) = split(outputs);
        assert_eq!((emitted.len(), forwarded.len()), (2, 1));
        for data in emitted {
            shared_samples_at(data, node_vec, 0.5);
        }
        shared_samples_at(forwarded[0], client, 0.25);
    };
    check(&outputs);
    graph.close().await;
    // Received owners stay valid and unmoved after the session joined.
    check(&outputs);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn legacy_vec_audio_shares_one_allocation_without_control() {
    assert_routes_share_owners(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn legacy_vec_audio_shares_one_allocation_through_control_fanout() {
    assert_routes_share_owners(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn taps_and_bypass_reference_the_routed_allocation() {
    let mut graph = start(true).await;
    let control = graph.control.clone().unwrap();
    control.set_node_state("relay", NodeState::Bypass);
    let mut emit_tap = control
        .subscribe(&ControlAddress::node_out("emit"))
        .unwrap();
    let mut pass_tap = control
        .subscribe(&ControlAddress::node_out("pass"))
        .unwrap();
    let client = graph.send_client_vec().await;
    let outputs = graph.receive(3).await;
    let node_vec = graph.emitter.emitted.lock().unwrap()[0];
    let (emitted, forwarded) = split(&outputs);
    for data in emitted {
        shared_samples_at(data, node_vec as *const f32, 0.5);
    }
    // `relay` is bypassed: the client owner still reaches egress unchanged.
    shared_samples_at(forwarded[0], client as *const f32, 0.25);
    let tapped = tokio::time::timeout(Duration::from_secs(2), emit_tap.recv())
        .await
        .unwrap()
        .unwrap();
    shared_samples_at(&tapped, node_vec as *const f32, 0.5);
    let tapped = tokio::time::timeout(Duration::from_secs(2), pass_tap.recv())
        .await
        .unwrap()
        .unwrap();
    shared_samples_at(&tapped, client as *const f32, 0.25);
    graph.close().await;
    // Tap receivers keep their owners after the router joined.
    shared_samples_at(&tapped, client as *const f32, 0.25);
}

#[tokio::test]
async fn client_output_router_promotes_direct_sends() {
    let (router, mut receivers) = ClientOutputRouter::new(2);
    let samples = vec![1.0_f32; SAMPLES];
    let address = samples.as_ptr();
    router.try_send(legacy_audio(samples)).unwrap();
    let samples = vec![1.0_f32; SAMPLES];
    let second = samples.as_ptr();
    router.send(legacy_audio(samples)).await.unwrap();
    shared_samples_at(&receivers.audio_rx.recv().await.unwrap().data, address, 1.0);
    shared_samples_at(&receivers.audio_rx.recv().await.unwrap().data, second, 1.0);
}
