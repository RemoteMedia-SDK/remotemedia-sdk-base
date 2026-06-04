//! WebRTC server binary entry point
//!
//! Starts the RemoteMedia WebRTC transport server for real-time media streaming.
//!
//! # Usage
//!
//! ```bash
//! # Start gRPC signaling server (default: 0.0.0.0:50051)
//! cargo run -p remotemedia-webrtc-server -- \
//!   --mode grpc \
//!   --grpc-address 0.0.0.0:50051 \
//!   --manifest ./examples/loopback.yaml
//!
//! # Start WebSocket client mode (connects to signaling server)
//! cargo run -p remotemedia-webrtc-server -- \
//!   --mode websocket \
//!   --signaling-url ws://localhost:8080
//!
//! # Configure STUN/TURN servers
//! cargo run -p remotemedia-webrtc-server -- \
//!   --stun-servers stun:stun.l.google.com:19302 \
//!   --max-peers 20
//! ```

use clap::Parser;
use remotemedia_webrtc::{WebRtcServerBuilder, WebRtcSignalingServerBuilder};
use std::path::PathBuf;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// RemoteMedia WebRTC Server
///
/// Real-time media streaming server with WebRTC transport.
/// Supports both gRPC signaling (server mode) and WebSocket signaling (client mode).
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Server mode: 'grpc' for signaling server, 'websocket' for signaling client
    #[arg(short, long, default_value = "websocket", env = "WEBRTC_MODE")]
    mode: ServerMode,

    /// gRPC signaling server address (gRPC mode only)
    #[arg(long, default_value = "0.0.0.0:50051", env = "GRPC_SIGNALING_ADDRESS")]
    grpc_address: String,

    /// Pipeline manifest path (gRPC mode only)
    #[arg(
        long,
        default_value = "./examples/loopback.yaml",
        env = "WEBRTC_PIPELINE_MANIFEST"
    )]
    manifest: PathBuf,

    /// WebSocket signaling URL (WebSocket mode only)
    #[arg(
        long,
        default_value = "ws://localhost:8080",
        env = "WEBRTC_SIGNALING_URL"
    )]
    signaling_url: String,

    /// STUN servers (comma-separated)
    #[arg(
        long,
        value_delimiter = ',',
        default_value = "stun:stun.l.google.com:19302"
    )]
    stun_servers: Vec<String>,

    /// Maximum concurrent peer connections
    #[arg(long, default_value_t = 10, env = "WEBRTC_MAX_PEERS")]
    max_peers: u32,

    /// Enable data channel support
    #[arg(long, default_value_t = true, env = "WEBRTC_ENABLE_DATA_CHANNEL")]
    enable_data_channel: bool,

    /// Jitter buffer size in milliseconds
    #[arg(long, default_value_t = 100, env = "WEBRTC_JITTER_BUFFER_MS")]
    jitter_buffer_ms: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum ServerMode {
    /// gRPC signaling server mode
    Grpc,
    /// WebSocket signaling client mode
    Websocket,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Initialize tracing
    let env_filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("info"))
        .unwrap();
    tracing_subscriber::registry()
        .with(env_filter)
        .with(tracing_subscriber::fmt::layer())
        .init();

    info!(
        version = env!("CARGO_PKG_VERSION"),
        mode = ?args.mode,
        "RemoteMedia WebRTC Server starting"
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(num_cpus::get())
        .thread_name("webrtc-worker")
        .enable_all()
        .build()?;

    runtime.block_on(async move {
        match args.mode {
            ServerMode::Grpc => {
                WebRtcSignalingServerBuilder::new()
                    .bind(&args.grpc_address)
                    .manifest_from_file(&args.manifest)?
                    .stun_servers(args.stun_servers)
                    .max_peers(args.max_peers)
                    .build()?
                    .run()
                    .await
            }
            ServerMode::Websocket => {
                WebRtcServerBuilder::new()
                    .signaling_url(&args.signaling_url)
                    .stun_servers(args.stun_servers)
                    .max_peers(args.max_peers)
                    .enable_data_channel(args.enable_data_channel)
                    .jitter_buffer_ms(args.jitter_buffer_ms)
                    .build()?
                    .run()
                    .await
            }
        }
    })
}
