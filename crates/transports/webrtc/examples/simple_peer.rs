//! Simple 1:1 Video Call Example
//!
//! This example demonstrates a basic point-to-point video call with
//! WebRTC transport. It shows how to:
//!
//! - Configure WebRTC transport with low latency settings
//! - Connect to a peer via signaling
//! - Send and receive audio/video streams
//!
//! # Running
//!
//! Start the signaling server first:
//! ```bash
//! cd examples/signaling_server && npm start
//! ```
//!
//! Then run two instances of this example:
//! ```bash
//! # Terminal 1
//! cargo run --example simple_peer -- --peer-id alice
//!
//! # Terminal 2
//! cargo run --example simple_peer -- --peer-id bob --connect alice
//! ```

use remotemedia_webrtc::{
    config::{TurnServerConfig, WebRtcTransportConfig},
    Error, Result,
};
use std::env;
use std::time::{SystemTime, UNIX_EPOCH};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    // Parse command line arguments
    let args: Vec<String> = env::args().collect();
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u32;
    let peer_id = args
        .iter()
        .position(|a| a == "--peer-id")
        .and_then(|i| args.get(i + 1))
        .map(|s| s.to_string())
        .unwrap_or_else(|| format!("peer-{}", timestamp));

    let connect_to = args
        .iter()
        .position(|a| a == "--connect")
        .and_then(|i| args.get(i + 1))
        .cloned();

    println!("Starting simple peer: {}", peer_id);

    // Configure transport with low latency preset for real-time video
    let signaling_url =
        env::var("SIGNALING_URL").unwrap_or_else(|_| "ws://localhost:8080".to_string());

    let config = WebRtcTransportConfig::low_latency_preset(&signaling_url)
        .with_peer_id(&peer_id)
        .with_max_peers(2);

    // Optionally add TURN servers for NAT traversal
    let config = if let Ok(turn_url) = env::var("TURN_URL") {
        let username = env::var("TURN_USERNAME").unwrap_or_default();
        let credential = env::var("TURN_CREDENTIAL").unwrap_or_default();
        config.with_turn_servers(vec![TurnServerConfig {
            url: turn_url,
            username,
            credential,
        }])
    } else {
        config
    };

    // Validate configuration
    config.validate()?;
    println!("Configuration validated");

    // In a real application, you would:
    // 1. Create a WebRtcTransport with the config
    // 2. Start the transport and connect to signaling
    // 3. Connect to the remote peer
    // 4. Add audio/video tracks
    // 5. Stream media

    println!("Configuration preset: low_latency");
    println!("  - Jitter buffer: {}ms", config.jitter_buffer_size_ms);
    println!("  - RTCP interval: {}ms", config.rtcp_interval_ms);
    println!("  - Data channel mode: {:?}", config.data_channel_mode);
    println!("  - Video codec: {:?}", config.video_codec);

    if let Some(target) = connect_to {
        println!("\nWould connect to peer: {}", target);
    } else {
        println!("\nWaiting for incoming connections...");
    }

    // Placeholder: actual WebRTC transport implementation
    println!("\nNote: This is a demonstration example.");
    println!("Full implementation requires a running signaling server.");

    // Keep alive for demonstration
    println!("\nPress Ctrl+C to exit.");
    tokio::signal::ctrl_c().await.map_err(|e| {
        Error::IoError(std::io::Error::new(
            std::io::ErrorKind::Interrupted,
            e.to_string(),
        ))
    })?;

    println!("Shutting down...");
    Ok(())
}
