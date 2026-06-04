//! Multi-Peer Audio Conference Example
//!
//! This example demonstrates a 5-peer audio conference with WebRTC transport.
//! It shows how to:
//!
//! - Configure WebRTC transport for multi-peer mesh topology
//! - Manage multiple peer connections
//! - Broadcast audio to all peers
//! - Handle peer join/leave events
//!
//! # Architecture
//!
//! ```text
//!     ┌─────┐
//!     │ P1  │────────┬──────────┐
//!     └──┬──┘        │          │
//!        │      ┌────┴───┐  ┌───┴───┐
//!        │      │   P2   │──│  P3   │
//!        │      └────┬───┘  └───┬───┘
//!     ┌──┴──┐        │          │
//!     │ P4  │────────┴──────────┤
//!     └──┬──┘                   │
//!        │               ┌──────┴───┐
//!        └───────────────│    P5    │
//!                        └──────────┘
//! ```
//!
//! # Running
//!
//! Start the signaling server:
//! ```bash
//! cd examples/signaling_server && npm start
//! ```
//!
//! Run multiple instances:
//! ```bash
//! # Terminal 1-5
//! cargo run --example conference -- --room "meeting-123" --name "Alice"
//! cargo run --example conference -- --room "meeting-123" --name "Bob"
//! # etc...
//! ```

use remotemedia_webrtc::{
    config::{AudioCodec, WebRtcTransportConfig},
    Error, Result,
};
use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

/// Tracks state for each participant in the conference
#[derive(Debug, Clone)]
struct Participant {
    peer_id: String,
    display_name: String,
    is_muted: bool,
    audio_level: f32,
}

/// Conference room state
struct ConferenceRoom {
    room_id: String,
    participants: HashMap<String, Participant>,
    max_participants: usize,
}

impl ConferenceRoom {
    fn new(room_id: String, max: usize) -> Self {
        Self {
            room_id,
            participants: HashMap::new(),
            max_participants: max,
        }
    }

    fn add_participant(&mut self, peer_id: String, name: String) -> Result<()> {
        if self.participants.len() >= self.max_participants {
            return Err(Error::SessionError(format!(
                "Room {} is full (max {} participants)",
                self.room_id, self.max_participants
            )));
        }

        self.participants.insert(
            peer_id.clone(),
            Participant {
                peer_id,
                display_name: name,
                is_muted: false,
                audio_level: 0.0,
            },
        );
        Ok(())
    }

    fn remove_participant(&mut self, peer_id: &str) {
        self.participants.remove(peer_id);
    }

    fn list_participants(&self) -> Vec<&Participant> {
        self.participants.values().collect()
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    // Parse command line arguments
    let args: Vec<String> = env::args().collect();
    let room_id = args
        .iter()
        .position(|a| a == "--room")
        .and_then(|i| args.get(i + 1))
        .map(|s| s.to_string())
        .unwrap_or_else(|| "default-room".to_string());

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u32;
    let display_name = args
        .iter()
        .position(|a| a == "--name")
        .and_then(|i| args.get(i + 1))
        .map(|s| s.to_string())
        .unwrap_or_else(|| format!("User-{}", timestamp % 10000));

    println!("Joining conference room: {}", room_id);
    println!("Display name: {}", display_name);

    // Configure transport for multi-peer audio conference
    let signaling_url =
        env::var("SIGNALING_URL").unwrap_or_else(|_| "ws://localhost:8080".to_string());

    // Use default config but optimize for audio
    let config = WebRtcTransportConfig {
        signaling_url,
        max_peers: 5, // 5-peer conference
        audio_codec: AudioCodec::Opus,
        enable_data_channel: true, // For chat/status messages
        jitter_buffer_size_ms: 80, // Slightly larger for conference
        ..Default::default()
    };

    config.validate()?;
    println!("Configuration validated for {} peers", config.max_peers);

    // Create conference room state
    let room = Arc::new(RwLock::new(ConferenceRoom::new(room_id.clone(), 5)));

    // Add ourselves to the room
    {
        let mut room = room.write().await;
        let peer_id = format!("peer-{}", timestamp);
        room.add_participant(peer_id.clone(), display_name.clone())?;
        println!("Joined as peer: {}", peer_id);
    }

    println!("\n--- Conference Settings ---");
    println!("Audio codec: {:?}", config.audio_codec);
    println!("Jitter buffer: {}ms", config.jitter_buffer_size_ms);
    println!("Max peers: {}", config.max_peers);
    println!("Data channel: {}", config.enable_data_channel);

    // In a real application, you would:
    // 1. Create WebRtcTransport with config
    // 2. Connect to signaling and join room
    // 3. Handle peer join/leave events
    // 4. Mix incoming audio from all peers
    // 5. Broadcast local audio to all peers
    // 6. Handle mute/unmute via data channel

    println!("\n--- Participants ---");
    {
        let room = room.read().await;
        for p in room.list_participants() {
            let mute_status = if p.is_muted { " [MUTED]" } else { "" };
            println!(
                "  - {} ({}) - audio level: {:.1}dB{}",
                p.display_name, p.peer_id, p.audio_level, mute_status
            );
        }
    }

    // Simulate peer leaving
    {
        let peer_id = format!("peer-{}", timestamp);
        println!("\n--- Simulating peer operations ---");

        // Add a simulated peer
        let mut room = room.write().await;
        room.add_participant("peer-simulated".to_string(), "Simulated User".to_string())?;
        println!("Simulated peer joined");

        // List updated participants
        println!("Participants after join:");
        for p in room.list_participants() {
            println!("  - {}", p.display_name);
        }

        // Remove the simulated peer
        room.remove_participant("peer-simulated");
        println!("Simulated peer left");

        // Show we're still in the room
        println!("Remaining participants: {}", room.list_participants().len());

        // Verify our peer is still there
        assert!(room.participants.contains_key(&peer_id));
    }

    println!("\nNote: This is a demonstration example.");
    println!("Full implementation requires a running signaling server.");

    // Simulate conference operations
    println!("\nPress Ctrl+C to leave the conference.");
    tokio::signal::ctrl_c().await.map_err(|e| {
        Error::IoError(std::io::Error::new(
            std::io::ErrorKind::Interrupted,
            e.to_string(),
        ))
    })?;

    println!("Leaving conference...");
    Ok(())
}
