//! Data Channel Control Example
//!
//! This example demonstrates using WebRTC data channels for pipeline control.
//! It shows how to:
//!
//! - Create reliable and unreliable data channels
//! - Send JSON control messages for pipeline reconfiguration
//! - Send binary data for efficient transfer
//! - Handle incoming control messages
//!
//! # Use Cases
//!
//! - Real-time pipeline parameter adjustment (e.g., filter intensity)
//! - Switch between different processing modes
//! - Send/receive metadata alongside media streams
//! - Coordinate multi-participant sessions
//!
//! # Running
//!
//! ```bash
//! cargo run --example data_channel_control
//! ```

use remotemedia_webrtc::{
    channels::{ControlMessage, DataChannelMessage},
    config::{DataChannelMode, WebRtcTransportConfig},
    Result,
};
use serde_json::json;
use std::collections::BTreeMap;
use std::env;
use std::time::{SystemTime, UNIX_EPOCH};

/// Example: Pipeline control message sent via data channel
fn create_pipeline_control_messages() -> Vec<DataChannelMessage> {
    vec![
        // Start a pipeline
        DataChannelMessage::Control {
            action: "pipeline_start".to_string(),
            headers: BTreeMap::new(),
            json: json!({
                "session_id": "session-123",
                "manifest": "audio_processing"
            }),
        },
        // Adjust parameters
        DataChannelMessage::Control {
            action: "update_params".to_string(),
            headers: BTreeMap::new(),
            json: json!({
                "node_id": "vad",
                "params": {
                    "threshold": 0.7,
                    "min_speech_duration_ms": 200
                }
            }),
        },
        // Send binary data (e.g., model weights)
        DataChannelMessage::Binary {
            action: "binary".to_string(),
            headers: BTreeMap::new(),
            data: vec![0x01, 0x02, 0x03, 0x04],
        },
        // Simple text message
        DataChannelMessage::Text {
            action: "text".to_string(),
            headers: BTreeMap::new(),
            text: "ping".to_string(),
        },
        // Pause pipeline
        DataChannelMessage::Control {
            action: "pipeline_pause".to_string(),
            headers: BTreeMap::new(),
            json: json!({
                "session_id": "session-123"
            }),
        },
    ]
}

/// Example: Using the ControlMessage enum for type-safe control
fn demonstrate_control_messages() {
    println!("\n--- Control Message Types ---\n");

    // Pipeline reconfiguration
    let reconfigure = ControlMessage::Reconfigure {
        manifest: json!({
            "nodes": [
                { "id": "input", "node_type": "Input" },
                { "id": "vad", "node_type": "VoiceActivityDetection" },
                { "id": "output", "node_type": "Output" }
            ],
            "connections": [
                { "from": "input", "to": "vad" },
                { "from": "vad", "to": "output" }
            ]
        }),
    };
    println!("Reconfigure: {:?}", reconfigure);

    // Pause streaming
    let pause = ControlMessage::Pause;
    println!("Pause: {:?}", pause);

    // Resume streaming
    let resume = ControlMessage::Resume;
    println!("Resume: {:?}", resume);

    // Get status
    let get_status = ControlMessage::GetStatus;
    println!("GetStatus: {:?}", get_status);

    // Status response
    let status = ControlMessage::Status {
        state: "running".to_string(),
        active_nodes: 3,
        timestamp_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64,
    };
    println!("Status: {:?}", status);

    // Ping for latency measurement
    let ping = ControlMessage::Ping {
        timestamp_ms: 1234567890,
    };
    println!("Ping: {:?}", ping);

    // Pong response
    let pong = ControlMessage::Pong {
        ping_timestamp_ms: 1234567890,
        pong_timestamp_ms: 1234567900,
    };
    println!("Pong: {:?}", pong);

    // Custom message
    let custom = ControlMessage::Custom {
        message_type: "node_params".to_string(),
        data: json!({
            "node_id": "noise_gate",
            "params": {
                "threshold_db": -40.0,
                "attack_ms": 5.0,
                "release_ms": 50.0
            }
        }),
    };
    println!("Custom: {:?}", custom);
}

/// Example: Handling incoming control messages
fn handle_incoming_message(msg: &DataChannelMessage) {
    match msg {
        DataChannelMessage::Control {
            action,
            json: value,
            ..
        } => {
            println!("Received control message:");
            println!("  Action: {}", action);
            println!("  Payload: {}", value);

            match action.as_str() {
                "pipeline_start" => println!("  -> Would start pipeline"),
                "pipeline_stop" => println!("  -> Would stop pipeline"),
                "pipeline_pause" => println!("  -> Would pause pipeline"),
                "pipeline_resume" => println!("  -> Would resume pipeline"),
                "update_params" => println!("  -> Would update node parameters"),
                _ => println!("  -> Unknown action"),
            }
        }
        DataChannelMessage::Binary { data, .. } => {
            println!("Received binary message: {} bytes", data.len());
            println!("  First bytes: {:02x?}", &data[..data.len().min(8)]);
        }
        DataChannelMessage::Text { text, .. } => {
            println!("Received text message: {}", text);
            if text == "ping" {
                println!("  -> Would respond with 'pong'");
            }
        }
        DataChannelMessage::RuntimeData { .. } => {
            println!("Received runtime data message");
        }
        DataChannelMessage::Chunk { .. } => {
            println!("Received chunked message");
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    println!("Data Channel Control Example");
    println!("============================\n");

    // Configure transport with data channel support
    let signaling_url =
        env::var("SIGNALING_URL").unwrap_or_else(|_| "ws://localhost:8080".to_string());

    // Use reliable mode for control messages (guaranteed delivery)
    let config = WebRtcTransportConfig {
        signaling_url,
        enable_data_channel: true,
        data_channel_mode: DataChannelMode::Reliable, // Guaranteed delivery
        ..Default::default()
    };

    config.validate()?;

    println!("--- Configuration ---");
    println!("Data channel enabled: {}", config.enable_data_channel);
    println!("Data channel mode: {:?}", config.data_channel_mode);
    println!("  - ordered: {}", config.data_channel_mode.ordered());
    println!(
        "  - max_retransmits: {:?}",
        config.data_channel_mode.max_retransmits()
    );

    // Demonstrate control message types
    demonstrate_control_messages();

    // Create and display example messages
    println!("\n--- Example Control Messages ---\n");
    let messages = create_pipeline_control_messages();

    for (i, msg) in messages.iter().enumerate() {
        println!("Message {}:", i + 1);
        handle_incoming_message(msg);
        println!();
    }

    // Show message size calculation
    println!("--- Message Sizes ---");
    for (i, msg) in messages.iter().enumerate() {
        let size = match msg {
            DataChannelMessage::Control { json: v, .. } => {
                serde_json::to_string(v).unwrap_or_default().len()
            }
            DataChannelMessage::Binary { data: d, .. } => d.len(),
            DataChannelMessage::Text { text: t, .. } => t.len(),
            DataChannelMessage::RuntimeData { data_buffer, .. } => data_buffer.len(),
            DataChannelMessage::Chunk { .. } => 0,
        };
        println!("Message {}: {} bytes", i + 1, size);
    }

    // Show max message size
    println!("\nMax message size: {} MB", 16);
    println!("(from channels::MAX_MESSAGE_SIZE)");

    println!("\n--- Unreliable Mode Example ---");
    let unreliable_config = WebRtcTransportConfig {
        data_channel_mode: DataChannelMode::Unreliable, // Low latency, may lose
        ..config.clone()
    };
    println!("Unreliable mode settings:");
    println!(
        "  - ordered: {}",
        unreliable_config.data_channel_mode.ordered()
    );
    println!(
        "  - max_retransmits: {:?} (no retransmits)",
        unreliable_config.data_channel_mode.max_retransmits()
    );
    println!("\nUse unreliable mode for:");
    println!("  - Real-time position updates");
    println!("  - Cursor positions in collaborative editing");
    println!("  - Game state that changes rapidly");

    println!("\nNote: This is a demonstration example.");
    println!("Full implementation requires a running signaling server.");

    Ok(())
}
