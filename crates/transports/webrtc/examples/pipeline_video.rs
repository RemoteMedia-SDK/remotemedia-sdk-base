//! Multi-Output Video Processing Example
//!
//! This example demonstrates video processing with selective routing.
//! It shows how to:
//!
//! - Configure high-quality video presets
//! - Route video to multiple processing pipelines
//! - Handle different output qualities/formats
//! - Monitor connection quality and adapt
//!
//! # Use Cases
//!
//! - Live streaming with multiple quality outputs
//! - Video recording while streaming
//! - Selective participant video routing
//!
//! # Running
//!
//! ```bash
//! cargo run --example pipeline_video
//! ```

use remotemedia_webrtc::{
    config::{VideoCodec, VideoResolution, WebRtcTransportConfig},
    Result,
};
use std::env;

/// Simulated connection quality metrics for demonstration
///
/// In a real application, these would come from `peer.get_metrics()`.
#[derive(Debug, Clone, Default)]
struct ConnectionQualityMetrics {
    pub latency_ms: f32,
    pub packet_loss_rate: f32,
    pub jitter_ms: f32,
    pub quality_score: u32,
}

/// Video quality settings for different outputs
#[derive(Debug, Clone)]
struct VideoQualityProfile {
    name: &'static str,
    resolution: VideoResolution,
    bitrate_kbps: u32,
    framerate: u32,
    codec: VideoCodec,
}

impl VideoQualityProfile {
    fn high() -> Self {
        Self {
            name: "high",
            resolution: VideoResolution::P1080,
            bitrate_kbps: 4000,
            framerate: 30,
            codec: VideoCodec::VP9,
        }
    }

    fn medium() -> Self {
        Self {
            name: "medium",
            resolution: VideoResolution::P720,
            bitrate_kbps: 2000,
            framerate: 30,
            codec: VideoCodec::VP9,
        }
    }

    fn low() -> Self {
        Self {
            name: "low",
            resolution: VideoResolution::P480,
            bitrate_kbps: 800,
            framerate: 24,
            codec: VideoCodec::VP8, // Better hardware support
        }
    }
}

/// Select appropriate quality based on connection metrics
fn select_quality_from_metrics(metrics: &ConnectionQualityMetrics) -> VideoQualityProfile {
    // Use quality score to select appropriate profile
    if metrics.quality_score >= 80 {
        println!("  -> High quality (score: {})", metrics.quality_score);
        VideoQualityProfile::high()
    } else if metrics.quality_score >= 50 {
        println!("  -> Medium quality (score: {})", metrics.quality_score);
        VideoQualityProfile::medium()
    } else {
        println!("  -> Low quality (score: {})", metrics.quality_score);
        VideoQualityProfile::low()
    }
}

/// Demonstrates adaptive bitrate based on network conditions
fn demonstrate_adaptive_quality() {
    println!("\n--- Adaptive Quality Selection ---\n");

    // Simulate different network conditions
    let scenarios = vec![
        (
            "Excellent network",
            ConnectionQualityMetrics {
                latency_ms: 20.0,
                packet_loss_rate: 0.001,
                jitter_ms: 5.0,
                quality_score: 95,
                ..Default::default()
            },
        ),
        (
            "Good network",
            ConnectionQualityMetrics {
                latency_ms: 50.0,
                packet_loss_rate: 0.01,
                jitter_ms: 15.0,
                quality_score: 75,
                ..Default::default()
            },
        ),
        (
            "Poor network",
            ConnectionQualityMetrics {
                latency_ms: 150.0,
                packet_loss_rate: 0.05,
                jitter_ms: 40.0,
                quality_score: 35,
                ..Default::default()
            },
        ),
    ];

    for (name, metrics) in scenarios {
        println!("Scenario: {}", name);
        println!("  Latency: {}ms", metrics.latency_ms);
        println!("  Packet loss: {:.2}%", metrics.packet_loss_rate * 100.0);
        println!("  Jitter: {}ms", metrics.jitter_ms);

        let profile = select_quality_from_metrics(&metrics);
        println!(
            "  Selected: {} ({:?}, {}kbps)",
            profile.name, profile.resolution, profile.bitrate_kbps
        );
        println!();
    }
}

/// Demonstrates multi-output routing configuration
fn demonstrate_multi_output() {
    println!("\n--- Multi-Output Routing ---\n");

    // Define output destinations
    let outputs = vec![
        ("Primary stream", VideoQualityProfile::high()),
        ("Mobile viewers", VideoQualityProfile::medium()),
        ("Thumbnail/preview", VideoQualityProfile::low()),
    ];

    println!("Video processing pipeline:");
    println!("  Input → Encoder → Router");
    println!("                       │");

    for (name, profile) in &outputs {
        println!(
            "                       ├─→ {} ({:?}, {}fps, {}kbps)",
            name, profile.resolution, profile.framerate, profile.bitrate_kbps
        );
    }
    println!();

    // Show bandwidth requirements
    let total_bitrate: u32 = outputs.iter().map(|(_, p)| p.bitrate_kbps).sum();
    println!(
        "Total output bandwidth: {} kbps ({:.1} Mbps)",
        total_bitrate,
        total_bitrate as f32 / 1000.0
    );
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    println!("Multi-Output Video Processing Example");
    println!("=====================================\n");

    // Configure transport with high quality preset
    let signaling_url =
        env::var("SIGNALING_URL").unwrap_or_else(|_| "ws://localhost:8080".to_string());

    let config = WebRtcTransportConfig::high_quality_preset(&signaling_url);
    config.validate()?;

    println!("--- High Quality Configuration ---");
    println!("Video codec: {:?}", config.video_codec);
    println!("Max resolution: {:?}", config.options.max_video_resolution);
    println!(
        "Target bitrate: {} kbps",
        config.options.target_bitrate_kbps
    );
    println!("Framerate: {} fps", config.options.video_framerate_fps);
    println!("Jitter buffer: {}ms", config.jitter_buffer_size_ms);

    // Show quality profiles
    println!("\n--- Quality Profiles ---\n");
    for profile in &[
        VideoQualityProfile::high(),
        VideoQualityProfile::medium(),
        VideoQualityProfile::low(),
    ] {
        println!("{} quality:", profile.name);
        println!("  Resolution: {:?}", profile.resolution);
        println!("  Bitrate: {} kbps", profile.bitrate_kbps);
        println!("  Framerate: {} fps", profile.framerate);
        println!("  Codec: {:?}", profile.codec);
        println!();
    }

    // Demonstrate adaptive quality
    demonstrate_adaptive_quality();

    // Demonstrate multi-output routing
    demonstrate_multi_output();

    // Show codec comparison
    println!("--- Codec Comparison ---\n");
    println!("VP9:");
    println!("  - Best compression efficiency");
    println!("  - Higher CPU usage for encoding");
    println!("  - Good for high quality streaming");
    println!();
    println!("VP8:");
    println!("  - Wider hardware decoder support");
    println!("  - Lower encoding complexity");
    println!("  - Good for mobile/low-power devices");
    println!();
    println!("H.264:");
    println!("  - Universal hardware support");
    println!("  - Patent considerations");
    println!("  - Best compatibility");

    println!("\nNote: This is a demonstration example.");
    println!("Full implementation requires a running signaling server.");

    Ok(())
}
