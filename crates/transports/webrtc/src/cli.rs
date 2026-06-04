//! CLI integration for WebRTC transport (requires `cli` feature)

use crate::builder::WebRtcServerBuilder;
use clap::Args;

/// CLI arguments for WebRTC transport in WebSocket client mode
#[derive(Args, Debug, Clone)]
pub struct WebRtcServeArgs {
    /// WebSocket signaling server URL
    #[arg(
        long,
        default_value = "ws://localhost:8080",
        env = "WEBRTC_SIGNALING_URL"
    )]
    pub webrtc_signaling_url: String,

    /// STUN servers (comma-separated)
    #[arg(
        long,
        value_delimiter = ',',
        default_value = "stun:stun.l.google.com:19302"
    )]
    pub webrtc_stun_servers: Vec<String>,

    /// Maximum concurrent peer connections
    #[arg(long, default_value_t = 10, env = "WEBRTC_MAX_PEERS")]
    pub webrtc_max_peers: u32,

    /// Enable data channel support
    #[arg(long, default_value_t = true, env = "WEBRTC_ENABLE_DATA_CHANNEL")]
    pub webrtc_enable_data_channel: bool,

    /// Jitter buffer size in milliseconds
    #[arg(long, default_value_t = 100, env = "WEBRTC_JITTER_BUFFER_MS")]
    pub webrtc_jitter_buffer_ms: u32,
}

impl WebRtcServeArgs {
    /// Run the WebRTC transport with these arguments
    pub async fn run(self) -> std::result::Result<(), Box<dyn std::error::Error>> {
        WebRtcServerBuilder::new()
            .signaling_url(&self.webrtc_signaling_url)
            .stun_servers(self.webrtc_stun_servers)
            .max_peers(self.webrtc_max_peers)
            .enable_data_channel(self.webrtc_enable_data_channel)
            .jitter_buffer_ms(self.webrtc_jitter_buffer_ms)
            .build()?
            .run()
            .await
    }
}

#[cfg(feature = "grpc-signaling")]
pub use grpc_signaling_cli::WebRtcSignalingServeArgs;

#[cfg(feature = "grpc-signaling")]
mod grpc_signaling_cli {
    use crate::builder::WebRtcSignalingServerBuilder;
    use clap::Args;
    use remotemedia_core::transport::{PipelineExecutor, PipelineSessionHost};
    use std::path::PathBuf;
    use std::sync::Arc;

    /// CLI arguments for WebRTC gRPC signaling server
    #[derive(Args, Debug, Clone)]
    pub struct WebRtcSignalingServeArgs {
        /// gRPC signaling server bind address
        #[arg(long, default_value = "0.0.0.0:50051", env = "GRPC_SIGNALING_ADDRESS")]
        pub webrtc_grpc_bind: String,

        /// Pipeline manifest path
        #[arg(long, env = "WEBRTC_PIPELINE_MANIFEST")]
        pub webrtc_manifest: PathBuf,

        /// STUN servers (comma-separated)
        #[arg(
            long,
            value_delimiter = ',',
            default_value = "stun:stun.l.google.com:19302"
        )]
        pub webrtc_stun_servers: Vec<String>,

        /// Maximum concurrent peer connections
        #[arg(long, default_value_t = 10, env = "WEBRTC_MAX_PEERS")]
        pub webrtc_max_peers: u32,
    }

    impl WebRtcSignalingServeArgs {
        /// Run the WebRTC gRPC signaling server
        pub async fn run(self) -> std::result::Result<(), Box<dyn std::error::Error>> {
            self.run_with_executor(None).await
        }

        /// Run with an optional pre-configured executor
        pub async fn run_with_executor(
            self,
            executor: Option<Arc<dyn PipelineSessionHost>>,
        ) -> std::result::Result<(), Box<dyn std::error::Error>> {
            let mut builder = WebRtcSignalingServerBuilder::new()
                .bind(&self.webrtc_grpc_bind)
                .manifest_from_file(&self.webrtc_manifest)?
                .stun_servers(self.webrtc_stun_servers)
                .max_peers(self.webrtc_max_peers);

            if let Some(exec) = executor {
                builder = builder.executor(exec);
            }

            builder.build()?.run().await
        }
    }
}
