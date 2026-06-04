//! Simple gRPC Client Example
//!
//! This example demonstrates how to create a client that connects to the
//! RemoteMedia gRPC server and gets version information.
//!
//! # Usage
//!
//! First, start the server:
//! ```bash
//! cargo run --example simple_server --package remotemedia-grpc
//! ```
//!
//! Then run the client:
//! ```bash
//! cargo run --example simple_client --package remotemedia-grpc
//! ```

use remotemedia_grpc::generated::{
    pipeline_execution_service_client::PipelineExecutionServiceClient, VersionRequest,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔌 RemoteMedia gRPC Client Example");
    println!();

    // Connect to server
    let server_addr =
        std::env::var("GRPC_SERVER_ADDR").unwrap_or_else(|_| "http://[::1]:50051".to_string());

    println!("📡 Connecting to: {}", server_addr);
    let mut client = PipelineExecutionServiceClient::connect(server_addr).await?;
    println!("✅ Connected!");
    println!();

    // Get version information
    println!("🔍 Querying server version...");
    let request = tonic::Request::new(VersionRequest {
        client_version: "v1".to_string(),
    });

    let response = client.get_version(request).await?;
    let version_response = response.into_inner();

    println!("✅ Server version information:");
    if let Some(info) = version_response.version_info {
        println!("   Protocol: {}", info.protocol_version);
        println!("   Runtime: {}", info.runtime_version);
        println!("   Supported protocols: {:?}", info.supported_protocols);
        println!(
            "   Node types: {} registered",
            info.supported_node_types.len()
        );
    }
    println!("   Compatible: {}", version_response.compatible);
    if !version_response.compatibility_message.is_empty() {
        println!("   Message: {}", version_response.compatibility_message);
    }

    println!();
    println!("✨ Done!");
    println!();
    println!("💡 Tip: For a full pipeline execution example, see the integration tests");
    println!("   in runtime/tests/grpc_integration/");
    Ok(())
}
