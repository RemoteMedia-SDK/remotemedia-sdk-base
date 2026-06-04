// Build script for remotemedia-grpc transport
// Handles protobuf code generation for gRPC services

fn main() {
    // Use vendored protoc from protobuf-src
    std::env::set_var("PROTOC", protobuf_src::protoc());

    compile_protos();

    // Rebuild when protobuf files change
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=../../../proto/");
}

/// Compile protocol buffers for gRPC service
fn compile_protos() {
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true) // Enable client generation for testing
        .out_dir("src/generated") // Output to src/generated directory
        .compile_protos(
            &[
                "../../../proto/common.proto",
                "../../../proto/execution.proto",
                "../../../proto/streaming.proto",
                "../../../proto/webrtc_signaling.proto",
                "../../../proto/control.proto",
            ],
            &["../../../proto/"],
        )
        .unwrap_or_else(|e| panic!("Failed to compile protos: {}", e));

    println!("cargo:warning=Successfully compiled protocol buffers");
}
