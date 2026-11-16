// Build script for remotemedia-grpc.
//
// The protobuf bindings in `src/generated/` are committed. A normal build
// uses them as they are: it does not parse the .proto files and writes
// nothing into `src/`. It only hashes the `.proto` files
// and warns when they no longer match `src/generated/protos.stamp`.
//
// Code generation sits behind the `regen-protos` feature, which pulls in
// protox (a pure-Rust .proto compiler: no protoc, CMake or C++ toolchain)
// and tonic-prost-build. From the sdk-base workspace root:
//
//   Check that the committed bindings match the .proto files (writes
//   nothing to `src/`, fails the build on any difference; CI-friendly):
//     cargo check -p remotemedia-grpc --features regen-protos
//
//   Regenerate after editing `proto/*.proto`:
//     REMOTEMEDIA_REGEN_PROTOS=1 cargo check -p remotemedia-grpc --features regen-protos
//     (PowerShell: $env:REMOTEMEDIA_REGEN_PROTOS=1; cargo check ...)
//
// `crates/transports/webrtc` compiles a subset of the same protos into its
// own `src/generated/`; regenerate it too when `common.proto`,
// `control.proto` or `webrtc_signaling.proto` change.

include!("../../../proto/codegen_support.rs");

const PROTOS: &[&str] = &[
    "../../../proto/common.proto",
    "../../../proto/execution.proto",
    "../../../proto/streaming.proto",
    "../../../proto/webrtc_signaling.proto",
    "../../../proto/control.proto",
    "../../../proto/deployment.proto",
    "../../../proto/runtime_control.proto",
];

fn main() {
    let crate_dir = std::path::PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").unwrap());
    let generated_dir = crate_dir.join("src").join("generated");

    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=../../../proto/codegen_support.rs");

    #[cfg(not(feature = "regen-protos"))]
    proto_codegen::default_build("remotemedia-grpc", &crate_dir, &generated_dir, PROTOS);

    #[cfg(feature = "regen-protos")]
    {
        let out_dir = std::path::PathBuf::from(std::env::var_os("OUT_DIR").unwrap());
        proto_codegen::regen_build(
            "remotemedia-grpc",
            &crate_dir,
            &generated_dir,
            &out_dir,
            PROTOS,
            |fresh_dir| {
                // protox parses the .proto files in pure Rust, so the output
                // does not depend on a protoc install or a C++ toolchain.
                let fds = protox::compile(PROTOS, ["../../../proto/"])
                    .unwrap_or_else(|e| panic!("Failed to parse protos: {e:?}"));
                tonic_prost_build::configure()
                    // `compile_protos` adds this to every type; `compile_fds`
                    // does not, so add it here to keep the output identical.
                    .type_attribute(".", "#[allow(clippy::all)]")
                    .build_server(true)
                    .build_client(true) // Enable client generation for testing
                    .out_dir(fresh_dir)
                    .compile_fds(fds)
                    .unwrap_or_else(|e| panic!("Failed to compile protos: {}", e));
            },
        );
    }
}
