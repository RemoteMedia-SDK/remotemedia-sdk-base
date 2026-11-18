// Build script for remotemedia-wire.
//
// The protobuf bindings in `src/generated/` are committed; a normal build
// only checks `protos.stamp` and warns when `proto/common.proto` changed.
//
//   Check:       cargo check -p remotemedia-wire --features regen-protos
//   Regenerate:  REMOTEMEDIA_REGEN_PROTOS=1 cargo check -p remotemedia-wire --features regen-protos
//
// `crates/transports/grpc` and `crates/transports/webrtc` compile the same
// `common.proto` into their own bindings; regenerate them too.

include!("../../proto/codegen_support.rs");

const PROTOS: &[&str] = &["../../proto/common.proto"];

fn main() {
    let crate_dir = std::path::PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").unwrap());
    let generated_dir = crate_dir.join("src").join("generated");

    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=../../proto/codegen_support.rs");

    #[cfg(not(feature = "regen-protos"))]
    proto_codegen::default_build("remotemedia-wire", &crate_dir, &generated_dir, PROTOS);

    #[cfg(feature = "regen-protos")]
    {
        let out_dir = std::path::PathBuf::from(std::env::var_os("OUT_DIR").unwrap());
        proto_codegen::regen_build(
            "remotemedia-wire",
            &crate_dir,
            &generated_dir,
            &out_dir,
            PROTOS,
            |fresh_dir| {
                let fds = protox::compile(PROTOS, ["../../proto/"])
                    .unwrap_or_else(|e| panic!("Failed to parse protos: {e:?}"));
                // Bulk payload fields are `bytes::Bytes`: decoding from a
                // received buffer keeps it instead of copying each payload.
                tonic_prost_build::configure()
                    .type_attribute(".", "#[allow(clippy::all)]")
                    .build_server(false)
                    .build_client(false)
                    .bytes(".remotemedia.v1.AudioBuffer.samples")
                    .bytes(".remotemedia.v1.VideoFrame.pixel_data")
                    .bytes(".remotemedia.v1.TensorBuffer.data")
                    .bytes(".remotemedia.v1.NumpyBuffer.data")
                    .bytes(".remotemedia.v1.BinaryBuffer.data")
                    .bytes(".remotemedia.v1.ImageBuffer.data")
                    .out_dir(fresh_dir)
                    .compile_fds(fds)
                    .unwrap_or_else(|e| panic!("Failed to compile protos: {}", e));
            },
        );
    }
}
