//! The wire form of a RemoteMedia frame: protobuf `remotemedia.v1.DataBuffer`
//! (`proto/common.proto`), converted losslessly to and from `RuntimeData`.
//!
//! Bulk data crosses as raw bytes: audio as f32 little-endian, video,
//! images, tensors and binary payloads as their own bytes. Decoding from a
//! received [`Bytes`] keeps that buffer as the payload's storage, so only
//! audio (which needs aligned `f32`s) is copied. The only JSON on the wire
//! is a value that is JSON by type: `RuntimeData::Json` and the free-form
//! `metadata` of audio, image, tensor and control payloads.
//!
//! Copy accounting follows the transports: encoding counts the payload's
//! bulk bytes as [`CopyReason::PortableSerialization`] (a strict context
//! rejects before any payload is touched); decoding counts audio samples as
//! [`CopyReason::PortableDeserialization`].

pub mod generated {
    include!("generated/remotemedia.v1.rs");
}

mod codec;

pub use codec::{
    data_buffer_into_frame, data_buffer_into_runtime_data, decode_frame, encode_frame,
    encode_frame_into, runtime_data_to_data_buffer, Frame, PreparedFrame, WireError,
};
pub use generated::DataBuffer;
pub use remotemedia_types::{CopyContext, CopyReason};

/// Guards the committed protobuf bindings in `src/generated/` (see `build.rs`).
#[cfg(test)]
mod proto_stamp_tests {
    include!("../../../proto/codegen_support.rs");

    #[test]
    fn committed_bindings_match_proto_files() {
        let crate_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let protos: Vec<&str> = env!("REMOTEMEDIA_PROTO_INPUTS").split(';').collect();
        if let Some(problem) =
            proto_codegen::stamp_mismatch(crate_dir, &crate_dir.join("src/generated"), &protos)
        {
            panic!(
                "{problem}\nregenerate: REMOTEMEDIA_REGEN_PROTOS=1 cargo check -p remotemedia-wire --features regen-protos"
            );
        }
    }
}
