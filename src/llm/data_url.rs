//! `data:` URL encoding for embedding still images in OpenAI-shape
//! `image_url` content parts. Modern vision LLMs (gpt-4o, llama.cpp +
//! llava, vLLM with vision) accept `data:image/png;base64,…` URLs
//! verbatim, so the multimodal node calls this and drops the result
//! into the request body.
//!
//! `RuntimeData::Image` with `ImageFormat::Raw` is rejected: raw
//! pixels need to be transcoded by an upstream image-encoding node
//! first. The error message names the format so the pipeline author
//! sees the gap immediately.

#[cfg(test)]
use crate::data::ImageFormat;
use crate::data::RuntimeData;
use crate::error::Error;
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;

/// Build a `data:image/...;base64,...` URL from a `RuntimeData::Image`.
pub fn image_to_data_url(data: &RuntimeData) -> Result<String, Error> {
    match data {
        RuntimeData::Image {
            data: bytes,
            format,
            ..
        } => {
            let mime = format.mime_type().ok_or_else(|| {
                Error::Execution(
                    "MultimodalLLMNode: raw-pixel images need an upstream encoder \
                     (insert a JPEG/PNG encoder node before the LLM)"
                        .into(),
                )
            })?;
            Ok(format!("data:{};base64,{}", mime, BASE64.encode(bytes)))
        }
        other => Err(Error::Execution(format!(
            "image_to_data_url: expected RuntimeData::Image, got {}",
            other.data_type()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn img_png(bytes: &[u8]) -> RuntimeData {
        RuntimeData::Image {
            data: bytes.to_vec(),
            format: ImageFormat::Png,
            width: 1,
            height: 1,
            timestamp_us: None,
            stream_id: None,
            metadata: None,
        }
    }

    #[test]
    fn png_round_trip() {
        let url = image_to_data_url(&img_png(&[1, 2, 3, 4])).unwrap();
        assert!(url.starts_with("data:image/png;base64,"));
        let payload = &url["data:image/png;base64,".len()..];
        let decoded = BASE64.decode(payload).unwrap();
        assert_eq!(decoded, vec![1, 2, 3, 4]);
    }

    #[test]
    fn jpeg_uses_correct_mime() {
        let img = RuntimeData::Image {
            data: vec![0xFF, 0xD8, 0xFF],
            format: ImageFormat::Jpeg,
            width: 1,
            height: 1,
            timestamp_us: None,
            stream_id: None,
            metadata: None,
        };
        let url = image_to_data_url(&img).unwrap();
        assert!(url.starts_with("data:image/jpeg;base64,"));
    }

    #[test]
    fn raw_pixels_rejected_with_actionable_error() {
        let img = RuntimeData::Image {
            data: vec![0u8; 4],
            format: ImageFormat::Raw {
                pixel_format: crate::data::PixelFormat::Rgb24,
            },
            width: 1,
            height: 1,
            timestamp_us: None,
            stream_id: None,
            metadata: None,
        };
        let err = image_to_data_url(&img).unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("upstream encoder"), "got {}", msg);
    }

    #[test]
    fn non_image_input_rejected() {
        let err = image_to_data_url(&RuntimeData::Text("hi".into())).unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("expected RuntimeData::Image"), "got {}", msg);
    }
}
