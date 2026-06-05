//! Error types for LiteRT operations.

use std::fmt;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum LiteRtError {
    #[error("Model loading failed ({model_path}): {source}")]
    ModelLoad {
        model_path: String,
        #[source]
        source: anyhow::Error,
    },

    #[error("Signature '{signature_name}' not found in model {model_path}. Available signatures: {available:?}")]
    SignatureNotFound {
        signature_name: String,
        model_path: String,
        available: Vec<String>,
    },

    #[error("Tensor '{tensor_name}' shape mismatch in signature '{signature_name}' (model: {model_path}): expected {expected:?}, got {actual:?}")]
    TensorShapeMismatch {
        tensor_name: String,
        signature_name: String,
        model_path: String,
        expected: Vec<i32>,
        actual: Vec<i32>,
    },

    #[error("Tensor '{tensor_name}' dtype mismatch in signature '{signature_name}' (model: {model_path}): expected {expected:?}, got {actual:?}")]
    TensorDtypeMismatch {
        tensor_name: String,
        signature_name: String,
        model_path: String,
        expected: &'static str,
        actual: &'static str,
    },

    #[error("Tensor data copy failed for '{tensor_name}': {reason}")]
    TensorCopy {
        tensor_name: String,
        reason: String,
    },

    #[error("Audio preprocessing failed: {reason}")]
    AudioPreprocessing { reason: String },

    #[error("Tokenizer error: {reason}")]
    Tokenizer { reason: String },

    #[error("Decode loop error: {reason}")]
    DecodeLoop { reason: String },

    #[error("Invalid configuration: {reason}")]
    InvalidConfig { reason: String },

    #[error("FFI error: {reason}")]
    Ffi { reason: String },

    #[error("Model file not found: {path}")]
    ModelNotFound { path: String },

    #[error("Android log integration not available")]
    AndroidLogUnavailable,
}

pub type Result<T> = std::result::Result<T, LiteRtError>;

impl LiteRtError {
    pub fn model_load(model_path: impl Into<String>, source: impl Into<anyhow::Error>) -> Self {
        Self::ModelLoad {
            model_path: model_path.into(),
            source: source.into(),
        }
    }

    pub fn signature_not_found(
        signature_name: impl Into<String>,
        model_path: impl Into<String>,
        available: Vec<String>,
    ) -> Self {
        Self::SignatureNotFound {
            signature_name: signature_name.into(),
            model_path: model_path.into(),
            available,
        }
    }

    pub fn tensor_shape_mismatch(
        tensor_name: impl Into<String>,
        signature_name: impl Into<String>,
        model_path: impl Into<String>,
        expected: Vec<i32>,
        actual: Vec<i32>,
    ) -> Self {
        Self::TensorShapeMismatch {
            tensor_name: tensor_name.into(),
            signature_name: signature_name.into(),
            model_path: model_path.into(),
            expected,
            actual,
        }
    }

    pub fn tensor_dtype_mismatch(
        tensor_name: impl Into<String>,
        signature_name: impl Into<String>,
        model_path: impl Into<String>,
        expected: &'static str,
        actual: &'static str,
    ) -> Self {
        Self::TensorDtypeMismatch {
            tensor_name: tensor_name.into(),
            signature_name: signature_name.into(),
            model_path: model_path.into(),
            expected,
            actual,
        }
    }

    pub fn tensor_copy(tensor_name: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::TensorCopy {
            tensor_name: tensor_name.into(),
            reason: reason.into(),
        }
    }

    pub fn audio_preprocessing(reason: impl Into<String>) -> Self {
        Self::AudioPreprocessing {
            reason: reason.into(),
        }
    }

    pub fn tokenizer(reason: impl Into<String>) -> Self {
        Self::Tokenizer {
            reason: reason.into(),
        }
    }

    pub fn decode_loop(reason: impl Into<String>) -> Self {
        Self::DecodeLoop {
            reason: reason.into(),
        }
    }

    pub fn invalid_config(reason: impl Into<String>) -> Self {
        Self::InvalidConfig {
            reason: reason.into(),
        }
    }

    pub fn ffi(reason: impl Into<String>) -> Self {
        Self::Ffi {
            reason: reason.into(),
        }
    }

    pub fn model_not_found(path: impl Into<String>) -> Self {
        Self::ModelNotFound { path: path.into() }
    }
}

#[cfg(target_os = "android")]
impl LiteRtError {
    /// Log error to Android logcat with full context
    pub fn log_android(&self, tag: &str) {
        use android_logger::log::Level;
        let level = match self {
            Self::ModelLoad { .. } | Self::ModelNotFound { .. } => Level::Error,
            Self::SignatureNotFound { .. } => Level::Error,
            Self::TensorShapeMismatch { .. } | Self::TensorDtypeMismatch { .. } => Level::Error,
            Self::TensorCopy { .. } => Level::Error,
            Self::AudioPreprocessing { .. } => Level::Warn,
            Self::Tokenizer { .. } => Level::Warn,
            Self::DecodeLoop { .. } => Level::Error,
            Self::InvalidConfig { .. } => Level::Error,
            Self::Ffi { .. } => Level::Error,
            Self::AndroidLogUnavailable => Level::Debug,
        };
        android_logger::log::logger().log(
            android_logger::log::Record::builder()
                .level(level)
                .target(tag)
                .args(format_args!("{}", self))
                .build(),
        );
    }
}
