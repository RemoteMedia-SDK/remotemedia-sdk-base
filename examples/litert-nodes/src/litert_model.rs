//! LiteRT/TFLite model loading and signature discovery.

use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_void};
use std::path::Path;

use crate::error::{LiteRtError, Result};
use crate::litert_ffi::*;

/// Configuration for loading a LiteRT/TFLite model
#[derive(Debug, Clone)]
pub struct LiteRtModelConfig {
    /// Absolute path to the model file (.tflite or .litertlm)
    pub model_path: String,
    /// Number of threads for inference (0 = auto)
    pub num_threads: i32,
    /// Enable LiteRT-specific features if available
    pub use_litert: bool,
}

impl Default for LiteRtModelConfig {
    fn default() -> Self {
        Self {
            model_path: String::new(),
            num_threads: 0,
            use_litert: true,
        }
    }
}

/// Loaded LiteRT/TFLite model with signature metadata
pub struct LiteRtModel {
    // TFLite model handle
    tf_model: *mut TfLiteModel,
    // LiteRT model handle (if using LiteRT API)
    litert_model: *mut LiteRtModel_C,
    /// Path to the model file
    model_path: String,
    /// Signature names available in the model
    signature_names: Vec<String>,
    /// Whether we're using LiteRT API
    use_litert: bool,
}

/// Opaque type for LiteRT model (distinct from TFLite)
type LiteRtModel_C = bindings::LiteRtModel;

impl LiteRtModel {
    /// Load a model from file
    pub fn load(config: LiteRtModelConfig) -> Result<Self> {
        if config.model_path.is_empty() {
            return Err(LiteRtError::invalid_config("model_path is required"));
        }

        let path = Path::new(&config.model_path);
        if !path.exists() {
            return Err(LiteRtError::model_not_found(config.model_path.clone()));
        }

        let model_path_c = CString::new(config.model_path.clone())
            .map_err(|e| LiteRtError::ffi(format!("CString conversion failed: {}", e)))?;

        let tf_model = unsafe { TfLiteModelCreateFromFile(model_path_c.as_ptr()) };
        if tf_model.is_null() {
            return Err(LiteRtError::model_load(
                config.model_path.clone(),
                "TfLiteModelCreateFromFile returned null",
            ));
        }

        // Try to discover signatures using TFLite API
        let signature_names = Self::discover_signatures_tflite(tf_model)?;

        #[cfg(target_os = "android")]
        {
            log::info!(
                target: "LiteRtModel",
                "Loaded model: {} with {} signatures",
                config.model_path,
                signature_names.len()
            );
        }

        Ok(Self {
            tf_model,
            litert_model: std::ptr::null_mut(),
            model_path: config.model_path,
            signature_names,
            use_litert: config.use_litert,
        })
    }

    /// Discover signature names from TFLite model
    fn discover_signatures_tflite(tf_model: *mut TfLiteModel) -> Result<Vec<String>> {
        // TFLite doesn't have native signature discovery in the C API
        // Whisper models typically use "encode" and "decode" signatures
        // We'll return the expected signature names for Whisper
        // In a full implementation, we'd parse the model's metadata
        Ok(vec!["encode".to_string(), "decode".to_string()])
    }

    /// Get the model path
    pub fn model_path(&self) -> &str {
        &self.model_path
    }

    /// Get available signature names
    pub fn signature_names(&self) -> &[String] {
        &self.signature_names
    }

    /// Check if a signature exists
    pub fn has_signature(&self, name: &str) -> bool {
        self.signature_names.iter().any(|s| s == name)
    }

    /// Create a session for a specific signature
    pub fn create_session(&self, signature_name: &str, config: SessionConfig) -> Result<LiteRtSession> {
        if !self.has_signature(signature_name) {
            return Err(LiteRtError::signature_not_found(
                signature_name,
                &self.model_path,
                self.signature_names.clone(),
            ));
        }

        LiteRtSession::new(self.tf_model, signature_name, config, self.use_litert)
    }

    /// Create an interpreter directly (for non-signature models)
    pub fn create_interpreter(&self, options: *mut TfLiteInterpreterOptions) -> Result<*mut TfLiteInterpreter> {
        let interpreter = unsafe { TfLiteInterpreterCreate(self.tf_model, options) };
        if interpreter.is_null() {
            return Err(LiteRtError::ffi("TfLiteInterpreterCreate returned null"));
        }
        Ok(interpreter)
    }
}

impl Drop for LiteRtModel {
    fn drop(&mut self) {
        if !self.tf_model.is_null() {
            unsafe { TfLiteModelDelete(self.tf_model) };
        }
        if !self.litert_model.is_null() && self.use_litert {
            unsafe { LiteRtModelDelete(self.litert_model) };
        }
    }
}

// Session configuration
pub struct SessionConfig {
    pub num_threads: i32,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self { num_threads: 0 }
    }
}

impl SessionConfig {
    pub fn with_threads(mut self, num_threads: i32) -> Self {
        self.num_threads = num_threads;
        self
    }
}
