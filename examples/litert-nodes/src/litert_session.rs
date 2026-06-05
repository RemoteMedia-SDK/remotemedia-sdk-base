//! LiteRT/TFLite session and signature runner.

use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_void};
use std::sync::Arc;

use crate::error::{LiteRtError, Result};
use crate::litert_ffi::*;
use crate::tensor_marshaling::{TensorMarshaler, TensorSpec};

/// LiteRT/TFLite inference session
pub struct LiteRtSession {
    interpreter: *mut TfLiteInterpreter,
    signature_name: String,
    input_specs: HashMap<String, TensorSpec>,
    output_specs: HashMap<String, TensorSpec>,
    use_litert: bool,
}

impl LiteRtSession {
    /// Create a new session for the given signature
    pub fn new(
        tf_model: *mut TfLiteModel,
        signature_name: &str,
        config: super::SessionConfig,
        use_litert: bool,
    ) -> Result<Self> {
        // Create interpreter options
        let options = unsafe { TfLiteInterpreterOptionsCreate() };
        if options.is_null() {
            return Err(LiteRtError::ffi("TfLiteInterpreterOptionsCreate returned null"));
        }

        if config.num_threads > 0 {
            unsafe { TfLiteInterpreterOptionsSetNumThreads(options, config.num_threads) };
        }

        // Create interpreter
        let interpreter = unsafe { TfLiteInterpreterCreate(tf_model, options) };
        unsafe { TfLiteInterpreterOptionsDelete(options) };

        if interpreter.is_null() {
            return Err(LiteRtError::ffi("TfLiteInterpreterCreate returned null"));
        }

        // Allocate tensors
        let status = unsafe { TfLiteInterpreterAllocateTensors(interpreter) };
        if status != 0 {
            return Err(LiteRtError::ffi(format!(
                "TfLiteInterpreterAllocateTensors failed with status: {}",
                status
            )));
        }

        // Discover input/output tensor specs for this signature
        let (input_specs, output_specs) = Self::discover_signature_tensors(interpreter, signature_name)?;

        #[cfg(target_os = "android")]
        {
            log::info!(
                target: "LiteRtSession",
                "Created session for signature '{}': {} inputs, {} outputs",
                signature_name,
                input_specs.len(),
                output_specs.len()
            );
        }

        Ok(Self {
            interpreter,
            signature_name: signature_name.to_string(),
            input_specs,
            output_specs,
            use_litert,
        })
    }

    /// Discover input/output tensor specifications for a signature
    fn discover_signature_tensors(
        interpreter: *mut TfLiteInterpreter,
        signature_name: &str,
    ) -> Result<(HashMap<String, TensorSpec>, HashMap<String, TensorSpec>)> {
        let mut input_specs = HashMap::new();
        let mut output_specs = HashMap::new();

        // Get input tensor count
        let input_count = unsafe { TfLiteInterpreterGetInputTensorCount(interpreter) };
        if input_count < 0 {
            return Err(LiteRtError::ffi("Failed to get input tensor count"));
        }

        // Get output tensor count
        let output_count = unsafe { TfLiteInterpreterGetOutputTensorCount(interpreter) };
        if output_count < 0 {
            return Err(LiteRtError::ffi("Failed to get output tensor count"));
        }

        // For signature-based models, we need to map signature names to tensor indices
        // The TFLite C API doesn't directly expose signature I/O mapping
        // We'll enumerate all tensors and use naming conventions
        // In practice, Whisper models have specific tensor names

        // Enumerate input tensors
        for i in 0..input_count {
            let tensor = unsafe { TfLiteInterpreterGetInputTensor(interpreter, i) };
            if tensor.is_null() {
                continue;
            }
            let spec = TensorSpec::from_tflite_tensor(tensor)?;
            let name = spec.name.clone();
            input_specs.insert(name, spec);
        }

        // Enumerate output tensors
        for i in 0..output_count {
            let tensor = unsafe { TfLiteInterpreterGetOutputTensor(interpreter, i) };
            if tensor.is_null() {
                continue;
            }
            let spec = TensorSpec::from_tflite_tensor(tensor)?;
            let name = spec.name.clone();
            output_specs.insert(name, spec);
        }

        #[cfg(target_os = "android")]
        {
            log::debug!(
                target: "LiteRtSession",
                "Signature '{}' inputs: {:?}, outputs: {:?}",
                signature_name,
                input_specs.keys().collect::<Vec<_>>(),
                output_specs.keys().collect::<Vec<_>>()
            );
        }

        Ok((input_specs, output_specs))
    }

    /// Get a signature runner for this session's signature
    pub fn signature_runner(&self) -> SignatureRunner {
        SignatureRunner {
            session: self,
            signature_name: self.signature_name.clone(),
        }
    }

    /// Get input tensor specifications
    pub fn input_specs(&self) -> &HashMap<String, TensorSpec> {
        &self.input_specs
    }

    /// Get output tensor specifications
    pub fn output_specs(&self) -> &HashMap<String, TensorSpec> {
        &self.output_specs
    }

    /// Get the raw interpreter pointer (for advanced usage)
    pub fn interpreter(&self) -> *mut TfLiteInterpreter {
        self.interpreter
    }
}

impl Drop for LiteRtSession {
    fn drop(&mut self) {
        if !self.interpreter.is_null() {
            unsafe { TfLiteInterpreterDelete(self.interpreter) };
        }
    }
}

/// Safe wrapper for running a specific signature
pub struct SignatureRunner<'a> {
    session: &'a LiteRtSession,
    signature_name: String,
}

impl<'a> SignatureRunner<'a> {
    /// Get the signature name
    pub fn signature_name(&self) -> &str {
        &self.signature_name
    }

    /// Get input tensor specification by name
    pub fn input_spec(&self, name: &str) -> Option<&TensorSpec> {
        self.session.input_specs.get(name)
    }

    /// Get output tensor specification by name
    pub fn output_spec(&self, name: &str) -> Option<&TensorSpec> {
        self.session.output_specs.get(name)
    }

    /// Set input tensor data by name
    pub fn set_input<T: TensorMarshaler>(&self, name: &str, data: &T) -> Result<()> {
        let spec = self.session.input_specs.get(name).ok_or_else(|| {
            LiteRtError::ffi(format!("Input tensor '{}' not found in signature '{}'", name, self.signature_name))
        })?;

        let tensor = self.get_input_tensor_by_name(name)?;
        spec.validate_for_input(tensor)?;
        data.copy_to_tensor(tensor, spec)
    }

    /// Get output tensor data by name
    pub fn get_output<T: TensorMarshaler>(&self, name: &str) -> Result<T> {
        let spec = self.session.output_specs.get(name).ok_or_else(|| {
            LiteRtError::ffi(format!("Output tensor '{}' not found in signature '{}'", name, self.signature_name))
        })?;

        let tensor = self.get_output_tensor_by_name(name)?;
        spec.validate_for_output(tensor)?;
        T::from_tensor(tensor, spec)
    }

    /// Invoke the signature
    pub fn invoke(&self) -> Result<()> {
        let status = unsafe { TfLiteInterpreterInvoke(self.session.interpreter) };
        if status != 0 {
            return Err(LiteRtError::ffi(format!(
                "TfLiteInterpreterInvoke failed with status: {} for signature '{}'",
                status, self.signature_name
            )));
        }
        Ok(())
    }

    /// Run inference with input data and return output data
    pub fn run<T: TensorMarshaler, U: TensorMarshaler>(
        &self,
        inputs: &HashMap<String, T>,
    ) -> Result<HashMap<String, U>> {
        // Set all inputs
        for (name, data) in inputs {
            self.set_input(name, data)?;
        }

        // Invoke
        self.invoke()?;

        // Get all outputs
        let mut outputs = HashMap::new();
        for name in self.session.output_specs.keys() {
            outputs.insert(name.clone(), self.get_output(name)?);
        }

        Ok(outputs)
    }

    fn get_input_tensor_by_name(&self, name: &str) -> Result<*mut TfLiteTensor> {
        // TFLite C API doesn't have direct name lookup for inputs/outputs
        // We need to find by iterating
        let input_count = unsafe { TfLiteInterpreterGetInputTensorCount(self.session.interpreter) };
        for i in 0..input_count {
            let tensor = unsafe { TfLiteInterpreterGetInputTensor(self.session.interpreter, i) };
            if tensor.is_null() {
                continue;
            }
            let tensor_name = unsafe { TfLiteTensorName(tensor) };
            if !tensor_name.is_null() {
                let cstr = unsafe { CStr::from_ptr(tensor_name) };
                if cstr.to_str().map(|s| s == name).unwrap_or(false) {
                    return Ok(tensor);
                }
            }
        }
        Err(LiteRtError::ffi(format!("Input tensor '{}' not found", name)))
    }

    fn get_output_tensor_by_name(&self, name: &str) -> Result<*mut TfLiteTensor> {
        let output_count = unsafe { TfLiteInterpreterGetOutputTensorCount(self.session.interpreter) };
        for i in 0..output_count {
            let tensor = unsafe { TfLiteInterpreterGetOutputTensor(self.session.interpreter, i) };
            if tensor.is_null() {
                continue;
            }
            let tensor_name = unsafe { TfLiteTensorName(tensor) };
            if !tensor_name.is_null() {
                let cstr = unsafe { CStr::from_ptr(tensor_name) };
                if cstr.to_str().map(|s| s == name).unwrap_or(false) {
                    return Ok(tensor);
                }
            }
        }
        Err(LiteRtError::ffi(format!("Output tensor '{}' not found", name)))
    }
}
