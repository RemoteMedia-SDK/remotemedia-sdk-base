//! The part of the TFLite C API this crate uses, resolved from a LiteRT
//! library at run time: the interpreter with the XNNPACK (CPU) delegate.

use std::collections::HashMap;
use std::ffi::{c_char, c_void, CString};
use std::ops::Range;
use std::sync::Arc;

use libloading::Library;

use crate::backend::Backend;

type Ptr = *mut c_void;

/// `TfLiteXNNPackDelegateOptions`, with room to spare: only `num_threads`
/// (the first field) is set here, and the struct is returned by value, so a
/// larger buffer is safe across versions.
#[repr(C)]
pub struct XnnPackOptions {
    pub num_threads: i32,
    rest: [u8; 508],
}

pub struct Api {
    _lib: Library,
    pub source: String,
    model_create_from_file: unsafe extern "C" fn(*const c_char) -> Ptr,
    model_delete: unsafe extern "C" fn(Ptr),
    options_create: unsafe extern "C" fn() -> Ptr,
    options_delete: unsafe extern "C" fn(Ptr),
    options_set_num_threads: unsafe extern "C" fn(Ptr, i32),
    options_add_delegate: unsafe extern "C" fn(Ptr, Ptr),
    interpreter_create: unsafe extern "C" fn(Ptr, Ptr) -> Ptr,
    interpreter_delete: unsafe extern "C" fn(Ptr),
    signature_runner: unsafe extern "C" fn(Ptr, *const c_char) -> Ptr,
    runner_allocate: unsafe extern "C" fn(Ptr) -> i32,
    runner_input: unsafe extern "C" fn(Ptr, *const c_char) -> Ptr,
    runner_output: unsafe extern "C" fn(Ptr, *const c_char) -> Ptr,
    runner_invoke: unsafe extern "C" fn(Ptr) -> i32,
    runner_delete: unsafe extern "C" fn(Ptr),
    tensor_data: unsafe extern "C" fn(Ptr) -> Ptr,
    tensor_byte_size: unsafe extern "C" fn(Ptr) -> usize,
    xnnpack_options_default: unsafe extern "C" fn() -> XnnPackOptions,
    xnnpack_create: unsafe extern "C" fn(*const XnnPackOptions) -> Ptr,
    xnnpack_delete: unsafe extern "C" fn(Ptr),
}

/// Library names tried when none is given.
pub(crate) fn default_candidates() -> &'static [&'static str] {
    if cfg!(target_os = "windows") {
        &["litert-lm.dll", "tensorflowlite_c.dll"]
    } else if cfg!(target_os = "macos") {
        &["liblitert-lm.dylib", "libtensorflowlite_c.dylib"]
    } else {
        // liblitert_lm.so: the LiteRT-LM build Android apps ship (it exports the
        // TFLite C API); liblitert-lm.so: LiteRT-LM's Linux release.
        &[
            "liblitert_lm.so",
            "liblitert-lm.so",
            "libtensorflowlite_c.so",
        ]
    }
}

macro_rules! sym {
    ($lib:expr, $name:literal) => {
        *$lib
            .get(concat!($name, "\0").as_bytes())
            .map_err(|e| format!("{}: {e}", $name))?
    };
}

impl Api {
    /// Load from `path`, else `REMOTEMEDIA_LITERT_LIB`, else the platform's
    /// usual names (found through the loader's search path).
    pub fn load(path: Option<&str>) -> Result<Arc<Self>, String> {
        let env = std::env::var("REMOTEMEDIA_LITERT_LIB").ok();
        let candidates: Vec<String> = match path.map(str::to_string).or(env) {
            Some(p) => vec![p],
            None => default_candidates().iter().map(|s| s.to_string()).collect(),
        };
        let mut errors = Vec::new();
        for name in &candidates {
            match unsafe { Library::new(name) } {
                Ok(lib) => match unsafe { Self::resolve(lib, name) } {
                    Ok(api) => return Ok(Arc::new(api)),
                    Err(e) => errors.push(format!("{name}: missing {e}")),
                },
                Err(e) => errors.push(format!("{name}: {e}")),
            }
        }
        Err(format!(
            "no LiteRT library with the TFLite C API: {}",
            errors.join("; ")
        ))
    }

    unsafe fn resolve(lib: Library, source: &str) -> Result<Self, String> {
        Ok(Self {
            model_create_from_file: sym!(lib, "TfLiteModelCreateFromFile"),
            model_delete: sym!(lib, "TfLiteModelDelete"),
            options_create: sym!(lib, "TfLiteInterpreterOptionsCreate"),
            options_delete: sym!(lib, "TfLiteInterpreterOptionsDelete"),
            options_set_num_threads: sym!(lib, "TfLiteInterpreterOptionsSetNumThreads"),
            options_add_delegate: sym!(lib, "TfLiteInterpreterOptionsAddDelegate"),
            interpreter_create: sym!(lib, "TfLiteInterpreterCreate"),
            interpreter_delete: sym!(lib, "TfLiteInterpreterDelete"),
            signature_runner: sym!(lib, "TfLiteInterpreterGetSignatureRunner"),
            runner_allocate: sym!(lib, "TfLiteSignatureRunnerAllocateTensors"),
            runner_input: sym!(lib, "TfLiteSignatureRunnerGetInputTensor"),
            runner_output: sym!(lib, "TfLiteSignatureRunnerGetOutputTensor"),
            runner_invoke: sym!(lib, "TfLiteSignatureRunnerInvoke"),
            runner_delete: sym!(lib, "TfLiteSignatureRunnerDelete"),
            tensor_data: sym!(lib, "TfLiteTensorData"),
            tensor_byte_size: sym!(lib, "TfLiteTensorByteSize"),
            xnnpack_options_default: sym!(lib, "TfLiteXNNPackDelegateOptionsDefault"),
            xnnpack_create: sym!(lib, "TfLiteXNNPackDelegateCreate"),
            xnnpack_delete: sym!(lib, "TfLiteXNNPackDelegateDelete"),
            source: source.to_string(),
            _lib: lib,
        })
    }
}

/// A model with one interpreter on the XNNPACK (CPU) delegate.
pub struct Interpreter {
    api: Arc<Api>,
    model: Ptr,
    delegate: Ptr,
    interpreter: Ptr,
}

impl Interpreter {
    pub fn load(api: Arc<Api>, model_path: &str, threads: i32) -> Result<Self, String> {
        let path = CString::new(model_path).map_err(|e| e.to_string())?;
        unsafe {
            let model = (api.model_create_from_file)(path.as_ptr());
            if model.is_null() {
                return Err(format!("cannot load model {model_path}"));
            }
            let mut xnn = (api.xnnpack_options_default)();
            xnn.num_threads = threads;
            let delegate = (api.xnnpack_create)(&xnn);
            let options = (api.options_create)();
            (api.options_set_num_threads)(options, threads);
            if !delegate.is_null() {
                (api.options_add_delegate)(options, delegate);
            }
            let interpreter = (api.interpreter_create)(model, options);
            (api.options_delete)(options);
            if interpreter.is_null() {
                if !delegate.is_null() {
                    (api.xnnpack_delete)(delegate);
                }
                (api.model_delete)(model);
                return Err(format!("cannot create an interpreter for {model_path}"));
            }
            Ok(Self {
                api,
                model,
                delegate,
                interpreter,
            })
        }
    }

    /// A signature runner. Keep it in a field declared before the
    /// interpreter, so it is dropped first.
    pub fn signature(&self, key: &str) -> Result<Signature, String> {
        let name = CString::new(key).map_err(|e| e.to_string())?;
        unsafe {
            let runner = (self.api.signature_runner)(self.interpreter, name.as_ptr());
            if runner.is_null() {
                return Err(format!("the model has no signature '{key}'"));
            }
            if (self.api.runner_allocate)(runner) != 0 {
                (self.api.runner_delete)(runner);
                return Err(format!("cannot allocate tensors for '{key}'"));
            }
            Ok(Signature {
                api: Arc::clone(&self.api),
                runner,
                key: key.to_string(),
            })
        }
    }
}

impl Drop for Interpreter {
    fn drop(&mut self) {
        unsafe {
            (self.api.interpreter_delete)(self.interpreter);
            if !self.delegate.is_null() {
                (self.api.xnnpack_delete)(self.delegate);
            }
            (self.api.model_delete)(self.model);
        }
    }
}

/// A signature runner of an [`Interpreter`]; must not outlive it.
pub struct Signature {
    api: Arc<Api>,
    runner: Ptr,
    key: String,
}

impl Signature {
    fn tensor(&self, name: &str, input: bool) -> Result<(*mut u8, usize), String> {
        let cname = CString::new(name).map_err(|e| e.to_string())?;
        unsafe {
            let tensor = if input {
                (self.api.runner_input)(self.runner, cname.as_ptr())
            } else {
                (self.api.runner_output)(self.runner, cname.as_ptr())
            };
            if tensor.is_null() {
                return Err(format!("'{}' has no tensor '{name}'", self.key));
            }
            let data = (self.api.tensor_data)(tensor) as *mut u8;
            if data.is_null() {
                return Err(format!("'{}' tensor '{name}' has no buffer", self.key));
            }
            Ok((data, (self.api.tensor_byte_size)(tensor)))
        }
    }

    /// An input tensor's size in bytes.
    pub fn input_bytes(&self, name: &str) -> Result<usize, String> {
        Ok(self.tensor(name, true)?.1)
    }

    /// An input tensor's buffer as `T` (f32 or i32), checked against `len`.
    pub fn input_mut<T: Copy>(&mut self, name: &str, len: usize) -> Result<&mut [T], String> {
        let (data, bytes) = self.tensor(name, true)?;
        if bytes != len * std::mem::size_of::<T>() {
            return Err(format!(
                "'{}' input '{name}' is {bytes} bytes, expected {len} values",
                self.key
            ));
        }
        Ok(unsafe { std::slice::from_raw_parts_mut(data as *mut T, len) })
    }

    /// An output tensor's buffer as f32 (valid until the next invoke).
    pub fn output_f32(&self, name: &str) -> Result<&[f32], String> {
        let (data, bytes) = self.tensor(name, false)?;
        Ok(unsafe { std::slice::from_raw_parts(data as *const f32, bytes / 4) })
    }

    pub fn invoke(&mut self) -> Result<(), String> {
        match unsafe { (self.api.runner_invoke)(self.runner) } {
            0 => Ok(()),
            status => Err(format!("'{}' invoke failed ({status})", self.key)),
        }
    }
}

impl Drop for Signature {
    fn drop(&mut self) {
        unsafe { (self.api.runner_delete)(self.runner) }
    }
}

/// The interpreter backend: XNNPACK on the CPU.
pub struct TfLiteBackend {
    // Signatures before the interpreter: fields drop in order.
    signatures: HashMap<String, Signature>,
    _interpreter: Interpreter,
    source: String,
    threads: i32,
}

impl TfLiteBackend {
    pub fn load(
        lib: Option<&str>,
        model_path: &str,
        threads: i32,
        signatures: &[&str],
    ) -> Result<Self, String> {
        let api = Api::load(lib)?;
        let source = api.source.clone();
        let interpreter = Interpreter::load(api, model_path, threads)?;
        let mut map = HashMap::new();
        for key in signatures {
            map.insert(key.to_string(), interpreter.signature(key)?);
        }
        Ok(Self {
            signatures: map,
            _interpreter: interpreter,
            source,
            threads,
        })
    }

    fn signature(&mut self, key: &str) -> Result<&mut Signature, String> {
        self.signatures
            .get_mut(key)
            .ok_or_else(|| format!("no signature '{key}'"))
    }
}

impl Backend for TfLiteBackend {
    fn input_len(&self, signature: &str, input: &str) -> Result<usize, String> {
        let s = self
            .signatures
            .get(signature)
            .ok_or_else(|| format!("no signature '{signature}'"))?;
        Ok(s.input_bytes(input)? / 4)
    }

    fn output_len(&self, signature: &str, output: &str) -> Result<usize, String> {
        let s = self
            .signatures
            .get(signature)
            .ok_or_else(|| format!("no signature '{signature}'"))?;
        Ok(s.output_f32(output)?.len())
    }

    fn write_f32(&mut self, signature: &str, input: &str, data: &[f32]) -> Result<(), String> {
        self.signature(signature)?
            .input_mut::<f32>(input, data.len())?
            .copy_from_slice(data);
        Ok(())
    }

    fn write_i32(&mut self, signature: &str, input: &str, data: &[i32]) -> Result<(), String> {
        self.signature(signature)?
            .input_mut::<i32>(input, data.len())?
            .copy_from_slice(data);
        Ok(())
    }

    fn invoke(&mut self, signature: &str) -> Result<(), String> {
        self.signature(signature)?.invoke()
    }

    fn read_f32(
        &mut self,
        signature: &str,
        output: &str,
        range: Range<usize>,
        out: &mut Vec<f32>,
    ) -> Result<(), String> {
        let data = self.signature(signature)?.output_f32(output)?;
        let slice = data.get(range).ok_or("output range out of bounds")?;
        out.clear();
        out.extend_from_slice(slice);
        Ok(())
    }

    fn describe(&self) -> String {
        format!(
            "{} (TFLite interpreter, XNNPACK CPU, {} threads)",
            self.source, self.threads
        )
    }
}
