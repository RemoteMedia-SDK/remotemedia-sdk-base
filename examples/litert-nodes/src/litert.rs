//! The part of the TFLite C API this crate uses, resolved from a LiteRT
//! library at run time: the interpreter with the XNNPACK (CPU) delegate.

use std::collections::HashMap;
use std::ffi::{c_char, c_void, CString};
use std::ops::Range;
use std::sync::Arc;

use libloading::Library;

use crate::backend::Backend;
use crate::compiled::ModelSource;

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
    /// Never unloaded: see [`crate::litert::keep_loaded`].
    _lib: std::mem::ManuallyDrop<Library>,
    pub source: String,
    model_create_from_file: unsafe extern "C" fn(*const c_char) -> Ptr,
    model_create: unsafe extern "C" fn(*const c_void, usize) -> Ptr,
    tensor_type: unsafe extern "C" fn(Ptr) -> i32,
    model_delete: unsafe extern "C" fn(Ptr),
    options_create: unsafe extern "C" fn() -> Ptr,
    options_delete: unsafe extern "C" fn(Ptr),
    options_set_num_threads: unsafe extern "C" fn(Ptr, i32),
    options_add_delegate: unsafe extern "C" fn(Ptr, Ptr),
    interpreter_create: unsafe extern "C" fn(Ptr, Ptr) -> Ptr,
    interpreter_delete: unsafe extern "C" fn(Ptr),
    signature_runner: unsafe extern "C" fn(Ptr, *const c_char) -> Ptr,
    runner_allocate: unsafe extern "C" fn(Ptr) -> i32,
    runner_resize: unsafe extern "C" fn(Ptr, *const c_char, *const i32, i32) -> i32,
    runner_input: unsafe extern "C" fn(Ptr, *const c_char) -> Ptr,
    runner_output: unsafe extern "C" fn(Ptr, *const c_char) -> Ptr,
    runner_invoke: unsafe extern "C" fn(Ptr) -> i32,
    runner_delete: unsafe extern "C" fn(Ptr),
    tensor_data: unsafe extern "C" fn(Ptr) -> Ptr,
    tensor_byte_size: unsafe extern "C" fn(Ptr) -> usize,
    xnnpack_options_default: unsafe extern "C" fn() -> XnnPackOptions,
    xnnpack_create: unsafe extern "C" fn(*const XnnPackOptions) -> Ptr,
    xnnpack_delete: unsafe extern "C" fn(Ptr),
    ops: Option<OperatorFns>,
}

/// The TFLite operator API, for registering the BiLSTM custom op.
#[derive(Clone, Copy)]
struct OperatorFns {
    create: unsafe extern "C" fn(i32, *const c_char, i32, Ptr) -> Ptr,
    delete: unsafe extern "C" fn(Ptr),
    set_invoke: unsafe extern "C" fn(Ptr, unsafe extern "C" fn(Ptr, Ptr) -> i32),
    add: unsafe extern "C" fn(Ptr, Ptr),
    num_inputs: unsafe extern "C" fn(Ptr) -> i32,
    input: unsafe extern "C" fn(Ptr, Ptr, i32) -> Ptr,
    output: unsafe extern "C" fn(Ptr, Ptr, i32) -> Ptr,
    data: unsafe extern "C" fn(Ptr) -> Ptr,
    byte_size: unsafe extern "C" fn(Ptr) -> usize,
    num_dims: unsafe extern "C" fn(Ptr) -> i32,
    dim: unsafe extern "C" fn(Ptr, i32) -> i32,
}

static OPERATOR_FNS: std::sync::OnceLock<OperatorFns> = std::sync::OnceLock::new();
const BUILTIN_CUSTOM: i32 = 32;

unsafe extern "C" fn bilstm_invoke(context: Ptr, node: Ptr) -> i32 {
    let Some(f) = OPERATOR_FNS.get().copied() else {
        return 1;
    };
    if (f.num_inputs)(node) != crate::bilstm::INPUTS as i32 {
        return 1;
    }
    let dims = |t: Ptr| -> Vec<usize> {
        (0..(f.num_dims)(t)).map(|i| (f.dim)(t, i) as usize).collect()
    };
    let tensors: Vec<Ptr> = (0..crate::bilstm::INPUTS as i32)
        .map(|i| (f.input)(context, node, i))
        .collect();
    if tensors.iter().any(|t| t.is_null() || (f.data)(*t).is_null()) {
        return 1;
    }
    let (x, w_hh) = (dims(tensors[0]), dims(tensors[4]));
    if x.len() != 3 || w_hh.len() != 2 {
        return 1;
    }
    let ins: [&[u8]; crate::bilstm::INPUTS] = std::array::from_fn(|i| {
        std::slice::from_raw_parts((f.data)(tensors[i]) as *const u8, (f.byte_size)(tensors[i]))
    });
    let out = (f.output)(context, node, 0);
    if out.is_null() || (f.data)(out).is_null() {
        return 1;
    }
    let out = std::slice::from_raw_parts_mut((f.data)(out) as *mut u8, (f.byte_size)(out));
    match crate::bilstm::run(ins, out, [x[0], x[1], x[2]], w_hh[1]) {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("[litert] {e}");
            1
        }
    }
}

/// Keeps a runtime library mapped for the rest of the process. The LiteRT
/// runtimes register thread-local destructors on the threads that call them
/// (the model thread, GPU worker threads); unloading the library when a
/// session ends leaves those pointing at unmapped code, and the thread's exit
/// then crashes the process (seen on Android: SIGSEGV in
/// `pthread_key_clean_all` on the `litert-lm` thread). A later session's
/// `dlopen` of the same path reuses the mapping.
pub(crate) fn keep_loaded(lib: Library) -> std::mem::ManuallyDrop<Library> {
    std::mem::ManuallyDrop::new(lib)
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
            model_create: sym!(lib, "TfLiteModelCreate"),
            tensor_type: sym!(lib, "TfLiteTensorType"),
            model_delete: sym!(lib, "TfLiteModelDelete"),
            options_create: sym!(lib, "TfLiteInterpreterOptionsCreate"),
            options_delete: sym!(lib, "TfLiteInterpreterOptionsDelete"),
            options_set_num_threads: sym!(lib, "TfLiteInterpreterOptionsSetNumThreads"),
            options_add_delegate: sym!(lib, "TfLiteInterpreterOptionsAddDelegate"),
            interpreter_create: sym!(lib, "TfLiteInterpreterCreate"),
            interpreter_delete: sym!(lib, "TfLiteInterpreterDelete"),
            signature_runner: sym!(lib, "TfLiteInterpreterGetSignatureRunner"),
            runner_allocate: sym!(lib, "TfLiteSignatureRunnerAllocateTensors"),
            runner_resize: sym!(lib, "TfLiteSignatureRunnerResizeInputTensor"),
            runner_input: sym!(lib, "TfLiteSignatureRunnerGetInputTensor"),
            runner_output: sym!(lib, "TfLiteSignatureRunnerGetOutputTensor"),
            runner_invoke: sym!(lib, "TfLiteSignatureRunnerInvoke"),
            runner_delete: sym!(lib, "TfLiteSignatureRunnerDelete"),
            tensor_data: sym!(lib, "TfLiteTensorData"),
            tensor_byte_size: sym!(lib, "TfLiteTensorByteSize"),
            xnnpack_options_default: sym!(lib, "TfLiteXNNPackDelegateOptionsDefault"),
            xnnpack_create: sym!(lib, "TfLiteXNNPackDelegateCreate"),
            xnnpack_delete: sym!(lib, "TfLiteXNNPackDelegateDelete"),
            ops: (|| -> Result<OperatorFns, String> {
                Ok(OperatorFns {
                    create: sym!(lib, "TfLiteOperatorCreate"),
                    delete: sym!(lib, "TfLiteOperatorDelete"),
                    set_invoke: sym!(lib, "TfLiteOperatorSetInvoke"),
                    add: sym!(lib, "TfLiteInterpreterOptionsAddOperator"),
                    num_inputs: sym!(lib, "TfLiteOpaqueNodeNumberOfInputs"),
                    input: sym!(lib, "TfLiteOpaqueNodeGetInput"),
                    output: sym!(lib, "TfLiteOpaqueNodeGetOutput"),
                    data: sym!(lib, "TfLiteOpaqueTensorData"),
                    byte_size: sym!(lib, "TfLiteOpaqueTensorByteSize"),
                    num_dims: sym!(lib, "TfLiteOpaqueTensorNumDims"),
                    dim: sym!(lib, "TfLiteOpaqueTensorDim"),
                })
            })()
            .ok(),
            source: source.to_string(),
            _lib: crate::litert::keep_loaded(lib),
        })
    }
}

/// A model with one interpreter on the XNNPACK (CPU) delegate.
pub struct Interpreter {
    api: Arc<Api>,
    model: Ptr,
    delegate: Ptr,
    interpreter: Ptr,
    operator: Ptr,
    _owner: Option<Arc<dyn Send + Sync>>,
}

impl Interpreter {
    pub fn load(api: Arc<Api>, model_path: &str, threads: i32) -> Result<Self, String> {
        Self::load_source(api, ModelSource::File(model_path.to_string()), threads)
    }

    pub fn load_source(api: Arc<Api>, source: ModelSource, threads: i32) -> Result<Self, String> {
        let (model, owner, model_path) = match source {
            ModelSource::File(path) => {
                let c = CString::new(path.as_str()).map_err(|e| e.to_string())?;
                (unsafe { (api.model_create_from_file)(c.as_ptr()) }, None, path)
            }
            ModelSource::Buffer { owner, ptr, len } => (
                unsafe { (api.model_create)(ptr as *const c_void, len) },
                Some(owner),
                format!("<{len}-byte buffer>"),
            ),
        };
        unsafe {
            if model.is_null() {
                return Err(format!("cannot load model {model_path}"));
            }
            let mut xnn = (api.xnnpack_options_default)();
            xnn.num_threads = threads;
            let delegate = (api.xnnpack_create)(&xnn);
            let options = (api.options_create)();
            (api.options_set_num_threads)(options, threads);
            // Kokoro's acoustic model needs the BiLSTM custom op; models
            // without it ignore the registration.
            let mut operator: Ptr = std::ptr::null_mut();
            if let Some(ops) = api.ops {
                let _ = OPERATOR_FNS.set(ops);
                operator = (ops.create)(
                    BUILTIN_CUSTOM,
                    c"litert_custom_op.bidirectional_lstm".as_ptr(),
                    1,
                    std::ptr::null_mut(),
                );
                if !operator.is_null() {
                    (ops.set_invoke)(operator, bilstm_invoke);
                    (ops.add)(options, operator);
                }
            }
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
                operator,
                _owner: owner,
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
            if let (Some(ops), false) = (self.api.ops, self.operator.is_null()) {
                (ops.delete)(self.operator);
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

    /// Bytes and `TfLiteType` of an input or output.
    pub fn info(&self, name: &str, input: bool) -> Result<(usize, u32), String> {
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
            Ok((
                (self.api.tensor_byte_size)(tensor),
                (self.api.tensor_type)(tensor) as u32,
            ))
        }
    }

    pub fn bytes_mut(&mut self, name: &str) -> Result<&mut [u8], String> {
        let (data, bytes) = self.tensor(name, true)?;
        Ok(unsafe { std::slice::from_raw_parts_mut(data, bytes) })
    }

    pub fn output_bytes(&self, name: &str) -> Result<&[u8], String> {
        let (data, bytes) = self.tensor(name, false)?;
        Ok(unsafe { std::slice::from_raw_parts(data as *const u8, bytes) })
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

    /// New shapes for inputs, then reallocation.
    pub fn resize(&mut self, shapes: &[(&str, &[i32])]) -> Result<(), String> {
        unsafe {
            for (name, dims) in shapes {
                let cname = CString::new(*name).map_err(|e| e.to_string())?;
                let status =
                    (self.api.runner_resize)(self.runner, cname.as_ptr(), dims.as_ptr(), dims.len() as i32);
                if status != 0 {
                    return Err(format!("'{}' cannot resize '{name}' to {dims:?}", self.key));
                }
            }
            if (self.api.runner_allocate)(self.runner) != 0 {
                return Err(format!("'{}' cannot reallocate after resizing", self.key));
            }
        }
        Ok(())
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
        Self::load_source(lib, ModelSource::File(model_path.to_string()), threads, signatures)
    }

    pub fn load_source(
        lib: Option<&str>,
        model: ModelSource,
        threads: i32,
        signatures: &[&str],
    ) -> Result<Self, String> {
        let api = Api::load(lib)?;
        let source = api.source.clone();
        let interpreter = Interpreter::load_source(api, model, threads)?;
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

// The interpreter is used by one thread at a time (the model's own thread).
unsafe impl Send for TfLiteBackend {}

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

    fn tensor_info(&self, signature: &str, name: &str, output: bool) -> Result<(usize, u32), String> {
        self.signatures
            .get(signature)
            .ok_or_else(|| format!("no signature '{signature}'"))?
            .info(name, !output)
    }

    fn write_bytes(&mut self, signature: &str, input: &str, data: &[u8]) -> Result<(), String> {
        let dst = self.signature(signature)?.bytes_mut(input)?;
        if data.len() > dst.len() {
            return Err(format!(
                "'{signature}' input '{input}' is {} bytes, got {}",
                dst.len(),
                data.len()
            ));
        }
        dst[..data.len()].copy_from_slice(data);
        dst[data.len()..].fill(0);
        Ok(())
    }

    fn resize_inputs(&mut self, signature: &str, shapes: &[(&str, &[i32])]) -> Result<(), String> {
        self.signature(signature)?.resize(shapes)
    }

    fn read_bytes(&mut self, signature: &str, output: &str, out: &mut Vec<u8>) -> Result<(), String> {
        let src = self.signature(signature)?.output_bytes(output)?;
        out.clear();
        out.extend_from_slice(src);
        Ok(())
    }
}
