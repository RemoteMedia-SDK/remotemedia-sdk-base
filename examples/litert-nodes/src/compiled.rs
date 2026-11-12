//! LiteRT's CompiledModel C API (LiteRT 2.x, `litert/c/litert_compiled_model.h`),
//! resolved at run time: the model compiled for the GPU, the NPU or the CPU,
//! with managed tensor buffers per signature input and output.
//!
//! On Android the GPU accelerator is `libLiteRtClGlAccelerator.so` (Maven
//! `com.google.ai.edge.litert:litert-gpu`) and the Google Tensor NPU runtime
//! is `libLiteRtDispatch_GoogleTensor.so` plus its compiler plugin
//! (`litert-npu-runtime-google-tensor`); both are found next to `libLiteRt.so`
//! through the loader path, or in `accelerator_dir`.

use std::collections::HashMap;
use std::ffi::{c_char, c_void, CStr, CString};
use std::ops::Range;
use std::sync::Arc;

use libloading::Library;

use crate::backend::Backend;

type H = *mut c_void;
type Status = i32;

/// `LiteRtRankedTensorType`: element type and `LiteRtLayout`, 72 bytes.
#[repr(C)]
#[derive(Clone, Copy)]
struct RankedTensorType([u32; 18]);

/// `LiteRtAny`: a type tag and an 8-byte union.
#[repr(C)]
struct Any {
    kind: i32,
    value: *const c_void,
}

/// `LiteRtEnvOption`.
#[repr(C)]
struct EnvOption {
    tag: i32,
    value: Any,
}

const ANY_STRING: i32 = 8;
const ENV_COMPILER_PLUGIN_DIR: i32 = 0;
const ENV_DISPATCH_DIR: i32 = 1;
const LOCK_READ: i32 = 0;
const LOCK_WRITE: i32 = 1;

pub const HW_CPU: i32 = 1;
pub const HW_GPU: i32 = 2;
pub const HW_NPU: i32 = 4;

struct Api {
    _lib: Library,
    source: String,
    status_string: unsafe extern "C" fn(Status) -> *const c_char,
    create_environment: unsafe extern "C" fn(i32, *const EnvOption, *mut H) -> Status,
    destroy_environment: unsafe extern "C" fn(H),
    num_accelerators: unsafe extern "C" fn(H, *mut usize) -> Status,
    get_accelerator: unsafe extern "C" fn(H, usize, *mut H) -> Status,
    accelerator_name: unsafe extern "C" fn(H, *mut *const c_char) -> Status,
    create_model_from_file: unsafe extern "C" fn(H, *const c_char, *mut H) -> Status,
    destroy_model: unsafe extern "C" fn(H),
    num_signatures: unsafe extern "C" fn(H, *mut usize) -> Status,
    model_signature: unsafe extern "C" fn(H, usize, *mut H) -> Status,
    signature_key: unsafe extern "C" fn(H, *mut *const c_char) -> Status,
    num_inputs: unsafe extern "C" fn(H, *mut usize) -> Status,
    input_name: unsafe extern "C" fn(H, usize, *mut *const c_char) -> Status,
    input_tensor: unsafe extern "C" fn(H, *const c_char, *mut H) -> Status,
    num_outputs: unsafe extern "C" fn(H, *mut usize) -> Status,
    output_name: unsafe extern "C" fn(H, usize, *mut *const c_char) -> Status,
    output_tensor: unsafe extern "C" fn(H, *const c_char, *mut H) -> Status,
    ranked_tensor_type: unsafe extern "C" fn(H, *mut RankedTensorType) -> Status,
    create_options: unsafe extern "C" fn(*mut H) -> Status,
    set_accelerators: unsafe extern "C" fn(H, i32) -> Status,
    destroy_options: unsafe extern "C" fn(H),
    create_compiled_model: unsafe extern "C" fn(H, H, H, *mut H) -> Status,
    destroy_compiled_model: unsafe extern "C" fn(H),
    input_requirements: unsafe extern "C" fn(H, usize, usize, *mut H) -> Status,
    output_requirements: unsafe extern "C" fn(H, usize, usize, *mut H) -> Status,
    create_buffer: unsafe extern "C" fn(H, *const RankedTensorType, H, *mut H) -> Status,
    destroy_buffer: unsafe extern "C" fn(H),
    buffer_packed_size: unsafe extern "C" fn(H, *mut usize) -> Status,
    lock_buffer: unsafe extern "C" fn(H, *mut *mut c_void, i32) -> Status,
    unlock_buffer: unsafe extern "C" fn(H) -> Status,
    run: unsafe extern "C" fn(H, usize, usize, *mut H, usize, *mut H) -> Status,
    fully_accelerated: unsafe extern "C" fn(H, *mut bool) -> Status,
    non_cpu_fully_accelerated: unsafe extern "C" fn(H, *mut bool) -> Status,
}

macro_rules! sym {
    ($lib:expr, $name:literal) => {
        *$lib
            .get(concat!($name, "\0").as_bytes())
            .map_err(|e| format!("{}: {e}", $name))?
    };
}

impl Api {
    fn load(path: Option<&str>) -> Result<Self, String> {
        let candidates: Vec<String> = match path
            .map(str::to_string)
            .or_else(|| std::env::var("REMOTEMEDIA_LITERT_LIB").ok())
        {
            Some(p) => vec![p],
            None => ["libLiteRt.so", "LiteRt.dll", "libLiteRt.dylib"]
                .iter()
                .chain(crate::litert::default_candidates())
                .map(|s| s.to_string())
                .collect(),
        };
        let mut errors = Vec::new();
        for name in &candidates {
            match unsafe { Library::new(name) } {
                Ok(lib) => match unsafe { Self::resolve(lib, name) } {
                    Ok(api) => return Ok(api),
                    Err(e) => errors.push(format!("{name}: missing {e}")),
                },
                Err(e) => errors.push(format!("{name}: {e}")),
            }
        }
        Err(format!(
            "no LiteRT library with the CompiledModel API: {}",
            errors.join("; ")
        ))
    }

    unsafe fn resolve(lib: Library, source: &str) -> Result<Self, String> {
        Ok(Self {
            status_string: sym!(lib, "LiteRtGetStatusString"),
            create_environment: sym!(lib, "LiteRtCreateEnvironment"),
            destroy_environment: sym!(lib, "LiteRtDestroyEnvironment"),
            num_accelerators: sym!(lib, "LiteRtGetNumAccelerators"),
            get_accelerator: sym!(lib, "LiteRtGetAccelerator"),
            accelerator_name: sym!(lib, "LiteRtGetAcceleratorName"),
            create_model_from_file: sym!(lib, "LiteRtCreateModelFromFile"),
            destroy_model: sym!(lib, "LiteRtDestroyModel"),
            num_signatures: sym!(lib, "LiteRtGetNumModelSignatures"),
            model_signature: sym!(lib, "LiteRtGetModelSignature"),
            signature_key: sym!(lib, "LiteRtGetSignatureKey"),
            num_inputs: sym!(lib, "LiteRtGetNumSignatureInputs"),
            input_name: sym!(lib, "LiteRtGetSignatureInputName"),
            input_tensor: sym!(lib, "LiteRtGetSignatureInputTensor"),
            num_outputs: sym!(lib, "LiteRtGetNumSignatureOutputs"),
            output_name: sym!(lib, "LiteRtGetSignatureOutputName"),
            output_tensor: sym!(lib, "LiteRtGetSignatureOutputTensor"),
            ranked_tensor_type: sym!(lib, "LiteRtGetRankedTensorType"),
            create_options: sym!(lib, "LiteRtCreateOptions"),
            set_accelerators: sym!(lib, "LiteRtSetOptionsHardwareAccelerators"),
            destroy_options: sym!(lib, "LiteRtDestroyOptions"),
            create_compiled_model: sym!(lib, "LiteRtCreateCompiledModel"),
            destroy_compiled_model: sym!(lib, "LiteRtDestroyCompiledModel"),
            input_requirements: sym!(lib, "LiteRtGetCompiledModelInputBufferRequirements"),
            output_requirements: sym!(lib, "LiteRtGetCompiledModelOutputBufferRequirements"),
            create_buffer: sym!(lib, "LiteRtCreateManagedTensorBufferFromRequirements"),
            destroy_buffer: sym!(lib, "LiteRtDestroyTensorBuffer"),
            buffer_packed_size: sym!(lib, "LiteRtGetTensorBufferPackedSize"),
            lock_buffer: sym!(lib, "LiteRtLockTensorBuffer"),
            unlock_buffer: sym!(lib, "LiteRtUnlockTensorBuffer"),
            run: sym!(lib, "LiteRtRunCompiledModel"),
            fully_accelerated: sym!(lib, "LiteRtCompiledModelIsFullyAccelerated"),
            non_cpu_fully_accelerated: sym!(lib, "LiteRtCompiledModelIsNonCpuFullyAccelerated"),
            source: source.to_string(),
            _lib: lib,
        })
    }

    fn check(&self, status: Status, what: &str) -> Result<(), String> {
        if status == 0 {
            return Ok(());
        }
        let text = unsafe {
            let p = (self.status_string)(status);
            if p.is_null() {
                String::new()
            } else {
                CStr::from_ptr(p).to_string_lossy().into_owned()
            }
        };
        Err(format!("{what}: status {status} {text}"))
    }
}

fn c_str(p: *const c_char) -> String {
    if p.is_null() {
        String::new()
    } else {
        unsafe { CStr::from_ptr(p).to_string_lossy().into_owned() }
    }
}

struct Tensor {
    name: String,
    buffer: H,
    bytes: usize,
}

struct SignatureBuffers {
    index: usize,
    inputs: Vec<Tensor>,
    outputs: Vec<Tensor>,
}

pub struct CompiledBackend {
    api: Arc<Api>,
    env: H,
    model: H,
    options: H,
    compiled: H,
    signatures: HashMap<String, SignatureBuffers>,
    description: String,
}

/// Which accelerators a name asks for (`gpu`, `npu`, `cpu`, or a
/// comma-separated list such as `npu,gpu`; the CPU is always the fallback).
pub fn accelerators(name: &str) -> Result<i32, String> {
    let mut set = HW_CPU;
    for part in name
        .split([',', '+'])
        .map(str::trim)
        .filter(|p| !p.is_empty())
    {
        set |= match part {
            "gpu" => HW_GPU,
            "npu" => HW_NPU,
            "cpu" => HW_CPU,
            other => return Err(format!("unknown accelerator '{other}'")),
        };
    }
    Ok(set)
}

impl CompiledBackend {
    /// `accelerator_dir`: where the NPU dispatch and compiler plugin
    /// libraries are (GPU accelerators are found through the loader path).
    pub fn load(
        lib: Option<&str>,
        model_path: &str,
        hw: i32,
        accelerator_dir: Option<&str>,
    ) -> Result<Self, String> {
        let api = Arc::new(Api::load(lib)?);
        unsafe {
            // Environment, with the NPU's library directories when given.
            let dir = accelerator_dir.map(|d| CString::new(d).unwrap());
            let options: Vec<EnvOption> = dir
                .iter()
                .flat_map(|d| {
                    [ENV_DISPATCH_DIR, ENV_COMPILER_PLUGIN_DIR].map(|tag| EnvOption {
                        tag,
                        value: Any {
                            kind: ANY_STRING,
                            value: d.as_ptr() as *const c_void,
                        },
                    })
                })
                .collect();
            let mut env: H = std::ptr::null_mut();
            api.check(
                (api.create_environment)(options.len() as i32, options.as_ptr(), &mut env),
                "LiteRtCreateEnvironment",
            )?;
            let mut this = Self {
                api: Arc::clone(&api),
                env,
                model: std::ptr::null_mut(),
                options: std::ptr::null_mut(),
                compiled: std::ptr::null_mut(),
                signatures: HashMap::new(),
                description: String::new(),
            };

            let mut registered = Vec::new();
            let mut n = 0usize;
            if (api.num_accelerators)(env, &mut n) == 0 {
                for i in 0..n {
                    let mut acc: H = std::ptr::null_mut();
                    let mut name: *const c_char = std::ptr::null();
                    if (api.get_accelerator)(env, i, &mut acc) == 0
                        && (api.accelerator_name)(acc, &mut name) == 0
                    {
                        registered.push(c_str(name));
                    }
                }
            }

            let path = CString::new(model_path).map_err(|e| e.to_string())?;
            api.check(
                (api.create_model_from_file)(env, path.as_ptr(), &mut this.model),
                "LiteRtCreateModelFromFile",
            )?;
            api.check(
                (api.create_options)(&mut this.options),
                "LiteRtCreateOptions",
            )?;
            api.check(
                (api.set_accelerators)(this.options, hw),
                "LiteRtSetOptionsHardwareAccelerators",
            )?;
            api.check(
                (api.create_compiled_model)(env, this.model, this.options, &mut this.compiled),
                "LiteRtCreateCompiledModel",
            )?;

            // Buffers for every signature's inputs and outputs.
            let mut count = 0usize;
            api.check(
                (api.num_signatures)(this.model, &mut count),
                "LiteRtGetNumModelSignatures",
            )?;
            for index in 0..count {
                let mut sig: H = std::ptr::null_mut();
                api.check(
                    (api.model_signature)(this.model, index, &mut sig),
                    "LiteRtGetModelSignature",
                )?;
                let mut key: *const c_char = std::ptr::null();
                api.check((api.signature_key)(sig, &mut key), "LiteRtGetSignatureKey")?;
                let mut buffers = SignatureBuffers {
                    index,
                    inputs: Vec::new(),
                    outputs: Vec::new(),
                };
                for output in [false, true] {
                    let mut n = 0usize;
                    if output {
                        api.check(
                            (api.num_outputs)(sig, &mut n),
                            "LiteRtGetNumSignatureOutputs",
                        )?;
                    } else {
                        api.check((api.num_inputs)(sig, &mut n), "LiteRtGetNumSignatureInputs")?;
                    }
                    for i in 0..n {
                        let mut name: *const c_char = std::ptr::null();
                        let mut tensor: H = std::ptr::null_mut();
                        let mut reqs: H = std::ptr::null_mut();
                        if output {
                            api.check((api.output_name)(sig, i, &mut name), "output name")?;
                            api.check(
                                (api.output_tensor)(sig, name, &mut tensor),
                                "output tensor",
                            )?;
                            api.check(
                                (api.output_requirements)(this.compiled, index, i, &mut reqs),
                                "output buffer requirements",
                            )?;
                        } else {
                            api.check((api.input_name)(sig, i, &mut name), "input name")?;
                            api.check((api.input_tensor)(sig, name, &mut tensor), "input tensor")?;
                            api.check(
                                (api.input_requirements)(this.compiled, index, i, &mut reqs),
                                "input buffer requirements",
                            )?;
                        }
                        let mut ty = RankedTensorType([0; 18]);
                        api.check(
                            (api.ranked_tensor_type)(tensor, &mut ty),
                            "ranked tensor type",
                        )?;
                        let mut buffer: H = std::ptr::null_mut();
                        api.check(
                            (api.create_buffer)(env, &ty, reqs, &mut buffer),
                            "create tensor buffer",
                        )?;
                        let mut bytes = 0usize;
                        api.check(
                            (api.buffer_packed_size)(buffer, &mut bytes),
                            "tensor buffer size",
                        )?;
                        let t = Tensor {
                            name: c_str(name),
                            buffer,
                            bytes,
                        };
                        if output {
                            buffers.outputs.push(t);
                        } else {
                            buffers.inputs.push(t);
                        }
                    }
                }
                this.signatures.insert(c_str(key), buffers);
            }

            let mut full = false;
            let mut non_cpu = false;
            let _ = (api.fully_accelerated)(this.compiled, &mut full);
            let _ = (api.non_cpu_fully_accelerated)(this.compiled, &mut non_cpu);
            let wanted: Vec<&str> = [(HW_NPU, "npu"), (HW_GPU, "gpu"), (HW_CPU, "cpu")]
                .iter()
                .filter(|(bit, _)| hw & bit != 0)
                .map(|(_, n)| *n)
                .collect();
            this.description = format!(
                "{} (CompiledModel, requested {}, registered [{}], fully accelerated {full}, non-CPU only {non_cpu})",
                api.source,
                wanted.join("+"),
                registered.join(", ")
            );
            Ok(this)
        }
    }

    fn tensor(&self, signature: &str, name: &str, output: bool) -> Result<&Tensor, String> {
        let sig = self
            .signatures
            .get(signature)
            .ok_or_else(|| format!("no signature '{signature}'"))?;
        let list = if output { &sig.outputs } else { &sig.inputs };
        list.iter().find(|t| t.name == name).ok_or_else(|| {
            format!(
                "'{signature}' has no {} '{name}'",
                if output { "output" } else { "input" }
            )
        })
    }

    fn write(
        &self,
        signature: &str,
        input: &str,
        src: *const u8,
        bytes: usize,
    ) -> Result<(), String> {
        let t = self.tensor(signature, input, false)?;
        if bytes != t.bytes {
            return Err(format!(
                "'{signature}' input '{input}' is {} bytes, got {bytes}",
                t.bytes
            ));
        }
        unsafe {
            let mut dst: *mut c_void = std::ptr::null_mut();
            self.api.check(
                (self.api.lock_buffer)(t.buffer, &mut dst, LOCK_WRITE),
                "lock input",
            )?;
            std::ptr::copy_nonoverlapping(src, dst as *mut u8, bytes);
            self.api
                .check((self.api.unlock_buffer)(t.buffer), "unlock input")
        }
    }
}

impl Backend for CompiledBackend {
    fn input_len(&self, signature: &str, input: &str) -> Result<usize, String> {
        Ok(self.tensor(signature, input, false)?.bytes / 4)
    }

    fn output_len(&self, signature: &str, output: &str) -> Result<usize, String> {
        Ok(self.tensor(signature, output, true)?.bytes / 4)
    }

    fn write_f32(&mut self, signature: &str, input: &str, data: &[f32]) -> Result<(), String> {
        self.write(signature, input, data.as_ptr() as *const u8, data.len() * 4)
    }

    fn write_i32(&mut self, signature: &str, input: &str, data: &[i32]) -> Result<(), String> {
        self.write(signature, input, data.as_ptr() as *const u8, data.len() * 4)
    }

    fn invoke(&mut self, signature: &str) -> Result<(), String> {
        let sig = self
            .signatures
            .get(signature)
            .ok_or_else(|| format!("no signature '{signature}'"))?;
        let mut inputs: Vec<H> = sig.inputs.iter().map(|t| t.buffer).collect();
        let mut outputs: Vec<H> = sig.outputs.iter().map(|t| t.buffer).collect();
        let status = unsafe {
            (self.api.run)(
                self.compiled,
                sig.index,
                inputs.len(),
                inputs.as_mut_ptr(),
                outputs.len(),
                outputs.as_mut_ptr(),
            )
        };
        self.api.check(status, &format!("run '{signature}'"))
    }

    fn read_f32(
        &mut self,
        signature: &str,
        output: &str,
        range: Range<usize>,
        out: &mut Vec<f32>,
    ) -> Result<(), String> {
        let t = self.tensor(signature, output, true)?;
        if range.end * 4 > t.bytes {
            return Err(format!(
                "'{signature}' output '{output}' range out of bounds"
            ));
        }
        unsafe {
            let mut src: *mut c_void = std::ptr::null_mut();
            self.api.check(
                (self.api.lock_buffer)(t.buffer, &mut src, LOCK_READ),
                "lock output",
            )?;
            let all = std::slice::from_raw_parts(src as *const f32, t.bytes / 4);
            out.clear();
            out.extend_from_slice(&all[range]);
            self.api
                .check((self.api.unlock_buffer)(t.buffer), "unlock output")
        }
    }

    fn describe(&self) -> String {
        self.description.clone()
    }
}

impl Drop for CompiledBackend {
    fn drop(&mut self) {
        unsafe {
            for sig in self.signatures.values() {
                for t in sig.inputs.iter().chain(&sig.outputs) {
                    (self.api.destroy_buffer)(t.buffer);
                }
            }
            if !self.compiled.is_null() {
                (self.api.destroy_compiled_model)(self.compiled);
            }
            if !self.options.is_null() {
                (self.api.destroy_options)(self.options);
            }
            if !self.model.is_null() {
                (self.api.destroy_model)(self.model);
            }
            if !self.env.is_null() {
                (self.api.destroy_environment)(self.env);
            }
        }
    }
}
