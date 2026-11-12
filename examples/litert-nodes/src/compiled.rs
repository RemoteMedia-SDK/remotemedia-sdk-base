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
    create_model_from_buffer: unsafe extern "C" fn(H, *const c_void, usize, *mut H) -> Status,
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
    add_custom_op: Option<
        unsafe extern "C" fn(H, *const c_char, i32, *const CustomOpKernel, *mut c_void) -> Status,
    >,
    buffer_tensor_type: unsafe extern "C" fn(H, *mut RankedTensorType) -> Status,
    create_opaque_options:
        Option<unsafe extern "C" fn(*const c_char, *mut c_void, unsafe extern "C" fn(*mut c_void), *mut H) -> Status>,
    add_opaque_options: Option<unsafe extern "C" fn(H, H) -> Status>,
}

unsafe extern "C" fn free_c_string(payload: *mut c_void) {
    drop(CString::from_raw(payload as *mut c_char));
}

/// `LiteRtLayout`: rank and has_strides bit-fields, dimensions, strides.
#[repr(C)]
#[derive(Clone, Copy)]
struct Layout {
    bits: u32,
    dims: [i32; 8],
    strides: [u32; 8],
}

/// `LiteRtCustomOpKernel`.
#[repr(C)]
pub struct CustomOpKernel {
    init: unsafe extern "C" fn(*mut c_void, *const c_void, usize) -> Status,
    output_layouts: unsafe extern "C" fn(*mut c_void, usize, *const Layout, usize, *mut Layout) -> Status,
    run: unsafe extern "C" fn(*mut c_void, usize, *const H, usize, *mut H) -> Status,
    destroy: unsafe extern "C" fn(*mut c_void) -> Status,
}

/// What the BiLSTM kernel's callbacks need from the library.
#[derive(Clone, Copy)]
struct KernelFns {
    tensor_type: unsafe extern "C" fn(H, *mut RankedTensorType) -> Status,
    packed_size: unsafe extern "C" fn(H, *mut usize) -> Status,
    lock: unsafe extern "C" fn(H, *mut *mut c_void, i32) -> Status,
    unlock: unsafe extern "C" fn(H) -> Status,
}

static KERNEL_FNS: std::sync::OnceLock<KernelFns> = std::sync::OnceLock::new();
const STATUS_ERROR: Status = 1;

unsafe extern "C" fn bilstm_init(_: *mut c_void, _: *const c_void, _: usize) -> Status {
    0
}

unsafe extern "C" fn bilstm_destroy(_: *mut c_void) -> Status {
    0
}

unsafe extern "C" fn bilstm_output_layouts(
    _: *mut c_void,
    num_inputs: usize,
    inputs: *const Layout,
    num_outputs: usize,
    outputs: *mut Layout,
) -> Status {
    if num_inputs != crate::bilstm::INPUTS || num_outputs != 1 {
        return STATUS_ERROR;
    }
    let inputs = std::slice::from_raw_parts(inputs, num_inputs);
    let (x, w_hh) = (inputs[0], inputs[4]);
    if x.bits & 0x7f != 3 || w_hh.bits & 0x7f != 2 {
        return STATUS_ERROR;
    }
    let mut out = Layout {
        bits: 3,
        dims: [0; 8],
        strides: [0; 8],
    };
    out.dims[0] = x.dims[0];
    out.dims[1] = x.dims[1];
    out.dims[2] = 2 * w_hh.dims[1];
    *outputs = out;
    0
}

unsafe extern "C" fn bilstm_run(
    _: *mut c_void,
    num_inputs: usize,
    inputs: *const H,
    num_outputs: usize,
    outputs: *mut H,
) -> Status {
    let Some(fns) = KERNEL_FNS.get().copied() else {
        return STATUS_ERROR;
    };
    if num_inputs != crate::bilstm::INPUTS || num_outputs != 1 {
        return STATUS_ERROR;
    }
    let inputs = std::slice::from_raw_parts(inputs, num_inputs);
    let output = *outputs;
    let dims = |buffer: H| -> Option<Vec<usize>> {
        let mut ty = RankedTensorType([0; 18]);
        if (fns.tensor_type)(buffer, &mut ty) != 0 {
            return None;
        }
        let rank = (ty.0[1] & 0x7f) as usize;
        Some((0..rank).map(|i| ty.0[2 + i] as usize).collect())
    };
    let (Some(x), Some(w_hh)) = (dims(inputs[0]), dims(inputs[4])) else {
        return STATUS_ERROR;
    };
    if x.len() != 3 || w_hh.len() != 2 {
        return STATUS_ERROR;
    }
    let mut locked: Vec<H> = Vec::new();
    let mut slices: Vec<&[u8]> = Vec::new();
    let mut failed = false;
    for &buffer in inputs {
        let mut size = 0usize;
        let mut ptr: *mut c_void = std::ptr::null_mut();
        if (fns.packed_size)(buffer, &mut size) != 0 || (fns.lock)(buffer, &mut ptr, LOCK_READ) != 0 {
            failed = true;
            break;
        }
        locked.push(buffer);
        slices.push(std::slice::from_raw_parts(ptr as *const u8, size));
    }
    let mut status = STATUS_ERROR;
    if !failed {
        let mut size = 0usize;
        let mut ptr: *mut c_void = std::ptr::null_mut();
        if (fns.packed_size)(output, &mut size) == 0 && (fns.lock)(output, &mut ptr, LOCK_WRITE) == 0 {
            let out = std::slice::from_raw_parts_mut(ptr as *mut u8, size);
            let ins: [&[u8]; crate::bilstm::INPUTS] = std::array::from_fn(|i| slices[i]);
            match crate::bilstm::run(ins, out, [x[0], x[1], x[2]], w_hh[1]) {
                Ok(()) => status = 0,
                Err(e) => eprintln!("[litert] {e}"),
            }
            (fns.unlock)(output);
        }
    }
    for buffer in locked {
        (fns.unlock)(buffer);
    }
    status
}

static BILSTM_KERNEL: CustomOpKernel = CustomOpKernel {
    init: bilstm_init,
    output_layouts: bilstm_output_layouts,
    run: bilstm_run,
    destroy: bilstm_destroy,
};

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
            create_model_from_buffer: sym!(lib, "LiteRtCreateModelFromBuffer"),
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
            add_custom_op: lib
                .get(b"LiteRtAddCustomOpKernelOption\0")
                .ok()
                .map(|s| *s),
            buffer_tensor_type: sym!(lib, "LiteRtGetTensorBufferTensorType"),
            create_opaque_options: lib.get(b"LiteRtCreateOpaqueOptions\0").ok().map(|s| *s),
            add_opaque_options: lib.get(b"LiteRtAddOpaqueOptions\0").ok().map(|s| *s),
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
    /// `LiteRtElementType` (1 f32, 2 i32, 4 i64, ...).
    element: u32,
}

/// Where a model's bytes come from. A buffer must outlive the model, so the
/// backend keeps its owner.
pub enum ModelSource {
    File(String),
    Buffer {
        owner: Arc<dyn Send + Sync>,
        ptr: *const u8,
        len: usize,
    },
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
    _owner: Option<Arc<dyn Send + Sync>>,
}

// The handles are used by one thread at a time (the model's own thread).
unsafe impl Send for CompiledBackend {}

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
        Self::load_source(lib, ModelSource::File(model_path.to_string()), hw, accelerator_dir, None)
    }

    /// `gpu_options`: the GPU accelerator's options as TOML (the payload of
    /// LiteRT's `gpu_options` opaque options), e.g. `precision = 2` (FP32)
    /// or `buffer_storage_type = 1` (buffers instead of textures).
    pub fn load_source(
        lib: Option<&str>,
        source: ModelSource,
        hw: i32,
        accelerator_dir: Option<&str>,
        gpu_options: Option<&str>,
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
                _owner: None,
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

            match source {
                ModelSource::File(model_path) => {
                    let path = CString::new(model_path).map_err(|e| e.to_string())?;
                    api.check(
                        (api.create_model_from_file)(env, path.as_ptr(), &mut this.model),
                        "LiteRtCreateModelFromFile",
                    )?;
                }
                ModelSource::Buffer { owner, ptr, len } => {
                    api.check(
                        (api.create_model_from_buffer)(
                            env,
                            ptr as *const c_void,
                            len,
                            &mut this.model,
                        ),
                        "LiteRtCreateModelFromBuffer",
                    )?;
                    this._owner = Some(owner);
                }
            }
            api.check(
                (api.create_options)(&mut this.options),
                "LiteRtCreateOptions",
            )?;
            api.check(
                (api.set_accelerators)(this.options, hw),
                "LiteRtSetOptionsHardwareAccelerators",
            )?;
            if let (Some(toml), true) = (gpu_options.filter(|t| !t.trim().is_empty()), hw & HW_GPU != 0) {
                let (Some(create), Some(add)) = (api.create_opaque_options, api.add_opaque_options) else {
                    return Err("this LiteRT library cannot take GPU options".into());
                };
                let payload = CString::new(toml.replace("\\n", "\n")).map_err(|e| e.to_string())?.into_raw();
                let mut opaque: H = std::ptr::null_mut();
                api.check(
                    create(c"gpu_options".as_ptr(), payload as *mut c_void, free_c_string, &mut opaque),
                    "LiteRtCreateOpaqueOptions",
                )?;
                api.check(add(this.options, opaque), "LiteRtAddOpaqueOptions")?;
            }
            // Kokoro's acoustic model needs the BiLSTM custom op; models
            // without it ignore the registration.
            if let Some(add) = api.add_custom_op {
                let _ = KERNEL_FNS.set(KernelFns {
                    tensor_type: api.buffer_tensor_type,
                    packed_size: api.buffer_packed_size,
                    lock: api.lock_buffer,
                    unlock: api.unlock_buffer,
                });
                api.check(
                    add(
                        this.options,
                        c"litert_custom_op.bidirectional_lstm".as_ptr(),
                        1,
                        &BILSTM_KERNEL,
                        std::ptr::null_mut(),
                    ),
                    "LiteRtAddCustomOpKernelOption",
                )?;
            }
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
                            element: ty.0[0],
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

    /// Bytes of an input (`output` false) or output, and its element type.
    pub fn tensor_info(
        &self,
        signature: &str,
        name: &str,
        output: bool,
    ) -> Result<(usize, u32), String> {
        let t = self.tensor(signature, name, output)?;
        Ok((t.bytes, t.element))
    }

    /// Signatures and their tensors: `(signature, inputs, outputs)`, each
    /// tensor as `(name, bytes, element type)`.
    pub fn layout(&self) -> Vec<(String, Vec<(String, usize, u32)>, Vec<(String, usize, u32)>)> {
        let list = |ts: &Vec<Tensor>| {
            ts.iter()
                .map(|t| (t.name.clone(), t.bytes, t.element))
                .collect::<Vec<_>>()
        };
        let mut out: Vec<_> = self
            .signatures
            .iter()
            .map(|(key, sig)| (key.clone(), list(&sig.inputs), list(&sig.outputs)))
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    /// The only signature's key (a model exported without one gets a default).
    pub fn sole_signature(&self) -> Result<String, String> {
        let mut keys = self.signatures.keys();
        match (keys.next(), keys.next()) {
            (Some(k), None) => Ok(k.clone()),
            _ => Err(format!(
                "expected one signature, found {}",
                self.signatures.len()
            )),
        }
    }

    /// Writes `data` at the start of an input and zeroes the rest.
    pub fn write_bytes(&self, signature: &str, input: &str, data: &[u8]) -> Result<(), String> {
        let t = self.tensor(signature, input, false)?;
        if data.len() > t.bytes {
            return Err(format!(
                "'{signature}' input '{input}' is {} bytes, got {}",
                t.bytes,
                data.len()
            ));
        }
        unsafe {
            let mut dst: *mut c_void = std::ptr::null_mut();
            self.api.check(
                (self.api.lock_buffer)(t.buffer, &mut dst, LOCK_WRITE),
                "lock input",
            )?;
            let all = std::slice::from_raw_parts_mut(dst as *mut u8, t.bytes);
            all[..data.len()].copy_from_slice(data);
            all[data.len()..].fill(0);
            self.api
                .check((self.api.unlock_buffer)(t.buffer), "unlock input")
        }
    }

    /// A whole output's bytes, after `invoke`.
    pub fn read_bytes(&self, signature: &str, output: &str, out: &mut Vec<u8>) -> Result<(), String> {
        let t = self.tensor(signature, output, true)?;
        unsafe {
            let mut src: *mut c_void = std::ptr::null_mut();
            self.api.check(
                (self.api.lock_buffer)(t.buffer, &mut src, LOCK_READ),
                "lock output",
            )?;
            out.clear();
            out.extend_from_slice(std::slice::from_raw_parts(src as *const u8, t.bytes));
            self.api
                .check((self.api.unlock_buffer)(t.buffer), "unlock output")
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

    fn tensor_info(&self, signature: &str, name: &str, output: bool) -> Result<(usize, u32), String> {
        CompiledBackend::tensor_info(self, signature, name, output)
    }

    fn write_bytes(&mut self, signature: &str, input: &str, data: &[u8]) -> Result<(), String> {
        CompiledBackend::write_bytes(self, signature, input, data)
    }

    fn read_bytes(&mut self, signature: &str, output: &str, out: &mut Vec<u8>) -> Result<(), String> {
        CompiledBackend::read_bytes(self, signature, output, out)
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
