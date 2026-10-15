# Design: Android RemoteMedia Scaling Entry/Runner via Microdroid VM

## Architecture Overview

Instead of executing Python nodes in-process under the Android application JVM/JNI, we decouple orchestrating (entry) and executing (runner) via Android Virtualization Framework (AVF).

```
┌────────────────────────────────────────────────────────────────────────┐
│                        Android Host App (AOSP)                         │
│                                                                        │
│  ┌───────────────────────┐            ┌─────────────────────────────┐  │
│  │   PipelineManager     │───────────►│    MicrodroidController     │  │
│  │  (Kotlin Orchestrator)│            │      (VM Lifecycle)         │  │
│  └───────────┬───────────┘            └──────────────┬──────────────┘  │
│              │ JNI call                              │ starts pVM      │
│              ▼                                       ▼                 │
│  ┌───────────────────────┐             ┌───────────────────────────┐   │
│  │ remotemedia-android   │             │   Microdroid pVM          │   │
│  │ JNI (Rust Entry Crate)│             │  ┌─────────────────────┐  │   │
│  │                       │             │  │ remotemedia-        │  │   │
│  │ ┌───────────────────┐ │    vsock    │  │ microdroid-runner   │  │   │
│  │ │Tonic gRPC Client  │◄├─────────────┼─►│ (Rust gRPC Server)  │  │   │
│  │ │(RunnerControl)    │ │ (Port 5555) │  │                     │  │   │
│  │ └───────────────────┘ │             │  │  ┌───────────────┐  │  │   │
│  └───────────────────────┘             │  │  │ PyO3 / Python │  │  │   │
│                                        │  │  │ Node Runner   │  │  │   │
│                                        │  │  └───────────────┘  │  │   │
│                                        │  └─────────────────────┘  │   │
│                                        └───────────────────────────┘   │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 1. Lifecycle and Connection Sequence

1. **Initialization**: The user starts a pipeline. `PipelineManager` triggers `MicrodroidController.startVm()`.
2. **Boot**: `MicrodroidController` uses `VirtualMachineManager` to locate/instantiate the VM config:
   - Config payload binary: `assets/microdroid/remotemedia-microdroid-runner`
   - Memory allocation: 512 MiB (or user-configured)
   - CPU: 1 or 2 vCPUs
3. **VM Init & Listen**: The pVM starts, runs the guest launcher which binds `AF_VSOCK` at port `5555`.
4. **Vsock Connect**: `MicrodroidController` receives `onPayloadReady()` from the `VirtualMachineCallback`. It invokes `vm.connectVsock(5555)` which returns an Android `ParcelFileDescriptor` (PFD) representing the raw vsock stream socket.
5. **JNI Handoff**: The host app passes the PFD to JNI:
   ```kotlin
   val pfd = vm.connectVsock(5555)
   NativeInterface.nativeSetVsockFd(pfd.detachFd())
   ```
6. **Rust Core Connection**: The Rust JNI layer takes ownership of the raw integer file descriptor:
   - Converts `RawFd` to `std::os::unix::net::UnixStream` (or a custom wrapper).
   - Convers the stream into an asynchronous `tokio::net::UnixStream` via `UnixStream::from_std()`.
   - Bootstraps a Tonic gRPC `Channel` using the socket stream.
7. **Control Handshake**: The host JNI triggers `RegisterSession` over the gRPC channel to deploy the pipeline manifest to the runner in the pVM.

---

## 2. Bidirectional Data Plane (Media Streaming Loop)

Since the pVM is isolated and has restricted access to host hardware (microphone, speakers, camera) and system networks, all media inputs and outputs are piped over the vsock connection:

1. **Participant Registration**: The host JNI registers as a participant with type `PARTICIPANT_TYPE_FFI` or `PARTICIPANT_TYPE_GRPC`.
2. **Audio/Video Ingestion**:
   - The host application captures PCM audio frames from the Android microphone (`AudioRecorder`).
   - The Kotlin code passes the frames to JNI.
   - The Rust JNI client packages the raw bytes into a `ParticipantInputMessage` and streams it over the `SendParticipantData` gRPC bidirectional stream.
3. **Inference & Execution**:
   - The runner inside the Microdroid VM receives the media packets from the gRPC stream.
   - It routes the packets through the pipeline nodes (e.g. Whisper node for transcription, LLM node for processing).
   - If an output is generated (e.g. speech via Kokoro TTS node), the runner packages the output audio frames into `ParticipantOutputMessage` envelopes.
4. **Playback & Rendering**:
   - The Rust JNI client receives the output packets from the gRPC stream.
   - It notifies the Kotlin caller (via callback or polling queue).
   - The Kotlin code writes the audio bytes to `AudioPlayer` / `AudioTrack` for playback.

This loop ensures minimal latency by keeping a single, multiplexed, persistent gRPC stream over vsock.

---

## 3. Host JNI API & Rust Vsock Connector

The JNI module `remotemedia_android_inprocess` gets expanded with vsock endpoint routing:

```rust
// In JNI bindings
#[no_mangle]
pub extern "system" fn Java_com_remotemedia_android_NativeInterface_nativeSetVsockFd(
    _env: JNIEnv,
    _class: JClass,
    fd: jint,
) {
    let raw_fd = fd as RawFd;
    // Store in global or executor registry
    executor::set_vsock_stream_fd(raw_fd);
}
```

The Rust-side connector uses Tonic's custom connector interface:

```rust
use std::os::unix::net::UnixStream as StdUnixStream;
use tokio::net::UnixStream;
use tower::service_fn;

pub async fn build_vsock_channel(fd: RawFd) -> Result<tonic::transport::Channel, tonic::transport::Error> {
    // Safety: raw_fd is owned by us after detaching from ParcelFileDescriptor
    let std_stream = unsafe { StdUnixStream::from_raw_fd(fd) };
    std_stream.set_nonblocking(true).unwrap();

    let connector = service_fn(move |_| {
        let std_clone = std_stream.try_clone().unwrap();
        async move {
            UnixStream::from_std(std_clone)
        }
    });

    tonic::transport::Endpoint::from_static("http://localhost")
        .connect_with_connector(connector)
        .await
}
```

---

## 4. Guest Payload C++ or Rust Runner

Inside the Microdroid pVM:
- We run a native Rust executable `remotemedia-microdroid-runner` compiled for the `aarch64-linux-android` target.
- This executable statically links the `@remotemedia-scaling` `runner` crate.
- It binds to `AF_VSOCK` at `VMADDR_CID_ANY` on port `5555` using standard Linux sockets.
- When the host connects, it initiates a Tonic gRPC server serving the `RunnerControl` service.
- The Python nodes (Whisper, Kokoro, etc.) run in-process or as local subprocesses inside the VM using a pre-installed/mounted Python environment. OOM limits are managed locally by the guest Linux kernel.

---

## 5. Managed Python Environment inside the Guest pVM

To execute Python nodes successfully under `PYTHON_ENV_MODE=managed` within the guest pVM, the runner uses the following directory layout, environment configuration, and execution boundaries:

### Directory & Path Mappings
1. **Mounted Host APK (`/mnt/apk`)**: The virtualization service mounts the host APK read-only.
   - Python Shared Library: `/mnt/apk/lib/arm64-v8a/libpython3.11.so`
   - Python Standard Library Zip: `/mnt/apk/assets/python-runtime/python311.zip`
   - Bundled Native `uv` Executable: `/mnt/apk/assets/bin/uv` (cross-compiled for `aarch64-linux-android`)
2. **Writable VM Storage (`/mnt/encryptedstore`)**: The Microdroid pVM has a persistent, encrypted writable partition.
   - Provisioned Virtual Environments: `/mnt/encryptedstore/remotemedia-venv/`
   - Downloaded Plugin Cache: `/mnt/encryptedstore/plugin-cache/`

### Environment Variable Bindings
When the guest launcher starts the runner process inside the VM, it sets the following env vars to configure the `remotemedia-py-env` manager:

```bash
# Force managed virtual environment provisioning
export PYTHON_ENV_MODE=managed

# Set Python execution and home paths inside the VM
export PYTHONHOME=/mnt/apk/assets/python-runtime
export PYTHONPATH=/mnt/apk/assets/python-runtime/python311.zip

# Map library loader paths for PyO3/C-extensions
export LD_LIBRARY_PATH=/mnt/apk/lib/arm64-v8a:$LD_LIBRARY_PATH

# Configure uv path override
export UV_BINARY_PATH=/mnt/apk/assets/bin/uv

# Set cache and build directory inside the writable store
export REMOTEMEDIA_CACHE_DIR=/mnt/encryptedstore/plugin-cache
```

### Dependency Provisioning Flow
1. **Manifest Load**: The JNI orchestrator sends a `RegisterSession` request containing the node definitions and plugin sources.
2. **Venv Creation**: The guest `PythonEnvManager` detects `PYTHON_ENV_MODE=managed`. It calls `uv venv /mnt/encryptedstore/remotemedia-venv` using `/mnt/apk/lib/arm64-v8a/libpython3.11.so` as the parent interpreter context.
3. **Dependency Lock**: `uv` resolves and installs required Python packages (such as numpy or torch) into the writable venv, utilizing the network interface of the VM (enabled via `--network-supported` / `setNetworkSupported(true)`).
4. **Node Launch**: The runner executes the Python nodes under the newly provisioned venv, linking the interpreter dynamically to load and process data.


