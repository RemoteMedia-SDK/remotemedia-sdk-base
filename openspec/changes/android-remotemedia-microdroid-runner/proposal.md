# Proposal: Android RemoteMedia Scaling Entry/Runner via Microdroid VM

## Why

The current Android integration runs RemoteMedia pipeline nodes (including heavy model-based Python nodes like Whisper, Kokoro, and LLaMA) inside the host application process via JNI (`libremotemedia_android_inprocess.so`) and a local python-for-android runtime. On resource-constrained mobile platforms like the Pixel 6 Pro, this approach introduces several critical limitations:
1. **OOM Ceilings**: Heavy deep-learning inference models run against the Android app process's memory space, triggering early OS process terminations.
2. **ABI and Linker Conflicts**: Bundling numpy, PyO3, and multiple native C/C++ dependencies in a single app process creates symbol collisions and ABI constraints.
3. **App Startup Lag**: Extracting python-for-android wheels and initializing the Python interpreter on the main process causes long application startup times and blocking GIL states.
4. **Lack of Isolation**: A crash in a single Python node (e.g. a segmentation fault in an ONNX runtime extension) kills the entire host Android application.

To solve these issues, this change brings the `@remotemedia-scaling` entry/runner orchestration architecture into the Android target. Instead of running nodes in-process, we will execute them inside an isolated protected Virtual Machine (pVM) powered by Android's Virtualization Framework (AVF) and Microdroid.

- The Android host app runs the **scaling entry (orchestration)** logic.
- The Microdroid pVM runs the **scaling runner (execution)** logic.
- They communicate directly over a low-latency, bidirectional **vsock** channel using the existing gRPC `RunnerControl` protocol.

This completely isolates the Python interpreter and nodes inside the pVM, freeing the host Android app from large binary packaging, GIL management, and OOM risks.

---

## What Changes

1. **Host-Side VM Controller in `kotlin-android`**:
   - Create `MicrodroidController.kt` to manage pVM creation, configuration, boot, callbacks, and lifecycle hooks via Android's `VirtualMachineManager`.
   - Add JNI bridge methods in `NativeInterface.kt` to pass the vsock `ParcelFileDescriptor` directly to the Rust core.
   - Update `PipelineManager.kt` to allow choosing between `InProcess` and `Microdroid` execution modes.

2. **Guest VM Payload Binary**:
   - Create a new guest payload crate/binary target `remotemedia-microdroid-runner`.
   - This binary runs as the init/payload command inside the Microdroid pVM.
   - It listens on a fixed vsock port (e.g., `5555`), accepts a connection, and starts the gRPC `RunnerControl` server (`remotemedia-scaling-runner`).
   - The runner executes pipeline sessions and nodes (including Python nodes via a standard local Python/PyO3 runtime running inside the VM).

3. **Vsock gRPC Transport Bridge (Rust JNI)**:
   - Implement a custom Tonic connector in the Rust JNI layer (`remotemedia_android_inprocess` / `remotemedia_android_entry`) that accepts a JNI-passed raw file descriptor (wrapped from the host's `ParcelFileDescriptor`) and runs a gRPC `Channel` over it.
   - Connect the host-side `EntryOrchestrator` to this channel, routing all control signals (`RegisterSession`, `NegotiateEdge`) and media streaming (`SendParticipantData`) over vsock.

4. **APK Packaging & Build Pipeline**:
   - Configure Gradle/CMake to compile the guest binary for `aarch64-linux-android` (Android/Bionic).
   - Package the guest binary and VM config JSON (`assets/microdroid/runner_vm_config.json`) inside the host APK.

---

## Capabilities

### New Capabilities
- `kotlin-android-microdroid-host`: Host-side Kotlin lifecycle management for Microdroid pVMs and vsock socket handoff.
- `vsock-grpc-transport`: Rust-side gRPC transport mapping a JNI raw file descriptor to a Tonic channel client/server.
- `remotemedia-microdroid-runner-payload`: Guest-side native binary capable of running nodes within the isolated pVM sandbox.

### Modified Capabilities
- `android-pipeline-management`: `PipelineManager` supports choosing the execution target (`InProcess` or `Microdroid`) programmatically or via configuration.
- `android-native-bundling`: Packaging pipeline extended to bundle the Microdroid config, guest launcher, and dependencies into APK assets.

---

## Impact

- **Build / Packaging**:
  - APK size will change. Python libraries, PyO3 bindings, and ONNX models are moved into a virtual disk image or asset group dedicated to the VM payload.
  - The guest binary `remotemedia-microdroid-runner` is compiled for `aarch64-linux-android` and packaged under `assets/microdroid/`.
- **Runtime**:
  - RAM isolation: Python and model inference execute inside a dedicated pVM memory allocation (e.g., 256MB to 1GB) completely isolated from the host app.
  - Target system: Requires Pixel 6 Pro or newer running a kernel with KVM enabled, and the `com.android.virt` APEX. Older or incompatible devices will automatically fail early or fall back to in-process execution depending on settings.
- **Developer Experience**:
  - The host app no longer handles Python environments or native ABI mismatches. Python extensions are verified and loaded only inside the VM.

---

## Open Questions

1. **Virtual Disk/Asset Mounting**: Should the VM payload access models and Python wheels directly from a read-only virtio-block device mounted by AVF, or should the host stream them over vsock at startup?
2. **pVM Networking**: Some nodes (like remote signaling or telemetry) require internet access. Since protected VMs have restricted network configurations, should all external networking be proxied by the host app via the gRPC control stream?

