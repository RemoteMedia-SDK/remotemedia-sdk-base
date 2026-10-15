# Tasks: Android RemoteMedia Scaling Entry/Runner via Microdroid VM

This document lists the implementation and verification tasks required to bring the `@remotemedia-scaling` entry/runner logic into a Microdroid pVM.

## Task Decomposition

### Phase 1: Environment & Lifecycle Setup
- [x] **T1.1: Verify Device Environment**
  - Run validation commands on target Pixel 6 Pro:
    ```bash
    adb shell getprop ro.product.device  # should be raven / aosp_raven
    adb shell ls /apex/com.android.virt  # verify virtualization APEX exists
    ```
- [x] **T1.2: Add Host-Side Kotlin controller**
  - Implement `MicrodroidController.kt` in `remotemedia-sdk-base/kotlin-android` using `VirtualMachineManager` / `VirtualMachineConfig` / `VirtualMachine`.
  - Handle callback events (`onPayloadStarted`, `onPayloadReady`, `onStopped`, `onError`).
  - Add feature gating; check for AVF support at runtime and throw `UnsupportedOperationException` on unsupported devices.

### Phase 2: Host ↔ Guest Vsock Bridge
- [x] **T2.1: Add JNI File Descriptor Handoff**
  - Implement `NativeInterface.nativeSetVsockFd(fd: Int)` JNI endpoint.
  - Call `vm.connectVsock(5555)` in `MicrodroidController` and hand over the detached FD.
- [x] **T2.2: Implement Rust Vsock Custom Connector**
  - In Rust JNI core, convert the raw file descriptor into a `UnixStream`.
  - Set the stream to non-blocking and wrap it with `tokio::net::UnixStream`.
  - Implement a Tonic custom connector function that establishes a gRPC `Channel` over the vsock connection.

### Phase 3: Guest VM Runner Payload
- [x] **T3.1: Create Guest Rust Runner Crate**
  - Create the `remotemedia-microdroid-runner` binary target under the workspace.
  - Implement a TCP/Vsock socket listener on port `5555` inside the guest main.
  - Bind to the `RunnerControl` gRPC service using the `@remotemedia-scaling-runner` server implementation.
- [x] **T3.2: Integrate Guest Python Runtime**
  - Ensure the guest runner initializes Python via PyO3 inside the pVM Bionic environment.
  - Setup model directory mounting or streaming pathways for node weights.
- [x] **T3.3: Configure Guest uv/Venv Backend**
  - Integrate `remotemedia-py-env` in the guest runner.
  - Set up path resolution for `/mnt/apk/assets/bin/uv` and configure environment variables (`PYTHON_ENV_MODE=managed`, `PYTHONHOME`, `LD_LIBRARY_PATH`).

### Phase 4: Build & Packaging
- [x] **T4.1: Cross-Compilation Rules**
  - Configure `CMakeLists.txt` and `Cargo.toml` to cross-compile `remotemedia-microdroid-runner` for target `aarch64-linux-android`.
- [x] **T4.2: APK Asset Bundling**
  - Configure Gradle script to copy the compiled guest binary and `runner_vm_config.json` into the host APK under `assets/microdroid/`.
  - Include the prebuilt `uv` binary and `libpython3.11.so` / stdlib zip in APK assets so they are visible under `/mnt/apk/` in the VM.

### Phase 5: PipelineManager Integration
- [x] **T5.1: Pipeline Routing Integration**
  - Update `PipelineManager.kt` to allow choosing `ExecutionMode.MICRODROID`.
  - When in Microdroid mode, skip in-process initialization and delegate to `MicrodroidController` lifecycle.
- [ ] **T5.2: Stream Media Over gRPC**
  - Implement the media streaming bridge: pipe microphone buffer packets through JNI to the `SendParticipantData` gRPC stream, and pull playback audio buffers from the stream.

### Phase 6: Verification & Edge Cases
- [ ] **T6.1: Connection Lifecycle Validation**
  - Verify VM starts, registers boot status, completes JNI handoff, and registers gRPC control session.
- [ ] **T6.2: Node Execution Test**
  - Run a Whisper or Kokoro pipeline session inside the pVM; verify media is correctly received and executed.
- [ ] **T6.3: Error Handlers**
  - Verify system stability when: VM crashes (reboot/clean finish), vsock disconnects, or memory limit is exceeded.
- [ ] **T6.4: Python Managed Env smoke test**
  - Verify guest runner boots under `PYTHON_ENV_MODE=managed`.
  - Check that `uv` successfully resolves and provisions a Python environment inside the VM's writable storage (`/mnt/encryptedstore/remotemedia-venv`) and loads Python nodes.


