# Specification: Microdroid VM Runner

Extend the `kotlin-android` library to support executing pipeline nodes inside a Microdroid pVM (protected Virtual Machine). The host Android app acts as the scaling entry orchestrator, and the pVM acts as the scaling runner. Communication is established using gRPC (`RunnerControl` protocol) over a `vsock` stream.

---

## 1. Scope

### In Scope
1. **Host-Side Kotlin Lifecycle API**: `MicrodroidController.kt` class managing the boot, callback reporting, and vsock connection retrieval of the pVM.
2. **JNI File Descriptor Bridge**: API and native plumbing to hand off the vsock `ParcelFileDescriptor` to the Rust core.
3. **Rust Vsock gRPC Connector**: Custom JNI-side Tonic connector binding gRPC services over the passed vsock socket stream.
4. **Guest VM Payload**: A native Rust/C++ executable `remotemedia-microdroid-runner` compiled for `aarch64-linux-android` that accepts vsock connections and runs the gRPC server.
5. **Bidirectional Media Streaming**: Forwarding of microphone and speaker buffers via the gRPC `SendParticipantData` bidirectional streaming RPC.
6. **Build System Integration**: Automatic Gradle and CMake compilation of the guest binary and packaging into host APK assets.

### Out of Scope Initially
1. **Dynamic Model Hot-Swapping**: Weights are either pre-packaged in the VM virtual disk/assets or streamed at session initialization.
2. **Multi-VM Load Balancing**: A single host app maps to exactly one running pVM instance.

---

## 2. Host-Side Lifecycle API

The `kotlin-android` library exposes the `MicrodroidController` class:

```kotlin
package com.remotemedia.android

import android.content.Context
import android.system.virtualmachine.VirtualMachine
import java.io.Closeable

class MicrodroidController(private val context: Context) : Closeable {
    /**
     * Starts the pVM. Blocks/suspends until the VM has completed boot and
     * the guest payload has notified onPayloadReady().
     */
    suspend fun startVm()

    /**
     * Connects to vsock port 5555 and returns the socket.
     * Throws IllegalStateException if the VM is not running.
     */
    fun connectVsock(): android.os.ParcelFileDescriptor

    /**
     * Gracefully stops the VM.
     */
    fun stopVm()

    /**
     * Closes the controller and terminates any active VM.
     */
    override fun close()
}
```

The controller hooks into `VirtualMachineCallback` to catch and propagate VM lifecycle states:
- `onPayloadStarted` -> Log boot initialization
- `onPayloadReady` -> Complete the `startVm` suspend continuation
- `onPayloadFinished(exitCode)` -> Clean termination, stop session
- `onError(errorCode, message)` -> Raise `NativeException` to the host

---

## 3. JNI Bridge API

The `NativeInterface` JNI interface is extended:

```kotlin
object NativeInterface {
    // ... existing JNI endpoints ...

    /**
     * Detaches the FD from the vsock ParcelFileDescriptor and transfers
     * ownership to the Rust JNI core.
     */
    @JvmStatic
    external fun nativeSetVsockFd(fd: Int): Boolean
}
```

On the Rust side:
- The raw `RawFd` integer is retrieved.
- Rust wraps it via `std::os::unix::net::UnixStream::from_raw_fd` and binds a Tonic `Channel` utilizing a custom `service_fn` async connector.
- Once configured, all session control requests route through this channel.

---

## 4. Guest Payload Protocol

- **Transport**: AF_VSOCK socket stream
- **Fixed Port**: 5555
- **CID**: Host assigns CID dynamically; Guest payload binds to `VMADDR_CID_ANY` to listen to any incoming host connection.
- **Service API**: Standard gRPC `remotemedia.scaling.runner.v1.RunnerControl` service.
- **Python Integration**: The guest runner runs a standard local Python node executor. Python standard library and dependencies are loaded in the pVM sandbox.

---

## 5. Media Pipeline Loop

To feed media to nodes inside the pVM:

```
[Android Mic Capture] ──► [PipelineManager (Kotlin)]
                                  │
                                  ▼
                        [JNI/Rust client]
                                  │  ParticipantInputMessage (PCM bytes)
                                  ▼  (via gRPC SendParticipantData)
                        [Microdroid Runner (pVM)]  ◄─── Runs Whisper / LLM / Kokoro
                                  │
                                  ▼  ParticipantOutputMessage (PCM bytes)
                        [JNI/Rust client]
                                  │
                                  ▼
[Android Speaker Playback] ◄── [AudioPlayer]
```

---

## 6. Verification Criteria

### Build Verification
- Gradle build compiles `remotemedia-microdroid-runner` target for `arm64-v8a` / `aarch64-linux-android`.
- The output APK contains `assets/microdroid/remotemedia-microdroid-runner` and `assets/microdroid/runner_vm_config.json`.

### Host Verification
- Executing `startVm` successfully boots the VM.
- `onPayloadReady` is captured, and `connectVsock` returns a valid socket descriptor.
- JNI handover succeeds and Rust core successfully registers gRPC handshake.

### Media Verification
- Streaming 16kHz PCM audio packets into the VM runner returns transcribed text events or output audio packets over the same vsock channel.
- System remains responsive and handles clean VM teardown via `stopVm()`.
- If the VM runs out of memory (OOM), the guest process is killed and the host receives a clean `onPayloadFinished` with a non-zero exit code, triggering a recovery/restart strategy.

### Python Managed Runner Verification
- Setting `PYTHON_ENV_MODE=managed` environment variable inside the VM triggers the `PythonEnvManager` to use the embedded `uv` binary (`/mnt/apk/assets/bin/uv`).
- The runner successfully provisions a virtual environment in `/mnt/encryptedstore/remotemedia-venv/` using Bionic `libpython3.11.so`.
- The runner successfully installs the dependencies listed in `manifest.json` for a source-loadable Python node (e.g. numpy, torch) into the VM's writable storage.
- Python nodes execute successfully inside the VM context and their standard output/error are captured and forwarded to the host app's logcat context with prefix `[pVM-Python]`.


