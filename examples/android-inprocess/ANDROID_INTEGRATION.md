# Android Integration Guide

This document describes how to integrate RemoteMedia pipelines into Android applications using the in-process Python execution engine.

## Overview

The RemoteMedia Android integration consists of:

1. **Rust JNI Library** (`libremotemedia_android_inprocess.so`) - Core pipeline executor
2. **Kotlin Wrapper** (`PipelineManager`, `NativeInterface`) - Android-friendly API
3. **Audio I/O** (`AudioRecorder`, `AudioPlayer`) - Low-latency audio capture/playback
4. **Pipeline Manifests** (YAML) - Declarative pipeline definitions

## Adding to Your Project

### 1. Copy Native Library

Place the built `.so` files in your project:
```
app/
└── src/
    └── main/
        └── jniLibs/
            ├── arm64-v8a/
            │   └── libremotemedia_android_inprocess.so
            └── x86_64/
                └── libremotemedia_android_inprocess.so
```

### 2. Configure Gradle

In `app/build.gradle.kts`:
```kotlin
android {
    sourceSets {
        main {
            jniLibs.srcDirs("src/main/jniLibs")
        }
    }
    
    packagingOptions {
        jniLibs {
            useLegacyPackaging = true
        }
        doNotStrip.add("**/libremotemedia_android_inprocess.so")
    }
}
```

### 3. Add Dependencies

```kotlin
dependencies {
    implementation("com.google.oboe:oboe:1.8.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-android:1.7.3")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.6.3")
}
```

### 4. Load Native Library

In your `Application` class:
```kotlin
class MyApplication : Application() {
    override fun onCreate() {
        super.onCreate()
        System.loadLibrary("remotemedia_android_inprocess")
        NativeInterface.initLogger()
    }
}
```

## Using the Pipeline API

### Basic Unary Execution

```kotlin
val pipelineManager = PipelineManager(this)

// Initialize
if (!pipelineManager.initialize()) {
    Log.e(TAG, "Failed to initialize")
    return
}

// Load manifest from assets
pipelineManager.loadManifest("voice-assistant-mobile.yaml")

// Execute single request
val result = pipelineManager.executeUnary("Hello, how are you?")
```

### Streaming Audio Pipeline

```kotlin
// Configure callbacks
pipelineManager.onOutput = { outputJson ->
    // Handle pipeline output (text, audio metadata)
    runOnUiThread { updateUI(outputJson) }
}

pipelineManager.onError = { error ->
    runOnUiThread { showError(error) }
}

pipelineManager.onStateChange = { state ->
    runOnUiThread { updateStateUI(state) }
}

// Load and start streaming
pipelineManager.loadManifest("voice-assistant-mobile.yaml")
pipelineManager.startStreaming()

// Start audio I/O
audioRecorder.start()
audioPlayer.start(24000)  // Kokoro sample rate

// In audio callback, send to pipeline
audioRecorder.onAudioData = { pcmData ->
    pipelineManager.sendAudio(pcmData)
}

// Receive audio from pipeline and play
// (PipelineManager handles this internally for voice assistant)
```

### Sending Text Input

```kotlin
// For TTS or LLM-only pipelines
pipelineManager.sendText("Your text here")
```

## Pipeline Manifests

Create YAML manifests in `assets/manifests/`:

```yaml
# my-pipeline.yaml
pipeline:
  name: "my-pipeline"
  execution_mode: "InProcess"  # Required for Android

nodes:
  - id: "stt"
    type: "PythonNode"
    class: "WhisperSTTNode"
    module_path: "assets://models/whisper/base.pt"
    execution_mode: "InProcess"
    config:
      model_size: "base"
      language: "en"
    inputs: ["audio_in"]
    outputs: ["text_out"]

  - id: "llm"
    type: "PythonNode"
    class: "Phi3LLMNode"
    module_path: "assets://models/llm/phi3_mini/"
    execution_mode: "InProcess"
    config:
      max_tokens: 100
      stream: true
    inputs: ["text_out"]
    outputs: ["llm_out"]

connections:
  - from: "stt.text_out"
    to: "llm.text_in"
```

Load in code:
```kotlin
pipelineManager.loadManifest("my-pipeline.yaml")
```

## Model Management

### Bundling Models

Models are bundled as APK assets. Use the bundling script:

```bash
python3 scripts/bundle_models.py \
  --output-dir app/src/main/assets/models \
  --manifest app/src/main/assets/models/manifest.json
```

### Loading Models

Models are referenced via `assets://` URIs in manifests:
```yaml
module_path: "assets://models/whisper/base.pt"
```

The runtime resolves this to:
1. Extracted cache directory (preferred, writable)
2. APK assets (fallback, read-only)

### Manifest Verification

The `manifest.json` contains SHA256 hashes for integrity:
```json
{
  "models": {
    "whisper_base": {
      "sha256": "abc123...",
      "path": "models/whisper/base"
    }
  }
}
```

## Audio I/O Integration

### AudioRecorder (Capture)

```kotlin
val recorder = AudioRecorder(context)

recorder.onAudioData = { pcmData ->
    // pcmData is 16kHz mono PCM16 ByteArray
    pipelineManager.sendAudio(pcmData)
}

recorder.onError = { error -> Log.e(TAG, error) }
recorder.onStateChange = { state -> updateUI(state) }

// Start recording
recorder.start()
```

Configuration:
- Input: 48kHz mono from microphone
- Output: 16kHz mono (resampled for Whisper)
- Frame size: 20ms (320 samples at 16kHz)

### AudioPlayer (Playback)

```kotlin
val player = AudioPlayer(context)

player.onError = { error -> Log.e(TAG, error) }
player.onStateChange = { state -> updateUI(state) }
player.onUnderrun = { Log.w(TAG, "Audio underrun") }

// Start playback
player.start(24000)  // Input sample rate (Kokoro = 24kHz)

// Queue audio from pipeline
// (PipelineManager handles this for voice assistant)
```

Configuration:
- Output: 48kHz mono to speaker
- Buffer: 3-5 frames ahead (60-100ms)
- Handles resampling from 16kHz/24kHz

## Adding Custom Nodes

### 1. Create Python Node

```python
# my_custom_node.py
class MyCustomNode:
    def initialize(self, config):
        self.config = config
        return {"status": "initialized"}
    
    def process(self, input_data):
        # Process input_data (RuntimeData)
        result = do_something(input_data)
        return RuntimeData.Text(result)
    
    def finalize(self):
        return {"status": "finalized"}
```

### 2. Bundle with App

Place in `assets/nodes/my_custom_node.py` and reference in manifest:
```yaml
- id: "custom"
  type: "PythonNode"
  class: "MyCustomNode"
  module_path: "assets://nodes/my_custom_node.py"
  execution_mode: "InProcess"
```

### 3. Register in Rust (Optional)

For better performance, implement as Rust node:
```rust
// In your custom crate
#[remotemedia::node]
pub struct MyCustomNode { ... }
```

## Advanced Configuration

### Runtime Selection

Android defaults to in-process CPython:
```kotlin
// Automatic - no config needed
val executor = NativeInterface.nativeCreateExecutor()
```

### Memory Limits

Configure in manifest:
```yaml
runtime:
  memory_limit_mb: 512
```

### Threading

Pipeline runs on dedicated Tokio runtime:
```kotlin
// Single-threaded per session
// Multiple sessions = multiple runtimes
```

### Battery Optimization

- Use `PARTIAL_WAKE_LOCK` during active sessions
- Aggressive VAD pauses LLM/TTS during silence
- Request `FOREGROUND_SERVICE_MICROPHONE` permission

## ProGuard Rules

```proguard
-keepclassmembers class com.yourpackage.NativeInterface {
    native <methods>;
}

-keep class kotlinx.coroutines.** { *; }
-keep class kotlinx.serialization.** { *; }
```

## Debugging

### Enable Debug Logging

```kotlin
// In Application
if (BuildConfig.DEBUG) {
    Timber.plant(Timber.DebugTree())
}
```

### Rust Logs

```bash
adb logcat -s RemoteMedia
```

### Common Issues

| Issue | Solution |
|-------|----------|
| `UnsatisfiedLinkError` | Check jniLibs/arch matches device |
| Model not found | Verify `manifest.json` paths, run bundling script |
| Audio glitches | Increase buffer size, check sample rates |
| OOM | Reduce model size, enable quantization |
| Slow startup | Pre-extract models, use smaller Whisper |

## Testing

### Unit Tests

```kotlin
@Test
fun testPipelineInitialization() {
    val manager = PipelineManager(context)
    assertTrue(manager.initialize())
    manager.destroy()
}
```

### Integration Tests

Run on device/emulator:
```bash
./gradlew connectedAndroidTest
```

## Migration from Subprocess

If migrating from the old subprocess-based Android integration:

1. Remove `iceoryx2` dependency
2. Change `ExecutionStrategy.Subprocess` to `InProcess`
3. Update manifests to use `execution_mode: "InProcess"`
4. Bundle Python stdlib + site-packages
5. Use `assets://` model paths

## Support

- Check [README.md](README.md) for build instructions
- See [MODELS.md](MODELS.md) for model details
- Main SDK docs: [RemoteMedia SDK](../../../docs/)