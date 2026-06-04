# RemoteMedia Android In-Process Voice Assistant

A complete Android application that executes RemoteMedia speech-to-speech pipelines entirely on-device using PyO3 in-process Python execution.

## Features

- 🎤 **Voice Assistant** - Full VAD → STT → LLM → TTS pipeline
- 📱 **Offline-First** - No network required after install
- 📦 **Single APK** - All models bundled (~100-150MB)
- ⚡ **Low Latency** - < 800ms TTFA on mid-tier devices
- 🔧 **Extensible** - Easy to add new Python nodes/models
- 🦀 **Rust + Kotlin** - Performance-critical path in Rust, UI in Kotlin

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Android App Process                       │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────┐  │
│  │   Kotlin UI  │◄──►│  Pipeline    │◄──►│   Rust JNI Lib   │  │
│  │  (Activity,  │    │  Manager     │    │  (cdylib)        │  │
│  │   ViewModel) │    │  (JNI wrap)  │    │                  │  │
│  └──────────────┘    └──────────────┘    └────────┬─────────┘  │
│                                                   │             │
│  ┌──────────────┐    ┌──────────────┐            │             │
│  │  Audio I/O   │    │   Assets     │            │             │
│  │  (Oboe/      │◄──►│  (Models,    │            │             │
│  │   AudioTrack)│    │   Manifests) │            │             │
│  └──────────────┘    └──────────────┘            │             │
│                                                   ▼             │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │              RemoteMedia Core (in-process)                  │ │
│  │  ┌─────────────┐  ┌──────────────┐  ┌──────────────────┐  │ │
│  │  │ Pipeline    │  │ Runtime      │  │ PythonNodeHandle │  │ │
│  │  │ Executor    │◄─►│ Selector     │◄─►│ (PyO3)           │  │ │
│  │  └─────────────┘  └──────────────┘  ├──────────────────┤  │ │
│  │                                    │ LoadableNode     │  │ │
│  │                                    │ (dynamic FFI)    │  │ │
│  │                                    └────────┬────┬────┘  │ │
│  └─────────────────────────────────────────────│────│───────┘ │
│                                                │    │         │
│  ┌──────────────────────────────────────────────▼────▼───────┐ │
│  │              Python Interpreter (embedded)                 │ │
│  │  ┌──────────┐          ┌──────────┐ ┌───────────────┐    │ │
│  │  │ Whisper  │          │ Kokoro   │ │ Silero VAD    │    │ │
│  │  │ (STT)    │          │ (TTS)    │ │ (ONNX)        │    │ │
│  │  └──────────┘          └──────────┘ └───────────────┘    │ │
│  └────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

## Requirements

- Android API 24+ (Android 7.0)
- ARM64 or x86_64 device
- ~500MB RAM for full pipeline
- 2-4 GB storage for models + app

## Building

### Prerequisites

1. **Android SDK** (API 34) and **NDK** (r25c / 25.2.9519653)
2. **Rust** (1.88+) with Android targets:
   ```bash
   rustup target add aarch64-linux-android x86_64-linux-android
   ```
3. **Python 3.11+** for model bundling:
   ```bash
   pip install huggingface_hub requests
   ```

### Quick Build

```bash
cd remotemedia-sdk-base/examples/android-inprocess

# Build release APK for ARM64 (default)
./build-android.sh

# Build for both architectures
./build-android.sh -a both

# Build with model bundling
./build-android.sh -b

# Debug build
./build-android.sh -t debug
```

### Manual Build Steps

1. **Build Rust library:**
   ```bash
   cargo build --target aarch64-linux-android --release
   ```

2. **Bundle models (optional):**
   ```bash
   python3 ../../../scripts/bundle_models.py \
     --output-dir app/src/main/assets/models \
     --manifest app/src/main/assets/models/manifest.json
   ```

3. **Build APK with Gradle:**
   ```bash
   ./gradlew assembleRelease
   ```

### Build Output

- Debug APK: `app/build/outputs/apk/debug/app-debug.apk`
- Release APK: `app/build/outputs/apk/release/app-release.apk`
- Copied to project root as `remotemedia-inprocess-{debug,release}.apk`

## Models

The app bundles the following models by default:

| Model | Category | Format | Size | Description |
|-------|----------|--------|------|-------------|
| Whisper tiny | STT | Candle | 39 MB | Fast transcription |
| Whisper base | STT | Candle | 74 MB | Better accuracy |
| Phi-3-mini | LLM | GGUF Q4 | 2.2 GB | Conversational AI |
| Kokoro v0.19 | TTS | ONNX | 82 MB + voices | Natural speech |
| Silero VAD | VAD | ONNX | 5 MB | Voice activity detection |

Models are stored in `app/src/main/assets/models/{category}/{model}/` and extracted to cache on first run.

## Pipeline Manifests

Three built-in pipelines:

1. **voice-assistant-mobile.yaml** - Full VAD→STT→LLM→TTS
2. **transcribe-mobile.yaml** - VAD→STT only (text output)
3. **tts-mobile.yaml** - LLM→TTS (text input, audio output)

All use `execution_mode: "InProcess"` for Android compatibility.

## Running

1. Install APK on device:
   ```bash
   adb install remotemedia-inprocess-release.apk
   ```

2. Grant microphone permission when prompted

3. Tap microphone button to start voice assistant

4. Speak - the assistant will transcribe, generate response, and speak back

## Configuration

### Settings (in-app)

- **Model size**: Whisper tiny vs base
- **Voice selection**: Kokoro voices (af_bella, af_sarah, am_michael, etc.)
- **VAD sensitivity**: Threshold 0.1-0.9

### Build-time Configuration

Edit `gradle.properties`:
```properties
android.ndkVersion=25.2.9519653
android.compileSdkVersion=34
android.minSdkVersion=24
```

## Project Structure

```
android-inprocess/
├── app/
│   ├── src/main/
│   │   ├── java/com/remotemedia/inprocess/
│   │   │   ├── MainActivity.kt          # Main UI
│   │   │   ├── PipelineManager.kt       # JNI pipeline wrapper
│   │   │   ├── AudioRecorder.kt         # Low-latency capture
│   │   │   ├── AudioPlayer.kt           # Low-latency playback
│   │   │   ├── NativeInterface.kt       # JNI bindings
│   │   │   └── AudioProcessingService.kt # Foreground service
│   │   ├── assets/
│   │   │   ├── manifests/               # Pipeline YAMLs
│   │   │   └── models/                  # Bundled models
│   │   ├── res/                         # Layouts, strings, themes
│   │   └── AndroidManifest.xml
│   ├── build.gradle.kts
│   └── proguard-rules.pro
├── src/
│   └── lib.rs                           # Rust JNI layer
├── Cargo.toml
├── build.gradle.kts
├── settings.gradle.kts
├── CMakeLists.txt
├── gradle.properties
├── local.properties.template
├── build-android.sh
└── README.md
```

## JNI API

```kotlin
// Initialize logger
NativeInterface.initLogger()

// Create executor
val handle = NativeInterface.nativeCreateExecutor()

// Unary execution
val result = NativeInterface.nativeRunPipeline(handle, manifestJson)

// Streaming
val session = NativeInterface.nativeCreateSession(handle, manifestJson)
NativeInterface.nativeSendInputAudio(session, pcmData, 16000, 1)
val output = NativeInterface.nativeRecvOutput(session)
NativeInterface.nativeCloseSession(session)

// Cleanup
NativeInterface.nativeDestroyExecutor(handle)
```

## Troubleshooting

### APK too large (>150MB)
- Use Whisper tiny instead of base
- Use Phi-3-mini Q4 instead of larger models
- Remove unused voices from Kokoro

### Audio latency issues
- Ensure Oboe is using AAudio (API 26+)
- Check buffer sizes in AudioRecorder/AudioPlayer
- Use 20ms frames (960 samples at 48kHz)

### Model loading fails
- Check manifest.json hashes match
- Verify models extracted to cache directory
- Ensure `libpython3.11.so` is in jniLibs

### Build fails
- Check NDK version matches (r25c)
- Ensure Rust targets installed
- Clear Gradle cache: `./gradlew clean`

## Performance Targets

| Metric | Target | Notes |
|--------|--------|-------|
| TTFA (Time To First Audio) | < 800ms | End-to-end speech→response |
| STT Latency | < 300ms | 5-second utterance |
| LLM First Token | < 200ms | Phi-3-mini streaming |
| TTS First Chunk | < 150ms | Kokoro streaming |
| APK Size | < 150MB | Play Store limit |
| Memory Usage | < 500MB | Typical mid-tier device |

## License

MIT OR Apache-2.0

## Contributing

See main [RemoteMedia SDK](../../../CONTRIBUTING.md) for contribution guidelines.