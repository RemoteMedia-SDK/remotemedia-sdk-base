## Why

The current RemoteMedia Android integration lives entirely within the example application (`com.remotemedia.inprocess` package in `examples/android-inprocess/app/`). This makes it impossible for other Android projects to cleanly consume RemoteMedia as a reusable library. Users must copy-paste Kotlin source files, manually handle native library bundling, and duplicate Gradle/CMake configuration. A proper Kotlin library crate (AAR) with Maven publishing would allow any Android project to add RemoteMedia with a single `implementation("com.remotemedia:remote-media-android:X.Y.Z")` dependency.

## What Changes

- **New Capability**: Publish a reusable Kotlin Android library (AAR) containing:
  - `PipelineManager` - High-level pipeline lifecycle and execution API
  - `NativeInterface` - JNI bindings to `libremotemedia_android_inprocess.so`
  - `AudioRecorder` / `AudioPlayer` - Low-latency audio I/O (Oboe-based)
  - `PipelineState`, `NodeInfo`, `PipelineMode` - Data models
  - `RemoteMediaApplication` (optional base class) for easy initialization
- **New Capability**: Gradle plugin / CMake integration for automatic native library copying to `jniLibs`
- **New Capability**: Maven publishing configuration for library distribution
- **Modified Capability**: `android-inprocess` example app refactored to consume the published AAR instead of local source
- **Breaking Change**: Package namespace changes from `com.remotemedia.inprocess` to `com.remotemedia.android` (or similar library-appropriate package)

## Capabilities

### New Capabilities

- `kotlin-android-library`: Publishable AAR artifact with idiomatic Kotlin API for RemoteMedia pipelines
- `android-native-bundling`: Automatic native library (`.so`) inclusion via Gradle/CMake without manual `jniLibs` copying
- `android-audio-io`: Reusable Oboe-based audio capture/playback components
- `android-pipeline-management`: High-level `PipelineManager` with unary + streaming modes, callbacks, state machine

### Modified Capabilities

- `android-inprocess-example`: Refactor example app to depend on published library instead of local source

## Impact

- **New crate/module**: `remotemedia-sdk-base/kotlin-android/` (or similar) with library structure
- **Existing**: `examples/android-inprocess/` becomes a consumer of the library
- **Build**: Cargo workspace gains a Kotlin/Android module (dual Cargo + Gradle), or separate Gradle-only module
- **Distribution**: Maven publishing (local, GitHub Packages, or Maven Central)
- **Dependencies**: `kotlinx-coroutines`, `kotlinx-serialization`, `com.google.oboe:oboe`, AndroidX core/ktx
- **Min SDK**: 24 (Android 7.0), Target SDK: 34+