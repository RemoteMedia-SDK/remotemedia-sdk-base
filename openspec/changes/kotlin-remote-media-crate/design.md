## Context

The RemoteMedia SDK currently provides an Android in-process integration as an **example application** (`examples/android-inprocess/`) rather than a reusable library. The Kotlin code (`PipelineManager`, `NativeInterface`, `AudioRecorder`, `AudioPlayer`, `RemoteMediaApplication`) lives in the example app's `com.remotemedia.inprocess` package. Any Android project wanting to use RemoteMedia must copy these files, manually configure `jniLibs` for native `.so` files, and duplicate CMake/Gradle setup.

This creates several problems:
- **No versioning**: Consumers can't pin to a specific release
- **Maintenance burden**: Bug fixes in the example don't propagate to consumers
- **Inconsistent APIs**: Each consumer may modify the copied code differently
- **Build complexity**: Manual native library handling is error-prone
- **No distribution**: Can't depend via Maven/Gradle

The Rust side (`libremotemedia_android_inprocess.so`) is already built as a cdylib via Cargo and works correctly. The gap is purely on the Kotlin/JVM side: packaging, publishing, and API design.

## Goals / Non-Goals

**Goals:**
- Create a publishable Kotlin Android library (AAR) with Maven coordinates `com.remotemedia:remote-media-android`
- Provide idiomatic Kotlin API using coroutines, flows, sealed classes, and modern Android patterns
- Bundle native libraries (`.so` for all ABIs) inside the AAR automatically
- Enable zero-config consumption: `implementation("com.remotemedia:remote-media-android:X.Y.Z")` only
- Refactor the example app to consume the library, proving the integration works
- Support Android API 24+ (minSdk=24) with compileSdk/targetSdk=34
- Include ProGuard/R8 rules in the AAR for release builds

**Non-Goals:**
- iOS/Swift bindings (separate effort)
- Remote gRPC/WebRTC transport support (library focuses on in-process)
- Python-for-Android build automation (consumers handle model bundling)
- Kotlin Multiplatform (JVM/Android only for now)
- Maven Central publishing (local/GitHub Packages first; Central later)

## Decisions

### 1. Module Structure: Standalone Gradle Module (Not Cargo Workspace Member)

**Decision**: Create a new Gradle-only module at `remotemedia-sdk-base/kotlin-android/` (sibling to `examples/`), not a Cargo workspace member.

**Rationale**:
- Kotlin/Android builds are Gradle-native; mixing Cargo + Gradle in one module adds complexity
- The Rust cdylib is already built by the existing Cargo workspace (`remotemedia-android-inprocess` crate)
- Gradle module can depend on the pre-built `.so` files from `../target/<abi>/release/`
- Keeps build systems cleanly separated: Cargo for Rust, Gradle for Kotlin/AAR

**Alternative Considered**: Single mixed Cargo+Gradle module with `cargo-ndk` and `cargo-xwin` plugins.
**Rejected**: Higher maintenance, less familiar to Android developers, complicates IDE support.

### 2. Package Namespace: `com.remotemedia.android`

**Decision**: Use `com.remotemedia.android` as the library package. Deprecate `com.remotemedia.inprocess` with `@Deprecated` type aliases for one release cycle.

**Rationale**:
- `inprocess` implies implementation detail; `android` is the platform target
- Cleaner for consumers: `import com.remotemedia.android.PipelineManager`
- Type aliases allow zero-code migration for the example app

### 3. API Design: Coroutine-First with Callback Compatibility

**Decision**: Primary API uses `suspend` functions and `kotlinx.coroutines`; callbacks retained for backward compatibility and UI-thread simplicity.

**Rationale**:
- Kotlin coroutines are the standard for async on Android
- `runBlocking` in current `PipelineManager` blocks threads; suspend functions allow structured concurrency
- Callbacks (`onOutput`, `onError`) remain for simple `Activity`/`ViewModel` integration without scopes

**Example Migration**:
```kotlin
// Current (blocking)
val result = pipelineManager.executeUnary("Hello")

// New (suspend)
val result = pipelineManager.executeUnary("Hello") // suspend fun
// Or with callback for streaming
pipelineManager.onOutput = { json -> ... }
```

### 4. Native Library Bundling: CMake `IMPORTED` Targets → AAR `jni/`

**Decision**: Keep existing CMake `add_rust_library()` approach to copy `.so` files to `src/main/jniLibs/<abi>/`, then configure Gradle to package `jniLibs` into AAR's `jni/`.

**Rationale**:
- CMake already handles multi-ABI copy logic correctly
- Gradle's `jniLibs { useLegacyPackaging = true }` packages them into AAR automatically
- No custom Gradle tasks needed; standard Android build flow

### 5. Audio I/O: Oboe-Based, Optional Dependency

**Decision**: Keep `AudioRecorder`/`AudioPlayer` in the library but make Oboe a `api` (transitive) dependency. Consumers get it automatically.

**Rationale**:
- Oboe is the standard for low-latency audio on Android
- Most voice assistant use cases need audio I/O
- Separate `remote-media-android-audio` module adds complexity for marginal benefit

### 6. Manifest/Plugin Handling: Asset-Based, Runtime Extraction

**Decision**: Keep current approach: pipeline manifests in `assets/manifests/`, loadable plugins in `assets/plugins/`, extracted to `context.filesDir/` on first init.

**Rationale**:
- Works with APK asset system (no external file management)
- Plugins are `.so` files that `dlopen()` needs real file paths for
- Extraction is fast (one-time) and survives app updates

### 7. Maven Publishing: `maven-publish` Plugin with GitHub Packages

**Decision**: Configure `maven-publish` in the library module to publish to GitHub Packages (or local `file://` repo for testing).

**Rationale**:
- GitHub Packages is free for public repos, integrated with GitHub Actions
- Easy migration to Maven Central later
- `signing` plugin can be added for Central requirements

## Risks / Trade-offs

| Risk | Impact | Mitigation |
|------|--------|------------|
| **ABI mismatch between Rust and Kotlin JNI signatures** | Crashes on `nativeCreateExecutor` etc. | Automated JNI header generation via `jni-rs` or manual sync; test matrix on all ABIs |
| **Python stdlib/site-packages size bloats APK** | APK > 100MB, slow installs | Document model bundling; recommend quantization; optional dynamic feature module |
| **Oboe version conflicts with consumer apps** | Build failures or runtime crashes | Use `api` dependency; document version; allow exclusion |
| **Gradle/CMake sync issues on CI** | Flaky builds | Pin NDK version; use `ndkVersion` in Gradle; cache Cargo target dir |
| **Namespace migration breaks example app** | Example won't compile | Provide `@Deprecated` type aliases in `com.remotemedia.inprocess` for one release |
| **AAR size with multi-ABI native libs** | Large download | ABI splits or dynamic delivery (future); current: acceptable for in-process use case |

## Migration Plan

1. **Create library module** at `kotlin-android/` with Gradle build, package `com.remotemedia.android`
2. **Move Kotlin sources** from `examples/android-inprocess/app/src/main/java/com/remotemedia/inprocess/` to library, update package declarations
3. **Refactor API** to use suspend functions, add ProGuard rules, bundle native libs via CMake
4. **Configure maven-publish** for local/GitHub Packages
5. **Update example app** to depend on library (`implementation(project(":kotlin-android"))` locally, then published coords)
6. **Verify** example app builds and runs on device/emulator (arm64-v8a, x86_64)
7. **Publish** first version (0.1.0) to GitHub Packages
8. **Deprecate** `com.remotemedia.inprocess` with type aliases; remove in next major version

## Open Questions

1. **Versioning scheme**: SemVer starting at 0.1.0 (unstable) or 1.0.0? Given active development, 0.x seems appropriate.
2. **Python library bundling**: Should `libpython3.11.so` be in the AAR or a separate `remote-media-android-python` artifact? Current spec includes it conditionally.
3. **Gradle convention plugin**: Is a separate `com.remotemedia.android` plugin worth it, or is standard dependency enough? Defer to v0.2.
4. **Kotlin serialization format**: Current uses `kotlinx-serialization` JSON. Will this be stable for manifest format? Yes, but version manifests if schema changes.
5. **Minimum Rust MSRV for Android**: Verify the `remotemedia-android-inprocess` crate's MSRV is compatible with NDK toolchain.