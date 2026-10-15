## ADDED Requirements

### Requirement: Library publishes as AAR with proper Maven coordinates
The library SHALL be published as an Android Archive (AAR) with Maven coordinates `com.remotemedia:remote-media-android:<version>`.

#### Scenario: AAR contains all public API classes
- **WHEN** a consumer adds `implementation("com.remotemedia:remote-media-android:1.0.0")` to their dependencies
- **THEN** the following classes are available for import: `PipelineManager`, `NativeInterface`, `AudioRecorder`, `AudioPlayer`, `PipelineState`, `NodeInfo`, `PipelineMode`, `NativeException`

#### Scenario: AAR includes native libraries for all supported ABIs
- **WHEN** the AAR is resolved by Gradle
- **THEN** `jni/arm64-v8a/libremotemedia_android_inprocess.so` and `jni/x86_64/libremotemedia_android_inprocess.so` are included

#### Scenario: AAR declares transitive dependencies
- **WHEN** a consumer adds the library
- **THEN** `kotlinx-coroutines-android`, `kotlinx-serialization-json`, `com.google.oboe:oboe`, `androidx.core:core-ktx` are transitively resolved

### Requirement: Library uses a clean, library-appropriate package namespace
The library SHALL use package `com.remotemedia.android` (not `com.remotemedia.inprocess`) for all public API.

#### Scenario: No internal/example package leakage
- **WHEN** a consumer imports from `com.remotemedia.android`
- **THEN** no classes from `com.remotemedia.inprocess` are exposed

#### Scenario: Backward compatibility via type aliases (optional)
- **WHEN** the library provides `@Deprecated` type aliases in `com.remotemedia.inprocess` pointing to `com.remotemedia.android`
- **THEN** existing example app code compiles without changes during transition

### Requirement: Library supports Android API 24+ with modern standards
The library SHALL target `minSdk = 24`, `compileSdk = 34`, `targetSdk = 34` and use Kotlin 1.9+, Java 8 bytecode.

#### Scenario: Build succeeds on API 24 device
- **WHEN** the library is built and tested on an API 24 emulator
- **THEN** all public APIs function without `ClassNotFoundException` or API-level crashes

### Requirement: ProGuard/R8 rules bundled in AAR
The library SHALL include `consumerProguardFiles` referencing bundled ProGuard rules to preserve JNI methods and Kotlin coroutines/serialization.

#### Scenario: Release build with minification keeps JNI methods
- **WHEN** a consumer builds a release APK with `isMinifyEnabled = true`
- **THEN** `NativeInterface` native methods are not stripped