# RemoteMedia Kotlin Android Build Guide

## Overview

`kotlin-android` is an Android library module providing Kotlin APIs for RemoteMedia pipeline execution, including:
- Pipeline lifecycle management
- Audio I/O (recording/playback)
- Native JNI interface to Rust executor
- Kotlin-native LiteRT-LM node bridge (optional)

## Prerequisites

- Android SDK (API 34, NDK 25.2.9519653)
- Kotlin 1.9.22+
- Gradle 8.5+

## Build Commands

### Build the AAR (Android Archive)

```bash
# From project root
cd kotlin-android
./gradlew assembleRelease
```

Output: `build/outputs/aar/kotlin-android-release.aar`

### Build with Dependencies (for local Maven)

```bash
# Publish to local Maven repository
./gradlew publishToMavenLocal
```

Then depend on it:
```kotlin
implementation("com.remotemedia.android:kotlin-android:1.0.0")
```

### Run Tests

```bash
./gradlew test
./gradlew connectedAndroidTest  # requires device/emulator
```

## Using in Another Project

### Option 1: Local filesystem dependency

In `settings.gradle.kts`:
```kotlin
include(":kotlin-android")
project(":kotlin-android").projectDir = file("../path/to/kotlin-android")
```

In `app/build.gradle.kts`:
```kotlin
dependencies {
    implementation(project(":kotlin-android"))
}
```

### Option 2: Publish to local Maven

```bash
cd kotlin-android
./gradlew publishToMavenLocal
```

```kotlin
// In consumer project
repositories {
    mavenLocal()
}
dependencies {
    implementation("com.remotemedia.android:kotlin-android:1.0.0")
}
```

## Module Structure

```
kotlin-android/
├── build.gradle.kts          # Library configuration
├── settings.gradle.kts       # Project settings
├── CMakeLists.txt            # Native library copying (Rust cdylib)
├── src/main/
│   ├── kotlin/com/remotemedia/android/
│   │   ├── AudioPlayer.kt           # Oboe-based audio output
│   │   ├── AudioRecorder.kt         # AudioRecord-based input
│   │   ├── AudioProcessingService.kt # Background audio service
│   │   ├── PipelineManager.kt       # High-level pipeline API
│   │   ├── NativeInterface.kt       # JNI to Rust executor
│   │   └── LiteRtLmNodeBridge.kt    # Optional: native LLM via LiteRT-LM
│   └── jniLibs/                # Native libraries (copied by CMake)
│       ├── arm64-v8a/
│       │   ├── libremotemedia_android_inprocess.so
│       │   ├── ${LIBPYTHON_NAME}
│       │   ├── liblitert_lm.so
│       │   └── libGemmaModelConstraintProvider.so
│       └── x86_64/
└── proguard-rules.pro
```

## Version Configuration

Edit `build.gradle.kts` for version:
```kotlin
android {
    defaultConfig {
        versionCode = 1
        versionName = "1.0.0"
    }
}
```

## CI/CD Notes

- Built as part of RemoteMedia release pipeline
- AAR published to Maven Central via GitHub Actions
- Minimum SDK: 24 (Android 7.0)
- Target SDK: 34 (Android 14)
- Requires Java 8 source/target compatibility

## Native Dependencies

The library expects these native libraries at runtime (included via `jniLibs`):

| Library | Source | Purpose |
|---------|--------|---------|
| `libremotemedia_android_inprocess.so` | Rust cargo build | Pipeline executor |
| `libpython3.11.so` | python-for-android | In-process Python runtime (see config/python-version.toml) |
| `liblitert_lm.so` | LiteRT-LM Bazel build | LLM engine |
| `libGemmaModelConstraintProvider.so` | LiteRT-LM prebuilt | Gemma model support |

These are copied by `CMakeLists.txt` from:
- `../../target/aarch64-linux-android/release/` (Rust)
- `../../python-libs/` (Python)
- `../../LiteRT-LM/prebuilt/android_arm64/` (LiteRT-LM)

## Troubleshooting

### "Plugin already on classpath with unknown version"
Ensure all modules use the same Android Gradle Plugin version. Keep versions in root `build.gradle.kts` and use `apply false` in subprojects.

### Missing native libraries
Run the full build from the android-inprocess example:
```bash
cd examples/android-inprocess
./build-android.sh -t release
```
This builds all native dependencies and copies them to `kotlin-android/src/main/jniLibs/`.