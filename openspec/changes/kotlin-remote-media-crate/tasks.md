## 1. Library Module Setup

- [ ] 1.1 Create `kotlin-android/` module directory structure
- [ ] 1.2 Create `kotlin-android/build.gradle.kts` with Android library plugin, Maven publishing, serialization plugin
- [ ] 1.3 Create `kotlin-android/settings.gradle.kts` (include in root settings)
- [ ] 1.4 Create `kotlin-android/CMakeLists.txt` mirroring example app's native library copy logic
- [ ] 1.5 Add `kotlin-android` to root `settings.gradle.kts`
- [ ] 1.6 Configure `minSdk=24`, `compileSdk=34`, `targetSdk=34`, Kotlin 1.9+, Java 8 in library module

## 2. Kotlin Source Migration & API Redesign

- [ ] 2.1 Move `PipelineManager.kt` to `kotlin-android/src/main/kotlin/com/remotemedia/android/`, update package
- [ ] 2.2 Move `NativeInterface.kt` to library, update package, add `@JvmStatic` where needed
- [ ] 2.3 Move `AudioRecorder.kt` to library, update package
- [ ] 2.4 Move `AudioPlayer.kt` to library, update package
- [ ] 2.5 Move `RemoteMediaApplication.kt` to library, update package
- [ ] 2.6 Move `AudioProcessingService.kt` to library, update package (or mark internal)
- [ ] 2.7 Refactor `PipelineManager` to use `suspend` functions instead of `runBlocking`
- [ ] 2.8 Add `PipelineState`, `NodeInfo`, `PipelineMode`, `NativeException` as public data classes/enums
- [ ] 2.9 Add `@Deprecated` type aliases in `com.remotemedia.inprocess` package for migration

## 3. Native Library Bundling

- [ ] 3.1 Configure CMake to copy `libremotemedia_android_inprocess.so` from `../target/<abi>/release/` to `src/main/jniLibs/<abi>/`
- [ ] 3.2 Configure CMake to conditionally copy `libpython3.11.so` from `../python-libs/<abi>/` if present
- [ ] 3.3 Add `jniLibs { useLegacyPackaging = true }` to library's `build.gradle.kts`
- [ ] 3.4 Verify AAR output contains `jni/arm64-v8a/libremotemedia_android_inprocess.so` and `jni/x86_64/...`

## 4. Dependencies & Build Configuration

- [ ] 4.1 Declare transitive dependencies (`api`): `kotlinx-coroutines-android`, `kotlinx-serialization-json`, `com.google.oboe:oboe`, `androidx.core:core-ktx`
- [ ] 4.2 Add ProGuard consumer rules file (`consumer-proguard-rules.pro`) preserving JNI methods, coroutines, serialization
- [ ] 4.3 Reference consumer ProGuard rules in `build.gradle.kts` via `consumerProguardFiles`
- [ ] 4.4 Configure `maven-publish` plugin with `MavenPublication` for AAR + sources + javadoc
- [ ] 4.5 Set version property (`0.1.0`) in `gradle.properties` or `build.gradle.kts`

## 5. Example App Refactoring

- [ ] 5.1 Remove local Kotlin sources from `examples/android-inprocess/app/src/main/java/com/remotemedia/inprocess/`
- [ ] 5.2 Update `app/build.gradle.kts` to depend on library: `implementation(project(":kotlin-android"))` (local) or published coords
- [ ] 5.3 Remove `jniLibs` configuration from example app's `build.gradle.kts`
- [ ] 5.4 Remove CMake external native build from example app (library handles it)
- [ ] 5.5 Update `MainActivity.kt` imports to `com.remotemedia.android.*`
- [ ] 5.6 Update `AndroidManifest.xml` to use `com.remotemedia.android.RemoteMediaApplication` (or custom subclass)
- [ ] 5.7 Verify example app builds: `./gradlew :examples:android-inprocess:app:assembleDebug`

## 6. Testing & Verification

- [ ] 6.1 Build library AAR: `./gradlew :kotlin-android:assembleRelease`
- [ ] 6.2 Verify AAR contents: `unzip -l kotlin-android/build/outputs/aar/kotlin-android-release.aar`
- [ ] 6.3 Test on arm64-v8a device/emulator: install example APK, run voice assistant flow
- [ ] 6.4 Test on x86_64 emulator: install example APK, run voice assistant flow
- [ ] 6.5 Test unary execution: `PipelineManager.executeUnary("test")` returns JSON
- [ ] 6.6 Test streaming: `startStreaming()` → `sendAudio()` → `onOutput` receives results
- [ ] 6.7 Test error handling: uninitialized executor, missing manifest, native errors
- [ ] 6.8 Test resource cleanup: `destroy()` releases native handles, no leaks

## 7. Publishing & Documentation

- [ ] 7.1 Configure GitHub Packages publishing (or local `file://` repo for testing)
- [ ] 7.2 Publish library: `./gradlew :kotlin-android:publish`
- [ ] 7.3 Verify published artifact resolvable from fresh project
- [ ] 7.4 Update `ANDROID_INTEGRATION.md` to show library dependency instead of local copy
- [ ] 7.5 Add library version to `gradle.properties` for easy updates
- [ ] 7.6 Create `CHANGELOG.md` in library module

## 8. CI/CD Integration

- [ ] 8.1 Add library build to CI workflow (Gradle + Cargo for native libs)
- [ ] 8.2 Cache Cargo target directory in CI for faster rebuilds
- [ ] 8.3 Add AAR artifact upload to CI for release workflow
- [ ] 8.4 Configure release tagging to trigger publish to GitHub Packages