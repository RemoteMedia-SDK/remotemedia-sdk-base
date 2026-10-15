## MODIFIED Requirements

### Requirement: Example app consumes published library instead of local source
The example app SHALL remove all local Kotlin source files (`PipelineManager.kt`, `NativeInterface.kt`, `AudioRecorder.kt`, `AudioPlayer.kt`, `RemoteMediaApplication.kt`, `AudioProcessingService.kt`) and depend solely on the published `com.remotemedia:remote-media-android` AAR.

#### Scenario: Build succeeds with only library dependency
- **WHEN** `app/build.gradle.kts` contains only `implementation("com.remotemedia:remote-media-android:1.0.0")` and no local source files
- **THEN** `./gradlew assembleDebug` succeeds and APK runs on device

#### Scenario: MainActivity uses library API without modification
- **WHEN** `MainActivity.kt` imports `com.remotemedia.android.*` and uses `PipelineManager`, `AudioRecorder`, `AudioPlayer`
- **THEN** all functionality works identically to local-source version

### Requirement: Example app uses library's RemoteMediaApplication base class (optional)
The example app SHALL optionally extend `com.remotemedia.android.RemoteMediaApplication` for automatic native library loading and logger initialization.

#### Scenario: Application class delegates to library init
- **WHEN** `class VoiceAssistantApp : RemoteMediaApplication()`
- **THEN** `onCreate()` loads `libremotemedia_android_inprocess.so` and initializes JNI logger automatically

### Requirement: Example app demonstrates library usage patterns
The example app SHALL serve as a reference implementation showing:
- Unary pipeline execution
- Streaming voice assistant with audio I/O
- Custom manifest loading from assets
- Error handling and state observation

#### Scenario: Voice assistant flow works end-to-end
- **WHEN** app launches, loads manifest, starts streaming, records audio, receives TTS output
- **THEN** user hears synthesized speech response to spoken input

## REMOVED Requirements

### Requirement: Local Kotlin source duplication
**Reason**: Moved to reusable library; duplication creates maintenance burden and version skew.
**Migration**: Delete `app/src/main/java/com/remotemedia/inprocess/*.kt`; add library dependency; update imports to `com.remotemedia.android`.

### Requirement: Manual jniLibs configuration in example app
**Reason**: Library AAR now bundles native libraries; consumer projects no longer need manual `jniLibs.srcDirs`.
**Migration**: Remove `jniLibs` block from `app/build.gradle.kts`; remove `src/main/jniLibs/` directory.