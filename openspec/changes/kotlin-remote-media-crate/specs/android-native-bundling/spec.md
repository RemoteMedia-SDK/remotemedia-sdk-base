## ADDED Requirements

### Requirement: Native library automatically copied to AAR jni/ directory
The build system SHALL automatically copy `libremotemedia_android_inprocess.so` from the Rust cargo target directory into the AAR's `jni/<abi>/` directories for all supported ABIs (arm64-v8a, x86_64).

#### Scenario: arm64-v8a library bundled
- **WHEN** the library is built for `aarch64-linux-android`
- **THEN** `build/outputs/aar/remote-media-android-release.aar` contains `jni/arm64-v8a/libremotemedia_android_inprocess.so`

#### Scenario: x86_64 library bundled
- **WHEN** the library is built for `x86_64-linux-android`
- **THEN** the AAR contains `jni/x86_64/libremotemedia_android_inprocess.so`

### Requirement: Python shared library bundled when available
The build system SHALL copy `libpython3.11.so` (or configured version) from a configured source directory into the AAR's `jni/<abi>/` if present.

#### Scenario: Python library included when built
- **WHEN** `../python-libs/<abi>/libpython3.11.so` exists at build time
- **THEN** the AAR includes `jni/<abi>/libpython3.11.so`

#### Scenario: Build succeeds without Python library
- **WHEN** the Python library source directory is missing
- **THEN** the build completes without error and the AAR contains only the RemoteMedia native library

### Requirement: CMake integration exposed for consumers who need customization
The library SHALL provide a CMake config/module that consumers can `find_package()` to access the native library paths for custom native code linking.

#### Scenario: Consumer uses native library in their own CMake
- **WHEN** a consumer adds `find_package(remotemedia_android REQUIRED)` in their CMakeLists.txt
- **THEN** `${REMOTEMEDIA_ANDROID_LIBRARIES}` provides the imported target for linking

### Requirement: Gradle plugin (optional) for zero-config native setup
A Gradle convention plugin SHALL be provided that automatically configures `jniLibs`, NDK, and CMake for projects consuming the library, requiring only `id("com.remotemedia.android")` in the consumer's build.gradle.kts.

#### Scenario: Consumer applies plugin and builds without manual jniLibs
- **WHEN** a consumer project applies the plugin and adds the library dependency
- **THEN** the project builds and runs without any manual `jniLibs.srcDirs` or CMake configuration