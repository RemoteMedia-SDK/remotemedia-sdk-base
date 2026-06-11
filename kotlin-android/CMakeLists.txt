cmake_minimum_required(VERSION 3.22.1)
project(remotemedia_android LANGUAGES CXX)

# We define imported targets for the Rust library and copy them to jniLibs

set(RUST_CRATE_NAME "remotemedia_android_inprocess")
set(RUST_TARGET_DIR "${CMAKE_SOURCE_DIR}/../examples/android-inprocess/target")

# Define copy_rust_libs target FIRST so dependencies can reference it
add_custom_target(copy_rust_libs)

function(add_rust_library arch)
    set(rust_lib_path "${RUST_TARGET_DIR}/${arch}/release/lib${RUST_CRATE_NAME}.so")
    set(jni_libs_dir "${CMAKE_SOURCE_DIR}/src/main/jniLibs/${arch}")

    add_library(${RUST_CRATE_NAME}_${arch} SHARED IMPORTED)
    set_target_properties(${RUST_CRATE_NAME}_${arch} PROPERTIES
        IMPORTED_LOCATION ${rust_lib_path}
    )

    # Copy to jniLibs for APK packaging
    add_custom_command(
        OUTPUT ${jni_libs_dir}/lib${RUST_CRATE_NAME}.so
        COMMAND ${CMAKE_COMMAND} -E make_directory ${jni_libs_dir}
        COMMAND ${CMAKE_COMMAND} -E copy_if_different ${rust_lib_path} ${jni_libs_dir}/lib${RUST_CRATE_NAME}.so
        DEPENDS ${rust_lib_path}
        COMMENT "Copying Rust library for ${arch}"
    )

    add_custom_target(copy_${RUST_CRATE_NAME}_${arch} DEPENDS ${jni_libs_dir}/lib${RUST_CRATE_NAME}.so)
    add_dependencies(copy_rust_libs copy_${RUST_CRATE_NAME}_${arch})
endfunction()

# Add targets for supported architectures
add_rust_library("aarch64-linux-android")
add_rust_library("x86_64-linux-android")

# Main target that Gradle will build
add_custom_target(remotemedia_android DEPENDS copy_rust_libs)