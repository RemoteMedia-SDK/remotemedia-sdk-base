cmake_minimum_required(VERSION 3.22.1)
project(remotemedia_android_inprocess LANGUAGES CXX)

# Android toolchain (provided by AGP)
# This file is invoked by externalNativeBuild in build.gradle.kts

# Rust library target directory
# The Rust cdylib is built by Cargo and placed in:
# ../target/aarch64-linux-android/release/libremotemedia_android_inprocess.so
# (or x86_64-linux-android for x86_64)

# We define imported targets for the Rust library and copy them to jniLibs

set(RUST_CRATE_NAME "remotemedia_android_inprocess")
set(RUST_TARGET_DIR "${CMAKE_SOURCE_DIR}/../target")

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

# Also copy python shared library if available
# This should be provided by python-for-android or built separately
function(add_python_library arch)
    set(python_lib_name "libpython3.11.so")
    set(python_lib_path "${CMAKE_SOURCE_DIR}/../python-libs/${arch}/${python_lib_name}")
    set(jni_libs_dir "${CMAKE_SOURCE_DIR}/src/main/jniLibs/${arch}")
    
    if(EXISTS ${python_lib_path})
        add_custom_command(
            OUTPUT ${jni_libs_dir}/${python_lib_name}
            COMMAND ${CMAKE_COMMAND} -E make_directory ${jni_libs_dir}
            COMMAND ${CMAKE_COMMAND} -E copy_if_different ${python_lib_path} ${jni_libs_dir}/${python_lib_name}
            DEPENDS ${python_lib_path}
            COMMENT "Copying Python library for ${arch}"
        )
        add_custom_target(copy_python_${arch} DEPENDS ${jni_libs_dir}/${python_lib_name})
        add_dependencies(copy_rust_libs copy_python_${arch})
    endif()
endfunction()

add_python_library("aarch64-linux-android")
add_python_library("x86_64-linux-android")

# Main target that Gradle will build
add_custom_target(remotemedia_android_inprocess DEPENDS copy_rust_libs)