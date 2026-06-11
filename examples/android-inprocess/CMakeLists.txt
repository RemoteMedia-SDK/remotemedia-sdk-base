cmake_minimum_required(VERSION 3.22.1)
project(remotemedia_android LANGUAGES CXX)

# Android toolchain (provided by AGP)
# This file is invoked by externalNativeBuild in build.gradle.kts

# Rust library target directory
# The Rust cdylib is built by Cargo and placed in:
# ../target/aarch64-linux-android/release/libremotemedia_android_inprocess.so
# (or x86_64-linux-android for x86_64)

# We define imported targets for the Rust library and copy them to jniLibs

set(RUST_CRATE_NAME "remotemedia_android_inprocess")
set(RUST_TARGET_DIR "${CMAKE_SOURCE_DIR}/target")

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
add_custom_target(remotemedia_android DEPENDS copy_rust_libs)

# Copy SPROOT glibc libraries from prebuilt location
# These are glibc (aarch64-unknown-linux-gnu) libraries, not Android-targeted
function(add_glibc_libraries arch)
    set(jni_libs_dir "${CMAKE_SOURCE_DIR}/src/main/jniLibs/${arch}")
    set(glibc_built_dir "${CMAKE_SOURCE_DIR}/../../../../remotemedia-scaling/target/aarch64-unknown-linux-gnu/release")

    # Copy runner binary
    set(runner_src "${glibc_built_dir}/remotemedia-sproot-runner")
    set(runner_dst "${jni_libs_dir}/libremotemedia_sproot_runner.so")
    if(EXISTS ${runner_src})
        add_custom_command(
            OUTPUT ${runner_dst}
            COMMAND ${CMAKE_COMMAND} -E make_directory ${jni_libs_dir}
            COMMAND ${CMAKE_COMMAND} -E copy_if_different ${runner_src} ${runner_dst}
            DEPENDS ${runner_src}
            COMMENT "Copying SPROOT runner for ${arch}"
        )
        add_custom_target(copy_runner_${arch} DEPENDS ${runner_dst})
        add_dependencies(copy_rust_libs copy_runner_${arch})
    else()
        message(WARNING "SPROOT runner not found: ${runner_src}")
    endif()

    # Source is always arm64-v8a for arm64, x86_64 for x86_64
    if(arch STREQUAL "aarch64-v8a")
        set(glibc_src_arch "arm64-v8a")
    elseif(arch STREQUAL "x86_64")
        set(glibc_src_arch "x86_64")
    else()
        set(glibc_src_arch ${arch})
    endif()
    set(glibc_libs_dir "${CMAKE_SOURCE_DIR}/../../../remotemedia-sdk-base/kotlin-android/app/src/main/jniLibs/${glibc_src_arch}")

    # List of glibc libraries to copy
    set(GLIBC_LIBS
        libglibc_ld.so
        libglibc_libc.so
        libglibc_libm.so
        libglibc_libdl.so
        libglibc_libpthread.so
        libglibc_libresolv.so
        libglibc_libutil.so
        libglibc_librt.so
        libglibc_libnsl.so
        libglibc_libgcc_s.so
        libglibc_libstdcxx.so
        libglibc_libz.so
        libglibc_libffi.so
        libglibc_libssl.so
        libglibc_libcrypto.so
        libglibc_libpython3_11.so
    )

    foreach(lib ${GLIBC_LIBS})
        set(src "${glibc_libs_dir}/${lib}")
        set(dst "${jni_libs_dir}/${lib}")
        if(EXISTS ${src})
            add_custom_command(
                OUTPUT ${dst}
                COMMAND ${CMAKE_COMMAND} -E make_directory ${jni_libs_dir}
                COMMAND ${CMAKE_COMMAND} -E copy_if_different ${src} ${dst}
                DEPENDS ${src}
                COMMENT "Copying glibc ${lib} for ${arch}"
            )
            add_custom_target(copy_glibc_${lib}_${arch} DEPENDS ${dst})
            add_dependencies(copy_rust_libs copy_glibc_${lib}_${arch})
        else()
            message(WARNING "glibc library not found: ${src}")
        endif()
    endforeach()
endfunction()

add_glibc_libraries("aarch64-v8a")
add_glibc_libraries("x86_64")