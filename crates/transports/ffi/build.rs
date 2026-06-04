//! Build script for remotemedia-ffi
//!
//! Conditionally runs napi-build when the napi feature is enabled,
//! and propagates static linking flags for FFmpeg / external codecs
//! when FFMPEG_LIBS_MODE is set to static.

use std::env;

fn main() {
    #[cfg(feature = "napi")]
    {
        napi_build::setup();
    }

    // Propagate static linking instructions for FFmpeg if FFMPEG_LIBS_MODE is static
    if env::var("FFMPEG_LIBS_MODE")
        .map(|v| v == "static")
        .unwrap_or(false)
    {
        println!("cargo:rustc-link-arg=-lz");
        println!("cargo:rustc-link-arg=-llzma");
        println!("cargo:rustc-link-arg=-lbz2");

        // Additional system paths and libraries for Linux
        let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
        if target_os == "linux" {
            println!("cargo:rustc-link-arg=-lX11");
            println!("cargo:rustc-link-arg=-lvdpau");
        }

        let target_env = env::var("CARGO_CFG_TARGET_ENV").unwrap_or_default();
        if target_env == "msvc" {
            println!("cargo:rustc-link-arg=-llibssl");
            println!("cargo:rustc-link-arg=-llibcrypto");
        } else {
            println!("cargo:rustc-link-arg=-lssl");
            println!("cargo:rustc-link-arg=-lcrypto");
        }

        if env::var("FFMPEG_LIBVPX").map(|v| v == "1").unwrap_or(false) {
            println!("cargo:rustc-link-lib=static:+whole-archive=vpx");
            println!("cargo:rustc-link-arg=-lm");
            println!("cargo:rustc-link-arg=-lpthread");
            if let Ok(dir) = env::var("FFMPEG_LIBVPX_LIB_DIR") {
                println!("cargo:rustc-link-search=native={}", dir);
            }
        }
        if env::var("FFMPEG_LIBX264")
            .map(|v| v == "1")
            .unwrap_or(false)
        {
            println!("cargo:rustc-link-lib=static:+whole-archive=x264");
            if let Ok(dir) = env::var("FFMPEG_LIBX264_LIB_DIR") {
                println!("cargo:rustc-link-search=native={}", dir);
            }
        }
        if env::var("FFMPEG_LIBAOM").map(|v| v == "1").unwrap_or(false) {
            println!("cargo:rustc-link-lib=static:+whole-archive=aom");
            if let Ok(dir) = env::var("FFMPEG_LIBAOM_LIB_DIR") {
                println!("cargo:rustc-link-search=native={}", dir);
            }
        }

        // Re-run build script if any FFmpeg build/link env vars change
        println!("cargo:rerun-if-env-changed=FFMPEG_LIBS_MODE");
        println!("cargo:rerun-if-env-changed=FFMPEG_LIBVPX");
        println!("cargo:rerun-if-env-changed=FFMPEG_LIBX264");
        println!("cargo:rerun-if-env-changed=FFMPEG_LIBAOM");
        println!("cargo:rerun-if-env-changed=FFMPEG_LIBVPX_LIB_DIR");
        println!("cargo:rerun-if-env-changed=FFMPEG_LIBX264_LIB_DIR");
        println!("cargo:rerun-if-env-changed=FFMPEG_LIBAOM_LIB_DIR");
    }
}
