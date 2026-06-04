// Build script for gRPC code generation

#[cfg(feature = "grpc-signaling")]
fn main() {
    // Use vendored protoc from protobuf-src
    std::env::set_var("PROTOC", protobuf_src::protoc());

    compile_protos();
    stage_llama_libs();
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=../../../proto/");
}

#[cfg(not(feature = "grpc-signaling"))]
fn main() {
    stage_llama_libs();
    println!("cargo:rerun-if-changed=build.rs");
}

// Work around llama-cpp-sys-4 bug: its post-build only globs `*.so` and
// stages just the unsuffixed symlink, leaving the SONAME chain
// (`lib*.so.0`, `lib*.so.0.X.Y`) only in `target/llama-cmake-cache/<hash>/lib/`.
// At runtime, the binary's NEEDED entry is the SONAME (`libggml-base.so.0`),
// not the unsuffixed name, so it fails to load.
//
// We mirror the full chain into `target/<profile>/`, `examples/`, and
// `deps/` after the upstream build runs. Linux only — Windows uses
// flat `.dll`s and macOS uses `.dylib`/install-name + already-correct rpaths.
#[cfg(not(target_os = "linux"))]
fn stage_llama_libs() {}

#[cfg(target_os = "linux")]
fn stage_llama_libs() {
    use std::path::PathBuf;

    let out_dir = match std::env::var_os("OUT_DIR") {
        Some(d) => PathBuf::from(d),
        None => return,
    };
    // OUT_DIR = target/<profile>/build/<crate>-<hash>/out — climb to target/<profile>/.
    let target_profile = match out_dir.ancestors().nth(3) {
        Some(p) => p.to_path_buf(),
        None => return,
    };
    let target_root = match target_profile.parent() {
        Some(p) => p.to_path_buf(),
        None => return,
    };
    let cache_root = target_root.join("llama-cmake-cache");
    if !cache_root.is_dir() {
        return;
    }

    // Pick the most recently modified llama-cmake-cache/<hash>/lib dir.
    let cache_lib = std::fs::read_dir(&cache_root)
        .ok()
        .into_iter()
        .flatten()
        .filter_map(|e| e.ok())
        .map(|e| e.path().join("lib"))
        .filter(|p| p.is_dir())
        .max_by_key(|p| {
            std::fs::metadata(p)
                .and_then(|m| m.modified())
                .unwrap_or(std::time::SystemTime::UNIX_EPOCH)
        });
    let cache_lib = match cache_lib {
        Some(p) => p,
        None => return,
    };

    let dests = [
        target_profile.clone(),
        target_profile.join("examples"),
        target_profile.join("deps"),
    ];
    for dest in &dests {
        let _ = std::fs::create_dir_all(dest);
    }

    let entries = match std::fs::read_dir(&cache_lib) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.filter_map(|e| e.ok()) {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if !name_str.starts_with("lib") || !name_str.contains(".so") {
            continue;
        }
        let src = entry.path();
        let smeta = match src.symlink_metadata() {
            Ok(m) => m,
            Err(_) => continue,
        };
        for dest in &dests {
            let dst = dest.join(&name);
            // Refresh whenever the cache version is newer than the staged copy.
            if let Ok(dst_meta) = dst.symlink_metadata() {
                let src_t = smeta.modified().ok();
                let dst_t = dst_meta.modified().ok();
                if let (Some(s), Some(d)) = (src_t, dst_t) {
                    if d >= s {
                        continue;
                    }
                }
                let _ = std::fs::remove_file(&dst);
            }
            if smeta.file_type().is_symlink() {
                if let Ok(target) = std::fs::read_link(&src) {
                    let _ = std::os::unix::fs::symlink(&target, &dst);
                }
            } else {
                if std::fs::hard_link(&src, &dst).is_err() {
                    let _ = std::fs::copy(&src, &dst);
                }
            }
        }
    }
    println!("cargo:rerun-if-changed={}", cache_lib.display());
}

#[cfg(feature = "grpc-signaling")]
fn compile_protos() {
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .out_dir("src/generated")
        .compile_protos(
            &[
                "../../../proto/webrtc_signaling.proto",
                "../../../proto/common.proto",
                "../../../proto/control.proto",
            ],
            &["../../../proto/"],
        )
        .unwrap_or_else(|e| panic!("Failed to compile protos: {}", e));

    println!("cargo:warning=Successfully compiled protocol buffers");
}
