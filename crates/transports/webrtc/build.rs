// Build script for remotemedia-webrtc.
//
// The protobuf bindings in `src/generated/` (used by the `grpc-signaling`
// feature) are committed. A normal build uses them as they are: it does not
// build protoc (protobuf-src), does not run it, and writes nothing into
// `src/`. It only hashes the `.proto` files and warns when they no longer
// match `src/generated/protos.stamp`.
//
// Code generation sits behind the `regen-protos` feature, which pulls in
// protobuf-src (builds protoc from source; needs CMake and a C++ compiler)
// and tonic-prost-build. From the sdk-base workspace root:
//
//   Check that the committed bindings match the .proto files (writes
//   nothing to `src/`, fails the build on any difference; CI-friendly):
//     cargo check -p remotemedia-webrtc --features regen-protos
//
//   Regenerate after editing `proto/*.proto`:
//     REMOTEMEDIA_REGEN_PROTOS=1 cargo check -p remotemedia-webrtc --features regen-protos
//     (PowerShell: $env:REMOTEMEDIA_REGEN_PROTOS=1; cargo check ...)
//
// `crates/transports/grpc` compiles a superset of these protos into its own
// `src/generated/`; regenerate it too.

include!("../../../proto/codegen_support.rs");

const PROTOS: &[&str] = &[
    "../../../proto/webrtc_signaling.proto",
    "../../../proto/common.proto",
    "../../../proto/control.proto",
];

fn main() {
    let crate_dir = std::path::PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").unwrap());
    let generated_dir = crate_dir.join("src").join("generated");

    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=../../../proto/codegen_support.rs");

    #[cfg(not(feature = "regen-protos"))]
    proto_codegen::default_build("remotemedia-webrtc", &crate_dir, &generated_dir, PROTOS);

    #[cfg(feature = "regen-protos")]
    {
        let out_dir = std::path::PathBuf::from(std::env::var_os("OUT_DIR").unwrap());
        proto_codegen::regen_build(
            "remotemedia-webrtc",
            &crate_dir,
            &generated_dir,
            &out_dir,
            PROTOS,
            |fresh_dir| {
                // Vendored protoc, so the output does not depend on a system install.
                std::env::set_var("PROTOC", protobuf_src::protoc());
                tonic_prost_build::configure()
                    .build_server(true)
                    .build_client(true)
                    .out_dir(fresh_dir)
                    .compile_protos(PROTOS, &["../../../proto/"])
                    .unwrap_or_else(|e| panic!("Failed to compile protos: {}", e));
            },
        );
    }

    stage_llama_libs();
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
