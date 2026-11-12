//! With the `kokoro` feature: builds Abseil with CMake and compiles
//! LiteRT-LM's English grapheme-to-phoneme front end (`cpp/`) against it.

use std::path::{Path, PathBuf};
use std::process::Command;

const ABSEIL_VERSION: &str = "20260526.0";

fn main() {
    println!("cargo:rerun-if-changed=cpp");
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=ABSEIL_SOURCE_DIR");
    if std::env::var_os("CARGO_FEATURE_KOKORO").is_none() {
        return;
    }
    let target = std::env::var("TARGET").unwrap();
    let android = target.contains("android");
    let msvc = target.contains("msvc");

    let source = abseil_source();
    let mut absl = cmake::Config::new(&source);
    absl.profile("Release")
        .generator("Ninja")
        .define("ABSL_PROPAGATE_CXX_STD", "ON")
        .define("CMAKE_CXX_STANDARD", "17")
        .define("ABSL_BUILD_TESTING", "OFF")
        .define("BUILD_TESTING", "OFF")
        .define("ABSL_ENABLE_INSTALL", "ON")
        .define("CMAKE_POSITION_INDEPENDENT_CODE", "ON");
    if android {
        let ndk = std::env::var("ANDROID_NDK_HOME")
            .or_else(|_| std::env::var("ANDROID_NDK_ROOT"))
            .expect("ANDROID_NDK_HOME (the NDK) for an Android build");
        absl.define(
            "CMAKE_TOOLCHAIN_FILE",
            Path::new(&ndk).join("build/cmake/android.toolchain.cmake"),
        )
        .define("ANDROID_ABI", "arm64-v8a")
        .define("ANDROID_PLATFORM", "android-28")
        .define("ANDROID_STL", "c++_shared");
    }
    if msvc {
        // Rust links the release DLL CRT (/MD). Name the compiler: with Ninja,
        // CMake would take the first C++ compiler on PATH, which may be a
        // clang that does not accept MSVC flags.
        absl.define("CMAKE_MSVC_RUNTIME_LIBRARY", "MultiThreadedDLL");
        if let Some(cl) = cc::windows_registry::find_tool(&target, "cl.exe") {
            let bin = cl.path().parent().unwrap().to_path_buf();
            absl.define("CMAKE_C_COMPILER", cl.path())
                .define("CMAKE_CXX_COMPILER", cl.path())
                .define("CMAKE_LINKER", bin.join("link.exe"))
                .define("CMAKE_AR", bin.join("lib.exe"));
            for (key, value) in cl.env() {
                absl.env(key, value);
            }
            // Other toolchains on PATH (MinGW, Strawberry Perl, conda's
            // clang) shadow link.exe / rc.exe.
            let path = std::env::var_os("PATH").unwrap_or_default();
            let kept: Vec<PathBuf> = std::env::split_paths(&path)
                .filter(|p| {
                    let p = p.to_string_lossy().to_ascii_lowercase();
                    !(p.contains("strawberry")
                        || p.contains("mingw")
                        || p.contains("usr\\bin")
                        || p.ends_with("library\\bin"))
                })
                .collect();
            let mut joined = std::env::join_paths(std::iter::once(bin).chain(kept)).unwrap();
            if let Some((_, tool_path)) = cl.env().iter().find(|(k, _)| k.eq_ignore_ascii_case("PATH")) {
                let mut p = tool_path.clone();
                p.push(";");
                p.push(&joined);
                joined = p;
            }
            absl.env("PATH", joined);
        }
    }
    let out = absl.build();

    let mut g2p = cc::Build::new();
    g2p.cpp(true)
        .std("c++17")
        .include("cpp")
        .include(out.join("include"))
        .files([
            "cpp/g2p_capi.cc",
            "cpp/omni/text2speech/kokoro/english_g2p.cc",
            "cpp/omni/text2speech/kokoro/cjk_blob.cc",
            "cpp/omni/text2speech/kokoro/unicode_utils.cc",
        ])
        .warnings(false)
        .opt_level(2);
    if msvc {
        // The sources carry IPA in UTF-8 string literals.
        g2p.flag("/utf-8").flag("/EHsc").define("NOMINMAX", None);
    }
    if android {
        g2p.cpp_link_stdlib("c++_shared");
    }
    g2p.compile("rm_kokoro_g2p");

    // Every Abseil archive; lld and link.exe resolve them in any order.
    let lib = out.join("lib");
    println!("cargo:rustc-link-search=native={}", lib.display());
    let mut names: Vec<String> = std::fs::read_dir(&lib)
        .expect("abseil lib dir")
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            let name = e.file_name().to_string_lossy().into_owned();
            let stem = name
                .strip_suffix(".lib")
                .map(str::to_string)
                .or_else(|| {
                    name.strip_prefix("lib")
                        .and_then(|n| n.strip_suffix(".a"))
                        .map(str::to_string)
                })?;
            stem.starts_with("absl_").then_some(stem)
        })
        .collect();
    names.sort();
    for name in names {
        println!("cargo:rustc-link-lib=static={name}");
    }
    if msvc {
        // Abseil's time zone and random code.
        println!("cargo:rustc-link-lib=advapi32");
        println!("cargo:rustc-link-lib=bcrypt");
    }
}

/// Abseil's source: `ABSEIL_SOURCE_DIR`, or the release tarball unpacked under
/// `cpp/third_party` (downloaded the first time).
fn abseil_source() -> PathBuf {
    if let Ok(dir) = std::env::var("ABSEIL_SOURCE_DIR") {
        return PathBuf::from(dir);
    }
    let manifest = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    let third_party = manifest.join("cpp").join("third_party");
    let dir = third_party.join(format!("abseil-cpp-{ABSEIL_VERSION}"));
    if dir.join("CMakeLists.txt").exists() {
        return dir;
    }
    std::fs::create_dir_all(&third_party).unwrap();
    let tarball = third_party.join(format!("abseil-cpp-{ABSEIL_VERSION}.tar.gz"));
    let url = format!("https://github.com/abseil/abseil-cpp/archive/{ABSEIL_VERSION}.tar.gz");
    let ok = Command::new("curl")
        .args(["-sSfL", "-o"])
        .arg(&tarball)
        .arg(&url)
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    assert!(
        ok,
        "could not download {url}; set ABSEIL_SOURCE_DIR to an Abseil {ABSEIL_VERSION} checkout"
    );
    let ok = Command::new("tar")
        .arg("-xzf")
        .arg(&tarball)
        .arg("-C")
        .arg(&third_party)
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    assert!(ok, "could not unpack {}", tarball.display());
    let _ = std::fs::remove_file(&tarball);
    dir
}
