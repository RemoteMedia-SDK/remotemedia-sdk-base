// Build script for core
// - Auto-downloads FFmpeg libraries for ac-ffmpeg when video feature is enabled
// - Auto-downloads speaker diarization ONNX models when speaker-diarization feature is enabled
// - Sets FFMPEG_INCLUDE_DIR and FFMPEG_LIB_DIR environment variables

use std::env;
use std::fs;
use std::path::PathBuf;

fn main() {
    // Only setup FFmpeg if the video feature is enabled
    #[cfg(feature = "video")]
    setup_ffmpeg();

    // Emit uv metadata when bundled-uv feature is enabled
    #[cfg(feature = "bundled-uv")]
    emit_uv_metadata();

    println!("cargo:rerun-if-changed=build.rs");
}

/// Emit compile-time metadata for the bundled uv binary.
///
/// Sets UV_VERSION and UV_CHECKSUM env vars so the runtime can verify
/// downloaded binaries against a build-pinned hash. Also wires
/// UV_BINARY_PATH for air-gapped envs (lets users bypass the download).
///
/// Checksums sourced from the per-asset `.sha256` files published with
/// each uv release, e.g.:
///
/// ```text
/// curl -fsSL https://github.com/astral-sh/uv/releases/download/0.6.14/uv-x86_64-unknown-linux-gnu.tar.gz.sha256
/// ```
///
/// Bumping `uv_version`: re-fetch all six sha256 lines below from the
/// new release tag. The full one-shot helper is on the release notes
/// page (search "Download uv" — the table links each `.sha256` file).
#[cfg(feature = "bundled-uv")]
fn emit_uv_metadata() {
    let uv_version = "0.6.14";
    println!("cargo:rustc-env=UV_VERSION={}", uv_version);

    // Per-platform SHA256 checksums for uv 0.6.14 release artifacts.
    // Pinned per (arch, os, libc) — gnu and musl share linux substrings
    // in the target triple, so match on the full distinguishing suffix
    // rather than `contains("linux")` alone. Unknown targets emit an
    // empty checksum, which makes the runtime download path
    // (`download_uv`) refuse to write the binary — safer than letting
    // an unverified blob land on disk.
    let target = env::var("TARGET").unwrap_or_default();
    let checksum = match target.as_str() {
        "x86_64-unknown-linux-gnu" => {
            "0aaf451c391d3913823bfb8ed354b446dcfd0553a32ed8266611e4181c61fd51"
        }
        "aarch64-unknown-linux-gnu" => {
            "ea25597354af186bdd55aee0de431e16d45d82951a4f41f065a8e4dc27885265"
        }
        "x86_64-apple-darwin" => "1d8ecb2eb3b68fb50e4249dc96ac9d2458dc24068848f04f4c5b42af2fd26552",
        "aarch64-apple-darwin" => {
            "4ea4731010fbd1bc8e790e07f199f55a5c7c2c732e9b77f85e302b0bee61b756"
        }
        "x86_64-pc-windows-msvc" => {
            "93b29fc234758e381df461d7638ff73d0f08bdf3a0dc37923b1ee0b9e442ca3f"
        }
        // musl + 32-bit + non-x86 archs not currently pinned; add as
        // demand surfaces. Returning "" prevents the runtime download
        // from writing an unverified binary.
        _ => "",
    };
    println!("cargo:rustc-env=UV_CHECKSUM={}", checksum);

    // Allow skipping download for air-gapped environments
    println!("cargo:rerun-if-env-changed=UV_BINARY_PATH");
}

#[cfg(feature = "video")]
fn setup_ffmpeg() {
    // Check if static linking mode - need extra dependencies
    if env::var("FFMPEG_LIBS_MODE")
        .map(|v| v == "static")
        .unwrap_or(false)
    {
        // Static FFmpeg requires zlib, lzma, and other compression libraries
        println!("cargo:rustc-link-lib=z");
        println!("cargo:rustc-link-lib=lzma");
        println!("cargo:rustc-link-lib=bz2");
        // OpenSSL for RTMPS/HTTPS network protocols.
        // Windows MSVC openssl-src / conda-forge ship `libssl.lib` /
        // `libcrypto.lib`; non-MSVC toolchains use the unprefixed names.
        #[cfg(target_env = "msvc")]
        {
            println!("cargo:rustc-link-lib=libssl");
            println!("cargo:rustc-link-lib=libcrypto");
        }
        #[cfg(not(target_env = "msvc"))]
        {
            println!("cargo:rustc-link-lib=ssl");
            println!("cargo:rustc-link-lib=crypto");
        }
        // Additional network protocol dependencies
        #[cfg(target_os = "linux")]
        {
            // GnuTLS alternative (if FFmpeg was built with GnuTLS instead of OpenSSL)
            // Uncomment if needed:
            // println!("cargo:rustc-link-lib=gnutls");

            // librtmp for native RTMP support (if FFmpeg was built with librtmp)
            // Uncomment if needed:
            // println!("cargo:rustc-link-lib=rtmp");
        }

        // External codec libs that FFmpeg's configure was opted into via
        // setup-ffmpeg.sh's pkg-config probes. `ac-ffmpeg`'s build.rs
        // only emits `-lavcodec/avformat/avutil/swresample/swscale` — it
        // doesn't introspect pkg-config Libs.private — so any codec
        // FFmpeg static-linked against must be re-declared here for the
        // downstream Rust link step.
        //
        // Each is gated on env vars set by setup-ffmpeg.sh:
        //   FFMPEG_LIBVPX=1   ← when scripts/install-libvpx.sh produced vendor/libvpx
        //   FFMPEG_LIBX264=1  ← when system libx264-dev was apt-installed
        //   FFMPEG_LIBAOM=1   ← when system libaom-dev was apt-installed
        // Absence of the env var = absence of the codec at FFmpeg
        // configure time = no `-l` emitted = link succeeds without it.
        if env::var("FFMPEG_LIBVPX").map(|v| v == "1").unwrap_or(false) {
            println!("cargo:rustc-link-lib=static=vpx");
            println!("cargo:rustc-link-lib=m");
            println!("cargo:rustc-link-lib=pthread");
            // libvpx lives in the project-vendored vendor/libvpx/lib
            // (built by scripts/install-libvpx.sh), which isn't on the
            // linker's default search path. install-libvpx.sh writes
            // LIBRARY_PATH to .cargo/config.toml but rust-lld doesn't
            // honor LIBRARY_PATH the way gcc does
            // (rust-lang/rust#52746) — explicit rustc-link-search is
            // the only reliable route. setup-ffmpeg.sh writes the
            // absolute path to FFMPEG_LIBVPX_LIB_DIR.
            if let Ok(dir) = env::var("FFMPEG_LIBVPX_LIB_DIR") {
                println!("cargo:rustc-link-search=native={}", dir);
            }
        }
        if env::var("FFMPEG_LIBX264")
            .map(|v| v == "1")
            .unwrap_or(false)
        {
            println!("cargo:rustc-link-lib=static=x264");
            // Vendored libx264 lives at vendor/x264/lib (built by
            // setup-ffmpeg.sh), which isn't on the linker's default
            // search path. setup-ffmpeg.sh writes the absolute path
            // to FFMPEG_LIBX264_LIB_DIR.
            if let Ok(dir) = env::var("FFMPEG_LIBX264_LIB_DIR") {
                println!("cargo:rustc-link-search=native={}", dir);
            }
        }
        if env::var("FFMPEG_LIBAOM").map(|v| v == "1").unwrap_or(false) {
            println!("cargo:rustc-link-lib=static=aom");
            // Vendored libaom lives at vendor/aom/lib (built by
            // setup-ffmpeg.sh). setup-ffmpeg.sh writes the absolute
            // path to FFMPEG_LIBAOM_LIB_DIR.
            if let Ok(dir) = env::var("FFMPEG_LIBAOM_LIB_DIR") {
                println!("cargo:rustc-link-search=native={}", dir);
            }
        }
        // Re-run if any of the codec gate vars change.
        println!("cargo:rerun-if-env-changed=FFMPEG_LIBVPX");
        println!("cargo:rerun-if-env-changed=FFMPEG_LIBX264");
        println!("cargo:rerun-if-env-changed=FFMPEG_LIBAOM");
        println!("cargo:rerun-if-env-changed=FFMPEG_LIBVPX_LIB_DIR");
        println!("cargo:rerun-if-env-changed=FFMPEG_LIBX264_LIB_DIR");
        println!("cargo:rerun-if-env-changed=FFMPEG_LIBAOM_LIB_DIR");
    }

    // Check if user already has FFMPEG_INCLUDE_DIR set
    if env::var("FFMPEG_INCLUDE_DIR").is_ok() {
        println!("cargo:warning=Using existing FFMPEG_INCLUDE_DIR from environment");
        return;
    }

    let target = env::var("TARGET").unwrap();
    let out_dir = env::var("OUT_DIR").unwrap();
    let ffmpeg_dir = PathBuf::from(&out_dir).join("ffmpeg");

    // Create ffmpeg directory if it doesn't exist
    fs::create_dir_all(&ffmpeg_dir).expect("Failed to create ffmpeg directory");

    // Download and extract FFmpeg based on target platform
    let (include_dir, lib_dir) = match target.as_str() {
        t if t.contains("linux") => download_ffmpeg_linux(&ffmpeg_dir),
        t if t.contains("darwin") || t.contains("macos") => download_ffmpeg_macos(&ffmpeg_dir),
        t if t.contains("windows") => download_ffmpeg_windows(&ffmpeg_dir),
        _ => {
            println!("cargo:warning=Unsupported platform for auto-download: {}. Please set FFMPEG_INCLUDE_DIR and FFMPEG_LIB_DIR manually.", target);
            return;
        }
    };

    // Set environment variables for ac-ffmpeg
    println!(
        "cargo:rustc-env=FFMPEG_INCLUDE_DIR={}",
        include_dir.display()
    );
    println!("cargo:rustc-env=FFMPEG_LIB_DIR={}", lib_dir.display());

    // Also set them for the current build
    env::set_var("FFMPEG_INCLUDE_DIR", &include_dir);
    env::set_var("FFMPEG_LIB_DIR", &lib_dir);

    println!(
        "cargo:warning=FFmpeg auto-configured: include={}, lib={}",
        include_dir.display(),
        lib_dir.display()
    );
}

#[cfg(feature = "video")]
fn download_ffmpeg_linux(ffmpeg_dir: &PathBuf) -> (PathBuf, PathBuf) {
    use std::process::Command;

    let include_dir = ffmpeg_dir.join("include");
    let lib_dir = ffmpeg_dir.join("lib");

    // Check if already downloaded
    if include_dir.exists() && lib_dir.exists() {
        println!(
            "cargo:warning=Using cached FFmpeg from {}",
            ffmpeg_dir.display()
        );
        return (include_dir, lib_dir);
    }

    println!("cargo:warning=Auto-downloading FFmpeg for Linux...");

    // Try to use system package manager to install development files
    // This is a build-time dependency, so we can use system packages
    let status = Command::new("sh")
        .arg("-c")
        .arg("command -v pkg-config")
        .status();

    if status.is_ok() && status.unwrap().success() {
        // Check if FFmpeg is already installed via pkg-config
        let pc_status = Command::new("pkg-config")
            .args(&["--exists", "libavcodec", "libavformat", "libavutil"])
            .status();

        if pc_status.is_ok() && pc_status.unwrap().success() {
            // Get paths from pkg-config
            let include_output = Command::new("pkg-config")
                .args(&["--variable=includedir", "libavcodec"])
                .output()
                .expect("Failed to run pkg-config");

            let lib_output = Command::new("pkg-config")
                .args(&["--variable=libdir", "libavcodec"])
                .output()
                .expect("Failed to run pkg-config");

            let pkg_include = String::from_utf8_lossy(&include_output.stdout)
                .trim()
                .to_string();
            let pkg_lib = String::from_utf8_lossy(&lib_output.stdout)
                .trim()
                .to_string();

            if !pkg_include.is_empty() && !pkg_lib.is_empty() {
                println!("cargo:warning=Found system FFmpeg via pkg-config");
                return (PathBuf::from(pkg_include), PathBuf::from(pkg_lib));
            }
        }
    }

    println!("cargo:warning=System FFmpeg not found. Please install FFmpeg development packages:");
    println!("cargo:warning=  Ubuntu/Debian: sudo apt-get install libavcodec-dev libavformat-dev libavutil-dev libswscale-dev");
    println!("cargo:warning=  Fedora/RHEL: sudo dnf install ffmpeg-devel");
    println!("cargo:warning=  Arch: sudo pacman -S ffmpeg");
    panic!("FFmpeg development libraries not found. Please install them or set FFMPEG_INCLUDE_DIR and FFMPEG_LIB_DIR manually.");
}

#[cfg(feature = "video")]
fn download_ffmpeg_macos(ffmpeg_dir: &PathBuf) -> (PathBuf, PathBuf) {
    use std::process::Command;

    let include_dir = ffmpeg_dir.join("include");
    let lib_dir = ffmpeg_dir.join("lib");

    // Check if already downloaded
    if include_dir.exists() && lib_dir.exists() {
        println!(
            "cargo:warning=Using cached FFmpeg from {}",
            ffmpeg_dir.display()
        );
        return (include_dir, lib_dir);
    }

    println!("cargo:warning=Auto-configuring FFmpeg for macOS...");

    // Try to find FFmpeg via Homebrew
    let brew_prefix_output = Command::new("brew").args(&["--prefix", "ffmpeg"]).output();

    if let Ok(output) = brew_prefix_output {
        if output.status.success() {
            let brew_prefix = String::from_utf8_lossy(&output.stdout).trim().to_string();
            let brew_include = PathBuf::from(&brew_prefix).join("include");
            let brew_lib = PathBuf::from(&brew_prefix).join("lib");

            if brew_include.exists() && brew_lib.exists() {
                println!("cargo:warning=Found FFmpeg via Homebrew at {}", brew_prefix);
                return (brew_include, brew_lib);
            }
        }
    }

    println!("cargo:warning=FFmpeg not found via Homebrew. Please install:");
    println!("cargo:warning=  brew install ffmpeg");
    panic!("FFmpeg not found. Please install via Homebrew or set FFMPEG_INCLUDE_DIR and FFMPEG_LIB_DIR manually.");
}

#[cfg(feature = "video")]
fn download_ffmpeg_windows(ffmpeg_dir: &PathBuf) -> (PathBuf, PathBuf) {
    let include_dir = ffmpeg_dir.join("include");
    let lib_dir = ffmpeg_dir.join("lib");

    // Check if already downloaded
    if include_dir.exists() && lib_dir.exists() {
        println!(
            "cargo:warning=Using cached FFmpeg from {}",
            ffmpeg_dir.display()
        );
        return (include_dir, lib_dir);
    }

    println!("cargo:warning=Auto-downloading FFmpeg for Windows...");

    // Download pre-built FFmpeg from gyan.dev (popular source for Windows FFmpeg builds)
    // Using shared builds (smaller and easier to work with)
    let download_url =
        "https://www.gyan.dev/ffmpeg/builds/ffmpeg-release-essentials.zip".to_string();

    let zip_path = ffmpeg_dir.join("ffmpeg.zip");
    let extract_dir = ffmpeg_dir.join("extracted");

    // Download FFmpeg zip
    println!("cargo:warning=Downloading FFmpeg from {}", download_url);
    let status = std::process::Command::new("powershell")
        .args(&[
            "-Command",
            &format!(
                "Invoke-WebRequest -Uri '{}' -OutFile '{}'",
                download_url,
                zip_path.display()
            ),
        ])
        .status();

    if status.is_err() || !status.unwrap().success() {
        // Try with curl as fallback
        let curl_status = std::process::Command::new("curl")
            .args(&["-L", "-o", zip_path.to_str().unwrap(), &download_url])
            .status();

        if curl_status.is_err() || !curl_status.unwrap().success() {
            panic!(
                "Failed to download FFmpeg. Please download manually from {} and extract to {}",
                download_url,
                ffmpeg_dir.display()
            );
        }
    }

    // Extract zip
    println!("cargo:warning=Extracting FFmpeg...");
    let extract_status = std::process::Command::new("powershell")
        .args(&[
            "-Command",
            &format!(
                "Expand-Archive -Path '{}' -DestinationPath '{}' -Force",
                zip_path.display(),
                extract_dir.display()
            ),
        ])
        .status();

    if extract_status.is_err() || !extract_status.unwrap().success() {
        panic!("Failed to extract FFmpeg zip. Please extract manually.");
    }

    // Find the extracted directory (usually ffmpeg-VERSION-essentials_build)
    let entries = fs::read_dir(&extract_dir).expect("Failed to read extract directory");
    let mut ffmpeg_build_dir = None;
    for entry in entries {
        if let Ok(entry) = entry {
            let path = entry.path();
            if path.is_dir()
                && path
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .starts_with("ffmpeg-")
            {
                ffmpeg_build_dir = Some(path);
                break;
            }
        }
    }

    let ffmpeg_build_dir = ffmpeg_build_dir.expect("Failed to find extracted FFmpeg directory");

    // Copy include and lib directories
    let src_include = ffmpeg_build_dir.join("include");
    let src_lib = ffmpeg_build_dir.join("lib");

    copy_dir_recursive(&src_include, &include_dir).expect("Failed to copy include directory");
    copy_dir_recursive(&src_lib, &lib_dir).expect("Failed to copy lib directory");

    println!("cargo:warning=FFmpeg extracted to {}", ffmpeg_dir.display());

    // Clean up zip file
    let _ = fs::remove_file(&zip_path);

    (include_dir, lib_dir)
}

#[cfg(feature = "video")]
fn copy_dir_recursive(src: &PathBuf, dst: &PathBuf) -> std::io::Result<()> {
    fs::create_dir_all(dst)?;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let ty = entry.file_type()?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());

        if ty.is_dir() {
            copy_dir_recursive(&src_path, &dst_path)?;
        } else {
            fs::copy(&src_path, &dst_path)?;
        }
    }
    Ok(())
}
