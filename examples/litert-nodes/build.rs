fn main() {
    println!("cargo:rerun-if-changed=src/litert_ffi.h");
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=LITERT_LIB_DIR");

    // Add search path for LiteRT/TFLite library
    if let Ok(dir) = std::env::var("LITERT_LIB_DIR") {
        println!("cargo:rustc-link-search=native={}", dir);
    }

    // Link against TFLite/LiteRT library
    // The standard library is typically named "tensorflowlite" or "litert"
    println!("cargo:rustc-link-lib=dylib=tensorflowlite");
    // Also try litert as an alternative
    println!("cargo:rustc-link-lib=dylib=litert");

    // For Android, we might need to link against the NDK's libtensorflowlite.so
    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    if target_os == "android" {
        // On Android, the library is typically provided by the app or system
        println!("cargo:rustc-link-lib=dylib=tensorflowlite_c");
        println!("cargo:rustc-link-lib=dylib=litert");
    }

    // Generate bindings from our header wrapper
    let bindings = bindgen::Builder::default()
        .header("src/litert_ffi.h")
        .allowlist_function("TfLite.*")
        .allowlist_type("TfLite.*")
        .allowlist_var("TfLite.*")
        .allowlist_function("TFLite.*")
        .allowlist_type("TFLite.*")
        .allowlist_var("TFLite.*")
        .allowlist_function("LiteRt.*")
        .allowlist_type("LiteRt.*")
        .allowlist_var("LiteRt.*")
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        .generate()
        .expect("Unable to generate bindings");

    let out_path = std::path::PathBuf::from(std::env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("litert_bindings.rs"))
        .expect("Couldn't write bindings");
}
