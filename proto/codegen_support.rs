// Shared helpers for the committed protobuf bindings of
// `crates/transports/grpc` and `crates/transports/webrtc`.
//
// This file is not a module of any crate. It is pulled in with `include!`
// by each crate's `build.rs` and by the crate's proto stamp unit test, so it
// may only use `std`.
//
// The bindings in `<crate>/src/generated/` are committed, so a normal build
// never runs protoc. Next to them sits `protos.stamp`: one FNV-1a hash per
// input `.proto` file, written whenever the bindings are regenerated. A
// cheap stamp comparison (no protoc) detects a `.proto` edit that was not
// followed by a regeneration; the full check under the `regen-protos`
// feature regenerates into `OUT_DIR` and compares byte for byte.
//
// Line endings are normalized (CRLF -> LF) before hashing or comparing, so a
// checkout with `core.autocrlf=true` compares equal to the LF-only index.

#[allow(dead_code)]
mod proto_codegen {
    use std::path::Path;

    /// File name of the stamp kept next to the generated bindings.
    pub const STAMP_FILE: &str = "protos.stamp";

    /// Environment variable that lets the `regen-protos` build write the
    /// regenerated bindings into `src/generated/` instead of only checking.
    pub const REGEN_ENV: &str = "REMOTEMEDIA_REGEN_PROTOS";

    /// Copy of `bytes` with every CRLF turned into LF.
    pub fn normalize_newlines(bytes: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(bytes.len());
        let mut i = 0;
        while i < bytes.len() {
            if bytes[i] == b'\r' && bytes.get(i + 1) == Some(&b'\n') {
                i += 1;
                continue;
            }
            out.push(bytes[i]);
            i += 1;
        }
        out
    }

    /// 64-bit FNV-1a. Stable across platforms and Rust versions, unlike
    /// `std::hash`.
    pub fn fnv1a64(bytes: &[u8]) -> u64 {
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        for &b in bytes {
            hash ^= u64::from(b);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        hash
    }

    /// Stamp text for `protos` (paths as passed to protoc, relative to the
    /// crate directory `crate_dir`): one `<hash>  <file name>` line each.
    pub fn render_stamp(crate_dir: &Path, protos: &[&str]) -> std::io::Result<String> {
        let mut out = String::from(
            "# Hashes (FNV-1a 64, LF-normalized) of the .proto files these bindings were\n\
             # generated from. Written by the `regen-protos` build; do not edit by hand.\n",
        );
        for proto in protos {
            let path = crate_dir.join(proto);
            let bytes = std::fs::read(&path).map_err(|e| {
                std::io::Error::new(e.kind(), format!("{}: {e}", path.display()))
            })?;
            let name = Path::new(proto)
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_else(|| proto.to_string());
            out.push_str(&format!(
                "{:016x}  {name}\n",
                fnv1a64(&normalize_newlines(&bytes))
            ));
        }
        Ok(out)
    }

    /// `None` when the committed stamp in `generated_dir` matches the
    /// current `.proto` files, otherwise a description of the mismatch.
    pub fn stamp_mismatch(
        crate_dir: &Path,
        generated_dir: &Path,
        protos: &[&str],
    ) -> Option<String> {
        let expected = match render_stamp(crate_dir, protos) {
            Ok(s) => s,
            Err(e) => return Some(format!("cannot hash .proto files: {e}")),
        };
        let stamp_path = generated_dir.join(STAMP_FILE);
        let committed = match std::fs::read(&stamp_path) {
            Ok(b) => String::from_utf8_lossy(&normalize_newlines(&b)).into_owned(),
            Err(e) => return Some(format!("cannot read {}: {e}", stamp_path.display())),
        };
        if committed == expected {
            return None;
        }
        let changed: Vec<&str> = expected
            .lines()
            .filter(|l| !l.starts_with('#') && !committed.lines().any(|c| c == *l))
            .filter_map(|l| l.split_whitespace().nth(1))
            .collect();
        Some(format!(
            "{} is out of date; changed or new .proto inputs: [{}]",
            stamp_path.display(),
            changed.join(", ")
        ))
    }

    /// Outcome of comparing freshly generated bindings with the committed
    /// ones.
    #[derive(Default)]
    pub struct Drift {
        /// File present in both, content differs (with first differing line).
        pub changed: Vec<(String, String)>,
        /// Generated now, not committed.
        pub missing: Vec<String>,
        /// Committed, no longer generated.
        pub extra: Vec<String>,
    }

    impl Drift {
        pub fn is_empty(&self) -> bool {
            self.changed.is_empty() && self.missing.is_empty() && self.extra.is_empty()
        }

        pub fn describe(&self) -> String {
            let mut s = String::new();
            for (f, detail) in &self.changed {
                s.push_str(&format!("  changed: {f} ({detail})\n"));
            }
            for f in &self.missing {
                s.push_str(&format!("  missing: {f} (generated, not committed)\n"));
            }
            for f in &self.extra {
                s.push_str(&format!("  extra:   {f} (committed, no longer generated)\n"));
            }
            s
        }
    }

    fn file_names(dir: &Path) -> std::io::Result<Vec<String>> {
        let mut names = Vec::new();
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                names.push(entry.file_name().to_string_lossy().into_owned());
            }
        }
        names.sort();
        Ok(names)
    }

    fn first_difference(a: &[u8], b: &[u8]) -> String {
        let (a, b) = (String::from_utf8_lossy(a), String::from_utf8_lossy(b));
        let (mut la, mut lb) = (a.lines(), b.lines());
        let mut n = 1;
        loop {
            match (la.next(), lb.next()) {
                (Some(x), Some(y)) if x == y => n += 1,
                (x, y) => {
                    return format!(
                        "first difference at line {n}: committed {:?}, generated {:?}",
                        x.unwrap_or("<EOF>"),
                        y.unwrap_or("<EOF>")
                    )
                }
            }
        }
    }

    /// Compare every file in `fresh_dir` with `committed_dir`
    /// (LF-normalized).
    pub fn compare(fresh_dir: &Path, committed_dir: &Path) -> std::io::Result<Drift> {
        let fresh = file_names(fresh_dir)?;
        let committed = file_names(committed_dir)?;
        let mut drift = Drift::default();
        for name in &fresh {
            let new = normalize_newlines(&std::fs::read(fresh_dir.join(name))?);
            if !committed.contains(name) {
                drift.missing.push(name.clone());
                continue;
            }
            let old = normalize_newlines(&std::fs::read(committed_dir.join(name))?);
            if old != new {
                drift
                    .changed
                    .push((name.clone(), first_difference(&old, &new)));
            }
        }
        for name in &committed {
            if !fresh.contains(name) {
                drift.extra.push(name.clone());
            }
        }
        Ok(drift)
    }

    fn warn_lines(text: &str) {
        for line in text.lines() {
            println!("cargo:warning={line}");
        }
    }

    /// Default build path: print the `rerun-if-changed` lines for the
    /// stamp inputs and warn (never fail) when the stamp is stale.
    pub fn default_build(krate: &str, crate_dir: &Path, generated_dir: &Path, protos: &[&str]) {
        emit_directives(generated_dir, protos);
        if let Some(problem) = stamp_mismatch(crate_dir, generated_dir, protos) {
            warn_lines(&format!(
                "{krate}: committed protobuf bindings may be stale: {problem}\n\
                 regenerate: REMOTEMEDIA_REGEN_PROTOS=1 cargo check -p {krate} --features regen-protos"
            ));
        }
    }

    /// Name of the compile-time env var (set by build.rs) listing the
    /// `.proto` inputs, `;`-separated, for the crate's stamp unit test.
    pub const INPUTS_ENV: &str = "REMOTEMEDIA_PROTO_INPUTS";

    fn emit_directives(generated_dir: &Path, protos: &[&str]) {
        println!("cargo:rustc-env={INPUTS_ENV}={}", protos.join(";"));
        for proto in protos {
            println!("cargo:rerun-if-changed={proto}");
        }
        println!(
            "cargo:rerun-if-changed={}",
            generated_dir.join(STAMP_FILE).display()
        );
    }

    /// `regen-protos` build path. `compile(out_dir)` runs protoc into
    /// `out_dir`; the result is compared with `generated_dir`. With
    /// `REMOTEMEDIA_REGEN_PROTOS=1` differences are written back, otherwise
    /// they fail the build.
    pub fn regen_build(
        krate: &str,
        crate_dir: &Path,
        generated_dir: &Path,
        out_dir: &Path,
        protos: &[&str],
        compile: impl FnOnce(&Path),
    ) {
        emit_directives(generated_dir, protos);
        println!("cargo:rerun-if-env-changed={REGEN_ENV}");
        // Re-check when the committed bindings are edited by hand.
        for name in file_names(generated_dir).expect("list src/generated") {
            println!(
                "cargo:rerun-if-changed={}",
                generated_dir.join(name).display()
            );
        }

        let fresh_dir = out_dir.join("protos");
        let _ = std::fs::remove_dir_all(&fresh_dir);
        std::fs::create_dir_all(&fresh_dir).expect("create OUT_DIR/protos");
        compile(&fresh_dir);
        std::fs::write(
            fresh_dir.join(STAMP_FILE),
            render_stamp(crate_dir, protos).expect("hash .proto files"),
        )
        .expect("write protos.stamp");

        let drift = compare(&fresh_dir, generated_dir).expect("compare bindings");
        if drift.is_empty() {
            return;
        }
        if std::env::var(REGEN_ENV).is_ok_and(|v| v == "1") {
            apply(&drift, &fresh_dir, generated_dir).expect("update src/generated");
            warn_lines(&format!(
                "{krate}: regenerated {}:\n{}",
                generated_dir.display(),
                drift.describe()
            ));
        } else {
            panic!(
                "{krate}: committed {} does not match the .proto files:\n{}\
                 regenerate: REMOTEMEDIA_REGEN_PROTOS=1 cargo check -p {krate} --features regen-protos",
                generated_dir.display(),
                drift.describe()
            );
        }
    }

    /// Make `committed_dir` match `fresh_dir`: copy changed and missing
    /// files (LF line endings, as protoc wrote them), delete extra ones.
    /// Untouched files keep their mtime, so cargo does not rebuild for
    /// nothing.
    pub fn apply(drift: &Drift, fresh_dir: &Path, committed_dir: &Path) -> std::io::Result<()> {
        let to_copy = drift
            .changed
            .iter()
            .map(|(f, _)| f)
            .chain(drift.missing.iter());
        for name in to_copy {
            std::fs::copy(fresh_dir.join(name), committed_dir.join(name))?;
        }
        for name in &drift.extra {
            std::fs::remove_file(committed_dir.join(name))?;
        }
        Ok(())
    }
}
