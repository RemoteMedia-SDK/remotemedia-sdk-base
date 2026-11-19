//! Worker-owned registration and per-run staging for prepared Space inputs.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use remotemedia_bundle::{sha256_digest, DescriptorIdentity};
use serde::{Deserialize, Serialize};
use tempfile::{NamedTempFile, TempDir};

use crate::{ContentStore, DeploymentError};

#[derive(Debug, thiserror::Error)]
pub enum SpaceArtifactError {
    #[error("invalid Space artifact: {0}")]
    Invalid(&'static str),
    #[error("the Space artifact is not registered for this launch policy")]
    Unregistered,
    #[error(transparent)]
    Content(#[from] DeploymentError),
    #[error("artifact registry I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("artifact registry metadata is invalid: {0}")]
    Json(#[from] serde_json::Error),
}

const MAX_ARTIFACT_BYTES: usize = 4 * 1024 * 1024;
const MAX_TREE_BYTES: usize = 32 * 1024 * 1024;
const MAX_REGISTRATION_BYTES: usize = 2 * (MAX_ARTIFACT_BYTES + MAX_TREE_BYTES) + 4096;
const GIB: u64 = 1024 * 1024 * 1024;

/// This scope comes from trusted worker admission or publication, outside the
/// artifact and pipeline manifest. Each registration is specific to one policy.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpaceArtifactScope {
    pub artifact_sha256: String,
    pub repository: String,
    pub commit: String,
    pub target: String,
    pub profile: String,
    pub policy_revision: String,
}

/// Storage rooted in a private operator-owned directory. Workloads must never
/// be able to write here. Its content store is dedicated to Space artifacts.
pub struct SpaceArtifactRegistry {
    root: PathBuf,
    content: ContentStore,
}

/// A private snapshot of verified bytes. Hold the lease until the provider has
/// confirmed teardown. Providers must expose this snapshot through read-only
/// mounts or an equivalent immutable view. The outer root stays host-private.
pub struct SpaceArtifactLease {
    files: TempDir,
    entry_file: String,
}

impl SpaceArtifactLease {
    pub fn root(&self) -> &Path {
        self.files.path()
    }

    pub fn entry_file(&self) -> &str {
        &self.entry_file
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Registration {
    scope: SpaceArtifactScope,
    artifact: String,
    source_tree: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Artifact {
    version: u32,
    repository: String,
    commit: String,
    source_tree_sha256: String,
    sdk: String,
    entry_file: String,
    target: Target,
    target_profile: Option<serde_json::Value>,
    wheels: BTreeMap<String, FileIdentity>,
    models: Vec<Model>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Target {
    platform: String,
    arch: String,
    python: String,
    abi: String,
    backend: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Model {
    repository: String,
    revision: String,
    files: BTreeMap<String, FileIdentity>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FileIdentity {
    size: u64,
    sha256: String,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum SourceEntry {
    Directory(String, DirectoryKind),
    File(String, FileKind, u64, u32, String),
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum DirectoryKind {
    Directory,
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum FileKind {
    File,
}

struct StagedFile {
    path: PathBuf,
    descriptor: DescriptorIdentity,
    execute_bits: u32,
}

struct Closure {
    entry_file: String,
    directories: BTreeSet<PathBuf>,
    files: Vec<StagedFile>,
}

impl SpaceArtifactRegistry {
    pub fn open(root: impl Into<PathBuf>) -> Result<Self, SpaceArtifactError> {
        let root = root.into();
        let content = ContentStore::open(root.join("content"))?;
        fs::create_dir_all(root.join("registrations"))?;
        fs::create_dir_all(root.join("leases"))?;
        Ok(Self { root, content })
    }

    /// Import bytes supplied by the trusted publisher. Importing bytes alone
    /// does not create a registration or approve their use.
    pub fn import_blob(
        &self,
        sha256: &str,
        size: u64,
        source: impl Read,
    ) -> Result<(), SpaceArtifactError> {
        if !digest(sha256, 64) || size > 128 * GIB {
            return Err(invalid("invalid blob hash or size"));
        }
        self.content
            .import_verified_blob(
                &DescriptorIdentity {
                    digest: format!("sha256:{sha256}"),
                    size,
                },
                source,
            )
            .map_err(Into::into)
    }

    /// Validate the entire closure before publishing the approval atomically.
    /// Repeating an identical registration is safe, including across processes.
    pub fn register(
        &self,
        scope: &SpaceArtifactScope,
        artifact: &str,
        source_tree: &str,
    ) -> Result<(), SpaceArtifactError> {
        let closure = validate(scope, artifact, source_tree)?;
        let mut seen = BTreeMap::new();
        for file in &closure.files {
            match seen.insert(&file.descriptor.digest, file.descriptor.size) {
                Some(size) if size == file.descriptor.size => (),
                Some(_) => return Err(invalid("one digest has conflicting sizes")),
                None => self.content.verify_blob(&file.descriptor)?,
            }
        }

        let destination = self.registration_path(scope)?;
        let mut staged = NamedTempFile::new_in(destination.parent().unwrap())?;
        serde_json::to_writer(
            &mut staged,
            &Registration {
                scope: scope.clone(),
                artifact: artifact.to_owned(),
                source_tree: source_tree.to_owned(),
            },
        )?;
        staged.flush()?;
        staged.as_file().sync_all()?;
        match staged.persist_noclobber(&destination) {
            Ok(_) => {
                #[cfg(unix)]
                File::open(destination.parent().unwrap())?.sync_all()?;
                Ok(())
            }
            Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
                let existing = self.registration(scope)?;
                if existing.artifact == artifact && existing.source_tree == source_tree {
                    return Ok(());
                }
                Err(invalid("conflicting registration"))
            }
            Err(error) => Err(error.error.into()),
        }
    }

    pub fn check_registration(&self, scope: &SpaceArtifactScope) -> Result<(), SpaceArtifactError> {
        let record = self.registration(scope)?;
        validate(scope, &record.artifact, &record.source_tree)?;
        Ok(())
    }

    /// Recheck authorization and copy exact verified bytes into a new lease.
    /// Failed and dropped leases are discarded. Leases never share writable files.
    pub fn lease(
        &self,
        scope: &SpaceArtifactScope,
    ) -> Result<SpaceArtifactLease, SpaceArtifactError> {
        let record = self.registration(scope)?;
        let closure = validate(scope, &record.artifact, &record.source_tree)?;
        let files = tempfile::Builder::new()
            .prefix("space-")
            .tempdir_in(self.root.join("leases"))?;
        for directory in closure.directories {
            fs::create_dir(files.path().join(directory))?;
        }
        for file in closure.files {
            let path = files.path().join(file.path);
            self.content.stage_verified_blob(&file.descriptor, &path)?;
            set_snapshot_mode(&path, file.execute_bits)?;
        }
        Ok(SpaceArtifactLease {
            files,
            entry_file: closure.entry_file,
        })
    }

    fn registration_path(&self, scope: &SpaceArtifactScope) -> Result<PathBuf, SpaceArtifactError> {
        validate_scope(scope)?;
        let digest = sha256_digest(&serde_json::to_vec(scope)?);
        Ok(self.root.join("registrations").join(&digest[7..]))
    }

    fn registration(&self, scope: &SpaceArtifactScope) -> Result<Registration, SpaceArtifactError> {
        let path = self.registration_path(scope)?;
        let file = File::open(path).map_err(|error| match error.kind() {
            std::io::ErrorKind::NotFound => SpaceArtifactError::Unregistered,
            _ => error.into(),
        })?;
        let mut bytes = Vec::new();
        file.take(MAX_REGISTRATION_BYTES as u64 + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() > MAX_REGISTRATION_BYTES {
            return Err(invalid("registration size limit exceeded"));
        }
        let registration: Registration = serde_json::from_slice(&bytes)?;
        if &registration.scope != scope {
            return Err(SpaceArtifactError::Unregistered);
        }
        Ok(registration)
    }
}

fn validate(
    scope: &SpaceArtifactScope,
    artifact: &str,
    tree: &str,
) -> Result<Closure, SpaceArtifactError> {
    validate_scope(scope)?;
    if artifact.len() > MAX_ARTIFACT_BYTES || tree.len() > MAX_TREE_BYTES {
        return Err(invalid("metadata size limit exceeded"));
    }
    if sha256_digest(artifact.as_bytes())[7..] != scope.artifact_sha256 {
        return Err(invalid("artifact digest mismatch"));
    }
    let record: Artifact = serde_json::from_str(artifact)?;
    if record.version != 1 || record.repository != scope.repository || record.commit != scope.commit
    {
        return Err(invalid("artifact identity or version mismatch"));
    }
    record
        .target
        .validate(scope, record.target_profile.as_ref())?;
    if sha256_digest(tree.as_bytes())[7..] != record.source_tree_sha256 {
        return Err(invalid("source inventory digest mismatch"));
    }
    if !matches!(
        (record.sdk.as_str(), record.wheels.is_empty()),
        ("static", true) | ("gradio", false)
    ) || record.wheels.len() > 2048
        || record.models.len() > 32
    {
        return Err(invalid("unsupported or incomplete workload closure"));
    }
    let mut closure = source_closure(tree, record.entry_file)?;
    let wheels_root = Path::new("wheels");
    closure.directories.insert(wheels_root.to_path_buf());
    let mut wheel_bytes = 0;
    let mut wheel_names = BTreeSet::new();
    for (name, file) in record.wheels {
        let stem = name.strip_suffix(".whl").filter(|stem| {
            !stem.is_empty()
                && stem.len() <= 220
                && stem
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"+_.-".contains(&byte))
        });
        if stem.is_none() || file.size == 0 || !wheel_names.insert(name.to_ascii_lowercase()) {
            return Err(invalid("invalid wheel name, size or duplicate"));
        }
        wheel_bytes = add_size(wheel_bytes, file.size, 64 * GIB)?;
        closure.files.push(staged_file(
            wheels_root.join(safe_path(&name)?),
            file.size,
            &file.sha256,
            0,
        )?);
    }
    let models_root = Path::new("models");
    closure.directories.insert(models_root.to_path_buf());
    let mut model_bytes = 0;
    let mut model_paths = 0;
    let mut model_pins = BTreeSet::new();
    for (index, model) in record.models.into_iter().enumerate() {
        if !repository(&model.repository)
            || !digest(&model.revision, 40)
            || model.files.is_empty()
            || !model_pins.insert((model.repository, model.revision))
        {
            return Err(invalid("invalid model closure"));
        }
        model_paths += model.files.len();
        if model_paths > 20_000 {
            return Err(invalid("model entry limit exceeded"));
        }
        let root = models_root.join(index.to_string());
        closure.directories.insert(root.clone());
        let mut inventory = BTreeMap::new();
        for (name, file) in model.files {
            let relative = safe_path(&name)?;
            insert_path(&mut inventory, &name, false)?;
            let path = root.join(relative);
            for (parent, _) in name.match_indices('/') {
                insert_path(&mut inventory, &name[..parent], true)?;
                closure.directories.insert(root.join(&name[..parent]));
            }
            model_bytes = add_size(model_bytes, file.size, 128 * GIB)?;
            closure
                .files
                .push(staged_file(path, file.size, &file.sha256, 0)?);
        }
    }
    Ok(closure)
}

fn source_closure(tree: &str, entry_file: String) -> Result<Closure, SpaceArtifactError> {
    let entries: Vec<SourceEntry> = serde_json::from_str(tree)?;
    if entries.is_empty() || entries.len() > 20_000 {
        return Err(invalid("source entry limit exceeded"));
    }
    safe_path(&entry_file)?;
    let root = Path::new("source");
    let mut directories = BTreeSet::from([root.to_path_buf()]);
    let mut files = Vec::new();
    let mut inventory = BTreeMap::new();
    let mut size = 0;
    let mut executable_entry = false;
    for entry in entries {
        match entry {
            SourceEntry::Directory(name, _) => {
                let relative = safe_path(&name)?;
                if inventory.contains_key(&name.to_lowercase()) {
                    return Err(invalid("duplicate directory in source inventory"));
                }
                insert_path(&mut inventory, &name, true)?;
                directories.insert(root.join(relative));
            }
            SourceEntry::File(name, _, bytes, mode, hash) => {
                let relative = safe_path(&name)?;
                insert_path(&mut inventory, &name, false)?;
                size = add_size(size, bytes, 32 * GIB)?;
                if name == entry_file {
                    executable_entry = true;
                }
                files.push(staged_file(root.join(relative), bytes, &hash, mode)?);
            }
        }
    }
    if !executable_entry
        || inventory.values().any(|(name, _)| {
            name.rsplit_once('/').is_some_and(|(parent, _)| {
                inventory.get(&parent.to_lowercase()) != Some(&(parent.to_owned(), true))
            })
        })
    {
        return Err(invalid("source parent or entry file is missing"));
    }
    Ok(Closure {
        entry_file,
        directories,
        files,
    })
}

impl Target {
    fn validate(
        &self,
        scope: &SpaceArtifactScope,
        profile: Option<&serde_json::Value>,
    ) -> Result<(), SpaceArtifactError> {
        let os = match self.platform.as_str() {
            "linux" => "linux",
            "win32" => "windows",
            "darwin" => "macos",
            _ => return Err(invalid("unknown target platform")),
        };
        let arch = match self.arch.as_str() {
            "x86_64" => "amd64",
            "arm64" => "arm64",
            _ => return Err(invalid("unknown target architecture")),
        };
        let accelerator = match (self.platform.as_str(), self.backend.as_str()) {
            (_, "cpu") | ("darwin", "macos") => "cpu",
            ("linux" | "win32", "cuda") => "cuda",
            ("darwin", "mlx-metal") => "metal",
            _ => return Err(invalid("unknown target backend")),
        };
        let version: Vec<_> = self.python.split('.').collect();
        if !matches!(version.as_slice(), ["3", _] | ["3", _, _])
            || version[1..].iter().any(|part| {
                part.is_empty() || part.len() > 3 || !part.bytes().all(|b| b.is_ascii_digit())
            })
            || self.abi != format!("cp3{}", version[1])
            || scope.target != format!("{os}-{arch}-{}", self.backend)
        {
            return Err(invalid("artifact target or Python ABI mismatch"));
        }
        if let Some(profile) = profile {
            let architecture = profile.get("architecture").and_then(|value| value.as_str());
            let expected_alt = if arch == "amd64" { "x86_64" } else { "aarch64" };
            if profile["schemaVersion"] != "1"
                || profile["os"] != os
                || !matches!(architecture, Some(value) if value == arch || value == expected_alt)
                || profile["python"]["implementation"] != "cpython"
                || profile["python"]["version"] != self.python
                || profile["python"]["abi"] != self.abi
                || profile["accelerator"]["kind"] != accelerator
            {
                return Err(invalid("artifact wheel profile mismatch"));
            }
        }
        Ok(())
    }
}

fn staged_file(
    path: PathBuf,
    size: u64,
    hash: &str,
    execute_bits: u32,
) -> Result<StagedFile, SpaceArtifactError> {
    if !digest(hash, 64) || execute_bits & !0o111 != 0 {
        return Err(invalid("invalid file hash or mode"));
    }
    Ok(StagedFile {
        path,
        descriptor: DescriptorIdentity {
            digest: format!("sha256:{hash}"),
            size,
        },
        execute_bits,
    })
}

fn insert_path(
    inventory: &mut BTreeMap<String, (String, bool)>,
    name: &str,
    directory: bool,
) -> Result<(), SpaceArtifactError> {
    use std::collections::btree_map::Entry;
    match inventory.entry(name.to_lowercase()) {
        Entry::Vacant(entry) => {
            entry.insert((name.to_owned(), directory));
        }
        Entry::Occupied(entry) if directory && entry.get().1 && entry.get().0 == name => (),
        Entry::Occupied(_) => return Err(invalid("colliding paths in artifact closure")),
    }
    Ok(())
}

fn safe_path(name: &str) -> Result<PathBuf, SpaceArtifactError> {
    if name.is_empty()
        || name.len() > 1024
        || name.split('/').any(|part| {
            let stem = part
                .split('.')
                .next()
                .unwrap_or_default()
                .trim_end()
                .to_ascii_uppercase();
            part.is_empty()
                || part.len() > 255
                || part.ends_with(['.', ' '])
                || part.eq_ignore_ascii_case(".git")
                || part
                    .chars()
                    .any(|ch| ch.is_control() || "\\:<>\"|?*".contains(ch))
                || matches!(
                    stem.as_str(),
                    "CON" | "PRN" | "AUX" | "NUL" | "CONIN$" | "CONOUT$"
                )
                || stem
                    .strip_prefix("COM")
                    .or_else(|| stem.strip_prefix("LPT"))
                    .is_some_and(|number| {
                        matches!(
                            number,
                            "0" | "1"
                                | "2"
                                | "3"
                                | "4"
                                | "5"
                                | "6"
                                | "7"
                                | "8"
                                | "9"
                                | "¹"
                                | "²"
                                | "³"
                        )
                    })
        })
    {
        return Err(invalid("unsafe artifact path"));
    }
    Ok(PathBuf::from(name))
}

fn add_size(total: u64, next: u64, limit: u64) -> Result<u64, SpaceArtifactError> {
    total
        .checked_add(next)
        .filter(|size| *size <= limit)
        .ok_or_else(|| invalid("artifact byte limit exceeded"))
}

fn validate_scope(scope: &SpaceArtifactScope) -> Result<(), SpaceArtifactError> {
    if !digest(&scope.artifact_sha256, 64)
        || !digest(&scope.policy_revision, 64)
        || !digest(&scope.commit, 40)
        || !repository(&scope.repository)
        || !matches!(scope.profile.as_str(), "reviewed" | "isolated")
        || !identifier(&scope.target)
    {
        return Err(invalid("invalid or incomplete approval scope"));
    }
    Ok(())
}

fn repository(value: &str) -> bool {
    value
        .split_once('/')
        .is_some_and(|(owner, name)| identifier(owner) && identifier(name))
}

fn identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && !matches!(value, "." | "..")
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_.-".contains(&byte))
}

fn digest(value: &str, size: usize) -> bool {
    value.len() == size
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn invalid(reason: &'static str) -> SpaceArtifactError {
    SpaceArtifactError::Invalid(reason)
}

fn set_snapshot_mode(path: &Path, execute_bits: u32) -> Result<(), SpaceArtifactError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o444 | execute_bits))?;
    }
    #[cfg(not(unix))]
    let _ = (path, execute_bits);
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    const SOURCE: &[u8] = b"print('ready')\n";
    const WHEEL: &[u8] = b"test wheel bytes";
    const MODEL: &[u8] = b"test model bytes";

    fn hash(bytes: &[u8]) -> String {
        sha256_digest(bytes)[7..].to_owned()
    }

    fn fixture() -> (SpaceArtifactScope, String, String) {
        let tree = json!([
            ["docs", "directory"],
            ["src", "directory"],
            ["src/app.py", "file", SOURCE.len(), 73, hash(SOURCE)]
        ])
        .to_string();
        let commit = "a".repeat(40);
        let artifact = json!({"version": 1, "repository": "owner/demo", "commit": commit,
            "source_tree_sha256": hash(tree.as_bytes()), "sdk": "gradio", "entry_file": "src/app.py",
            "target": {"platform":"linux", "arch":"x86_64", "python":"3.12", "abi":"cp312", "backend":"cpu"},
            "target_profile": {"schemaVersion":"1", "os":"linux", "architecture":"amd64",
                "python":{"implementation":"cpython", "version":"3.12", "abi":"cp312"},
                "accelerator":{"kind":"cpu"}},
            "wheels": {"fixture-1-py3-none-any.whl":{"size":WHEEL.len(), "sha256":hash(WHEEL)}},
            "models": [{"repository":"owner/model", "revision":"b".repeat(40),
                "files":{"data/model.bin":{"size":MODEL.len(), "sha256":hash(MODEL)}}}]
        }).to_string();
        let scope = SpaceArtifactScope {
            artifact_sha256: hash(artifact.as_bytes()),
            repository: "owner/demo".into(),
            commit,
            target: "linux-amd64-cpu".into(),
            profile: "isolated".into(),
            policy_revision: "c".repeat(64),
        };
        (scope, artifact, tree)
    }

    #[test]
    fn space_artifact_requires_a_complete_registered_closure_and_exact_scope() {
        let root = tempfile::tempdir().unwrap();
        let registry = SpaceArtifactRegistry::open(root.path()).unwrap();
        let (mut scope, artifact, tree) = fixture();
        assert!(matches!(
            registry.lease(&scope),
            Err(SpaceArtifactError::Unregistered)
        ));
        assert!(registry.register(&scope, &artifact, &tree).is_err());
        assert!(matches!(
            registry.check_registration(&scope),
            Err(SpaceArtifactError::Unregistered)
        ));
        assert!(registry
            .import_blob(&hash(SOURCE), SOURCE.len() as u64, &b"wrong bytes"[..])
            .is_err());
        for bytes in [SOURCE, WHEEL, MODEL] {
            registry
                .import_blob(&hash(bytes), bytes.len() as u64, bytes)
                .unwrap();
        }
        registry.register(&scope, &artifact, &tree).unwrap();
        let lease = registry.lease(&scope).unwrap();
        assert_eq!(lease.entry_file(), "src/app.py");
        assert_eq!(
            fs::read(lease.root().join("source/src/app.py")).unwrap(),
            SOURCE
        );
        assert_eq!(
            fs::read(lease.root().join("wheels/fixture-1-py3-none-any.whl")).unwrap(),
            WHEEL
        );
        assert_eq!(
            fs::read(lease.root().join("models/0/data/model.bin")).unwrap(),
            MODEL
        );
        assert!(lease.root().join("source/docs").is_dir());
        scope.policy_revision = "d".repeat(64);
        assert!(matches!(
            registry.lease(&scope),
            Err(SpaceArtifactError::Unregistered)
        ));
        scope.target = "linux-arm64-cpu".into();
        assert!(matches!(
            registry.register(&scope, &artifact, &tree),
            Err(SpaceArtifactError::Invalid(_))
        ));
    }

    #[test]
    fn space_artifact_concurrent_leases_survive_mutation_and_failed_staging_is_discarded() {
        let root = tempfile::tempdir().unwrap();
        let registry = SpaceArtifactRegistry::open(root.path()).unwrap();
        let (scope, artifact, tree) = fixture();
        let (first, second) = std::thread::scope(|threads| {
            let lease = || {
                for bytes in [SOURCE, WHEEL, MODEL] {
                    registry
                        .import_blob(&hash(bytes), bytes.len() as u64, bytes)
                        .unwrap();
                }
                registry.register(&scope, &artifact, &tree).unwrap();
                registry.lease(&scope).unwrap()
            };
            let first = threads.spawn(lease);
            let second = threads.spawn(lease);
            (first.join().unwrap(), second.join().unwrap())
        });
        assert_ne!(first.root(), second.root());
        let cached_wheel = root.path().join("content/blobs/sha256").join(hash(WHEEL));
        fs::write(&cached_wheel, vec![0; WHEEL.len()]).unwrap();
        assert!(matches!(
            registry.lease(&scope),
            Err(SpaceArtifactError::Content(
                DeploymentError::DigestMismatch { .. }
            ))
        ));
        assert_eq!(fs::read_dir(root.path().join("leases")).unwrap().count(), 2);
        assert_eq!(
            fs::read(first.root().join("wheels/fixture-1-py3-none-any.whl")).unwrap(),
            WHEEL
        );
        assert_eq!(
            fs::read(second.root().join("wheels/fixture-1-py3-none-any.whl")).unwrap(),
            WHEEL
        );
        fs::write(cached_wheel, WHEEL).unwrap();
        drop(registry.lease(&scope).unwrap());
        drop(first);
        drop(second);
        assert_eq!(fs::read_dir(root.path().join("leases")).unwrap().count(), 0);
    }

    #[test]
    fn space_artifact_checks_safe_paths_and_binds_source_inventory_bytes() {
        let (mut scope, artifact, tree) = fixture();
        assert!(matches!(
            validate(&scope, &artifact, &(tree.clone() + " ")),
            Err(SpaceArtifactError::Invalid(_))
        ));
        for name in [
            "../outside",
            "/absolute",
            "src//app.py",
            "src/../app.py",
            "src\\alias.py",
            "tmp/CON.py",
            "src/.git/config",
            "src/app.py.",
            "SRC/APP.PY",
        ] {
            let mut entries: serde_json::Value = serde_json::from_str(&tree).unwrap();
            entries.as_array_mut().unwrap().push(json!([
                name,
                "file",
                SOURCE.len(),
                0,
                hash(SOURCE)
            ]));
            let tree = entries.to_string();
            let mut changed: serde_json::Value = serde_json::from_str(&artifact).unwrap();
            changed["source_tree_sha256"] = json!(hash(tree.as_bytes()));
            let changed = changed.to_string();
            scope.artifact_sha256 = hash(changed.as_bytes());
            assert!(
                matches!(
                    validate(&scope, &changed, &tree),
                    Err(SpaceArtifactError::Invalid(_))
                ),
                "accepted {name}"
            );
        }
    }
}
