//! Verify and stage the source used by an isolated preparation controller.

use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Read;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

use sha2::{Digest, Sha256};

use super::{invalid_controller, Result};

const PACKAGE: &str = "hf_spaces_runner";
const MAX_ENTRIES: usize = 512;
const MAX_BYTES: u64 = 4 * 1024 * 1024;
const DOMAIN: &[u8] = b"remotemedia-space-controller-v1\0";

pub(super) fn verify(root: &Path, expected: &str) -> Result<()> {
    read_verified(root, expected).map(|_| ())
}

pub(super) fn stage(root: &Path, expected: &str) -> Result<tempfile::TempDir> {
    let verified = read_verified(root, expected)?;
    let staged = tempfile::Builder::new()
        .prefix("rm-prepare-controller-")
        .tempdir()?;
    fs::create_dir(staged.path().join(PACKAGE))?;
    for (name, bytes) in verified {
        fs::write(staged.path().join(name), bytes)?;
    }
    Ok(staged)
}

fn read_verified(root: &Path, expected: &str) -> Result<BTreeMap<String, Vec<u8>>> {
    let files = read_release(root).map_err(|_| invalid_controller())?;
    if fingerprint(&files) != expected {
        return Err(invalid_controller());
    }
    Ok(files)
}

fn read_release(root: &Path) -> Result<BTreeMap<String, Vec<u8>>> {
    let package = root.join(PACKAGE);
    if !fs::symlink_metadata(&package)?.is_dir() {
        return Err(invalid_controller());
    }
    let mut remaining = MAX_BYTES;
    let mut files = BTreeMap::from([(
        "plugin.toml".into(),
        read_file(&root.join("plugin.toml"), &mut remaining)?,
    )]);
    for (index, entry) in fs::read_dir(&package)?.enumerate() {
        if index >= MAX_ENTRIES {
            return Err(invalid_controller());
        }
        let entry = entry?;
        let leaf = entry.file_name();
        if Path::new(&leaf).extension().is_none_or(|ext| ext != "py") {
            continue;
        }
        let leaf = leaf.to_str().ok_or_else(invalid_controller)?;
        if !leaf[..leaf.len() - 3]
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_')
        {
            return Err(invalid_controller());
        }
        let bytes = read_file(&entry.path(), &mut remaining)?;
        files.insert(format!("{PACKAGE}/{leaf}"), bytes);
    }
    if !files.contains_key("hf_spaces_runner/__init__.py")
        || !files.contains_key("hf_spaces_runner/nodes.py")
    {
        return Err(invalid_controller());
    }
    Ok(files)
}

fn read_file(path: &Path, remaining: &mut u64) -> Result<Vec<u8>> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    if !file.metadata()?.is_file() {
        return Err(invalid_controller());
    }
    let mut bytes = Vec::new();
    file.take(*remaining + 1).read_to_end(&mut bytes)?;
    *remaining = remaining
        .checked_sub(bytes.len() as u64)
        .ok_or_else(invalid_controller)?;
    Ok(bytes)
}

fn fingerprint(files: &BTreeMap<String, Vec<u8>>) -> String {
    let mut hash = Sha256::new();
    hash.update(DOMAIN);
    for (path, bytes) in files {
        hash.update(path.as_bytes());
        hash.update([0]);
        hash.update((bytes.len() as u64).to_be_bytes());
        hash.update(bytes);
    }
    format!("{:x}", hash.finalize())
}

#[cfg(test)]
pub(super) fn digest_fixture(root: &Path) -> String {
    fingerprint(&read_release(root).unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;

    #[test]
    fn only_the_pinned_source_can_be_staged() {
        let root = tempfile::tempdir().unwrap();
        let package = root.path().join(PACKAGE);
        fs::create_dir(&package).unwrap();
        fs::write(root.path().join("plugin.toml"), b"approved manifest").unwrap();
        fs::write(package.join("__init__.py"), b"").unwrap();
        let entry = package.join("nodes.py");
        fs::write(&entry, b"approved code").unwrap();
        fs::write(package.join("extra.pyc"), b"unapproved bytecode").unwrap();
        let expected = digest_fixture(root.path());

        let staged = stage(root.path(), &expected).unwrap();
        let staged_package = staged.path().join(PACKAGE);
        assert!(!staged_package.join("extra.pyc").exists());
        fs::write(&entry, b"changed source").unwrap();
        assert!(stage(root.path(), &expected).is_err());
        assert_eq!(
            fs::read(staged_package.join("nodes.py")).unwrap(),
            b"approved code"
        );

        fs::remove_file(&entry).unwrap();
        symlink(staged_package.join("nodes.py"), &entry).unwrap();
        assert!(stage(root.path(), &expected).is_err());
    }
}
