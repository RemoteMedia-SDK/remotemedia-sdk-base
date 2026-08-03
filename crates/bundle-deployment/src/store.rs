use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use remotemedia_bundle::{sha256_digest, DescriptorIdentity};

use crate::DeploymentError;

#[derive(Debug, Clone)]
pub struct ContentStore {
    root: PathBuf,
}

impl ContentStore {
    pub fn open(root: impl Into<PathBuf>) -> Result<Self, DeploymentError> {
        let root = root.into();
        fs::create_dir_all(root.join("blobs/sha256"))?;
        fs::create_dir_all(root.join("uploads"))?;
        fs::create_dir_all(root.join("quarantine"))?;
        Ok(Self { root })
    }

    pub fn contains(&self, digest: &str) -> bool {
        self.blob_path(digest).is_ok_and(|path| path.is_file())
    }

    pub fn missing<'a>(
        &self,
        descriptors: impl IntoIterator<Item = &'a DescriptorIdentity>,
    ) -> Vec<DescriptorIdentity> {
        descriptors
            .into_iter()
            .filter(|descriptor| !self.contains(&descriptor.digest))
            .cloned()
            .collect()
    }

    pub fn begin_upload(
        &self,
        descriptor: DescriptorIdentity,
    ) -> Result<UploadSession, DeploymentError> {
        validate_digest(&descriptor.digest)?;
        let hex = descriptor.digest.trim_start_matches("sha256:");
        let path = self.root.join("uploads").join(format!("{hex}.partial"));
        let offset = fs::metadata(&path).map(|meta| meta.len()).unwrap_or(0);
        if offset > descriptor.size {
            fs::rename(
                &path,
                self.root.join("quarantine").join(format!("{hex}.oversize")),
            )?;
            return Err(DeploymentError::SizeMismatch {
                expected: descriptor.size,
                actual: offset,
            });
        }
        Ok(UploadSession {
            store: self.clone(),
            descriptor,
            path,
            offset,
        })
    }

    pub fn read(&self, digest: &str) -> Result<Vec<u8>, DeploymentError> {
        Ok(fs::read(self.blob_path(digest)?)?)
    }

    pub fn remove_unreferenced(
        &self,
        keep: &std::collections::BTreeSet<String>,
    ) -> Result<Vec<String>, DeploymentError> {
        let directory = self.root.join("blobs/sha256");
        let mut removed = Vec::new();
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let digest = format!("sha256:{}", entry.file_name().to_string_lossy());
            if !keep.contains(&digest) {
                fs::remove_file(entry.path())?;
                removed.push(digest);
            }
        }
        removed.sort();
        Ok(removed)
    }

    fn blob_path(&self, digest: &str) -> Result<PathBuf, DeploymentError> {
        validate_digest(digest)?;
        Ok(self
            .root
            .join("blobs/sha256")
            .join(digest.trim_start_matches("sha256:")))
    }
}

pub struct UploadSession {
    store: ContentStore,
    descriptor: DescriptorIdentity,
    path: PathBuf,
    offset: u64,
}

impl UploadSession {
    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn append(&mut self, offset: u64, bytes: &[u8]) -> Result<u64, DeploymentError> {
        if offset != self.offset {
            return Err(DeploymentError::OffsetMismatch {
                expected: self.offset,
                actual: offset,
            });
        }
        let new_size = self.offset.saturating_add(bytes.len() as u64);
        if new_size > self.descriptor.size {
            return Err(DeploymentError::SizeMismatch {
                expected: self.descriptor.size,
                actual: new_size,
            });
        }
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .truncate(false)
            .open(&self.path)?;
        file.seek(SeekFrom::Start(self.offset))?;
        file.write_all(bytes)?;
        file.sync_data()?;
        self.offset = new_size;
        Ok(self.offset)
    }

    pub fn finish(self) -> Result<PathBuf, DeploymentError> {
        if self.offset != self.descriptor.size {
            return Err(DeploymentError::SizeMismatch {
                expected: self.descriptor.size,
                actual: self.offset,
            });
        }
        let mut file = File::open(&self.path)?;
        let mut bytes = Vec::with_capacity(self.offset as usize);
        file.read_to_end(&mut bytes)?;
        let actual = sha256_digest(&bytes);
        if actual != self.descriptor.digest {
            let quarantine = quarantine_path(&self.store.root, &self.path, "digest-mismatch");
            fs::rename(&self.path, quarantine)?;
            return Err(DeploymentError::DigestMismatch {
                expected: self.descriptor.digest,
                actual,
            });
        }
        let destination = self.store.blob_path(&self.descriptor.digest)?;
        if destination.exists() {
            fs::remove_file(&self.path)?;
        } else {
            fs::rename(&self.path, &destination)?;
        }
        sync_parent(&destination)?;
        Ok(destination)
    }
}

fn validate_digest(digest: &str) -> Result<(), DeploymentError> {
    let Some(hex) = digest.strip_prefix("sha256:") else {
        return Err(DeploymentError::InvalidDigest(digest.to_owned()));
    };
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(DeploymentError::InvalidDigest(digest.to_owned()));
    }
    Ok(())
}

fn quarantine_path(root: &Path, upload: &Path, reason: &str) -> PathBuf {
    let name = upload.file_name().unwrap_or_default().to_string_lossy();
    root.join("quarantine").join(format!("{name}.{reason}"))
}

fn sync_parent(path: &Path) -> Result<(), DeploymentError> {
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use remotemedia_bundle::sha256_digest;

    use super::*;

    #[test]
    fn interrupted_upload_resumes_and_publishes_only_after_verification() {
        let temp = tempfile::tempdir().unwrap();
        let store = ContentStore::open(temp.path()).unwrap();
        let bytes = b"portable bundle blob";
        let descriptor = DescriptorIdentity {
            digest: sha256_digest(bytes),
            size: bytes.len() as u64,
        };
        let mut first = store.begin_upload(descriptor.clone()).unwrap();
        first.append(0, &bytes[..8]).unwrap();
        drop(first);
        assert!(!store.contains(&descriptor.digest));

        let mut resumed = store.begin_upload(descriptor.clone()).unwrap();
        assert_eq!(resumed.offset(), 8);
        resumed.append(8, &bytes[8..]).unwrap();
        resumed.finish().unwrap();
        assert_eq!(store.read(&descriptor.digest).unwrap(), bytes);
        assert!(store.missing([&descriptor]).is_empty());
    }

    #[test]
    fn corrupt_upload_never_enters_verified_cache() {
        let temp = tempfile::tempdir().unwrap();
        let store = ContentStore::open(temp.path()).unwrap();
        let descriptor = DescriptorIdentity {
            digest: format!("sha256:{}", "a".repeat(64)),
            size: 3,
        };
        let mut upload = store.begin_upload(descriptor.clone()).unwrap();
        upload.append(0, b"bad").unwrap();
        assert!(matches!(
            upload.finish(),
            Err(DeploymentError::DigestMismatch { .. })
        ));
        assert!(!store.contains(&descriptor.digest));
    }
}
