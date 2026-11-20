//! Aggregate resource accounting and per-controller ownership on cgroup v2.

use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::{Error, Result};

const PREFIX: &str = "rm-prepare-";
const CLEANUP_TIMEOUT: Duration = Duration::from_secs(3);
const RETRY_INTERVAL: Duration = Duration::from_millis(20);

#[must_use = "the preparation owner must confirm cgroup cleanup"]
pub struct ProtectedPreparationCgroup {
    directory: PathBuf,
    membership: Arc<File>,
    _owner: File,
    closed: bool,
}

impl ProtectedPreparationCgroup {
    pub(super) fn check(root: &Path, processes: u32) -> io::Result<(PathBuf, u64)> {
        if !root.is_absolute() {
            return Err(invalid_limits());
        }
        let root = root.canonicalize()?;
        let directory = File::open(&root)?;
        let mut stat = std::mem::MaybeUninit::<libc::statfs>::uninit();
        // SAFETY: fstatfs initializes this structure on success.
        if unsafe { libc::fstatfs(directory.as_raw_fd(), stat.as_mut_ptr()) } != 0 {
            return Err(io::Error::last_os_error());
        }
        if unsafe { stat.assume_init() }.f_type != libc::CGROUP2_SUPER_MAGIC as libc::c_long {
            return Err(invalid_limits());
        }
        let controllers = fs::read_to_string(root.join("cgroup.subtree_control"))?;
        if ["cpu", "memory", "pids"].iter().any(|controller| {
            !controllers
                .split_whitespace()
                .any(|name| &name == controller)
        }) || !fs::read_to_string(root.join("cgroup.procs"))?
            .trim()
            .is_empty()
        {
            return Err(invalid_limits());
        }
        let capacity = pid_limit(&root)?;
        if processes == 0
            || u64::from(processes) > capacity
            || !positive(fs::read_to_string(root.join("memory.max"))?.trim())
        {
            return Err(invalid_limits());
        }
        let cpu = fs::read_to_string(root.join("cpu.max"))?;
        let mut fields = cpu.split_whitespace();
        if !fields.next().is_some_and(positive)
            || !fields.next().is_some_and(positive)
            || fields.next().is_some()
            || fs::read_to_string(root.join("memory.swap.max"))?.trim() != "0"
        {
            return Err(invalid_limits());
        }
        Ok((root, capacity))
    }

    pub(super) async fn reserve(root: PathBuf, processes: u32) -> Result<Self> {
        tokio::task::spawn_blocking(move || Self::open(&root, processes))
            .await
            .map_err(|error| Error::Execution(error.to_string()))?
            .map_err(|error| Error::ConfigError(format!("protected preparation cgroup: {error}")))
    }

    fn open(root: &Path, processes: u32) -> io::Result<Self> {
        let (root, _) = Self::check(root, processes)?;
        // Serialize creation and abandoned-scope recovery across dedicated
        // Runners. The per-scope lock stays held for the controller lifetime.
        let parent = File::open(&root)?;
        let deadline = Instant::now() + CLEANUP_TIMEOUT;
        while !try_lock(&parent)? {
            wait(deadline)?;
        }
        let (_, mut available) = Self::check(&root, processes)?;
        for entry in fs::read_dir(&root)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            if !owned_name(&entry.file_name().to_string_lossy()) {
                return Err(io::Error::other(
                    "preparation cgroup parent has an unrelated child",
                ));
            }
            let file = match File::open(entry.path()) {
                Ok(file) => file,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error),
            };
            if try_lock(&file)? {
                if let Err(error) = cleanup(&entry.path(), deadline) {
                    // A live owner can finish removal between our directory
                    // scan and acquisition of the now-unlinked inode's lock.
                    if error.kind() != io::ErrorKind::NotFound || entry.path().try_exists()? {
                        return Err(error);
                    }
                }
            } else {
                match pid_limit(&entry.path()) {
                    Ok(limit) => available = available.saturating_sub(limit),
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error),
                }
            }
        }
        // cgroup.procs permits migration past pids.max. Reserve every leaf's
        // full allowance before admitting a controller from outside the parent.
        if available < u64::from(processes) {
            return Err(io::Error::other(
                "protected preparation process capacity is reserved",
            ));
        }

        let directory = root.join(format!("{PREFIX}{}", uuid::Uuid::new_v4().simple()));
        fs::create_dir(&directory)?;
        let result = (|| {
            let owner = File::open(&directory)?;
            if !try_lock(&owner)? {
                return Err(io::Error::other("preparation cgroup is already owned"));
            }
            fs::write(directory.join("pids.max"), processes.to_string())?;
            if pid_limit(&directory)? != u64::from(processes) {
                return Err(invalid_limits());
            }
            // Check that the leaf supports recursive termination before spawning.
            OpenOptions::new()
                .write(true)
                .open(directory.join("cgroup.kill"))?;
            let membership = OpenOptions::new()
                .write(true)
                .open(directory.join("cgroup.procs"))?;
            Ok(Self {
                directory: directory.clone(),
                membership: Arc::new(membership),
                _owner: owner,
                closed: false,
            })
        })();
        if result.is_err() {
            let _ = fs::remove_dir(&directory);
        }
        result
    }

    pub(super) fn verify_root(&self, root: &Path, processes: u32) -> Result<()> {
        let (root, _) =
            Self::check(root, processes).map_err(|error| Error::ConfigError(error.to_string()))?;
        if self.directory.parent() != Some(root.as_path()) {
            return Err(Error::ConfigError(
                "protected preparation cgroup changed".into(),
            ));
        }
        Ok(())
    }

    pub(super) fn membership(&self) -> Arc<File> {
        Arc::clone(&self.membership)
    }

    pub async fn close(mut self) -> Result<()> {
        tokio::task::spawn_blocking(move || {
            cleanup(&self.directory, Instant::now() + CLEANUP_TIMEOUT)?;
            self.closed = true;
            Ok(())
        })
        .await
        .map_err(|error| Error::Execution(error.to_string()))?
        .map_err(|error: io::Error| {
            Error::Execution(format!("protected preparation cgroup cleanup: {error}"))
        })
    }
}

impl Drop for ProtectedPreparationCgroup {
    fn drop(&mut self) {
        if !self.closed {
            // Fast fallback. The next reservation confirms emptiness and reaps
            // the directory under the ownership lock.
            let _ = fs::write(self.directory.join("cgroup.kill"), "1");
        }
    }
}

fn cleanup(directory: &Path, deadline: Instant) -> io::Result<()> {
    let events = directory.join("cgroup.events");
    if !empty(&events)? {
        fs::write(directory.join("cgroup.kill"), "1")?;
        while !empty(&events)? {
            wait(deadline)?;
        }
    }
    fs::remove_dir(directory)
}

fn empty(events: &Path) -> io::Result<bool> {
    for line in fs::read_to_string(events)?.lines() {
        match line {
            "populated 0" => return Ok(true),
            "populated 1" => return Ok(false),
            _ => {}
        }
    }
    Err(io::Error::other("missing cgroup population state"))
}

fn owned_name(name: &str) -> bool {
    name.strip_prefix(PREFIX)
        .is_some_and(|id| super::lower_hex(id, 32))
}

fn positive(value: &str) -> bool {
    value.parse::<u64>().is_ok_and(|value| value > 0)
}

fn pid_limit(root: &Path) -> io::Result<u64> {
    fs::read_to_string(root.join("pids.max"))?
        .trim()
        .parse::<u64>()
        .ok()
        .filter(|limit| *limit > 0)
        .ok_or_else(invalid_limits)
}

fn try_lock(file: &File) -> io::Result<bool> {
    // SAFETY: the live directory descriptor remains owned by the caller.
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
        return Ok(true);
    }
    let error = io::Error::last_os_error();
    if error.kind() == io::ErrorKind::WouldBlock {
        Ok(false)
    } else {
        Err(error)
    }
}

fn wait(deadline: Instant) -> io::Result<()> {
    if Instant::now() >= deadline {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "preparation cgroup is busy",
        ));
    }
    std::thread::sleep(RETRY_INTERVAL);
    Ok(())
}

fn invalid_limits() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, "expected a delegated cgroup v2 parent with finite CPU, memory and process limits and swap disabled")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordinary_directories_cannot_masquerade_as_preparation_cgroups() {
        let directory = tempfile::tempdir().unwrap();
        for (file, value) in [
            ("cgroup.subtree_control", "cpu memory pids\n"),
            ("cgroup.procs", ""),
            ("cpu.max", "100000 100000\n"),
            ("memory.max", "1048576\n"),
            ("memory.swap.max", "0\n"),
            ("pids.max", "32\n"),
        ] {
            fs::write(directory.path().join(file), value).unwrap();
        }
        assert!(ProtectedPreparationCgroup::check(directory.path(), 16).is_err());
    }
}
