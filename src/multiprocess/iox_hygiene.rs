//! Keeping the machine's iceoryx2 state from growing.
//!
//! A process that dies without dropping its iceoryx2 node (a killed worker,
//! a crash, `os._exit`) leaves its node, its ports and its shared memory
//! registered. Something must remove those "dead nodes", or every pipeline
//! that ever ran leaves files behind under the iceoryx2 root and the temp
//! directory until node creation and service opening start failing.
//!
//! iceoryx2 0.10 can remove dead nodes itself, and by default every node does
//! so when it is created, when it opens a service and when it is dropped. On
//! Windows that is unsafe when processes do it at the same time: the liveness
//! check emulates `F_GETLK` by briefly taking the lock it inspects, so one
//! sweeping process can see another's probe as a live owner and iceoryx2
//! fatal-panics ("the Node is still alive") in the middle of a sweep.
//!
//! So every node this code base creates turns those sweeps off
//! ([`node_config_without_dead_node_sweeps`]) and [`cleanup_dead_nodes`] is the
//! only sweep. It runs one process at a time on the machine, cannot unwind
//! into its caller, and on Windows it also repairs what iceoryx2 cannot:
//!
//! - a dead node whose cleanup was interrupted after its monitor's state and
//!   owner-lock files were removed but before its context file was. iceoryx2
//!   no longer lists such a node at all, so its node directory, its services
//!   and its shared memory are never removed. When the process named in the
//!   context file no longer exists, the two missing files are restored and
//!   iceoryx2's own cleanup removes the node.
//! - the `*.shm_state` files iceoryx2 keeps in the temp directory next to
//!   each named file mapping. Windows destroys a mapping with its last handle,
//!   so after a crash the state file is all that is left, and iceoryx2 does
//!   not remove every kind of them with a dead node (a killed listener's
//!   `.event_mgmt` among others). A state file whose mapping no longer exists
//!   is removed.

use iceoryx2::config::Config;
use iceoryx2::node::NodeState;
use iceoryx2::prelude::{ipc, CallbackProgression};
use std::time::Duration;

/// How long a sweep waits for another process's sweep to finish before it
/// leaves the work to that one.
const MACHINE_LOCK_WAIT: Duration = Duration::from_secs(10);

/// iceoryx2 config for every node this process creates: the global config
/// with iceoryx2's own dead-node sweeps off. See the module documentation.
pub fn node_config_without_dead_node_sweeps() -> Config {
    let mut config = Config::global_config().clone();
    config.global.node.cleanup_dead_nodes_on_creation = false;
    config.global.node.cleanup_dead_nodes_on_destruction = false;
    config.global.service.cleanup_dead_nodes_on_open = false;
    config
}

/// Remove the stale resources of every dead iceoryx2 node on the machine,
/// without creating a node. Returns `(removed, failed)`. Nodes that cannot be
/// removed now are left for a later sweep.
///
/// Blocks for at most a few seconds while another process sweeps; call it
/// from a thread that may block.
pub fn cleanup_dead_nodes(config: &Config) -> (u64, u64) {
    // One sweep at a time in this process, and on the machine.
    static SWEEPING: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let _sweeping = SWEEPING
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let Some(_machine) = machine_lock::acquire(MACHINE_LOCK_WAIT) else {
        tracing::debug!("iceoryx2: another process is sweeping dead nodes; skipping this sweep");
        return (0, 0);
    };

    let swept = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        #[cfg(windows)]
        {
            let rearmed = win::rearm_interrupted_cleanups(config);
            if rearmed > 0 {
                tracing::info!(
                    "iceoryx2: resuming the interrupted cleanup of {rearmed} dead node(s)"
                );
            }
        }
        sweep(config)
    }));
    let counts = swept.unwrap_or_else(|_| {
        tracing::warn!("iceoryx2: the dead-node sweep panicked; the remaining nodes are left for the next sweep");
        (0, 1)
    });

    #[cfg(windows)]
    win::collect_orphaned_shm_state();

    counts
}

/// One pass over the node list: remove every dead node. Returns
/// `(removed, failed)`.
fn sweep(config: &Config) -> (u64, u64) {
    let (mut removed, mut failed) = (0, 0);
    let listed = iceoryx2::node::Node::<ipc::Service>::list(config, |state| {
        if let NodeState::Dead(dead_node) = state {
            match dead_node.blocking_remove_stale_resources(Duration::ZERO) {
                Ok(_) => removed += 1,
                Err(_) => failed += 1,
            }
        }
        CallbackProgression::Continue
    });
    if let Err(error) = listed {
        tracing::debug!("iceoryx2: could not list nodes for the dead-node sweep: {error:?}");
    }
    (removed, failed)
}

#[cfg(windows)]
mod machine_lock {
    use std::time::Duration;
    use windows::core::w;
    use windows::Win32::Foundation::{CloseHandle, HANDLE, WAIT_ABANDONED, WAIT_OBJECT_0};
    use windows::Win32::System::Threading::{CreateMutexW, ReleaseMutex, WaitForSingleObject};

    /// Held while this process sweeps. A named mutex: Windows releases it
    /// when its owner dies, so a crashed sweep never blocks the next one.
    pub(super) struct Guard(HANDLE);

    pub(super) fn acquire(wait: Duration) -> Option<Guard> {
        // SAFETY: plain Win32 calls on a handle owned by this function.
        unsafe {
            let handle = CreateMutexW(
                None,
                false,
                w!("Local\\remotemedia-iceoryx2-dead-node-sweep"),
            )
            .ok()?;
            let waited = WaitForSingleObject(handle, wait.as_millis().min(u32::MAX as u128) as u32);
            if waited == WAIT_OBJECT_0 || waited == WAIT_ABANDONED {
                Some(Guard(handle))
            } else {
                let _ = CloseHandle(handle);
                None
            }
        }
    }

    impl Drop for Guard {
        fn drop(&mut self) {
            // SAFETY: this guard owns the mutex and the handle.
            unsafe {
                let _ = ReleaseMutex(self.0);
                let _ = CloseHandle(self.0);
            }
        }
    }
}

#[cfg(not(windows))]
mod machine_lock {
    /// The race the machine-wide lock prevents is specific to iceoryx2's
    /// Windows lock emulation; elsewhere the in-process lock is enough.
    pub(super) struct Guard;

    pub(super) fn acquire(_wait: std::time::Duration) -> Option<Guard> {
        Some(Guard)
    }
}

#[cfg(windows)]
mod win {
    use iceoryx2::config::Config;
    use std::path::{Path, PathBuf};
    use std::sync::Mutex;
    use std::time::{Duration, Instant, SystemTime};
    use windows::core::PCSTR;
    use windows::Win32::Foundation::{
        CloseHandle, ERROR_FILE_NOT_FOUND, ERROR_INVALID_PARAMETER, FILETIME,
    };
    use windows::Win32::System::Memory::{OpenFileMappingA, FILE_MAP_READ};
    use windows::Win32::System::Threading::{
        GetExitCodeProcess, GetProcessTimes, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
    };

    /// `STILL_ACTIVE`: the exit code of a process that has not exited.
    const STILL_ACTIVE: u32 = 259;

    /// A state file younger than this may belong to a mapping that is being
    /// created right now (the file comes first), so it is never removed.
    const SHM_STATE_MIN_AGE: Duration = Duration::from_secs(120);

    /// The temp directory is scanned at most this often per process.
    const SHM_STATE_SCAN_INTERVAL: Duration = Duration::from_secs(60);

    const SHM_STATE_SUFFIX: &str = ".shm_state";

    /// Find dead nodes whose cleanup was interrupted (a monitor context file
    /// without its state or owner-lock file) and restore the missing files so
    /// that iceoryx2 lists them as dead and can clean them up. Returns how
    /// many were restored.
    ///
    /// Only called while holding the machine-wide sweep lock: no cooperating
    /// process is cleaning up any node at the same time.
    pub(super) fn rearm_interrupted_cleanups(config: &Config) -> usize {
        let nodes = PathBuf::from(config.global.root_path().to_string())
            .join(config.global.node.directory.to_string());
        let prefix = config.global.prefix.to_string();
        let suffix = config.global.node.monitor_suffix.to_string();
        let context_suffix = format!("{suffix}_context");
        let Ok(entries) = std::fs::read_dir(&nodes) else {
            return 0;
        };
        let mut rearmed = 0;
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(id) = name
                .to_str()
                .and_then(|n| n.strip_prefix(prefix.as_str()))
                .and_then(|n| n.strip_suffix(context_suffix.as_str()))
            else {
                continue;
            };
            let state = nodes.join(format!("{prefix}{id}{suffix}"));
            let owner_lock = nodes.join(format!("{prefix}{id}{suffix}_owner_lock"));
            if state.exists() && owner_lock.exists() {
                // Alive, dead and listed, or held by a cleaner.
                continue;
            }
            // The context names the process the node belonged to and when it
            // got its id: seconds, nanoseconds, pid, counter. A node still
            // being created has an unreadable context and is skipped.
            let Ok(bytes) = std::fs::read(entry.path()) else {
                continue;
            };
            if bytes.len() < 12 {
                continue;
            }
            let created = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as u64;
            let pid = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
            if pid == std::process::id() || process_may_be_alive(pid, created) {
                continue;
            }
            let mut ok = true;
            for missing in [&state, &owner_lock] {
                if !missing.exists() {
                    if let Err(error) = std::fs::OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .open(missing)
                    {
                        tracing::debug!(
                            "iceoryx2: could not restore {}: {error}",
                            missing.display()
                        );
                        ok = false;
                    }
                }
            }
            if ok {
                rearmed += 1;
            }
        }
        rearmed
    }

    /// Whether process `pid` might still be the one that created a node at
    /// `created_unix` seconds. Unknown counts as alive.
    fn process_may_be_alive(pid: u32, created_unix: u64) -> bool {
        // SAFETY: plain Win32 calls on a handle owned by this function.
        unsafe {
            let handle = match OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, false, pid) {
                Ok(handle) => handle,
                Err(error) => return error.code() != ERROR_INVALID_PARAMETER.to_hresult(),
            };
            let mut exit_code = 0u32;
            let running =
                GetExitCodeProcess(handle, &mut exit_code).is_ok() && exit_code == STILL_ACTIVE;
            let (mut start, mut exit, mut kernel, mut user) = (
                FILETIME::default(),
                FILETIME::default(),
                FILETIME::default(),
                FILETIME::default(),
            );
            let started = GetProcessTimes(handle, &mut start, &mut exit, &mut kernel, &mut user)
                .is_ok()
                .then(|| filetime_to_unix(start));
            let _ = CloseHandle(handle);
            if !running {
                return false;
            }
            // A process started after the node existed reuses the pid.
            match started {
                Some(started) => started <= created_unix + 1,
                None => true,
            }
        }
    }

    fn filetime_to_unix(time: FILETIME) -> u64 {
        const UNIX_EPOCH_AS_FILETIME: u64 = 116_444_736_000_000_000;
        let ticks = (u64::from(time.dwHighDateTime) << 32) | u64::from(time.dwLowDateTime);
        ticks.saturating_sub(UNIX_EPOCH_AS_FILETIME) / 10_000_000
    }

    /// Remove the iceoryx2 `*.shm_state` files in the temp directory whose
    /// file mapping no longer exists. At most once per
    /// [`SHM_STATE_SCAN_INTERVAL`].
    pub(super) fn collect_orphaned_shm_state() {
        static LAST_SCAN: Mutex<Option<Instant>> = Mutex::new(None);
        {
            let mut last = LAST_SCAN
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if last.is_some_and(|at| at.elapsed() < SHM_STATE_SCAN_INTERVAL) {
                return;
            }
            *last = Some(Instant::now());
        }
        let temp = String::from_utf8_lossy(iceoryx2_pal_configuration::TEMP_DIRECTORY).into_owned();
        let prefix = Config::global_config().global.prefix.to_string();
        let removed = remove_orphaned_shm_state(Path::new(&temp), &prefix, SHM_STATE_MIN_AGE);
        if removed > 0 {
            tracing::info!(
                "iceoryx2: removed {removed} orphaned shared-memory state file(s) from {temp}"
            );
        }
    }

    /// Remove every `{prefix}*.shm_state` file in `dir`, older than
    /// `min_age`, whose named file mapping does not exist. Returns how many
    /// were removed.
    pub(crate) fn remove_orphaned_shm_state(dir: &Path, prefix: &str, min_age: Duration) -> usize {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return 0;
        };
        let now = SystemTime::now();
        let mut removed = 0;
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            let Some(mapping) = name.strip_suffix(SHM_STATE_SUFFIX) else {
                continue;
            };
            if !name.starts_with(prefix) {
                continue;
            }
            let old_enough = entry
                .metadata()
                .and_then(|m| m.modified())
                .ok()
                .and_then(|modified| now.duration_since(modified).ok())
                .is_some_and(|age| age >= min_age);
            if !old_enough || !mapping_is_gone(mapping) {
                continue;
            }
            if std::fs::remove_file(entry.path()).is_ok() {
                removed += 1;
            }
        }
        removed
    }

    /// Whether the named file mapping is known not to exist. iceoryx2 makes
    /// the same check before it reuses a leftover state file.
    fn mapping_is_gone(name: &str) -> bool {
        let Ok(c_name) = std::ffi::CString::new(name) else {
            return false;
        };
        // SAFETY: plain Win32 calls; `c_name` outlives the call.
        unsafe {
            match OpenFileMappingA(FILE_MAP_READ.0, false, PCSTR(c_name.as_ptr().cast())) {
                Ok(handle) => {
                    let _ = CloseHandle(handle);
                    false
                }
                Err(error) => error.code() == ERROR_FILE_NOT_FOUND.to_hresult(),
            }
        }
    }
}

#[cfg(all(test, windows))]
mod tests {
    use super::win::remove_orphaned_shm_state;
    use std::time::Duration;

    #[test]
    fn orphaned_shm_state_files_are_removed_and_others_kept() {
        let dir = tempfile::tempdir().unwrap();
        let orphan = dir
            .path()
            .join("iox2_test_no_such_mapping_7f3a.event_mgmt.shm_state");
        let foreign = dir.path().join("other_no_such_mapping_7f3a.shm_state");
        let unrelated = dir.path().join("iox2_test_no_such_mapping_7f3a.txt");
        for file in [&orphan, &foreign, &unrelated] {
            std::fs::write(file, 0u64.to_le_bytes()).unwrap();
        }

        // Too young: kept.
        assert_eq!(
            remove_orphaned_shm_state(dir.path(), "iox2_", Duration::from_secs(3600)),
            0
        );
        assert!(orphan.exists());

        assert_eq!(
            remove_orphaned_shm_state(dir.path(), "iox2_", Duration::ZERO),
            1
        );
        assert!(!orphan.exists());
        assert!(foreign.exists());
        assert!(unrelated.exists());
    }
}
