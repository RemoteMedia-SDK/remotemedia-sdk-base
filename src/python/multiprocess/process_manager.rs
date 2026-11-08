//! Process management for multiprocess Python nodes
//!
//! Handles spawning, monitoring, and lifecycle management of Python node processes.

use crate::{Error, Result};
use serde_json::Value;
use std::collections::HashMap;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock};

#[cfg(feature = "docker")]
use bollard::Docker;

use super::multiprocess_executor::MultiprocessConfig;

/// Process lifecycle states
#[derive(Debug, Clone, PartialEq)]
pub enum ProcessStatus {
    Idle,          // Process created but not initialized
    Initializing,  // Loading models/resources
    Ready,         // Initialized and waiting for data
    Processing,    // Actively processing data
    Stopping,      // Graceful shutdown initiated
    Stopped,       // Process terminated
    Error(String), // Failed with error message
}

/// Exit reasons for process termination
#[derive(Debug, Clone)]
pub enum ExitReason {
    Normal,     // Clean exit
    Error(i32), // Exit with error code
    Killed,     // Terminated by signal
    Timeout,    // Initialization timeout
}

/// Unified handle for processes and containers
#[derive(Debug)]
pub enum ExecutionTarget {
    /// Standard process (existing)
    Process(Child),

    /// Docker container (NEW)
    #[cfg(feature = "docker")]
    Container {
        /// Docker container ID
        container_id: String,
        /// Docker client reference
        docker_client: Arc<Docker>,
    },
}

impl ExecutionTarget {
    /// Check if the execution target is still running
    pub async fn is_alive(&self) -> bool {
        match self {
            ExecutionTarget::Process(_child) => {
                // Check process status
                // TODO: This would need the child to be mutable to call try_wait()
                // For now, return true as placeholder
                true
            }
            #[cfg(feature = "docker")]
            ExecutionTarget::Container {
                container_id,
                docker_client,
            } => {
                // Check container status
                use bollard::query_parameters::InspectContainerOptions;
                match docker_client
                    .inspect_container(container_id, None::<InspectContainerOptions>)
                    .await
                {
                    Ok(info) => info.state.and_then(|s| s.running).unwrap_or(false),
                    Err(_) => false,
                }
            }
        }
    }

    /// Terminate the execution target
    pub async fn terminate(&self, timeout: std::time::Duration) -> Result<()> {
        match self {
            ExecutionTarget::Process(_child) => {
                // Terminate process
                // Implementation would send SIGTERM, wait, then SIGKILL
                Ok(())
            }
            #[cfg(feature = "docker")]
            ExecutionTarget::Container {
                container_id,
                docker_client,
            } => {
                // Stop container with timeout
                use bollard::query_parameters::StopContainerOptionsBuilder;
                let options = StopContainerOptionsBuilder::default()
                    .t(timeout.as_secs() as i32)
                    .build();
                docker_client
                    .stop_container(container_id, Some(options))
                    .await
                    .map_err(|e| Error::Execution(format!("Failed to stop container: {}", e)))
            }
        }
    }
}

/// Handle to a running process
#[derive(Debug, Clone)]
pub struct ProcessHandle {
    /// Process ID
    pub id: u32,

    /// Node ID in the pipeline
    pub node_id: String,

    /// Node type identifier
    pub node_type: String,

    /// Session ID
    pub session_id: String,

    /// Current process status
    pub status: Arc<RwLock<ProcessStatus>>,

    /// Process start time
    pub started_at: Instant,

    /// Internal process handle
    pub inner: Arc<Mutex<Option<Child>>>,

    /// Execution target (process or container)
    pub execution_target: Arc<Mutex<Option<ExecutionTarget>>>,
}

impl ProcessHandle {
    /// Check if the process is still alive
    pub async fn is_alive(&self) -> bool {
        if let Some(ref mut child) = *self.inner.lock().await {
            child.try_wait().ok().flatten().is_none()
        } else {
            false
        }
    }

    /// Get the exit status if process has terminated
    pub async fn exit_status(&self) -> Option<std::process::ExitStatus> {
        if let Some(ref mut child) = *self.inner.lock().await {
            child.try_wait().ok().flatten()
        } else {
            None
        }
    }

    /// Kill the process forcefully
    pub async fn kill(&self) -> Result<()> {
        if let Some(ref mut child) = *self.inner.lock().await {
            child
                .kill()
                .map_err(|e| Error::Execution(format!("Failed to kill process: {}", e)))?;
        }
        Ok(())
    }
}

/// Process spawn configuration
#[derive(Debug, Clone)]
pub struct SpawnConfig {
    /// Python executable path
    pub python_executable: std::path::PathBuf,

    /// Additional Python path entries
    pub python_path: Vec<std::path::PathBuf>,

    /// Environment variables
    pub env_vars: HashMap<String, String>,

    /// Working directory
    pub working_dir: Option<std::path::PathBuf>,

    /// Capture stdout/stderr
    pub capture_output: bool,

    /// Additional Python modules to import for node registration
    /// These modules are imported before looking up the node type,
    /// allowing tests and custom applications to register nodes dynamically.
    pub register_modules: Vec<String>,
}

impl Default for SpawnConfig {
    fn default() -> Self {
        Self {
            python_executable: std::path::PathBuf::from("python"),
            python_path: Vec::new(),
            env_vars: HashMap::new(),
            working_dir: None,
            capture_output: true,
            register_modules: Vec::new(),
        }
    }
}

/// Process manager for spawning and monitoring Python nodes
pub struct ProcessManager {
    /// Each reporting service retains the exact initialization ledger until joined exit.
    copy_authorities: Arc<std::sync::Mutex<HashMap<u32, Arc<super::copy_authority::CopyAuthority>>>>,
    /// Active processes
    processes: Arc<RwLock<HashMap<u32, ProcessHandle>>>,

    /// Spawn configuration (wrapped in RwLock for runtime updates)
    spawn_config: Arc<RwLock<SpawnConfig>>,

    /// Configuration
    #[allow(dead_code)] // Reserved for future process management policies
    config: MultiprocessConfig,

    /// Process event handlers
    exit_handlers: Arc<RwLock<Vec<Box<dyn Fn(u32, ExitReason) + Send + Sync>>>>,
}

impl Drop for ProcessManager {
    fn drop(&mut self) {
        // Best-effort synchronous kill of all tracked child processes.
        // This runs even when the tokio runtime is shutting down (where async
        // code would panic), so we use try_write() / try_lock() instead of
        // .await variants.
        if let Ok(processes) = self.processes.try_write() {
            for (pid, handle) in processes.iter() {
                // Try to kill via the Child handle first
                if let Ok(mut guard) = handle.inner.try_lock() {
                    if let Some(ref mut child) = *guard {
                        let _ = child.kill();
                    }
                } else {
                    // Fallback: send SIGKILL directly via pid
                    #[cfg(unix)]
                    {
                        use nix::sys::signal::{kill, Signal};
                        use nix::unistd::Pid;
                        let _ = kill(Pid::from_raw(*pid as i32), Signal::SIGKILL);
                    }
                }
            }
        }
    }
}

impl ProcessManager {
    /// Create a new process manager
    pub fn new(config: MultiprocessConfig) -> Self {
        let spawn_config = SpawnConfig {
            python_executable: config.python_executable.clone(),
            python_path: config.python_path.clone(),
            ..Default::default()
        };

        Self {
            processes: Arc::new(RwLock::new(HashMap::new())),
            copy_authorities: Arc::new(std::sync::Mutex::new(HashMap::new())),
            spawn_config: Arc::new(RwLock::new(spawn_config)),
            config,
            exit_handlers: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Get access to the spawn configuration (for updating python_executable etc.)
    pub fn spawn_config(&self) -> &Arc<RwLock<SpawnConfig>> {
        &self.spawn_config
    }

    /// Register a Python module for node registration
    ///
    /// This module will be imported before looking up node types,
    /// allowing tests and custom applications to register nodes dynamically.
    pub async fn register_module(&self, module: String) {
        let mut config = self.spawn_config.write().await;
        if !config.register_modules.contains(&module) {
            config.register_modules.push(module);
        }
    }

    /// Get the list of registered modules
    pub async fn get_registered_modules(&self) -> Vec<String> {
        self.spawn_config.read().await.register_modules.clone()
    }

    /// Spawn a new Python node process
    pub async fn spawn_node(
        &self,
        node_type: &str,
        node_id: &str,
        params: &Value,
        session_id: &str,
    ) -> Result<ProcessHandle> {
        self.spawn_node_with_copy_context(node_type, node_id, params, session_id, None).await
    }

    /// Native legacy launch with acknowledged reports to the retained host ledger.
    pub async fn spawn_node_with_copy_context(
        &self,
        node_type: &str,
        node_id: &str,
        params: &Value,
        session_id: &str,
        copies: Option<remotemedia_types::CopyContext>,
    ) -> Result<ProcessHandle> {
        let authority = copies.map(|copies| {
            super::copy_authority::CopyAuthority::start(copies, node_id, session_id)
        }).transpose().map_err(|error| Error::Execution(format!("Copy authority startup failed: {error}")))?;
        tracing::info!(
            "Spawning process for node: {} ({}) in session: {}",
            node_id,
            node_type,
            session_id
        );

        // Read spawn config once at the start
        let spawn_config = self.spawn_config.read().await;

        // Build command for Python subprocess
        let mut command = Command::new(&spawn_config.python_executable);

        // Add multiprocess runner module
        command.args([
            "-m",
            "remotemedia.core.multiprocessing.runner",
            "--node-type",
            node_type,
            "--node-id",
            node_id,
            "--session-id",
            session_id,       // Pass session_id for channel naming
            "--params-stdin", // Signal that params come from stdin
        ]);

        if authority.is_some() {
            command.arg("--copy-authority-stdin");
        }

        // Add custom node registration modules
        for module in &spawn_config.register_modules {
            command.args(["--register-module", module]);
        }

        // Set environment variables
        for (key, value) in &spawn_config.env_vars {
            command.env(key, value);
        }

        // Set Python path
        if !spawn_config.python_path.is_empty() {
            let python_path = spawn_config
                .python_path
                .iter()
                .map(|p| p.to_string_lossy())
                .collect::<Vec<_>>()
                .join(if cfg!(windows) { ";" } else { ":" });

            command.env("PYTHONPATH", python_path);
        }

        // Set working directory
        if let Some(ref dir) = spawn_config.working_dir {
            command.current_dir(dir);
        }

        // Configure process group and parent-death signal for cleanup
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            command.process_group(0);
            // SAFETY: prctl(PR_SET_PDEATHSIG) is async-signal-safe.
            // When the parent process dies (crash, SIGKILL, normal exit),
            // the kernel sends SIGTERM to this child automatically.
            // Only available on Linux; macOS/BSD lack PR_SET_PDEATHSIG.
            #[cfg(target_os = "linux")]
            unsafe {
                command.pre_exec(|| {
                    if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM) != 0 {
                        return Err(std::io::Error::last_os_error());
                    }
                    Ok(())
                });
            }
        }

        // No console window per Python node when the host is a GUI app, and
        // start suspended: it joins the kill-on-close job before it can run
        // (a venv's python.exe is a launcher that starts the real
        // interpreter, which must be born inside the job too).
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            command.creation_flags(no_window_flag().unwrap_or(0) | win_job::CREATE_SUSPENDED);
        }

        // Configure I/O
        command.stdin(Stdio::piped()); // Always need stdin for params
        let capture_output = spawn_config.capture_output;
        if capture_output {
            command.stdout(Stdio::piped());
            command.stderr(Stdio::piped());
        }

        // Drop the lock before spawning (don't hold across await points)
        drop(spawn_config);

        // Spawn the process
        let mut child = command
            .spawn()
            .map_err(|e| Error::Execution(format!("Failed to spawn process: {}", e)))?;

        let pid = child.id();

        // Windows has no parent-death signal: without the job, a worker
        // outlives a host that exits or crashes and keeps answering on the
        // next run's `session_0_*` channels.
        #[cfg(windows)]
        win_job::adopt_and_resume(&child);

        // Capture stderr for logging if process crashes
        let stderr_handle = if capture_output {
            child.stderr.take()
        } else {
            None
        };

        // Write params to stdin (avoids command-line length limits)
        {
            use std::io::Write;
            let bootstrap = match &authority {
                Some(authority) => serde_json::json!({
                    "params": params, "copy_authority": authority.bootstrap(),
                }),
                None => params.clone(),
            };
            // Always close stdin, including null parameters. On failure reap the
            // newly spawned child before releasing its reporting capability.
            let result = child.stdin.take().ok_or_else(|| std::io::Error::other("missing child stdin"))
                .and_then(|mut stdin| stdin.write_all(bootstrap.to_string().as_bytes()));
            if let Err(error) = result {
                let _ = child.kill();
                let _ = child.wait();
                return Err(Error::Execution(format!("Failed to write child bootstrap: {error}")));
            }
        }
        if let Some(authority) = authority {
            self.copy_authorities.lock().unwrap().insert(pid, Arc::new(authority));
        }

        // Create process handle
        let handle = ProcessHandle {
            id: pid,
            node_id: node_id.to_string(),
            node_type: node_type.to_string(),
            session_id: session_id.to_string(),
            status: Arc::new(RwLock::new(ProcessStatus::Initializing)),
            started_at: Instant::now(),
            inner: Arc::new(Mutex::new(Some(child))),
            execution_target: Arc::new(Mutex::new(None)),
        };

        // Register process
        self.processes.write().await.insert(pid, handle.clone());

        // Start monitoring
        self.start_monitoring(handle.clone(), stderr_handle);

        tracing::info!("Process {} spawned for node {}", pid, node_id);
        Ok(handle)
    }

    /// Terminate a process gracefully
    pub async fn terminate_process(
        &self,
        process: ProcessHandle,
        grace_period: Duration,
    ) -> Result<()> {
        tracing::info!(
            "Terminating process {} for node {}",
            process.id,
            process.node_id
        );

        // Update status
        {
            let mut status = process.status.write().await;
            if !matches!(*status, ProcessStatus::Error(_)) {
                *status = ProcessStatus::Stopping;
            }
        }

        // Try graceful termination first (SIGTERM on Unix, terminate on Windows)
        if let Some(ref mut child) = *process.inner.lock().await {
            #[cfg(unix)]
            {
                use nix::sys::signal::{kill, Signal};
                use nix::unistd::Pid;

                let pid = Pid::from_raw(child.id() as i32);
                let _ = kill(pid, Signal::SIGTERM);
            }

            #[cfg(windows)]
            {
                // Windows doesn't have SIGTERM, just try to kill
                let _ = child.kill();
            }

            // Wait for graceful shutdown
            let start = Instant::now();
            while start.elapsed() < grace_period {
                if child.try_wait().ok().flatten().is_some() {
                    // Process terminated gracefully
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }

            // Force kill if still running
            if child.try_wait().ok().flatten().is_none() {
                tracing::warn!(
                    "Process {} did not terminate gracefully, forcing kill",
                    process.id
                );
                child
                    .kill()
                    .map_err(|e| Error::Execution(format!("Failed to kill process: {}", e)))?;
            }
        }

        if let Some(child) = process.inner.lock().await.as_mut() {
            child.wait().map_err(|error| Error::Execution(format!("Failed to confirm process exit: {error}")))?;
        }
        let authority = self.copy_authorities.lock().unwrap().get(&process.id).cloned();
        let accounting_ok = if let Some(authority) = authority {
            tokio::task::spawn_blocking(move || authority.finish()).await
                .is_ok_and(|result| result.is_ok())
        } else { true };
        // Termination succeeded even if forced shutdown interrupted reporting.
        // Preserve that distinction in status and the failed-operation ledger.
        let mut status = process.status.write().await;
        if !accounting_ok {
            *status = ProcessStatus::Error("Copy accounting interrupted by shutdown".into());
        } else if !matches!(*status, ProcessStatus::Error(_)) {
            *status = ProcessStatus::Stopped;
        }
        drop(status);
        self.processes.write().await.remove(&process.id);

        tracing::info!("Process {} terminated", process.id);
        Ok(())
    }

    /// Start monitoring a process
    fn start_monitoring(&self, process: ProcessHandle, stderr: Option<std::process::ChildStderr>) {
        let processes = self.processes.clone();
        let handlers = self.exit_handlers.clone();
        let copy_authorities = self.copy_authorities.clone();

        // Spawn task to capture stderr output
        if let Some(stderr) = stderr {
            let node_id = process.node_id.clone();
            let pid = process.id;
            tokio::spawn(async move {
                use tokio::io::{AsyncBufReadExt, BufReader};
                let stderr = tokio::process::ChildStderr::from_std(stderr).unwrap();
                let mut lines = BufReader::new(stderr).lines();

                while let Ok(Some(line)) = lines.next_line().await {
                    tracing::warn!("[Process {}] {}: {}", pid, node_id, line);
                }
            });
        }

        // Spawn monitoring task
        tokio::spawn(async move {
            let mut reporting_failed = false;
            loop {
                if !reporting_failed {
                    reporting_failed = copy_authorities.lock().unwrap().get(&process.id)
                        .is_some_and(|authority| authority.failed());
                    if reporting_failed {
                        // An idle worker must not survive loss of its reporting authority.
                        let _ = process.kill().await;
                    }
                }
                // Check process status
                if let Some(exit_status) = process.exit_status().await {
                    // Process exited
                    let authority = copy_authorities.lock().unwrap().get(&process.id).cloned();
                    let accounting_ok = if let Some(authority) = authority {
                        tokio::task::spawn_blocking(move || authority.finish()).await
                            .is_ok_and(|result| result.is_ok())
                    } else { true };
                    copy_authorities.lock().unwrap().remove(&process.id);
                    let reason = if !accounting_ok {
                        tracing::error!("Python copy accounting did not close cleanly for node {}", process.node_id);
                        ExitReason::Error(-1)
                    } else if exit_status.success() {
                        ExitReason::Normal
                    } else if let Some(code) = exit_status.code() {
                        ExitReason::Error(code)
                    } else {
                        ExitReason::Killed
                    };

                    tracing::info!("Process {} exited with reason: {:?}", process.id, reason);

                    // Update status
                    *process.status.write().await = match &reason {
                        ExitReason::Normal => ProcessStatus::Stopped,
                        ExitReason::Error(code) => {
                            ProcessStatus::Error(format!("Process exited with code {}", code))
                        }
                        ExitReason::Killed => {
                            ProcessStatus::Error("Process killed by signal".to_string())
                        }
                        ExitReason::Timeout => {
                            ProcessStatus::Error("Process initialization timeout".to_string())
                        }
                    };

                    // Remove from registry
                    processes.write().await.remove(&process.id);

                    // Call exit handlers
                    for handler in handlers.read().await.iter() {
                        handler(process.id, reason.clone());
                    }

                    break;
                }

                // Check periodically
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        });
    }

    /// Register an exit handler
    pub async fn on_process_exit<F>(&self, handler: F)
    where
        F: Fn(u32, ExitReason) + Send + Sync + 'static,
    {
        self.exit_handlers.write().await.push(Box::new(handler));
    }

    /// Get all active processes
    pub async fn get_processes(&self) -> Vec<ProcessHandle> {
        self.processes.read().await.values().cloned().collect()
    }

    /// Get a specific process by ID
    pub async fn get_process(&self, pid: u32) -> Option<ProcessHandle> {
        self.processes.read().await.get(&pid).cloned()
    }

    /// Terminate all processes
    pub async fn terminate_all(&self, grace_period: Duration) -> Result<()> {
        let processes = self.get_processes().await;

        for process in processes {
            if let Err(e) = self.terminate_process(process, grace_period).await {
                tracing::error!("Failed to terminate process: {}", e);
            }
        }

        Ok(())
    }

    /// Spawn a node as a Docker container
    #[cfg(feature = "docker")]
    pub async fn spawn_docker_container(
        &self,
        node_type: &str,
        node_id: &str,
        _params: &Value, // Reserved for node-specific container configuration
        session_id: &str,
        docker_support: &super::docker_support::DockerSupport,
        docker_config: &super::docker_support::DockerNodeConfig,
    ) -> Result<ProcessHandle> {
        tracing::info!(
            "Spawning Docker container for node {} ({})",
            node_id,
            node_type
        );

        // Create container with IPC volume mounts
        let container_id = docker_support
            .create_container(node_id, session_id, docker_config)
            .await?;

        // Start the container
        docker_support.start_container(&container_id).await?;

        // Create process handle with container execution target
        let handle = ProcessHandle {
            id: 0, // Container doesn't have a traditional PID
            node_id: node_id.to_string(),
            node_type: node_type.to_string(),
            session_id: session_id.to_string(),
            status: Arc::new(RwLock::new(ProcessStatus::Initializing)),
            started_at: Instant::now(),
            inner: Arc::new(Mutex::new(None)),
            execution_target: Arc::new(Mutex::new(Some(ExecutionTarget::Container {
                container_id: container_id.clone(),
                docker_client: docker_support.docker_client().clone(),
            }))),
        };

        // Register process (using node_id hash as key since we don't have a PID)
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        format!("{}_{}", session_id, node_id).hash(&mut hasher);
        let pseudo_pid = (hasher.finish() & 0xFFFFFFFF) as u32;

        self.processes
            .write()
            .await
            .insert(pseudo_pid, handle.clone());

        tracing::info!(
            "Docker container {} started for node {}",
            container_id,
            node_id
        );

        Ok(handle)
    }
}

// Platform-specific imports
#[cfg(unix)]
use nix;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_process_handle_creation() {
        let handle = ProcessHandle {
            id: 1234,
            node_id: "test_node".to_string(),
            node_type: "test_type".to_string(),
            session_id: "test_session".to_string(),
            status: Arc::new(RwLock::new(ProcessStatus::Idle)),
            started_at: Instant::now(),
            inner: Arc::new(Mutex::new(None)),
            execution_target: Arc::new(Mutex::new(None)),
        };

        assert_eq!(handle.id, 1234);
        assert_eq!(handle.node_id, "test_node");
        assert_eq!(handle.session_id, "test_session");
        assert_eq!(*handle.status.read().await, ProcessStatus::Idle);
    }

    #[tokio::test]
    async fn test_process_manager_creation() {
        let config = MultiprocessConfig::default();
        let manager = ProcessManager::new(config);

        let processes = manager.get_processes().await;
        assert!(processes.is_empty());
    }
}

/// `CREATE_NO_WINDOW` when this process has no console of its own (a GUI app
/// such as face-desktop), so each child (python, uv) does not open a console
/// window; `None` in a console app, whose children keep sharing its console.
#[cfg(windows)]
pub(crate) fn no_window_flag() -> Option<u32> {
    #[link(name = "kernel32")]
    extern "system" {
        fn GetConsoleWindow() -> *mut core::ffi::c_void;
    }
    // SAFETY: takes no arguments; returns this process's console window or null.
    unsafe { GetConsoleWindow() }.is_null().then_some(0x0800_0000)
}

/// Python node workers on Windows live in one job object that kills its
/// members when its last handle closes. This process holds that handle for
/// its lifetime, so the workers (and whatever they start) end with it, even
/// on a crash; the Linux equivalent is `PR_SET_PDEATHSIG` above.
#[cfg(windows)]
mod win_job {
    use std::os::windows::io::AsRawHandle;
    use std::sync::OnceLock;
    use windows::core::PCWSTR;
    use windows::Win32::Foundation::{CloseHandle, HANDLE};
    use windows::Win32::System::Diagnostics::ToolHelp::{
        CreateToolhelp32Snapshot, Thread32First, Thread32Next, TH32CS_SNAPTHREAD, THREADENTRY32,
    };
    use windows::Win32::System::JobObjects::{
        AssignProcessToJobObject, CreateJobObjectW, JobObjectExtendedLimitInformation,
        SetInformationJobObject, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
        JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
    };
    use windows::Win32::System::Threading::{OpenThread, ResumeThread, THREAD_SUSPEND_RESUME};

    pub(super) const CREATE_SUSPENDED: u32 = 0x0000_0004;

    /// The job, created on first use; its handle is never closed (the OS
    /// closes it when this process ends, which kills the members). `None`
    /// if it could not be set up.
    fn job() -> Option<HANDLE> {
        static JOB: OnceLock<Option<isize>> = OnceLock::new();
        JOB.get_or_init(|| unsafe {
            let job = CreateJobObjectW(None, PCWSTR::null()).ok()?;
            let mut info = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
            info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
            if let Err(e) = SetInformationJobObject(
                job,
                JobObjectExtendedLimitInformation,
                &info as *const _ as *const core::ffi::c_void,
                std::mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
            ) {
                tracing::warn!("Python workers may outlive this process: job setup failed: {e}");
                let _ = CloseHandle(job);
                return None;
            }
            Some(job.0)
        })
        .map(HANDLE)
    }

    /// Put the (suspended) `child` in the job, then let it run.
    pub(super) fn adopt_and_resume(child: &std::process::Child) {
        if let Some(job) = job() {
            let process = HANDLE(child.as_raw_handle() as isize);
            // SAFETY: both handles are valid for the duration of the call.
            if let Err(e) = unsafe { AssignProcessToJobObject(job, process) } {
                tracing::warn!("Python worker {} may outlive this process: {e}", child.id());
            }
        }
        resume(child.id());
    }

    /// Resume every thread of `pid` (a CREATE_SUSPENDED process has one).
    fn resume(pid: u32) {
        // SAFETY: plain Toolhelp/thread calls on handles we open and close here.
        unsafe {
            let Ok(snap) = CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0) else {
                tracing::error!("cannot resume Python worker {pid}: thread snapshot failed");
                return;
            };
            let mut entry = THREADENTRY32 { dwSize: std::mem::size_of::<THREADENTRY32>() as u32, ..Default::default() };
            let mut ok = Thread32First(snap, &mut entry).is_ok();
            while ok {
                if entry.th32OwnerProcessID == pid {
                    if let Ok(thread) = OpenThread(THREAD_SUSPEND_RESUME, false, entry.th32ThreadID) {
                        ResumeThread(thread);
                        let _ = CloseHandle(thread);
                    }
                }
                ok = Thread32Next(snap, &mut entry).is_ok();
            }
            let _ = CloseHandle(snap);
        }
    }
}
