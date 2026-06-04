//! Docker support for multiprocess executor
//!
//! Provides Docker container management functionality integrated with
//! the multiprocess executor system, including container lifecycle,
//! IPC volume mounting, health monitoring, and resource usage monitoring.
//!
//! # Security Hardening (T057)
//!
//! This module implements comprehensive container security hardening:
//!
//! ## Linux Capabilities
//!
//! By default, all Linux capabilities are dropped (`cap_drop: ["ALL"]`), and only
//! essential capabilities are added back:
//! - `IPC_LOCK`: Required for iceoryx2 shared memory operations
//! - `SYS_NICE`: Required for process priority management in real-time pipelines
//!
//! ## Read-Only Root Filesystem
//!
//! Containers run with read-only root filesystems by default (`read_only_rootfs: true`).
//! Writable areas are provided via tmpfs mounts with restrictive options:
//! - `/tmp`, `/var/tmp`, `/run`: Mounted with `noexec,nosuid,size=64m`
//! - Prevents arbitrary code execution from writable areas
//! - Limits tmpfs size to prevent denial-of-service attacks
//!
//! ## Non-Root User Execution
//!
//! All containers run as non-root user (default: `1000:1000`) to limit the
//! impact of container breakout vulnerabilities.
//!
//! ## Privilege Escalation Prevention
//!
//! The `no-new-privileges` security option prevents processes from gaining
//! additional privileges via setuid/setgid executables.
//!
//! ## AppArmor/SELinux Profiles
//!
//! When available, containers use the `docker-default` AppArmor profile,
//! which provides additional mandatory access control restrictions.
//!
//! ## Security Configuration
//!
//! All security settings can be customized via `SecurityConfig`:
//!
//! ```
//! use remotemedia_core::python::multiprocess::docker_support::SecurityConfig;
//!
//! let security = SecurityConfig::default();
//! ```
//!
//! ## Security Implications
//!
//! **Important considerations:**
//!
//! 1. **IPC_LOCK capability**: Required for iceoryx2 shared memory. Allows
//!    locking memory pages to prevent swapping, which could be used to
//!    consume system memory. Mitigated by memory limits.
//!
//! 2. **SYS_NICE capability**: Allows changing process priorities. Could be
//!    used for local denial-of-service. Mitigated by container isolation.
//!
//! 3. **Volume mounts**: `/tmp` and `/dev` are mounted from host for IPC.
//!    Ensure proper host-level permissions. Consider using namespaced IPC
//!    in high-security environments.
//!
//! 4. **Read-only rootfs**: May break applications that write to unexpected
//!    locations. Use tmpfs_mounts to provide writable areas as needed.
//!
//! 5. **Non-root user**: Applications must be compatible with non-root
//!    execution. Ensure file permissions in Docker images are correct.
//!
//! # Resource Monitoring
//!
//! The module provides real-time resource usage monitoring via Docker's stats API:
//!
//! ```
//! use remotemedia_core::python::multiprocess::docker_support::DockerSupport;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let docker_support = DockerSupport::new().await?;
//! let container_id = "my_container_id";
//!
//! // Monitor resource usage
//! let stats = docker_support.monitor_resource_usage(container_id).await?;
//! println!("CPU: {:.2}%", stats.cpu_percent);
//! println!("Memory: {} MB", stats.memory_mb);
//! if let Some(limit) = stats.memory_limit_mb {
//!     println!("Memory Limit: {} MB", limit);
//! }
//! # Ok(())
//! # }
//! ```

use crate::{Error, Result};
use bollard::Docker;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, instrument, warn, Level};

/// Configuration for Docker-based node execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DockerNodeConfig {
    /// Python version (e.g., "3.9", "3.10", "3.11")
    pub python_version: String,

    /// Optional custom base image
    pub base_image: Option<String>,

    /// System packages to install via apt-get
    #[serde(default)]
    pub system_packages: Vec<String>,

    /// Python packages to install via pip
    #[serde(default)]
    pub python_packages: Vec<String>,

    /// Memory limit in megabytes
    pub memory_mb: u64,

    /// CPU cores allocation (e.g., 2.5)
    pub cpu_cores: f32,

    /// GPU device IDs or ["all"]
    #[serde(default)]
    pub gpu_devices: Vec<String>,

    /// Shared memory size in megabytes (default: 2048)
    #[serde(default = "default_shm_size")]
    pub shm_size_mb: u64,

    /// Environment variables
    #[serde(default)]
    pub env_vars: HashMap<String, String>,

    /// Additional volume mounts
    #[serde(default)]
    pub volumes: Vec<VolumeMount>,

    /// Security configuration for the container
    #[serde(default)]
    pub security: SecurityConfig,
}

/// Security configuration for Docker containers (T057)
///
/// Controls container security hardening settings including:
/// - Linux capability management (drop unnecessary privileges)
/// - Read-only root filesystem with tmpfs for writable areas
/// - Non-root user execution
/// - Security profiles (AppArmor/SELinux)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityConfig {
    /// Linux capabilities to drop (default: ["ALL"])
    /// Drops all capabilities by default for minimal privilege
    #[serde(default = "default_cap_drop")]
    pub cap_drop: Vec<String>,

    /// Linux capabilities to add back (default: ["IPC_LOCK", "SYS_NICE"])
    /// IPC_LOCK: Required for iceoryx2 shared memory operations
    /// SYS_NICE: Allows priority adjustment for real-time processing
    #[serde(default = "default_cap_add")]
    pub cap_add: Vec<String>,

    /// Run container with read-only root filesystem (default: true)
    /// Writable areas are provided via tmpfs mounts
    #[serde(default = "default_read_only_rootfs")]
    pub read_only_rootfs: bool,

    /// Security options to prevent privilege escalation (default: ["no-new-privileges:true"])
    #[serde(default = "default_security_opt")]
    pub security_opt: Vec<String>,

    /// User ID to run container as (default: "1000")
    /// Non-root user for improved security
    #[serde(default = "default_user")]
    pub user: String,

    /// Group ID to run container as (default: "1000")
    #[serde(default = "default_group")]
    pub group: String,

    /// Enable AppArmor profile (default: true if available)
    #[serde(default = "default_enable_apparmor")]
    pub enable_apparmor: bool,

    /// Custom AppArmor profile name (default: "docker-default")
    #[serde(default = "default_apparmor_profile")]
    pub apparmor_profile: String,

    /// Tmpfs mounts for writable areas when using read-only rootfs
    /// Default includes /tmp, /var/tmp, /run for temporary files
    #[serde(default = "default_tmpfs_mounts")]
    pub tmpfs_mounts: Vec<String>,
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            cap_drop: default_cap_drop(),
            cap_add: default_cap_add(),
            read_only_rootfs: default_read_only_rootfs(),
            security_opt: default_security_opt(),
            user: default_user(),
            group: default_group(),
            enable_apparmor: default_enable_apparmor(),
            apparmor_profile: default_apparmor_profile(),
            tmpfs_mounts: default_tmpfs_mounts(),
        }
    }
}

fn default_cap_drop() -> Vec<String> {
    vec!["ALL".to_string()]
}

fn default_cap_add() -> Vec<String> {
    vec![
        "IPC_LOCK".to_string(), // Required for iceoryx2 shared memory
        "SYS_NICE".to_string(), // Required for process priority management
    ]
}

fn default_read_only_rootfs() -> bool {
    true
}

fn default_security_opt() -> Vec<String> {
    vec!["no-new-privileges:true".to_string()]
}

fn default_user() -> String {
    "1000".to_string()
}

fn default_group() -> String {
    "1000".to_string()
}

fn default_enable_apparmor() -> bool {
    true
}

fn default_apparmor_profile() -> String {
    "docker-default".to_string()
}

fn default_tmpfs_mounts() -> Vec<String> {
    vec![
        "/tmp".to_string(),
        "/var/tmp".to_string(),
        "/run".to_string(),
    ]
}

/// Volume mount configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeMount {
    /// Host filesystem path
    pub host_path: std::path::PathBuf,

    /// Container mount point
    pub container_path: std::path::PathBuf,

    /// Whether mount is read-only
    #[serde(default)]
    pub read_only: bool,
}

fn default_shm_size() -> u64 {
    2048 // 2GB default for shared memory
}

/// Configuration for container log forwarding (T059)
///
/// Controls how container logs are captured and forwarded to the tracing infrastructure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogForwardingConfig {
    /// Enable log forwarding (default: true)
    #[serde(default = "default_log_forwarding_enabled")]
    pub enabled: bool,

    /// Maximum log buffer size in bytes (default: 65536 = 64KB)
    /// Prevents memory exhaustion from high-volume log streams
    #[serde(default = "default_log_buffer_size")]
    pub buffer_size: usize,

    /// Minimum log level to forward (default: Debug)
    /// Options: Trace, Debug, Info, Warn, Error
    #[serde(default = "default_log_level")]
    pub min_level: LogLevel,

    /// Parse JSON logs (default: true)
    /// If enabled, attempts to parse log lines as JSON and extract structured fields
    #[serde(default = "default_parse_json")]
    pub parse_json: bool,

    /// Include timestamps in forwarded logs (default: true)
    #[serde(default = "default_include_timestamps")]
    pub include_timestamps: bool,
}

/// Log level enum for configuration
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl LogLevel {
    /// Convert to tracing Level
    fn to_tracing_level(&self) -> Level {
        match self {
            LogLevel::Trace => Level::TRACE,
            LogLevel::Debug => Level::DEBUG,
            LogLevel::Info => Level::INFO,
            LogLevel::Warn => Level::WARN,
            LogLevel::Error => Level::ERROR,
        }
    }
}

fn default_log_forwarding_enabled() -> bool {
    true
}

fn default_log_buffer_size() -> usize {
    65536 // 64KB
}

fn default_log_level() -> LogLevel {
    LogLevel::Debug
}

fn default_parse_json() -> bool {
    true
}

fn default_include_timestamps() -> bool {
    true
}

impl Default for LogForwardingConfig {
    fn default() -> Self {
        Self {
            enabled: default_log_forwarding_enabled(),
            buffer_size: default_log_buffer_size(),
            min_level: default_log_level(),
            parse_json: default_parse_json(),
            include_timestamps: default_include_timestamps(),
        }
    }
}

/// Structured log entry parsed from container output
#[derive(Debug, Clone)]
struct ParsedLogEntry {
    /// Log level extracted from message
    level: LogLevel,
    /// Log message content
    message: String,
    /// Container ID
    container_id: String,
    /// Node ID (if available)
    node_id: Option<String>,
    /// Timestamp from container (if available)
    timestamp: Option<String>,
    /// Whether this is from stderr
    is_stderr: bool,
    /// Additional fields from JSON logs
    #[allow(dead_code)] // Reserved for structured logging metadata
    fields: HashMap<String, serde_json::Value>,
}

/// Resource usage statistics from Docker container
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceUsageStats {
    /// CPU usage as percentage (0-100% per core, can exceed 100% for multi-core)
    pub cpu_percent: f32,

    /// Current memory usage in megabytes
    pub memory_mb: u64,

    /// Memory limit in megabytes (if set)
    pub memory_limit_mb: Option<u64>,
}

/// Time-series metric data point for container resource usage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricDataPoint {
    /// Timestamp of the metric collection
    pub timestamp: std::time::SystemTime,

    /// CPU usage percentage at this point in time
    pub cpu_percent: f32,

    /// Memory usage in megabytes at this point in time
    pub memory_mb: u64,

    /// Memory limit in megabytes (if set)
    pub memory_limit_mb: Option<u64>,

    /// Network I/O received bytes (if available)
    pub network_rx_bytes: Option<u64>,

    /// Network I/O transmitted bytes (if available)
    pub network_tx_bytes: Option<u64>,

    /// Container uptime at this point in time
    pub uptime_secs: u64,
}

/// Aggregated metrics for a time period
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregatedMetrics {
    /// Time period start
    pub period_start: std::time::SystemTime,

    /// Time period end
    pub period_end: std::time::SystemTime,

    /// Number of data points in this aggregation
    pub sample_count: usize,

    /// Average CPU usage percentage
    pub avg_cpu_percent: f32,

    /// Peak CPU usage percentage
    pub peak_cpu_percent: f32,

    /// Minimum CPU usage percentage
    pub min_cpu_percent: f32,

    /// Average memory usage in megabytes
    pub avg_memory_mb: u64,

    /// Peak memory usage in megabytes
    pub peak_memory_mb: u64,

    /// Minimum memory usage in megabytes
    pub min_memory_mb: u64,

    /// Memory limit in megabytes (if set)
    pub memory_limit_mb: Option<u64>,

    /// Total network I/O received bytes (if available)
    pub total_network_rx_bytes: Option<u64>,

    /// Total network I/O transmitted bytes (if available)
    pub total_network_tx_bytes: Option<u64>,

    /// Container restart count during this period
    pub restart_count: u32,
}

/// Container metrics history with circular buffer storage
pub struct ContainerMetrics {
    /// Container ID
    container_id: String,

    /// Container start time
    start_time: std::time::SystemTime,

    /// Circular buffer of metric data points (most recent first)
    data_points: VecDeque<MetricDataPoint>,

    /// Maximum number of data points to store
    max_data_points: usize,

    /// Container restart count
    restart_count: u32,

    /// Last collection timestamp
    last_collection: Option<std::time::SystemTime>,
}

impl ContainerMetrics {
    /// Create a new container metrics instance
    ///
    /// # Arguments
    ///
    /// * `container_id` - The Docker container ID
    /// * `max_data_points` - Maximum number of data points to store (default: 1000)
    pub fn new(container_id: String, max_data_points: Option<usize>) -> Self {
        Self {
            container_id,
            start_time: std::time::SystemTime::now(),
            data_points: VecDeque::with_capacity(max_data_points.unwrap_or(1000)),
            max_data_points: max_data_points.unwrap_or(1000),
            restart_count: 0,
            last_collection: None,
        }
    }

    /// Add a new metric data point
    ///
    /// Maintains the circular buffer by removing oldest points when capacity is reached
    pub fn add_data_point(&mut self, data_point: MetricDataPoint) {
        // Remove oldest point if at capacity
        if self.data_points.len() >= self.max_data_points {
            self.data_points.pop_back();
        }

        self.last_collection = Some(data_point.timestamp);
        self.data_points.push_front(data_point);
    }

    /// Increment restart count
    pub fn increment_restart_count(&mut self) {
        self.restart_count += 1;
    }

    /// Get container uptime
    pub fn uptime(&self) -> std::time::Duration {
        std::time::SystemTime::now()
            .duration_since(self.start_time)
            .unwrap_or(std::time::Duration::ZERO)
    }

    /// Get the most recent N data points
    pub fn get_recent_points(&self, count: usize) -> Vec<MetricDataPoint> {
        self.data_points.iter().take(count).cloned().collect()
    }

    /// Get all data points within a time window
    pub fn get_points_in_window(&self, duration: std::time::Duration) -> Vec<MetricDataPoint> {
        let cutoff = std::time::SystemTime::now()
            .checked_sub(duration)
            .unwrap_or(self.start_time);

        self.data_points
            .iter()
            .filter(|point| point.timestamp >= cutoff)
            .cloned()
            .collect()
    }

    /// Calculate aggregated metrics for a time window
    pub fn calculate_aggregates(&self, duration: std::time::Duration) -> Option<AggregatedMetrics> {
        let points = self.get_points_in_window(duration);

        if points.is_empty() {
            return None;
        }

        let mut cpu_sum = 0.0_f32;
        let mut peak_cpu = 0.0_f32;
        let mut min_cpu = f32::MAX;

        let mut memory_sum = 0u64;
        let mut peak_memory = 0u64;
        let mut min_memory = u64::MAX;

        let mut total_rx_bytes = 0u64;
        let mut total_tx_bytes = 0u64;
        let mut has_network_stats = false;

        let memory_limit = points.first().and_then(|p| p.memory_limit_mb);

        for point in &points {
            // CPU statistics
            cpu_sum += point.cpu_percent;
            peak_cpu = peak_cpu.max(point.cpu_percent);
            min_cpu = min_cpu.min(point.cpu_percent);

            // Memory statistics
            memory_sum += point.memory_mb;
            peak_memory = peak_memory.max(point.memory_mb);
            min_memory = min_memory.min(point.memory_mb);

            // Network I/O statistics
            if let Some(rx) = point.network_rx_bytes {
                total_rx_bytes = total_rx_bytes.max(rx);
                has_network_stats = true;
            }
            if let Some(tx) = point.network_tx_bytes {
                total_tx_bytes = total_tx_bytes.max(tx);
                has_network_stats = true;
            }
        }

        let sample_count = points.len();
        let period_start = points
            .last()
            .map(|p| p.timestamp)
            .unwrap_or(self.start_time);
        let period_end = points
            .first()
            .map(|p| p.timestamp)
            .unwrap_or_else(std::time::SystemTime::now);

        Some(AggregatedMetrics {
            period_start,
            period_end,
            sample_count,
            avg_cpu_percent: cpu_sum / sample_count as f32,
            peak_cpu_percent: peak_cpu,
            min_cpu_percent: if min_cpu == f32::MAX { 0.0 } else { min_cpu },
            avg_memory_mb: memory_sum / sample_count as u64,
            peak_memory_mb: peak_memory,
            min_memory_mb: if min_memory == u64::MAX {
                0
            } else {
                min_memory
            },
            memory_limit_mb: memory_limit,
            total_network_rx_bytes: if has_network_stats {
                Some(total_rx_bytes)
            } else {
                None
            },
            total_network_tx_bytes: if has_network_stats {
                Some(total_tx_bytes)
            } else {
                None
            },
            restart_count: self.restart_count,
        })
    }

    /// Get the latest metric data point
    pub fn latest_point(&self) -> Option<&MetricDataPoint> {
        self.data_points.front()
    }

    /// Get total number of stored data points
    pub fn data_point_count(&self) -> usize {
        self.data_points.len()
    }

    /// Get container ID
    pub fn container_id(&self) -> &str {
        &self.container_id
    }

    /// Get restart count
    pub fn restart_count(&self) -> u32 {
        self.restart_count
    }
}

impl SecurityConfig {
    /// Validate security configuration
    ///
    /// Checks for common misconfigurations and security issues.
    pub fn validate(&self) -> Result<()> {
        // Warn if no capabilities are dropped (running with full privileges)
        if self.cap_drop.is_empty() {
            warn!("No capabilities dropped - container will run with elevated privileges");
        }

        // Warn if both cap_drop ALL and no cap_add (may break IPC)
        if self.cap_drop.contains(&"ALL".to_string()) && self.cap_add.is_empty() {
            warn!("All capabilities dropped without adding any back - IPC operations may fail");
        }

        // Validate user format (should be numeric or name)
        if self.user.is_empty() {
            return Err(Error::Execution(
                "Security config user cannot be empty".to_string(),
            ));
        }

        // Warn if running as root
        if self.user == "0" || self.user.to_lowercase() == "root" {
            warn!(
                "Container configured to run as root (user={}). \
                 This significantly increases security risk. \
                 Consider using a non-root user (e.g., user=\"1000\")",
                self.user
            );
        }

        // Warn if read-only rootfs is disabled
        if !self.read_only_rootfs {
            warn!(
                "Read-only root filesystem is disabled. \
                 This allows modification of system files and increases attack surface."
            );
        }

        // Validate tmpfs mounts if read-only rootfs is enabled
        if self.read_only_rootfs && self.tmpfs_mounts.is_empty() {
            warn!(
                "Read-only rootfs enabled but no tmpfs mounts configured. \
                 Applications may fail if they need writable directories."
            );
        }

        Ok(())
    }

    /// Create a permissive security configuration for development/testing
    ///
    /// **WARNING**: This disables security hardening and should ONLY be used
    /// in trusted development environments. Never use in production.
    pub fn permissive() -> Self {
        Self {
            cap_drop: vec![],
            cap_add: vec![],
            read_only_rootfs: false,
            security_opt: vec![],
            user: "root".to_string(),
            group: "root".to_string(),
            enable_apparmor: false,
            apparmor_profile: String::new(),
            tmpfs_mounts: vec![],
        }
    }

    /// Create a strict security configuration with minimal privileges
    ///
    /// Drops all capabilities except IPC_LOCK (absolute minimum for iceoryx2).
    /// Use this when maximum security is required.
    pub fn strict() -> Self {
        Self {
            cap_drop: vec!["ALL".to_string()],
            cap_add: vec!["IPC_LOCK".to_string()], // Only IPC_LOCK, no SYS_NICE
            read_only_rootfs: true,
            security_opt: vec!["no-new-privileges:true".to_string()],
            user: "65534".to_string(),  // nobody user
            group: "65534".to_string(), // nogroup
            enable_apparmor: true,
            apparmor_profile: "docker-default".to_string(),
            tmpfs_mounts: vec![
                "/tmp".to_string(),
                "/var/tmp".to_string(),
                "/run".to_string(),
            ],
        }
    }
}

impl DockerNodeConfig {
    /// Compute a SHA256 hash of the configuration for caching and image tagging
    ///
    /// Generates a deterministic hash that includes all configuration details:
    /// - python_version: Base Python version (e.g., "3.10")
    /// - base_image: Custom base image if specified
    /// - system_packages: System-level dependencies (sorted for determinism)
    /// - python_packages: Python package dependencies (sorted for determinism)
    /// - memory_mb: Memory limit in megabytes
    /// - cpu_cores: CPU allocation in cores
    /// - shm_size_mb: Shared memory size for IPC
    ///
    /// The hash is deterministic: the same configuration will always produce
    /// the same hash, enabling:
    /// - Image caching and reuse
    /// - Consistent image tagging
    /// - Configuration change detection
    ///
    /// # Returns
    /// A 64-character hexadecimal SHA256 hash string
    pub fn compute_config_hash(&self) -> String {
        use sha2::{Digest, Sha256};

        let mut hasher = Sha256::new();

        // Hash Python version - fundamental to environment
        hasher.update(self.python_version.as_bytes());

        // Hash base image if specified
        if let Some(base) = &self.base_image {
            hasher.update(base.as_bytes());
        }

        // Sort system packages for deterministic hashing
        let mut sys_pkgs = self.system_packages.clone();
        sys_pkgs.sort();
        for pkg in &sys_pkgs {
            hasher.update(pkg.as_bytes());
            hasher.update(b"\0"); // Null separator between packages
        }

        // Sort Python packages for deterministic hashing
        let mut py_pkgs = self.python_packages.clone();
        py_pkgs.sort();
        for pkg in &py_pkgs {
            hasher.update(pkg.as_bytes());
            hasher.update(b"\0"); // Null separator between packages
        }

        // Hash resource limits as little-endian bytes for consistency
        hasher.update(&self.memory_mb.to_le_bytes());
        hasher.update(&self.cpu_cores.to_le_bytes());
        hasher.update(&self.shm_size_mb.to_le_bytes());

        // Finalize and return as hexadecimal string
        format!("{:x}", hasher.finalize())
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<()> {
        // Validate Python version
        const SUPPORTED_VERSIONS: &[&str] = &["3.9", "3.10", "3.11"];
        if !SUPPORTED_VERSIONS.contains(&self.python_version.as_str()) {
            return Err(Error::Execution(format!(
                "Unsupported Python version: {}. Supported: {:?}",
                self.python_version, SUPPORTED_VERSIONS
            )));
        }

        // Validate memory limits
        if self.memory_mb < 512 {
            return Err(Error::Execution(format!(
                "Memory limit too low: {}MB. Minimum: 512MB",
                self.memory_mb
            )));
        }

        // Validate CPU cores
        if self.cpu_cores < 0.1 {
            return Err(Error::Execution(format!(
                "CPU cores too low: {}. Minimum: 0.1",
                self.cpu_cores
            )));
        }

        // Validate shared memory size
        if self.shm_size_mb < 64 {
            return Err(Error::Execution(format!(
                "Shared memory too low: {}MB. Minimum: 64MB",
                self.shm_size_mb
            )));
        }

        // T057: Validate security configuration
        self.security.validate()?;

        Ok(())
    }
}

/// Metrics collector for Docker containers
///
/// Collects and manages time-series metrics for multiple containers.
/// Provides background collection, aggregation, and query APIs.
pub struct MetricsCollector {
    /// Per-container metrics storage
    metrics: Arc<RwLock<HashMap<String, ContainerMetrics>>>,

    /// Collection interval
    collection_interval: std::time::Duration,

    /// Maximum data points per container
    max_data_points: usize,

    /// Shutdown signal sender
    shutdown_tx: Option<tokio::sync::watch::Sender<bool>>,
}

impl MetricsCollector {
    /// Create a new metrics collector
    ///
    /// # Arguments
    ///
    /// * `collection_interval` - How often to collect metrics (default: 5 seconds)
    /// * `max_data_points` - Maximum data points per container (default: 1000)
    pub fn new(
        collection_interval: Option<std::time::Duration>,
        max_data_points: Option<usize>,
    ) -> Self {
        Self {
            metrics: Arc::new(RwLock::new(HashMap::new())),
            collection_interval: collection_interval.unwrap_or(std::time::Duration::from_secs(5)),
            max_data_points: max_data_points.unwrap_or(1000),
            shutdown_tx: None,
        }
    }

    /// Start collecting metrics for a container
    ///
    /// Spawns a background task that periodically collects container stats
    ///
    /// # Arguments
    ///
    /// * `container_id` - The Docker container ID to monitor
    /// * `docker` - Arc reference to Docker client
    ///
    /// # Returns
    ///
    /// A shutdown handle to stop the collection task
    #[instrument(skip(self, docker))]
    pub async fn start_collection(
        &mut self,
        container_id: String,
        docker: Arc<Docker>,
    ) -> Result<()> {
        info!(
            container_id = %container_id,
            interval = ?self.collection_interval,
            "Starting metrics collection for container"
        );

        // Initialize metrics storage for this container
        {
            let mut metrics = self.metrics.write().await;
            metrics.insert(
                container_id.clone(),
                ContainerMetrics::new(container_id.clone(), Some(self.max_data_points)),
            );
        }

        // Create shutdown channel if not already created
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
        self.shutdown_tx = Some(shutdown_tx);

        // Spawn background collection task
        let metrics = self.metrics.clone();
        let collection_interval = self.collection_interval;
        let container_id_clone = container_id.clone();

        tokio::spawn(async move {
            info!(
                container_id = %container_id_clone,
                "Metrics collection task started"
            );

            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            info!(
                                container_id = %container_id_clone,
                                "Metrics collection shutting down"
                            );
                            break;
                        }
                    }
                    _ = tokio::time::sleep(collection_interval) => {
                        // Collect metrics
                        match Self::collect_stats(&container_id_clone, &docker).await {
                            Ok(data_point) => {
                                // Store the data point
                                if let Some(container_metrics) = metrics.write().await.get_mut(&container_id_clone) {
                                    container_metrics.add_data_point(data_point.clone());
                                    debug!(
                                        container_id = %container_id_clone,
                                        cpu_percent = data_point.cpu_percent,
                                        memory_mb = data_point.memory_mb,
                                        "Collected metrics data point"
                                    );
                                }
                            }
                            Err(e) => {
                                debug!(
                                    container_id = %container_id_clone,
                                    error = %e,
                                    "Failed to collect metrics (container may have stopped)"
                                );
                                // Container may have stopped, continue collecting to detect restart
                            }
                        }
                    }
                }
            }

            info!(
                container_id = %container_id_clone,
                "Metrics collection task terminated"
            );
        });

        Ok(())
    }

    /// Stop collecting metrics for a container
    pub async fn stop_collection(&self, container_id: &str) -> Result<()> {
        info!(
            container_id = %container_id,
            "Stopping metrics collection for container"
        );

        // Remove from metrics storage
        self.metrics.write().await.remove(container_id);

        Ok(())
    }

    /// Collect stats from a container (internal helper)
    async fn collect_stats(container_id: &str, docker: &Arc<Docker>) -> Result<MetricDataPoint> {
        use futures::StreamExt;

        // Use stats API with one-shot mode
        let options = Some(
            bollard::query_parameters::StatsOptionsBuilder::new()
                .stream(false)
                .one_shot(true)
                .build(),
        );

        let mut stats_stream = docker.stats(container_id, options);

        if let Some(stats_result) = stats_stream.next().await {
            let stats = stats_result
                .map_err(|e| Error::Execution(format!("Failed to get container stats: {}", e)))?;

            // Extract memory usage and limit
            let (memory_usage, memory_limit) =
                if let Some(memory_stats) = stats.memory_stats.as_ref() {
                    let usage = memory_stats.usage.unwrap_or(0);
                    let limit = memory_stats.limit;
                    (usage, limit)
                } else {
                    (0, None)
                };

            // Calculate CPU usage percentage
            let cpu_percent = if let (Some(cpu_stats), Some(precpu_stats)) =
                (stats.cpu_stats.as_ref(), stats.precpu_stats.as_ref())
            {
                let total_usage = cpu_stats
                    .cpu_usage
                    .as_ref()
                    .and_then(|u| u.total_usage)
                    .unwrap_or(0);
                let prev_total_usage = precpu_stats
                    .cpu_usage
                    .as_ref()
                    .and_then(|u| u.total_usage)
                    .unwrap_or(0);

                let cpu_delta = total_usage.saturating_sub(prev_total_usage);

                let system_usage = cpu_stats.system_cpu_usage.unwrap_or(0);
                let prev_system_usage = precpu_stats.system_cpu_usage.unwrap_or(0);
                let system_delta = system_usage.saturating_sub(prev_system_usage);

                if system_delta > 0 && cpu_delta > 0 {
                    let cpu_count = cpu_stats.online_cpus.unwrap_or(1) as f64;
                    ((cpu_delta as f64 / system_delta as f64) * cpu_count * 100.0) as f32
                } else {
                    0.0
                }
            } else {
                0.0
            };

            // Extract network I/O statistics
            let (network_rx_bytes, network_tx_bytes) =
                if let Some(networks) = stats.networks.as_ref() {
                    let mut total_rx = 0u64;
                    let mut total_tx = 0u64;

                    for (_interface_name, network_stats) in networks {
                        total_rx += network_stats.rx_bytes.unwrap_or(0);
                        total_tx += network_stats.tx_bytes.unwrap_or(0);
                    }

                    (Some(total_rx), Some(total_tx))
                } else {
                    (None, None)
                };

            // Get container uptime from read timestamp
            // The read field is a DateTime from chrono, convert to Unix timestamp
            let uptime_secs = if let Some(read_time) = stats.read {
                read_time.timestamp() as u64
            } else {
                0
            };

            Ok(MetricDataPoint {
                timestamp: std::time::SystemTime::now(),
                cpu_percent,
                memory_mb: memory_usage / 1_048_576,
                memory_limit_mb: memory_limit.map(|limit| limit / 1_048_576),
                network_rx_bytes,
                network_tx_bytes,
                uptime_secs,
            })
        } else {
            Err(Error::Execution(format!(
                "Failed to get container stats: no data returned for container {}",
                container_id
            )))
        }
    }

    /// Get metrics for a specific container
    pub async fn get_metrics(&self, container_id: &str) -> Option<ContainerMetrics> {
        self.metrics.read().await.get(container_id).cloned()
    }

    /// Get recent data points for a container
    pub async fn get_recent_points(
        &self,
        container_id: &str,
        count: usize,
    ) -> Vec<MetricDataPoint> {
        if let Some(metrics) = self.metrics.read().await.get(container_id) {
            metrics.get_recent_points(count)
        } else {
            Vec::new()
        }
    }

    /// Get aggregated metrics for a time window
    pub async fn get_aggregates(
        &self,
        container_id: &str,
        duration: std::time::Duration,
    ) -> Option<AggregatedMetrics> {
        if let Some(metrics) = self.metrics.read().await.get(container_id) {
            metrics.calculate_aggregates(duration)
        } else {
            None
        }
    }

    /// Get aggregates for the last N minutes
    pub async fn get_aggregates_last_minutes(
        &self,
        container_id: &str,
        minutes: u64,
    ) -> Option<AggregatedMetrics> {
        self.get_aggregates(container_id, std::time::Duration::from_secs(minutes * 60))
            .await
    }

    /// Increment restart count for a container
    pub async fn increment_restart_count(&self, container_id: &str) {
        if let Some(metrics) = self.metrics.write().await.get_mut(container_id) {
            metrics.increment_restart_count();
            info!(
                container_id = %container_id,
                restart_count = metrics.restart_count(),
                "Container restart count incremented"
            );
        }
    }

    /// Get all monitored container IDs
    pub async fn get_monitored_containers(&self) -> Vec<String> {
        self.metrics.read().await.keys().cloned().collect()
    }

    /// Export metrics as JSON for a container
    pub async fn export_metrics_json(&self, container_id: &str) -> Option<serde_json::Value> {
        let metrics = self.metrics.read().await;
        let container_metrics = metrics.get(container_id)?;

        let recent_points: Vec<serde_json::Value> = container_metrics
            .get_recent_points(100)
            .iter()
            .map(|point| {
                serde_json::json!({
                    "timestamp": point.timestamp
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or(std::time::Duration::ZERO)
                        .as_secs(),
                    "cpu_percent": point.cpu_percent,
                    "memory_mb": point.memory_mb,
                    "memory_limit_mb": point.memory_limit_mb,
                    "network_rx_bytes": point.network_rx_bytes,
                    "network_tx_bytes": point.network_tx_bytes,
                    "uptime_secs": point.uptime_secs,
                })
            })
            .collect();

        // Get aggregates for last 5, 15, and 60 minutes
        let aggregates_5m =
            container_metrics.calculate_aggregates(std::time::Duration::from_secs(5 * 60));
        let aggregates_15m =
            container_metrics.calculate_aggregates(std::time::Duration::from_secs(15 * 60));
        let aggregates_60m =
            container_metrics.calculate_aggregates(std::time::Duration::from_secs(60 * 60));

        Some(serde_json::json!({
            "container_id": container_metrics.container_id(),
            "uptime_secs": container_metrics.uptime().as_secs(),
            "restart_count": container_metrics.restart_count(),
            "data_point_count": container_metrics.data_point_count(),
            "recent_points": recent_points,
            "aggregates": {
                "5_minutes": aggregates_5m,
                "15_minutes": aggregates_15m,
                "60_minutes": aggregates_60m,
            }
        }))
    }
}

impl Clone for ContainerMetrics {
    fn clone(&self) -> Self {
        Self {
            container_id: self.container_id.clone(),
            start_time: self.start_time,
            data_points: self.data_points.clone(),
            max_data_points: self.max_data_points,
            restart_count: self.restart_count,
            last_collection: self.last_collection,
        }
    }
}

/// Docker support module for multiprocess executor
pub struct DockerSupport {
    docker: Arc<Docker>,
    metrics_collector: Option<Arc<RwLock<MetricsCollector>>>,
}

impl DockerSupport {
    /// Create a new Docker support instance with logging
    #[instrument(skip_all)]
    pub async fn new() -> Result<Self> {
        info!("Initializing Docker support");

        let docker = match Docker::connect_with_local_defaults() {
            Ok(d) => {
                info!("Successfully connected to Docker daemon");
                d
            }
            Err(e) => {
                error!("Failed to connect to Docker daemon: {}", e);
                return Err(Error::Execution(format!(
                    "Failed to connect to Docker: {}",
                    e
                )));
            }
        };

        // Verify Docker is responsive
        match docker.version().await {
            Ok(version) => {
                info!(
                    "Docker daemon version: {}, API version: {}",
                    version.version.unwrap_or_else(|| "unknown".to_string()),
                    version.api_version.unwrap_or_else(|| "unknown".to_string())
                );
            }
            Err(e) => {
                warn!("Could not retrieve Docker version info: {}", e);
            }
        }

        Ok(Self {
            docker: Arc::new(docker),
            metrics_collector: None,
        })
    }

    /// Create a new Docker support instance with metrics collection enabled
    ///
    /// # Arguments
    ///
    /// * `collection_interval` - How often to collect metrics (default: 5 seconds)
    /// * `max_data_points` - Maximum data points per container (default: 1000)
    #[instrument(skip_all)]
    pub async fn new_with_metrics(
        collection_interval: Option<std::time::Duration>,
        max_data_points: Option<usize>,
    ) -> Result<Self> {
        info!("Initializing Docker support with metrics collection");

        let docker = match Docker::connect_with_local_defaults() {
            Ok(d) => {
                info!("Successfully connected to Docker daemon");
                d
            }
            Err(e) => {
                error!("Failed to connect to Docker daemon: {}", e);
                return Err(Error::Execution(format!(
                    "Failed to connect to Docker: {}",
                    e
                )));
            }
        };

        // Verify Docker is responsive
        match docker.version().await {
            Ok(version) => {
                info!(
                    "Docker daemon version: {}, API version: {}",
                    version.version.unwrap_or_else(|| "unknown".to_string()),
                    version.api_version.unwrap_or_else(|| "unknown".to_string())
                );
            }
            Err(e) => {
                warn!("Could not retrieve Docker version info: {}", e);
            }
        }

        // Initialize metrics collector
        let metrics_collector = MetricsCollector::new(collection_interval, max_data_points);
        info!(
            interval = ?collection_interval.unwrap_or(std::time::Duration::from_secs(5)),
            max_points = max_data_points.unwrap_or(1000),
            "Metrics collector initialized"
        );

        Ok(Self {
            docker: Arc::new(docker),
            metrics_collector: Some(Arc::new(RwLock::new(metrics_collector))),
        })
    }

    /// Enable metrics collection for this Docker support instance
    ///
    /// # Arguments
    ///
    /// * `collection_interval` - How often to collect metrics (default: 5 seconds)
    /// * `max_data_points` - Maximum data points per container (default: 1000)
    pub async fn enable_metrics_collection(
        &mut self,
        collection_interval: Option<std::time::Duration>,
        max_data_points: Option<usize>,
    ) {
        info!("Enabling metrics collection");

        let metrics_collector = MetricsCollector::new(collection_interval, max_data_points);
        self.metrics_collector = Some(Arc::new(RwLock::new(metrics_collector)));

        info!(
            interval = ?collection_interval.unwrap_or(std::time::Duration::from_secs(5)),
            max_points = max_data_points.unwrap_or(1000),
            "Metrics collection enabled"
        );
    }

    /// Start collecting metrics for a container
    ///
    /// Requires metrics collection to be enabled via `new_with_metrics()` or `enable_metrics_collection()`
    pub async fn start_metrics_collection(&self, container_id: &str) -> Result<()> {
        if let Some(collector) = &self.metrics_collector {
            collector
                .write()
                .await
                .start_collection(container_id.to_string(), self.docker.clone())
                .await
        } else {
            Err(Error::Execution(
                "Metrics collection not enabled. Use new_with_metrics() or enable_metrics_collection()".to_string()
            ))
        }
    }

    /// Stop collecting metrics for a container
    pub async fn stop_metrics_collection(&self, container_id: &str) -> Result<()> {
        if let Some(collector) = &self.metrics_collector {
            collector.read().await.stop_collection(container_id).await
        } else {
            Ok(()) // Not an error if metrics collection is not enabled
        }
    }

    /// Get aggregated metrics for a container over a time window
    ///
    /// # Arguments
    ///
    /// * `container_id` - The container to query metrics for
    /// * `duration` - Time window to aggregate over
    pub async fn get_container_metrics(
        &self,
        container_id: &str,
        duration: std::time::Duration,
    ) -> Option<AggregatedMetrics> {
        if let Some(collector) = &self.metrics_collector {
            collector
                .read()
                .await
                .get_aggregates(container_id, duration)
                .await
        } else {
            None
        }
    }

    /// Get aggregated metrics for the last N minutes
    ///
    /// # Arguments
    ///
    /// * `container_id` - The container to query metrics for
    /// * `minutes` - Number of minutes to aggregate over
    pub async fn get_container_metrics_last_minutes(
        &self,
        container_id: &str,
        minutes: u64,
    ) -> Option<AggregatedMetrics> {
        if let Some(collector) = &self.metrics_collector {
            collector
                .read()
                .await
                .get_aggregates_last_minutes(container_id, minutes)
                .await
        } else {
            None
        }
    }

    /// Get recent metric data points for a container
    ///
    /// # Arguments
    ///
    /// * `container_id` - The container to query metrics for
    /// * `count` - Number of recent points to retrieve
    pub async fn get_recent_metric_points(
        &self,
        container_id: &str,
        count: usize,
    ) -> Vec<MetricDataPoint> {
        if let Some(collector) = &self.metrics_collector {
            collector
                .read()
                .await
                .get_recent_points(container_id, count)
                .await
        } else {
            Vec::new()
        }
    }

    /// Export container metrics as JSON
    ///
    /// Includes recent data points and aggregates for 5, 15, and 60 minutes
    pub async fn export_container_metrics_json(
        &self,
        container_id: &str,
    ) -> Option<serde_json::Value> {
        if let Some(collector) = &self.metrics_collector {
            collector
                .read()
                .await
                .export_metrics_json(container_id)
                .await
        } else {
            None
        }
    }

    /// Increment restart count for a container (call after restarting)
    pub async fn increment_container_restart_count(&self, container_id: &str) {
        if let Some(collector) = &self.metrics_collector {
            collector
                .read()
                .await
                .increment_restart_count(container_id)
                .await;
        }
    }

    /// Get all monitored container IDs
    pub async fn get_monitored_containers(&self) -> Vec<String> {
        if let Some(collector) = &self.metrics_collector {
            collector.read().await.get_monitored_containers().await
        } else {
            Vec::new()
        }
    }

    /// Check if metrics collection is enabled
    pub fn is_metrics_enabled(&self) -> bool {
        self.metrics_collector.is_some()
    }

    /// Log Docker container events
    #[instrument(skip(self))]
    pub async fn log_container_event(&self, container_id: &str, event: &str, details: &str) {
        info!(
            container_id = %container_id,
            event = %event,
            details = %details,
            "Docker container event"
        );
    }

    /// Log Docker image operations
    #[instrument(skip(self))]
    pub fn log_image_operation(&self, operation: &str, image: &str, status: &str) {
        debug!(
            operation = %operation,
            image = %image,
            status = %status,
            "Docker image operation"
        );
    }

    /// Check Docker daemon availability
    #[instrument(skip(self))]
    pub async fn check_availability(&self) -> bool {
        match self.docker.ping().await {
            Ok(_) => {
                debug!("Docker daemon is available");
                true
            }
            Err(e) => {
                warn!("Docker daemon not responding: {}", e);
                false
            }
        }
    }

    /// Check if Docker daemon is running and ready for operations
    ///
    /// This is a quick pre-flight check before attempting container operations.
    /// It provides diagnostic information if the daemon is not available.
    ///
    /// Returns `Ok(())` if Docker is responsive, or a detailed error with suggestions.
    #[instrument(skip(self))]
    pub async fn check_daemon_ready(&self) -> Result<()> {
        debug!("Performing pre-flight Docker daemon check");

        // Try a simple ping first
        match self.docker.ping().await {
            Ok(_) => {
                debug!("Docker daemon is responsive and ready");
                Ok(())
            }
            Err(e) => {
                let error_msg = e.to_string();
                error!(
                    error = %error_msg,
                    "Docker daemon is not responding to ping"
                );

                // Provide specific guidance based on error type
                let suggestion = if error_msg.contains("permission denied") {
                    "Docker daemon permission denied. Ensure your user is in the docker group: \
                     'sudo usermod -aG docker $USER' (Linux), or try with 'sudo' (macOS)."
                        .to_string()
                } else if error_msg.contains("Cannot connect")
                    || error_msg.contains("connection refused")
                {
                    "Docker daemon is not running. Start Docker:\n  \
                     - macOS/Windows: Open Docker Desktop\n  \
                     - Linux: Run 'sudo systemctl start docker'"
                        .to_string()
                } else if error_msg.contains("not found") {
                    "Docker is not installed on this system. Please install Docker first."
                        .to_string()
                } else {
                    format!(
                        "Docker daemon is unreachable. Verify Docker is installed and running. \
                         Original error: {}",
                        error_msg
                    )
                };

                warn!("Docker daemon check failed: {}", suggestion);
                Err(Error::Execution(format!(
                    "Docker daemon is not ready: {}",
                    suggestion
                )))
            }
        }
    }

    /// Validate Docker availability and compatibility
    ///
    /// Performs comprehensive checks to ensure:
    /// - Docker daemon is reachable and responsive
    /// - API version is compatible with this executor
    /// - Docker is properly configured for container operations
    ///
    /// Returns `Ok(())` if Docker is available and compatible, or a detailed error message.
    /// Includes fallback logic to continue with warnings for non-critical failures.
    #[instrument(skip(self))]
    pub async fn validate_docker_availability(&self) -> Result<()> {
        info!("Validating Docker availability and compatibility");

        // Step 1: Ping the Docker daemon - this is critical
        match self.docker.ping().await {
            Ok(_) => {
                debug!("Docker daemon ping successful");
                info!("Docker daemon is responding");
            }
            Err(e) => {
                let error_msg = e.to_string();
                error!("Docker daemon not responding to ping: {}", error_msg);

                // Provide specific error messaging based on the type of failure
                let error_detail = if error_msg.contains("permission denied") {
                    "Permission denied when accessing Docker daemon. \
                     Fix: Add your user to the docker group with 'sudo usermod -aG docker $USER' \
                     or run with 'sudo', then restart Docker."
                } else if error_msg.contains("Cannot connect")
                    || error_msg.contains("connection refused")
                {
                    "Cannot connect to Docker daemon - it may not be running. \
                     Fix: Start Docker (macOS/Windows: Open Docker Desktop, Linux: sudo systemctl start docker)"
                } else if error_msg.contains("not found") {
                    "Docker is not installed. \
                     Fix: Install Docker from https://docs.docker.com/get-docker/"
                } else {
                    "Docker daemon is unreachable for an unknown reason"
                };

                return Err(Error::Execution(format!(
                    "Docker daemon is not reachable. {}. Original error: {}",
                    error_detail, error_msg
                )));
            }
        }

        // Step 2: Get version information and check API compatibility
        let mut version_valid = true;
        match self.docker.version().await {
            Ok(version) => {
                let docker_version = version.version.unwrap_or_else(|| "unknown".to_string());
                let api_version = version.api_version.unwrap_or_else(|| "unknown".to_string());

                info!(
                    docker_version = %docker_version,
                    api_version = %api_version,
                    "Docker daemon version information retrieved"
                );

                // Log OS/Arch information if available
                if let Some(os) = version.os {
                    if let Some(arch) = version.arch {
                        info!(
                            os = %os,
                            arch = %arch,
                            "Docker system information"
                        );
                    }
                }

                // Validate minimum API version (Docker API 1.40+ required for modern features)
                // This corresponds to Docker 19.03+
                match parse_api_version(&api_version) {
                    Ok(api_ver) => {
                        if api_ver < (1, 40) {
                            warn!(
                                api_version = %api_version,
                                minimum_required = "1.40",
                                "Docker API version is older than recommended. \
                                 Some features may not work correctly. \
                                 Upgrade Docker to 19.03 or later if possible."
                            );
                            // Continue with fallback - older versions may still work
                        } else {
                            debug!("Docker API version {} is compatible", api_version);
                        }
                    }
                    Err(parse_err) => {
                        warn!(
                            api_version = %api_version,
                            error = %parse_err,
                            "Could not parse Docker API version, proceeding with caution"
                        );
                        version_valid = false;
                        // Continue with fallback
                    }
                }

                debug!(
                    "Docker version validation completed: {} (API {})",
                    docker_version, api_version
                );
            }
            Err(e) => {
                error!("Failed to retrieve Docker version information: {}", e);
                warn!(
                    "Proceeding without version validation. \
                     Docker may be misconfigured. Error: {}",
                    e
                );
                version_valid = false;
                // Fallback: Continue to next validation step
            }
        }

        // Step 3: Get Docker info to verify daemon is fully operational
        let mut info_valid = true;
        match self.docker.info().await {
            Ok(info) => {
                let containers = info.containers.unwrap_or(0);
                let images = info.images.unwrap_or(0);
                let driver = info.driver.unwrap_or_else(|| "unknown".to_string());

                info!(
                    containers = containers,
                    images = images,
                    driver = %driver,
                    "Docker daemon operational status"
                );

                // Check if Docker has proper storage driver
                if driver == "unknown" {
                    warn!(
                        "Docker storage driver could not be determined. \
                           Docker may not be fully initialized. \
                           Check: docker info to verify storage configuration"
                    );
                    info_valid = false;
                    // Fallback: Continue with warning
                } else {
                    debug!("Docker is using {} storage driver", driver);
                }

                debug!(
                    "Docker info validation completed: {} containers, {} images",
                    containers, images
                );
            }
            Err(e) => {
                error!("Failed to retrieve Docker daemon info: {}", e);
                warn!(
                    "Could not retrieve Docker system information, proceeding with caution. \
                     Docker daemon may not be fully operational. Error: {}",
                    e
                );
                info_valid = false;
                // Fallback: Continue anyway since daemon responded to ping
            }
        }

        // Summary of validation results
        if version_valid && info_valid {
            info!("Docker availability validation successful - all checks passed");
            Ok(())
        } else {
            warn!(
                "Docker availability validation completed with warnings. \
                 version_valid={}, info_valid={}. \
                 Docker is available but may have reduced functionality.",
                version_valid, info_valid
            );
            Ok(())
        }
    }

    /// Get Docker daemon info with logging
    #[instrument(skip(self))]
    pub async fn get_docker_info(&self) -> Result<String> {
        match self.docker.info().await {
            Ok(info) => {
                let summary = format!(
                    "Docker: {} containers, {} images, driver: {}",
                    info.containers.unwrap_or(0),
                    info.images.unwrap_or(0),
                    info.driver.unwrap_or_else(|| "unknown".to_string())
                );
                info!("{}", summary);
                Ok(summary)
            }
            Err(e) => {
                error!("Failed to get Docker info: {}", e);
                Err(Error::Execution(format!(
                    "Failed to get Docker info: {}",
                    e
                )))
            }
        }
    }

    /// Verify Docker is ready for container operations
    ///
    /// Performs a quick health check before attempting to create/start containers.
    /// This is useful as a pre-flight check before container operations.
    ///
    /// Returns `Ok(())` if Docker is ready, with detailed error messages if not.
    #[instrument(skip(self))]
    pub async fn verify_container_operations_ready(&self) -> Result<()> {
        debug!("Verifying Docker is ready for container operations");

        // First, ensure daemon is responding
        if !self.check_availability().await {
            return Err(Error::Execution(
                "Docker daemon is not responding. \
                 Cannot perform container operations. \
                 Ensure Docker daemon is running."
                    .to_string(),
            ));
        }

        // Try to list containers as a functional test
        let options = bollard::query_parameters::ListContainersOptions::default();
        match self.docker.list_containers(Some(options)).await {
            Ok(_) => {
                debug!("Successfully listed containers - Docker is ready for operations");
                info!("Docker is ready for container operations");
                Ok(())
            }
            Err(e) => {
                error!(
                    error = %e,
                    "Failed to list containers - Docker may not be fully operational"
                );

                let error_msg = e.to_string();
                let detail = if error_msg.contains("permission denied") {
                    "Permission denied. Check Docker daemon socket permissions."
                } else if error_msg.contains("refused") {
                    "Connection refused. Docker daemon may not be fully initialized."
                } else {
                    "Unknown error preventing container operations"
                };

                Err(Error::Execution(format!(
                    "Docker is not ready for container operations: {}. \
                     Error: {}",
                    detail, error_msg
                )))
            }
        }
    }

    /// Log Docker readiness status
    ///
    /// Logs the current state of Docker availability and provides operational status.
    /// Useful for diagnostics and monitoring.
    #[instrument(skip(self))]
    pub async fn log_docker_status(&self) {
        info!("Logging Docker operational status");

        // Check daemon availability
        match self.docker.ping().await {
            Ok(_) => {
                info!("Docker daemon status: RESPONSIVE");
            }
            Err(e) => {
                warn!("Docker daemon status: NOT RESPONDING - {}", e);
                return; // No point checking further
            }
        }

        // Get version info
        match self.docker.version().await {
            Ok(version) => {
                let docker_ver = version.version.unwrap_or_else(|| "unknown".to_string());
                let api_ver = version.api_version.unwrap_or_else(|| "unknown".to_string());
                info!(
                    docker_version = %docker_ver,
                    api_version = %api_ver,
                    "Docker version information"
                );
            }
            Err(e) => {
                warn!("Could not retrieve Docker version: {}", e);
            }
        }

        // Get system info
        match self.docker.info().await {
            Ok(info) => {
                let containers = info.containers.unwrap_or(0);
                let images = info.images.unwrap_or(0);
                let driver = info.driver.unwrap_or_else(|| "unknown".to_string());
                let mem_total = info.mem_total.unwrap_or(0);

                info!(
                    containers = containers,
                    images = images,
                    storage_driver = %driver,
                    memory_total_bytes = mem_total,
                    "Docker system information"
                );
            }
            Err(e) => {
                warn!("Could not retrieve Docker system info: {}", e);
            }
        }

        info!("Docker status logging completed");
    }

    /// Get the Docker client reference
    pub fn docker_client(&self) -> &Arc<Docker> {
        &self.docker
    }

    /// Create a Docker container for a Python node
    #[instrument(skip(self, config))]
    pub async fn create_container(
        &self,
        node_id: &str,
        session_id: &str,
        config: &DockerNodeConfig,
    ) -> Result<String> {
        use bollard::models::{ContainerCreateBody, DeviceRequest, HostConfig};
        use bollard::query_parameters::CreateContainerOptionsBuilder;

        info!(
            node_id = %node_id,
            session_id = %session_id,
            "Creating Docker container for node"
        );

        // Container name must be unique
        let container_name = format!("{}_{}", session_id, node_id);

        // T057: Security hardening - Apply security configuration
        info!(
            node_id = %node_id,
            cap_drop = ?config.security.cap_drop,
            cap_add = ?config.security.cap_add,
            read_only_rootfs = config.security.read_only_rootfs,
            user = %config.security.user,
            "Applying container security hardening"
        );

        // Prepare host configuration with resource limits and security settings
        let mut host_config = HostConfig {
            memory: Some(config.memory_mb as i64 * 1_048_576), // Convert MB to bytes
            nano_cpus: Some((config.cpu_cores * 1_000_000_000.0) as i64), // Convert cores to nano CPUs
            shm_size: Some(config.shm_size_mb as i64 * 1_048_576),        // Shared memory size

            // T057: Drop all capabilities and add only required ones
            cap_drop: if !config.security.cap_drop.is_empty() {
                Some(config.security.cap_drop.clone())
            } else {
                None
            },
            cap_add: if !config.security.cap_add.is_empty() {
                Some(config.security.cap_add.clone())
            } else {
                None
            },

            // T057: Read-only root filesystem for security
            readonly_rootfs: Some(config.security.read_only_rootfs),

            // T057: Security options to prevent privilege escalation
            security_opt: if !config.security.security_opt.is_empty() {
                let mut opts = config.security.security_opt.clone();

                // Add AppArmor profile if enabled
                if config.security.enable_apparmor && !config.security.apparmor_profile.is_empty() {
                    opts.push(format!("apparmor={}", config.security.apparmor_profile));
                }

                Some(opts)
            } else {
                None
            },

            ..Default::default()
        };

        debug!(
            read_only_rootfs = config.security.read_only_rootfs,
            cap_drop = ?host_config.cap_drop,
            cap_add = ?host_config.cap_add,
            security_opt = ?host_config.security_opt,
            "Host security configuration applied"
        );

        // T014: IPC volume mounting for iceoryx2
        // According to iceoryx2 documentation, we need to mount /tmp and /dev for IPC
        let mut binds = Vec::new();

        // Mount /tmp for Unix domain sockets, file locks, and iceoryx2 service files
        binds.push("/tmp:/tmp".to_string());
        debug!("Added /tmp volume mount for iceoryx2 IPC");

        // Mount /dev for shared memory and semaphores
        binds.push("/dev:/dev".to_string());
        debug!("Added /dev volume mount for iceoryx2 shared memory");

        // Add custom volume mounts from config
        for volume in &config.volumes {
            let mount_str = if volume.read_only {
                format!(
                    "{}:{}:ro",
                    volume.host_path.display(),
                    volume.container_path.display()
                )
            } else {
                format!(
                    "{}:{}",
                    volume.host_path.display(),
                    volume.container_path.display()
                )
            };
            binds.push(mount_str.clone());
            debug!(
                host_path = %volume.host_path.display(),
                container_path = %volume.container_path.display(),
                read_only = volume.read_only,
                "Added custom volume mount"
            );
        }

        host_config.binds = Some(binds);
        info!("Configured IPC volume mounts for iceoryx2: /tmp and /dev");

        // T057: Configure tmpfs mounts for writable areas when using read-only rootfs
        if config.security.read_only_rootfs && !config.security.tmpfs_mounts.is_empty() {
            use std::collections::HashMap as StdHashMap;

            let mut tmpfs = StdHashMap::new();
            for mount_point in &config.security.tmpfs_mounts {
                // Default tmpfs options: rw,noexec,nosuid,size=64m
                // - rw: read-write access
                // - noexec: cannot execute binaries from tmpfs
                // - nosuid: ignore set-user-ID and set-group-ID bits
                // - size=64m: limit size to prevent DoS via tmpfs filling
                tmpfs.insert(mount_point.clone(), "rw,noexec,nosuid,size=64m".to_string());
            }

            host_config.tmpfs = Some(tmpfs);
            debug!(
                tmpfs_mounts = ?config.security.tmpfs_mounts,
                "Configured tmpfs mounts for writable areas"
            );
        }

        // T037: GPU device passthrough for NVIDIA
        if !config.gpu_devices.is_empty() {
            debug!(
                gpu_devices = ?config.gpu_devices,
                "Configuring GPU device passthrough for NVIDIA"
            );

            // Check if "all" is specified or specific device IDs
            let is_all_devices = config.gpu_devices.contains(&"all".to_string());
            let device_ids = if is_all_devices {
                // Request all available GPUs
                None
            } else {
                // Request specific GPU device IDs
                Some(config.gpu_devices.clone())
            };

            let device_request = DeviceRequest {
                driver: Some("nvidia".to_string()),
                count: if is_all_devices { Some(-1) } else { None }, // -1 means all devices
                device_ids,
                capabilities: Some(vec![vec!["gpu".to_string()]]),
                options: None,
            };

            host_config.device_requests = Some(vec![device_request]);

            info!(
                gpu_count = if is_all_devices {
                    "all".to_string()
                } else {
                    config.gpu_devices.len().to_string()
                },
                "Configured NVIDIA GPU device passthrough"
            );
        } else {
            debug!("No GPU devices requested, skipping GPU configuration");
        }

        // Prepare container configuration
        let default_image = format!("python:{}", config.python_version);
        let image = config.base_image.as_ref().unwrap_or(&default_image);

        let mut env = Vec::new();
        env.push(format!("NODE_ID={}", node_id));
        env.push(format!("SESSION_ID={}", session_id));
        env.push("PYTHONUNBUFFERED=1".to_string());

        // Add custom environment variables
        for (key, value) in &config.env_vars {
            env.push(format!("{}={}", key, value));
        }

        debug!(
            image = %image,
            env_vars = env.len(),
            memory_mb = config.memory_mb,
            cpu_cores = config.cpu_cores,
            "Container configuration prepared"
        );

        // Add labels for container tracking
        let mut labels = HashMap::new();
        labels.insert("remotemedia.node_id".to_string(), node_id.to_string());
        labels.insert("remotemedia.session_id".to_string(), session_id.to_string());

        // T057: Configure non-root user for container execution
        let user_config = format!("{}:{}", config.security.user, config.security.group);
        debug!(
            user = %user_config,
            "Configuring container to run as non-root user"
        );

        let container_config = ContainerCreateBody {
            image: Some(image.clone()),
            env: Some(env),
            host_config: Some(host_config),
            labels: Some(labels),
            user: Some(user_config), // T057: Run as non-root user
            ..Default::default()
        };

        let options = CreateContainerOptionsBuilder::default()
            .name(&container_name)
            .build();

        // Create the container
        match self
            .docker
            .create_container(Some(options), container_config)
            .await
        {
            Ok(response) => {
                info!(
                    container_id = %response.id,
                    container_name = %container_name,
                    "Container created successfully"
                );
                self.log_container_event(&response.id, "created", &format!("Node: {}", node_id))
                    .await;
                Ok(response.id)
            }
            Err(e) => {
                error!(
                    container_name = %container_name,
                    error = %e,
                    "Failed to create container"
                );
                Err(Error::Execution(format!(
                    "Failed to create container: {}",
                    e
                )))
            }
        }
    }

    /// Start a Docker container
    #[instrument(skip(self))]
    pub async fn start_container(&self, container_id: &str) -> Result<()> {
        info!(container_id = %container_id, "Starting container");

        self.docker
            .start_container(
                container_id,
                None::<bollard::query_parameters::StartContainerOptions>,
            )
            .await
            .map_err(|e| {
                error!(
                    container_id = %container_id,
                    error = %e,
                    "Failed to start container"
                );
                Error::Execution(format!("Failed to start container: {}", e))
            })?;

        info!(container_id = %container_id, "Container started successfully");
        self.log_container_event(container_id, "started", "Container is now running")
            .await;
        Ok(())
    }

    /// Stop a Docker container
    #[instrument(skip(self))]
    pub async fn stop_container(
        &self,
        container_id: &str,
        timeout: std::time::Duration,
    ) -> Result<()> {
        info!(
            container_id = %container_id,
            timeout = ?timeout,
            "Stopping container"
        );

        let options = bollard::query_parameters::StopContainerOptions {
            t: Some(timeout.as_secs() as i32),
            signal: None,
        };

        match self
            .docker
            .stop_container(container_id, Some(options))
            .await
        {
            Ok(_) => {
                info!(container_id = %container_id, "Container stopped successfully");
                self.log_container_event(container_id, "stopped", "Container stopped successfully")
                    .await;
                Ok(())
            }
            Err(e) => {
                error!(
                    container_id = %container_id,
                    error = %e,
                    "Failed to stop container"
                );
                Err(Error::Execution(format!("Failed to stop container: {}", e)))
            }
        }
    }

    /// Remove a Docker container
    #[instrument(skip(self))]
    pub async fn remove_container(&self, container_id: &str, force: bool) -> Result<()> {
        info!(
            container_id = %container_id,
            force = force,
            "Removing container"
        );

        let options = bollard::query_parameters::RemoveContainerOptions {
            force,
            v: true, // Remove volumes associated with the container
            link: false,
        };

        match self
            .docker
            .remove_container(container_id, Some(options))
            .await
        {
            Ok(_) => {
                info!(container_id = %container_id, "Container removed successfully");
                self.log_container_event(container_id, "removed", "Container removed from system")
                    .await;
                Ok(())
            }
            Err(e) => {
                error!(
                    container_id = %container_id,
                    error = %e,
                    "Failed to remove container"
                );
                Err(Error::Execution(format!(
                    "Failed to remove container: {}",
                    e
                )))
            }
        }
    }

    /// Restart a Docker container
    #[instrument(skip(self))]
    pub async fn restart_container(
        &self,
        container_id: &str,
        timeout: std::time::Duration,
    ) -> Result<()> {
        info!(
            container_id = %container_id,
            timeout = ?timeout,
            "Restarting container"
        );

        let options = bollard::query_parameters::RestartContainerOptions {
            t: Some(timeout.as_secs() as i32),
            signal: None,
        };

        match self
            .docker
            .restart_container(container_id, Some(options))
            .await
        {
            Ok(_) => {
                info!(container_id = %container_id, "Container restarted successfully");
                self.log_container_event(
                    container_id,
                    "restarted",
                    "Container restarted successfully",
                )
                .await;
                Ok(())
            }
            Err(e) => {
                error!(
                    container_id = %container_id,
                    error = %e,
                    "Failed to restart container"
                );
                Err(Error::Execution(format!(
                    "Failed to restart container: {}",
                    e
                )))
            }
        }
    }

    /// Check if a container is running
    #[instrument(skip(self))]
    pub async fn is_container_running(&self, container_id: &str) -> Result<bool> {
        debug!(container_id = %container_id, "Checking if container is running");

        match self
            .docker
            .inspect_container(
                container_id,
                None::<bollard::query_parameters::InspectContainerOptions>,
            )
            .await
        {
            Ok(info) => {
                let running = info.state.and_then(|s| s.running).unwrap_or(false);

                debug!(
                    container_id = %container_id,
                    running = running,
                    "Container running status retrieved"
                );
                Ok(running)
            }
            Err(e) => {
                warn!(
                    container_id = %container_id,
                    error = %e,
                    "Failed to inspect container, assuming not running"
                );
                Ok(false) // Assume not running if we can't inspect
            }
        }
    }

    /// Get container logs
    #[instrument(skip(self))]
    pub async fn get_container_logs(
        &self,
        container_id: &str,
        tail: Option<usize>,
    ) -> Result<String> {
        use bollard::query_parameters::LogsOptionsBuilder;
        use futures::StreamExt;

        debug!(
            container_id = %container_id,
            tail = ?tail,
            "Retrieving container logs"
        );

        let tail_str = tail
            .map(|n| n.to_string())
            .unwrap_or_else(|| "all".to_string());
        let options = LogsOptionsBuilder::default()
            .stdout(true)
            .stderr(true)
            .tail(&tail_str)
            .follow(false)
            .build();

        let mut logs_stream = self.docker.logs(container_id, Some(options));
        let mut logs = String::new();

        while let Some(log_result) = logs_stream.next().await {
            match log_result {
                Ok(log_output) => {
                    logs.push_str(&log_output.to_string());
                }
                Err(e) => {
                    warn!(
                        container_id = %container_id,
                        error = %e,
                        "Error reading logs from container"
                    );
                }
            }
        }

        debug!(
            container_id = %container_id,
            log_size = logs.len(),
            "Container logs retrieved"
        );
        Ok(logs)
    }

    /// Monitor resource usage of a Docker container
    ///
    /// Queries the Docker stats API to retrieve real-time resource usage metrics
    /// for a running container. This includes CPU percentage, memory usage, and
    /// memory limits.
    ///
    /// # Arguments
    ///
    /// * `container_id` - The ID or name of the container to monitor
    ///
    /// # Returns
    ///
    /// Returns `ResourceUsageStats` containing:
    /// - `cpu_percent`: CPU usage as percentage (can exceed 100% for multi-core)
    /// - `memory_mb`: Current memory usage in megabytes
    /// - `memory_limit_mb`: Memory limit in megabytes (if configured)
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Container does not exist
    /// - Container is stopped or not running
    /// - Failed to retrieve stats from Docker daemon
    #[instrument(skip(self))]
    pub async fn monitor_resource_usage(&self, container_id: &str) -> Result<ResourceUsageStats> {
        use futures::StreamExt;

        debug!(
            container_id = %container_id,
            "Monitoring container resource usage"
        );

        // First verify the container is running
        let is_running = self.is_container_running(container_id).await?;
        if !is_running {
            warn!(
                container_id = %container_id,
                "Cannot monitor resource usage: container is not running"
            );
            return Err(Error::Execution(format!(
                "Container {} is not running, cannot retrieve stats",
                container_id
            )));
        }

        // Use stats API with one-shot mode (don't stream continuously)
        let options = Some(
            bollard::query_parameters::StatsOptionsBuilder::new()
                .stream(false)
                .one_shot(true)
                .build(),
        );

        let mut stats_stream = self.docker.stats(container_id, options);

        // Get the first (and only) stats result
        if let Some(stats_result) = stats_stream.next().await {
            let stats = stats_result.map_err(|e| {
                error!(
                    container_id = %container_id,
                    error = %e,
                    "Failed to retrieve container stats"
                );
                Error::Execution(format!("Failed to get container stats: {}", e))
            })?;

            // Extract memory usage and limit from MemoryStats
            let (memory_usage, memory_limit) =
                if let Some(memory_stats) = stats.memory_stats.as_ref() {
                    let usage = memory_stats.usage.unwrap_or(0);
                    let limit = memory_stats.limit;
                    (usage, limit)
                } else {
                    warn!(
                        container_id = %container_id,
                        "Memory stats not available"
                    );
                    (0, None)
                };

            // Calculate CPU usage percentage
            let cpu_percent = if let (Some(cpu_stats), Some(precpu_stats)) =
                (stats.cpu_stats.as_ref(), stats.precpu_stats.as_ref())
            {
                // Get total CPU usage
                let total_usage = cpu_stats
                    .cpu_usage
                    .as_ref()
                    .and_then(|u| u.total_usage)
                    .unwrap_or(0);
                let prev_total_usage = precpu_stats
                    .cpu_usage
                    .as_ref()
                    .and_then(|u| u.total_usage)
                    .unwrap_or(0);

                let cpu_delta = total_usage.saturating_sub(prev_total_usage);

                // Get system CPU usage
                let system_usage = cpu_stats.system_cpu_usage.unwrap_or(0);
                let prev_system_usage = precpu_stats.system_cpu_usage.unwrap_or(0);
                let system_delta = system_usage.saturating_sub(prev_system_usage);

                // Calculate percentage based on number of CPU cores
                if system_delta > 0 && cpu_delta > 0 {
                    let cpu_count = cpu_stats.online_cpus.unwrap_or(1) as f64;
                    ((cpu_delta as f64 / system_delta as f64) * cpu_count * 100.0) as f32
                } else {
                    0.0
                }
            } else {
                warn!(
                    container_id = %container_id,
                    "CPU stats not available"
                );
                0.0
            };

            // Convert bytes to megabytes
            let memory_mb = memory_usage / 1_048_576;
            let memory_limit_mb = memory_limit.map(|limit| limit / 1_048_576);

            let resource_stats = ResourceUsageStats {
                cpu_percent,
                memory_mb,
                memory_limit_mb,
            };

            debug!(
                container_id = %container_id,
                cpu_percent = cpu_percent,
                memory_mb = memory_mb,
                memory_limit_mb = ?memory_limit_mb,
                "Successfully retrieved container resource usage"
            );

            // Log the resource usage using the existing logger
            DockerLogger::log_resource_usage(container_id, cpu_percent, memory_mb);

            Ok(resource_stats)
        } else {
            error!(
                container_id = %container_id,
                "Stats stream returned no data"
            );
            Err(Error::Execution(format!(
                "Failed to get container stats: no data returned for container {}",
                container_id
            )))
        }
    }

    /// Clean up stopped containers for a session
    #[instrument(skip(self))]
    pub async fn cleanup_session_containers(&self, session_id: &str) -> Result<Vec<String>> {
        info!(session_id = %session_id, "Cleaning up containers for session");

        let filters = HashMap::from([(
            "label".to_string(),
            vec![format!("remotemedia.session_id={}", session_id)],
        )]);

        let options = bollard::query_parameters::ListContainersOptionsBuilder::default()
            .all(true)
            .filters(&filters)
            .build();

        let containers = self
            .docker
            .list_containers(Some(options))
            .await
            .map_err(|e| {
                error!(
                    session_id = %session_id,
                    error = %e,
                    "Failed to list containers"
                );
                Error::Execution(format!("Failed to list containers: {}", e))
            })?;

        let mut removed = Vec::new();

        for container in containers {
            if let Some(id) = container.id {
                // Stop if running
                if container.state == Some(bollard::models::ContainerSummaryStateEnum::RUNNING) {
                    debug!(
                        container_id = %id,
                        "Stopping running container before removal"
                    );
                    self.stop_container(&id, std::time::Duration::from_secs(5))
                        .await?;
                }

                // Remove container
                self.remove_container(&id, true).await?;
                removed.push(id);
            }
        }

        info!(
            session_id = %session_id,
            containers_removed = removed.len(),
            "Session cleanup completed"
        );
        Ok(removed)
    }

    /// Forward container logs to tracing infrastructure (T059)
    ///
    /// Spawns a background task that continuously streams container logs and forwards
    /// them to the tracing system with appropriate log levels and metadata.
    ///
    /// # Arguments
    ///
    /// * `container_id` - The container to forward logs for
    /// * `node_id` - Optional node ID for additional context
    /// * `config` - Log forwarding configuration
    ///
    /// # Returns
    ///
    /// A handle to stop the log forwarding task
    ///
    /// # Example
    ///
    /// ```
    /// use remotemedia_core::python::multiprocess::docker_support::{
    ///     DockerSupport, LogForwardingConfig
    /// };
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let docker_support = DockerSupport::new().await?;
    /// let container_id = "my_container_id";
    ///
    /// // Start log forwarding with default config
    /// let shutdown_tx = docker_support
    ///     .forward_container_logs(container_id, Some("node_1"), LogForwardingConfig::default())
    ///     .await?;
    ///
    /// // Later: stop log forwarding
    /// let _ = shutdown_tx.send(true);
    /// # Ok(())
    /// # }
    /// ```
    #[instrument(skip(self, config))]
    pub async fn forward_container_logs(
        &self,
        container_id: &str,
        node_id: Option<&str>,
        config: LogForwardingConfig,
    ) -> Result<tokio::sync::watch::Sender<bool>> {
        if !config.enabled {
            debug!(
                container_id = %container_id,
                "Log forwarding is disabled in configuration"
            );
            let (tx, _rx) = tokio::sync::watch::channel(false);
            return Ok(tx);
        }

        info!(
            container_id = %container_id,
            node_id = ?node_id,
            buffer_size = config.buffer_size,
            min_level = ?config.min_level,
            "Starting container log forwarding"
        );

        use bollard::query_parameters::LogsOptionsBuilder;
        use futures::StreamExt;

        let options = LogsOptionsBuilder::default()
            .stdout(true)
            .stderr(true)
            .follow(true) // Stream logs in real-time
            .tail("0") // Start from now (don't retrieve historical logs)
            .timestamps(config.include_timestamps)
            .build();

        let logs_stream = self.docker.logs(container_id, Some(options));
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);

        let container_id = container_id.to_string();
        let node_id = node_id.map(|s| s.to_string());

        // Spawn background task for log forwarding
        tokio::spawn(async move {
            info!(
                container_id = %container_id,
                "Log forwarding task started"
            );

            let mut logs_stream = logs_stream;
            let mut buffer = String::with_capacity(config.buffer_size);

            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            info!(
                                container_id = %container_id,
                                "Log forwarding shutting down"
                            );
                            break;
                        }
                    }
                    log_result = logs_stream.next() => {
                        match log_result {
                            Some(Ok(log_output)) => {
                                // Convert LogOutput to string
                                let log_str = log_output.to_string();

                                // Parse and forward the log line
                                Self::parse_and_forward_log(
                                    &log_str,
                                    &container_id,
                                    node_id.as_deref(),
                                    &config,
                                    &mut buffer,
                                );

                                // Implement backpressure: if buffer exceeds limit, clear it
                                if buffer.len() > config.buffer_size {
                                    warn!(
                                        container_id = %container_id,
                                        buffer_size = buffer.len(),
                                        limit = config.buffer_size,
                                        "Log buffer overflow, clearing buffer to prevent memory exhaustion"
                                    );
                                    buffer.clear();
                                }
                            }
                            Some(Err(e)) => {
                                warn!(
                                    container_id = %container_id,
                                    error = %e,
                                    "Error reading logs from container"
                                );
                                // Continue streaming despite errors
                            }
                            None => {
                                info!(
                                    container_id = %container_id,
                                    "Container log stream ended (container stopped or removed)"
                                );
                                break;
                            }
                        }
                    }
                }
            }

            info!(
                container_id = %container_id,
                "Log forwarding task terminated"
            );
        });

        Ok(shutdown_tx)
    }

    /// Parse a log line and forward it to tracing
    ///
    /// Handles both plain text and JSON-formatted logs, extracting log level
    /// and metadata where possible.
    fn parse_and_forward_log(
        log_line: &str,
        container_id: &str,
        node_id: Option<&str>,
        config: &LogForwardingConfig,
        _buffer: &mut String, // Reserved for buffered log output
    ) {
        let log_line = log_line.trim();
        if log_line.is_empty() {
            return;
        }

        // Determine if this is from stderr (Docker log format detection)
        let is_stderr = log_line.starts_with("Error:")
            || log_line.starts_with("ERROR")
            || log_line.starts_with("FATAL")
            || log_line.starts_with("CRITICAL");

        // Try to parse as JSON if configured
        let parsed = if config.parse_json {
            Self::try_parse_json_log(log_line, container_id, node_id, is_stderr)
        } else {
            None
        };

        let entry = parsed.unwrap_or_else(|| {
            // Fallback to plain text parsing
            Self::parse_plain_text_log(log_line, container_id, node_id, is_stderr)
        });

        // Filter by log level
        if entry.level < config.min_level {
            return;
        }

        // Forward to tracing with appropriate level and metadata
        let level = entry.level.to_tracing_level();

        match level {
            Level::ERROR => {
                if let Some(nid) = &entry.node_id {
                    error!(
                        container_id = %entry.container_id,
                        node_id = %nid,
                        is_stderr = entry.is_stderr,
                        timestamp = ?entry.timestamp,
                        "{}",
                        entry.message
                    );
                } else {
                    error!(
                        container_id = %entry.container_id,
                        is_stderr = entry.is_stderr,
                        timestamp = ?entry.timestamp,
                        "{}",
                        entry.message
                    );
                }
            }
            Level::WARN => {
                if let Some(nid) = &entry.node_id {
                    warn!(
                        container_id = %entry.container_id,
                        node_id = %nid,
                        is_stderr = entry.is_stderr,
                        timestamp = ?entry.timestamp,
                        "{}",
                        entry.message
                    );
                } else {
                    warn!(
                        container_id = %entry.container_id,
                        is_stderr = entry.is_stderr,
                        timestamp = ?entry.timestamp,
                        "{}",
                        entry.message
                    );
                }
            }
            Level::INFO => {
                if let Some(nid) = &entry.node_id {
                    info!(
                        container_id = %entry.container_id,
                        node_id = %nid,
                        is_stderr = entry.is_stderr,
                        timestamp = ?entry.timestamp,
                        "{}",
                        entry.message
                    );
                } else {
                    info!(
                        container_id = %entry.container_id,
                        is_stderr = entry.is_stderr,
                        timestamp = ?entry.timestamp,
                        "{}",
                        entry.message
                    );
                }
            }
            Level::DEBUG => {
                if let Some(nid) = &entry.node_id {
                    debug!(
                        container_id = %entry.container_id,
                        node_id = %nid,
                        is_stderr = entry.is_stderr,
                        timestamp = ?entry.timestamp,
                        "{}",
                        entry.message
                    );
                } else {
                    debug!(
                        container_id = %entry.container_id,
                        is_stderr = entry.is_stderr,
                        timestamp = ?entry.timestamp,
                        "{}",
                        entry.message
                    );
                }
            }
            Level::TRACE => {
                if let Some(nid) = &entry.node_id {
                    tracing::trace!(
                        container_id = %entry.container_id,
                        node_id = %nid,
                        is_stderr = entry.is_stderr,
                        timestamp = ?entry.timestamp,
                        "{}",
                        entry.message
                    );
                } else {
                    tracing::trace!(
                        container_id = %entry.container_id,
                        is_stderr = entry.is_stderr,
                        timestamp = ?entry.timestamp,
                        "{}",
                        entry.message
                    );
                }
            }
        }
    }

    /// Try to parse log line as JSON
    fn try_parse_json_log(
        log_line: &str,
        container_id: &str,
        node_id: Option<&str>,
        is_stderr: bool,
    ) -> Option<ParsedLogEntry> {
        let json: serde_json::Value = serde_json::from_str(log_line).ok()?;

        // Extract common JSON log fields
        let message = json
            .get("message")
            .or_else(|| json.get("msg"))
            .or_else(|| json.get("text"))
            .and_then(|v| v.as_str())
            .unwrap_or(log_line)
            .to_string();

        // Extract log level from JSON
        let level = json
            .get("level")
            .or_else(|| json.get("severity"))
            .and_then(|v| v.as_str())
            .and_then(|s| Self::parse_log_level_string(s))
            .unwrap_or(if is_stderr {
                LogLevel::Error
            } else {
                LogLevel::Info
            });

        // Extract timestamp
        let timestamp = json
            .get("timestamp")
            .or_else(|| json.get("time"))
            .or_else(|| json.get("@timestamp"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        // Extract additional fields
        let mut fields = HashMap::new();
        if let Some(obj) = json.as_object() {
            for (key, value) in obj {
                if ![
                    "message",
                    "msg",
                    "text",
                    "level",
                    "severity",
                    "timestamp",
                    "time",
                    "@timestamp",
                ]
                .contains(&key.as_str())
                {
                    fields.insert(key.clone(), value.clone());
                }
            }
        }

        Some(ParsedLogEntry {
            level,
            message,
            container_id: container_id.to_string(),
            node_id: node_id.map(|s| s.to_string()),
            timestamp,
            is_stderr,
            fields,
        })
    }

    /// Parse plain text log line
    fn parse_plain_text_log(
        log_line: &str,
        container_id: &str,
        node_id: Option<&str>,
        is_stderr: bool,
    ) -> ParsedLogEntry {
        // Try to detect log level from message content
        let level = Self::detect_log_level_from_text(log_line, is_stderr);

        ParsedLogEntry {
            level,
            message: log_line.to_string(),
            container_id: container_id.to_string(),
            node_id: node_id.map(|s| s.to_string()),
            timestamp: None,
            is_stderr,
            fields: HashMap::new(),
        }
    }

    /// Parse log level from string
    fn parse_log_level_string(s: &str) -> Option<LogLevel> {
        match s.to_uppercase().as_str() {
            "TRACE" | "TRCE" => Some(LogLevel::Trace),
            "DEBUG" | "DBG" | "DEBG" => Some(LogLevel::Debug),
            "INFO" | "INF" | "INFORMATION" => Some(LogLevel::Info),
            "WARN" | "WARNING" | "WRN" => Some(LogLevel::Warn),
            "ERROR" | "ERR" | "FATAL" | "CRITICAL" | "CRIT" => Some(LogLevel::Error),
            _ => None,
        }
    }

    /// Detect log level from plain text content
    fn detect_log_level_from_text(text: &str, is_stderr: bool) -> LogLevel {
        let text_upper = text.to_uppercase();

        if text_upper.contains("ERROR")
            || text_upper.contains("FATAL")
            || text_upper.contains("CRITICAL")
        {
            LogLevel::Error
        } else if text_upper.contains("WARN") || text_upper.contains("WARNING") {
            LogLevel::Warn
        } else if text_upper.contains("INFO") {
            LogLevel::Info
        } else if text_upper.contains("DEBUG") || text_upper.contains("TRACE") {
            LogLevel::Debug
        } else if is_stderr {
            // Default stderr to Error level
            LogLevel::Error
        } else {
            // Default stdout to Info level
            LogLevel::Info
        }
    }
}

/// Parse Docker API version string into tuple format
///
/// Converts version strings like "1.40" or "1.41" into (major, minor) tuples
/// for easy comparison.
fn parse_api_version(version_str: &str) -> Result<(u32, u32)> {
    let parts: Vec<&str> = version_str.split('.').collect();

    if parts.len() < 2 {
        return Err(Error::Execution(format!(
            "Invalid Docker API version format: {}",
            version_str
        )));
    }

    let major = parts[0].parse::<u32>().map_err(|_| {
        Error::Execution(format!(
            "Could not parse major version from: {}",
            version_str
        ))
    })?;

    let minor = parts[1].parse::<u32>().map_err(|_| {
        Error::Execution(format!(
            "Could not parse minor version from: {}",
            version_str
        ))
    })?;

    Ok((major, minor))
}

/// Log configuration for Docker operations
pub struct DockerLogger;

impl DockerLogger {
    /// Initialize Docker-specific logging
    pub fn init() {
        // This would be called once during executor initialization
        info!("Docker logging initialized");
    }

    /// Log a Docker build step
    pub fn log_build_step(step: usize, total: usize, message: &str) {
        info!(
            step = step,
            total = total,
            progress = format!("{}/{}", step, total),
            "{}",
            message
        );
    }

    /// Log resource usage
    pub fn log_resource_usage(container_id: &str, cpu: f32, memory_mb: u64) {
        debug!(
            container_id = %container_id,
            cpu_percent = cpu,
            memory_mb = memory_mb,
            "Container resource usage"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_security_config_default() {
        let config = SecurityConfig::default();
        assert_eq!(config.cap_drop, vec!["ALL".to_string()]);
        assert_eq!(
            config.cap_add,
            vec!["IPC_LOCK".to_string(), "SYS_NICE".to_string()]
        );
        assert!(config.read_only_rootfs);
        assert_eq!(
            config.security_opt,
            vec!["no-new-privileges:true".to_string()]
        );
        assert_eq!(config.user, "1000");
        assert_eq!(config.group, "1000");
        assert!(config.enable_apparmor);
        assert_eq!(config.apparmor_profile, "docker-default");
    }

    #[test]
    fn test_security_config_permissive() {
        let config = SecurityConfig::permissive();
        assert!(config.cap_drop.is_empty());
        assert!(config.cap_add.is_empty());
        assert!(!config.read_only_rootfs);
        assert!(config.security_opt.is_empty());
        assert_eq!(config.user, "root");
        assert!(!config.enable_apparmor);
    }

    #[test]
    fn test_security_config_strict() {
        let config = SecurityConfig::strict();
        assert_eq!(config.cap_drop, vec!["ALL".to_string()]);
        assert_eq!(config.cap_add, vec!["IPC_LOCK".to_string()]); // Only IPC_LOCK
        assert!(config.read_only_rootfs);
        assert_eq!(config.user, "65534"); // nobody
        assert!(config.enable_apparmor);
    }

    #[test]
    fn test_security_config_validation_warns_root() {
        let config = SecurityConfig {
            user: "root".to_string(),
            ..Default::default()
        };
        // Should succeed but log warning
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_security_config_validation_empty_user() {
        let config = SecurityConfig {
            user: "".to_string(),
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_docker_node_config_with_security() {
        let config = DockerNodeConfig {
            python_version: "3.10".to_string(),
            base_image: None,
            system_packages: vec![],
            python_packages: vec![],
            memory_mb: 1024,
            cpu_cores: 1.0,
            gpu_devices: vec![],
            shm_size_mb: 512,
            env_vars: HashMap::new(),
            volumes: vec![],
            security: SecurityConfig::default(),
        };

        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_docker_node_config_validation_includes_security() {
        let config = DockerNodeConfig {
            python_version: "3.10".to_string(),
            base_image: None,
            system_packages: vec![],
            python_packages: vec![],
            memory_mb: 1024,
            cpu_cores: 1.0,
            gpu_devices: vec![],
            shm_size_mb: 512,
            env_vars: HashMap::new(),
            volumes: vec![],
            security: SecurityConfig {
                user: "".to_string(), // Invalid: empty user
                ..Default::default()
            },
        };

        // Should fail validation due to empty user
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_container_metrics_creation() {
        let metrics = ContainerMetrics::new("test_container".to_string(), Some(100));
        assert_eq!(metrics.container_id(), "test_container");
        assert_eq!(metrics.data_point_count(), 0);
        assert_eq!(metrics.restart_count(), 0);
    }

    #[test]
    fn test_add_data_point() {
        let mut metrics = ContainerMetrics::new("test_container".to_string(), Some(5));

        // Add data points
        for i in 0..10 {
            let data_point = MetricDataPoint {
                timestamp: std::time::SystemTime::now(),
                cpu_percent: (i * 10) as f32,
                memory_mb: (i * 100) as u64,
                memory_limit_mb: Some(1024),
                network_rx_bytes: Some(1000),
                network_tx_bytes: Some(500),
                uptime_secs: i * 60,
            };
            metrics.add_data_point(data_point);
        }

        // Should only keep the last 5 points (circular buffer)
        assert_eq!(metrics.data_point_count(), 5);
    }

    #[test]
    fn test_get_recent_points() {
        let mut metrics = ContainerMetrics::new("test_container".to_string(), Some(100));

        // Add 10 data points
        for i in 0..10 {
            let data_point = MetricDataPoint {
                timestamp: std::time::SystemTime::now(),
                cpu_percent: (i * 10) as f32,
                memory_mb: (i * 100) as u64,
                memory_limit_mb: Some(1024),
                network_rx_bytes: None,
                network_tx_bytes: None,
                uptime_secs: i * 60,
            };
            metrics.add_data_point(data_point);
        }

        // Get recent 3 points
        let recent = metrics.get_recent_points(3);
        assert_eq!(recent.len(), 3);

        // Most recent should be the last one added (90% CPU)
        assert_eq!(recent[0].cpu_percent, 90.0);
    }

    #[test]
    fn test_calculate_aggregates() {
        let mut metrics = ContainerMetrics::new("test_container".to_string(), Some(100));

        // Add data points with known values
        for i in 0..10 {
            let data_point = MetricDataPoint {
                timestamp: std::time::SystemTime::now(),
                cpu_percent: 50.0, // All 50%
                memory_mb: 100,    // All 100MB
                memory_limit_mb: Some(1024),
                network_rx_bytes: Some(1000),
                network_tx_bytes: Some(500),
                uptime_secs: i * 60,
            };
            metrics.add_data_point(data_point);
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        // Calculate aggregates for the last 1 minute
        let aggregates = metrics.calculate_aggregates(std::time::Duration::from_secs(60));
        assert!(aggregates.is_some());

        let aggregates = aggregates.unwrap();
        assert_eq!(aggregates.sample_count, 10);
        assert_eq!(aggregates.avg_cpu_percent, 50.0);
        assert_eq!(aggregates.peak_cpu_percent, 50.0);
        assert_eq!(aggregates.min_cpu_percent, 50.0);
        assert_eq!(aggregates.avg_memory_mb, 100);
        assert_eq!(aggregates.peak_memory_mb, 100);
        assert_eq!(aggregates.min_memory_mb, 100);
    }

    #[test]
    fn test_restart_count() {
        let mut metrics = ContainerMetrics::new("test_container".to_string(), Some(100));
        assert_eq!(metrics.restart_count(), 0);

        metrics.increment_restart_count();
        assert_eq!(metrics.restart_count(), 1);

        metrics.increment_restart_count();
        assert_eq!(metrics.restart_count(), 2);
    }

    #[test]
    fn test_uptime() {
        let metrics = ContainerMetrics::new("test_container".to_string(), Some(100));
        std::thread::sleep(std::time::Duration::from_millis(100));

        let uptime = metrics.uptime();
        assert!(uptime >= std::time::Duration::from_millis(100));
    }

    #[tokio::test]
    async fn test_metrics_collector_creation() {
        let collector = MetricsCollector::new(Some(std::time::Duration::from_secs(1)), Some(100));

        let containers = collector.get_monitored_containers().await;
        assert_eq!(containers.len(), 0);
    }

    #[tokio::test]
    async fn test_metrics_collector_aggregates() {
        let collector = MetricsCollector::new(None, None);

        // Get aggregates for non-existent container
        let aggregates = collector
            .get_aggregates("non_existent", std::time::Duration::from_secs(60))
            .await;
        assert!(aggregates.is_none());
    }

    #[test]
    fn test_aggregated_metrics_with_variable_values() {
        let mut metrics = ContainerMetrics::new("test_container".to_string(), Some(100));

        // Add data points with varying values
        let cpu_values = vec![10.0, 20.0, 30.0, 40.0, 50.0];
        let memory_values = vec![100, 200, 300, 400, 500];

        for (i, (&cpu, &mem)) in cpu_values.iter().zip(memory_values.iter()).enumerate() {
            let data_point = MetricDataPoint {
                timestamp: std::time::SystemTime::now(),
                cpu_percent: cpu,
                memory_mb: mem,
                memory_limit_mb: Some(1024),
                network_rx_bytes: Some((i * 1000) as u64),
                network_tx_bytes: Some((i * 500) as u64),
                uptime_secs: i as u64 * 60,
            };
            metrics.add_data_point(data_point);
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        let aggregates = metrics
            .calculate_aggregates(std::time::Duration::from_secs(60))
            .unwrap();

        // Check averages
        assert_eq!(aggregates.avg_cpu_percent, 30.0); // (10+20+30+40+50)/5
        assert_eq!(aggregates.avg_memory_mb, 300); // (100+200+300+400+500)/5

        // Check peaks
        assert_eq!(aggregates.peak_cpu_percent, 50.0);
        assert_eq!(aggregates.peak_memory_mb, 500);

        // Check minimums
        assert_eq!(aggregates.min_cpu_percent, 10.0);
        assert_eq!(aggregates.min_memory_mb, 100);

        // Check network I/O (should be max values)
        assert_eq!(aggregates.total_network_rx_bytes, Some(4000));
        assert_eq!(aggregates.total_network_tx_bytes, Some(2000));
    }

    // Tests for log forwarding functionality (T059)

    #[test]
    fn test_log_forwarding_config_default() {
        let config = LogForwardingConfig::default();
        assert!(config.enabled);
        assert_eq!(config.buffer_size, 65536); // 64KB
        assert_eq!(config.min_level, LogLevel::Debug);
        assert!(config.parse_json);
        assert!(config.include_timestamps);
    }

    #[test]
    fn test_log_level_ordering() {
        assert!(LogLevel::Trace < LogLevel::Debug);
        assert!(LogLevel::Debug < LogLevel::Info);
        assert!(LogLevel::Info < LogLevel::Warn);
        assert!(LogLevel::Warn < LogLevel::Error);
    }

    #[test]
    fn test_log_level_to_tracing_level() {
        assert_eq!(LogLevel::Trace.to_tracing_level(), Level::TRACE);
        assert_eq!(LogLevel::Debug.to_tracing_level(), Level::DEBUG);
        assert_eq!(LogLevel::Info.to_tracing_level(), Level::INFO);
        assert_eq!(LogLevel::Warn.to_tracing_level(), Level::WARN);
        assert_eq!(LogLevel::Error.to_tracing_level(), Level::ERROR);
    }

    #[test]
    fn test_parse_log_level_string() {
        assert_eq!(
            DockerSupport::parse_log_level_string("TRACE"),
            Some(LogLevel::Trace)
        );
        assert_eq!(
            DockerSupport::parse_log_level_string("DEBUG"),
            Some(LogLevel::Debug)
        );
        assert_eq!(
            DockerSupport::parse_log_level_string("INFO"),
            Some(LogLevel::Info)
        );
        assert_eq!(
            DockerSupport::parse_log_level_string("WARN"),
            Some(LogLevel::Warn)
        );
        assert_eq!(
            DockerSupport::parse_log_level_string("WARNING"),
            Some(LogLevel::Warn)
        );
        assert_eq!(
            DockerSupport::parse_log_level_string("ERROR"),
            Some(LogLevel::Error)
        );
        assert_eq!(
            DockerSupport::parse_log_level_string("FATAL"),
            Some(LogLevel::Error)
        );
        assert_eq!(
            DockerSupport::parse_log_level_string("CRITICAL"),
            Some(LogLevel::Error)
        );

        // Case insensitive
        assert_eq!(
            DockerSupport::parse_log_level_string("debug"),
            Some(LogLevel::Debug)
        );
        assert_eq!(
            DockerSupport::parse_log_level_string("Debug"),
            Some(LogLevel::Debug)
        );

        // Unknown level
        assert_eq!(DockerSupport::parse_log_level_string("UNKNOWN"), None);
    }

    #[test]
    fn test_detect_log_level_from_text() {
        // Error detection
        assert_eq!(
            DockerSupport::detect_log_level_from_text("ERROR: something went wrong", false),
            LogLevel::Error
        );
        assert_eq!(
            DockerSupport::detect_log_level_from_text("FATAL error occurred", false),
            LogLevel::Error
        );
        assert_eq!(
            DockerSupport::detect_log_level_from_text("CRITICAL failure", false),
            LogLevel::Error
        );

        // Warning detection
        assert_eq!(
            DockerSupport::detect_log_level_from_text("WARN: potential issue", false),
            LogLevel::Warn
        );
        assert_eq!(
            DockerSupport::detect_log_level_from_text("WARNING: check this", false),
            LogLevel::Warn
        );

        // Info detection
        assert_eq!(
            DockerSupport::detect_log_level_from_text("INFO: processing started", false),
            LogLevel::Info
        );

        // Debug detection
        assert_eq!(
            DockerSupport::detect_log_level_from_text("DEBUG: variable value is 42", false),
            LogLevel::Debug
        );
        assert_eq!(
            DockerSupport::detect_log_level_from_text("TRACE: entering function", false),
            LogLevel::Debug
        );

        // Default for stdout
        assert_eq!(
            DockerSupport::detect_log_level_from_text("Some normal message", false),
            LogLevel::Info
        );

        // Default for stderr
        assert_eq!(
            DockerSupport::detect_log_level_from_text("Some stderr message", true),
            LogLevel::Error
        );
    }

    #[test]
    fn test_try_parse_json_log() {
        // Valid JSON with standard fields
        let json_log =
            r#"{"level":"info","message":"Test message","timestamp":"2024-01-01T00:00:00Z"}"#;
        let entry =
            DockerSupport::try_parse_json_log(json_log, "container123", Some("node1"), false);

        assert!(entry.is_some());
        let entry = entry.unwrap();
        assert_eq!(entry.level, LogLevel::Info);
        assert_eq!(entry.message, "Test message");
        assert_eq!(entry.container_id, "container123");
        assert_eq!(entry.node_id, Some("node1".to_string()));
        assert_eq!(entry.timestamp, Some("2024-01-01T00:00:00Z".to_string()));
        assert!(!entry.is_stderr);

        // JSON with alternative field names
        let json_log2 =
            r#"{"severity":"error","msg":"Error occurred","time":"2024-01-01T01:00:00Z"}"#;
        let entry2 = DockerSupport::try_parse_json_log(json_log2, "container456", None, true);

        assert!(entry2.is_some());
        let entry2 = entry2.unwrap();
        assert_eq!(entry2.level, LogLevel::Error);
        assert_eq!(entry2.message, "Error occurred");
        assert_eq!(entry2.timestamp, Some("2024-01-01T01:00:00Z".to_string()));
        assert!(entry2.is_stderr);

        // JSON with extra fields
        let json_log3 = r#"{"level":"warn","message":"Warning","user":"alice","request_id":12345}"#;
        let entry3 = DockerSupport::try_parse_json_log(json_log3, "container789", None, false);

        assert!(entry3.is_some());
        let entry3 = entry3.unwrap();
        assert_eq!(entry3.level, LogLevel::Warn);
        assert_eq!(entry3.message, "Warning");
        assert_eq!(entry3.fields.len(), 2); // user and request_id
        assert!(entry3.fields.contains_key("user"));
        assert!(entry3.fields.contains_key("request_id"));

        // Invalid JSON
        let invalid_json = "not a valid json";
        let entry4 = DockerSupport::try_parse_json_log(invalid_json, "container999", None, false);
        assert!(entry4.is_none());
    }

    #[test]
    fn test_parse_plain_text_log() {
        let entry = DockerSupport::parse_plain_text_log(
            "ERROR: Something went wrong",
            "container123",
            Some("node1"),
            false,
        );

        assert_eq!(entry.level, LogLevel::Error);
        assert_eq!(entry.message, "ERROR: Something went wrong");
        assert_eq!(entry.container_id, "container123");
        assert_eq!(entry.node_id, Some("node1".to_string()));
        assert_eq!(entry.timestamp, None);
        assert!(!entry.is_stderr);
        assert!(entry.fields.is_empty());
    }

    #[test]
    fn test_log_forwarding_config_custom() {
        let config = LogForwardingConfig {
            enabled: false,
            buffer_size: 4096,
            min_level: LogLevel::Warn,
            parse_json: false,
            include_timestamps: false,
        };

        assert!(!config.enabled);
        assert_eq!(config.buffer_size, 4096);
        assert_eq!(config.min_level, LogLevel::Warn);
        assert!(!config.parse_json);
        assert!(!config.include_timestamps);
    }
}
