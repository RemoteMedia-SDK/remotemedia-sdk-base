//! Multiprocess execution for Python nodes via iceoryx2 IPC

pub mod data_transfer;
#[cfg(feature = "multiprocess")]
pub mod health_monitor;
#[cfg(feature = "multiprocess")]
pub mod ipc_channel;
#[cfg(feature = "multiprocess")]
pub mod multiprocess_executor;
#[cfg(feature = "multiprocess")]
pub mod process_manager;
// In-process channel for Android (and opt-in on Linux/macOS)
#[cfg(feature = "inprocess-python")]
pub mod inprocess_channel;

#[cfg(feature = "docker")]
pub mod docker_support;

#[cfg(feature = "docker")]
pub mod container_builder;

pub use data_transfer::{from_python_wire, to_python_wire};
#[cfg(feature = "multiprocess")]
pub use multiprocess_executor::{
    InitStatus, MultiprocessConfig, MultiprocessExecutor, SessionState,
};

// Re-export env manager types for convenience
pub use super::env_manager::{EnvScope, PythonEnvConfig, PythonEnvMode};

#[cfg(feature = "docker")]
pub use docker_support::ResourceUsageStats;

/// Execution mode for Python nodes
#[derive(Debug, Clone)]
pub enum ExecutionMode {
    /// Direct process execution (existing)
    Native,

    /// Process with IPC (existing)
    Multiprocess,

    /// In-process execution via PyO3 (Android, and opt-in on Linux/macOS)
    InProcess,

    /// Docker container execution (NEW)
    #[cfg(feature = "docker")]
    Docker {
        /// Whether to use Docker
        use_docker: bool,
        /// Docker configuration
        docker_config: Option<docker_support::DockerNodeConfig>,
    },
}

impl PartialEq for ExecutionMode {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (ExecutionMode::Native, ExecutionMode::Native) => true,
            (ExecutionMode::Multiprocess, ExecutionMode::Multiprocess) => true,
            (ExecutionMode::InProcess, ExecutionMode::InProcess) => true,
            #[cfg(feature = "docker")]
            (
                ExecutionMode::Docker {
                    use_docker: use_docker1,
                    docker_config: config1,
                },
                ExecutionMode::Docker {
                    use_docker: use_docker2,
                    docker_config: config2,
                },
            ) => {
                // Compare use_docker flags
                if use_docker1 != use_docker2 {
                    return false;
                }

                // Compare docker configs by serializing and comparing JSON
                // This is necessary because DockerExecutorConfig doesn't implement PartialEq
                match (config1, config2) {
                    (None, None) => true,
                    (Some(c1), Some(c2)) => {
                        // Compare via config hash which is deterministic
                        c1.compute_config_hash() == c2.compute_config_hash()
                    }
                    _ => false,
                }
            }
            _ => false,
        }
    }
}
