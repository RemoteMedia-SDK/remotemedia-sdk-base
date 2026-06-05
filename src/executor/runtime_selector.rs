//! Runtime Selector (Phase 1.10.6)
//!
//! This module provides runtime selection logic for Python nodes.
//! It determines whether to use RustPython, CPython (PyO3), or CPython WASM
//! based on:
//! - Explicit runtime hints in the manifest
//! - Environment variables (REMOTEMEDIA_PYTHON_RUNTIME, REMOTEMEDIA_EXECUTION_STRATEGY)
//! - Auto-detection based on node requirements and capabilities
//!
//! Decision hierarchy:
//! 1. Explicit manifest runtime_hint (if provided)
//! 2. Environment variable override (REMOTEMEDIA_PYTHON_RUNTIME)
//! 3. Execution strategy override (REMOTEMEDIA_EXECUTION_STRATEGY)
//! 4. Auto-detection based on node characteristics and target platform

use crate::manifest::{NodeManifest, RuntimeHint};
use std::env;

/// Execution strategy for Python nodes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionStrategy {
    /// In-process execution via PyO3
    InProcess,
    /// Subprocess execution via iceoryx2 IPC
    Subprocess,
    /// Auto-detect based on platform and node requirements
    Auto,
}

/// Selected runtime for a Python node
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectedRuntime {
    /// Use RustPython embedded interpreter
    RustPython,

    /// Use CPython via PyO3 in-process
    CPython,

    /// Use CPython compiled to WASM (Phase 3, not yet implemented)
    CPythonWasm,
}

/// Runtime selector with auto-detection logic
pub struct RuntimeSelector {
    /// Global runtime override from environment variable
    env_override: Option<RuntimeHint>,

    /// Execution strategy from environment variable
    execution_strategy: ExecutionStrategy,

    /// Whether to enable fallback from RustPython to CPython on error
    enable_fallback: bool,
}

impl RuntimeSelector {
    /// Create a new runtime selector
    ///
    /// Reads REMOTEMEDIA_PYTHON_RUNTIME and REMOTEMEDIA_EXECUTION_STRATEGY
    /// environment variables on construction.
    pub fn new() -> Self {
        let env_override = env::var("REMOTEMEDIA_PYTHON_RUNTIME")
            .ok()
            .and_then(|val| Self::parse_runtime_hint(&val));

        let execution_strategy = env::var("REMOTEMEDIA_EXECUTION_STRATEGY")
            .ok()
            .and_then(|val| Self::parse_execution_strategy(&val))
            .unwrap_or(ExecutionStrategy::Auto);

        let enable_fallback = env::var("REMOTEMEDIA_ENABLE_FALLBACK")
            .ok()
            .and_then(|val| val.parse::<bool>().ok())
            .unwrap_or(true); // Fallback enabled by default

        if let Some(hint) = env_override {
            tracing::info!(
                "Runtime override from environment: REMOTEMEDIA_PYTHON_RUNTIME={:?}",
                hint
            );
        }

        if execution_strategy != ExecutionStrategy::Auto {
            tracing::info!(
                "Execution strategy override from environment: REMOTEMEDIA_EXECUTION_STRATEGY={:?}",
                execution_strategy
            );
        }

        if !enable_fallback {
            tracing::info!("Runtime fallback disabled via REMOTEMEDIA_ENABLE_FALLBACK=false");
        }

        Self {
            env_override,
            execution_strategy,
            enable_fallback,
        }
    }

    /// Select runtime for a given node
    ///
    /// Decision process:
    /// 1. If execution strategy explicitly set to InProcess, use CPython
    /// 2. If execution strategy explicitly set to Subprocess, use subprocess (if available)
    /// 3. If environment variable set, use it (unless node explicitly specifies runtime)
    /// 4. If node has runtime_hint, use it
    /// 5. If executing on Android target, default to CPython (in-process only)
    /// 6. Otherwise, auto-detect based on node characteristics
    pub fn select_runtime(&self, node: &NodeManifest) -> SelectedRuntime {
        // Check execution strategy override first
        match self.execution_strategy {
            ExecutionStrategy::InProcess => {
                tracing::info!("In-process execution forced via REMOTEMEDIA_EXECUTION_STRATEGY=inprocess for node {}", node.id);
                return SelectedRuntime::CPython;
            }
            ExecutionStrategy::Subprocess => {
                // On Android, subprocess is unavailable
                if cfg!(target_os = "android") || std::env::consts::OS == "android" {
                    tracing::error!(
                        "REMOTEMEDIA_EXECUTION_STRATEGY=subprocess requested on Android, but subprocess execution is unavailable. Falling back to in-process CPython."
                    );
                    return SelectedRuntime::CPython;
                }
                tracing::info!("Subprocess execution forced via REMOTEMEDIA_EXECUTION_STRATEGY=subprocess for node {}", node.id);
                // Subprocess still uses CPython runtime
                return SelectedRuntime::CPython;
            }
            ExecutionStrategy::Auto => {
                // Continue to normal selection logic
            }
        }

        // Check for explicit runtime hint in manifest first
        if let Some(hint) = node.runtime_hint {
            match hint {
                RuntimeHint::RustPython => {
                    // RustPython is not available on Android
                    if cfg!(target_os = "android") || std::env::consts::OS == "android" {
                        tracing::warn!(
                            "Node {} explicitly requests RustPython but running on Android. Falling back to CPython (in-process).",
                            node.id
                        );
                        return SelectedRuntime::CPython;
                    }
                    tracing::info!("Node {} explicitly requests RustPython", node.id);
                    return SelectedRuntime::RustPython;
                }
                RuntimeHint::Cpython => {
                    tracing::info!("Node {} explicitly requests CPython", node.id);
                    return SelectedRuntime::CPython;
                }
                RuntimeHint::CpythonWasm => {
                    tracing::warn!(
                        "Node {} requests CPython WASM (Phase 3 - not implemented), falling back to CPython",
                        node.id
                    );
                    return SelectedRuntime::CPython;
                }
                RuntimeHint::Auto => {
                    // Continue to auto-detection below
                }
            }
        }

        // Check environment variable override
        if let Some(hint) = self.env_override {
            match hint {
                RuntimeHint::RustPython => {
                    if cfg!(target_os = "android") || std::env::consts::OS == "android" {
                        tracing::warn!(
                            "RustPython requested via env but running on Android. Using CPython (in-process) for node {}",
                            node.id
                        );
                        return SelectedRuntime::CPython;
                    }
                    tracing::info!("Using RustPython (env override) for node {}", node.id);
                    return SelectedRuntime::RustPython;
                }
                RuntimeHint::Cpython => {
                    tracing::info!("Using CPython (env override) for node {}", node.id);
                    return SelectedRuntime::CPython;
                }
                RuntimeHint::CpythonWasm => {
                    tracing::warn!(
                        "CPython WASM requested via env (Phase 3 - not implemented), using CPython for node {}",
                        node.id
                    );
                    return SelectedRuntime::CPython;
                }
                RuntimeHint::Auto => {
                    // Continue to auto-detection
                }
            }
        }

        // Auto-detection based on node characteristics and platform
        self.auto_detect_runtime(node)
    }

    /// Auto-detect the best runtime for a node
    ///
    /// Heuristics:
    /// - If target is Android → CPython (in-process only)
    /// - If node has GPU requirements → CPython (likely needs torch/transformers)
    /// - If node has high memory requirements (>4GB) → CPython (likely ML workload)
    /// - If node type contains known C-extension keywords → CPython
    /// - Otherwise → RustPython (faster, lower overhead for simple nodes)
    fn auto_detect_runtime(&self, node: &NodeManifest) -> SelectedRuntime {
        // Check for Android target first (compile-time or runtime)
        if cfg!(target_os = "android") || std::env::consts::OS == "android" {
            tracing::info!(
                "Android target detected, defaulting to CPython (in-process only) for node {}",
                node.id
            );
            return SelectedRuntime::CPython;
        }

        // Check for GPU requirements
        if let Some(caps) = &node.capabilities {
            if caps.gpu.is_some() {
                tracing::info!(
                    "Node {} requires GPU, selecting CPython (likely ML workload)",
                    node.id
                );
                return SelectedRuntime::CPython;
            }

            // Check for high memory requirements (>4GB suggests ML)
            if let Some(mem_gb) = caps.memory_gb {
                if mem_gb > 4.0 {
                    tracing::info!(
                        "Node {} requires {}GB memory, selecting CPython (likely ML workload)",
                        node.id,
                        mem_gb
                    );
                    return SelectedRuntime::CPython;
                }
            }
        }

        // Check node type for known C-extension dependencies
        let node_type_lower = node.node_type.to_lowercase();
        let cpython_keywords = [
            "torch",
            "transformers",
            "pandas",
            "numpy",
            "scipy",
            "sklearn",
            "cv2",
            "opencv",
            "tensorflow",
            "keras",
            "jax",
            "pil",
            "pillow",
        ];

        for keyword in &cpython_keywords {
            if node_type_lower.contains(keyword) {
                tracing::info!(
                    "Node {} type contains '{}', selecting CPython (C-extension dependency)",
                    node.id,
                    keyword
                );
                return SelectedRuntime::CPython;
            }
        }

        // Default to RustPython for simple nodes
        tracing::info!(
            "Node {} has no special requirements, selecting RustPython (default)",
            node.id
        );
        SelectedRuntime::RustPython
    }

    /// Check if fallback is enabled
    pub fn is_fallback_enabled(&self) -> bool {
        self.enable_fallback
    }

    /// Get the execution strategy
    pub fn execution_strategy(&self) -> ExecutionStrategy {
        self.execution_strategy
    }

    /// Parse runtime hint from string
    fn parse_runtime_hint(s: &str) -> Option<RuntimeHint> {
        match s.to_lowercase().as_str() {
            "rustpython" | "rust" => Some(RuntimeHint::RustPython),
            "cpython" | "python" => Some(RuntimeHint::Cpython),
            "cpython_wasm" | "wasm" => Some(RuntimeHint::CpythonWasm),
            "auto" => Some(RuntimeHint::Auto),
            _ => {
                tracing::warn!("Invalid REMOTEMEDIA_PYTHON_RUNTIME value: {}", s);
                None
            }
        }
    }

    /// Parse execution strategy from string
    fn parse_execution_strategy(s: &str) -> Option<ExecutionStrategy> {
        match s.to_lowercase().as_str() {
            "inprocess" | "in_process" | "in-process" => Some(ExecutionStrategy::InProcess),
            "subprocess" | "sub_process" | "sub-process" => Some(ExecutionStrategy::Subprocess),
            "auto" => Some(ExecutionStrategy::Auto),
            _ => {
                tracing::warn!("Invalid REMOTEMEDIA_EXECUTION_STRATEGY value: {}", s);
                None
            }
        }
    }
}

impl Default for RuntimeSelector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{CapabilityRequirements, GpuRequirement};
    use serde_json::Value;

    fn create_test_node(
        id: &str,
        node_type: &str,
        runtime_hint: Option<RuntimeHint>,
        capabilities: Option<CapabilityRequirements>,
    ) -> NodeManifest {
        NodeManifest {
            id: id.to_string(),
            node_type: node_type.to_string(),
            params: Value::Null,
            is_streaming: false,
            is_output_node: false,
            capabilities,
            host: None,
            runtime_hint,
            execution: None,
            ..Default::default()
        }
    }

    #[test]
    fn test_explicit_runtime_hint() {
        let selector = RuntimeSelector::new();

        let node = create_test_node("test", "SimpleNode", Some(RuntimeHint::Cpython), None);
        assert_eq!(selector.select_runtime(&node), SelectedRuntime::CPython);

        let node = create_test_node("test", "SimpleNode", Some(RuntimeHint::RustPython), None);
        assert_eq!(selector.select_runtime(&node), SelectedRuntime::RustPython);
    }

    #[test]
    fn test_auto_detection_gpu() {
        let selector = RuntimeSelector::new();

        let caps = CapabilityRequirements {
            gpu: Some(GpuRequirement {
                gpu_type: "cuda".to_string(),
                min_memory_gb: Some(8.0),
                required: true,
            }),
            cpu: None,
            memory_gb: None,
        };

        let node = create_test_node("gpu_node", "SimpleNode", None, Some(caps));
        assert_eq!(selector.select_runtime(&node), SelectedRuntime::CPython);
    }

    #[test]
    fn test_auto_detection_memory() {
        let selector = RuntimeSelector::new();

        let caps = CapabilityRequirements {
            gpu: None,
            cpu: None,
            memory_gb: Some(8.0),
        };

        let node = create_test_node("mem_node", "SimpleNode", None, Some(caps));
        assert_eq!(selector.select_runtime(&node), SelectedRuntime::CPython);
    }

    #[test]
    fn test_auto_detection_node_type() {
        let selector = RuntimeSelector::new();

        // Torch node
        let node = create_test_node("torch_node", "TorchModel", None, None);
        assert_eq!(selector.select_runtime(&node), SelectedRuntime::CPython);

        // Transformers node
        let node = create_test_node("hf_node", "TransformersNode", None, None);
        assert_eq!(selector.select_runtime(&node), SelectedRuntime::CPython);

        // Simple node
        let node = create_test_node("simple_node", "PassThroughNode", None, None);
        assert_eq!(selector.select_runtime(&node), SelectedRuntime::RustPython);
    }

    #[test]
    fn test_parse_runtime_hint() {
        assert_eq!(
            RuntimeSelector::parse_runtime_hint("rustpython"),
            Some(RuntimeHint::RustPython)
        );
        assert_eq!(
            RuntimeSelector::parse_runtime_hint("cpython"),
            Some(RuntimeHint::Cpython)
        );
        assert_eq!(
            RuntimeSelector::parse_runtime_hint("auto"),
            Some(RuntimeHint::Auto)
        );
        assert_eq!(RuntimeSelector::parse_runtime_hint("invalid"), None);
    }
}
