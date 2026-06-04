//! Executor Registry for mapping node types to executor implementations
//!
//! Provides pattern-based routing to determine which executor (Native, Multiprocess, WASM)
//! should handle a given node type based on manifest node_type field.

use regex::Regex;
use std::collections::HashMap;

/// Type of executor for node execution
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExecutorType {
    /// Native Rust nodes (executed in same process)
    Native,

    /// Multiprocess Python nodes (separate processes with independent GILs)
    #[cfg(feature = "multiprocess")]
    Multiprocess,

    /// WebAssembly nodes (sandboxed execution)
    #[cfg(feature = "wasmtime-runtime")]
    Wasm,
}

impl ExecutorType {
    /// Get string name for this executor type
    pub fn as_str(&self) -> &'static str {
        match self {
            ExecutorType::Native => "native",
            #[cfg(feature = "multiprocess")]
            ExecutorType::Multiprocess => "multiprocess",
            #[cfg(feature = "wasmtime-runtime")]
            ExecutorType::Wasm => "wasm",
        }
    }
}

/// Pattern rule for matching node types to executors
#[derive(Debug, Clone)]
pub struct PatternRule {
    /// Regex pattern for matching node types
    pattern: Regex,

    /// Executor type for matches
    executor_type: ExecutorType,

    /// Priority (higher = checked first)
    priority: u32,

    /// Human-readable description
    description: String,
}

impl PatternRule {
    /// Create a new pattern rule
    pub fn new(
        pattern: &str,
        executor_type: ExecutorType,
        priority: u32,
        description: impl Into<String>,
    ) -> Result<Self, regex::Error> {
        Ok(Self {
            pattern: Regex::new(pattern)?,
            executor_type,
            priority,
            description: description.into(),
        })
    }

    /// Check if this pattern matches the given node type
    pub fn matches(&self, node_type: &str) -> bool {
        self.pattern.is_match(node_type)
    }
}

/// Registry for mapping node types to executor implementations
pub struct ExecutorRegistry {
    /// Explicit node type → executor mappings (highest priority)
    explicit_mappings: HashMap<String, ExecutorType>,

    /// Pattern-based rules for matching node types
    pattern_rules: Vec<PatternRule>,

    /// Default executor for unmatched nodes
    default_executor: ExecutorType,
}

impl ExecutorRegistry {
    /// Create a new empty executor registry
    pub fn new() -> Self {
        Self {
            explicit_mappings: HashMap::new(),
            pattern_rules: Vec::new(),
            default_executor: ExecutorType::Native,
        }
    }

    /// Register an explicit node type → executor mapping
    ///
    /// This takes highest priority over pattern matching.
    pub fn register_explicit(&mut self, node_type: impl Into<String>, executor_type: ExecutorType) {
        self.explicit_mappings
            .insert(node_type.into(), executor_type);
    }

    /// Register a pattern-based matching rule
    ///
    /// Rules are evaluated in priority order (highest first).
    pub fn register_pattern(&mut self, rule: PatternRule) {
        // Insert in sorted order by priority (descending)
        let insert_pos = self
            .pattern_rules
            .iter()
            .position(|r| r.priority < rule.priority)
            .unwrap_or(self.pattern_rules.len());

        self.pattern_rules.insert(insert_pos, rule);
    }

    /// Set the default executor for unmatched node types
    pub fn set_default(&mut self, executor_type: ExecutorType) {
        self.default_executor = executor_type;
    }

    /// Determine which executor should handle a given node type
    ///
    /// Evaluation order:
    /// 1. Explicit mappings (highest priority)
    /// 2. Pattern rules (sorted by priority)
    /// 3. Default executor (fallback)
    pub fn get_executor_for_node(&self, node_type: &str) -> ExecutorType {
        // 1. Check explicit mappings first
        if let Some(&executor_type) = self.explicit_mappings.get(node_type) {
            tracing::debug!(
                "Node type '{}' matched explicit mapping: {:?}",
                node_type,
                executor_type
            );
            return executor_type;
        }

        // 2. Check pattern rules (already sorted by priority)
        for rule in &self.pattern_rules {
            if rule.matches(node_type) {
                tracing::debug!(
                    "Node type '{}' matched pattern '{}' (priority {}): {:?}",
                    node_type,
                    rule.description,
                    rule.priority,
                    rule.executor_type
                );
                return rule.executor_type;
            }
        }

        // 3. Fall back to default
        tracing::debug!(
            "Node type '{}' using default executor: {:?}",
            node_type,
            self.default_executor
        );
        self.default_executor
    }

    /// Get a summary of registered mappings
    pub fn summary(&self) -> RegistrySummary {
        RegistrySummary {
            explicit_mappings_count: self.explicit_mappings.len(),
            pattern_rules_count: self.pattern_rules.len(),
            default_executor: self.default_executor,
        }
    }
}

impl Default for ExecutorRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Summary of registry configuration
#[derive(Debug)]
pub struct RegistrySummary {
    pub explicit_mappings_count: usize,
    pub pattern_rules_count: usize,
    pub default_executor: ExecutorType,
}
