//! Node provider trait for auto-registration of node factories
//!
//! This module defines the `NodeProvider` trait that allows external crates
//! to register their node factories with the streaming registry at compile time.
//!
//! # Usage
//!
//! Node crates implement `NodeProvider` and use `inventory::submit!` to register:
//!
//! ```ignore
//! use remotemedia_core::nodes::{NodeProvider, StreamingNodeRegistry};
//!
//! struct MyNodesProvider;
//!
//! impl NodeProvider for MyNodesProvider {
//!     fn register(&self, registry: &mut StreamingNodeRegistry) {
//!         registry.register(Arc::new(MyNodeFactory));
//!     }
//!
//!     fn provider_name(&self) -> &'static str {
//!         "my-nodes"
//!     }
//! }
//!
//! // Auto-register at compile time
//! inventory::submit! {
//!     &MyNodesProvider as &'static dyn NodeProvider
//! }
//! ```
//!
//! # Automatic Collection
//!
//! All registered providers are automatically collected when calling
//! `create_default_streaming_registry()`:
//!
//! ```ignore
//! let registry = create_default_streaming_registry();
//! // All providers have been loaded
//! ```

use crate::nodes::streaming_node::StreamingNodeRegistry;

/// Trait for node providers that register factories with the streaming registry.
///
/// Implement this trait in your node crate and use `inventory::submit!` to
/// automatically register your nodes when the crate is linked.
///
/// # Example
///
/// ```ignore
/// struct AudioNodesProvider;
///
/// impl NodeProvider for AudioNodesProvider {
///     fn register(&self, registry: &mut StreamingNodeRegistry) {
///         registry.register(Arc::new(ResampleNodeFactory));
///         registry.register(Arc::new(VadNodeFactory));
///     }
///
///     fn provider_name(&self) -> &'static str {
///         "audio-nodes"
///     }
///
///     fn node_count(&self) -> usize {
///         2
///     }
/// }
/// ```
pub trait NodeProvider: Send + Sync {
    /// Register all node factories from this provider with the registry.
    ///
    /// Called automatically when `create_default_streaming_registry()` collects
    /// all providers via `inventory::iter`.
    fn register(&self, registry: &mut StreamingNodeRegistry);

    /// Get the human-readable name of this provider.
    ///
    /// Used for logging and debugging. Should be a short kebab-case identifier
    /// like "core-nodes", "python-nodes", "candle-whisper".
    fn provider_name(&self) -> &'static str;

    /// Get the number of node types this provider registers.
    ///
    /// Used for logging. Default returns 0 (unknown count).
    fn node_count(&self) -> usize {
        0
    }

    /// Get the priority of this provider (higher = registered first).
    ///
    /// Providers with higher priority are registered before those with lower
    /// priority. This allows overriding nodes from lower-priority providers.
    ///
    /// Default priority is 100. Core nodes use 1000, user nodes typically use 0-99.
    fn priority(&self) -> i32 {
        100
    }
}

// Collect all NodeProvider implementations at compile time
// We use &'static dyn NodeProvider to allow const initialization
inventory::collect!(&'static dyn NodeProvider);

/// Iterate over all registered node providers.
///
/// Providers are yielded in priority order (highest first).
pub fn iter_providers() -> impl Iterator<Item = &'static dyn NodeProvider> {
    let mut providers: Vec<_> = inventory::iter::<&'static dyn NodeProvider>
        .into_iter()
        .copied()
        .collect();
    providers.sort_by(|a, b| b.priority().cmp(&a.priority()));
    providers.into_iter()
}

/// Get the total count of registered providers.
pub fn provider_count() -> usize {
    inventory::iter::<&'static dyn NodeProvider>
        .into_iter()
        .count()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_provider_count() {
        // At minimum, should be able to count providers (may be 0 in unit tests)
        let count = provider_count();
        assert!(count >= 0);
    }

    #[test]
    fn test_iter_providers() {
        // Should be able to iterate (may be empty in unit tests)
        for provider in iter_providers() {
            assert!(!provider.provider_name().is_empty());
        }
    }
}
