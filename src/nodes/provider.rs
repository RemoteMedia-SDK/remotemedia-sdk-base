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
//! use remotemedia_sdk_base::nodes::{NodeProvider, StreamingNodeRegistry};
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
//! # Providers from the plugin SDK
//!
//! A crate of nodes that should not depend on this crate implements
//! `remotemedia_traits::NodeFactoryProvider` instead, which hands out
//! factories rather than writing into a registry. [`iter_providers`] includes
//! those too, adapted to [`NodeProvider`], so every registry built from the
//! defaults registers them with the same priority rules.
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

use std::sync::OnceLock;

use remotemedia_traits::provider::{iter_factory_providers, NodeFactoryProvider};

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

/// A plugin SDK [`NodeFactoryProvider`] seen as a [`NodeProvider`].
struct SdkProvider(&'static dyn NodeFactoryProvider);

impl NodeProvider for SdkProvider {
    fn register(&self, registry: &mut StreamingNodeRegistry) {
        for factory in self.0.factories() {
            registry.register(factory);
        }
    }

    fn provider_name(&self) -> &'static str {
        self.0.provider_name()
    }

    fn priority(&self) -> i32 {
        self.0.priority()
    }
}

/// The plugin SDK providers linked into this binary, adapted once.
fn sdk_providers() -> &'static [SdkProvider] {
    static ADAPTED: OnceLock<Vec<SdkProvider>> = OnceLock::new();
    ADAPTED.get_or_init(|| iter_factory_providers().map(SdkProvider).collect())
}

/// Iterate over all registered node providers: this crate's
/// [`NodeProvider`]s and the plugin SDK's `NodeFactoryProvider`s.
///
/// Providers are yielded in priority order (highest first).
pub fn iter_providers() -> impl Iterator<Item = &'static dyn NodeProvider> {
    let mut providers: Vec<&'static dyn NodeProvider> = inventory::iter::<&'static dyn NodeProvider>
        .into_iter()
        .copied()
        .chain(sdk_providers().iter().map(|provider| provider as &'static dyn NodeProvider))
        .collect();
    providers.sort_by(|a, b| b.priority().cmp(&a.priority()));
    providers.into_iter()
}

/// Get the total count of registered providers, plugin SDK ones included.
pub fn provider_count() -> usize {
    inventory::iter::<&'static dyn NodeProvider>
        .into_iter()
        .count()
        + sdk_providers().len()
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

    use crate::nodes::streaming_node::{StreamingNode, StreamingNodeFactory};
    use crate::Error;
    use serde_json::Value;
    use std::sync::Arc;

    struct SdkTestFactory;

    impl StreamingNodeFactory for SdkTestFactory {
        fn create(
            &self,
            _node_id: String,
            _params: &Value,
            _session_id: Option<String>,
        ) -> Result<Box<dyn StreamingNode>, Error> {
            Err(Error::Execution("not needed by this test".into()))
        }
        fn node_type(&self) -> &str {
            "SdkProviderTestNode"
        }
    }

    struct SdkTestProvider;

    impl NodeFactoryProvider for SdkTestProvider {
        fn provider_name(&self) -> &'static str {
            "sdk-provider-test"
        }
        fn factories(&self) -> Vec<Arc<dyn StreamingNodeFactory>> {
            vec![Arc::new(SdkTestFactory)]
        }
    }

    remotemedia_traits::inventory::submit! {
        &SdkTestProvider as &'static dyn NodeFactoryProvider
    }

    #[test]
    fn sdk_providers_are_included() {
        let provider = iter_providers()
            .find(|provider| provider.provider_name() == "sdk-provider-test")
            .expect("the plugin SDK provider is listed");
        assert_eq!(provider.priority(), 100);

        let mut registry = StreamingNodeRegistry::new();
        provider.register(&mut registry);
        assert!(registry.list_types().iter().any(|t| t == "SdkProviderTestNode"));
    }

    #[test]
    fn default_registries_register_sdk_provider_nodes() {
        let defaults = crate::nodes::streaming_registry::create_default_streaming_registry();
        assert!(defaults.list_types().iter().any(|t| t == "SdkProviderTestNode"));

        let built = StreamingNodeRegistry::builder().with_defaults().build();
        assert!(built.list_types().iter().any(|t| t == "SdkProviderTestNode"));
    }

    #[test]
    fn test_iter_providers() {
        // Should be able to iterate (may be empty in unit tests)
        for provider in iter_providers() {
            assert!(!provider.provider_name().is_empty());
        }
    }
}
