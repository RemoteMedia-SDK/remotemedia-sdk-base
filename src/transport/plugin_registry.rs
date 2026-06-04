//! Transport plugin registry
//!
//! Global registry for transport plugins that allows dynamic registration
//! and lookup of transport implementations.

use crate::transport::TransportPlugin;
use crate::{Error, Result};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};

/// Registry for transport plugins
///
/// This registry maintains a global mapping of transport names (e.g., "grpc", "webrtc")
/// to their plugin implementations. It supports thread-safe registration and lookup
/// using an RwLock-protected HashMap.
///
/// # Thread Safety
///
/// The registry uses RwLock to allow multiple concurrent readers or a single writer.
/// This is appropriate because lookups are frequent but registrations are rare.
///
/// # Example
///
/// ```
/// use remotemedia_core::transport::plugin_registry::TransportPluginRegistry;
///
/// let registry = TransportPluginRegistry::new();
/// assert!(registry.list().is_empty());
/// ```
pub struct TransportPluginRegistry {
    plugins: RwLock<HashMap<String, Arc<dyn TransportPlugin>>>,
}

impl TransportPluginRegistry {
    /// Create a new empty registry
    ///
    /// # Example
    ///
    /// ```
    /// use remotemedia_core::transport::plugin_registry::TransportPluginRegistry;
    ///
    /// let registry = TransportPluginRegistry::new();
    /// ```
    pub fn new() -> Self {
        Self {
            plugins: RwLock::new(HashMap::new()),
        }
    }

    /// Register a transport plugin
    ///
    /// Adds a new transport plugin to the registry. The plugin's name (from `plugin.name()`)
    /// is used as the lookup key. If a plugin with the same name already exists, this method
    /// returns an error.
    ///
    /// # Arguments
    ///
    /// * `plugin` - Arc-wrapped plugin implementing the TransportPlugin trait
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Plugin successfully registered
    /// * `Err(Error::ConfigError)` - Plugin with this name already exists
    ///
    /// # Errors
    ///
    /// Returns `Error::ConfigError` if:
    /// - A plugin with the same name is already registered
    /// - Failed to acquire write lock (lock poisoned)
    ///
    /// # Example
    ///
    /// See the integration tests in `tests/fixtures/mock_transport_plugin.rs` for
    /// a complete example of plugin registration.
    ///
    /// # Panics
    ///
    /// This method will panic if the internal RwLock is poisoned (another thread panicked
    /// while holding the lock). In practice, this should never happen as the registry
    /// operations are simple and don't panic.
    pub fn register(&self, plugin: Arc<dyn TransportPlugin>) -> Result<()> {
        let name = plugin.name();

        // Acquire write lock
        let mut plugins = self.plugins.write().map_err(|e| {
            Error::ConfigError(format!("Failed to acquire registry write lock: {}", e))
        })?;

        // Check for duplicate
        if plugins.contains_key(name) {
            return Err(Error::ConfigError(format!(
                "Plugin '{}' already registered",
                name
            )));
        }

        // Insert into registry
        plugins.insert(name.to_string(), plugin);

        Ok(())
    }

    /// List all registered plugin names
    ///
    /// Returns a vector of all plugin names currently registered in the registry.
    /// The order of names is not guaranteed.
    ///
    /// # Returns
    ///
    /// * `Vec<String>` - List of all registered plugin names, or empty vec if lock is poisoned
    ///
    /// # Example
    ///
    /// ```
    /// use remotemedia_core::transport::plugin_registry::TransportPluginRegistry;
    ///
    /// let registry = TransportPluginRegistry::new();
    /// // Initially empty
    /// assert!(registry.list().is_empty());
    /// // After registering plugins, list() returns their names
    /// ```
    ///
    /// # Lock Poisoning
    ///
    /// If the internal RwLock is poisoned (a thread panicked while holding the lock),
    /// this method returns an empty vector rather than panicking. This provides
    /// graceful degradation in error scenarios.
    pub fn list(&self) -> Vec<String> {
        self.plugins
            .read()
            .ok()
            .map(|guard| guard.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Look up a transport plugin by name
    ///
    /// Retrieves a plugin from the registry by its name. This method is thread-safe
    /// and allows concurrent reads.
    ///
    /// # Arguments
    ///
    /// * `name` - Name of the transport plugin to look up (e.g., "grpc", "webrtc")
    ///
    /// # Returns
    ///
    /// * `Some(Arc<dyn TransportPlugin>)` - Clone of the plugin Arc if found
    /// * `None` - If no plugin with this name exists or lock is poisoned
    ///
    /// # Example
    ///
    /// ```
    /// use remotemedia_core::transport::plugin_registry::TransportPluginRegistry;
    ///
    /// let registry = TransportPluginRegistry::new();
    ///
    /// // Lookup for non-existent plugin returns None
    /// let plugin = registry.get("nonexistent");
    /// assert!(plugin.is_none());
    /// ```
    ///
    /// # Thread Safety
    ///
    /// This method acquires a read lock, allowing multiple concurrent lookups.
    /// The returned Arc can be safely shared across threads.
    ///
    /// # Lock Poisoning
    ///
    /// If the internal RwLock is poisoned (a thread panicked while holding the lock),
    /// this method returns None rather than panicking. This provides graceful
    /// degradation in error scenarios.
    pub fn get(&self, name: &str) -> Option<Arc<dyn TransportPlugin>> {
        self.plugins.read().ok()?.get(name).cloned()
    }
}

impl Default for TransportPluginRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// Global registry singleton
static GLOBAL_REGISTRY: OnceLock<TransportPluginRegistry> = OnceLock::new();

/// Get the global transport plugin registry
///
/// Returns a reference to the global registry singleton. The registry is
/// lazily initialized on first access with an empty state.
///
/// # Thread Safety
///
/// This function is thread-safe and can be called concurrently from multiple threads.
/// The registry is initialized exactly once, even if called from multiple threads.
///
/// # Returns
///
/// A reference to the global `TransportPluginRegistry` instance.
///
/// # Example
///
/// ```
/// use remotemedia_core::transport::plugin_registry::global_registry;
///
/// let registry = global_registry();
/// let plugins = registry.list();
/// println!("Available plugins: {:?}", plugins);
/// ```
pub fn global_registry() -> &'static TransportPluginRegistry {
    GLOBAL_REGISTRY.get_or_init(|| TransportPluginRegistry::new())
}

/// Initialize the global registry with custom plugins
///
/// This function should be called once during application startup to register
/// plugins. Subsequent calls will be ignored.
///
/// # Arguments
///
/// * `init_fn` - Closure that receives a mutable reference to the registry for initialization
///
/// # Returns
///
/// * `Ok(())` - Registry initialized successfully
/// * `Err(Error)` - Registry already initialized
///
/// # Example
///
/// ```
/// use remotemedia_core::transport::plugin_registry::init_global_registry;
///
/// init_global_registry(|_registry| {
///     // Register plugins here
///     Ok(())
/// }).unwrap();
/// ```
pub fn init_global_registry<F>(init_fn: F) -> Result<()>
where
    F: FnOnce(&TransportPluginRegistry) -> Result<()>,
{
    let registry = global_registry();
    init_fn(registry)
}

/// Register default transport plugins
///
/// **IMPORTANT**: This function is intentionally minimal to avoid circular dependencies.
/// The `core` crate defines the plugin infrastructure but does NOT depend on
/// transport implementation crates (remotemedia-grpc, remotemedia-webrtc, etc.).
///
/// # Architecture
///
/// Plugin registration follows a layered architecture:
///
/// 1. **core**: Defines `TransportPlugin` trait and registry infrastructure
/// 2. **Transport crates** (remotemedia-grpc, remotemedia-webrtc): Implement plugins
/// 3. **Applications**: Import transport crates and register plugins at startup
///
/// This prevents circular dependencies:
/// - core does NOT depend on transport crates
/// - Transport crates depend on core
/// - Applications depend on both and wire them together
///
/// # Application-Level Registration
///
/// Applications should register transport plugins explicitly:
///
/// ```
/// use remotemedia_core::transport::plugin_registry::global_registry;
///
/// // Get the global registry singleton
/// let registry = global_registry();
///
/// // List currently registered plugins
/// let plugins = registry.list();
/// println!("Registered plugins: {:?}", plugins);
///
/// // At application startup, register plugins from transport crates:
/// // use remotemedia_grpc::GrpcTransportPlugin;
/// // registry.register(Arc::new(GrpcTransportPlugin)).unwrap();
/// ```
///
/// # Future HTTP Plugin
///
/// The HTTP plugin will be the only plugin registered here, as it uses
/// `reqwest` which is already a dependency of core for health checks
/// and simple HTTP operations.
///
/// # Returns
///
/// * `Ok(())` - All plugins registered successfully
/// * `Err(Error)` - Plugin registration failed (currently never fails)
pub fn register_default_plugins() -> Result<()> {
    let _registry = global_registry();

    // TODO: Register HTTP plugin (always enabled, uses reqwest dependency)
    // registry.register(Arc::new(HttpTransportPlugin))?;
    //
    // Note: gRPC and WebRTC plugins should be registered at the application level,
    // not here, to avoid circular dependencies. See documentation above.

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::client::PipelineClient;
    use crate::transport::executor::PipelineExecutor;
    use crate::transport::{ClientConfig, PipelineTransport, ServerConfig};
    use async_trait::async_trait;

    // Mock TransportPlugin for testing
    struct MockTransportPlugin {
        name: &'static str,
    }

    #[async_trait]
    impl TransportPlugin for MockTransportPlugin {
        fn name(&self) -> &'static str {
            self.name
        }

        async fn create_client(&self, _config: &ClientConfig) -> Result<Box<dyn PipelineClient>> {
            unimplemented!("Mock plugin - not used in registry tests")
        }

        async fn create_server(
            &self,
            _config: &ServerConfig,
            _executor: Arc<PipelineExecutor>,
        ) -> Result<Box<dyn PipelineTransport>> {
            unimplemented!("Mock plugin - not used in registry tests")
        }
    }

    #[test]
    fn test_new_registry_is_empty() {
        let registry = TransportPluginRegistry::new();
        let plugins = registry.plugins.read().unwrap();
        assert_eq!(plugins.len(), 0);
    }

    #[test]
    fn test_register_success() {
        let registry = TransportPluginRegistry::new();
        let plugin = Arc::new(MockTransportPlugin { name: "test" });

        let result = registry.register(plugin);
        assert!(result.is_ok());

        let plugins = registry.plugins.read().unwrap();
        assert_eq!(plugins.len(), 1);
        assert!(plugins.contains_key("test"));
    }

    #[test]
    fn test_register_duplicate_fails() {
        let registry = TransportPluginRegistry::new();
        let plugin1 = Arc::new(MockTransportPlugin { name: "grpc" });
        let plugin2 = Arc::new(MockTransportPlugin { name: "grpc" });

        // First registration should succeed
        let result1 = registry.register(plugin1);
        assert!(result1.is_ok());

        // Second registration with same name should fail
        let result2 = registry.register(plugin2);
        assert!(result2.is_err());

        match result2 {
            Err(Error::ConfigError(msg)) => {
                assert!(msg.contains("Plugin 'grpc' already registered"));
            }
            _ => panic!("Expected ConfigError"),
        }
    }

    #[test]
    fn test_register_multiple_different_plugins() {
        let registry = TransportPluginRegistry::new();
        let grpc_plugin = Arc::new(MockTransportPlugin { name: "grpc" });
        let webrtc_plugin = Arc::new(MockTransportPlugin { name: "webrtc" });
        let http_plugin = Arc::new(MockTransportPlugin { name: "http" });

        assert!(registry.register(grpc_plugin).is_ok());
        assert!(registry.register(webrtc_plugin).is_ok());
        assert!(registry.register(http_plugin).is_ok());

        let plugins = registry.plugins.read().unwrap();
        assert_eq!(plugins.len(), 3);
        assert!(plugins.contains_key("grpc"));
        assert!(plugins.contains_key("webrtc"));
        assert!(plugins.contains_key("http"));
    }

    #[test]
    fn test_default_creates_empty_registry() {
        let registry = TransportPluginRegistry::default();
        let plugins = registry.plugins.read().unwrap();
        assert_eq!(plugins.len(), 0);
    }

    #[test]
    fn test_list_empty_registry() {
        let registry = TransportPluginRegistry::new();
        let plugins = registry.list();
        assert_eq!(plugins.len(), 0);
    }

    #[test]
    fn test_list_single_plugin() {
        let registry = TransportPluginRegistry::new();
        let plugin = Arc::new(MockTransportPlugin { name: "grpc" });
        registry.register(plugin).unwrap();

        let plugins = registry.list();
        assert_eq!(plugins.len(), 1);
        assert!(plugins.contains(&"grpc".to_string()));
    }

    #[test]
    fn test_list_multiple_plugins() {
        let registry = TransportPluginRegistry::new();
        let grpc_plugin = Arc::new(MockTransportPlugin { name: "grpc" });
        let webrtc_plugin = Arc::new(MockTransportPlugin { name: "webrtc" });
        let http_plugin = Arc::new(MockTransportPlugin { name: "http" });

        registry.register(grpc_plugin).unwrap();
        registry.register(webrtc_plugin).unwrap();
        registry.register(http_plugin).unwrap();

        let plugins = registry.list();
        assert_eq!(plugins.len(), 3);
        assert!(plugins.contains(&"grpc".to_string()));
        assert!(plugins.contains(&"webrtc".to_string()));
        assert!(plugins.contains(&"http".to_string()));
    }

    #[test]
    fn test_list_returns_cloned_strings() {
        let registry = TransportPluginRegistry::new();
        let plugin = Arc::new(MockTransportPlugin { name: "grpc" });
        registry.register(plugin).unwrap();

        // Get list twice to ensure we get independent copies
        let plugins1 = registry.list();
        let plugins2 = registry.list();

        assert_eq!(plugins1, plugins2);
        assert_eq!(plugins1.len(), 1);
    }

    #[test]
    fn test_get_existing_plugin() {
        let registry = TransportPluginRegistry::new();
        let plugin = Arc::new(MockTransportPlugin { name: "grpc" });
        registry.register(plugin).unwrap();

        let retrieved = registry.get("grpc");
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().name(), "grpc");
    }

    #[test]
    fn test_get_nonexistent_plugin() {
        let registry = TransportPluginRegistry::new();
        let retrieved = registry.get("nonexistent");
        assert!(retrieved.is_none());
    }

    #[test]
    fn test_global_registry_singleton() {
        // Get the global registry multiple times
        let registry1 = global_registry();
        let registry2 = global_registry();

        // Should return the same instance (same memory address)
        assert!(std::ptr::eq(registry1, registry2));
    }

    #[test]
    fn test_init_global_registry() {
        // Initialize with a plugin
        let result = init_global_registry(|registry| {
            let plugin = Arc::new(MockTransportPlugin {
                name: "test_global",
            });
            registry.register(plugin)
        });

        assert!(result.is_ok());

        // Verify the plugin was registered
        let registry = global_registry();
        let retrieved = registry.get("test_global");
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().name(), "test_global");
    }
}
