//! Plugin registry for ingest sources
//!
//! This module provides the [`IngestRegistry`] for discovering and creating
//! ingest sources based on URI schemes.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};

use crate::Error;

use super::{IngestConfig, IngestPlugin, IngestSource};

/// Global singleton ingest registry
static GLOBAL_REGISTRY: OnceLock<IngestRegistry> = OnceLock::new();

/// Get the global ingest registry
///
/// The registry is lazily initialized with built-in plugins on first access.
/// The `FileIngestPlugin` is automatically registered for `file://`, bare paths, and stdin.
///
/// # Example
///
/// ```ignore
/// use remotemedia_core::ingestion::{global_ingest_registry, IngestConfig};
///
/// let registry = global_ingest_registry();
/// let schemes = registry.list_schemes();
/// println!("Available schemes: {:?}", schemes);
/// ```
pub fn global_ingest_registry() -> &'static IngestRegistry {
    GLOBAL_REGISTRY.get_or_init(|| {
        let registry = IngestRegistry::new();

        // Register built-in FileIngestPlugin
        if let Err(e) = registry.register(Arc::new(super::file::FileIngestPlugin)) {
            tracing::warn!("Failed to register FileIngestPlugin: {}", e);
        }

        registry
    })
}

/// Registry for ingest plugins
///
/// Manages plugin registration and provides URI-based source creation.
/// Plugins are matched by URI scheme (e.g., "rtmp://", "file://").
///
/// # Thread Safety
///
/// The registry uses `RwLock` internally for thread-safe access.
/// Registration is write-locked, lookups are read-locked.
#[derive(Default)]
pub struct IngestRegistry {
    /// Plugins indexed by name
    plugins: RwLock<HashMap<String, Arc<dyn IngestPlugin>>>,

    /// Scheme-to-plugin name mapping
    schemes: RwLock<HashMap<String, String>>,
}

impl std::fmt::Debug for IngestRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let plugins = self.list_plugins();
        let schemes = self.list_schemes();
        f.debug_struct("IngestRegistry")
            .field("plugins", &plugins)
            .field("schemes", &schemes)
            .finish()
    }
}

impl IngestRegistry {
    /// Create a new empty registry
    pub fn new() -> Self {
        Self {
            plugins: RwLock::new(HashMap::new()),
            schemes: RwLock::new(HashMap::new()),
        }
    }

    /// Register a plugin
    ///
    /// The plugin's schemes are indexed for fast URI matching.
    ///
    /// # Errors
    ///
    /// Returns error if a plugin with the same name is already registered.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use std::sync::Arc;
    /// use remotemedia_core::ingestion::IngestRegistry;
    ///
    /// let registry = IngestRegistry::new();
    /// registry.register(Arc::new(MyPlugin))?;
    /// ```
    pub fn register(&self, plugin: Arc<dyn IngestPlugin>) -> Result<(), Error> {
        let name = plugin.name().to_string();

        // Acquire write locks
        let mut plugins = self
            .plugins
            .write()
            .map_err(|e| Error::Other(format!("Failed to acquire plugins lock: {}", e)))?;

        let mut schemes = self
            .schemes
            .write()
            .map_err(|e| Error::Other(format!("Failed to acquire schemes lock: {}", e)))?;

        // Check for duplicate
        if plugins.contains_key(&name) {
            return Err(Error::ConfigError(format!(
                "Plugin '{}' is already registered",
                name
            )));
        }

        // Index schemes
        for scheme in plugin.schemes() {
            let scheme_lower = scheme.to_lowercase();
            if schemes.contains_key(&scheme_lower) {
                tracing::warn!(
                    "Scheme '{}' already registered by '{}', overwriting with '{}'",
                    scheme,
                    schemes[&scheme_lower],
                    name
                );
            }
            schemes.insert(scheme_lower, name.clone());
        }

        plugins.insert(name, plugin);
        Ok(())
    }

    /// Create an ingest source from a URI
    ///
    /// Extracts the scheme from the config URL and looks up the appropriate plugin.
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - No plugin handles the URI scheme
    /// - Plugin validation fails
    /// - Source creation fails
    ///
    /// # Example
    ///
    /// ```ignore
    /// let config = IngestConfig::from_url("rtmp://localhost:1935/live/stream");
    /// let source = registry.create_from_uri(&config)?;
    /// ```
    pub fn create_from_uri(&self, config: &IngestConfig) -> Result<Box<dyn IngestSource>, Error> {
        let scheme = extract_scheme(&config.url);

        // Get plugin name from scheme (in its own scope to release lock)
        let plugin_name = {
            let schemes = self
                .schemes
                .read()
                .map_err(|e| Error::Other(format!("Failed to acquire schemes lock: {}", e)))?;

            schemes.get(&scheme).cloned().ok_or_else(|| {
                let available: Vec<_> = schemes.keys().cloned().collect();
                Error::ConfigError(format!(
                    "No plugin registered for scheme '{}'. Available schemes: {:?}",
                    scheme, available
                ))
            })?
        };

        // Get plugin (in its own scope to release lock)
        let plugin = {
            let plugins = self
                .plugins
                .read()
                .map_err(|e| Error::Other(format!("Failed to acquire plugins lock: {}", e)))?;

            plugins.get(&plugin_name).cloned().ok_or_else(|| {
                Error::Other(format!(
                    "Plugin '{}' registered for scheme '{}' but not found",
                    plugin_name, scheme
                ))
            })?
        };

        // Validate and create
        plugin.validate(config)?;
        plugin.create(config)
    }

    /// Get a plugin by name
    pub fn get_plugin(&self, name: &str) -> Option<Arc<dyn IngestPlugin>> {
        self.plugins
            .read()
            .ok()
            .and_then(|plugins| plugins.get(name).cloned())
    }

    /// List all registered plugin names
    pub fn list_plugins(&self) -> Vec<String> {
        self.plugins
            .read()
            .map(|plugins| plugins.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// List all registered URI schemes
    pub fn list_schemes(&self) -> Vec<String> {
        self.schemes
            .read()
            .map(|schemes| schemes.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Get the plugin name for a given scheme
    pub fn plugin_for_scheme(&self, scheme: &str) -> Option<String> {
        self.schemes
            .read()
            .ok()
            .and_then(|schemes| schemes.get(&scheme.to_lowercase()).cloned())
    }
}

/// Extract the URI scheme from a URL
///
/// Handles various URL formats:
/// - `scheme://path` → `"scheme"`
/// - `scheme:path` → `"scheme"`
/// - `-` (stdin) → `"-"`
/// - Bare paths (`./file`, `/path`, `C:\path`) → `""`
///
/// # Examples
///
/// ```ignore
/// assert_eq!(extract_scheme("rtmp://localhost/stream"), "rtmp");
/// assert_eq!(extract_scheme("file:///path/to/file.wav"), "file");
/// assert_eq!(extract_scheme("./local.wav"), "");
/// assert_eq!(extract_scheme("/absolute/path.mp4"), "");
/// assert_eq!(extract_scheme("-"), "-");
/// ```
pub fn extract_scheme(url: &str) -> String {
    // Special case: stdin
    if url == "-" {
        return "-".to_string();
    }

    // Look for scheme separator
    if let Some(colon_pos) = url.find(':') {
        let potential_scheme = &url[..colon_pos];

        // Check if it looks like a scheme (alphanumeric, no path separators)
        // Also check it's not a Windows drive letter (e.g., "C:")
        if potential_scheme.len() > 1
            && potential_scheme
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '+' || c == '-' || c == '.')
        {
            return potential_scheme.to_lowercase();
        }
    }

    // No scheme found - bare path
    String::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    // Mock plugin for testing
    struct MockPlugin {
        name: &'static str,
        schemes: &'static [&'static str],
    }

    impl IngestPlugin for MockPlugin {
        fn name(&self) -> &'static str {
            self.name
        }

        fn schemes(&self) -> &'static [&'static str] {
            self.schemes
        }

        fn create(&self, _config: &IngestConfig) -> Result<Box<dyn IngestSource>, Error> {
            Err(Error::Other(
                "Mock plugin cannot create sources".to_string(),
            ))
        }
    }

    #[test]
    fn test_register_plugin_and_lookup_by_name() {
        let registry = IngestRegistry::new();
        let plugin = Arc::new(MockPlugin {
            name: "test",
            schemes: &["test"],
        });

        registry.register(plugin).unwrap();
        assert!(registry.get_plugin("test").is_some());
        assert!(registry.get_plugin("nonexistent").is_none());
    }

    #[test]
    fn test_register_plugin_and_lookup_by_scheme() {
        let registry = IngestRegistry::new();
        let plugin = Arc::new(MockPlugin {
            name: "myproto",
            schemes: &["proto1", "proto2"],
        });

        registry.register(plugin).unwrap();

        assert_eq!(
            registry.plugin_for_scheme("proto1"),
            Some("myproto".to_string())
        );
        assert_eq!(
            registry.plugin_for_scheme("PROTO2"),
            Some("myproto".to_string())
        ); // case-insensitive
        assert_eq!(registry.plugin_for_scheme("proto3"), None);
    }

    #[test]
    fn test_duplicate_registration_returns_error() {
        let registry = IngestRegistry::new();
        let plugin1 = Arc::new(MockPlugin {
            name: "duplicate",
            schemes: &["dup"],
        });
        let plugin2 = Arc::new(MockPlugin {
            name: "duplicate",
            schemes: &["other"],
        });

        registry.register(plugin1).unwrap();
        let result = registry.register(plugin2);

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("already registered"));
    }

    #[test]
    fn test_unknown_scheme_returns_error_with_available_schemes() {
        let registry = IngestRegistry::new();
        let plugin = Arc::new(MockPlugin {
            name: "test",
            schemes: &["known"],
        });
        registry.register(plugin).unwrap();

        let config = IngestConfig::from_url("unknown://test");
        let result = registry.create_from_uri(&config);

        match result {
            Err(e) => {
                let err_msg = e.to_string();
                assert!(
                    err_msg.contains("unknown"),
                    "Error should contain 'unknown': {}",
                    err_msg
                );
                assert!(
                    err_msg.contains("known"),
                    "Error should list available schemes: {}",
                    err_msg
                );
            }
            Ok(_) => panic!("Expected error for unknown scheme"),
        }
    }

    #[test]
    fn test_extract_scheme_handles_various_formats() {
        // Standard URL schemes
        assert_eq!(extract_scheme("rtmp://localhost:1935/live"), "rtmp");
        assert_eq!(extract_scheme("rtmps://server/stream"), "rtmps");
        assert_eq!(extract_scheme("file:///path/to/file.wav"), "file");
        assert_eq!(extract_scheme("srt://192.168.1.1:4000"), "srt");
        assert_eq!(extract_scheme("HTTP://example.com"), "http");

        // Stdin
        assert_eq!(extract_scheme("-"), "-");

        // Bare paths (no scheme)
        assert_eq!(extract_scheme("./local.wav"), "");
        assert_eq!(extract_scheme("/absolute/path.mp4"), "");
        assert_eq!(extract_scheme("relative/path.wav"), "");

        // Windows paths should not be confused with schemes
        assert_eq!(extract_scheme("C:\\Windows\\file.mp4"), "");

        // Complex schemes
        assert_eq!(extract_scheme("rtsp+http://server"), "rtsp+http");
    }

    #[test]
    fn test_list_plugins_returns_all_registered() {
        let registry = IngestRegistry::new();
        registry
            .register(Arc::new(MockPlugin {
                name: "plugin_a",
                schemes: &["a"],
            }))
            .unwrap();
        registry
            .register(Arc::new(MockPlugin {
                name: "plugin_b",
                schemes: &["b"],
            }))
            .unwrap();

        let plugins = registry.list_plugins();
        assert_eq!(plugins.len(), 2);
        assert!(plugins.contains(&"plugin_a".to_string()));
        assert!(plugins.contains(&"plugin_b".to_string()));
    }

    #[test]
    fn test_list_schemes_returns_all_registered() {
        let registry = IngestRegistry::new();
        registry
            .register(Arc::new(MockPlugin {
                name: "multi",
                schemes: &["scheme1", "scheme2", "scheme3"],
            }))
            .unwrap();

        let schemes = registry.list_schemes();
        assert_eq!(schemes.len(), 3);
        assert!(schemes.contains(&"scheme1".to_string()));
        assert!(schemes.contains(&"scheme2".to_string()));
        assert!(schemes.contains(&"scheme3".to_string()));
    }
}
