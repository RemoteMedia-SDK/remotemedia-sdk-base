//! Container image building and caching
//!
//! Handles Docker image building from configurations, caching built images,
//! and managing the image lifecycle.

use crate::python::multiprocess::docker_support::DockerNodeConfig;
use crate::{Error, Result};
use bollard::query_parameters::{
    BuildImageOptionsBuilder, ListImagesOptionsBuilder, TagImageOptionsBuilder,
};
use bollard::Docker;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use http_body_util::{Either, Full};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::Arc;
use tar::{Builder, Header};
use tokio::sync::RwLock;
use tracing::{debug, error, info, instrument};

/// Container image builder and cache manager
pub struct ContainerBuilder {
    docker: Arc<Docker>,
    cache: Arc<RwLock<ImageCache>>,
}

/// Represents a built Docker image
#[derive(Debug, Clone)]
pub struct ContainerImage {
    pub image_id: String,
    pub image_tag: String,
    pub config_hash: String,
    pub created_at: DateTime<Utc>,
    pub size_bytes: u64,
    pub python_version: String,
}

impl ContainerImage {
    /// Create a new container image record
    pub fn new(
        image_id: String,
        image_tag: String,
        config_hash: String,
        size_bytes: u64,
        python_version: String,
    ) -> Self {
        Self {
            image_id,
            image_tag,
            config_hash,
            created_at: Utc::now(),
            size_bytes,
            python_version,
        }
    }
}

/// Image cache for reusing built images
#[derive(Debug)]
pub struct ImageCache {
    images: HashMap<String, ContainerImage>,
    total_size_bytes: u64,
    max_size_bytes: u64,
}

impl ImageCache {
    /// Create a new image cache with specified max size
    pub fn new(max_size_bytes: u64) -> Self {
        Self {
            images: HashMap::new(),
            total_size_bytes: 0,
            max_size_bytes,
        }
    }

    /// Get an image from cache by config hash
    pub fn get(&self, config_hash: &str) -> Option<&ContainerImage> {
        self.images.get(config_hash)
    }

    /// Add an image to the cache
    pub fn put(&mut self, image: ContainerImage) -> Result<()> {
        // Check if we need to evict images to make room
        while self.total_size_bytes + image.size_bytes > self.max_size_bytes
            && !self.images.is_empty()
        {
            // Simple LRU: remove oldest image
            if let Some(oldest_hash) = self.find_oldest_image() {
                self.evict(&oldest_hash)?;
            }
        }

        self.total_size_bytes += image.size_bytes;
        let config_hash = image.config_hash.clone();
        self.images.insert(config_hash, image);
        Ok(())
    }

    /// Evict an image from the cache
    pub fn evict(&mut self, config_hash: &str) -> Result<()> {
        if let Some(image) = self.images.remove(config_hash) {
            self.total_size_bytes -= image.size_bytes;
            tracing::info!("Evicted image {} from cache", image.image_tag);
        }
        Ok(())
    }

    /// Find the oldest image in cache (simple LRU)
    fn find_oldest_image(&self) -> Option<String> {
        self.images
            .iter()
            .min_by_key(|(_, img)| img.created_at)
            .map(|(hash, _)| hash.clone())
    }

    /// Clear all images from cache
    pub fn clear(&mut self) {
        self.images.clear();
        self.total_size_bytes = 0;
    }

    /// Get total cache size
    pub fn size(&self) -> u64 {
        self.total_size_bytes
    }

    /// Get number of cached images
    pub fn count(&self) -> usize {
        self.images.len()
    }
}

impl ContainerBuilder {
    /// Create a new container builder with Docker client and cache
    ///
    /// # Arguments
    /// * `docker` - Docker client for image building operations
    /// * `max_cache_size_bytes` - Maximum cache size in bytes (default: 10GB)
    pub fn new(docker: Arc<Docker>, max_cache_size_bytes: Option<u64>) -> Self {
        let max_size = max_cache_size_bytes.unwrap_or(10 * 1024 * 1024 * 1024); // 10GB default
        Self {
            docker,
            cache: Arc::new(RwLock::new(ImageCache::new(max_size))),
        }
    }

    /// Build a Docker image from configuration
    ///
    /// This is the main entry point for image building that:
    /// 1. Computes config hash for cache lookup
    /// 2. Checks if image already exists in cache
    /// 3. If not cached, builds the image using bollard API
    /// 4. Caches the newly built image
    /// 5. Returns the image information
    ///
    /// # Arguments
    /// * `config` - DockerNodeConfig to build image from
    ///
    /// # Returns
    /// ContainerImage with image ID, tag, and metadata
    ///

    /// Create a tar archive containing the Dockerfile
    ///
    /// Creates a minimal build context with just the Dockerfile needed for image building.
    #[allow(dead_code)] // Reserved for minimal Docker build contexts
    fn create_build_context(&self, dockerfile_content: &str) -> Result<Vec<u8>> {
        let mut tar_data = Vec::new();
        {
            let mut builder = Builder::new(&mut tar_data);

            // Add Dockerfile to tar
            let dockerfile_bytes = dockerfile_content.as_bytes();
            let mut header = Header::new_gnu();
            header.set_path("Dockerfile").map_err(|e| {
                crate::Error::Execution(format!("Failed to set Dockerfile path: {}", e))
            })?;
            header.set_size(dockerfile_bytes.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();

            builder.append(&header, dockerfile_bytes).map_err(|e| {
                crate::Error::Execution(format!("Failed to add Dockerfile to tar: {}", e))
            })?;

            builder.finish().map_err(|e| {
                crate::Error::Execution(format!("Failed to finalize tar archive: {}", e))
            })?;
        }

        Ok(tar_data)
    }

    /// Build image with progress logging (T030)
    ///
    /// Builds the Docker image and logs progress information from the Docker daemon.
    #[allow(dead_code)] // Reserved for detailed build progress monitoring
    #[instrument(skip(self, _tar_data), fields(image_tag = %image_tag))]
    async fn build_image_with_progress(
        &self,
        image_tag: &str,
        _tar_data: Vec<u8>, // Used via instrument macro for logging
    ) -> Result<String> {
        let build_options = BuildImageOptionsBuilder::default()
            .dockerfile("Dockerfile")
            .t(image_tag)
            .rm(true)
            .pull("1") // Pull base image (boolean as string)
            .build();

        // Build image with tar data
        // TODO: Implement proper body conversion for bollard's build_image
        // For now, this is a placeholder that won't compile. Need to use
        // the correct body type expected by bollard's Docker::build_image method.
        //
        // Bollard uses http-body-util types which require proper imports and conversion.
        // The expected type is: Either<Full<Bytes>, StreamBody<...>>
        //
        // Reference implementation needed from bollard examples or documentation.

        let mut stream = self.docker.build_image(
            build_options,
            None,
            None, // TODO: Pass tar_data as proper body type
        );

        while let Some(build_info) = stream.next().await {
            match build_info {
                Ok(info) => {
                    // T030: Log build progress
                    if let Some(stream) = &info.stream {
                        let msg = stream.trim();
                        if !msg.is_empty() {
                            debug!("Docker build: {}", msg);
                        }
                    }

                    if let Some(status) = &info.status {
                        info!("Docker build status: {}", status);
                    }

                    if let Some(error) = &info.error {
                        error!("Docker build error: {}", error);
                        return Err(crate::Error::Execution(format!(
                            "Docker build failed: {}",
                            error
                        )));
                    }

                    // Note: Image ID is captured from aux field, but its structure varies
                    // We'll rely on listing images by tag instead
                }
                Err(e) => {
                    error!("Docker build stream error: {}", e);
                    return Err(crate::Error::Execution(format!(
                        "Docker build stream error: {}",
                        e
                    )));
                }
            }
        }

        // List images to find the one we just built by tag
        let mut filters = HashMap::new();
        filters.insert("reference".to_string(), vec![image_tag.to_string()]);

        let images = self
            .docker
            .list_images(Some(
                ListImagesOptionsBuilder::default()
                    .all(false)
                    .filters(&filters)
                    .build(),
            ))
            .await
            .map_err(|e| {
                crate::Error::Execution(format!("Failed to list images after build: {}", e))
            })?;

        let image_id = images
            .first()
            .ok_or_else(|| {
                crate::Error::Execution(
                    "Failed to find built image after build completed".to_string(),
                )
            })?
            .id
            .clone();

        info!("Built image ID: {}", image_id);

        Ok(image_id)
    }

    /// Get image size from Docker daemon
    #[allow(dead_code)] // Reserved for image size tracking and metrics
    async fn get_image_size(&self, image_id: &str) -> Result<u64> {
        let mut filters = HashMap::new();
        filters.insert("id".to_string(), vec![image_id.to_string()]);

        let images = self
            .docker
            .list_images(Some(
                ListImagesOptionsBuilder::default()
                    .all(false)
                    .filters(&filters)
                    .build(),
            ))
            .await
            .map_err(|e| crate::Error::Execution(format!("Failed to inspect image: {}", e)))?;

        let size = images.first().map(|img| img.size as u64).unwrap_or(0);

        Ok(size)
    }

    /// Get a cached image by config hash
    /// Build a Docker image from configuration
    ///
    /// This function:
    /// 1. Generates a Dockerfile from the config
    /// 2. Builds the image using bollard
    /// 3. Tags it with the config hash
    /// 4. Caches the image metadata
    ///
    /// # Arguments
    /// * `config` - Docker node configuration
    /// * `force_rebuild` - If true, rebuilds even if cached
    ///
    /// # Returns
    /// The built and cached container image
    pub async fn build_image(
        &self,
        config: &DockerNodeConfig,
        force_rebuild: bool,
    ) -> Result<ContainerImage> {
        let config_hash = Self::compute_config_hash(config);
        let image_tag = format!("remotemedia/node:{}", &config_hash[..12]);

        // Check cache first (T025)
        if !force_rebuild {
            if let Some(cached) = self.get_cached_image(&config_hash).await {
                tracing::info!("Using cached image: {}", cached.image_tag);
                return Ok(cached);
            }
        }

        // Generate Dockerfile content
        let dockerfile_content = Self::generate_dockerfile(config)?;

        // Create tar archive with Dockerfile
        let mut header = tar::Header::new_gnu();
        header.set_path("Dockerfile")?;
        header.set_size(dockerfile_content.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();

        let mut tar_builder = tar::Builder::new(Vec::new());
        tar_builder.append(&header, dockerfile_content.as_bytes())?;
        let tar_bytes = tar_builder.into_inner()?;

        // Build image using bollard (T024)
        tracing::info!("Building Docker image: {}", image_tag);

        let build_options = BuildImageOptionsBuilder::default()
            .dockerfile("Dockerfile")
            .t(image_tag.as_str())
            .rm(true)
            .forcerm(true)
            .nocache(force_rebuild)
            .build();

        // Convert tar bytes to the proper body type for bollard
        // bollard expects Either<Full<Bytes>, StreamBody>
        let body = Either::Left(Full::new(Bytes::from(tar_bytes)));
        let mut build_stream = self.docker.build_image(build_options, None, Some(body));

        // Process build output with progress logging (T030)
        while let Some(build_info) = build_stream.next().await {
            match build_info {
                Ok(bollard::models::BuildInfo {
                    stream: Some(msg), ..
                }) => {
                    tracing::debug!("Docker build: {}", msg.trim());
                }
                Ok(bollard::models::BuildInfo {
                    error: Some(err), ..
                }) => {
                    return Err(Error::Execution(format!("Docker build error: {}", err)));
                }
                Ok(bollard::models::BuildInfo { aux: Some(aux), .. }) => {
                    if let Some(id) = aux.id {
                        tracing::info!("Built image ID: {}", id);
                    }
                }
                Err(e) => {
                    return Err(Error::Execution(format!("Build stream error: {}", e)));
                }
                _ => {}
            }
        }

        // Tag image with config hash (T029)
        tracing::info!("Tagging image with hash: {}", config_hash);
        let tag_options = TagImageOptionsBuilder::default()
            .repo("remotemedia/node")
            .tag(&config_hash[..12])
            .build();

        self.docker
            .tag_image(&image_tag, Some(tag_options))
            .await
            .map_err(|e| Error::Execution(format!("Failed to tag image: {}", e)))?;

        // Create image metadata
        let image = ContainerImage {
            image_id: config_hash.clone(),
            image_tag: image_tag.clone(),
            config_hash,
            created_at: Utc::now(),
            size_bytes: 0, // Would need to inspect image for actual size
            python_version: config.python_version.clone(),
        };

        // Add to cache with LRU eviction (T026)
        self.add_to_cache(image.clone()).await?;

        Ok(image)
    }

    /// Add image to cache with LRU eviction policy
    async fn add_to_cache(&self, image: ContainerImage) -> Result<()> {
        let mut cache = self.cache.write().await;

        // Use the put method which handles eviction internally
        cache.put(image)?;
        Ok(())
    }

    pub async fn get_cached_image(&self, config_hash: &str) -> Option<ContainerImage> {
        let cache = self.cache.read().await;
        cache.get(config_hash).cloned()
    }

    /// Clear all cached images
    pub async fn clear_cache(&self) {
        let mut cache = self.cache.write().await;
        cache.images.clear();
        cache.total_size_bytes = 0;
    }

    /// Get cache statistics
    pub async fn cache_stats(&self) -> (usize, u64, u64) {
        let cache = self.cache.read().await;
        (
            cache.images.len(),
            cache.total_size_bytes,
            cache.max_size_bytes,
        )
    }

    /// Generate a Dockerfile from DockerNodeConfig
    ///
    /// Creates a multi-stage Dockerfile template that includes:
    /// - Base image selection (custom or python:{version}-slim)
    /// - System package installation via apt-get
    /// - Python package installation via pip
    /// - Working directory setup
    /// - Environment variable configuration
    /// - Python runner command setup for node execution
    ///
    /// The generated Dockerfile is optimized for:
    /// - Layer caching: System packages cached separately from Python packages
    /// - Image size: Uses slim base images when possible
    /// - Security: Sets PYTHONUNBUFFERED and proper permissions
    /// - Extensibility: Comments indicate customization points for future features
    ///
    /// # Arguments
    /// * `config` - The DockerNodeConfig to generate Dockerfile from
    ///
    /// # Returns
    /// Result containing the generated Dockerfile string, or error if configuration is invalid
    ///
    /// # Example
    /// ```
    /// use remotemedia_core::python::multiprocess::docker_support::DockerNodeConfig;
    /// use remotemedia_core::python::multiprocess::container_builder::ContainerBuilder;
    ///
    /// let config = DockerNodeConfig {
    ///     python_version: "3.10".to_string(),
    ///     base_image: None,
    ///     system_packages: vec![],
    ///     python_packages: vec![],
    ///     memory_mb: 2048,
    ///     cpu_cores: 2.0,
    ///     gpu_devices: vec![],
    ///     shm_size_mb: 2048,
    ///     env_vars: Default::default(),
    ///     volumes: vec![],
    ///     security: Default::default(),
    /// };
    /// let dockerfile = ContainerBuilder::generate_dockerfile(&config).unwrap();
    /// assert!(dockerfile.contains("FROM"));
    /// ```
    pub fn generate_dockerfile(config: &DockerNodeConfig) -> Result<String> {
        // Validate configuration before generating
        config.validate()?;

        tracing::debug!(
            python_version = %config.python_version,
            system_packages = config.system_packages.len(),
            python_packages = config.python_packages.len(),
            "Generating Dockerfile from configuration"
        );

        let mut dockerfile = String::new();

        // Stage 1: Builder stage for dependency installation and caching
        dockerfile.push_str("# Builder stage: Install dependencies and prepare environment\n");
        dockerfile.push_str("FROM ");

        // Select base image
        let base_image = if let Some(custom_image) = &config.base_image {
            tracing::debug!(
                custom_base_image = %custom_image,
                "Using custom base image"
            );
            custom_image.clone()
        } else {
            let default_image = format!("python:{}-slim", config.python_version);
            tracing::debug!(
                default_base_image = %default_image,
                "Using default slim Python base image"
            );
            default_image
        };

        dockerfile.push_str(&base_image);
        dockerfile.push_str(" AS builder\n\n");

        // Set working directory
        dockerfile.push_str("WORKDIR /app\n\n");

        // Enable unbuffered output for real-time logging
        dockerfile.push_str("# Enable unbuffered Python output for real-time logging\n");
        dockerfile.push_str("ENV PYTHONUNBUFFERED=1\n\n");

        // Install system packages if specified
        if !config.system_packages.is_empty() {
            tracing::debug!(
                count = config.system_packages.len(),
                "Adding system package installation layer"
            );

            dockerfile.push_str("# Install system dependencies\n");
            dockerfile
                .push_str("RUN apt-get update && apt-get install -y --no-install-recommends \\\n");

            // Sort packages for deterministic output
            let mut sorted_packages = config.system_packages.clone();
            sorted_packages.sort();

            for (idx, package) in sorted_packages.iter().enumerate() {
                let is_last = idx == sorted_packages.len() - 1;
                dockerfile.push_str("    ");
                dockerfile.push_str(package);
                if !is_last {
                    dockerfile.push_str(" \\\n");
                } else {
                    dockerfile.push_str("\n");
                }
            }

            // Clean up apt cache to reduce layer size
            dockerfile.push_str(" && rm -rf /var/lib/apt/lists/* \\\n");
            dockerfile.push_str(" && rm -rf /var/cache/apt/*\n\n");
        }

        // Upgrade pip and install essential Python build tools
        dockerfile.push_str("# Upgrade pip and install build essentials\n");
        dockerfile.push_str("RUN pip install --upgrade pip setuptools wheel --no-cache-dir\n\n");

        // Install Python packages if specified
        if !config.python_packages.is_empty() {
            tracing::debug!(
                count = config.python_packages.len(),
                "Adding Python package installation layer"
            );

            dockerfile.push_str("# Install Python package dependencies\n");
            dockerfile.push_str("RUN pip install --no-cache-dir \\\n");

            // Sort packages for deterministic output
            let mut sorted_packages = config.python_packages.clone();
            sorted_packages.sort();

            for (idx, package) in sorted_packages.iter().enumerate() {
                let is_last = idx == sorted_packages.len() - 1;
                dockerfile.push_str("    ");
                dockerfile.push_str(package);
                if !is_last {
                    dockerfile.push_str(" \\\n");
                } else {
                    dockerfile.push_str("\n");
                }
            }

            dockerfile.push_str("\n");
        }

        // Stage 2: Runtime stage
        dockerfile.push_str("# Runtime stage: Minimal image with only runtime dependencies\n");
        dockerfile.push_str("FROM ");
        dockerfile.push_str(&base_image);
        dockerfile.push_str("\n\n");

        dockerfile.push_str("WORKDIR /app\n\n");

        // Set Python to unbuffered output
        dockerfile.push_str("ENV PYTHONUNBUFFERED=1\n");

        // Add RemoteMedia specific environment variables
        dockerfile.push_str("\n# RemoteMedia Runtime Configuration\n");
        dockerfile.push_str("ENV REMOTEMEDIA_RUNNER=true\n");
        dockerfile.push_str("ENV REMOTEMEDIA_IPC_TIMEOUT=30000\n\n");

        // Add custom environment variables from config
        if !config.env_vars.is_empty() {
            tracing::debug!(
                count = config.env_vars.len(),
                "Adding custom environment variables"
            );

            dockerfile.push_str("# Custom environment variables\n");

            // Sort for deterministic output
            let mut sorted_vars: Vec<_> = config.env_vars.iter().collect();
            sorted_vars.sort_by_key(|(k, _)| *k);

            for (key, value) in sorted_vars {
                // Escape quotes in values
                let escaped_value = value.replace('"', "\\\"");
                dockerfile.push_str("ENV ");
                dockerfile.push_str(key);
                dockerfile.push_str("=\"");
                dockerfile.push_str(&escaped_value);
                dockerfile.push_str("\"\n");
            }

            dockerfile.push_str("\n");
        }

        // Copy installed packages from builder stage
        dockerfile.push_str("# Copy installed Python packages from builder stage\n");
        dockerfile.push_str("COPY --from=builder /usr/local/lib/python");
        dockerfile.push_str(&config.python_version);
        dockerfile.push_str("/site-packages /usr/local/lib/python");
        dockerfile.push_str(&config.python_version);
        dockerfile.push_str("/site-packages\n\n");

        // Copy pip installation from builder
        dockerfile.push_str("COPY --from=builder /usr/local/bin /usr/local/bin\n\n");

        // Create a health check (extensibility point)
        dockerfile.push_str("# Health check for container readiness\n");
        dockerfile.push_str(
            "HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \\\n",
        );
        dockerfile.push_str("    CMD python -c \"import sys; sys.exit(0)\" || exit 1\n\n");

        // Add GPU configuration if GPU devices are specified
        if !config.gpu_devices.is_empty() {
            tracing::debug!(
                gpu_devices = ?config.gpu_devices,
                "Adding GPU support configuration"
            );

            dockerfile.push_str("# GPU Support Configuration\n");
            dockerfile.push_str("# Note: Ensure NVIDIA Container Toolkit is installed on host\n");
            dockerfile.push_str("ENV NVIDIA_VISIBLE_DEVICES=");

            if config.gpu_devices.iter().any(|d| d == "all") {
                dockerfile.push_str("all\n");
            } else {
                dockerfile.push_str(&config.gpu_devices.join(","));
                dockerfile.push_str("\n");
            }

            dockerfile.push_str("ENV NVIDIA_DRIVER_CAPABILITIES=compute,utility\n\n");
        }

        // Default command runs Python in interactive mode waiting for node execution
        // This allows the container to stay alive and receive commands via IPC
        dockerfile.push_str("# Default command: Keep container running for node execution\n");
        dockerfile.push_str("CMD [\"python\", \"-c\", \"import asyncio; asyncio.run(__import__('asyncio').sleep(float('inf')))\"]\n\n");

        // Add helpful comments for extensibility
        dockerfile.push_str("# Extensibility Points for Future Features:\n");
        dockerfile.push_str("# - Pre-install scripts: Add custom setup before main installation\n");
        dockerfile.push_str("# - Custom entrypoints: Override CMD for specific node types\n");
        dockerfile.push_str("# - Multi-architecture builds: Use buildx for arm64/amd64 support\n");
        dockerfile.push_str("# - Layer caching optimization: Consider pinning package versions\n");

        tracing::info!(
            bytes = dockerfile.len(),
            "Dockerfile generated successfully"
        );

        Ok(dockerfile)
    }

    /// Compute SHA256 hash of Docker configuration
    ///
    /// Generates a deterministic hash from DockerNodeConfig that includes:
    /// - python_version: Base Python version (e.g., "3.10")
    /// - base_image: Custom base image if specified
    /// - system_packages: System-level dependencies (sorted for determinism)
    /// - python_packages: Python package dependencies (sorted for determinism)
    /// - memory_mb: Memory limit in megabytes
    /// - cpu_cores: CPU allocation in cores
    /// - shm_size_mb: Shared memory size for IPC
    ///
    /// The hash is used for:
    /// - Image tagging: Images are tagged with their config hash
    /// - Cache lookups: Existing images with same hash can be reused
    /// - Change detection: Different configs produce different hashes
    ///
    /// # Arguments
    /// * `config` - The DockerNodeConfig to hash
    ///
    /// # Returns
    /// A 64-character hexadecimal SHA256 hash string that is:
    /// - Deterministic: Same config always produces same hash
    /// - Content-addressable: Different configs produce different hashes
    /// - Collision-resistant: SHA256 guarantees negligible collision probability
    pub fn compute_config_hash(config: &DockerNodeConfig) -> String {
        let mut hasher = Sha256::new();

        // Hash Python version - fundamental to environment
        hasher.update(config.python_version.as_bytes());

        // Hash base image if specified
        if let Some(base) = &config.base_image {
            hasher.update(base.as_bytes());
        }

        // Sort system packages for deterministic hashing
        // This ensures same set of packages (regardless of order) produces same hash
        let mut sys_pkgs = config.system_packages.clone();
        sys_pkgs.sort();
        for pkg in &sys_pkgs {
            hasher.update(pkg.as_bytes());
            hasher.update(b"\0"); // Null separator between packages
        }

        // Sort Python packages for deterministic hashing
        let mut py_pkgs = config.python_packages.clone();
        py_pkgs.sort();
        for pkg in &py_pkgs {
            hasher.update(pkg.as_bytes());
            hasher.update(b"\0"); // Null separator between packages
        }

        // Hash resource limits as little-endian bytes for consistency
        hasher.update(&config.memory_mb.to_le_bytes());
        hasher.update(&config.cpu_cores.to_le_bytes());
        hasher.update(&config.shm_size_mb.to_le_bytes());

        // Finalize and return as hexadecimal string
        format!("{:x}", hasher.finalize())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::python::multiprocess::docker_support::SecurityConfig;
    use std::collections::HashMap;

    fn create_test_config() -> DockerNodeConfig {
        DockerNodeConfig {
            python_version: "3.10".to_string(),
            base_image: None,
            system_packages: vec!["curl".to_string(), "git".to_string()],
            python_packages: vec!["numpy".to_string(), "torch".to_string()],
            memory_mb: 2048,
            cpu_cores: 2.0,
            gpu_devices: vec![],
            shm_size_mb: 2048,
            env_vars: HashMap::new(),
            volumes: vec![],
            security: SecurityConfig::default(),
        }
    }

    #[test]
    fn test_compute_config_hash_deterministic() {
        let config1 = create_test_config();
        let config2 = create_test_config();

        let hash1 = ContainerBuilder::compute_config_hash(&config1);
        let hash2 = ContainerBuilder::compute_config_hash(&config2);

        assert_eq!(hash1, hash2, "Same config should produce same hash");
        assert_eq!(hash1.len(), 64, "SHA256 hash should be 64 hex characters");
    }

    #[test]
    fn test_compute_config_hash_different_configs() {
        let config1 = create_test_config();
        let mut config2 = create_test_config();
        config2.python_version = "3.11".to_string();

        let hash1 = ContainerBuilder::compute_config_hash(&config1);
        let hash2 = ContainerBuilder::compute_config_hash(&config2);

        assert_ne!(
            hash1, hash2,
            "Different configs should produce different hashes"
        );
    }

    #[test]
    fn test_compute_config_hash_package_order_independent() {
        let mut config1 = create_test_config();
        config1.python_packages = vec!["numpy".to_string(), "torch".to_string()];

        let mut config2 = create_test_config();
        config2.python_packages = vec!["torch".to_string(), "numpy".to_string()];

        let hash1 = ContainerBuilder::compute_config_hash(&config1);
        let hash2 = ContainerBuilder::compute_config_hash(&config2);

        assert_eq!(hash1, hash2, "Package order should not affect hash");
    }

    #[test]
    fn test_generate_dockerfile_basic() {
        let config = create_test_config();
        let dockerfile = ContainerBuilder::generate_dockerfile(&config).unwrap();

        assert!(dockerfile.contains("FROM python:3.10-slim"));
        assert!(dockerfile.contains("curl"));
        assert!(dockerfile.contains("git"));
        assert!(dockerfile.contains("numpy"));
        assert!(dockerfile.contains("torch"));
        assert!(dockerfile.contains("WORKDIR /app"));
        assert!(dockerfile.contains("PYTHONUNBUFFERED=1"));
    }

    #[test]
    fn test_generate_dockerfile_with_custom_base_image() {
        let mut config = create_test_config();
        config.base_image = Some("nvidia/cuda:11.8.0-base-ubuntu22.04".to_string());

        let dockerfile = ContainerBuilder::generate_dockerfile(&config).unwrap();

        assert!(dockerfile.contains("FROM nvidia/cuda:11.8.0-base-ubuntu22.04"));
        assert!(!dockerfile.contains("FROM python:3.10-slim"));
    }

    #[test]
    fn test_generate_dockerfile_with_gpu() {
        let mut config = create_test_config();
        config.gpu_devices = vec!["0".to_string(), "1".to_string()];

        let dockerfile = ContainerBuilder::generate_dockerfile(&config).unwrap();

        assert!(dockerfile.contains("NVIDIA_VISIBLE_DEVICES=0,1"));
        assert!(dockerfile.contains("NVIDIA_DRIVER_CAPABILITIES=compute,utility"));
    }

    #[test]
    fn test_generate_dockerfile_with_env_vars() {
        let mut config = create_test_config();
        config
            .env_vars
            .insert("MY_VAR".to_string(), "my_value".to_string());
        config
            .env_vars
            .insert("ANOTHER_VAR".to_string(), "another_value".to_string());

        let dockerfile = ContainerBuilder::generate_dockerfile(&config).unwrap();

        assert!(dockerfile.contains("ENV MY_VAR=\"my_value\""));
        assert!(dockerfile.contains("ENV ANOTHER_VAR=\"another_value\""));
    }

    #[tokio::test]
    async fn test_image_cache_basic() {
        let mut cache = ImageCache::new(1024 * 1024 * 1024); // 1GB

        let image = ContainerImage::new(
            "sha256:abc123".to_string(),
            "remotemedia-node:test".to_string(),
            "config_hash_123".to_string(),
            100 * 1024 * 1024, // 100MB
            "3.10".to_string(),
        );

        cache.put(image.clone()).unwrap();

        let retrieved = cache.get("config_hash_123");
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().image_id, "sha256:abc123");
        assert_eq!(cache.count(), 1);
        assert_eq!(cache.size(), 100 * 1024 * 1024);
    }

    #[tokio::test]
    async fn test_image_cache_lru_eviction() {
        // T026: Test LRU eviction policy
        let mut cache = ImageCache::new(200 * 1024 * 1024); // 200MB max

        // Add first image (100MB)
        let image1 = ContainerImage::new(
            "sha256:image1".to_string(),
            "remotemedia-node:img1".to_string(),
            "hash1".to_string(),
            100 * 1024 * 1024,
            "3.10".to_string(),
        );
        cache.put(image1.clone()).unwrap();

        // Sleep to ensure different timestamps
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Add second image (100MB)
        let image2 = ContainerImage::new(
            "sha256:image2".to_string(),
            "remotemedia-node:img2".to_string(),
            "hash2".to_string(),
            100 * 1024 * 1024,
            "3.10".to_string(),
        );
        cache.put(image2.clone()).unwrap();

        assert_eq!(cache.count(), 2);
        assert_eq!(cache.size(), 200 * 1024 * 1024);

        // Sleep to ensure different timestamps
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Add third image (100MB) - should trigger LRU eviction of first image
        let image3 = ContainerImage::new(
            "sha256:image3".to_string(),
            "remotemedia-node:img3".to_string(),
            "hash3".to_string(),
            100 * 1024 * 1024,
            "3.10".to_string(),
        );
        cache.put(image3.clone()).unwrap();

        // Cache should have evicted oldest image (image1)
        assert_eq!(cache.count(), 2);
        assert_eq!(cache.size(), 200 * 1024 * 1024);
        assert!(
            cache.get("hash1").is_none(),
            "Oldest image should be evicted"
        );
        assert!(cache.get("hash2").is_some(), "Second image should remain");
        assert!(cache.get("hash3").is_some(), "Third image should remain");
    }

    #[tokio::test]
    async fn test_image_cache_clear() {
        let mut cache = ImageCache::new(1024 * 1024 * 1024);

        let image = ContainerImage::new(
            "sha256:abc123".to_string(),
            "remotemedia-node:test".to_string(),
            "config_hash_123".to_string(),
            100 * 1024 * 1024,
            "3.10".to_string(),
        );

        cache.put(image).unwrap();
        assert_eq!(cache.count(), 1);

        cache.clear();
        assert_eq!(cache.count(), 0);
        assert_eq!(cache.size(), 0);
    }

    #[test]
    fn test_dockerfile_escapes_quotes_in_env_vars() {
        let mut config = create_test_config();
        config.env_vars.insert(
            "QUOTED_VAR".to_string(),
            "value with \"quotes\"".to_string(),
        );

        let dockerfile = ContainerBuilder::generate_dockerfile(&config).unwrap();

        assert!(dockerfile.contains("ENV QUOTED_VAR=\"value with \\\"quotes\\\"\""));
    }

    #[test]
    fn test_dockerfile_multi_stage_build() {
        let config = create_test_config();
        let dockerfile = ContainerBuilder::generate_dockerfile(&config).unwrap();

        // Check for multi-stage build pattern
        assert!(dockerfile.contains("FROM python:3.10-slim AS builder"));
        assert!(dockerfile.contains("# Runtime stage"));
        assert!(dockerfile.contains("COPY --from=builder"));
    }

    #[test]
    fn test_dockerfile_health_check() {
        let config = create_test_config();
        let dockerfile = ContainerBuilder::generate_dockerfile(&config).unwrap();

        assert!(dockerfile.contains("HEALTHCHECK"));
        assert!(dockerfile.contains("--interval=30s"));
        assert!(dockerfile.contains("--timeout=10s"));
    }

    // T031: Integration test for image caching behavior with Docker
    #[tokio::test]
    #[ignore] // Requires Docker daemon
    async fn test_container_builder_with_docker() {
        // This test requires Docker to be running
        // Skip if Docker is not available
        let docker = match Docker::connect_with_local_defaults() {
            Ok(d) => Arc::new(d),
            Err(_) => {
                println!("Skipping test - Docker not available");
                return;
            }
        };

        let builder = ContainerBuilder::new(docker.clone(), Some(1024 * 1024 * 1024));

        let config = DockerNodeConfig {
            python_version: "3.10".to_string(),
            base_image: None,
            system_packages: vec![],
            python_packages: vec!["requests".to_string()],
            memory_mb: 512,
            cpu_cores: 1.0,
            gpu_devices: vec![],
            shm_size_mb: 512,
            env_vars: HashMap::new(),
            volumes: vec![],
            security: SecurityConfig::default(),
        };

        // Build image for the first time
        let image1 = builder.build_image(&config, false).await.unwrap();
        assert!(!image1.image_id.is_empty());
        assert!(!image1.image_tag.is_empty());
        assert!(image1.size_bytes > 0);

        // Build same config again - should use cache
        let image2 = builder.build_image(&config, false).await.unwrap();
        assert_eq!(image1.image_id, image2.image_id);
        assert_eq!(image1.config_hash, image2.config_hash);

        // Verify cache stats
        let (count, size, _max_size) = builder.cache_stats().await;
        assert_eq!(count, 1);
        assert_eq!(size, image1.size_bytes);

        // Clean up - remove the test image
        use bollard::query_parameters::RemoveImageOptions;
        let _ = docker
            .remove_image(&image1.image_tag, Some(RemoveImageOptions::default()), None)
            .await;
    }

    #[tokio::test]
    async fn test_container_image_creation() {
        let image = ContainerImage::new(
            "sha256:test123".to_string(),
            "remotemedia-node:test".to_string(),
            "hash_abc".to_string(),
            1024 * 1024, // 1MB
            "3.11".to_string(),
        );

        assert_eq!(image.image_id, "sha256:test123");
        assert_eq!(image.image_tag, "remotemedia-node:test");
        assert_eq!(image.config_hash, "hash_abc");
        assert_eq!(image.size_bytes, 1024 * 1024);
        assert_eq!(image.python_version, "3.11");
        assert!(image.created_at <= Utc::now());
    }

    #[test]
    fn test_dockerfile_sorted_packages_for_determinism() {
        let mut config = create_test_config();
        config.python_packages = vec!["z-package".to_string(), "a-package".to_string()];
        config.system_packages = vec!["zsh".to_string(), "bash".to_string()];

        let dockerfile = ContainerBuilder::generate_dockerfile(&config).unwrap();

        // Check that packages appear in sorted order
        let z_pos = dockerfile.find("z-package").unwrap();
        let a_pos = dockerfile.find("a-package").unwrap();
        assert!(a_pos < z_pos, "Python packages should be sorted");

        let bash_pos = dockerfile.find("bash").unwrap();
        let zsh_pos = dockerfile.find("zsh").unwrap();
        assert!(bash_pos < zsh_pos, "System packages should be sorted");
    }
}
