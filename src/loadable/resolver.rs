//! Manifest-driven plugin resolver.
//!
//! Takes a [`Manifest::plugins`][crate::manifest::Manifest::plugins] list
//! and produces a list of local file paths ready to dlopen via
//! [`LoadableNodeBundle::load`].
//!
//! ## Scope
//!
//! - **Local paths**: `./foo.so`, `/abs/foo.so`, `libfoo.so`. Anchored
//!   at the resolver's base directory when relative.
//! - **GitHub releases**: canonical-org shorthand (`name[@version]` →
//!   `github.com/RemoteMedia-SDK/name`), `github.com/owner/repo[@version]`,
//!   `owner/repo[@version]`. Fetches `release-manifest.json` from the
//!   release's assets, picks the platform-matching entry, downloads
//!   the binary, verifies SHA256, caches in `<base>/remotemedia-plugins/cache/`.
//! - **Direct HTTPS URLs**: `https://...so` with optional `sha256` pinning.
//!
//! Phase 1C (next) adds Python source-load via tarball + plugin.toml
//! — for Python authors who want to skip the cdylib step entirely.
//!
//! ## Path anchoring
//!
//! Relative paths in plugin specs are resolved against a **base
//! directory** the caller passes in (typically the directory of the
//! manifest file, or the process CWD when the manifest was supplied as
//! a JSON string with no file backing). This avoids the "where am I
//! running from?" ambiguity that bites every other tooling system.
//!
//! ## Cache layout
//!
//! Downloads are stashed in `<base_dir>/remotemedia-plugins/cache/`:
//!
//! ```text
//! <base_dir>/remotemedia-plugins/cache/
//!   RemoteMedia-SDK_echo-python-loadable_v0.3_x86_64-linux/
//!     release-manifest.json
//!     libecho-x86_64-linux.so
//! ```
//!
//! Cache hits short-circuit the download. The cache key includes
//! `(owner, repo, version, platform)` so different versions /
//! architectures live side by side without conflict. SHA256 is
//! re-verified on every load (cheap relative to dlopen) — corrupt
//! caches surface immediately rather than dlopen'ing garbage.
//!
//! ## Example
//!
//! ```ignore
//! use std::path::Path;
//! use remotemedia_core::loadable::resolver::PluginResolver;
//!
//! let manifest = remotemedia_core::manifest::parse(manifest_json)?;
//! let resolver = PluginResolver::new(Path::new("/path/to/project"));
//! // Local-only — synchronous, no I/O network.
//! let local_paths = resolver.resolve_all(&[/* local path specs */])?;
//! // With network — async, downloads + caches as needed.
//! let any_paths = resolver.resolve_all_async(&manifest.plugins).await?;
//! ```

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use flate2::read::GzDecoder;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tar::Archive;

use crate::loadable::plugin_toml::{PluginLanguage, PluginToml};
use crate::manifest::{PluginSpec, PluginSpecExplicit};

/// Org name used when a bare plugin name (no slashes) is given as
/// shorthand. `"echo-python-loadable"` resolves to
/// `github.com/RemoteMedia-SDK/echo-python-loadable`.
pub const CANONICAL_ORG: &str = "RemoteMedia-SDK";

/// GitHub API base URL. Override via `REMOTEMEDIA_GITHUB_API_BASE` for
/// testing against a mock server.
const GITHUB_API_BASE: &str = "https://api.github.com";

/// GitHub release-asset URL prefix. Constructed as
/// `{base}/{owner}/{repo}/releases/download/{tag}/{file}`. Override via
/// `REMOTEMEDIA_GITHUB_RELEASE_BASE` for testing.
const GITHUB_RELEASE_BASE: &str = "https://github.com";

/// GitHub raw-content URL prefix. Used to fetch `plugin.toml` from a
/// repo at a specific tag without going through the API (no rate
/// limits for unauthenticated reads). Override via
/// `REMOTEMEDIA_GITHUB_RAW_BASE` for testing.
const GITHUB_RAW_BASE: &str = "https://raw.githubusercontent.com";

/// GitHub source-tarball download base. Constructed as
/// `{base}/{owner}/{repo}/tar.gz/refs/tags/{tag}`. Override via
/// `REMOTEMEDIA_GITHUB_CODELOAD_BASE` for testing.
const GITHUB_CODELOAD_BASE: &str = "https://codeload.github.com";

/// Filename the resolver looks for at the repo root to discover Python
/// source-load plugins. Optional for cdylib plugins (the abi_stable
/// factory list is authoritative).
pub const PLUGIN_TOML_FILE: &str = "plugin.toml";

/// Name of the metadata file the plugin author publishes alongside the
/// platform-specific binary assets in each GitHub release.
pub const RELEASE_MANIFEST_FILE: &str = "release-manifest.json";

/// Cache directory name (project-local) under the resolver's base_dir.
const CACHE_DIR_NAME: &str = "remotemedia-plugins/cache";

/// HTTP request timeout for both API and asset fetches.
const HTTP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// Errors surfacing during plugin resolution.
#[derive(Debug, thiserror::Error)]
pub enum ResolverError {
    #[error("plugin spec is empty (no value)")]
    EmptySpec,

    #[error(
        "plugin spec '{0}' has no recognizable form. \
         Expected one of: local path ('./foo.so', '/abs/foo.so'), \
         'github.com/owner/repo[@version]', 'owner/repo[@version]', \
         or canonical-org shorthand 'name[@version]'."
    )]
    UnrecognizedShorthand(String),

    #[error(
        "explicit plugin spec must have exactly one of `url`, `name`, or `path` \
         (got url={url:?}, name={name:?}, path={path:?})"
    )]
    ExplicitSpecAmbiguous { url: bool, name: bool, path: bool },

    #[error("local plugin file not found: {0} (resolved against base {1})")]
    LocalFileNotFound(PathBuf, PathBuf),

    #[error(
        "remote plugin resolution is not yet supported in this build \
         (spec: {0}). Network resolver lands in Phase 1B. Use a local \
         path (`./plugins/...`) in the meantime, or build with the \
         `plugin-resolver-network` feature once it's available."
    )]
    NetworkDownloadNotYetSupported(String),

    #[error("HTTP error fetching {url}: {source}")]
    Http {
        url: String,
        #[source]
        source: reqwest::Error,
    },

    #[error("HTTP {status} fetching {url}: {body}")]
    HttpStatus {
        url: String,
        status: u16,
        body: String,
    },

    #[error(
        "release-manifest.json at {url} couldn't be parsed: {source}. \
         Plugin author should publish a release-manifest.json matching \
         the schema in `remotemedia_core::loadable::resolver::ReleaseManifest`."
    )]
    BadReleaseManifest {
        url: String,
        #[source]
        source: serde_json::Error,
    },

    #[error(
        "release-manifest.json at {manifest_url} has no entry for the \
         current platform {platform:?}. Available platforms: {available:?}. \
         The plugin author needs to add a build for this platform, or \
         you can override with an explicit local path."
    )]
    PlatformNotPublished {
        manifest_url: String,
        platform: String,
        available: Vec<String>,
    },

    #[error(
        "downloaded {file} (from {url}) failed SHA256 verification. \
         Expected {expected_sha256}, got {actual_sha256}. The release \
         binary may be corrupt or tampered with — refusing to dlopen."
    )]
    Sha256Mismatch {
        url: String,
        file: String,
        expected_sha256: String,
        actual_sha256: String,
    },

    #[error("filesystem error in plugin cache: {source}")]
    CacheIo {
        #[source]
        source: std::io::Error,
    },

    #[error(
        "GitHub release lookup for {owner}/{repo} at {version} returned \
         no `tag_name` field — API response shape unexpected. URL: {url}"
    )]
    MissingTag {
        owner: String,
        repo: String,
        version: String,
        url: String,
    },

    #[error("plugin.toml at {url} couldn't be parsed: {source}")]
    BadPluginToml {
        url: String,
        #[source]
        source: crate::loadable::plugin_toml::PluginTomlError,
    },

    #[error("tarball extraction failed for {url}: {source}")]
    TarballExtract {
        url: String,
        #[source]
        source: std::io::Error,
    },

    #[error(
        "plugin '{name}' declares language=python in plugin.toml but the \
         python-source-plugin feature was not enabled at build time. \
         Rebuild with `--features python-source-plugin` on remotemedia-core."
    )]
    PythonSourceFeatureDisabled { name: String },
}

/// Resolution outcome — either a path to dlopen via `LoadableNodeBundle`
/// (the cdylib / release-load path) OR a Python source plugin with
/// already-extracted source + parsed `plugin.toml`.
///
/// The caller (`ensure_plugins_loaded`) dispatches on the variant to
/// either dlopen the .so or register a `SourcePythonFactory` per
/// `node_types` entry.
#[derive(Debug, Clone)]
pub enum ResolvedPlugin {
    /// Native cdylib (or direct URL pointing at one). Path is the
    /// fully-cached local file ready for `LoadableNodeBundle::load`.
    Cdylib { path: PathBuf },
    /// Python source plugin — repo tarball already downloaded +
    /// extracted into `module_root`. `plugin_toml` is the parsed
    /// metadata; `hash` is the tarball's SHA256 (stable cache key for
    /// the venv).
    SourcePython {
        plugin_toml: PluginToml,
        module_root: PathBuf,
        hash: String,
    },
}

/// Schema for the `release-manifest.json` file plugin authors publish
/// alongside their platform-specific binary assets in each GitHub
/// release.
///
/// Example:
/// ```json
/// {
///   "name": "echo-python-loadable",
///   "version": "v0.3",
///   "platforms": {
///     "x86_64-linux":  { "file": "libecho-x86_64-linux.so",   "sha256": "..." },
///     "aarch64-linux": { "file": "libecho-aarch64-linux.so",  "sha256": "..." },
///     "x86_64-darwin": { "file": "libecho-x86_64-darwin.dylib","sha256": "..." }
///   }
/// }
/// ```
///
/// `platforms` keys follow the convention `{arch}-{os}` where:
/// - `arch`: `x86_64` / `aarch64`
/// - `os`: `linux` / `darwin` / `windows`
///
/// Matches what `current_platform()` returns. Authors can also include
/// additional metadata fields — the resolver ignores anything outside
/// `name` / `version` / `platforms`, so it's safe to add tags / authors
/// / license fields without breaking the resolver.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReleaseManifest {
    /// Plugin name (informational; the resolver doesn't validate it).
    pub name: String,

    /// Plugin version (informational).
    pub version: String,

    /// Map of platform-string → asset entry. See
    /// [`current_platform()`] for the platform-string format.
    pub platforms: BTreeMap<String, ReleaseAsset>,
}

/// One platform-specific binary entry inside a [`ReleaseManifest`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReleaseAsset {
    /// Filename within the release. Used to construct the asset URL
    /// `{release_base}/{owner}/{repo}/releases/download/{tag}/{file}`.
    pub file: String,

    /// Hex-encoded SHA256 of the file contents. Verified after download
    /// — mismatch aborts with [`ResolverError::Sha256Mismatch`].
    pub sha256: String,
}

/// Return the `{arch}-{os}` platform string for the current host,
/// matching the `platforms` keys plugin authors should use in
/// `release-manifest.json`.
///
/// Mapping:
/// - Linux x86_64 → `x86_64-linux`
/// - Linux aarch64 → `aarch64-linux`
/// - macOS Intel → `x86_64-darwin`
/// - macOS Apple Silicon → `aarch64-darwin`
/// - Windows x86_64 → `x86_64-windows`
/// - Windows aarch64 → `aarch64-windows`
///
/// Returns `"{arch}-{os}"` for anything else — gives the resolver a
/// stable string to match on without exhaustively enumerating every
/// rustc target.
pub fn current_platform() -> String {
    let arch = std::env::consts::ARCH;
    let os = std::env::consts::OS;
    format!("{arch}-{os}")
}

/// Resolves [`PluginSpec`] entries to concrete local file paths.
///
/// Stateless apart from the base directory; clone freely.
#[derive(Debug, Clone)]
pub struct PluginResolver {
    /// Directory that anchors relative paths in plugin specs. Usually
    /// the directory containing the manifest file. When the manifest
    /// originated from a JSON string with no file backing, the caller
    /// passes the process CWD.
    base_dir: PathBuf,
}

impl PluginResolver {
    /// Create a new resolver anchored at `base_dir`.
    pub fn new(base_dir: impl Into<PathBuf>) -> Self {
        Self {
            base_dir: base_dir.into(),
        }
    }

    /// Resolve a single spec to a concrete local path.
    ///
    /// Phase 1A: only local-path forms succeed. Remote forms (URL /
    /// canonical-org shorthand / `owner/repo`) return
    /// [`ResolverError::NetworkDownloadNotYetSupported`].
    pub fn resolve(&self, spec: &PluginSpec) -> Result<PathBuf, ResolverError> {
        let parsed = parse_spec(spec)?;
        match parsed {
            ParsedSpec::LocalPath(p) => self.resolve_local_path(&p),
            ParsedSpec::CanonicalOrg { .. }
            | ParsedSpec::GithubRepo { .. }
            | ParsedSpec::DirectUrl { .. } => Err(ResolverError::NetworkDownloadNotYetSupported(
                format!("{spec:?}"),
            )),
        }
    }

    /// Resolve every spec in `specs`, returning a vec of local paths
    /// in the same order. Stops at the first error.
    ///
    /// Synchronous — only handles local-path specs. Use
    /// [`Self::resolve_all_async`] when the manifest may contain
    /// remote (GitHub / URL) specs.
    pub fn resolve_all(&self, specs: &[PluginSpec]) -> Result<Vec<PathBuf>, ResolverError> {
        specs.iter().map(|s| self.resolve(s)).collect()
    }

    /// Async variant of [`Self::resolve`]. Returns the full
    /// `ResolvedPlugin` enum so the caller can dispatch on cdylib vs.
    /// source-python paths.
    pub async fn resolve_async(&self, spec: &PluginSpec) -> Result<ResolvedPlugin, ResolverError> {
        let parsed = parse_spec(spec)?;
        match parsed {
            ParsedSpec::LocalPath(p) => self.resolve_local_path_dispatched(&p),
            ParsedSpec::CanonicalOrg { name, version } => {
                self.resolve_github_dispatched(CANONICAL_ORG, &name, &version, sha256_of_spec(spec))
                    .await
            }
            ParsedSpec::GithubRepo {
                owner,
                repo,
                version,
            } => {
                self.resolve_github_dispatched(&owner, &repo, &version, sha256_of_spec(spec))
                    .await
            }
            ParsedSpec::DirectUrl { url, sha256 } => {
                let path = self.resolve_direct_url(&url, sha256.as_deref()).await?;
                Ok(ResolvedPlugin::Cdylib { path })
            }
        }
    }

    /// Async variant of [`Self::resolve_all`]. Stops at the first error.
    pub async fn resolve_all_async(
        &self,
        specs: &[PluginSpec],
    ) -> Result<Vec<ResolvedPlugin>, ResolverError> {
        let mut out = Vec::with_capacity(specs.len());
        for spec in specs {
            out.push(self.resolve_async(spec).await?);
        }
        Ok(out)
    }

    /// Dispatch a GitHub-shaped spec: fetch `plugin.toml` first to see
    /// if this is a Python source plugin. If so, route through the
    /// source-load path. Otherwise (404 on plugin.toml or
    /// language=rust) fall back to the release-load path.
    async fn resolve_github_dispatched(
        &self,
        owner: &str,
        repo: &str,
        version_spec: &str,
        user_expected_sha256: Option<String>,
    ) -> Result<ResolvedPlugin, ResolverError> {
        let client = http_client()?;

        // Resolve `latest` to a concrete tag up front so plugin.toml +
        // cache keys are stable across re-publishes of "latest".
        let tag = if version_spec == "latest" {
            resolve_latest_tag(&client, owner, repo).await?
        } else {
            version_spec.to_string()
        };

        // Try to fetch plugin.toml. Two outcomes drive dispatch:
        //   - 200 + parses → use the metadata's `language` discriminant.
        //   - 404 / other error → assume legacy cdylib repo without
        //     plugin.toml; fall back to release-load.
        if let Some(plugin_toml) = self
            .try_fetch_plugin_toml(&client, owner, repo, &tag)
            .await?
        {
            match plugin_toml.plugin.language {
                PluginLanguage::Python => {
                    let (module_root, hash) = self
                        .download_and_extract_tarball(&client, owner, repo, &tag)
                        .await?;
                    return Ok(ResolvedPlugin::SourcePython {
                        plugin_toml,
                        module_root,
                        hash,
                    });
                }
                PluginLanguage::Rust => {
                    // Fall through to release-load path. plugin.toml
                    // for Rust plugins is informational; the cdylib's
                    // factory list is authoritative.
                }
            }
        }

        // Release-load (Phase 1B) — download release-manifest.json,
        // pick platform-matching asset, verify SHA256, cache.
        let path = self
            .resolve_github(owner, repo, &tag, user_expected_sha256)
            .await?;
        Ok(ResolvedPlugin::Cdylib { path })
    }

    /// Try to fetch `plugin.toml` from the repo at the given tag via
    /// raw.githubusercontent.com. Returns `Ok(None)` on 404 so the
    /// caller can fall back gracefully. Other HTTP errors propagate.
    async fn try_fetch_plugin_toml(
        &self,
        client: &reqwest::Client,
        owner: &str,
        repo: &str,
        tag: &str,
    ) -> Result<Option<PluginToml>, ResolverError> {
        let url = format!(
            "{}/{}/{}/{}/{}",
            raw_base(),
            owner,
            repo,
            tag,
            PLUGIN_TOML_FILE
        );

        // Cache plugin.toml alongside the eventual tarball — single
        // cache subdir per (owner, repo, tag) holds everything.
        let cache_subdir = self.cache_dir().join(format!("{owner}_{repo}_{tag}"));
        ensure_dir(&cache_subdir)?;
        let cached_path = cache_subdir.join(PLUGIN_TOML_FILE);

        if !cached_path.exists() {
            let resp = client
                .get(&url)
                .send()
                .await
                .map_err(|source| ResolverError::Http {
                    url: url.clone(),
                    source,
                })?;
            let status = resp.status();
            if status.as_u16() == 404 {
                // Mark "absent" with a sentinel file so subsequent
                // resolutions don't re-hit the network.
                let _ = std::fs::write(cache_subdir.join(".plugin-toml-absent"), b"");
                return Ok(None);
            }
            if !status.is_success() {
                let body = resp
                    .text()
                    .await
                    .unwrap_or_else(|_| "<no body>".to_string());
                return Err(ResolverError::HttpStatus {
                    url,
                    status: status.as_u16(),
                    body,
                });
            }
            let bytes = resp.bytes().await.map_err(|source| ResolverError::Http {
                url: url.clone(),
                source,
            })?;
            std::fs::write(&cached_path, &bytes)
                .map_err(|e| ResolverError::CacheIo { source: e })?;
        }

        // Check the absent-sentinel before re-parsing — saves a
        // file_to_string round on the common "no plugin.toml" case
        // once we've cached the 404.
        if cache_subdir.join(".plugin-toml-absent").exists() {
            return Ok(None);
        }

        let text = std::fs::read_to_string(&cached_path)
            .map_err(|e| ResolverError::CacheIo { source: e })?;
        let parsed = PluginToml::parse(&text).map_err(|source| ResolverError::BadPluginToml {
            url: url.clone(),
            source,
        })?;
        Ok(Some(parsed))
    }

    /// Download + extract a GitHub repo tarball. Returns
    /// `(module_root, sha256)` — module_root is the directory
    /// containing the repo's contents (top-level dir from tar.gz),
    /// sha256 is the tarball hash (used as the venv cache key).
    async fn download_and_extract_tarball(
        &self,
        client: &reqwest::Client,
        owner: &str,
        repo: &str,
        tag: &str,
    ) -> Result<(PathBuf, String), ResolverError> {
        let url = format!(
            "{}/{}/{}/tar.gz/refs/tags/{}",
            codeload_base(),
            owner,
            repo,
            tag
        );

        let cache_subdir = self.cache_dir().join(format!("{owner}_{repo}_{tag}"));
        ensure_dir(&cache_subdir)?;
        let source_dir = cache_subdir.join("source");

        // If we already have an extracted source dir AND know the
        // hash from a prior run, short-circuit.
        let hash_path = cache_subdir.join(".tarball-sha256");
        if source_dir.exists() && hash_path.exists() {
            let hash = std::fs::read_to_string(&hash_path)
                .map_err(|e| ResolverError::CacheIo { source: e })?;
            return Ok((source_dir, hash.trim().to_string()));
        }

        let bytes = fetch_bytes(client, &url).await?;
        let hash = sha256_hex(&bytes);

        // Clean any partial / stale extraction.
        if source_dir.exists() {
            std::fs::remove_dir_all(&source_dir)
                .map_err(|e| ResolverError::CacheIo { source: e })?;
        }
        std::fs::create_dir_all(&source_dir).map_err(|e| ResolverError::CacheIo { source: e })?;

        // GitHub tarballs unpack into a single top-level dir like
        // `{repo}-{tag-hash}/`. We flatten that — extract to a temp
        // dir, then move the inner directory's contents up to
        // source_dir.
        let extraction_root = cache_subdir.join("source.extract");
        if extraction_root.exists() {
            std::fs::remove_dir_all(&extraction_root)
                .map_err(|e| ResolverError::CacheIo { source: e })?;
        }
        std::fs::create_dir_all(&extraction_root)
            .map_err(|e| ResolverError::CacheIo { source: e })?;

        let decoder = GzDecoder::new(std::io::Cursor::new(&bytes));
        let mut archive = Archive::new(decoder);
        archive
            .unpack(&extraction_root)
            .map_err(|source| ResolverError::TarballExtract {
                url: url.clone(),
                source,
            })?;

        // Find the single top-level directory and move its children
        // into source_dir.
        let mut entries = std::fs::read_dir(&extraction_root)
            .map_err(|e| ResolverError::CacheIo { source: e })?
            .filter_map(|e| e.ok())
            .collect::<Vec<_>>();
        if entries.len() == 1 && entries[0].file_type().map(|t| t.is_dir()).unwrap_or(false) {
            let top = entries.remove(0).path();
            // Move (rename) the top-level dir → source_dir.
            std::fs::remove_dir(&source_dir).ok(); // we just created it; rename needs it absent
            std::fs::rename(&top, &source_dir).map_err(|e| ResolverError::CacheIo { source: e })?;
        } else {
            // No single top-level dir — copy everything verbatim.
            for entry in entries {
                let from = entry.path();
                let to = source_dir.join(entry.file_name());
                std::fs::rename(&from, &to).map_err(|e| ResolverError::CacheIo { source: e })?;
            }
        }
        let _ = std::fs::remove_dir_all(&extraction_root);

        std::fs::write(&hash_path, &hash).map_err(|e| ResolverError::CacheIo { source: e })?;
        Ok((source_dir, hash))
    }

    /// Returns the project-local cache directory used for downloaded
    /// release assets. Created lazily on first download.
    pub fn cache_dir(&self) -> PathBuf {
        self.base_dir.join(CACHE_DIR_NAME)
    }

    /// Resolve a GitHub-shaped spec: fetch the release-manifest, pick
    /// the platform-matching asset, download + verify SHA256, return
    /// the cached local path.
    async fn resolve_github(
        &self,
        owner: &str,
        repo: &str,
        version_spec: &str,
        user_expected_sha256: Option<String>,
    ) -> Result<PathBuf, ResolverError> {
        let client = http_client()?;
        // `latest` needs a concrete tag lookup; everything else is
        // already a tag. Resolving early lets the cache key be stable
        // across "latest" calls that resolve to the same release.
        let tag = if version_spec == "latest" {
            resolve_latest_tag(&client, owner, repo).await?
        } else {
            version_spec.to_string()
        };

        let platform = current_platform();
        let cache_subdir = self
            .cache_dir()
            .join(format!("{owner}_{repo}_{tag}_{platform}"));
        ensure_dir(&cache_subdir)?;

        // Fetch (or re-use cached) release-manifest.json.
        let release_manifest_url = format!(
            "{}/{}/{}/releases/download/{}/{}",
            release_base(),
            owner,
            repo,
            tag,
            RELEASE_MANIFEST_FILE
        );
        let release_manifest_path = cache_subdir.join(RELEASE_MANIFEST_FILE);
        if !release_manifest_path.exists() {
            let bytes = fetch_bytes(&client, &release_manifest_url).await?;
            std::fs::write(&release_manifest_path, &bytes)
                .map_err(|e| ResolverError::CacheIo { source: e })?;
        }
        let manifest_bytes = std::fs::read(&release_manifest_path)
            .map_err(|e| ResolverError::CacheIo { source: e })?;
        let manifest: ReleaseManifest =
            serde_json::from_slice(&manifest_bytes).map_err(|source| {
                ResolverError::BadReleaseManifest {
                    url: release_manifest_url.clone(),
                    source,
                }
            })?;

        // Pick the platform-matching asset.
        let asset = manifest.platforms.get(&platform).ok_or_else(|| {
            ResolverError::PlatformNotPublished {
                manifest_url: release_manifest_url.clone(),
                platform: platform.clone(),
                available: manifest.platforms.keys().cloned().collect(),
            }
        })?;

        // Honor user-provided SHA256 pin if set — overrides the one in
        // release-manifest.json. Use case: paranoid lockfile-style
        // pinning from the manifest author who doesn't fully trust the
        // upstream release-manifest.
        let expected_sha256 = user_expected_sha256
            .as_deref()
            .unwrap_or(&asset.sha256)
            .to_string();

        let asset_url = format!(
            "{}/{}/{}/releases/download/{}/{}",
            release_base(),
            owner,
            repo,
            tag,
            asset.file
        );
        let asset_path = cache_subdir.join(&asset.file);

        // Re-use existing download if SHA matches. Cheap relative to
        // dlopen — catches "user manually overwrote the cached file"
        // and "release was re-uploaded with different bytes".
        if asset_path.exists() {
            if file_sha256(&asset_path)?.eq_ignore_ascii_case(&expected_sha256) {
                return Ok(asset_path);
            }
            // Stale cache — drop it and re-download.
            let _ = std::fs::remove_file(&asset_path);
        }

        let bytes = fetch_bytes(&client, &asset_url).await?;
        let actual_sha = sha256_hex(&bytes);
        if !actual_sha.eq_ignore_ascii_case(&expected_sha256) {
            return Err(ResolverError::Sha256Mismatch {
                url: asset_url,
                file: asset.file.clone(),
                expected_sha256,
                actual_sha256: actual_sha,
            });
        }
        std::fs::write(&asset_path, &bytes).map_err(|e| ResolverError::CacheIo { source: e })?;
        // Set executable bit on Unix so the .so loader doesn't trip
        // permissions. Same convention dlopen consumers expect.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = std::fs::metadata(&asset_path)
                .map_err(|e| ResolverError::CacheIo { source: e })?
                .permissions();
            perms.set_mode(0o755);
            let _ = std::fs::set_permissions(&asset_path, perms);
        }
        Ok(asset_path)
    }

    /// Direct-URL resolution: download the file at `url` (optionally
    /// SHA256-pinned by the user), cache under a hash-derived
    /// subdirectory, return the cached local path.
    async fn resolve_direct_url(
        &self,
        url: &str,
        expected_sha256: Option<&str>,
    ) -> Result<PathBuf, ResolverError> {
        let client = http_client()?;
        // Cache key derived from the URL — stable across runs but
        // doesn't dedupe across renamed URLs that serve the same
        // bytes (acceptable tradeoff for v1).
        let key = sha256_hex(url.as_bytes());
        let cache_subdir = self.cache_dir().join(format!("url_{}", &key[..16]));
        ensure_dir(&cache_subdir)?;

        // Filename: last path segment of the URL (or "plugin.bin"
        // when the URL has no path component).
        let filename = url
            .rsplit('/')
            .next()
            .filter(|s| !s.is_empty())
            .unwrap_or("plugin.bin")
            .to_string();
        let asset_path = cache_subdir.join(&filename);

        if asset_path.exists() {
            if let Some(expected) = expected_sha256 {
                if file_sha256(&asset_path)?.eq_ignore_ascii_case(expected) {
                    return Ok(asset_path);
                }
                let _ = std::fs::remove_file(&asset_path);
            } else {
                return Ok(asset_path);
            }
        }

        let bytes = fetch_bytes(&client, url).await?;
        if let Some(expected) = expected_sha256 {
            let actual = sha256_hex(&bytes);
            if !actual.eq_ignore_ascii_case(expected) {
                return Err(ResolverError::Sha256Mismatch {
                    url: url.to_string(),
                    file: filename,
                    expected_sha256: expected.to_string(),
                    actual_sha256: actual,
                });
            }
        }
        std::fs::write(&asset_path, &bytes).map_err(|e| ResolverError::CacheIo { source: e })?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = std::fs::metadata(&asset_path)
                .map_err(|e| ResolverError::CacheIo { source: e })?
                .permissions();
            perms.set_mode(0o755);
            let _ = std::fs::set_permissions(&asset_path, perms);
        }
        Ok(asset_path)
    }

    /// Local-path dispatch: if `raw` resolves to a directory containing
    /// a `plugin.toml`, treat as a source-load Python plugin; otherwise
    /// the path is a cdylib to dlopen. Lets the example layout work
    /// without going through GitHub (`"plugins": ["./examples/python-source-plugin"]`).
    fn resolve_local_path_dispatched(&self, raw: &str) -> Result<ResolvedPlugin, ResolverError> {
        let path = self.resolve_local_path(raw)?;
        if path.is_dir() {
            let plugin_toml_path = path.join(PLUGIN_TOML_FILE);
            if plugin_toml_path.exists() {
                let text = std::fs::read_to_string(&plugin_toml_path)
                    .map_err(|e| ResolverError::CacheIo { source: e })?;
                let plugin_toml =
                    PluginToml::parse(&text).map_err(|source| ResolverError::BadPluginToml {
                        url: plugin_toml_path.display().to_string(),
                        source,
                    })?;
                // For local source plugins the "hash" is just the
                // canonical absolute path — no tarball to checksum.
                // Different plugins at different paths get distinct
                // venv cache slots; the same path always re-uses the
                // same venv across runs (PEP 723 deps determine the
                // contents, so this is safe).
                let canonical = path.canonicalize().unwrap_or(path.clone());
                let hash = sha256_hex(canonical.display().to_string().as_bytes());
                return Ok(ResolvedPlugin::SourcePython {
                    plugin_toml,
                    module_root: canonical,
                    hash,
                });
            }
            // Directory without plugin.toml is ambiguous — not a
            // cdylib (`LoadableNodeBundle::load` would fail with a
            // confusing error), not a source plugin. Surface the
            // mismatch directly.
            return Err(ResolverError::LocalFileNotFound(
                plugin_toml_path,
                self.base_dir.clone(),
            ));
        }
        Ok(ResolvedPlugin::Cdylib { path })
    }

    fn resolve_local_path(&self, raw: &str) -> Result<PathBuf, ResolverError> {
        let p = Path::new(raw);
        let resolved = if p.is_absolute() {
            p.to_path_buf()
        } else {
            self.base_dir.join(p)
        };
        if !resolved.exists() {
            return Err(ResolverError::LocalFileNotFound(
                resolved,
                self.base_dir.clone(),
            ));
        }
        Ok(resolved)
    }
}

/// Internal: classification of a parsed plugin spec.
///
/// `pub(crate)` so tests in this module can match on it without
/// exposing it as a stable public surface.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ParsedSpec {
    /// Local filesystem path. Anchored at resolver's base_dir when relative.
    LocalPath(String),
    /// Canonical-org shorthand: `github.com/RemoteMedia-SDK/<name>` at `version`.
    CanonicalOrg { name: String, version: String },
    /// Explicit `owner/repo` or `github.com/owner/repo`.
    GithubRepo {
        owner: String,
        repo: String,
        version: String,
    },
    /// Direct HTTP(S) URL to a plugin binary. Optionally pinned by SHA256.
    DirectUrl { url: String, sha256: Option<String> },
}

/// Parse a [`PluginSpec`] into the internal classification, applying the
/// resolution rules documented on `PluginSpec`.
///
/// Pure function — no I/O. Test-friendly.
pub(crate) fn parse_spec(spec: &PluginSpec) -> Result<ParsedSpec, ResolverError> {
    match spec {
        PluginSpec::Shorthand(s) => parse_shorthand(s),
        PluginSpec::Explicit(e) => parse_explicit(e),
    }
}

fn parse_shorthand(s: &str) -> Result<ParsedSpec, ResolverError> {
    let s = s.trim();
    if s.is_empty() {
        return Err(ResolverError::EmptySpec);
    }

    // 1. Direct HTTP(S) URL — checked BEFORE the local-path extension
    //    heuristic so a URL ending in `.so` (the common case) isn't
    //    misclassified as a local file.
    if s.starts_with("http://") || s.starts_with("https://") {
        return Ok(ParsedSpec::DirectUrl {
            url: s.to_string(),
            sha256: None,
        });
    }

    // 2. Local path: starts with `./`, `../`, `/`, equals `.` / `..`,
    //    OR ends in a known plugin extension (.so/.dylib/.dll). The
    //    extension check catches bare filenames like `libfoo.so` that
    //    aren't absolute or `./`-prefixed; the bare-`.` check catches
    //    "this directory" — common for source-load Python plugins
    //    where the manifest sits in the same dir as `plugin.toml`.
    if s == "."
        || s == ".."
        || s.starts_with("./")
        || s.starts_with("../")
        || s.starts_with('/')
        || looks_like_plugin_file(s)
    {
        return Ok(ParsedSpec::LocalPath(s.to_string()));
    }

    // Split off optional `@version` suffix. The first '@' wins — slugs
    // in canonical-org names don't contain '@'.
    let (slug, version) = match s.split_once('@') {
        Some((slug, ver)) => (slug.trim(), ver.trim().to_string()),
        None => (s, "latest".to_string()),
    };
    if slug.is_empty() {
        return Err(ResolverError::UnrecognizedShorthand(s.to_string()));
    }
    if version.is_empty() {
        return Err(ResolverError::UnrecognizedShorthand(s.to_string()));
    }

    // 3. Full `github.com/owner/repo` form.
    if let Some(rest) = slug.strip_prefix("github.com/") {
        return parse_owner_repo(rest, version);
    }

    // 4. Bare `owner/repo` — two segments separated by `/`.
    if slug.contains('/') {
        return parse_owner_repo(slug, version);
    }

    // 5. Canonical-org shorthand: single name, no slashes.
    // Must be a valid GitHub-style identifier (alphanumeric + `-` + `_` + `.`).
    if !slug
        .chars()
        .all(|c| c.is_alphanumeric() || c == '-' || c == '_' || c == '.')
    {
        return Err(ResolverError::UnrecognizedShorthand(s.to_string()));
    }
    Ok(ParsedSpec::CanonicalOrg {
        name: slug.to_string(),
        version,
    })
}

fn parse_owner_repo(rest: &str, version: String) -> Result<ParsedSpec, ResolverError> {
    // Strip any trailing `/` so `owner/repo/` and `owner/repo` parse the same.
    let rest = rest.trim_end_matches('/');
    let mut parts = rest.splitn(2, '/');
    let owner = parts.next().unwrap_or("").trim();
    let repo = parts.next().unwrap_or("").trim();
    if owner.is_empty() || repo.is_empty() {
        return Err(ResolverError::UnrecognizedShorthand(format!(
            "{rest}@{version}"
        )));
    }
    // Reject extra slashes — `owner/repo/foo` is not a plugin spec.
    if repo.contains('/') {
        return Err(ResolverError::UnrecognizedShorthand(format!(
            "{rest}@{version}"
        )));
    }
    Ok(ParsedSpec::GithubRepo {
        owner: owner.to_string(),
        repo: repo.to_string(),
        version,
    })
}

fn parse_explicit(e: &PluginSpecExplicit) -> Result<ParsedSpec, ResolverError> {
    let url_set = e.url.is_some();
    let name_set = e.name.is_some();
    let path_set = e.path.is_some();
    let count = (url_set as u8) + (name_set as u8) + (path_set as u8);
    if count != 1 {
        return Err(ResolverError::ExplicitSpecAmbiguous {
            url: url_set,
            name: name_set,
            path: path_set,
        });
    }
    if let Some(path) = &e.path {
        return Ok(ParsedSpec::LocalPath(path.clone()));
    }
    if let Some(url) = &e.url {
        return Ok(ParsedSpec::DirectUrl {
            url: url.clone(),
            sha256: e.sha256.clone(),
        });
    }
    if let Some(name) = &e.name {
        // Reuse the shorthand parser to honor identical rules.
        let combined = match &e.version {
            Some(v) => format!("{name}@{v}"),
            None => name.clone(),
        };
        return parse_shorthand(&combined);
    }
    unreachable!("count == 1 guarantees one of url/name/path is set")
}

fn looks_like_plugin_file(s: &str) -> bool {
    let lower = s.to_lowercase();
    lower.ends_with(".so") || lower.ends_with(".dylib") || lower.ends_with(".dll")
}

/// Resolve `latest` → concrete tag via the GitHub API. The API
/// response shape is documented at
/// <https://docs.github.com/en/rest/releases/releases#get-the-latest-release>;
/// we only need `tag_name`.
async fn resolve_latest_tag(
    client: &reqwest::Client,
    owner: &str,
    repo: &str,
) -> Result<String, ResolverError> {
    let url = format!("{}/repos/{}/{}/releases/latest", api_base(), owner, repo);
    let resp = client
        .get(&url)
        .send()
        .await
        .map_err(|source| ResolverError::Http {
            url: url.clone(),
            source,
        })?;
    let status = resp.status();
    if !status.is_success() {
        let body = resp
            .text()
            .await
            .unwrap_or_else(|_| "<no body>".to_string());
        return Err(ResolverError::HttpStatus {
            url,
            status: status.as_u16(),
            body,
        });
    }
    let v: serde_json::Value = resp.json().await.map_err(|source| ResolverError::Http {
        url: url.clone(),
        source,
    })?;
    v.get("tag_name")
        .and_then(|t| t.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| ResolverError::MissingTag {
            owner: owner.to_string(),
            repo: repo.to_string(),
            version: "latest".to_string(),
            url,
        })
}

/// Single shared reqwest client construction. Sets a User-Agent
/// (GitHub API rejects requests without one) and a 60s timeout.
fn http_client() -> Result<reqwest::Client, ResolverError> {
    reqwest::Client::builder()
        .user_agent(format!(
            "remotemedia-core/{} plugin-resolver",
            env!("CARGO_PKG_VERSION")
        ))
        .timeout(HTTP_TIMEOUT)
        .build()
        .map_err(|source| ResolverError::Http {
            url: "<client-init>".to_string(),
            source,
        })
}

async fn fetch_bytes(client: &reqwest::Client, url: &str) -> Result<Vec<u8>, ResolverError> {
    let resp = client
        .get(url)
        .send()
        .await
        .map_err(|source| ResolverError::Http {
            url: url.to_string(),
            source,
        })?;
    let status = resp.status();
    if !status.is_success() {
        let body = resp
            .text()
            .await
            .unwrap_or_else(|_| "<no body>".to_string());
        return Err(ResolverError::HttpStatus {
            url: url.to_string(),
            status: status.as_u16(),
            body,
        });
    }
    let bytes = resp.bytes().await.map_err(|source| ResolverError::Http {
        url: url.to_string(),
        source,
    })?;
    Ok(bytes.to_vec())
}

fn api_base() -> String {
    std::env::var("REMOTEMEDIA_GITHUB_API_BASE").unwrap_or_else(|_| GITHUB_API_BASE.to_string())
}

fn release_base() -> String {
    std::env::var("REMOTEMEDIA_GITHUB_RELEASE_BASE")
        .unwrap_or_else(|_| GITHUB_RELEASE_BASE.to_string())
}

fn raw_base() -> String {
    std::env::var("REMOTEMEDIA_GITHUB_RAW_BASE").unwrap_or_else(|_| GITHUB_RAW_BASE.to_string())
}

fn codeload_base() -> String {
    std::env::var("REMOTEMEDIA_GITHUB_CODELOAD_BASE")
        .unwrap_or_else(|_| GITHUB_CODELOAD_BASE.to_string())
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

fn file_sha256(path: &Path) -> Result<String, ResolverError> {
    let bytes = std::fs::read(path).map_err(|e| ResolverError::CacheIo { source: e })?;
    Ok(sha256_hex(&bytes))
}

fn ensure_dir(p: &Path) -> Result<(), ResolverError> {
    std::fs::create_dir_all(p).map_err(|source| ResolverError::CacheIo { source })
}

/// Extract a user-pinned SHA256 from the explicit object form, if any.
fn sha256_of_spec(spec: &PluginSpec) -> Option<String> {
    match spec {
        PluginSpec::Explicit(e) => e.sha256.clone(),
        PluginSpec::Shorthand(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sh(s: &str) -> PluginSpec {
        PluginSpec::Shorthand(s.to_string())
    }

    #[test]
    fn parse_relative_local_path() {
        let s = parse_spec(&sh("./plugins/libfoo.so")).unwrap();
        assert_eq!(s, ParsedSpec::LocalPath("./plugins/libfoo.so".into()));
    }

    #[test]
    fn parse_absolute_local_path() {
        let s = parse_spec(&sh("/abs/libfoo.so")).unwrap();
        assert_eq!(s, ParsedSpec::LocalPath("/abs/libfoo.so".into()));
    }

    #[test]
    fn parse_bare_filename_with_so_extension() {
        let s = parse_spec(&sh("libfoo.so")).unwrap();
        assert_eq!(s, ParsedSpec::LocalPath("libfoo.so".into()));
    }

    #[test]
    fn parse_bare_filename_with_dll_extension() {
        let s = parse_spec(&sh("foo.dll")).unwrap();
        assert_eq!(s, ParsedSpec::LocalPath("foo.dll".into()));
    }

    #[test]
    fn parse_bare_filename_with_dylib_extension() {
        let s = parse_spec(&sh("libfoo.dylib")).unwrap();
        assert_eq!(s, ParsedSpec::LocalPath("libfoo.dylib".into()));
    }

    #[test]
    fn parse_direct_url() {
        let s = parse_spec(&sh("https://example.com/foo.so")).unwrap();
        assert_eq!(
            s,
            ParsedSpec::DirectUrl {
                url: "https://example.com/foo.so".into(),
                sha256: None,
            }
        );
    }

    #[test]
    fn parse_canonical_org_shorthand_no_version() {
        let s = parse_spec(&sh("echo-python-loadable")).unwrap();
        assert_eq!(
            s,
            ParsedSpec::CanonicalOrg {
                name: "echo-python-loadable".into(),
                version: "latest".into(),
            }
        );
    }

    #[test]
    fn parse_canonical_org_shorthand_with_version() {
        let s = parse_spec(&sh("moss-tts-realtime@v0.3")).unwrap();
        assert_eq!(
            s,
            ParsedSpec::CanonicalOrg {
                name: "moss-tts-realtime".into(),
                version: "v0.3".into(),
            }
        );
    }

    #[test]
    fn parse_github_full_form() {
        let s = parse_spec(&sh("github.com/owner/repo@v1.0")).unwrap();
        assert_eq!(
            s,
            ParsedSpec::GithubRepo {
                owner: "owner".into(),
                repo: "repo".into(),
                version: "v1.0".into(),
            }
        );
    }

    #[test]
    fn parse_github_bare_owner_repo() {
        let s = parse_spec(&sh("owner/repo")).unwrap();
        assert_eq!(
            s,
            ParsedSpec::GithubRepo {
                owner: "owner".into(),
                repo: "repo".into(),
                version: "latest".into(),
            }
        );
    }

    #[test]
    fn parse_explicit_path() {
        let spec = PluginSpec::Explicit(PluginSpecExplicit {
            path: Some("./local/libfoo.so".into()),
            ..Default::default()
        });
        assert_eq!(
            parse_spec(&spec).unwrap(),
            ParsedSpec::LocalPath("./local/libfoo.so".into())
        );
    }

    #[test]
    fn parse_explicit_url_with_sha256() {
        let spec = PluginSpec::Explicit(PluginSpecExplicit {
            url: Some("https://example.com/foo.so".into()),
            sha256: Some("abc123".into()),
            ..Default::default()
        });
        assert_eq!(
            parse_spec(&spec).unwrap(),
            ParsedSpec::DirectUrl {
                url: "https://example.com/foo.so".into(),
                sha256: Some("abc123".into()),
            }
        );
    }

    #[test]
    fn parse_explicit_name_with_version() {
        let spec = PluginSpec::Explicit(PluginSpecExplicit {
            name: Some("echo-python-loadable".into()),
            version: Some("v0.2".into()),
            ..Default::default()
        });
        assert_eq!(
            parse_spec(&spec).unwrap(),
            ParsedSpec::CanonicalOrg {
                name: "echo-python-loadable".into(),
                version: "v0.2".into(),
            }
        );
    }

    #[test]
    fn parse_explicit_ambiguous_url_and_name_rejected() {
        let spec = PluginSpec::Explicit(PluginSpecExplicit {
            url: Some("https://example.com/foo.so".into()),
            name: Some("foo".into()),
            ..Default::default()
        });
        assert!(matches!(
            parse_spec(&spec),
            Err(ResolverError::ExplicitSpecAmbiguous { .. })
        ));
    }

    #[test]
    fn parse_empty_shorthand_rejected() {
        assert!(matches!(parse_spec(&sh("")), Err(ResolverError::EmptySpec)));
    }

    #[test]
    fn parse_invalid_canonical_name_rejected() {
        // Spaces / special chars are not valid GitHub names.
        assert!(matches!(
            parse_spec(&sh("not a valid name")),
            Err(ResolverError::UnrecognizedShorthand(_))
        ));
    }

    #[test]
    fn resolve_relative_local_path_anchors_at_base_dir() {
        // Build a temp directory layout
        let tmp = tempfile::tempdir().expect("tempdir");
        let plugins_dir = tmp.path().join("plugins");
        std::fs::create_dir(&plugins_dir).unwrap();
        let plugin_file = plugins_dir.join("libfoo.so");
        std::fs::write(&plugin_file, b"x").unwrap();

        let resolver = PluginResolver::new(tmp.path());
        let resolved = resolver.resolve(&sh("./plugins/libfoo.so")).unwrap();
        assert_eq!(resolved, plugin_file);
    }

    #[test]
    fn resolve_absolute_local_path_passes_through() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let plugin_file = tmp.path().join("libbar.so");
        std::fs::write(&plugin_file, b"x").unwrap();

        // Anchor resolver somewhere unrelated — absolute paths win.
        let resolver = PluginResolver::new("/nonexistent/base");
        let resolved = resolver
            .resolve(&sh(plugin_file.to_str().unwrap()))
            .unwrap();
        assert_eq!(resolved, plugin_file);
    }

    #[test]
    fn resolve_missing_local_path_errors_with_full_paths() {
        let resolver = PluginResolver::new("/some/base");
        let err = resolver.resolve(&sh("./does-not-exist.so")).unwrap_err();
        match err {
            ResolverError::LocalFileNotFound(path, base) => {
                assert_eq!(path, std::path::Path::new("/some/base/./does-not-exist.so"));
                assert_eq!(base, std::path::Path::new("/some/base"));
            }
            other => panic!("expected LocalFileNotFound, got {other:?}"),
        }
    }

    #[test]
    fn resolve_remote_spec_errors_with_phase_1b_hint() {
        let resolver = PluginResolver::new("/some/base");
        let err = resolver.resolve(&sh("echo-python-loadable")).unwrap_err();
        match err {
            ResolverError::NetworkDownloadNotYetSupported(msg) => {
                assert!(
                    msg.contains("echo-python-loadable"),
                    "error should reference the offending spec, got: {msg}"
                );
            }
            other => panic!("expected NetworkDownloadNotYetSupported, got {other:?}"),
        }
    }

    #[test]
    fn resolve_all_collects_in_order_stopping_at_first_error() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let plugin_file = tmp.path().join("libfoo.so");
        std::fs::write(&plugin_file, b"x").unwrap();

        let resolver = PluginResolver::new(tmp.path());

        // Two-element list where the second errors → expect Err.
        let specs = vec![sh("./libfoo.so"), sh("./missing.so")];
        assert!(resolver.resolve_all(&specs).is_err());

        // Single valid → expect Ok with the resolved path.
        let ok = resolver.resolve_all(&[sh("./libfoo.so")]).unwrap();
        assert_eq!(ok, vec![plugin_file]);
    }

    // ===== Phase 1B (network) tests =====

    #[test]
    fn current_platform_returns_arch_dash_os_shape() {
        let p = current_platform();
        // Must be `{something}-{something}` — exact value depends on host.
        let parts: Vec<&str> = p.split('-').collect();
        assert_eq!(parts.len(), 2, "expected `arch-os`, got {p:?}");
        // The shape should match what plugin authors write in
        // release-manifest.json.
        assert!(!parts[0].is_empty());
        assert!(!parts[1].is_empty());
    }

    #[test]
    fn release_manifest_serde_round_trip() {
        let manifest_json = r#"{
            "name": "echo-python-loadable",
            "version": "v0.3",
            "platforms": {
                "x86_64-linux":  { "file": "libecho-x86_64-linux.so",   "sha256": "deadbeef" },
                "aarch64-linux": { "file": "libecho-aarch64-linux.so",  "sha256": "cafebabe" }
            }
        }"#;
        let parsed: ReleaseManifest = serde_json::from_str(manifest_json).unwrap();
        assert_eq!(parsed.name, "echo-python-loadable");
        assert_eq!(parsed.version, "v0.3");
        assert_eq!(parsed.platforms.len(), 2);
        assert_eq!(
            parsed.platforms["x86_64-linux"].file,
            "libecho-x86_64-linux.so"
        );
        assert_eq!(parsed.platforms["x86_64-linux"].sha256, "deadbeef");

        // Re-serialize and re-parse to verify the schema round-trips.
        let reserialized = serde_json::to_string(&parsed).unwrap();
        let reparsed: ReleaseManifest = serde_json::from_str(&reserialized).unwrap();
        assert_eq!(reparsed.platforms.len(), 2);
    }

    #[test]
    fn release_manifest_ignores_unknown_extra_fields() {
        // Plugin authors might add `author`, `license`, `repository`,
        // etc. — the resolver should not break on those.
        let manifest_json = r#"{
            "name": "foo",
            "version": "v1",
            "platforms": {
                "x86_64-linux": { "file": "lib.so", "sha256": "abc" }
            },
            "author": "Jane Doe",
            "license": "MIT",
            "repository": "github.com/jane/foo"
        }"#;
        let parsed: ReleaseManifest = serde_json::from_str(manifest_json).unwrap();
        assert_eq!(parsed.name, "foo");
    }

    #[test]
    fn sha256_hex_matches_known_vector() {
        // SHA256("hello") = 2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824
        assert_eq!(
            sha256_hex(b"hello"),
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }

    #[test]
    fn file_sha256_matches_in_memory_hash() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let f = tmp.path().join("blob.bin");
        std::fs::write(&f, b"the quick brown fox").unwrap();
        let from_file = file_sha256(&f).unwrap();
        let from_bytes = sha256_hex(b"the quick brown fox");
        assert_eq!(from_file, from_bytes);
    }

    #[test]
    fn sha256_of_spec_returns_sha_from_explicit_form() {
        let spec = PluginSpec::Explicit(PluginSpecExplicit {
            url: Some("https://example.com/foo.so".into()),
            sha256: Some("abc123".into()),
            ..Default::default()
        });
        assert_eq!(sha256_of_spec(&spec), Some("abc123".into()));
    }

    #[test]
    fn sha256_of_spec_returns_none_for_shorthand() {
        let spec = PluginSpec::Shorthand("foo".into());
        assert_eq!(sha256_of_spec(&spec), None);
    }

    #[test]
    fn cache_dir_anchored_at_base_dir() {
        let resolver = PluginResolver::new("/my/project");
        assert_eq!(
            resolver.cache_dir(),
            std::path::PathBuf::from("/my/project/remotemedia-plugins/cache")
        );
    }

    #[tokio::test]
    async fn resolve_async_handles_local_path_same_as_sync() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let plugin_file = tmp.path().join("libfoo.so");
        std::fs::write(&plugin_file, b"x").unwrap();

        let resolver = PluginResolver::new(tmp.path());
        let sync_result = resolver.resolve(&sh("./libfoo.so")).unwrap();
        let async_result = resolver.resolve_async(&sh("./libfoo.so")).await.unwrap();
        // Async returns ResolvedPlugin::Cdylib { path } — extract for comparison.
        match async_result {
            ResolvedPlugin::Cdylib { path } => assert_eq!(sync_result, path),
            other => panic!("expected Cdylib for local path, got {other:?}"),
        }
    }

    /// Integration: hits real github.com to look up a known-bad
    /// repo/tag combination, exercises the HTTP path end-to-end without
    /// requiring a published plugin. Surfaces a precise error rather
    /// than the generic NetworkDownloadNotYetSupported the sync path
    /// would emit. `#[ignore]`-gated so CI doesn't depend on network.
    ///
    /// Run manually:
    ///   cargo test -p remotemedia-core --lib \
    ///     loadable::resolver::tests::resolve_async_real_github_404 -- --ignored
    #[tokio::test]
    #[ignore = "hits real github.com — run manually with --ignored"]
    async fn resolve_async_real_github_404() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let resolver = PluginResolver::new(tmp.path());
        let err = resolver
            .resolve_async(&sh(
                "github.com/RemoteMedia-SDK/this-plugin-does-not-exist-zzzz@v999.999.999",
            ))
            .await
            .unwrap_err();
        // Either HttpStatus 404 (tag-specific lookup) or the asset 404
        // — both are acceptable "bad coordinate" signals. With Phase
        // 1C, plugin.toml is also fetched first and may return 404
        // separately from the release-manifest; both 404s surface as
        // HttpStatus errors.
        match err {
            ResolverError::Http { .. } | ResolverError::HttpStatus { .. } => {}
            other => panic!("expected Http error for nonexistent tag, got {other:?}"),
        }
    }
}
