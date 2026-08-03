use crate::manifest::{ModelSourceFile, ResolvedModelAssets, SourceKind};
use anyhow::{Context, Result};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct HfModelDownloader {
    cache_root: PathBuf,
}

impl Default for HfModelDownloader {
    fn default() -> Self {
        Self {
            cache_root: default_cache_root(),
        }
    }
}

impl HfModelDownloader {
    pub fn new(cache_root: impl Into<PathBuf>) -> Self {
        Self {
            cache_root: cache_root.into(),
        }
    }

    pub fn set_cache_root(&mut self, root: impl Into<PathBuf>) {
        self.cache_root = root.into();
    }

    /// Ensure every declared file in `model_sources` is present locally.
    /// HuggingFace assets must include an explicit download URL in metadata.
    pub fn ensure_assets(&self, assets: &ResolvedModelAssets) -> Result<Vec<PathBuf>> {
        let mut resolved = Vec::new();
        for asset in &assets.assets {
            if asset.exists {
                resolved.push(
                    asset
                        .resolved_path
                        .clone()
                        .unwrap_or_else(|| PathBuf::from(&asset.path)),
                );
                continue;
            }
            if !asset.required {
                continue;
            }
            let Some(url) = &asset.url else {
                anyhow::bail!(
                    "Missing download URL for required asset: {} (node={})",
                    asset.filename,
                    asset.node_id
                );
            };
            let dest = resolve_dest(self, asset);
            std::fs::create_dir_all(dest.parent().unwrap_or(Path::new("/")))?;
            download_file(url, &dest)?;
            resolved.push(dest);
        }
        Ok(resolved)
    }
}

fn resolve_dest(
    downloader: &HfModelDownloader,
    asset: &crate::manifest::ResolvedModelAsset,
) -> PathBuf {
    if asset.path.is_empty() {
        downloader
            .cache_root
            .join(&asset.node_id)
            .join(&asset.filename)
    } else {
        Path::new(&asset.path)
            .parent()
            .map(PathBuf::from)
            .unwrap_or_else(|| downloader.cache_root.join(&asset.node_id))
            .join(&asset.filename)
    }
}

fn download_file(url: &str, dest: &Path) -> Result<()> {
    let client = reqwest::blocking::Client::builder()
        .user_agent("remotemedia-hf-downloader")
        .build()?;
    let mut resp = client.get(url).send()?;
    if !resp.status().is_success() {
        anyhow::bail!("Download failed: {} {}", resp.status(), url);
    }
    let tmp = dest.with_extension("tmp");
    let mut f = std::fs::File::create(&tmp)?;
    resp.copy_to(&mut f)?;
    std::fs::rename(&tmp, dest)?;
    Ok(())
}

fn default_cache_root() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".cache")
        .join("remotemedia")
        .join("models")
}
