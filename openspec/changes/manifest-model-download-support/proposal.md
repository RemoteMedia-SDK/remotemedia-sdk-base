## Why

Nodes frequently depend on external model files hosted remotely (e.g. Hugging Face). On Android, APK assets are read-only and storage-permission-restricted, so shipping every model inside the bundle is impractical. Today `remotemedia-litert-nodes/src/whisper.rs` shows the right node-side contract — `model_path` / `tokenizer_path` / `config_path` — but there is no standard way for the platform layer to discover *which* paths are resumable, *what* source they came from, or *whether* they need a fetch before the native executor sees the manifest.

We need a minimal metadata contract on `NodeManifest`/node configs so the platform (`PipelineManager.kt` on Android, CLI on desktop) can resolve and download models before session creation, without baking network logic or Hugging Face policy into the SDK runtime.

## What Changes

- Add an optional `model_sources` block to `NodeManifest.params` so a node can declare each file it downloads, its source kind, and retrieval hints.
- Add `ModelSource` / `ModelSourceFile` Rust types in `remotemedia-core` (`src/manifest.rs`) describing downloadable assets.
- Add a resolution helper on the Rust side that enumerates missing files from the current manifest's node configs. The helper remains source-agnostic; it does not fetch, it only reports what is absent.
- Android `PipelineManager` already has `ModelDownloader` and `resolveAndDownloadModels`; update it to inspect the standard metadata and fill `model_path`/`tokenizer_path` before calling native.
- HuggingFace remains a platform-choice dependency (hf-hub on Rust native, Python hf_hub CLI/script on Android Gradle side, etc.). The SDK never directly depends on HF network behavior.
- Precedent already exists in `candle-nodes/src/cache.rs` (`download_model`/`download_model_sync`) and `remotemedia-litert-nodes/src/whisper.rs` (`model_path` config fields). This change aligns those patterns under a shared contract.

## Capabilities

### New Capabilities

- `manifest-model-metadata`: Declare downloadable model assets in `NodeManifest` and expose a structured Rust representation (`ModelSource`, `ModelSourceFile`, `SourceKind`).

- `manifest-model-resolution`: Surface "missing file" analysis over a `Manifest` so the platform layer knows what to fetch without making network decisions.
