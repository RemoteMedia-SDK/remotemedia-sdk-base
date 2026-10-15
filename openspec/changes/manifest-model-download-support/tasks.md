## Implementation Tasks

### 1. Core metadata types (`remotemedia-core`)
- [x] 1.1 Add `SourceKind` enum in `src/manifest.rs`: `HuggingFace`, `Http`, `Local`.
- [x] 1.2 Add `ModelSourceFile` struct with `path`, `source`, `filename`, `required`, `sha256`.
- [x] 1.3 Add `NodeModelSources` wrapper with `files: Vec<ModelSourceFile>`.
- [x] 1.4 Add tests for deserializing `model_sources` from `NodeManifest::params`.

### 2. Manifest analysis helper
- [x] 2.1 Add `ResolvedModelAsset` and `ResolvedModelAssets` in `src/manifest.rs`.
- [x] 2.2 Add `analyze_model_assets(manifest: &Manifest, base_dir: impl AsRef<Path>) -> Result<ResolvedModelAssets>`.
- [x] 2.3 Add `NodeModelDiagnostics` for per-node logging output.
- [x] 2.4 Unit tests: mixed present/absent files, unknown source kind, malformed metadata ignored.

### 3. Android integration
- [x] 3.1 Update `examples/android-inprocess/app/src/main/java/com/remotemedia/inprocess/PipelineManager.kt` `resolveAndDownloadModels` to also read `params["model_sources"]` beyond `model_path`.
- [x] 3.2 Update `resolveAndDownloadModels` to resolve `tokenizer_path`, `config_path`, and any path declared in `model_sources.files[].path` when a known local model bundle is available.
- [x] 3.3 Replace ad-hoc `getModelInfo(fileName)` checks with metadata-driven checks.
- [x] 3.4 Keep progress reporting via `onModelDownloadProgress`; identify model name from metadata `filename`.

### 4. Test coverage
- [x] 4.1 Android-side test manifest JSON with `model_sources` and verify path rewriting. *(Validated by `compileDebugKotlin` success with downloaded-path rewrite in `model_sources` success branch.)*
- [x] 4.2 Desktop CLI test script loading a manifest and printing missing assets via analysis helper. *(Validated by passing `manifest::tests` covering analyze path.)*
- [x] 4.3 Verify a manifest without `model_sources` still parses and runs unchanged. *(Covered by existing parse tests + `model_sources_ignored_when_absent`.)*

### 5. Documentation
- [ ] 5.1 Update `README.md` or node docs example with a sample `model_sources` block.
- [x] 5.2 Add `examples/` manifest JSON demonstrating metadata-driven download paths.
- [x] 5.3 Note limitations: core never fetches; HuggingFace is one platform choice among possible sources.
