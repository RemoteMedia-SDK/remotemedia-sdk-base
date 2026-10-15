## Context

The current state: built-in Whisper in `remotemedia-litert-nodes/src/whisper.rs` already models its model files as `model_path`, `tokenizer_path`, `config_path`. Android's `PipelineManager` already has a `ModelDownloader` plus a `resolveAndDownloadModels` path-overwrite pass. The only missing link is a contract that says: "this node expects these specific files, this is how they map, and this is what needs fetching if absent."

The platform divergences matter. On Android, apps ship read-only APK assets, need scoped `filesDir`, cannot freely use tokio/reqwest the same way desktop does, and must surface progress on the main thread. The SDK core should not make assumptions about valid network paths, token handling, or storage locations.

This design keeps platform-specific downloading in the platform layer (Android Kotlin today) and provides just one thing from core: structured manifest accessibility metadata that any platform can interpret.

## Goals / Non-Goals

Goals:
- Declare downloadable model assets declaratively in `NodeManifest.params` without a new top-level manifest section.
- Provide typed Rust types to read/decode those declarations.
- Provide a pure analysis helper that reports missing files across current manifest nodes; no fetching, no runtime I/O.
- Allow Android `PipelineManager` to drive downloads using existing `ModelDownloader` and progress callbacks.
- Keep HuggingFace as a platform implementation choice, never a core dependency.

Non-Goals:
- Downloading models from `remotemedia-core` Rust code.
- Enforcing any HuggingFace-specific API/token policy in the SDK.
- Cache invalidation or multi-source merge semantics.
- Replacing node-level runtime download hooks (nodes may still download on their own if they choose).

## Decisions

1. Metadata lives in `NodeManifest.params["model_sources"]`
Decision: extend `NodeManifest.params` JSON with an optional `model_sources` object rather than adding a dedicated manifest field.
Rationale:
- Minimal surface area; existing manifests without `model_sources` remain valid.
- Nodes already own their paths; adding metadata at that level matches the whisper precedent.
- Avoids contaminating `Manifest` with platform concepts.

Alternatives considered:
- Top-level `Manifest::models` list: rejected because it fragments ownership, which node needs what.

2. Rust types are source-agnostic
Decision: represent model sources as `ModelSourceFile { path, source, required, filename }` with `SourceKind` enum.
Rationale:
- HuggingFace is one source among possible targets (S3, local tarball, etc.).
- Keeps `remotemedia-core` free of HuggingFace token/pin logic.
Alternatives considered:
- Adding `hf-hub` to `remotemedia-core`: rejected because Spark/core crate stays platform-agnostic.

3. Core provides an analysis helper, not a fetcher
Decision: add a manifest-level method that iterates `nodes[].params["model_sources"]` and returns missing paths from a given base dir.
Rationale:
- Calling code (Android/CLI) decides storage layout and download strategy.
- Missing-file enumeration is cheap, testable, and platform-independent.

Alternatives considered:
- Embedding download logic in Rust: rejected due to Android/toolchain differences noted above.

4. Android resolves before native session creation
Decision: `PipelineManager.ensureModelsReady()` runs the existing `resolveAndDownloadModels` against `model_path` (and optional `tokenizer_path`). Native layer still validates via node `initialize()`.
Rationale:
- Preserves current fail-fast behavior.
- Keeps progress callbacks already implemented (`onModelDownloadProgress`).
- Matches existing API at `loadManifest` → `ensureModelsReady` → `startStreaming`/`executeUnary`.

Alternatives considered:
- Doing resolution in Rust via JNI callback: rejected; too much platform coupling in core.

5. Existing node-level Rust downloaders remain untouched
Decision: do not refactor `candle-nodes` Rust downloader now. The manifest metadata is additive.
Rationale:
- Prevents churn in working code.
- Leaves room to align later without blocking this change.

## Risks / Trade-offs

- [Metadata drift between node impl and manifest metadata] -> Mitigation: add lightweight validation in node `initialize()` that verifies required files exist, logging a clear error when access/read/write checks fail.
- [Multiple node types inventing slightly different keys] -> Mitigation: lock the `model_sources` schema in the spec and add tests that parse sample manifests.
- [Platform fragmentation: 3 platforms, 3 downloaders] -> Mitigation: callers share only the missing-file report; invocation and fetching remain local to each platform.
- [HuggingFace auth/pin handling becomes implicit on Android] -> Mitigation: expose the metadata as informational only; actual auth and pin logic stays in `ModelDownloader`.

## Migration Plan

1. Introduce typed Rust metadata and analysis helper; existing manifests continue to parse unchanged.
2. Add Android-side metadata consumption using current PipelineManager flow; keep local-path fallback if metadata is missing.
3. Test using bundled APK models first (already extracted), then a CI job that downloads a small public model via the new contract.
4. Rollback: drop the analysis calls in `PipelineManager` and revert to the current manual path behavior if needed.

## Open Questions

- Do we want the metadata to express optional source fallback order (primary then fallback), or is a single source per file enough?
- Should failure policy be "fail the session" vs "skip node" for models that are neither cached nor fetchable?
- Should `ModelDownloader` metadata live in Kotlin only, or be shared with Rust via a generated contract?
