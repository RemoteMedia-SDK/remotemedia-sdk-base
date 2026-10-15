# Capability: manifest-model-metadata

Requirement: each node in a pipeline manifest can declare the model files it expects, and what retrieval hints apply to those files, without breaking manifests that omit this metadata.

REQ-1 An optional `model_sources` object MAY be present inside a node's `params`.
REQ-2 When present, `model_sources.files` MUST be an array of objects.
REQ-3 Each entry in `files` MUST include:
REQ-3a `path` — the filesystem path the node expects at runtime.
REQ-3b `source` — one of `huggingface`, `http`, `local`.
REQ-3c `filename` — the file name used when fetching/caching.
REQ-4 Each entry MAY include `required` (default true). When false, missing files MUST NOT block session creation by this analyzer.
REQ-5 Each entry MAY include `sha256` (hex, lowercase) for cache verification.
REQ-6 Unknown fields outside `path`, `source`, `filename`, `required`, `sha256` MUST be ignored so metadata can evolve.
REQ-7 `NodeManifest::params` remains `serde_json::Value`; no manifest schema change is required at the top level.
REQ-8 Nodes whose `params["model_sources"]` is missing or malformed MUST behave as though they declared no metadata, not as a hard error.
