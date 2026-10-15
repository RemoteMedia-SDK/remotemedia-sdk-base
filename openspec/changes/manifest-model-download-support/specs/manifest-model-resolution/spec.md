# Capability: manifest-model-resolution

Requirement: the SDK provides a manifest-level analysis helper that tells the platform which files already exist and which are missing, so the platform can decide when and how to fetch.

REQ-1 The SDK MUST provide a function that accepts a resolved `Manifest` and a base directory for model files.
REQ-2 The function MUST return `ResolvedModelAssets` describing files referenced by `model_sources` in the manifest's nodes.
REQ-3 For each referenced file, `ResolvedModelAssets` MUST record whether the file exists at the supplied base dir/path.
REQ-4 The function MUST NOT perform network I/O or file downloads.
REQ-5 For files that do not exist, the function MUST include the node id, declared path, filename, source kind, and optional expected `sha256`.
REQ-6 The helper MUST not validate that `params["model_path"]` equals `params["model_sources"].files[].path`, but a future validator MAY make that connection.
REQ-7 A separate optional helper MAY return node-level diagnostics suitable for logging (`NodeModelDiagnostics`).
REQ-8 Platform layers MUST remain responsible for any auth, storage location choices, and progress reporting.
