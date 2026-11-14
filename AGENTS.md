# RemoteMedia SDK Base

## Scope and ownership

This repository owns the public execution engine. The root package is `remotemedia-sdk-base`, imported in Rust as `remotemedia_sdk_base`; consumers may alias it to `remotemedia-core`. The SDK facade in `../remotemedia-sdk/crates/core/` re-exports this implementation.

| Area | Location |
| --- | --- |
| Public modules, re-exports and errors | `src/lib.rs`, `src/error.rs` |
| Manifest parsing and validation | `src/manifest.rs`, `src/validation/` |
| Pipeline execution and scheduling | `src/executor/` |
| Sessions, routing, control and lifecycle | `src/transport/` |
| Host-side plugin resolution and adapters | `src/loadable/` |
| Multiprocess execution and Python integration | `src/multiprocess/`, `src/python/`, `crates/python-nodes/` |
| Built-in nodes, audio I/O and real-time bridge | `src/nodes/`, `src/audio_io/`, `src/rt_bridge/` |
| gRPC, WebRTC, HTTP, telephony and FFI | `crates/transports/` |
| Server entry points | `crates/services/` |
| Bundles and deployment metadata | `crates/bundle/`, `crates/bundle-deployment/` |
| Android integration | `crates/android/`, `kotlin-android/` |

Shared payload types, node traits, procedural macros, loadable ABI and shared-memory primitives belong in `../remotemedia-plugin-sdk/`. Distributed placement and runner orchestration belong in `../remotemedia-scaling/`. Read the sibling's `AGENTS.md` before changing that code. Keep transport-specific behavior in the transport crates rather than introducing transport dependencies into the engine.

The SDK repository also has `crates/transports/ffi/` with the same package name, `remotemedia-ffi`. Identify the consumer and manifest being built before applying an FFI fix; do not assume the two copies are interchangeable.

## Build context

- Root `Cargo.toml` declares Rust 1.88, edition 2021, and empty default features for the base package. It is both a package and a workspace: plain `cargo test` selects the root package, not all transports.
- Cargo patches resolve plugin SDK crates from `../remotemedia-plugin-sdk/`. Keep related trait/type dependencies on the same resolved crate instance when changing both repositories.
- Feature gates distinguish `multiprocess`, `loadable`, `native-worker`, `mapped-python`, `python-source-plugin`, and `inprocess-python`. Read the manifest and relevant `cfg` gates; these execution paths are not interchangeable.
- Native dependencies vary by feature and target. `.github/actions/setup/action.yml` records Linux CI prerequisites; `.cargo/config.toml` contains machine-specific library paths. Device, video and Python features may need additional installed libraries or environments.
- Android is excluded from the main workspace. Build and validate it through its own manifest and Kotlin project when it is in scope; a desktop workspace check does not cover it.

## Validation

Run commands from this repository. Use focused tests for the affected subsystem, then the relevant CI configuration from `.github/workflows/rust.yml`:

```sh
cargo fmt --all -- --check
cargo check -p remotemedia-grpc -p remotemedia-webrtc --all-targets
cargo check -p remotemedia-grpc -p remotemedia-webrtc --all-targets --features remotemedia-grpc/multiprocess,remotemedia-webrtc/multiprocess
cargo test -p remotemedia-sdk-base -p remotemedia-grpc -p remotemedia-webrtc --lib --bins --tests --no-fail-fast
cargo test -p remotemedia-sdk-base -p remotemedia-grpc -p remotemedia-webrtc --doc --no-fail-fast
```

CI selects these packages together to unify features as a real host does. This does not cover every workspace member or every optional execution path; add the affected package and features explicitly.

- Routing/control/lifecycle changes: inspect the unit tests beside `src/transport/session_router.rs`, the `src/transport/session_router/` tests, and the relevant transport's tests. Preserve session/participant identity, ordered completion, cancellation and shutdown behavior.
- Loadable ABI or RPC changes: run the real host/plugin fixture in `integration/session-rpc/` with its required DLL configured. The Windows orchestrator `../remotemedia-plugin-sdk/scripts/test-session-rpc.ps1` builds the fixtures and runs the cross-repository checks; it also needs Node.js and, when the client checkout is present, Python test dependencies.
- Shared-storage changes: use `integration/shared-storage/` and the corresponding plugin SDK fixtures. Preserve ownership, lifetime and copy-accounting contracts across boundaries.
- Native worker or Python changes: validate the execution mode actually changed, including process startup, output draining, failure and cleanup. A test using another mode is not equivalent coverage.
- Hardware, model, network and credential-dependent tests need their prerequisites. State what ran, what was skipped and why; do not equate unit tests with live transport or device validation.

## Specifications

Inspect the local `openspec/changes/` and any related SDK proposal before feature, protocol or architecture changes. Cross-repository changes should name the owning repositories and their validation tasks. When the SDK checkout is available, use [its OpenSpec workflow](../remotemedia-sdk/openspec/AGENTS.md); run OpenSpec commands from the repository containing the change. Existing approval remains valid for the approved scope. Documentation corrections and fixes restoring intended behavior can proceed directly.

Treat old SDK documents as design context. Verify implementation claims against this repository's source and manifests, especially when a document still points into the SDK facade.
