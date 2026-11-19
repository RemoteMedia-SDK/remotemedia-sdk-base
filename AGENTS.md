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

### Behaviour over a transport: `remotemedia test`

To find out whether a node, a session path or a transport does what it should, as seen by a client, run a scenario with `remotemedia test` (the CLI in `../remotemedia-sdk/examples/cli/remotemedia-cli/`). Do not write a one-off client, server or measurement script for that, and do not grep logs: a scenario is a few lines of YAML, runs the same check over in-process, gRPC, HTTP, WebRTC and SIP, and gives a verdict with the measured values.

```sh
# from ../remotemedia-sdk, built against this checkout (its README, "Sibling repositories")
remotemedia test examples/cli/remotemedia-cli/tests/scenarios --transport inproc,grpc,http,webrtc,sip
```

Exit codes: `0` passed, `1` failed, `2` invalid scenario, `3` unsupported or nothing ran; `2` and `3` mean "not verified". The scenario format, the assertions and what each transport carries are in `../remotemedia-sdk/examples/cli/remotemedia-cli/tests/scenarios/README.md`, and the SDK's `AGENTS.md` ("Checking that something works") has the rule in full. The SDK must be built against this checkout for a change here to be what is tested: the rev it pins is not your working tree.

What this repository provides for it:

- `remotemedia_webrtc::peer_client::PeerClient` (feature `ws-signaling`): a real WebRTC member in Rust. Use it in this crate's tests instead of hand-built peer connections or placeholder RTP payloads (`crates/transports/webrtc/src/peer_client_tests.rs` shows a server and a client in one test).
- Model-free nodes: `PassThrough`, `MediaKindFilterNode`, `TestToneTtsNode` (text to a tone), `TestPatternVideoNode` (any input to a video frame), `TestEnergyVadNode`, and `TestInputProbeNode` (a JSON description of every input, for checking what a transport delivers into a pipeline).
- `peer.stats` on WebSocket signalling: the calling member's own counters (frames encoded, frames not encoded, keyframes requested). Read counters there, not from log lines.

When a check needs something the harness cannot observe, add it here (a counter, a field on the wire, a test node) and the assertion in the SDK in the same change, and say so in your report. On an FFmpeg build with neither libx264 nor NVENC, H.264 falls back to Media Foundation, which holds back 16 frames before its first packet: a test must not assume a packet per frame (`h264_encoder_emits_a_packet_for_the_first_frame`, ignored, measures it).

## Committing progress

Commit development progress while implementing a task or an OpenSpec change; do not leave the whole result uncommitted until the end. Commit on the working feature branch, not the default branch, at each verified checkpoint: a completed task or coherent group of tasks that builds and passes its focused checks. Include the matching `tasks.md` checkbox updates in the same commit, and name the change ID and task numbers in the message when an OpenSpec change applies. Stage only the files the task touched; leave unrelated or pre-existing changes out. If partial or failing work must be saved, mark it as work in progress in the message and keep its tasks unchecked.

## Specifications

Inspect the local `openspec/changes/` and any related SDK proposal before feature, protocol or architecture changes. Cross-repository changes should name the owning repositories and their validation tasks. When the SDK checkout is available, use [its OpenSpec workflow](../remotemedia-sdk/openspec/AGENTS.md); run OpenSpec commands from the repository containing the change. Existing approval remains valid for the approved scope. Documentation corrections and fixes restoring intended behavior can proceed directly.

Treat old SDK documents as design context. Verify implementation claims against this repository's source and manifests, especially when a document still points into the SDK facade.
