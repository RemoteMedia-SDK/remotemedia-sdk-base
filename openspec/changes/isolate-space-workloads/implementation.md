# Implementation and validation

The WebSocket signaling server supports an explicit bind IP for the protected Spaces control topology. Its established default remains available to existing callers. The rebuilt Windows Scaling Runner used loopback room signaling with the shared control integration fixture. [Spaces verification](../../../../huggingface-spaces-runner/openspec/changes/isolate-space-workloads/implementation.md)

`ContentStore::stage_verified_blob` copies the exact declared byte count into a new staging file and hashes the bytes while copying. Corrupt and truncated copies are discarded. Upload completion and native runtime materialization reuse the streaming verifier. A staging directory is trusted and remains private until its caller validates the whole artifact and publishes or mounts it.

The focused Windows test verified that a same-length replacement or truncated blob is refused and that an existing staged file remains intact. The full Windows bundle-deployment suite exposed parent-directory flush support and path-format assertions in the existing publishing and native release code. The full Linux bundle-deployment library suite passed all 23 tests in Ubuntu 24.04 under WSL.

## Protected execution handoff

Scaling hands SDK Base a host-created `ProtectedSpaceApproval` after authenticating Entry and checking the worker's current approval file. `PipelineExecutor::verify_protected_space` checks expiry, exact manifest bytes and the phase-specific controller shape. `VerifiedSpaceExecution` retains the approval and worker-configured controller path independently of the parsed manifest. The controller shape excludes manifest-selected Python environments, source checkout paths, alternate plugin inputs, host placement and extra workload parameters.

An executor created with `for_protected_spaces` denies ordinary plugin resolution, ordinary session creation and warm-pool construction before provisioning. The protected Runner creates this executor at startup. Its registration handler still returns runtime unavailable after verified admission. The Linux CPU handoff below is available to the trusted host for integration with that handler.

The Windows build with `loadable,python-source-plugin` passed three protected execution tests and the three existing source-Python wire tests. The protected tests cover both phases, expired or changed requests, environment and plugin overrides with matching digests, and refusal before ordinary loaders or warm sessions are called. Scaling exercised the SDK Base verifier over the real mutual TLS listener. [Scaling verification](../../../../remotemedia-scaling/openspec/changes/isolate-space-workloads/implementation.md)

## Artifact registration and staging

`SpaceArtifactRegistry` uses an operator-owned directory with a dedicated content store, atomic registration records, and separate per-run staging directories. The trusted publisher imports bounded streams by SHA-256 and byte count and registers the exact `workload-artifact.json` and `source-tree.json` bytes against the launch scope. Registration checks repository, commit, target, Python ABI, policy, paths and closure before publishing. Store the directory outside workload access and restrict its ACLs or permissions to the trusted processes.

An acquisition rechecks the registration and copies the exact verified source, wheels, and model files into an independent lease. Active leases keep their copied bytes if the content store changes. Failed and normally released leases are removed. The provider must retain the lease through confirmed teardown and expose its inputs through read-only mounts. Startup reconciliation remains open.

On Unix, the lease root stays private at `0700`. The source, wheel and model directory trees use `0755` so a different guest UID can traverse them after each tree is mounted read-only. Files are readable and retain only their verified execute bits. The four focused Linux artifact tests passed with worker umask `077`, including exact mode checks for the lease root, nested trees and files.

`receive_publication` accepts a bounded header and streams the closure in inventory order. It verifies and imports every file before returning its readiness byte. The worker rechecks launch policy, then sends a commit byte and closes the stream. The registry installs the record atomically after receiving both. A denied or interrupted publication may retain verified store blobs for reuse. Registration stays absent until the commit succeeds.

All 27 Linux bundle-deployment library tests and four focused Windows registry tests passed. A Windows fixture generated the records with the Spaces Python worker code, including a Unicode source filename, and published those bytes through the native Runner subcommand. The live Runner denied the serve request before registration and reached the runtime availability check after publication.

## Isolated Linux CPU handoff

The `protected-space-runsc` feature stages a registered closure directly into a gVisor reservation. SDK Base derives the registration scope from the verified serve approval. It checks the staged Python ABI against the operator's guest image configuration and reads the SDK and entry file from the verified artifact. The operator configures the trusted launcher in that image. It receives the SDK, the absolute guest entry path, and the loopback port as arguments.

The caller must present current verified admission when starting a staged run. The launch and gateway stream capability both match the entire original approval, including the run, owner, policy, manifest and artifact digest. The provider bounds the workload lifetime by the earlier of the configured limit and the approval expiry and retains the artifact through teardown. Scaling still needs to revalidate worker policy and lease ownership before launch, authorize gateway requests, and stop affected runs on policy revocation.

The Linux integration fixture used pinned gVisor on WSL2 with worker `umask 077` and registered a synthetic source, wheel and model closure. It refused an unregistered policy, an incompatible Python ABI, and a mismatched run at launch and relay open. The matching grant started a guest that confirmed all three byte sequences. Stopping the run closed the stream and cleared the sandbox reservation. The three protected manifest and admission regression tests also passed. The integration test uses a trusted C launcher and exercises the SDK Base boundary. Actual Python environment installation and a real Gradio response through Scaling and the Spaces gateway remain release gates.

For the Linux fixture, set `RM_SPACE_TEST_RUNSC` to the pinned runtime and `RM_SPACE_TEST_CGROUP` to an exclusive delegated test subtree with the required controllers enabled. With a static C toolchain available, run `cargo test -p remotemedia-sdk-base --features protected-space-runsc --lib registered_lease_launches_in_run_bound_sandbox -- --ignored`.
