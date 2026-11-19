# Implementation and validation

The WebSocket signaling server supports an explicit bind IP for the protected Spaces control topology. Its established default remains available to existing callers. The rebuilt Windows Scaling Runner used loopback room signaling with the shared control integration fixture. [Spaces verification](../../../../huggingface-spaces-runner/openspec/changes/isolate-space-workloads/implementation.md)

`ContentStore::stage_verified_blob` copies the exact declared byte count into a new staging file and hashes the bytes while copying. Corrupt and truncated copies are discarded. Upload completion and native runtime materialization reuse the streaming verifier. A staging directory is trusted and remains private until its caller validates the whole artifact and publishes or mounts it.

The focused Windows test verified that a same-length replacement or truncated blob is refused and that an existing staged file remains intact. The full Windows bundle-deployment suite exposed parent-directory flush support and path-format assertions in the existing publishing and native release code. The full Linux bundle-deployment library suite passed all 23 tests in Ubuntu 24.04 under WSL.

## Protected execution handoff

Scaling hands SDK Base a host-created `ProtectedSpaceApproval` after authenticating Entry and checking the worker's current approval file. `PipelineExecutor::verify_protected_space` checks expiry, exact manifest bytes and the phase-specific controller shape. `VerifiedSpaceExecution` retains the approval and worker-configured controller path independently of the parsed manifest. The controller shape excludes manifest-selected Python environments, source checkout paths, alternate plugin inputs, host placement and extra workload parameters.

An executor created with `for_protected_spaces` denies ordinary plugin resolution, ordinary session creation and warm-pool construction before provisioning. The protected Runner creates this executor at startup. Verified admission currently ends before controller or workload startup. Controller release and environment verification, authorized artifact registration, immutable artifact mounting, provider integration, and workload endpoint lifecycle remain open.

The Windows build with `loadable,python-source-plugin` passed three protected execution tests and the three existing source-Python wire tests. The protected tests cover both phases, expired or changed requests, environment and plugin overrides with matching digests, and refusal before ordinary loaders or warm sessions are called. Scaling exercised the SDK Base verifier over the real mutual TLS listener. [Scaling verification](../../../../remotemedia-scaling/openspec/changes/isolate-space-workloads/implementation.md)

## Artifact registration and staging

`SpaceArtifactRegistry` uses an operator-owned directory with a dedicated content store, atomic registration records, and separate per-run staging directories. The trusted publisher imports bounded streams by SHA-256 and byte count and registers the exact `workload-artifact.json` and `source-tree.json` bytes against the launch scope. Registration checks repository, commit, target, Python ABI, policy, paths and closure before publishing. Store the directory outside workload access and restrict its ACLs or permissions to the trusted processes.

An acquisition rechecks the registration and copies the exact verified source, wheels, and model files into an independent lease. Active leases keep their copied bytes if the content store changes. Failed and normally released leases are removed. The provider must retain the lease through confirmed teardown and expose its inputs through read-only mounts. Startup reconciliation and automatic publication from Prepare remain open.

All 26 Linux bundle-deployment library tests and all three focused Windows registry tests passed. A Windows fixture generated the records with the Spaces Python worker code, including a Unicode source filename, and imported and leased those bytes through the Rust registry. The live Runner denied the serve request before the registration and reached the runtime availability check after publication.
