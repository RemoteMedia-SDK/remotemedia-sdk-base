# Implementation and validation

The WebSocket signaling server supports an explicit bind IP for the protected Spaces control topology. Its established default remains available to existing callers. The rebuilt Windows Scaling Runner used loopback room signaling with the shared control integration fixture. [Spaces verification](../../../../huggingface-spaces-runner/openspec/changes/isolate-space-workloads/implementation.md)

`ContentStore::stage_verified_blob` copies the exact declared byte count into a new staging file and hashes the bytes while copying. Corrupt and truncated copies are discarded. Upload completion and native runtime materialization reuse the streaming verifier. A staging directory is trusted and remains private until its caller validates the whole artifact and publishes or mounts it.

The focused Windows test verified that a same-length replacement or truncated blob is refused and that an existing staged file remains intact. The full Windows bundle-deployment suite exposed parent-directory flush support and path-format assertions in the existing publishing and native release code. The full Linux bundle-deployment library suite passed all 23 tests in Ubuntu 24.04 under WSL.

Verified execution context, authorized artifact registration, immutable artifact mounting, provider integration, and workload endpoint lifecycle remain open.
