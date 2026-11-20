## 1. Verified execution and artifact handoff

- [x] 1.1 Add a validated context consumed by runtime and source-plugin loading. Prove node and manifest parameters cannot supply or overwrite trusted policy and authorization.
- [x] 1.2 Extend or reuse `crates/bundle-deployment` to verify and pin trusted artifact registrations and complete immutable contents. Check safe paths, target, policy, limits, and closure before making an artifact usable. Publish atomically and lease immutable content for the run.
- [ ] 1.3 Wire `src/loadable/source_python.rs` and the runtime lifecycle to use trusted control dependencies and a separate provider-backed workload. Expose a run-bound private endpoint to the trusted gateway. Keep provider handles, remote fetch credentials, and IPC outside workload access.

## 2. Runtime verification

- [ ] 2.1 Test untrusted manifest overrides, unregistered digests, incorrect targets, mutation between validation and mount, and concurrent leases with failure recovery.
- [ ] 2.2 Run the real source-Python control fixture with isolated provisioning and serving. Verify endpoint ownership, resource enforcement, and teardown on node failure, cancellation, and restart, alongside legacy trusted source-plugin tests.
