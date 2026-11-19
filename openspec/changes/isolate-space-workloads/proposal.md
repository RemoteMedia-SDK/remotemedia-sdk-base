# Enforce protected workload execution in SDK Base

## Why

Source-Python plugin loading currently provisions its interpreter from manifest environment settings. For a protected workload, those inputs and the component using the interpreter have different trust requirements. A digest check also needs to bind the same immutable bytes that the workload consumes.

## What Changes

- Carry a verified execution context from the trusted caller into source-plugin lifecycle and artifact mounting. Keep the RemoteMedia control shim in a released, trusted environment.
- Verify and lease prepared workload artifacts. Start and stop the workload through the Plugin SDK isolation provider, and return a bounded run-scoped endpoint capability to the trusted gateway.

## Impact

- Adds `protected-workload-runtime`. Implementation spans `src/loadable/source_python.rs`, the execution lifecycle and manifest context, and `crates/bundle-deployment` content verification and ownership primitives. Follow the [shared design](../../../../huggingface-spaces-runner/openspec/changes/isolate-space-workloads/design.md).
- Existing source plugins retain their default execution path when no protected policy applies. Once protected execution is required, legacy clients, missing mounts, and unsupported provider contracts cause an explicit denial.
