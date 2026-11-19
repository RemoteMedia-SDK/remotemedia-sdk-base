## ADDED Requirements

### Requirement: Verified execution context outside the manifest

SDK Base SHALL accept protected execution from a verified caller context that binds run identity, permitted phase, target, policy, execution profile, and the workload artifact for serve. The required execution class SHALL be enforced independently of manifest and node parameters. The trusted source plugin SHALL start from operator-controlled dependencies. For an isolated profile, the runtime SHALL route workload installation, imports, and app execution through the protected workload provider. It SHALL reject missing, incompatible, or conflicting context before plugin resolution or provisioning can execute workload-controlled inputs.

#### Scenario: A protected manifest requests ordinary source execution

- **WHEN** a manifest requests an alternate plugin path, environment, or execution profile that conflicts with the verified context or trusted policy
- **THEN** the runtime rejects it before resolving or starting workload-controlled code

#### Scenario: A regular trusted plugin executes

- **WHEN** an existing trusted application runs outside protected admission with its existing source-plugin configuration
- **THEN** it retains the established behavior and its capabilities make no protected isolation claim

### Requirement: Immutable and authorized workload inputs

The runtime SHALL accept only a workload artifact registered by an authorized publisher. It SHALL validate descriptor version, authorization, target, closure, normalized paths, size limits, and content digests before use. It SHALL hold an immutable lease on the verified bytes through startup and service. For isolated execution, the workload SHALL have write access only to run-scoped writable storage under its policy.

#### Scenario: Source content changes between stages

- **WHEN** an imported source file, wheel, model asset, or underlying artifact is changed after prepare or during serve handoff
- **THEN** the runtime either uses the original verified immutable bytes or refuses activation with an integrity error

#### Scenario: An unregistered artifact arrives with a matching digest

- **WHEN** an artifact has consistent digests but no trusted registration for the launch
- **THEN** the runtime denies activation before creating an installer or app process

### Requirement: Run-scoped endpoint and lifecycle

The runtime SHALL expose a private endpoint capability bound to a verified run and its authenticated gateway. It SHALL invalidate the capability on stop or authorization loss and confirm provider termination before releasing mutable state or reserved capacity. It SHALL quarantine resources pending confirmed cleanup and reconcile orphaned isolation units on restart.

#### Scenario: The trusted host fails after provisioning starts

- **WHEN** the host exits and restarts while a protected run holds an isolation unit and a mutable lease
- **THEN** no stale route is accepted, the surviving unit is reconciled and terminated, and its mutable resources remain unavailable until confirmed cleanup
