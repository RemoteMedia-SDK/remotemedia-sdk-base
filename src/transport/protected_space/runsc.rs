//! Verified, run-bound access to the standalone Linux CPU workload provider.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use remotemedia_bundle_deployment::{
    SpaceArtifactError, SpaceArtifactRegistry, SpaceArtifactScope,
};
pub use remotemedia_workload_sandbox::runsc::{ApprovedWheelBuild, RunscConfig, RunscStream};
use remotemedia_workload_sandbox::runsc::{RunscProvider, RunscReservation, RunscSandbox};
use remotemedia_workload_sandbox::{SandboxError, WorkloadPhase, WorkloadRequest, WorkloadTarget};
pub use remotemedia_workload_sandbox::{WorkloadExit, WorkloadLimits};

use super::{
    ProtectedSpaceApproval, ProtectedSpaceError, ProtectedSpacePhase, ProtectedSpaceProfile,
    VerifiedSpaceExecution,
};

#[cfg(test)]
mod tests;

const TARGET: &str = "linux-amd64-cpu";

/// Build a reviewed dependency using a provider dedicated to the build. The
/// result remains unpublished until the trusted caller admits its artifact.
pub async fn build_approved_space_wheel(
    config: RunscConfig,
    inputs: &std::path::Path,
    plan: &ApprovedWheelBuild,
    limits: WorkloadLimits,
    cancelled: impl std::future::Future<Output = ()>,
) -> Result<std::fs::File, IsolatedSpaceError> {
    let builder = RunscProvider::open(config).await?;
    Ok(builder
        .build_python_wheel(inputs, plan, limits, cancelled)
        .await?)
}

/// All fields come from the host operator. The launcher is a program in the
/// verified rootfs. It receives the SDK name, mounted entry path and port.
pub struct IsolatedSpaceConfig {
    pub runtime: RunscConfig,
    pub launcher: String,
    pub python_abi: String,
    pub loopback_port: u16,
    pub limits: WorkloadLimits,
}

pub struct IsolatedSpaceRuntime {
    provider: RunscProvider,
    artifacts: Arc<SpaceArtifactRegistry>,
    launcher: String,
    python_abi: String,
    loopback_port: u16,
    limits: WorkloadLimits,
}

pub struct PreparedIsolatedSpace {
    approval: ProtectedSpaceApproval,
    reservation: RunscReservation,
    request: WorkloadRequest,
}

pub struct IsolatedSpaceRun {
    approval: ProtectedSpaceApproval,
    sandbox: RunscSandbox,
    deadline_is_approval_expiry: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum IsolatedSpaceError {
    #[error(transparent)]
    Approval(#[from] ProtectedSpaceError),
    #[error("the verified execution does not match this run")]
    WrongRun,
    #[error("the Linux CPU sandbox cannot provide the approved phase or profile")]
    UnsupportedProfile,
    #[error("the verified artifact requires a different Python ABI")]
    WrongPythonAbi,
    #[error(transparent)]
    Artifact(#[from] SpaceArtifactError),
    #[error("verified artifact staging was interrupted")]
    StagingInterrupted,
    #[error(transparent)]
    Sandbox(#[from] SandboxError),
}

impl IsolatedSpaceRuntime {
    pub async fn check_ready(&self) -> Result<(), IsolatedSpaceError> {
        self.provider
            .check_ready(std::path::Path::new(&self.launcher))
            .await?;
        Ok(())
    }

    pub fn limits(&self) -> &WorkloadLimits {
        &self.limits
    }

    /// Reconcile abandoned provider state without opening the artifact registry
    /// or starting a listener. Provider locks exclude a live worker.
    pub async fn reconcile_abandoned(config: RunscConfig) -> Result<(), IsolatedSpaceError> {
        if !cfg!(target_arch = "x86_64") {
            return Err(IsolatedSpaceError::UnsupportedProfile);
        }
        RunscProvider::open(config).await?;
        Ok(())
    }

    pub async fn open(
        artifacts: Arc<SpaceArtifactRegistry>,
        config: IsolatedSpaceConfig,
    ) -> Result<Self, IsolatedSpaceError> {
        if !cfg!(target_arch = "x86_64") {
            return Err(IsolatedSpaceError::UnsupportedProfile);
        }
        let provider = RunscProvider::open(config.runtime).await?;
        provider.probe(WorkloadTarget::LinuxCpu).await?;
        Ok(Self {
            provider,
            artifacts,
            launcher: config.launcher,
            python_abi: config.python_abi,
            loopback_port: config.loopback_port,
            limits: config.limits,
        })
    }

    /// Copy the registered closure into the sandbox's durable reservation.
    /// The caller rechecks effective worker policy before starting it.
    pub async fn stage(
        &self,
        execution: &VerifiedSpaceExecution,
    ) -> Result<PreparedIsolatedSpace, IsolatedSpaceError> {
        execution.check_current()?;
        let approval = execution.approval();
        let ProtectedSpacePhase::Serve { artifact_sha256 } = &approval.phase else {
            return Err(IsolatedSpaceError::UnsupportedProfile);
        };
        if approval.profile != ProtectedSpaceProfile::Isolated || approval.target != TARGET {
            return Err(IsolatedSpaceError::UnsupportedProfile);
        }
        let scope = SpaceArtifactScope {
            artifact_sha256: artifact_sha256.clone(),
            repository: approval.repository.clone(),
            commit: approval.commit.clone(),
            target: approval.target.clone(),
            profile: "isolated".into(),
            policy_revision: approval.policy_revision.clone(),
        };
        let artifacts = self.artifacts.clone();
        let provider = self.provider.clone();
        let (reservation, staged) = tokio::task::spawn_blocking(move || {
            let reservation = provider.reserve()?;
            let staged = artifacts.stage_lease(&scope, &reservation.input_root())?;
            Ok::<_, IsolatedSpaceError>((reservation, staged))
        })
        .await
        .map_err(|_| IsolatedSpaceError::StagingInterrupted)??;
        if staged.python_abi != self.python_abi {
            return Err(IsolatedSpaceError::WrongPythonAbi);
        }
        Ok(PreparedIsolatedSpace {
            approval: approval.clone(),
            reservation,
            request: WorkloadRequest {
                target: WorkloadTarget::LinuxCpu,
                phase: WorkloadPhase::Serve {
                    loopback_port: self.loopback_port,
                },
                limits: self.limits,
                argv: vec![
                    self.launcher.clone(),
                    staged.sdk,
                    format!("/inputs/source/{}", staged.entry_file),
                    self.loopback_port.to_string(),
                ],
            },
        })
    }
}

impl PreparedIsolatedSpace {
    /// Use the freshly verified approval from the trusted caller. Replacing a
    /// grant, changing policy, or changing the manifest requires restaging.
    pub async fn start(
        mut self,
        current: &VerifiedSpaceExecution,
    ) -> Result<IsolatedSpaceRun, IsolatedSpaceError> {
        check_run(&self.approval, current)?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| ProtectedSpaceError::Clock)?;
        let remaining = Duration::from_secs(self.approval.expires_unix).saturating_sub(now);
        if remaining.is_zero() {
            return Err(ProtectedSpaceError::Expired.into());
        }
        let deadline_is_approval_expiry = remaining <= self.request.limits.lifetime;
        self.request.limits.lifetime = self.request.limits.lifetime.min(remaining);
        let sandbox = self.reservation.start(self.request).await?;
        if let Err(error) = current.check_current() {
            sandbox.stop().await?;
            return Err(error.into());
        }
        Ok(IsolatedSpaceRun {
            approval: self.approval,
            sandbox,
            deadline_is_approval_expiry,
        })
    }
}

impl IsolatedSpaceRun {
    /// Whether the run's absolute approval expiry selected the provider lifetime.
    /// Use the selected limit when interpreting the provider's monotonic deadline.
    pub fn deadline_is_approval_expiry(&self) -> bool {
        self.deadline_is_approval_expiry
    }

    /// The authenticated gateway uses its current out-of-band authorization
    /// and receives only a stream to this run's fixed guest loopback port.
    pub fn open_stream(
        &self,
        current: &VerifiedSpaceExecution,
    ) -> Result<RunscStream, IsolatedSpaceError> {
        check_run(&self.approval, current)?;
        Ok(self.sandbox.open_stream()?)
    }

    pub async fn wait(self) -> Result<WorkloadExit, IsolatedSpaceError> {
        Ok(self.sandbox.wait().await?)
    }

    /// A cancel-safe wait for the run owner to select against authorization
    /// changes and stop requests.
    pub async fn wait_for_exit(&mut self) -> Result<WorkloadExit, IsolatedSpaceError> {
        Ok(self.sandbox.wait_for_exit().await?)
    }

    pub async fn stop(self) -> Result<WorkloadExit, IsolatedSpaceError> {
        Ok(self.sandbox.stop().await?)
    }
}

impl IsolatedSpaceError {
    pub fn cleanup_pending(&self) -> bool {
        matches!(self, Self::Sandbox(SandboxError::CleanupPending(_)))
    }
}

fn check_run(
    approval: &ProtectedSpaceApproval,
    current: &VerifiedSpaceExecution,
) -> Result<(), IsolatedSpaceError> {
    current.check_current()?;
    if approval != current.approval() {
        return Err(IsolatedSpaceError::WrongRun);
    }
    Ok(())
}
