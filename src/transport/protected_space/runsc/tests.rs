use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::Command;

use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::super::tests::{encode, fixture};
use super::*;
use crate::transport::PipelineExecutor;

const SOURCE: &str = "verified source";
const WHEEL: &str = "verified wheel";
const MODEL: &str = "verified model";

mod gradio;

#[test]
fn runtime_placement_requires_the_configured_cuda_uuid_and_target() {
    const ASSIGNED: &str = "GPU-11111111-1111-1111-1111-111111111111";
    const OTHER: &str = "GPU-22222222-2222-2222-2222-222222222222";
    let (mut approval, _, _) = fixture(ProtectedSpacePhase::Serve {
        artifact_sha256: "a".repeat(64),
    });
    assert_eq!(
        placement_target(&approval, None).unwrap(),
        WorkloadTarget::LinuxCpu
    );
    assert!(placement_target(&approval, Some(ASSIGNED)).is_err());
    approval.target = PROTECTED_CUDA_TARGET.into();
    approval.cuda_device_uuid = Some(OTHER.into());
    assert!(placement_target(&approval, None).is_err());
    assert!(placement_target(&approval, Some(ASSIGNED)).is_err());
    approval.cuda_device_uuid = Some(ASSIGNED.into());
    assert_eq!(
        placement_target(&approval, Some(ASSIGNED)).unwrap(),
        WorkloadTarget::LinuxCuda
    );
}

fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn registered_execution(
    executor: &PipelineExecutor,
    artifacts: &SpaceArtifactRegistry,
    python: &str,
    abi: &str,
) -> (
    ProtectedSpaceApproval,
    PathBuf,
    Vec<u8>,
    VerifiedSpaceExecution,
) {
    let source_tree =
        json!([["app.py", "file", SOURCE.len(), 0, hash(SOURCE.as_bytes())]]).to_string();
    let commit = "a".repeat(40);
    let artifact = json!({
        "version":1, "repository":"org/demo", "commit":commit, "sdk":"gradio", "entry_file":"app.py",
        "source_tree_sha256":hash(source_tree.as_bytes()),
        "target":{"platform":"linux", "arch":"x86_64", "python":python, "abi":abi, "backend":"cpu"},
        "wheels":{"fixture-1-py3-none-any.whl":{"size":WHEEL.len(), "sha256":hash(WHEEL.as_bytes())}},
        "models":[{"repository":"org/model", "revision":"c".repeat(40),
            "files":{"model.bin":{"size":MODEL.len(), "sha256":hash(MODEL.as_bytes())}}}]
    }).to_string();
    let digest = hash(artifact.as_bytes());
    let (mut approval, controller_path, manifest) = fixture(ProtectedSpacePhase::Serve {
        artifact_sha256: digest.clone(),
    });
    let scope = SpaceArtifactScope {
        artifact_sha256: digest,
        repository: approval.repository.clone(),
        commit,
        target: approval.target.clone(),
        profile: "isolated".into(),
        policy_revision: approval.policy_revision.clone(),
    };
    for blob in [SOURCE, WHEEL, MODEL].map(str::as_bytes) {
        artifacts
            .import_blob(&hash(blob), blob.len() as u64, blob)
            .unwrap();
    }
    artifacts.register(&scope, &artifact, &source_tree).unwrap();
    let bytes = encode(&mut approval, &manifest);
    let execution = executor
        .verify_protected_space(approval.clone(), &controller_path, &bytes)
        .unwrap();
    (approval, controller_path, bytes, execution)
}

#[tokio::test]
#[ignore = "requires pinned runsc, a static C compiler and an exclusive cgroup v2 subtree"]
async fn registered_lease_launches_in_run_bound_sandbox() {
    let temp = tempfile::tempdir().unwrap();
    let rootfs = temp.path().join("rootfs");
    fs::create_dir(&rootfs).unwrap();
    fs::set_permissions(&rootfs, fs::Permissions::from_mode(0o755)).unwrap();
    let fixture = temp.path().join("guest.c");
    fs::write(&fixture, include_str!("guest.c")).unwrap();
    assert!(Command::new("cc")
        .arg("-static")
        .arg(fixture)
        .arg("-o")
        .arg(rootfs.join("fixture"))
        .status()
        .unwrap()
        .success());
    fs::set_permissions(rootfs.join("fixture"), fs::Permissions::from_mode(0o755)).unwrap();
    let executable =
        PathBuf::from(std::env::var_os("RM_SPACE_TEST_RUNSC").expect("set pinned runtime path"));
    let delegated_cgroup = PathBuf::from(
        std::env::var_os("RM_SPACE_TEST_CGROUP").expect("set exclusive cgroup subtree"),
    );
    let state = temp.path().join("state");
    let store = temp.path().join("artifacts");
    let artifacts = Arc::new(SpaceArtifactRegistry::open(&store).unwrap());
    let runtime = IsolatedSpaceRuntime::open(
        artifacts.clone(),
        IsolatedSpaceConfig {
            runtime: RunscConfig {
                executable_sha256: hash(&fs::read(&executable).unwrap()),
                executable,
                rootfs,
                state: state.clone(),
                delegated_cgroup,
                relay_program: "/fixture".into(),
            },
            launcher: "/fixture".into(),
            python_abi: "cp312".into(),
            loopback_port: 24621,
            limits: WorkloadLimits {
                cpu_millis: 500,
                memory_bytes: 128 * 1024 * 1024,
                scratch_bytes: 16 * 1024 * 1024,
                processes: 64,
                lifetime: Duration::from_secs(20),
            },
        },
    )
    .await
    .unwrap();
    let capabilities = runtime.capabilities();
    assert_eq!(capabilities.contract_version, 1);
    assert_eq!(capabilities.target, WorkloadTarget::LinuxCpu);
    assert!(capabilities.runtime_version.starts_with("runsc version "));
    assert_eq!(runtime.limits().cpu_millis, 500);
    let executor = PipelineExecutor::for_protected_spaces().unwrap();
    let (_, _, _, wrong_abi) = registered_execution(&executor, &artifacts, "3.11", "cp311");
    assert!(matches!(
        runtime.stage(&wrong_abi).await,
        Err(IsolatedSpaceError::WrongPythonAbi)
    ));

    let (mut approval, controller, bytes, execution) =
        registered_execution(&executor, &artifacts, "3.12", "cp312");
    let mut unsupported = approval.clone();
    unsupported.target = "linux-arm64-cpu".into();
    let unsupported = executor
        .verify_protected_space(unsupported, &controller, &bytes)
        .unwrap();
    assert!(matches!(
        runtime.stage(&unsupported).await,
        Err(IsolatedSpaceError::UnsupportedProfile)
    ));
    approval.run_id = "different-run".into();
    let other = executor
        .verify_protected_space(approval.clone(), &controller, &bytes)
        .unwrap();
    approval.policy_revision = "d".repeat(64);
    let unregistered = executor
        .verify_protected_space(approval, &controller, &bytes)
        .unwrap();
    assert!(matches!(
        runtime.stage(&unregistered).await,
        Err(IsolatedSpaceError::Artifact(
            SpaceArtifactError::Unregistered
        ))
    ));
    assert!(matches!(
        runtime.stage(&execution).await.unwrap().start(&other).await,
        Err(IsolatedSpaceError::WrongRun)
    ));
    assert_eq!(fs::read_dir(state.join("bundles")).unwrap().count(), 0);

    // A change after authorization must fail while copying the registered
    // store. A staged run must then outlive a subsequent change to that store.
    let cached_source = store
        .join("content/blobs/sha256")
        .join(hash(SOURCE.as_bytes()));
    fs::write(&cached_source, vec![0; SOURCE.len()]).unwrap();
    assert!(matches!(
        runtime.stage(&execution).await,
        Err(IsolatedSpaceError::Artifact(SpaceArtifactError::Content(
            remotemedia_bundle_deployment::DeploymentError::DigestMismatch { .. }
        )))
    ));
    assert_eq!(fs::read_dir(state.join("bundles")).unwrap().count(), 0);
    fs::write(&cached_source, SOURCE).unwrap();
    let staged = runtime.stage(&execution).await.unwrap();
    fs::write(&cached_source, vec![0; SOURCE.len()]).unwrap();
    let run = staged.start(&execution).await.unwrap();
    assert!(!run.deadline_is_approval_expiry());
    assert!(matches!(
        run.open_stream(&other),
        Err(IsolatedSpaceError::WrongRun)
    ));
    let mut stream = run.open_stream(&execution).unwrap();
    let mut bytes = [0; SOURCE.len()];
    tokio::time::timeout(Duration::from_secs(5), stream.output.read_exact(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&bytes, SOURCE.as_bytes());
    assert_eq!(run.stop().await.unwrap(), WorkloadExit::Stopped);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), stream.output.read(&mut bytes))
            .await
            .unwrap()
            .unwrap(),
        0
    );
    assert_eq!(fs::read_dir(state.join("bundles")).unwrap().count(), 0);
}
