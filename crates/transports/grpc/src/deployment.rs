//! Authenticated gRPC adapter for transport-neutral bundle deployment.

use futures::StreamExt;
use remotemedia_bundle::{canonical_json, DescriptorIdentity, VariantCandidate};
use remotemedia_bundle_deployment::{
    DeploymentError, DeploymentRevision, DeploymentService as CoreDeploymentService,
};
use tonic::{Request, Response, Status};

use crate::auth::{check_auth, AuthConfig};
use crate::generated::{
    bundle_deployment_service_server::BundleDeploymentService, ActivateBundleRequest, BlobIdentity,
    BundlePreflightRequest, CancelInstallRequest, DeploymentCapabilitiesRequest,
    DeploymentJsonResponse, EmptyDeploymentResponse, InstallBundleRequest, InstallStatusRequest,
    ListDeploymentsRequest, MissingBlobsRequest, MissingBlobsResponse, RollbackBundleRequest,
    RollbackBundleResponse, SmokeTestDeploymentRequest, SmokeTestDeploymentResponse,
    UploadBlobChunk, UploadBlobResponse,
};
use remotemedia_core::{
    data::RuntimeData,
    manifest::Manifest,
    transport::{PipelineExecutor, TransportData},
};
use std::sync::Arc;

const PROTOCOL_VERSION: &str = "v1";

pub struct BundleDeploymentServiceImpl {
    auth: AuthConfig,
    service: CoreDeploymentService,
    executor: Arc<PipelineExecutor>,
}

impl BundleDeploymentServiceImpl {
    pub fn new(
        auth: AuthConfig,
        service: CoreDeploymentService,
        executor: Arc<PipelineExecutor>,
    ) -> Self {
        Self {
            auth,
            service,
            executor,
        }
    }
}

#[tonic::async_trait]
impl BundleDeploymentService for BundleDeploymentServiceImpl {
    async fn get_deployment_capabilities(
        &self,
        request: Request<DeploymentCapabilitiesRequest>,
    ) -> Result<Response<DeploymentJsonResponse>, Status> {
        let token = authorize(&request, &self.auth)?;
        require_version(&request.get_ref().protocol_version)?;
        let capabilities = self
            .service
            .capabilities(token.as_bytes())
            .map_err(status)?;
        Ok(Response::new(json_response(&capabilities)?))
    }

    async fn list_deployments(
        &self,
        request: Request<ListDeploymentsRequest>,
    ) -> Result<Response<DeploymentJsonResponse>, Status> {
        let token = authorize(&request, &self.auth)?;
        require_version(&request.get_ref().protocol_version)?;
        let deployments = self
            .service
            .list_deployments(token.as_bytes())
            .map_err(status)?;
        Ok(Response::new(json_response(&deployments)?))
    }

    async fn preflight_bundle(
        &self,
        request: Request<BundlePreflightRequest>,
    ) -> Result<Response<DeploymentJsonResponse>, Status> {
        let token = authorize(&request, &self.auth)?;
        require_version(&request.get_ref().protocol_version)?;
        let candidates: Vec<VariantCandidate> =
            serde_json::from_slice(&request.get_ref().candidates_json).map_err(|error| {
                Status::invalid_argument(format!("invalid candidates: {error}"))
            })?;
        let report = self
            .service
            .preflight(token.as_bytes(), &candidates)
            .map_err(status)?;
        Ok(Response::new(json_response(&report)?))
    }

    async fn negotiate_missing_blobs(
        &self,
        request: Request<MissingBlobsRequest>,
    ) -> Result<Response<MissingBlobsResponse>, Status> {
        let token = authorize(&request, &self.auth)?;
        require_version(&request.get_ref().protocol_version)?;
        let descriptors = request
            .get_ref()
            .blobs
            .iter()
            .map(descriptor_from_proto)
            .collect::<Vec<_>>();
        let missing = self
            .service
            .missing_blobs(token.as_bytes(), &descriptors)
            .map_err(status)?
            .into_iter()
            .map(descriptor_to_proto)
            .collect();
        Ok(Response::new(MissingBlobsResponse {
            protocol_version: PROTOCOL_VERSION.to_owned(),
            missing,
        }))
    }

    async fn upload_blob(
        &self,
        request: Request<tonic::Streaming<UploadBlobChunk>>,
    ) -> Result<Response<UploadBlobResponse>, Status> {
        let token = authorize(&request, &self.auth)?;
        let mut stream = request.into_inner();
        let first = stream
            .next()
            .await
            .ok_or_else(|| Status::invalid_argument("upload stream is empty"))??;
        require_version(&first.protocol_version)?;
        let descriptor = first
            .blob
            .as_ref()
            .map(descriptor_from_proto)
            .ok_or_else(|| Status::invalid_argument("first upload chunk has no blob identity"))?;
        let mut upload = self
            .service
            .begin_upload(token.as_bytes(), descriptor.clone())
            .map_err(status)?;
        upload.append(first.offset, &first.data).map_err(status)?;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            require_version(&chunk.protocol_version)?;
            if chunk.blob.as_ref().map(descriptor_from_proto) != Some(descriptor.clone()) {
                return Err(Status::invalid_argument(
                    "all upload chunks must identify the same blob",
                ));
            }
            upload.append(chunk.offset, &chunk.data).map_err(status)?;
        }
        upload.finish().map_err(status)?;
        Ok(Response::new(UploadBlobResponse {
            protocol_version: PROTOCOL_VERSION.to_owned(),
            digest: descriptor.digest,
            committed_size: descriptor.size,
        }))
    }

    async fn install_bundle(
        &self,
        request: Request<InstallBundleRequest>,
    ) -> Result<Response<DeploymentJsonResponse>, Status> {
        let token = authorize(&request, &self.auth)?;
        require_version(&request.get_ref().protocol_version)?;
        let revision: DeploymentRevision = serde_json::from_slice(&request.get_ref().revision_json)
            .map_err(|error| Status::invalid_argument(format!("invalid revision: {error}")))?;
        self.service
            .install(
                token.as_bytes(),
                &request.get_ref().operation_id,
                revision,
                request.get_ref().total_bytes,
                |_| Ok(()),
            )
            .map_err(status)?;
        let installed = self
            .service
            .status(token.as_bytes(), &request.get_ref().operation_id)
            .map_err(status)?
            .ok_or_else(|| Status::internal("install status disappeared"))?;
        Ok(Response::new(json_response(&installed)?))
    }

    async fn get_install_status(
        &self,
        request: Request<InstallStatusRequest>,
    ) -> Result<Response<DeploymentJsonResponse>, Status> {
        let token = authorize(&request, &self.auth)?;
        require_version(&request.get_ref().protocol_version)?;
        let status_value = self
            .service
            .status(token.as_bytes(), &request.get_ref().operation_id)
            .map_err(status)?
            .ok_or_else(|| Status::not_found("install operation was not found"))?;
        Ok(Response::new(json_response(&status_value)?))
    }

    async fn cancel_install(
        &self,
        request: Request<CancelInstallRequest>,
    ) -> Result<Response<EmptyDeploymentResponse>, Status> {
        let token = authorize(&request, &self.auth)?;
        require_version(&request.get_ref().protocol_version)?;
        self.service
            .cancel(token.as_bytes(), &request.get_ref().operation_id)
            .map_err(status)?;
        Ok(Response::new(empty_response()))
    }

    async fn activate_bundle(
        &self,
        request: Request<ActivateBundleRequest>,
    ) -> Result<Response<EmptyDeploymentResponse>, Status> {
        let token = authorize(&request, &self.auth)?;
        require_version(&request.get_ref().protocol_version)?;
        self.service
            .activate(
                token.as_bytes(),
                &request.get_ref().deployment_name,
                &request.get_ref().bundle_digest,
            )
            .map_err(status)?;
        Ok(Response::new(empty_response()))
    }

    async fn rollback_bundle(
        &self,
        request: Request<RollbackBundleRequest>,
    ) -> Result<Response<RollbackBundleResponse>, Status> {
        let token = authorize(&request, &self.auth)?;
        require_version(&request.get_ref().protocol_version)?;
        let active_bundle_digest = self
            .service
            .rollback(token.as_bytes(), &request.get_ref().deployment_name)
            .map_err(status)?;
        Ok(Response::new(RollbackBundleResponse {
            protocol_version: PROTOCOL_VERSION.to_owned(),
            active_bundle_digest,
        }))
    }

    async fn smoke_test_deployment(
        &self,
        request: Request<SmokeTestDeploymentRequest>,
    ) -> Result<Response<SmokeTestDeploymentResponse>, Status> {
        const MAX_SMOKE_TEXT_BYTES: usize = 4096;

        let token = authorize(&request, &self.auth)?;
        let request = request.into_inner();
        require_version(&request.protocol_version)?;
        if request.input_text.len() > MAX_SMOKE_TEXT_BYTES {
            return Err(Status::invalid_argument(format!(
                "smoke input exceeds {MAX_SMOKE_TEXT_BYTES} bytes"
            )));
        }
        let (revision, manifest_bytes) = self
            .service
            .active_manifest(token.as_bytes(), &request.deployment_name)
            .map_err(status)?;
        let manifest: Manifest = serde_json::from_slice(&manifest_bytes)
            .map_err(|error| Status::failed_precondition(format!("invalid deployed manifest: {error}")))?;
        let output = self
            .executor
            .execute_unary(
                Arc::new(manifest),
                TransportData::new(RuntimeData::Text(request.input_text)),
            )
            .await
            .map_err(|error| Status::failed_precondition(format!("deployment smoke test failed: {error}")))?;
        let RuntimeData::Text(output_text) = output.data else {
            return Err(Status::failed_precondition(
                "deployment smoke test produced non-text output",
            ));
        };
        Ok(Response::new(SmokeTestDeploymentResponse {
            protocol_version: PROTOCOL_VERSION.to_owned(),
            active_bundle_digest: revision.bundle_digest,
            output_text,
        }))
    }
}

fn authorize<T>(request: &Request<T>, auth: &AuthConfig) -> Result<String, Status> {
    check_auth(request, auth)?;
    let value = request
        .metadata()
        .get("authorization")
        .ok_or_else(|| Status::unauthenticated("authorization metadata is required"))?
        .to_str()
        .map_err(|_| Status::unauthenticated("authorization metadata is invalid"))?;
    value
        .strip_prefix("Bearer ")
        .map(str::to_owned)
        .ok_or_else(|| Status::unauthenticated("Bearer authentication is required"))
}

fn require_version(version: &str) -> Result<(), Status> {
    if version == PROTOCOL_VERSION {
        Ok(())
    } else {
        Err(Status::failed_precondition(format!(
            "deployment protocol {version:?} is unsupported; expected {PROTOCOL_VERSION}"
        )))
    }
}

fn json_response<T: serde::Serialize>(value: &T) -> Result<DeploymentJsonResponse, Status> {
    Ok(DeploymentJsonResponse {
        protocol_version: PROTOCOL_VERSION.to_owned(),
        canonical_json: canonical_json(value)
            .map_err(|error| Status::internal(format!("canonical JSON failed: {error}")))?,
    })
}

fn descriptor_from_proto(value: &BlobIdentity) -> DescriptorIdentity {
    DescriptorIdentity {
        digest: value.digest.clone(),
        size: value.size,
    }
}

fn descriptor_to_proto(value: DescriptorIdentity) -> BlobIdentity {
    BlobIdentity {
        digest: value.digest,
        size: value.size,
    }
}

fn empty_response() -> EmptyDeploymentResponse {
    EmptyDeploymentResponse {
        protocol_version: PROTOCOL_VERSION.to_owned(),
    }
}

fn status(error: DeploymentError) -> Status {
    match error {
        DeploymentError::Unauthenticated => Status::unauthenticated("authentication failed"),
        DeploymentError::InvalidDigest(_)
        | DeploymentError::SizeMismatch { .. }
        | DeploymentError::OffsetMismatch { .. }
        | DeploymentError::InvalidName(_)
        | DeploymentError::InvalidAssetSource(_)
        | DeploymentError::State(_) => Status::invalid_argument(error.to_string()),
        DeploymentError::NotInstalled(_)
        | DeploymentError::NoPreviousRevision(_)
        | DeploymentError::MissingManifestDigest(_) => {
            Status::failed_precondition(error.to_string())
        }
        DeploymentError::ExternalAssetFetch(_) => Status::failed_precondition(error.to_string()),
        DeploymentError::OperationExists(_) => Status::already_exists(error.to_string()),
        DeploymentError::Cancelled(_) => Status::cancelled(error.to_string()),
        DeploymentError::DigestMismatch { .. }
        | DeploymentError::Provisioning(_)
        | DeploymentError::Bundle(_)
        | DeploymentError::Io(_) => Status::internal(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_unversioned_protocol_requests() {
        assert_eq!(
            require_version("").unwrap_err().code(),
            tonic::Code::FailedPrecondition
        );
        assert!(require_version(PROTOCOL_VERSION).is_ok());
        assert_eq!(remotemedia_bundle::BUNDLE_SCHEMA_VERSION, "1");
    }
}
