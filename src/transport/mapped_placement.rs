//! Bind trusted worker registrations and local producers to one pool per session.
//! This selects storage/placement; it does not certify arbitrary node internals
//! or observers as end-to-end zero-copy.
use crate::manifest::{Manifest, NodeManifest, RuntimeHint};
use crate::multiprocess::native_worker::{PoolConfig, SessionId, SharedPool, WorkerInputCopies};
use crate::nodes::StreamingNodeRegistry;
use crate::{Error, Result};

pub(super) fn bind_session_workers(
    manifest: &Manifest,
    registry: &mut StreamingNodeRegistry,
    config: Option<&PoolConfig>,
    session_id: &str,
    policy: remotemedia_types::CopyPolicy,
) -> Result<(Option<SharedPool>, WorkerInputCopies)> {
    // Validate placement before reserving storage or provisioning any workers.
    for node in &manifest.nodes {
        if registry.session_worker(&node.node_type).is_none() {
            continue;
        }
        let reason = if config.is_none() {
            Some("ExecutorConfig.mapped_pool is required")
        } else {
            incompatible_placement(node)
        };
        if let Some(reason) = reason {
            return Err(Error::ConfigError(format!(
                "mapped worker node '{}': {reason}",
                node.id
            )));
        }
    }
    let copies = WorkerInputCopies::new(
        session_id,
        manifest
            .nodes
            .iter()
            .filter(|node| registry.session_worker(&node.node_type).is_some())
            .map(|node| node.id.clone()),
        policy,
    );
    let Some(config) = config else {
        return Ok((None, copies));
    };
    let pool = SharedPool::new(SessionId(*uuid::Uuid::new_v4().as_bytes()), config.clone())
        .map_err(|error| Error::ConfigError(format!("session mapped pool: {error}")))?;
    for node in &manifest.nodes {
        if let Some(factory) = registry.session_worker(&node.node_type) {
            let bound = factory.bind_session(pool.clone(), copies.clone());
            registry.register(bound);
        }
    }
    Ok((Some(pool), copies))
}

fn incompatible_placement(node: &NodeManifest) -> Option<&'static str> {
    if node.host.is_some() {
        return Some("host routing is unsupported for same-host mapped workers");
    }
    if let Some(execution) = &node.execution {
        if !matches!(
            execution.placement.as_str(),
            "" | "local" | "prefer_local" | "auto"
        ) || execution.fallback.is_some()
        {
            return Some("execution placement/fallback conflicts with the registered local worker");
        }
    }
    if !matches!(node.runtime_hint, None | Some(RuntimeHint::Auto)) {
        return Some("runtime_hint conflicts with registered subprocess execution");
    }
    #[cfg(feature = "docker")]
    if node.docker.is_some() {
        return Some("Docker placement is unsupported for mapped workers");
    }
    None
}
