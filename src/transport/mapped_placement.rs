//! Bind trusted worker registrations and local producers to one pool per session.
//! This selects storage/placement; it does not certify arbitrary node internals
//! or observers as end-to-end zero-copy.
use crate::manifest::{Manifest, NodeManifest};
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
    bind_registered_factories(manifest, registry, &pool, &copies)?;
    Ok((Some(pool), copies))
}

fn bind_registered_factories(
    manifest: &Manifest,
    registry: &mut StreamingNodeRegistry,
    pool: &SharedPool,
    copies: &WorkerInputCopies,
) -> Result<()> {
    let mut bound_types = std::collections::HashSet::new();
    for node in &manifest.nodes {
        if let Some(factory) = registry.session_worker(&node.node_type).cloned() {
            if bound_types.insert(node.node_type.clone()) {
                let bound = factory.bind_session(pool.clone(), copies.clone());
                registry.bind_session_worker(&node.node_type, bound)?;
            }
            let bound = registry
                .get_factory(&node.node_type)
                .expect("bound session factory");
            if bound.try_buffer_contract(&node.params)?
                != factory.try_buffer_contract(&node.params)?
            {
                return Err(Error::ConfigError(format!(
                    "mapped worker '{}': session binding changed its buffer declaration",
                    node.id
                )));
            }
        }
    }
    Ok(())
}

fn incompatible_placement(node: &NodeManifest) -> Option<&'static str> {
    use super::buffer_preflight::{local_placement_conflict, LocalPlacementConflict};
    local_placement_conflict(node).map(|conflict| match conflict {
        LocalPlacementConflict::Host => "host routing is unsupported for same-host mapped workers",
        LocalPlacementConflict::Execution => {
            "execution placement/fallback conflicts with the registered local worker"
        }
        LocalPlacementConflict::Runtime => {
            "runtime_hint conflicts with registered subprocess execution"
        }
        #[cfg(feature = "docker")]
        LocalPlacementConflict::Docker => "Docker placement is unsupported for mapped workers",
    })
}
