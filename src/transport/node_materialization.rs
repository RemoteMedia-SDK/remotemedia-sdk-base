//! Attributed materialization contexts retained for the lifetime of a session.
use remotemedia_types::{CopyContext, CopyPolicy, CopyReport};
use std::{collections::BTreeMap, sync::Arc};

/// Host Rust node materialization ledgers, separate from worker ingress ledgers.
/// Reports cover operations explicitly using the node context, not arbitrary
/// node internals, foreign code or every adapter. Quiesce calls before final audit.
#[derive(Clone)]
pub struct NodeMaterializationCopies(Arc<BTreeMap<String, CopyContext>>);

impl NodeMaterializationCopies {
    pub(crate) fn new<'a>(
        session: &str,
        nodes: impl Iterator<Item = &'a str>,
        policy: CopyPolicy,
    ) -> Self {
        Self(Arc::new(
            nodes
                .map(|node| {
                    (
                        node.to_string(),
                        CopyContext::new(
                            policy,
                            format!("session/{session}/node/{node}/materialization"),
                        ),
                    )
                })
                .collect(),
        ))
    }

    pub(crate) fn context(&self, node: &str) -> crate::Result<CopyContext> {
        self.0.get(node).cloned().ok_or_else(|| {
            crate::Error::ConfigError(format!("node materialization policy has no node '{node}'"))
        })
    }

    /// Clones share ledgers even after SessionHandle::wait consumes the session.
    /// Each report is coherent; concurrent work can advance between node reports.
    pub fn reports(&self) -> BTreeMap<String, CopyReport> {
        self.0
            .iter()
            .map(|(node, context)| (node.clone(), context.report()))
            .collect()
    }
}
