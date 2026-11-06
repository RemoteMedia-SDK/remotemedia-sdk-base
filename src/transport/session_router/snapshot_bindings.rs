use super::{Error, Result, SessionRouter};
use crate::nodes::ports::{PortKind, SnapshotPort};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

pub(super) struct SnapshotBindings {
    pub inputs: HashMap<String, HashMap<String, Arc<dyn SnapshotPort>>>,
    pub edges: HashSet<usize>,
}

impl SessionRouter {
    /// Resolve once after initialization, before any node media tasks run.
    /// Retain the validated readers so execution cannot bind a different slot.
    pub(super) fn bind_snapshot_inputs(&self) -> Result<SnapshotBindings> {
        let mut bindings = SnapshotBindings {
            inputs: HashMap::new(),
            edges: HashSet::new(),
        };
        let mut outputs = HashMap::new();
        for (index, connection) in self.manifest.connections.iter().enumerate() {
            let input = connection.to_port.as_deref().unwrap_or("default");
            let kind = self
                .graph
                .nodes
                .get(&connection.to)
                .and_then(|node| self.registry.get_factory(&node.node_type))
                .and_then(|factory| factory.input_port_kinds().get(input).copied())
                .unwrap_or(PortKind::Stream);
            if kind != PortKind::Snapshot {
                continue;
            }
            let output = connection.from_port.as_deref().unwrap_or("default");
            let description = format!(
                "snapshot edge '{}::{output}' -> '{}::{input}'",
                connection.from, connection.to
            );
            let producer = self
                .cached_nodes
                .get(&connection.from)
                .ok_or_else(|| Error::Execution(format!("{description}: producer is missing")))?;
            let consumer = self
                .cached_nodes
                .get(&connection.to)
                .ok_or_else(|| Error::Execution(format!("{description}: consumer is missing")))?;
            let ports = outputs
                .entry(connection.from.clone())
                .or_insert_with(|| producer.snapshot_outputs());
            let reader = ports.get(output).ok_or_else(|| {
                Error::Execution(format!(
                    "{description}: producer did not expose the declared snapshot output"
                ))
            })?;
            consumer
                .validate_snapshot_input(input, reader.as_ref())
                .map_err(|error| Error::Execution(format!("{description}: {error}")))?;
            let inputs = bindings.inputs.entry(connection.to.clone()).or_default();
            if inputs.insert(input.to_owned(), reader.clone()).is_some() {
                return Err(Error::Execution(format!(
                    "{description}: multiple connections bind the same snapshot input"
                )));
            }
            bindings.edges.insert(index);
        }
        Ok(bindings)
    }
}
