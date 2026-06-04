//! Pipeline graph data structures
//!
//! Defines the DAG representation of a pipeline with topological sorting
//! and cycle detection.

use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};

/// A node in the pipeline graph
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineNode {
    /// Unique node identifier
    pub id: String,

    /// Node type (e.g., "audio.resample", "python.custom")
    pub node_type: String,

    /// Node configuration (parameters)
    #[serde(default)]
    pub config: serde_json::Value,

    /// Dependencies (node IDs this node depends on)
    #[serde(default)]
    pub dependencies: Vec<String>,

    /// Whether this node can run in parallel with others
    #[serde(default = "default_parallel_safe")]
    pub parallel_safe: bool,

    /// Optional timeout in milliseconds
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

fn default_parallel_safe() -> bool {
    true
}

impl PipelineNode {
    /// Create a new pipeline node
    pub fn new(id: impl Into<String>, node_type: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            node_type: node_type.into(),
            config: serde_json::Value::Null,
            dependencies: Vec::new(),
            parallel_safe: true,
            timeout_ms: None,
        }
    }

    /// Add a dependency to this node
    pub fn add_dependency(&mut self, dep: impl Into<String>) {
        self.dependencies.push(dep.into());
    }

    /// Set node configuration
    pub fn with_config(mut self, config: serde_json::Value) -> Self {
        self.config = config;
        self
    }
}

/// Pipeline directed acyclic graph (DAG)
#[derive(Debug, Clone)]
pub struct PipelineGraph {
    /// All nodes in the graph
    nodes: HashMap<String, PipelineNode>,

    /// Adjacency list (node_id -> dependent node IDs)
    edges: HashMap<String, Vec<String>>,

    /// Entry points (nodes with no dependencies)
    entry_points: Vec<String>,
}

impl PipelineGraph {
    /// Create a new empty graph
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            edges: HashMap::new(),
            entry_points: Vec::new(),
        }
    }

    /// Add a node to the graph
    pub fn add_node(&mut self, node: PipelineNode) -> Result<()> {
        if self.nodes.contains_key(&node.id) {
            return Err(Error::Manifest(format!("Duplicate node ID: {}", node.id)));
        }

        let node_id = node.id.clone();
        self.nodes.insert(node_id.clone(), node);
        self.edges.insert(node_id, Vec::new());

        Ok(())
    }

    /// Add an edge from source to target
    pub fn add_edge(&mut self, source: impl Into<String>, target: impl Into<String>) -> Result<()> {
        let source = source.into();
        let target = target.into();

        if !self.nodes.contains_key(&source) {
            return Err(Error::Manifest(format!(
                "Source node not found: {}",
                source
            )));
        }

        if !self.nodes.contains_key(&target) {
            return Err(Error::Manifest(format!(
                "Target node not found: {}",
                target
            )));
        }

        self.edges.entry(source).or_default().push(target);

        Ok(())
    }

    /// Build edges from node dependencies
    pub fn build_edges(&mut self) -> Result<()> {
        let nodes = self.nodes.clone();

        for (node_id, node) in &nodes {
            for dep in &node.dependencies {
                if !self.nodes.contains_key(dep) {
                    return Err(Error::Manifest(format!(
                        "Node '{}' depends on non-existent node '{}'",
                        node_id, dep
                    )));
                }

                // Edge from dependency to dependent
                self.edges
                    .entry(dep.clone())
                    .or_default()
                    .push(node_id.clone());
            }
        }

        self.compute_entry_points();

        Ok(())
    }

    /// Compute entry points (nodes with no dependencies)
    fn compute_entry_points(&mut self) {
        self.entry_points = self
            .nodes
            .values()
            .filter(|n| n.dependencies.is_empty())
            .map(|n| n.id.clone())
            .collect();
    }

    /// Get entry point nodes
    pub fn entry_points(&self) -> &[String] {
        &self.entry_points
    }

    /// Get a node by ID
    pub fn get_node(&self, id: &str) -> Option<&PipelineNode> {
        self.nodes.get(id)
    }

    /// Get all nodes
    pub fn nodes(&self) -> &HashMap<String, PipelineNode> {
        &self.nodes
    }

    /// Get edges from a node
    pub fn edges(&self, node_id: &str) -> Option<&[String]> {
        self.edges.get(node_id).map(|v| v.as_slice())
    }

    /// Validate the graph
    pub fn validate(&self) -> Result<()> {
        // Check for cycles
        if let Some(cycle) = self.detect_cycles() {
            return Err(Error::Manifest(format!(
                "Cycle detected: {}",
                cycle.join(" -> ")
            )));
        }

        // Check all dependencies exist
        for (node_id, node) in &self.nodes {
            for dep in &node.dependencies {
                if !self.nodes.contains_key(dep) {
                    return Err(Error::Manifest(format!(
                        "Node '{}' depends on non-existent node '{}'",
                        node_id, dep
                    )));
                }
            }
        }

        Ok(())
    }

    /// Topological sort using Kahn's algorithm
    pub fn topological_sort(&self) -> Result<Vec<String>> {
        let mut in_degree: HashMap<String, usize> = HashMap::new();
        let mut queue: VecDeque<String> = VecDeque::new();
        let mut result: Vec<String> = Vec::new();

        // Calculate in-degrees
        for node_id in self.nodes.keys() {
            in_degree.insert(node_id.clone(), 0);
        }

        for node in self.nodes.values() {
            for _dep in &node.dependencies {
                *in_degree.get_mut(&node.id).unwrap() += 1;
            }
        }

        // Find nodes with in-degree 0 (entry points)
        for (node_id, &degree) in &in_degree {
            if degree == 0 {
                queue.push_back(node_id.clone());
            }
        }

        // Process queue
        while let Some(node_id) = queue.pop_front() {
            result.push(node_id.clone());

            // Reduce in-degree for dependents
            if let Some(edges) = self.edges.get(&node_id) {
                for dependent in edges {
                    let degree = in_degree.get_mut(dependent).unwrap();
                    *degree -= 1;
                    if *degree == 0 {
                        queue.push_back(dependent.clone());
                    }
                }
            }
        }

        // Check if all nodes were processed (no cycles)
        if result.len() != self.nodes.len() {
            return Err(Error::Manifest("Cycle detected in graph".to_string()));
        }

        Ok(result)
    }

    /// Detect cycles using DFS
    pub fn detect_cycles(&self) -> Option<Vec<String>> {
        let mut visited: HashSet<String> = HashSet::new();
        let mut rec_stack: HashSet<String> = HashSet::new();
        let mut path: Vec<String> = Vec::new();

        for node_id in self.nodes.keys() {
            if !visited.contains(node_id) {
                if let Some(cycle) =
                    self.dfs_cycle_detect(node_id, &mut visited, &mut rec_stack, &mut path)
                {
                    return Some(cycle);
                }
            }
        }

        None
    }

    fn dfs_cycle_detect(
        &self,
        node_id: &str,
        visited: &mut HashSet<String>,
        rec_stack: &mut HashSet<String>,
        path: &mut Vec<String>,
    ) -> Option<Vec<String>> {
        visited.insert(node_id.to_string());
        rec_stack.insert(node_id.to_string());
        path.push(node_id.to_string());

        if let Some(edges) = self.edges.get(node_id) {
            for neighbor in edges {
                if !visited.contains(neighbor) {
                    if let Some(cycle) = self.dfs_cycle_detect(neighbor, visited, rec_stack, path) {
                        return Some(cycle);
                    }
                } else if rec_stack.contains(neighbor) {
                    // Cycle found
                    let cycle_start = path.iter().position(|n| n == neighbor).unwrap();
                    let mut cycle = path[cycle_start..].to_vec();
                    cycle.push(neighbor.clone());
                    return Some(cycle);
                }
            }
        }

        path.pop();
        rec_stack.remove(node_id);
        None
    }
}

impl Default for PipelineGraph {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_node() {
        let mut graph = PipelineGraph::new();
        let node = PipelineNode::new("node1", "test");

        assert!(graph.add_node(node).is_ok());
        assert!(graph.get_node("node1").is_some());
    }

    #[test]
    fn test_duplicate_node() {
        let mut graph = PipelineGraph::new();
        let node1 = PipelineNode::new("node1", "test");
        let node2 = PipelineNode::new("node1", "test");

        graph.add_node(node1).unwrap();
        assert!(graph.add_node(node2).is_err());
    }

    #[test]
    fn test_topological_sort() {
        let mut graph = PipelineGraph::new();

        let mut node1 = PipelineNode::new("node1", "test");
        let mut node2 = PipelineNode::new("node2", "test");
        node2.add_dependency("node1");
        let mut node3 = PipelineNode::new("node3", "test");
        node3.add_dependency("node2");

        graph.add_node(node1).unwrap();
        graph.add_node(node2).unwrap();
        graph.add_node(node3).unwrap();
        graph.build_edges().unwrap();

        let sorted = graph.topological_sort().unwrap();
        assert_eq!(sorted, vec!["node1", "node2", "node3"]);
    }

    #[test]
    fn test_cycle_detection() {
        let mut graph = PipelineGraph::new();

        let mut node1 = PipelineNode::new("node1", "test");
        node1.add_dependency("node3");
        let mut node2 = PipelineNode::new("node2", "test");
        node2.add_dependency("node1");
        let mut node3 = PipelineNode::new("node3", "test");
        node3.add_dependency("node2");

        graph.add_node(node1).unwrap();
        graph.add_node(node2).unwrap();
        graph.add_node(node3).unwrap();
        graph.build_edges().unwrap();

        assert!(graph.detect_cycles().is_some());
        assert!(graph.validate().is_err());
    }

    #[test]
    fn test_entry_points() {
        let mut graph = PipelineGraph::new();

        let node1 = PipelineNode::new("node1", "test");
        let mut node2 = PipelineNode::new("node2", "test");
        node2.add_dependency("node1");

        graph.add_node(node1).unwrap();
        graph.add_node(node2).unwrap();
        graph.build_edges().unwrap();

        assert_eq!(graph.entry_points(), &["node1"]);
    }
}
