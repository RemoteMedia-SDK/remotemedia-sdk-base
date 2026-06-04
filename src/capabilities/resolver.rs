//! Pipeline Capability Resolver (spec 023)
//!
//! This module provides the `CapabilityResolver` which automatically resolves,
//! propagates, and validates media capabilities during pipeline construction.
//!
//! # Resolution Algorithm
//!
//! The resolver uses a two-pass algorithm:
//!
//! 1. **Forward Pass** (topological order):
//!    - Static nodes: Use `media_capabilities()` directly
//!    - Configured nodes: Use factory's `media_capabilities(params)`
//!    - Passthrough nodes: Inherit output from upstream
//!    - RuntimeDiscovered nodes: Use `potential_capabilities()` (provisional)
//!    - Adaptive nodes: Mark as `NeedsReverse`
//!
//! 2. **Reverse Pass** (reverse topological order):
//!    - Adaptive nodes: Set output to match downstream input requirements
//!
//! # Two-Phase Resolution (RuntimeDiscovered)
//!
//! For nodes with `RuntimeDiscovered` behavior:
//! - **Phase 1**: Use `potential_capabilities()` for early validation
//! - **Phase 2**: After `node.initialize()`, call `revalidate()` with actual capabilities
//!
//! # Example
//!
//! ```ignore
//! let resolver = CapabilityResolver::new(&registry);
//! let ctx = resolver.resolve(&graph, &params)?;
//!
//! // Check for errors
//! if ctx.has_errors() {
//!     for error in &ctx.errors {
//!         eprintln!("Mismatch: {}", error);
//!     }
//! }
//!
//! // Query resolved capabilities
//! if let Some(caps) = ctx.resolved.get("whisper") {
//!     println!("Whisper input: {:?}", caps.capabilities.default_input());
//! }
//! ```

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::constraints::MediaCapabilities;
use super::dynamic::{
    CapabilityBehavior, ResolutionContext, ResolutionState, ResolvedCapabilities,
};
use crate::nodes::streaming_node::StreamingNodeRegistry;
use crate::Error;

// =============================================================================
// CapabilityHints (spec 023 - US6)
// =============================================================================

/// Hints for guiding capability resolution when multiple valid values exist.
///
/// Users can provide hints in the manifest to resolve ambiguity during
/// capability negotiation. For example, when an Adaptive node could output
/// multiple sample rates that all satisfy the downstream requirements, hints
/// can specify a preferred value.
///
/// # Example (in manifest YAML)
///
/// ```yaml
/// capability_hints:
///   resample:
///     preferred_sample_rate: 16000
///     preferred_channels: 1
///   mic:
///     prefer_exact: true  # Prefer exact constraints over ranges
/// ```
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CapabilityHints {
    /// Per-node hints (node_id -> hint configuration)
    #[serde(default)]
    pub nodes: HashMap<String, NodeHints>,

    /// Global preference: prefer lower sample rates when multiple are valid
    #[serde(default)]
    pub prefer_lower_sample_rate: bool,

    /// Global preference: prefer lower channel counts when multiple are valid
    #[serde(default)]
    pub prefer_lower_channels: bool,

    /// Global preference: prefer exact constraints over ranges
    #[serde(default)]
    pub prefer_exact: bool,
}

/// Hints for a specific node.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NodeHints {
    /// Preferred sample rate when multiple are valid
    #[serde(default)]
    pub preferred_sample_rate: Option<u32>,

    /// Preferred channel count when multiple are valid
    #[serde(default)]
    pub preferred_channels: Option<u32>,

    /// Preferred audio format (e.g., "f32", "i16")
    #[serde(default)]
    pub preferred_format: Option<String>,

    /// Whether to prefer exact constraints for this node
    #[serde(default)]
    pub prefer_exact: Option<bool>,
}

impl CapabilityHints {
    /// Create empty hints (no preferences).
    pub fn new() -> Self {
        Self::default()
    }

    /// Get hints for a specific node.
    pub fn get_node_hints(&self, node_id: &str) -> Option<&NodeHints> {
        self.nodes.get(node_id)
    }

    /// Get preferred sample rate for a node (node-specific or global).
    pub fn preferred_sample_rate(&self, node_id: &str) -> Option<u32> {
        self.nodes
            .get(node_id)
            .and_then(|h| h.preferred_sample_rate)
    }

    /// Get preferred channel count for a node (node-specific or global).
    pub fn preferred_channels(&self, node_id: &str) -> Option<u32> {
        self.nodes.get(node_id).and_then(|h| h.preferred_channels)
    }

    /// Check if exact constraints are preferred for a node.
    pub fn prefers_exact(&self, node_id: &str) -> bool {
        self.nodes
            .get(node_id)
            .and_then(|h| h.prefer_exact)
            .unwrap_or(self.prefer_exact)
    }
}

// =============================================================================
// CapabilityResolver
// =============================================================================

/// Resolves media capabilities for pipeline nodes.
///
/// The resolver queries the `StreamingNodeRegistry` to get capability information
/// from node factories, then applies the resolution algorithm to determine
/// final capabilities for each node in the pipeline.
pub struct CapabilityResolver<'a> {
    /// Reference to the streaming node registry
    registry: &'a StreamingNodeRegistry,
}

impl<'a> CapabilityResolver<'a> {
    /// Create a new resolver using the node registry.
    ///
    /// The resolver will query factory capabilities from the registry
    /// during resolution.
    pub fn new(registry: &'a StreamingNodeRegistry) -> Self {
        Self { registry }
    }

    /// Resolve capabilities for a pipeline (Phase 1).
    ///
    /// This method performs the full resolution algorithm:
    /// 1. Build connection graph from node definitions
    /// 2. Forward pass to resolve Static, Configured, Passthrough, RuntimeDiscovered
    /// 3. Reverse pass to resolve Adaptive nodes
    /// 4. Validation pass to check all connections
    ///
    /// # Arguments
    /// * `nodes` - Node definitions (node_id, node_type)
    /// * `connections` - Edge definitions (from_node_id, to_node_id)
    /// * `params` - Node parameters (node_id -> params)
    ///
    /// # Returns
    /// * `Ok(ResolutionContext)` - Resolution context with all resolved capabilities
    /// * `Err(Error)` - If resolution fails (missing node types, cycles, etc.)
    pub fn resolve(
        &self,
        nodes: &[(String, String)],       // (node_id, node_type)
        connections: &[(String, String)], // (from_node_id, to_node_id)
        params: &HashMap<String, Value>,
    ) -> Result<ResolutionContext, Error> {
        self.resolve_with_hints(nodes, connections, params, &CapabilityHints::new())
    }

    /// Resolve capabilities for a pipeline with user-provided hints.
    ///
    /// This is the same as `resolve()` but accepts `CapabilityHints` to guide
    /// resolution when multiple valid values exist (e.g., Adaptive nodes).
    ///
    /// # Arguments
    /// * `nodes` - Node definitions (node_id, node_type)
    /// * `connections` - Edge definitions (from_node_id, to_node_id)
    /// * `params` - Node parameters (node_id -> params)
    /// * `hints` - User-provided hints for ambiguous resolution
    ///
    /// # Returns
    /// * `Ok(ResolutionContext)` - Resolution context with all resolved capabilities
    /// * `Err(Error)` - If resolution fails (missing node types, cycles, etc.)
    pub fn resolve_with_hints(
        &self,
        nodes: &[(String, String)],       // (node_id, node_type)
        connections: &[(String, String)], // (from_node_id, to_node_id)
        params: &HashMap<String, Value>,
        hints: &CapabilityHints,
    ) -> Result<ResolutionContext, Error> {
        let mut ctx = ResolutionContext::new();

        // Step 1: Build connection graph and set behaviors
        self.build_graph(&mut ctx, nodes, connections, params)?;

        // Step 2: Forward pass (topological order)
        let topo_order = self.topological_sort(&ctx, nodes)?;
        self.forward_pass(&mut ctx, &topo_order)?;

        // Step 3: Reverse pass for Adaptive nodes (with hints)
        self.reverse_pass_with_hints(&mut ctx, &topo_order, hints)?;

        // Step 4: Validation pass
        self.validation_pass(&mut ctx, connections)?;

        Ok(ctx)
    }

    /// Re-validate after RuntimeDiscovered nodes report actual capabilities (Phase 2).
    ///
    /// Called after `node.initialize()` completes for RuntimeDiscovered nodes.
    /// Updates the node's capabilities with actual values and re-validates
    /// affected connections.
    ///
    /// # Arguments
    /// * `ctx` - Resolution context to update
    /// * `node_id` - Node that was initialized
    /// * `actual` - Actual capabilities discovered during initialization
    pub fn revalidate(
        &self,
        ctx: &mut ResolutionContext,
        node_id: &str,
        actual: MediaCapabilities,
    ) -> Result<(), Error> {
        // Update the resolved capabilities
        if let Some(resolved) = ctx.resolved.get_mut(node_id) {
            resolved.confirm_actual(actual);
        } else {
            return Err(Error::Execution(format!(
                "Cannot revalidate unknown node: {}",
                node_id
            )));
        }

        // Re-validate connections from this node
        let downstream: Vec<String> = ctx
            .downstream_nodes(node_id)
            .iter()
            .map(|s| s.to_string())
            .collect();

        for to_node in downstream {
            self.validate_connection(ctx, node_id, &to_node)?;
        }

        // Re-validate connections to this node
        let upstream: Vec<String> = ctx
            .upstream_nodes(node_id)
            .iter()
            .map(|s| s.to_string())
            .collect();

        for from_node in upstream {
            self.validate_connection(ctx, &from_node, node_id)?;
        }

        Ok(())
    }

    /// Re-validate and propagate capabilities after RuntimeDiscovered node initialization (spec 025).
    ///
    /// This method extends `revalidate()` by also propagating the actual capabilities
    /// to downstream Adaptive and Passthrough nodes. It:
    ///
    /// 1. Updates the source node's capabilities with actual values
    /// 2. Re-validates connections from this node
    /// 3. Creates `CapabilityNotification` for each downstream Adaptive/Passthrough node
    /// 4. Stores notifications in `ctx.pending_updates` for later application
    ///
    /// The caller (typically `SessionRouter`) is responsible for:
    /// - Calling `node.configure_from_upstream()` for each pending update
    /// - Clearing pending updates after application
    ///
    /// # Arguments
    /// * `ctx` - Resolution context to update
    /// * `node_id` - Node that was initialized (RuntimeDiscovered source)
    /// * `actual` - Actual capabilities discovered during initialization
    ///
    /// # Example
    ///
    /// ```ignore
    /// // After MicInput discovers actual 48kHz output
    /// resolver.revalidate_and_propagate(&mut ctx, "mic", actual_caps)?;
    ///
    /// // Apply pending updates to downstream nodes
    /// for node_id in ctx.nodes_with_pending_updates() {
    ///     if let Some(update) = ctx.take_pending_update(node_id) {
    ///         node.configure_from_upstream(&update.upstream_output)?;
    ///     }
    /// }
    /// ```
    pub fn revalidate_and_propagate(
        &self,
        ctx: &mut ResolutionContext,
        node_id: &str,
        actual: MediaCapabilities,
    ) -> Result<(), Error> {
        use super::dynamic::CapabilityNotification;

        tracing::info!(
            "[spec025] Starting capability re-propagation from node '{}'",
            node_id
        );

        // Step 1: Update the source node's capabilities (same as revalidate)
        if let Some(resolved) = ctx.resolved.get_mut(node_id) {
            resolved.confirm_actual(actual.clone());
            tracing::debug!(
                "[spec025] Updated '{}' capabilities: provisional=false",
                node_id
            );
        } else {
            tracing::error!("[spec025] Cannot revalidate unknown node: {}", node_id);
            return Err(Error::Execution(format!(
                "Cannot revalidate unknown node: {}",
                node_id
            )));
        }

        // Step 2: Get downstream nodes and their behaviors
        let downstream: Vec<String> = ctx
            .downstream_nodes(node_id)
            .iter()
            .map(|s| s.to_string())
            .collect();

        tracing::debug!(
            "[spec025] Node '{}' has {} downstream node(s): {:?}",
            node_id,
            downstream.len(),
            downstream
        );

        // Step 3: Create pending updates for Adaptive and Passthrough downstream nodes
        let mut pending_count = 0;
        for to_node in &downstream {
            let behavior = ctx.get_behavior(to_node);

            match behavior {
                CapabilityBehavior::Adaptive | CapabilityBehavior::Passthrough => {
                    // Create a notification for this downstream node
                    let notification = CapabilityNotification::new(
                        to_node.clone(),
                        node_id.to_string(),
                        actual.clone(),
                    );
                    ctx.add_pending_update(notification);
                    pending_count += 1;

                    tracing::debug!(
                        "[spec025] Created pending update for '{}' (behavior: {:?}) from '{}'",
                        to_node,
                        behavior,
                        node_id
                    );
                }
                _ => {
                    tracing::trace!(
                        "[spec025] Skipping '{}' (behavior: {:?}) - does not propagate",
                        to_node,
                        behavior
                    );
                }
            }
        }

        tracing::info!(
            "[spec025] Created {} pending update(s) for downstream nodes of '{}'",
            pending_count,
            node_id
        );

        // Step 4: Re-validate connections from this node
        // Clear previous errors to detect new mismatches
        let errors_before = ctx.errors.len();

        for to_node in &downstream {
            self.validate_connection(ctx, node_id, to_node)?;
        }

        // Step 5: Re-validate connections to this node
        let upstream: Vec<String> = ctx
            .upstream_nodes(node_id)
            .iter()
            .map(|s| s.to_string())
            .collect();

        for from_node in upstream {
            self.validate_connection(ctx, &from_node, node_id)?;
        }

        // Step 6: Check if validation produced any new errors
        if ctx.errors.len() > errors_before {
            let new_errors = &ctx.errors[errors_before..];
            let error_msg = new_errors
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join("; ");

            tracing::error!(
                "[spec025] Re-validation failed for '{}': {}",
                node_id,
                error_msg
            );

            return Err(Error::Execution(format!(
                "Capability mismatch after '{}' discovered actual capabilities: {}",
                node_id, error_msg
            )));
        }

        tracing::debug!(
            "[spec025] Re-propagation complete for '{}', {} pending update(s) queued",
            node_id,
            ctx.nodes_with_pending_updates().len()
        );

        Ok(())
    }

    /// Compute the order for applying capability updates (spec 025).
    ///
    /// Returns nodes that need to receive capability updates in topological order,
    /// starting from the source node and traversing downstream. Only includes
    /// Adaptive and Passthrough nodes that should receive updates.
    ///
    /// # Arguments
    /// * `ctx` - Resolution context with graph structure
    /// * `source_node_id` - The RuntimeDiscovered node that triggered propagation
    ///
    /// # Returns
    /// Vector of node IDs in the order they should receive updates
    ///
    /// # Example
    ///
    /// ```ignore
    /// // For pipeline: mic -> resample -> vad -> whisper
    /// // Where mic is RuntimeDiscovered, resample is Adaptive, vad is Passthrough
    /// let order = resolver.compute_propagation_order(&ctx, "mic");
    /// assert_eq!(order, vec!["resample", "vad"]);
    /// // whisper is Static so it's not included
    /// ```
    pub fn compute_propagation_order(
        &self,
        ctx: &ResolutionContext,
        source_node_id: &str,
    ) -> Vec<String> {
        let mut result = Vec::new();
        let mut visited = std::collections::HashSet::new();
        let mut queue = std::collections::VecDeque::new();

        // Start with immediate downstream nodes of the source
        for downstream in ctx.downstream_nodes(source_node_id) {
            queue.push_back(downstream.to_string());
        }

        // BFS traversal to maintain topological order
        while let Some(node_id) = queue.pop_front() {
            if visited.contains(&node_id) {
                continue;
            }
            visited.insert(node_id.clone());

            let behavior = ctx.get_behavior(&node_id);

            match behavior {
                CapabilityBehavior::Adaptive | CapabilityBehavior::Passthrough => {
                    // This node needs an update
                    result.push(node_id.clone());

                    // Continue propagation to its downstream nodes
                    for downstream in ctx.downstream_nodes(&node_id) {
                        if !visited.contains(downstream) {
                            queue.push_back(downstream.to_string());
                        }
                    }
                }
                _ => {
                    // Static/Configured/RuntimeDiscovered don't propagate further
                    // (RuntimeDiscovered will trigger their own propagation when they init)
                }
            }
        }

        result
    }

    /// Get resolved capabilities for a node.
    pub fn get_resolved<'b>(
        &self,
        ctx: &'b ResolutionContext,
        node_id: &str,
    ) -> Option<&'b ResolvedCapabilities> {
        ctx.resolved.get(node_id)
    }

    // =========================================================================
    // Internal Methods
    // =========================================================================

    /// Build the connection graph and set behaviors.
    fn build_graph(
        &self,
        ctx: &mut ResolutionContext,
        nodes: &[(String, String)],
        connections: &[(String, String)],
        params: &HashMap<String, Value>,
    ) -> Result<(), Error> {
        // Add nodes with their behaviors and types
        for (node_id, node_type) in nodes {
            let behavior = self.registry.get_capability_behavior(node_type);
            ctx.set_behavior(node_id, behavior);
            ctx.set_state(node_id, ResolutionState::Pending);

            // Store node type
            ctx.node_types.insert(node_id.clone(), node_type.clone());

            // Store params
            if let Some(node_params) = params.get(node_id) {
                ctx.params.insert(node_id.clone(), node_params.clone());
            } else {
                ctx.params.insert(node_id.clone(), Value::Null);
            }
        }

        // Add connections
        for (from, to) in connections {
            ctx.add_connection(from, to);
        }

        Ok(())
    }

    /// Compute topological sort of nodes.
    fn topological_sort(
        &self,
        ctx: &ResolutionContext,
        nodes: &[(String, String)],
    ) -> Result<Vec<String>, Error> {
        let mut in_degree: HashMap<String, usize> = HashMap::new();
        let mut result = Vec::new();
        let mut queue = Vec::new();

        // Initialize in-degrees
        for (node_id, _) in nodes {
            in_degree.insert(node_id.clone(), 0);
        }

        // Count incoming edges
        for targets in ctx.connections.values() {
            for target in targets {
                *in_degree.entry(target.clone()).or_default() += 1;
            }
        }

        // Start with nodes that have no incoming edges
        for (node_id, degree) in &in_degree {
            if *degree == 0 {
                queue.push(node_id.clone());
            }
        }

        // Process queue
        while let Some(node_id) = queue.pop() {
            result.push(node_id.clone());

            if let Some(targets) = ctx.connections.get(&node_id) {
                for target in targets {
                    if let Some(degree) = in_degree.get_mut(target) {
                        *degree -= 1;
                        if *degree == 0 {
                            queue.push(target.clone());
                        }
                    }
                }
            }
        }

        // Check for cycles
        if result.len() != nodes.len() {
            return Err(Error::Execution(
                "Cycle detected in pipeline graph - capability resolution requires a DAG"
                    .to_string(),
            ));
        }

        Ok(result)
    }

    /// Forward pass: resolve Static, Configured, Passthrough, RuntimeDiscovered.
    fn forward_pass(
        &self,
        ctx: &mut ResolutionContext,
        topo_order: &[String],
    ) -> Result<(), Error> {
        for node_id in topo_order {
            let behavior = ctx.get_behavior(node_id);
            let params = ctx.params.get(node_id).cloned().unwrap_or(Value::Null);

            match behavior {
                CapabilityBehavior::Static | CapabilityBehavior::Configured => {
                    // Get capabilities from factory
                    self.resolve_static_or_configured(ctx, node_id, &params)?;
                }
                CapabilityBehavior::Passthrough => {
                    // Inherit from upstream
                    self.resolve_passthrough(ctx, node_id)?;
                }
                CapabilityBehavior::Adaptive => {
                    // Check if capabilities already have output defined (explicit config)
                    // If so, treat it as Configured instead of needing reverse pass
                    if let Some(caps) = self.get_factory_capabilities(ctx, node_id, &params) {
                        if caps.default_output().is_some() {
                            // Output is already defined - treat as Configured
                            // Update the behavior in context to reflect this
                            ctx.set_behavior(node_id, CapabilityBehavior::Configured);
                            self.resolve_static_or_configured(ctx, node_id, &params)?;
                        } else {
                            // No output defined - needs reverse pass to determine output
                            self.mark_needs_reverse(ctx, node_id, &params)?;
                        }
                    } else {
                        // No capabilities at all - still mark for reverse pass
                        self.mark_needs_reverse(ctx, node_id, &params)?;
                    }
                }
                CapabilityBehavior::RuntimeDiscovered => {
                    // Use potential capabilities (provisional)
                    self.resolve_runtime_discovered(ctx, node_id, &params)?;
                }
            }
        }

        Ok(())
    }

    /// Reverse pass: resolve Adaptive nodes (without hints).
    #[allow(dead_code)]
    fn reverse_pass(
        &self,
        ctx: &mut ResolutionContext,
        topo_order: &[String],
    ) -> Result<(), Error> {
        self.reverse_pass_with_hints(ctx, topo_order, &CapabilityHints::new())
    }

    /// Reverse pass: resolve Adaptive nodes with user-provided hints.
    fn reverse_pass_with_hints(
        &self,
        ctx: &mut ResolutionContext,
        topo_order: &[String],
        hints: &CapabilityHints,
    ) -> Result<(), Error> {
        // Process in reverse order
        for node_id in topo_order.iter().rev() {
            if ctx.get_state(node_id) == &ResolutionState::NeedsReverse {
                self.resolve_adaptive_with_hints(ctx, node_id, hints)?;
            }
        }

        Ok(())
    }

    /// Validation pass: check all connections.
    fn validation_pass(
        &self,
        ctx: &mut ResolutionContext,
        connections: &[(String, String)],
    ) -> Result<(), Error> {
        for (from, to) in connections {
            self.validate_connection(ctx, from, to)?;
        }
        Ok(())
    }

    /// Resolve Static or Configured node.
    fn resolve_static_or_configured(
        &self,
        ctx: &mut ResolutionContext,
        node_id: &str,
        params: &Value,
    ) -> Result<(), Error> {
        // Get node_type from behaviors (we need to look it up)
        // For now, we need to find the node_type from the params or stored info
        // Actually, we need to store node_type in the context

        // Try to get capabilities from registry using stored node type
        // We'll need to enhance the context to store node types

        // For now, check if we have capabilities in params (temporary workaround)
        // In a full implementation, we'd look up the node_type and query the registry

        let behavior = ctx.get_behavior(node_id);

        // Look up capabilities - for now we'll create empty if not found
        // The actual implementation would query registry.get_media_capabilities(node_type, params)
        if let Some(caps) = self.get_factory_capabilities(ctx, node_id, params) {
            let resolved = match behavior {
                CapabilityBehavior::Static => ResolvedCapabilities::from_static(node_id, caps),
                CapabilityBehavior::Configured => {
                    ResolvedCapabilities::from_configured(node_id, caps)
                }
                _ => unreachable!(),
            };
            ctx.resolved.insert(node_id.to_string(), resolved);
            ctx.set_state(node_id, ResolutionState::Complete);
        } else {
            // No capabilities declared - treat as passthrough
            ctx.set_state(node_id, ResolutionState::Complete);
        }

        Ok(())
    }

    /// Resolve Passthrough node (inherit from upstream).
    fn resolve_passthrough(&self, ctx: &mut ResolutionContext, node_id: &str) -> Result<(), Error> {
        let upstream = ctx.upstream_nodes(node_id);

        if upstream.is_empty() {
            // Source node with no upstream - can't be passthrough
            // Treat as having no constraints
            ctx.set_state(node_id, ResolutionState::Complete);
            return Ok(());
        }

        // Get output from first upstream node
        if let Some(upstream_output) = ctx.get_output(upstream[0]) {
            let resolved = ResolvedCapabilities::passthrough(node_id, upstream_output);
            ctx.resolved.insert(node_id.to_string(), resolved);
        }

        ctx.set_state(node_id, ResolutionState::Complete);
        Ok(())
    }

    /// Mark Adaptive node as needing reverse pass.
    fn mark_needs_reverse(
        &self,
        ctx: &mut ResolutionContext,
        node_id: &str,
        params: &Value,
    ) -> Result<(), Error> {
        // Get input capabilities (if any) from factory
        if let Some(caps) = self.get_factory_capabilities(ctx, node_id, params) {
            let resolved = ResolvedCapabilities::needs_reverse(node_id, caps);
            ctx.resolved.insert(node_id.to_string(), resolved);
        }

        ctx.set_state(node_id, ResolutionState::NeedsReverse);
        Ok(())
    }

    /// Resolve RuntimeDiscovered node (use potential capabilities).
    fn resolve_runtime_discovered(
        &self,
        ctx: &mut ResolutionContext,
        node_id: &str,
        params: &Value,
    ) -> Result<(), Error> {
        // Use potential capabilities for Phase 1 validation
        // These are broad capabilities that will be refined after device initialization
        if let Some(caps) = self.get_potential_capabilities(ctx, node_id, params) {
            let resolved = ResolvedCapabilities::provisional(node_id, caps);
            ctx.resolved.insert(node_id.to_string(), resolved);
        }

        ctx.set_state(node_id, ResolutionState::ResolvedForward);
        Ok(())
    }

    /// Get potential capabilities from factory using node_type lookup.
    fn get_potential_capabilities(
        &self,
        ctx: &ResolutionContext,
        node_id: &str,
        params: &Value,
    ) -> Option<MediaCapabilities> {
        // Look up the node_type for this node_id
        let node_type = ctx.get_node_type(node_id)?;

        // Query the registry for potential capabilities
        self.registry.get_potential_capabilities(node_type, params)
    }

    /// Resolve Adaptive node during reverse pass (without hints).
    #[allow(dead_code)]
    fn resolve_adaptive(&self, ctx: &mut ResolutionContext, node_id: &str) -> Result<(), Error> {
        self.resolve_adaptive_with_hints(ctx, node_id, &CapabilityHints::new())
    }

    /// Resolve Adaptive node during reverse pass with user-provided hints.
    ///
    /// Hints are applied when the downstream node accepts a range of values.
    /// For example, if downstream accepts sample_rate 8000-48000 and hints
    /// specify preferred_sample_rate=16000, the output will use 16000.
    fn resolve_adaptive_with_hints(
        &self,
        ctx: &mut ResolutionContext,
        node_id: &str,
        hints: &CapabilityHints,
    ) -> Result<(), Error> {
        let downstream: Vec<String> = ctx
            .downstream_nodes(node_id)
            .iter()
            .map(|s| s.to_string())
            .collect();

        if downstream.is_empty() {
            // No downstream - can't adapt, mark as complete
            ctx.set_state(node_id, ResolutionState::Complete);
            return Ok(());
        }

        // Get input requirements from first downstream node (clone to avoid borrow issues)
        let downstream_input = ctx.get_input(&downstream[0]).cloned();

        if let Some(input_constraints) = downstream_input {
            // Apply hints to refine the constraints if possible
            let refined_constraints =
                self.apply_hints_to_constraints(node_id, input_constraints, hints);

            // Update output to match downstream input (with hints applied)
            if let Some(resolved) = ctx.resolved.get_mut(node_id) {
                // Set output to match downstream input requirements
                resolved
                    .capabilities
                    .outputs
                    .insert("default".to_string(), refined_constraints);
                resolved.sources.insert(
                    "default".to_string(),
                    super::dynamic::CapabilitySource::Negotiated,
                );
                resolved.mark_complete();
            }
        }

        ctx.set_state(node_id, ResolutionState::Complete);
        Ok(())
    }

    /// Apply hints to refine media constraints.
    ///
    /// When constraints contain ranges, hints can narrow them to specific values.
    fn apply_hints_to_constraints(
        &self,
        node_id: &str,
        constraints: super::constraints::MediaConstraints,
        hints: &CapabilityHints,
    ) -> super::constraints::MediaConstraints {
        use super::constraints::{ConstraintValue, MediaConstraints};

        match constraints {
            MediaConstraints::Audio(audio) => {
                let mut refined = audio.clone();

                // Apply preferred sample rate if hint exists and constraint allows it
                if let Some(preferred_rate) = hints.preferred_sample_rate(node_id) {
                    if self.constraint_allows_value(&audio.sample_rate, preferred_rate) {
                        refined.sample_rate = Some(ConstraintValue::Exact(preferred_rate));
                    }
                }

                // Apply preferred channels if hint exists and constraint allows it
                if let Some(preferred_ch) = hints.preferred_channels(node_id) {
                    if self.constraint_allows_value(&audio.channels, preferred_ch) {
                        refined.channels = Some(ConstraintValue::Exact(preferred_ch));
                    }
                }

                // Apply preferred format if hint exists
                if let Some(ref preferred_fmt) = hints
                    .nodes
                    .get(node_id)
                    .and_then(|h| h.preferred_format.as_ref())
                {
                    if let Some(format) = self.parse_audio_format(preferred_fmt) {
                        refined.format = Some(ConstraintValue::Exact(format));
                    }
                }

                MediaConstraints::Audio(refined)
            }
            // For other constraint types, return as-is (hints only support audio for now)
            other => other,
        }
    }

    /// Check if a constraint allows a specific value.
    fn constraint_allows_value(
        &self,
        constraint: &Option<super::constraints::ConstraintValue<u32>>,
        value: u32,
    ) -> bool {
        use super::constraints::ConstraintValue;

        match constraint {
            None => true, // No constraint means any value is allowed
            Some(ConstraintValue::Exact(exact)) => *exact == value,
            Some(ConstraintValue::Range { min, max }) => value >= *min && value <= *max,
            Some(ConstraintValue::Set(values)) => values.contains(&value),
        }
    }

    /// Parse an audio format string into AudioSampleFormat.
    fn parse_audio_format(&self, format: &str) -> Option<super::constraints::AudioSampleFormat> {
        use super::constraints::AudioSampleFormat;

        match format.to_lowercase().as_str() {
            "f32" | "float32" => Some(AudioSampleFormat::F32),
            "i16" | "int16" | "s16" => Some(AudioSampleFormat::I16),
            "i32" | "int32" | "s32" => Some(AudioSampleFormat::I32),
            "u8" | "uint8" => Some(AudioSampleFormat::U8),
            _ => None,
        }
    }

    /// Validate a connection between two nodes.
    fn validate_connection(
        &self,
        ctx: &mut ResolutionContext,
        from_node: &str,
        to_node: &str,
    ) -> Result<(), Error> {
        // Get resolved capabilities for both nodes
        let from_caps = ctx.resolved.get(from_node);
        let to_caps = ctx.resolved.get(to_node);

        // If either node has no declared capabilities, skip validation
        let (from_resolved, to_resolved) = match (from_caps, to_caps) {
            (Some(from), Some(to)) => (from, to),
            _ => return Ok(()), // No validation needed
        };

        // Use the existing validation logic from the validation module
        use super::validation::validate_connection;

        let mismatches = validate_connection(
            from_node,
            &from_resolved.capabilities,
            to_node,
            &to_resolved.capabilities,
            "default", // source port
            "default", // target port
        );

        // Add any mismatches to context
        for mismatch in mismatches {
            ctx.add_error(mismatch);
        }

        Ok(())
    }

    /// Get capabilities from factory using node_type lookup.
    fn get_factory_capabilities(
        &self,
        ctx: &ResolutionContext,
        node_id: &str,
        params: &Value,
    ) -> Option<MediaCapabilities> {
        // Look up the node_type for this node_id
        let node_type = ctx.get_node_type(node_id)?;

        // Query the registry for capabilities
        self.registry.get_media_capabilities(node_type, params)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolver_creation() {
        let registry = StreamingNodeRegistry::new();
        let _resolver = CapabilityResolver::new(&registry);
    }

    #[test]
    fn test_empty_pipeline() {
        let registry = StreamingNodeRegistry::new();
        let resolver = CapabilityResolver::new(&registry);

        let nodes: Vec<(String, String)> = vec![];
        let connections: Vec<(String, String)> = vec![];
        let params = HashMap::new();

        let ctx = resolver.resolve(&nodes, &connections, &params).unwrap();
        assert!(ctx.resolved.is_empty());
        assert!(!ctx.has_errors());
    }

    #[test]
    fn test_topological_sort_linear() {
        let registry = StreamingNodeRegistry::new();
        let resolver = CapabilityResolver::new(&registry);

        let nodes = vec![
            ("a".to_string(), "NodeA".to_string()),
            ("b".to_string(), "NodeB".to_string()),
            ("c".to_string(), "NodeC".to_string()),
        ];
        let connections = vec![
            ("a".to_string(), "b".to_string()),
            ("b".to_string(), "c".to_string()),
        ];
        let params = HashMap::new();

        let ctx = resolver.resolve(&nodes, &connections, &params).unwrap();

        // All nodes should have been processed
        assert_eq!(ctx.states.len(), 3);
    }

    #[test]
    fn test_cycle_detection() {
        let registry = StreamingNodeRegistry::new();
        let resolver = CapabilityResolver::new(&registry);

        let nodes = vec![
            ("a".to_string(), "NodeA".to_string()),
            ("b".to_string(), "NodeB".to_string()),
        ];
        // Create a cycle: a -> b -> a
        let connections = vec![
            ("a".to_string(), "b".to_string()),
            ("b".to_string(), "a".to_string()),
        ];
        let params = HashMap::new();

        let result = resolver.resolve(&nodes, &connections, &params);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Cycle"));
    }

    #[test]
    fn test_resolution_performance_sc001() {
        // SC-001: Capability resolution should complete within 10ms
        use std::time::Instant;

        let registry = StreamingNodeRegistry::new();
        let resolver = CapabilityResolver::new(&registry);

        // Create a moderately complex pipeline with 10 nodes
        let nodes: Vec<_> = (0..10)
            .map(|i| (format!("node_{}", i), format!("NodeType{}", i)))
            .collect();

        // Create a linear chain of connections
        let connections: Vec<_> = (0..9)
            .map(|i| (format!("node_{}", i), format!("node_{}", i + 1)))
            .collect();

        let params = HashMap::new();

        // Measure resolution time
        let start = Instant::now();
        let _result = resolver.resolve(&nodes, &connections, &params);
        let elapsed = start.elapsed();

        // SC-001: Must be under 10ms
        assert!(
            elapsed.as_millis() < 10,
            "Resolution took {}ms, expected <10ms (SC-001)",
            elapsed.as_millis()
        );
    }

    #[test]
    fn test_introspection_performance_sc005() {
        // SC-005: Introspection queries should complete within 1ms
        use super::super::constraints::MediaCapabilities;
        use super::super::dynamic::ResolvedCapabilities;
        use std::time::Instant;

        let mut ctx = ResolutionContext::new();

        // Add some resolved capabilities
        for i in 0..100 {
            let node_id = format!("node_{}", i);
            let caps = MediaCapabilities {
                inputs: HashMap::new(),
                outputs: HashMap::new(),
            };
            ctx.resolved.insert(
                node_id.clone(),
                ResolvedCapabilities::from_static(&node_id, caps),
            );
        }

        // Measure introspection time
        let start = Instant::now();
        for i in 0..100 {
            let _caps = ctx.resolved.get(&format!("node_{}", i));
        }
        let elapsed = start.elapsed();

        // SC-005: Must be under 1ms for 100 queries
        assert!(
            elapsed.as_micros() < 1000,
            "Introspection took {}μs for 100 queries, expected <1000μs (SC-005)",
            elapsed.as_micros()
        );
    }

    // =========================================================================
    // Spec 025: Capability Re-propagation Tests
    // =========================================================================

    #[test]
    fn test_revalidate_and_propagate_basic() {
        // T010 [US1]: Test basic revalidate_and_propagate functionality
        // Scenario: RuntimeDiscovered source -> Adaptive resample -> Static sink
        // When source reports actual 48kHz, resample should receive pending update
        use super::super::constraints::{
            AudioConstraints, AudioSampleFormat, ConstraintValue, MediaConstraints,
        };
        use super::super::dynamic::{CapabilityBehavior, ResolvedCapabilities};

        let registry = StreamingNodeRegistry::new();
        let resolver = CapabilityResolver::new(&registry);

        // Build a pipeline: mic (RuntimeDiscovered) -> resample (Adaptive) -> whisper (Static)
        let connections = vec![
            ("mic".to_string(), "resample".to_string()),
            ("resample".to_string(), "whisper".to_string()),
        ];

        let mut ctx = ResolutionContext::new();

        // Set up behaviors
        ctx.set_behavior("mic", CapabilityBehavior::RuntimeDiscovered);
        ctx.set_behavior("resample", CapabilityBehavior::Adaptive);
        ctx.set_behavior("whisper", CapabilityBehavior::Static);

        // Add node types
        ctx.node_types
            .insert("mic".to_string(), "MicInput".to_string());
        ctx.node_types
            .insert("resample".to_string(), "FastResampleNode".to_string());
        ctx.node_types
            .insert("whisper".to_string(), "RustWhisperNode".to_string());

        // Add connections
        for (from, to) in &connections {
            ctx.add_connection(from, to);
        }

        // Set up initial provisional capabilities for mic (Phase 1)
        let potential_caps =
            MediaCapabilities::with_output(MediaConstraints::Audio(AudioConstraints {
                sample_rate: Some(ConstraintValue::Range {
                    min: 8000,
                    max: 192000,
                }),
                channels: Some(ConstraintValue::Range { min: 1, max: 8 }),
                format: Some(ConstraintValue::Exact(AudioSampleFormat::F32)),
            }));
        let provisional = ResolvedCapabilities::provisional("mic", potential_caps);
        ctx.resolved.insert("mic".to_string(), provisional);

        // Simulate mic discovering actual 48kHz after initialization
        let actual_caps =
            MediaCapabilities::with_output(MediaConstraints::Audio(AudioConstraints {
                sample_rate: Some(ConstraintValue::Exact(48000)),
                channels: Some(ConstraintValue::Exact(2)),
                format: Some(ConstraintValue::Exact(AudioSampleFormat::F32)),
            }));

        // Call revalidate_and_propagate (this is what we're testing)
        let result = resolver.revalidate_and_propagate(&mut ctx, "mic", actual_caps.clone());
        assert!(result.is_ok(), "revalidate_and_propagate should succeed");

        // Verify: mic's capabilities were updated
        let mic_resolved = ctx
            .resolved
            .get("mic")
            .expect("mic should have resolved caps");
        assert!(
            !mic_resolved.provisional,
            "mic should no longer be provisional"
        );

        // Verify: resample should have a pending update
        let pending = ctx.get_pending_update("resample");
        assert!(pending.is_some(), "resample should have a pending update");

        let update = pending.unwrap();
        assert_eq!(update.upstream_node_id, "mic");
        assert_eq!(update.node_id, "resample");

        // Verify the upstream_output contains the actual 48kHz sample rate
        if let Some(MediaConstraints::Audio(audio)) = update.upstream_output.default_output() {
            assert_eq!(
                audio.sample_rate,
                Some(ConstraintValue::Exact(48000)),
                "Pending update should contain 48kHz sample rate"
            );
        } else {
            panic!("Expected audio constraints in pending update");
        }
    }

    #[test]
    fn test_compute_propagation_order_basic() {
        // T013: Test compute_propagation_order returns nodes in topological order
        use super::super::dynamic::CapabilityBehavior;

        let registry = StreamingNodeRegistry::new();
        let resolver = CapabilityResolver::new(&registry);

        let mut ctx = ResolutionContext::new();

        // Set up behaviors: mic -> resample -> vad -> whisper
        ctx.set_behavior("mic", CapabilityBehavior::RuntimeDiscovered);
        ctx.set_behavior("resample", CapabilityBehavior::Adaptive);
        ctx.set_behavior("vad", CapabilityBehavior::Passthrough);
        ctx.set_behavior("whisper", CapabilityBehavior::Static);

        // Add connections
        ctx.add_connection("mic", "resample");
        ctx.add_connection("resample", "vad");
        ctx.add_connection("vad", "whisper");

        // Compute propagation order from mic
        let order = resolver.compute_propagation_order(&ctx, "mic");

        // Should include resample and vad (Adaptive and Passthrough)
        // Should NOT include whisper (Static)
        assert_eq!(order.len(), 2);
        assert_eq!(order[0], "resample");
        assert_eq!(order[1], "vad");
    }

    #[test]
    fn test_compute_propagation_order_stops_at_static() {
        // Verify propagation stops at Static nodes
        use super::super::dynamic::CapabilityBehavior;

        let registry = StreamingNodeRegistry::new();
        let resolver = CapabilityResolver::new(&registry);

        let mut ctx = ResolutionContext::new();

        // Set up: source -> adaptive1 -> static -> adaptive2
        // Propagation should stop at static, not reaching adaptive2
        ctx.set_behavior("source", CapabilityBehavior::RuntimeDiscovered);
        ctx.set_behavior("adaptive1", CapabilityBehavior::Adaptive);
        ctx.set_behavior("static", CapabilityBehavior::Static);
        ctx.set_behavior("adaptive2", CapabilityBehavior::Adaptive);

        ctx.add_connection("source", "adaptive1");
        ctx.add_connection("adaptive1", "static");
        ctx.add_connection("static", "adaptive2");

        let order = resolver.compute_propagation_order(&ctx, "source");

        // Only adaptive1 should be included, not adaptive2 (blocked by static)
        assert_eq!(order.len(), 1);
        assert_eq!(order[0], "adaptive1");
    }

    // =========================================================================
    // Spec 025 - User Story 2: Cascade Propagation Tests
    // =========================================================================

    #[test]
    fn test_cascade_propagation_order() {
        // T020 [US2]: Test cascade propagation through multiple Adaptive nodes
        // Chain: source (RuntimeDiscovered) -> resample1 (Adaptive) -> resample2 (Adaptive) -> sink (Static)
        use super::super::dynamic::CapabilityBehavior;

        let registry = StreamingNodeRegistry::new();
        let resolver = CapabilityResolver::new(&registry);

        let mut ctx = ResolutionContext::new();

        // Set up chain: source -> resample1 -> resample2 -> sink
        ctx.set_behavior("source", CapabilityBehavior::RuntimeDiscovered);
        ctx.set_behavior("resample1", CapabilityBehavior::Adaptive);
        ctx.set_behavior("resample2", CapabilityBehavior::Adaptive);
        ctx.set_behavior("sink", CapabilityBehavior::Static);

        ctx.add_connection("source", "resample1");
        ctx.add_connection("resample1", "resample2");
        ctx.add_connection("resample2", "sink");

        let order = resolver.compute_propagation_order(&ctx, "source");

        // Both resample nodes should be in order
        assert_eq!(order.len(), 2);
        assert_eq!(order[0], "resample1");
        assert_eq!(order[1], "resample2");
    }

    #[test]
    fn test_cascade_propagation_fan_out() {
        // T025 [US2]: Test fan-out scenarios (one source, multiple downstream paths)
        // source -> resample -> branch1 (Passthrough)
        //                    -> branch2 (Adaptive)
        use super::super::dynamic::CapabilityBehavior;

        let registry = StreamingNodeRegistry::new();
        let resolver = CapabilityResolver::new(&registry);

        let mut ctx = ResolutionContext::new();

        ctx.set_behavior("source", CapabilityBehavior::RuntimeDiscovered);
        ctx.set_behavior("resample", CapabilityBehavior::Adaptive);
        ctx.set_behavior("branch1", CapabilityBehavior::Passthrough);
        ctx.set_behavior("branch2", CapabilityBehavior::Adaptive);
        ctx.set_behavior("sink1", CapabilityBehavior::Static);
        ctx.set_behavior("sink2", CapabilityBehavior::Static);

        // Fan-out topology
        ctx.add_connection("source", "resample");
        ctx.add_connection("resample", "branch1");
        ctx.add_connection("resample", "branch2");
        ctx.add_connection("branch1", "sink1");
        ctx.add_connection("branch2", "sink2");

        let order = resolver.compute_propagation_order(&ctx, "source");

        // resample, branch1, and branch2 should all receive updates
        assert_eq!(order.len(), 3);
        assert_eq!(order[0], "resample");
        // branch1 and branch2 order may vary (BFS from resample)
        assert!(order.contains(&"branch1".to_string()));
        assert!(order.contains(&"branch2".to_string()));
    }

    #[test]
    fn test_multi_adaptive_cascade_creates_pending_updates() {
        // T020/T022/T023 [US2]: Test that cascade creates pending updates for all Adaptive nodes
        use super::super::constraints::{
            AudioConstraints, AudioSampleFormat, ConstraintValue, MediaConstraints,
        };
        use super::super::dynamic::{CapabilityBehavior, ResolvedCapabilities};

        let registry = StreamingNodeRegistry::new();
        let resolver = CapabilityResolver::new(&registry);

        let mut ctx = ResolutionContext::new();

        // Chain: mic -> resample1 -> resample2 -> whisper
        ctx.set_behavior("mic", CapabilityBehavior::RuntimeDiscovered);
        ctx.set_behavior("resample1", CapabilityBehavior::Adaptive);
        ctx.set_behavior("resample2", CapabilityBehavior::Adaptive);
        ctx.set_behavior("whisper", CapabilityBehavior::Static);

        ctx.node_types
            .insert("mic".to_string(), "MicInput".to_string());
        ctx.node_types
            .insert("resample1".to_string(), "FastResampleNode".to_string());
        ctx.node_types
            .insert("resample2".to_string(), "FastResampleNode".to_string());
        ctx.node_types
            .insert("whisper".to_string(), "RustWhisperNode".to_string());

        ctx.add_connection("mic", "resample1");
        ctx.add_connection("resample1", "resample2");
        ctx.add_connection("resample2", "whisper");

        // Set up provisional capabilities for mic
        let potential_caps =
            MediaCapabilities::with_output(MediaConstraints::Audio(AudioConstraints {
                sample_rate: Some(ConstraintValue::Range {
                    min: 8000,
                    max: 192000,
                }),
                channels: Some(ConstraintValue::Range { min: 1, max: 8 }),
                format: Some(ConstraintValue::Exact(AudioSampleFormat::F32)),
            }));
        ctx.resolved.insert(
            "mic".to_string(),
            ResolvedCapabilities::provisional("mic", potential_caps),
        );

        // Mic discovers actual 48kHz
        let actual_caps =
            MediaCapabilities::with_output(MediaConstraints::Audio(AudioConstraints {
                sample_rate: Some(ConstraintValue::Exact(48000)),
                channels: Some(ConstraintValue::Exact(2)),
                format: Some(ConstraintValue::Exact(AudioSampleFormat::F32)),
            }));

        // Propagate
        let result = resolver.revalidate_and_propagate(&mut ctx, "mic", actual_caps);
        assert!(result.is_ok());

        // Current implementation only creates pending update for immediate downstream (resample1)
        // For full cascade support, both resample1 and resample2 need updates
        // This test documents current behavior - resample1 gets an update directly from mic
        assert!(
            ctx.get_pending_update("resample1").is_some(),
            "resample1 should have pending update"
        );

        let update = ctx.get_pending_update("resample1").unwrap();
        assert_eq!(update.upstream_node_id, "mic");

        // Note: resample2 doesn't get an update directly from mic because the current
        // implementation only propagates to immediate downstream nodes.
        // For full cascade support, SessionRouter.apply_pending_updates() should
        // iteratively apply updates and re-propagate when Adaptive nodes are configured.
    }

    // =========================================================================
    // Spec 025 - User Story 3: Incompatibility Detection Tests
    // =========================================================================

    #[test]
    fn test_revalidate_detects_phase2_mismatch() {
        // T026 [US3]: Test that revalidate_and_propagate() detects incompatible actual capabilities
        // Scenario: Mic (RuntimeDiscovered) -> Whisper (Static, requires 16kHz)
        // Mic discovers actual 48kHz which is incompatible with Whisper's 16kHz requirement
        use super::super::constraints::{
            AudioConstraints, AudioSampleFormat, ConstraintValue, MediaConstraints,
        };
        use super::super::dynamic::{CapabilityBehavior, ResolvedCapabilities};

        let registry = StreamingNodeRegistry::new();
        let resolver = CapabilityResolver::new(&registry);

        let mut ctx = ResolutionContext::new();

        // Set up: mic (RuntimeDiscovered) -> whisper (Static, requires 16kHz)
        ctx.set_behavior("mic", CapabilityBehavior::RuntimeDiscovered);
        ctx.set_behavior("whisper", CapabilityBehavior::Static);

        ctx.node_types
            .insert("mic".to_string(), "MicInput".to_string());
        ctx.node_types
            .insert("whisper".to_string(), "RustWhisperNode".to_string());

        ctx.add_connection("mic", "whisper");

        // Set up mic's provisional capabilities (wide range - passes Phase 1)
        let potential_caps =
            MediaCapabilities::with_output(MediaConstraints::Audio(AudioConstraints {
                sample_rate: Some(ConstraintValue::Range {
                    min: 8000,
                    max: 192000,
                }),
                channels: Some(ConstraintValue::Range { min: 1, max: 8 }),
                format: Some(ConstraintValue::Exact(AudioSampleFormat::F32)),
            }));
        ctx.resolved.insert(
            "mic".to_string(),
            ResolvedCapabilities::provisional("mic", potential_caps),
        );

        // Set up whisper's static capabilities (requires exactly 16kHz)
        let whisper_caps =
            MediaCapabilities::with_input(MediaConstraints::Audio(AudioConstraints {
                sample_rate: Some(ConstraintValue::Exact(16000)),
                channels: Some(ConstraintValue::Exact(1)),
                format: Some(ConstraintValue::Exact(AudioSampleFormat::F32)),
            }));
        ctx.resolved.insert(
            "whisper".to_string(),
            ResolvedCapabilities::from_static("whisper", whisper_caps),
        );

        // Mic discovers actual 48kHz - this is incompatible with whisper's 16kHz
        let actual_caps =
            MediaCapabilities::with_output(MediaConstraints::Audio(AudioConstraints {
                sample_rate: Some(ConstraintValue::Exact(48000)),
                channels: Some(ConstraintValue::Exact(2)),
                format: Some(ConstraintValue::Exact(AudioSampleFormat::F32)),
            }));

        // revalidate_and_propagate should detect the mismatch and return an error
        let result = resolver.revalidate_and_propagate(&mut ctx, "mic", actual_caps);

        // The result should be an error because 48kHz is incompatible with 16kHz
        assert!(result.is_err(), "Should detect sample rate mismatch");

        // Verify the error message mentions the incompatibility
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("mic") || err_msg.contains("whisper") || err_msg.contains("sample"),
            "Error should mention nodes or constraints involved: {}",
            err_msg
        );
    }

    // =========================================================================
    // Spec 025 - User Story 4: Passthrough Node Support Tests
    // =========================================================================

    #[test]
    fn test_passthrough_inherits_discovered_caps() {
        // T031 [US4]: Test that Passthrough nodes receive pending updates
        // Scenario: source (RuntimeDiscovered) -> passthrough (Passthrough) -> sink (Static)
        use super::super::constraints::{
            AudioConstraints, AudioSampleFormat, ConstraintValue, MediaConstraints,
        };
        use super::super::dynamic::{CapabilityBehavior, ResolvedCapabilities};

        let registry = StreamingNodeRegistry::new();
        let resolver = CapabilityResolver::new(&registry);

        let mut ctx = ResolutionContext::new();

        // Set up: source -> passthrough -> sink
        ctx.set_behavior("source", CapabilityBehavior::RuntimeDiscovered);
        ctx.set_behavior("passthrough", CapabilityBehavior::Passthrough);
        ctx.set_behavior("sink", CapabilityBehavior::Static);

        ctx.node_types
            .insert("source".to_string(), "MicInput".to_string());
        ctx.node_types
            .insert("passthrough".to_string(), "SpeakerOutput".to_string());
        ctx.node_types
            .insert("sink".to_string(), "AudioSink".to_string());

        ctx.add_connection("source", "passthrough");
        ctx.add_connection("passthrough", "sink");

        // Set up source's provisional capabilities
        let potential_caps =
            MediaCapabilities::with_output(MediaConstraints::Audio(AudioConstraints {
                sample_rate: Some(ConstraintValue::Range {
                    min: 8000,
                    max: 192000,
                }),
                channels: Some(ConstraintValue::Range { min: 1, max: 8 }),
                format: Some(ConstraintValue::Exact(AudioSampleFormat::F32)),
            }));
        ctx.resolved.insert(
            "source".to_string(),
            ResolvedCapabilities::provisional("source", potential_caps),
        );

        // Source discovers actual 44100Hz
        let actual_caps =
            MediaCapabilities::with_output(MediaConstraints::Audio(AudioConstraints {
                sample_rate: Some(ConstraintValue::Exact(44100)),
                channels: Some(ConstraintValue::Exact(2)),
                format: Some(ConstraintValue::Exact(AudioSampleFormat::F32)),
            }));

        // Propagate
        let result = resolver.revalidate_and_propagate(&mut ctx, "source", actual_caps);
        assert!(result.is_ok());

        // Passthrough should have a pending update
        let pending = ctx.get_pending_update("passthrough");
        assert!(
            pending.is_some(),
            "Passthrough should receive pending update"
        );

        let update = pending.unwrap();
        assert_eq!(update.upstream_node_id, "source");
        assert_eq!(update.node_id, "passthrough");

        // Verify the update contains the actual 44100Hz
        if let Some(MediaConstraints::Audio(audio)) = update.upstream_output.default_output() {
            assert_eq!(audio.sample_rate, Some(ConstraintValue::Exact(44100)));
        } else {
            panic!("Expected audio constraints in pending update");
        }
    }

    #[test]
    fn test_passthrough_after_adaptive_receives_update() {
        // T035 [US4]: Test Passthrough after Adaptive: Source -> Adaptive -> Passthrough
        use super::super::dynamic::CapabilityBehavior;

        let registry = StreamingNodeRegistry::new();
        let resolver = CapabilityResolver::new(&registry);

        let mut ctx = ResolutionContext::new();

        // Set up: source -> adaptive -> passthrough -> sink
        ctx.set_behavior("source", CapabilityBehavior::RuntimeDiscovered);
        ctx.set_behavior("adaptive", CapabilityBehavior::Adaptive);
        ctx.set_behavior("passthrough", CapabilityBehavior::Passthrough);
        ctx.set_behavior("sink", CapabilityBehavior::Static);

        ctx.add_connection("source", "adaptive");
        ctx.add_connection("adaptive", "passthrough");
        ctx.add_connection("passthrough", "sink");

        // Compute propagation order
        let order = resolver.compute_propagation_order(&ctx, "source");

        // Both adaptive and passthrough should be in propagation order
        assert_eq!(order.len(), 2);
        assert!(order.contains(&"adaptive".to_string()));
        assert!(order.contains(&"passthrough".to_string()));

        // Adaptive should come before passthrough
        let adaptive_idx = order.iter().position(|x| x == "adaptive").unwrap();
        let passthrough_idx = order.iter().position(|x| x == "passthrough").unwrap();
        assert!(
            adaptive_idx < passthrough_idx,
            "Adaptive should come before Passthrough in propagation order"
        );
    }

    // =========================================================================
    // Spec 025 - Phase 7: Edge Case Tests
    // =========================================================================

    #[test]
    fn test_update_queuing_during_processing() {
        // T038: Test update queuing when node already processing (batch boundary application)
        // Scenario: Updates can be added while a node is "processing", and new updates
        // replace old ones. The SessionRouter would apply updates at batch boundaries.
        use super::super::constraints::{
            AudioConstraints, AudioSampleFormat, ConstraintValue, MediaConstraints,
        };
        use super::super::dynamic::CapabilityBehavior;

        let registry = StreamingNodeRegistry::new();
        let resolver = CapabilityResolver::new(&registry);

        let mut ctx = ResolutionContext::new();

        // Set up: source1 -> resample, source2 -> resample (fan-in)
        // Both sources are RuntimeDiscovered and may report capabilities at different times
        ctx.set_behavior("source1", CapabilityBehavior::RuntimeDiscovered);
        ctx.set_behavior("source2", CapabilityBehavior::RuntimeDiscovered);
        ctx.set_behavior("resample", CapabilityBehavior::Adaptive);

        ctx.add_connection("source1", "resample");
        ctx.add_connection("source2", "resample");

        // Set up provisional capabilities for sources
        let potential_caps =
            MediaCapabilities::with_output(MediaConstraints::Audio(AudioConstraints {
                sample_rate: Some(ConstraintValue::Range {
                    min: 8000,
                    max: 192000,
                }),
                channels: Some(ConstraintValue::Range { min: 1, max: 8 }),
                format: Some(ConstraintValue::Exact(AudioSampleFormat::F32)),
            }));
        ctx.resolved.insert(
            "source1".to_string(),
            ResolvedCapabilities::provisional("source1", potential_caps.clone()),
        );
        ctx.resolved.insert(
            "source2".to_string(),
            ResolvedCapabilities::provisional("source2", potential_caps),
        );

        // Simulate: source1 discovers 44100Hz while resample is "busy"
        let actual1 = MediaCapabilities::with_output(MediaConstraints::Audio(AudioConstraints {
            sample_rate: Some(ConstraintValue::Exact(44100)),
            channels: Some(ConstraintValue::Exact(2)),
            format: Some(ConstraintValue::Exact(AudioSampleFormat::F32)),
        }));
        resolver
            .revalidate_and_propagate(&mut ctx, "source1", actual1)
            .unwrap();

        // Verify: resample has pending update from source1
        assert!(
            ctx.has_pending_updates(),
            "Should have pending update after source1"
        );
        let update1 = ctx.get_pending_update("resample").unwrap();
        assert_eq!(update1.upstream_node_id, "source1");
        if let Some(MediaConstraints::Audio(audio)) = update1.upstream_output.default_output() {
            assert_eq!(audio.sample_rate, Some(ConstraintValue::Exact(44100)));
        }

        // Simulate: source2 discovers 48000Hz before resample consumed update1
        // (simulates concurrent initialization or re-discovery)
        let actual2 = MediaCapabilities::with_output(MediaConstraints::Audio(AudioConstraints {
            sample_rate: Some(ConstraintValue::Exact(48000)),
            channels: Some(ConstraintValue::Exact(1)),
            format: Some(ConstraintValue::Exact(AudioSampleFormat::F32)),
        }));
        resolver
            .revalidate_and_propagate(&mut ctx, "source2", actual2)
            .unwrap();

        // Verify: resample's pending update was REPLACED by source2's (newer wins)
        // This is the expected behavior - only one pending update per node
        assert!(
            ctx.has_pending_updates(),
            "Should still have pending update"
        );
        let update2 = ctx.get_pending_update("resample").unwrap();
        assert_eq!(
            update2.upstream_node_id, "source2",
            "Newer update should replace older"
        );
        if let Some(MediaConstraints::Audio(audio)) = update2.upstream_output.default_output() {
            assert_eq!(
                audio.sample_rate,
                Some(ConstraintValue::Exact(48000)),
                "Should have 48kHz from source2, not 44.1kHz from source1"
            );
        }

        // Simulate: SessionRouter.apply_pending_updates() at batch boundary
        // Takes the update and applies it to the node
        let taken = ctx.take_pending_update("resample");
        assert!(taken.is_some(), "Should be able to take the pending update");
        assert!(
            !ctx.has_pending_updates(),
            "No pending updates after taking"
        );

        // After applying, the node is ready for new updates
        // If another source re-discovers, it can queue a new update
        let actual3 = MediaCapabilities::with_output(MediaConstraints::Audio(AudioConstraints {
            sample_rate: Some(ConstraintValue::Exact(96000)),
            channels: Some(ConstraintValue::Exact(2)),
            format: Some(ConstraintValue::Exact(AudioSampleFormat::F32)),
        }));
        resolver
            .revalidate_and_propagate(&mut ctx, "source1", actual3)
            .unwrap();

        // New update queued
        assert!(ctx.has_pending_updates(), "New update should be queued");
        let update3 = ctx.get_pending_update("resample").unwrap();
        assert_eq!(update3.upstream_node_id, "source1");
        if let Some(MediaConstraints::Audio(audio)) = update3.upstream_output.default_output() {
            assert_eq!(audio.sample_rate, Some(ConstraintValue::Exact(96000)));
        }
    }

    #[test]
    fn test_repropagation_performance_10_nodes() {
        // T039: Verify re-propagation performance < 10ms for 10 nodes
        // Note: revalidate_and_propagate only creates pending updates for IMMEDIATE downstream nodes.
        // For a full cascade, SessionRouter.apply_pending_updates() iteratively applies updates.
        // This test measures the resolver's portion: computing propagation order and initial update.
        use super::super::constraints::{
            AudioConstraints, AudioSampleFormat, ConstraintValue, MediaCapabilities,
            MediaConstraints,
        };
        use super::super::dynamic::CapabilityBehavior;
        use std::time::Instant;

        let registry = StreamingNodeRegistry::new();
        let resolver = CapabilityResolver::new(&registry);

        let mut ctx = ResolutionContext::new();

        // Set up a chain of 10 nodes: source -> node1 -> node2 -> ... -> node9
        ctx.set_behavior("source", CapabilityBehavior::RuntimeDiscovered);
        for i in 1..=9 {
            ctx.set_behavior(&format!("node{}", i), CapabilityBehavior::Adaptive);
        }

        // Create connections
        ctx.add_connection("source", "node1");
        for i in 1..9 {
            ctx.add_connection(&format!("node{}", i), &format!("node{}", i + 1));
        }

        // Set up initial resolved capabilities for source (provisional)
        let source_caps =
            MediaCapabilities::with_output(MediaConstraints::Audio(AudioConstraints {
                sample_rate: Some(ConstraintValue::Range {
                    min: 8000,
                    max: 192000,
                }),
                channels: Some(ConstraintValue::Range { min: 1, max: 8 }),
                format: Some(ConstraintValue::Exact(AudioSampleFormat::F32)),
            }));
        ctx.resolved.insert(
            "source".to_string(),
            ResolvedCapabilities::provisional("source", source_caps),
        );

        // Set up resolved capabilities for each adaptive node
        for i in 1..=9 {
            let node_caps = MediaCapabilities::with_input_output(
                MediaConstraints::Audio(AudioConstraints {
                    sample_rate: Some(ConstraintValue::Range {
                        min: 8000,
                        max: 192000,
                    }),
                    channels: Some(ConstraintValue::Range { min: 1, max: 8 }),
                    format: Some(ConstraintValue::Exact(AudioSampleFormat::F32)),
                }),
                MediaConstraints::Audio(AudioConstraints {
                    sample_rate: Some(ConstraintValue::Range {
                        min: 8000,
                        max: 192000,
                    }),
                    channels: Some(ConstraintValue::Range { min: 1, max: 8 }),
                    format: Some(ConstraintValue::Exact(AudioSampleFormat::F32)),
                }),
            );
            ctx.resolved.insert(
                format!("node{}", i),
                ResolvedCapabilities::needs_reverse(&format!("node{}", i), node_caps),
            );
        }

        // Actual capabilities discovered by source
        let actual_caps =
            MediaCapabilities::with_output(MediaConstraints::Audio(AudioConstraints {
                sample_rate: Some(ConstraintValue::Exact(48000)),
                channels: Some(ConstraintValue::Exact(2)),
                format: Some(ConstraintValue::Exact(AudioSampleFormat::F32)),
            }));

        // Test 1: Verify compute_propagation_order includes all 9 adaptive nodes
        let order = resolver.compute_propagation_order(&ctx, "source");
        assert_eq!(
            order.len(),
            9,
            "Propagation order should include all 9 adaptive nodes"
        );

        // Test 2: Measure re-propagation time for a single step (source -> node1)
        let start = Instant::now();
        let result = resolver.revalidate_and_propagate(&mut ctx, "source", actual_caps.clone());
        let elapsed = start.elapsed();

        // Verify success
        assert!(
            result.is_ok(),
            "Re-propagation should succeed: {:?}",
            result
        );

        // Verify immediate downstream (node1) received pending update
        assert!(
            ctx.get_pending_update("node1").is_some(),
            "node1 should have pending update from source"
        );

        // Test 3: Simulate full cascade (9 steps) and measure total time
        // Each step would be triggered when the Adaptive node is configured
        let mut total_time = elapsed;
        for i in 1..9 {
            let node_id = format!("node{}", i);
            let next_node = format!("node{}", i + 1);

            // Clear pending and simulate this node being configured
            ctx.take_pending_update(&node_id);

            // Simulate the node re-propagating to its downstream
            let node_actual = actual_caps.clone(); // In reality, this would be computed
            let start = Instant::now();
            let _result = resolver.revalidate_and_propagate(&mut ctx, &node_id, node_actual);
            total_time += start.elapsed();

            // Verify next node gets update
            assert!(
                ctx.get_pending_update(&next_node).is_some(),
                "{} should have pending update from {}",
                next_node,
                node_id
            );
        }

        // Verify performance: total cascade < 10ms for 10 nodes
        assert!(
            total_time.as_millis() < 10,
            "Full cascade re-propagation should complete in < 10ms, took {}ms",
            total_time.as_millis()
        );

        println!(
            "Full cascade re-propagation for 10 nodes completed in {:?}",
            total_time
        );
    }
}
