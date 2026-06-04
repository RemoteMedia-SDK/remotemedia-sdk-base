//! Dynamic capability resolution (spec 022 extension, spec 023 pipeline resolution)
//!
//! This module extends the static capability system to support nodes with
//! runtime-dependent capabilities. Many nodes have capabilities that depend on:
//!
//! - **Configuration parameters** (e.g., MicInput's `sample_rate` param)
//! - **Connected upstream nodes** (e.g., SpeakerOutput matches its input)
//! - **Target requirements** (e.g., Resample outputs what downstream needs)
//! - **Runtime discovery** (e.g., actual device capabilities)
//!
//! # Capability Resolution Order (spec 023)
//!
//! 1. **Static** - Fixed at compile time (e.g., Whisper requires 16kHz mono)
//! 2. **Configured** - Resolved from node params after manifest parsing
//! 3. **Passthrough** - Output inherits from input (resolved during graph traversal)
//! 4. **Adaptive** - Output adapts to downstream requirements (reverse pass)
//! 5. **RuntimeDiscovered** - Two-phase: potential_capabilities() then actual_capabilities()
//!
//! # Two-Phase Resolution (spec 023)
//!
//! For RuntimeDiscovered nodes (e.g., hardware devices):
//! - **Phase 1**: Use `potential_capabilities()` for early validation (before device init)
//! - **Phase 2**: Call `actual_capabilities()` after `node.initialize()` completes
//!
//! # Example
//!
//! ```ignore
//! // MicInput has configured output based on params
//! let mic_caps = resolve_capabilities(&mic_node, &mic_params, None)?;
//!
//! // Resample adapts output to match downstream Whisper requirements
//! let resample_caps = resolve_capabilities(
//!     &resample_node,
//!     &resample_params,
//!     Some(&whisper_input_caps),  // Target to adapt to
//! )?;
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Instant;

use super::constraints::{AudioConstraints, ConstraintValue, MediaCapabilities, MediaConstraints};
use super::validation::CapabilityMismatch;

// =============================================================================
// Capability Notification (spec 025)
// =============================================================================

/// Notification of a pending capability update for a node (spec 025).
///
/// Created by `CapabilityResolver.revalidate_and_propagate()` when a RuntimeDiscovered
/// node reports its actual capabilities. Consumed by `SessionRouter` after node
/// initialization to configure downstream Adaptive/Passthrough nodes.
///
/// # Lifecycle
///
/// 1. Created by `revalidate_and_propagate()` when upstream capabilities change
/// 2. Stored in `ResolutionContext.pending_updates`
/// 3. Retrieved by SessionRouter after node initialization
/// 4. Applied via `node.configure_from_upstream()`
/// 5. Removed from pending_updates after successful application
///
/// # Example
///
/// ```ignore
/// let notification = CapabilityNotification::new(
///     "resample",  // target node
///     "mic",       // upstream node that triggered update
///     MediaCapabilities::with_output(audio_constraints),
/// );
/// ctx.add_pending_update(notification);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapabilityNotification {
    /// Target node to receive the update
    pub node_id: String,

    /// Source node that triggered the propagation
    pub upstream_node_id: String,

    /// Capabilities from upstream node's output
    pub upstream_output: MediaCapabilities,

    /// When the update was created (for ordering concurrent updates)
    #[serde(skip)]
    pub timestamp: Option<Instant>,
}

impl CapabilityNotification {
    /// Create a new capability notification.
    pub fn new(
        node_id: impl Into<String>,
        upstream_node_id: impl Into<String>,
        upstream_output: MediaCapabilities,
    ) -> Self {
        Self {
            node_id: node_id.into(),
            upstream_node_id: upstream_node_id.into(),
            upstream_output,
            timestamp: Some(Instant::now()),
        }
    }
}

// =============================================================================
// Capability Behavior (spec 023) — re-exported from `remotemedia-traits`
// =============================================================================

// Plugin-author surface; moved out of core in Task A4.
pub use remotemedia_traits::capabilities::CapabilityBehavior;

// =============================================================================
// Resolution State (spec 023)
// =============================================================================

/// Tracks the resolution status of a node during graph traversal.
///
/// Used internally by `CapabilityResolver` to track which nodes have been
/// processed and what state they're in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolutionState {
    /// Not yet processed
    Pending,

    /// Resolved during forward pass
    ///
    /// For RuntimeDiscovered nodes, this is provisional until Phase 2.
    ResolvedForward,

    /// Needs reverse pass (Adaptive nodes only)
    ///
    /// The node was visited during forward pass but its output capabilities
    /// depend on downstream requirements that weren't yet resolved.
    NeedsReverse,

    /// Fully resolved (including reverse pass if needed)
    Complete,

    /// Resolution failed with error message
    Failed(String),
}

impl Default for ResolutionState {
    fn default() -> Self {
        ResolutionState::Pending
    }
}

// =============================================================================
// Capability Source (for introspection)
// =============================================================================

/// Describes how a capability was determined.
///
/// Useful for debugging and explaining to users why certain formats were chosen.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CapabilitySource {
    /// Fixed at compile time, cannot be changed
    Static,
    /// Determined by node configuration/parameters
    Configured,
    /// Inherited from upstream node's output
    Passthrough,
    /// Adapted to match downstream node's requirements
    Negotiated,
    /// Discovered at runtime (e.g., device enumeration)
    RuntimeDiscovered,
    /// Default value used when nothing else specified
    Default,
}

// =============================================================================
// Resolved Capabilities
// =============================================================================

/// Capabilities with resolution metadata.
///
/// Extends `MediaCapabilities` with information about how each constraint
/// was determined, enabling better error messages and debugging.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolvedCapabilities {
    /// The resolved capability constraints
    pub capabilities: MediaCapabilities,
    /// How each port's constraints were determined
    pub sources: HashMap<String, CapabilitySource>,
    /// Node ID this was resolved for
    pub node_id: String,
    /// Resolution state (spec 023)
    #[serde(skip)]
    pub state: ResolutionState,
    /// Whether this is provisional (RuntimeDiscovered Phase 1)
    ///
    /// Provisional capabilities were determined using `potential_capabilities()`
    /// and will be re-validated after `node.initialize()` completes.
    pub provisional: bool,
}

impl ResolvedCapabilities {
    /// Create resolved capabilities from static constraints.
    pub fn from_static(node_id: &str, caps: MediaCapabilities) -> Self {
        let mut sources = HashMap::new();
        for port in caps.inputs.keys().chain(caps.outputs.keys()) {
            sources.insert(port.clone(), CapabilitySource::Static);
        }
        Self {
            capabilities: caps,
            sources,
            node_id: node_id.to_string(),
            state: ResolutionState::Complete,
            provisional: false,
        }
    }

    /// Create resolved capabilities from configured constraints.
    pub fn from_configured(node_id: &str, caps: MediaCapabilities) -> Self {
        let mut sources = HashMap::new();
        for port in caps.inputs.keys().chain(caps.outputs.keys()) {
            sources.insert(port.clone(), CapabilitySource::Configured);
        }
        Self {
            capabilities: caps,
            sources,
            node_id: node_id.to_string(),
            state: ResolutionState::Complete,
            provisional: false,
        }
    }

    /// Create passthrough capabilities where output matches input.
    pub fn passthrough(node_id: &str, input_caps: &MediaConstraints) -> Self {
        let caps = MediaCapabilities::with_input_output(input_caps.clone(), input_caps.clone());
        let mut sources = HashMap::new();
        sources.insert("default".to_string(), CapabilitySource::Passthrough);
        Self {
            capabilities: caps,
            sources,
            node_id: node_id.to_string(),
            state: ResolutionState::Complete,
            provisional: false,
        }
    }

    /// Create provisional capabilities for RuntimeDiscovered nodes (Phase 1).
    ///
    /// These capabilities are based on `potential_capabilities()` and will
    /// be re-validated after device initialization.
    pub fn provisional(node_id: &str, caps: MediaCapabilities) -> Self {
        let mut sources = HashMap::new();
        for port in caps.inputs.keys().chain(caps.outputs.keys()) {
            sources.insert(port.clone(), CapabilitySource::RuntimeDiscovered);
        }
        Self {
            capabilities: caps,
            sources,
            node_id: node_id.to_string(),
            state: ResolutionState::ResolvedForward,
            provisional: true,
        }
    }

    /// Create adaptive capabilities that need reverse pass resolution.
    pub fn needs_reverse(node_id: &str, input_caps: MediaCapabilities) -> Self {
        let mut sources = HashMap::new();
        for port in input_caps.inputs.keys() {
            sources.insert(port.clone(), CapabilitySource::Static);
        }
        // Output source will be set during reverse pass
        Self {
            capabilities: input_caps,
            sources,
            node_id: node_id.to_string(),
            state: ResolutionState::NeedsReverse,
            provisional: false,
        }
    }

    /// Mark as complete after reverse pass resolution.
    pub fn mark_complete(&mut self) {
        self.state = ResolutionState::Complete;
    }

    /// Mark as no longer provisional after Phase 2 validation.
    pub fn confirm_actual(&mut self, actual_caps: MediaCapabilities) {
        self.capabilities = actual_caps;
        self.provisional = false;
        self.state = ResolutionState::Complete;
    }

    /// Get the capability source for a port.
    pub fn source(&self, port: &str) -> Option<&CapabilitySource> {
        self.sources.get(port)
    }

    /// Check if this resolution is complete.
    pub fn is_complete(&self) -> bool {
        matches!(self.state, ResolutionState::Complete)
    }

    /// Check if this resolution needs reverse pass.
    pub fn needs_reverse_pass(&self) -> bool {
        matches!(self.state, ResolutionState::NeedsReverse)
    }
}

// =============================================================================
// Dynamic Capability Provider Trait
// =============================================================================

/// Trait for nodes that can provide dynamic capabilities.
///
/// Nodes implementing this trait can compute their capabilities based on
/// configuration, upstream connections, or target requirements.
pub trait DynamicCapabilityProvider {
    /// Returns the static (compile-time) capabilities, if any.
    ///
    /// These are the fixed constraints that never change regardless of
    /// configuration. For example, Whisper always requires 16kHz mono audio.
    ///
    /// Return `None` if capabilities are entirely dynamic.
    fn static_capabilities(&self) -> Option<MediaCapabilities> {
        None
    }

    /// Resolve capabilities based on configuration.
    ///
    /// Called after node parameters are parsed but before the pipeline
    /// graph is fully connected. This is where nodes like MicInput
    /// determine their output format based on `sample_rate` params.
    ///
    /// # Arguments
    /// * `params` - The node's configuration parameters
    ///
    /// # Returns
    /// * `Some(caps)` - Resolved capabilities
    /// * `None` - Cannot resolve from params alone (needs upstream/target info)
    fn resolve_from_params(&self, params: &serde_json::Value) -> Option<MediaCapabilities> {
        let _ = params;
        self.static_capabilities()
    }

    /// Resolve output capabilities given upstream input.
    ///
    /// Called during graph traversal when we know what the upstream node
    /// will produce. Used by passthrough nodes like SpeakerOutput.
    ///
    /// # Arguments
    /// * `params` - The node's configuration parameters
    /// * `upstream_output` - What the upstream node will produce
    ///
    /// # Returns
    /// Resolved capabilities with output adapted to upstream
    fn resolve_with_upstream(
        &self,
        params: &serde_json::Value,
        upstream_output: &MediaConstraints,
    ) -> Option<MediaCapabilities> {
        let _ = (params, upstream_output);
        self.static_capabilities()
    }

    /// Resolve output capabilities to match target requirements.
    ///
    /// Called when we know what the downstream node needs. Used by
    /// converter nodes like AudioResample that adapt their output.
    ///
    /// # Arguments
    /// * `params` - The node's configuration parameters
    /// * `upstream_output` - What the upstream node produces
    /// * `downstream_input` - What the downstream node requires
    ///
    /// # Returns
    /// Resolved capabilities with output matching downstream requirements
    fn resolve_to_target(
        &self,
        params: &serde_json::Value,
        upstream_output: &MediaConstraints,
        downstream_input: &MediaConstraints,
    ) -> Option<MediaCapabilities> {
        let _ = (params, upstream_output, downstream_input);
        self.static_capabilities()
    }

    /// Check if this node's output depends on its input (passthrough behavior).
    fn is_passthrough(&self) -> bool {
        false
    }

    /// Check if this node can adapt its output to match target requirements.
    fn is_adaptive(&self) -> bool {
        false
    }
}

// =============================================================================
// Capability Resolution Context
// =============================================================================

/// Context for resolving capabilities during graph traversal.
///
/// This struct maintains all state needed during capability resolution,
/// including the resolved capabilities, connection graph, and any errors
/// encountered during resolution.
#[derive(Debug, Clone)]
pub struct ResolutionContext {
    /// Resolved capabilities for each node (node_id -> capabilities)
    pub resolved: HashMap<String, ResolvedCapabilities>,
    /// Node parameters (node_id -> params)
    pub params: HashMap<String, serde_json::Value>,
    /// Node types (node_id -> node_type) (spec 023)
    pub node_types: HashMap<String, String>,
    /// Graph connections (source_id -> vec of target_ids)
    pub connections: HashMap<String, Vec<String>>,
    /// Reverse connections (target_id -> vec of source_ids)
    pub reverse_connections: HashMap<String, Vec<String>>,
    /// Node capability behaviors (node_id -> behavior) (spec 023)
    pub behaviors: HashMap<String, CapabilityBehavior>,
    /// Resolution states (node_id -> state) (spec 023)
    pub states: HashMap<String, ResolutionState>,
    /// Errors accumulated during resolution (spec 023)
    pub errors: Vec<CapabilityMismatch>,
    /// Pending capability updates for downstream nodes (spec 025)
    pub pending_updates: HashMap<String, CapabilityNotification>,
}

impl ResolutionContext {
    /// Create a new resolution context.
    pub fn new() -> Self {
        Self {
            resolved: HashMap::new(),
            params: HashMap::new(),
            node_types: HashMap::new(),
            connections: HashMap::new(),
            reverse_connections: HashMap::new(),
            behaviors: HashMap::new(),
            states: HashMap::new(),
            errors: Vec::new(),
            pending_updates: HashMap::new(),
        }
    }

    /// Get node type for a node_id.
    pub fn get_node_type(&self, node_id: &str) -> Option<&str> {
        self.node_types.get(node_id).map(|s| s.as_str())
    }

    /// Add a connection to the context.
    pub fn add_connection(&mut self, from: &str, to: &str) {
        self.connections
            .entry(from.to_string())
            .or_default()
            .push(to.to_string());
        self.reverse_connections
            .entry(to.to_string())
            .or_default()
            .push(from.to_string());
    }

    /// Get upstream node IDs for a given node.
    pub fn upstream_nodes(&self, node_id: &str) -> Vec<&str> {
        self.reverse_connections
            .get(node_id)
            .map(|v| v.iter().map(|s| s.as_str()).collect())
            .unwrap_or_default()
    }

    /// Get downstream node IDs for a given node.
    pub fn downstream_nodes(&self, node_id: &str) -> Vec<&str> {
        self.connections
            .get(node_id)
            .map(|v| v.iter().map(|s| s.as_str()).collect())
            .unwrap_or_default()
    }

    /// Get resolved output for a node (if available).
    pub fn get_output(&self, node_id: &str) -> Option<&MediaConstraints> {
        self.resolved
            .get(node_id)
            .and_then(|r| r.capabilities.default_output())
    }

    /// Get resolved input for a node (if available).
    pub fn get_input(&self, node_id: &str) -> Option<&MediaConstraints> {
        self.resolved
            .get(node_id)
            .and_then(|r| r.capabilities.default_input())
    }

    /// Set the capability behavior for a node. (spec 023)
    pub fn set_behavior(&mut self, node_id: &str, behavior: CapabilityBehavior) {
        self.behaviors.insert(node_id.to_string(), behavior);
    }

    /// Get the capability behavior for a node. (spec 023)
    pub fn get_behavior(&self, node_id: &str) -> CapabilityBehavior {
        self.behaviors
            .get(node_id)
            .copied()
            .unwrap_or(CapabilityBehavior::Passthrough)
    }

    /// Set the resolution state for a node. (spec 023)
    pub fn set_state(&mut self, node_id: &str, state: ResolutionState) {
        self.states.insert(node_id.to_string(), state);
    }

    /// Get the resolution state for a node. (spec 023)
    pub fn get_state(&self, node_id: &str) -> &ResolutionState {
        self.states
            .get(node_id)
            .unwrap_or(&ResolutionState::Pending)
    }

    /// Add an error to the context. (spec 023)
    pub fn add_error(&mut self, error: CapabilityMismatch) {
        self.errors.push(error);
    }

    /// Check if resolution has any errors. (spec 023)
    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }

    /// Get all nodes that need reverse pass. (spec 023)
    pub fn nodes_needing_reverse(&self) -> Vec<&str> {
        self.states
            .iter()
            .filter(|(_, state)| matches!(state, ResolutionState::NeedsReverse))
            .map(|(id, _)| id.as_str())
            .collect()
    }

    /// Get all provisional (RuntimeDiscovered Phase 1) nodes. (spec 023)
    pub fn provisional_nodes(&self) -> Vec<&str> {
        self.resolved
            .iter()
            .filter(|(_, caps)| caps.provisional)
            .map(|(id, _)| id.as_str())
            .collect()
    }

    /// Check if all nodes are fully resolved. (spec 023)
    pub fn is_complete(&self) -> bool {
        self.states
            .values()
            .all(|s| matches!(s, ResolutionState::Complete))
            && self.errors.is_empty()
    }

    // =========================================================================
    // Pending Update Methods (spec 025)
    // =========================================================================

    /// Add a pending capability update for a node. (spec 025)
    ///
    /// If an update already exists for the node, it will be replaced.
    /// This ensures only the most recent update is pending.
    ///
    /// # Arguments
    /// * `notification` - The capability notification to add
    pub fn add_pending_update(&mut self, notification: CapabilityNotification) {
        self.pending_updates
            .insert(notification.node_id.clone(), notification);
    }

    /// Take (remove and return) the pending update for a node. (spec 025)
    ///
    /// Returns `Some(notification)` if there was a pending update, `None` otherwise.
    /// After calling this, the node will have no pending update.
    ///
    /// # Arguments
    /// * `node_id` - The ID of the node to get the update for
    pub fn take_pending_update(&mut self, node_id: &str) -> Option<CapabilityNotification> {
        self.pending_updates.remove(node_id)
    }

    /// Get a reference to the pending update for a node without removing it. (spec 025)
    ///
    /// Use this to inspect pending updates without consuming them.
    ///
    /// # Arguments
    /// * `node_id` - The ID of the node to get the update for
    pub fn get_pending_update(&self, node_id: &str) -> Option<&CapabilityNotification> {
        self.pending_updates.get(node_id)
    }

    /// Clear all pending updates. (spec 025)
    ///
    /// Call this after all updates have been applied to clean up state.
    pub fn clear_pending_updates(&mut self) {
        self.pending_updates.clear();
    }

    /// Get all node IDs that have pending updates. (spec 025)
    ///
    /// Useful for iterating over nodes that need configuration.
    pub fn nodes_with_pending_updates(&self) -> Vec<&str> {
        self.pending_updates.keys().map(|s| s.as_str()).collect()
    }

    /// Check if there are any pending updates. (spec 025)
    pub fn has_pending_updates(&self) -> bool {
        !self.pending_updates.is_empty()
    }
}

impl Default for ResolutionContext {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Extract sample rate from audio constraints.
pub fn get_sample_rate(constraints: &MediaConstraints) -> Option<u32> {
    match constraints {
        MediaConstraints::Audio(audio) => match &audio.sample_rate {
            Some(ConstraintValue::Exact(rate)) => Some(*rate),
            Some(ConstraintValue::Range { min, .. }) => Some(*min), // Use min as default
            Some(ConstraintValue::Set(rates)) => rates.first().copied(),
            None => None,
        },
        _ => None,
    }
}

/// Extract channel count from audio constraints.
pub fn get_channels(constraints: &MediaConstraints) -> Option<u32> {
    match constraints {
        MediaConstraints::Audio(audio) => match &audio.channels {
            Some(ConstraintValue::Exact(ch)) => Some(*ch),
            Some(ConstraintValue::Range { min, .. }) => Some(*min),
            Some(ConstraintValue::Set(chs)) => chs.first().copied(),
            None => None,
        },
        _ => None,
    }
}

/// Create audio constraints with specific sample rate and channels.
pub fn audio_constraints(sample_rate: u32, channels: u32) -> MediaConstraints {
    MediaConstraints::Audio(AudioConstraints {
        sample_rate: Some(ConstraintValue::Exact(sample_rate)),
        channels: Some(ConstraintValue::Exact(channels)),
        format: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capabilities::{AudioSampleFormat, TextConstraints};

    #[test]
    fn test_resolved_capabilities_from_static() {
        let caps = MediaCapabilities::with_input_output(
            MediaConstraints::Audio(AudioConstraints {
                sample_rate: Some(ConstraintValue::Exact(16000)),
                channels: Some(ConstraintValue::Exact(1)),
                format: Some(ConstraintValue::Exact(AudioSampleFormat::F32)),
            }),
            MediaConstraints::Text(TextConstraints::default()),
        );

        let resolved = ResolvedCapabilities::from_static("whisper", caps);
        assert_eq!(resolved.node_id, "whisper");
        assert_eq!(resolved.source("default"), Some(&CapabilitySource::Static));
    }

    #[test]
    fn test_resolved_capabilities_passthrough() {
        let input = MediaConstraints::Audio(AudioConstraints {
            sample_rate: Some(ConstraintValue::Exact(48000)),
            channels: Some(ConstraintValue::Exact(2)),
            format: None,
        });

        let resolved = ResolvedCapabilities::passthrough("speaker", &input);
        assert_eq!(
            resolved.source("default"),
            Some(&CapabilitySource::Passthrough)
        );

        // Output should match input
        let output = resolved.capabilities.default_output().unwrap();
        match output {
            MediaConstraints::Audio(audio) => {
                assert_eq!(audio.sample_rate, Some(ConstraintValue::Exact(48000)));
                assert_eq!(audio.channels, Some(ConstraintValue::Exact(2)));
            }
            _ => panic!("Expected audio output"),
        }
    }

    #[test]
    fn test_resolution_context() {
        let mut ctx = ResolutionContext::new();
        ctx.add_connection("mic", "resample");
        ctx.add_connection("resample", "whisper");

        assert_eq!(ctx.upstream_nodes("whisper"), vec!["resample"]);
        assert_eq!(ctx.downstream_nodes("mic"), vec!["resample"]);
        assert_eq!(ctx.upstream_nodes("resample"), vec!["mic"]);
    }

    #[test]
    fn test_get_sample_rate() {
        let audio = MediaConstraints::Audio(AudioConstraints {
            sample_rate: Some(ConstraintValue::Exact(44100)),
            channels: None,
            format: None,
        });
        assert_eq!(get_sample_rate(&audio), Some(44100));

        let range = MediaConstraints::Audio(AudioConstraints {
            sample_rate: Some(ConstraintValue::Range {
                min: 8000,
                max: 48000,
            }),
            channels: None,
            format: None,
        });
        assert_eq!(get_sample_rate(&range), Some(8000));

        let text = MediaConstraints::Text(TextConstraints::default());
        assert_eq!(get_sample_rate(&text), None);
    }

    // =========================================================================
    // Spec 025: Capability Re-propagation Tests
    // =========================================================================

    #[test]
    fn test_pending_updates_storage() {
        // T011 [US1]: Test pending update storage and retrieval
        let mut ctx = ResolutionContext::new();

        // Initially no pending updates
        assert!(!ctx.has_pending_updates());
        assert!(ctx.get_pending_update("resample").is_none());

        // Create a notification
        let caps = MediaCapabilities::with_output(MediaConstraints::Audio(AudioConstraints {
            sample_rate: Some(ConstraintValue::Exact(48000)),
            channels: Some(ConstraintValue::Exact(2)),
            format: Some(ConstraintValue::Exact(AudioSampleFormat::F32)),
        }));
        let notification = CapabilityNotification::new("resample", "mic", caps);

        // Add the pending update
        ctx.add_pending_update(notification);

        // Verify it was stored
        assert!(ctx.has_pending_updates());
        assert!(ctx.get_pending_update("resample").is_some());
        assert_eq!(ctx.nodes_with_pending_updates(), vec!["resample"]);

        // Verify the stored data
        let stored = ctx.get_pending_update("resample").unwrap();
        assert_eq!(stored.node_id, "resample");
        assert_eq!(stored.upstream_node_id, "mic");
        assert!(stored.timestamp.is_some());

        // Take the update (removes it)
        let taken = ctx.take_pending_update("resample");
        assert!(taken.is_some());
        assert_eq!(taken.unwrap().node_id, "resample");

        // Verify it was removed
        assert!(!ctx.has_pending_updates());
        assert!(ctx.get_pending_update("resample").is_none());
    }

    #[test]
    fn test_pending_updates_replacement() {
        // Test that adding a new update for the same node replaces the old one
        let mut ctx = ResolutionContext::new();

        // Create first notification with 44100 Hz
        let caps1 = MediaCapabilities::with_output(MediaConstraints::Audio(AudioConstraints {
            sample_rate: Some(ConstraintValue::Exact(44100)),
            channels: Some(ConstraintValue::Exact(1)),
            format: None,
        }));
        let notification1 = CapabilityNotification::new("resample", "mic", caps1);
        ctx.add_pending_update(notification1);

        // Create second notification with 48000 Hz (should replace)
        let caps2 = MediaCapabilities::with_output(MediaConstraints::Audio(AudioConstraints {
            sample_rate: Some(ConstraintValue::Exact(48000)),
            channels: Some(ConstraintValue::Exact(2)),
            format: None,
        }));
        let notification2 = CapabilityNotification::new("resample", "mic", caps2);
        ctx.add_pending_update(notification2);

        // Verify only one update and it's the second one
        assert_eq!(ctx.nodes_with_pending_updates().len(), 1);
        let stored = ctx.get_pending_update("resample").unwrap();
        if let Some(MediaConstraints::Audio(audio)) = stored.upstream_output.default_output() {
            assert_eq!(audio.sample_rate, Some(ConstraintValue::Exact(48000)));
            assert_eq!(audio.channels, Some(ConstraintValue::Exact(2)));
        } else {
            panic!("Expected audio constraints");
        }
    }

    #[test]
    fn test_clear_pending_updates() {
        let mut ctx = ResolutionContext::new();

        // Add multiple pending updates
        let caps = MediaCapabilities::with_output(MediaConstraints::Audio(AudioConstraints {
            sample_rate: Some(ConstraintValue::Exact(48000)),
            channels: Some(ConstraintValue::Exact(2)),
            format: None,
        }));

        ctx.add_pending_update(CapabilityNotification::new("node1", "source", caps.clone()));
        ctx.add_pending_update(CapabilityNotification::new("node2", "source", caps.clone()));
        ctx.add_pending_update(CapabilityNotification::new("node3", "source", caps.clone()));

        assert_eq!(ctx.nodes_with_pending_updates().len(), 3);

        // Clear all
        ctx.clear_pending_updates();

        assert!(!ctx.has_pending_updates());
        assert_eq!(ctx.nodes_with_pending_updates().len(), 0);
    }
}
