//! Session management and routing
//!
//! Manages streaming sessions and routes data between peers and pipelines.

pub mod router;
#[allow(clippy::module_inception)]
pub mod session;

// Re-exports for public API - used by external consumers of this crate
#[allow(unused_imports)]
pub use router::{
    // Phase 6 routing types
    BitrateAction,
    OutputRoute,
    PeerBitrateState,
    QualityTier,
    // Phase 8 recovery types
    ReconnectionEvent,
    RoutingPolicy,
    SessionMetrics,
    SessionRouter,
    SessionStateSnapshot,
};
pub use session::{Session, SessionId, SessionManager, SessionState};
