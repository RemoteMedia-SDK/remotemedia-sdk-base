//! Diagnostic streaming node for inspecting the values flowing through
//! an avatar pipeline. Lives behind no feature gate — useful in any
//! manifest that wants to peek at blendshape / skeletal_pose / generic
//! Json envelopes without affecting the data flow.

mod envelope_debug_log;

pub use envelope_debug_log::{EnvelopeDebugLogConfig, EnvelopeDebugLogNode};
