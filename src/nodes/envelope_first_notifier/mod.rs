//! `EnvelopeFirstNotifierNode` — fires once on the first input whose
//! Json `kind` matches a configured target, emitting a synthetic
//! `{"kind":"first_emit", ...}` envelope. Subsequent inputs are
//! silently dropped (passthrough is intentionally NOT supported — wire
//! this as a parallel sink off the source you want to observe).
//!
//! Used by the offline-batch smoke binary to synchronize the audio
//! timeline (Kokoro) with the motion timeline (Kimodo): kimodo can
//! take 10–90 s to diffuse, so we send `motion_intent` first, watch
//! for this notifier's signal on `output_rx`, then send the `Text`
//! packet so that audio + animation begin in lockstep.

mod node;

pub use node::{EnvelopeFirstNotifierConfig, EnvelopeFirstNotifierNode};
