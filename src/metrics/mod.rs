//! Real-time performance instrumentation.
//!
//! This module provides low-overhead latency probes for measuring
//! per-frame timings across the pipeline. Probes use `hdrhistogram`
//! for accurate p50/p99/p99.9/p99.99 tracking and are designed to
//! be safe to call from both async and sync/RT-priority threads.
//!
//! The probes are scaffolding for the tokio → RT-thread migration
//! (see the RT migration plan). Record deltas at the probe points,
//! read snapshots out-of-band to validate SLOs and regressions.

pub mod rt_probe;

pub use rt_probe::{
    CounterProbe, GaugeProbe, LatencyProbe, OperationalSnapshot, ProbeSnapshot, RtProbeSet,
};
