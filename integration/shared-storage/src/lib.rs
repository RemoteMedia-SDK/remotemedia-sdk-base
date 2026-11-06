//! Compile the production pool and bridge with only their direct dependencies.
//! This runs their actual ownership tests without unrelated transport/model
//! builds. Full runtime integration tests remain a separate verification gate.

#[path = "../../../src/data/audio_buffer_pool.rs"]
pub mod audio_buffer_pool;
#[path = "../../../src/data/audio_samples.rs"]
pub mod audio_samples;
#[path = "../../../src/multiprocess/data_transfer.rs"]
pub mod data_transfer;

// Preserve the module path used by the production files' unit tests.
pub mod data {
    pub use crate::audio_buffer_pool;
}
