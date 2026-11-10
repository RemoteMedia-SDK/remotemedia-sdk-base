use remotemedia_shared_memory::{PoolConfig, SessionId, SharedPool, SizeClass};

pub fn pool(slots: usize) -> SharedPool {
    SharedPool::new(
        SessionId([41; 16]),
        PoolConfig {
            classes: vec![SizeClass { bytes: 4096, slots }],
            max_bytes: slots * 4096,
            max_leases: 16,
        },
    )
    .unwrap()
}
