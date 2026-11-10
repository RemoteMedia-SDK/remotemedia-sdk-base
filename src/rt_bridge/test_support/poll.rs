use std::time::{Duration, Instant};

pub fn until_ready<T>(mut poll: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(value) = poll() {
            return value;
        }
        assert!(Instant::now() < deadline, "worker did not make progress");
        std::thread::yield_now();
    }
}
