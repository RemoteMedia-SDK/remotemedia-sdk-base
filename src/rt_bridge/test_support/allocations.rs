use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

struct CountingAllocator;
thread_local! {
    static COUNTS: Cell<Option<(usize, usize)>> = const { Cell::new(None) };
}
fn count(allocation: bool) {
    let _ = COUNTS.try_with(|counts| {
        if let Some((allocs, frees)) = counts.get() {
            counts.set(Some((
                allocs + usize::from(allocation),
                frees + usize::from(!allocation),
            )));
        }
    });
}
// Forward exact pointers/layouts to System, measuring only the callback thread.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count(true);
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        count(true);
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        count(true);
        unsafe { System.realloc(ptr, layout, size) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        count(false);
        unsafe { System.dealloc(ptr, layout) }
    }
}
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

pub fn callback<T>(run: impl FnOnce() -> T) -> T {
    let (result, counts) = measure(run);
    assert_eq!(counts, (0, 0), "callback allocated or freed storage");
    result
}

fn measure<T>(run: impl FnOnce() -> T) -> (T, (usize, usize)) {
    COUNTS.with(|counts| counts.set(Some((0, 0))));
    let result = run();
    let counts = COUNTS.with(|counts| counts.replace(None).unwrap());
    (result, counts)
}

#[test]
fn host_allocation_probe_detects_positive_control() {
    let (_, (allocs, frees)) = measure(|| {
        let value = Box::new([0u8; 64]);
        std::hint::black_box(&value);
        drop(value);
    });
    assert!(allocs > 0 && frees > 0);
}
