// D1: Custom counting allocator — proves zero-copy hot path
// Activate in main.rs with:  #[global_allocator] static A: CountingAllocator = CountingAllocator;
// Usage: read ALLOC_COUNT before/after parse_event() to verify no hot-path heap allocations.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};

pub static ALLOC_COUNT: AtomicU64 = AtomicU64::new(0);

pub struct CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        System.alloc(layout)
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout)
    }
}
