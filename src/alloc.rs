//! Timing global allocator (heavy probe).
//!
//! Wraps jemalloc. When the per-thread `MEASURING` flag is set, every alloc /
//! dealloc / realloc is timed and the nanoseconds (and byte volume) accumulate
//! in thread-local counters. When the flag is clear (the default, and the case
//! for every Agave worker thread during setup), the wrapper adds only a
//! thread-local bool check, so global overhead is negligible.
//!
//! Install [`TimingAlloc`] as the binary's `#[global_allocator]`, then wrap the
//! region of interest in [`measure_alloc`] to read the true time spent in
//! malloc / free on the calling thread.

use std::{
    alloc::{GlobalAlloc, Layout},
    cell::Cell,
    time::Instant,
};

use tikv_jemallocator::Jemalloc;

thread_local! {
    static MEASURING: Cell<bool> = const { Cell::new(false) };
    static ALLOC_NS: Cell<u64> = const { Cell::new(0) };
    static ALLOC_BYTES: Cell<u64> = const { Cell::new(0) };
}

pub struct TimingAlloc;

#[inline(always)]
fn timed<T>(bytes: usize, f: impl FnOnce() -> T) -> T {
    if MEASURING.with(Cell::get) {
        let t = Instant::now();
        let r = f();
        let ns = t.elapsed().as_nanos() as u64;
        ALLOC_NS.with(|c| c.set(c.get().wrapping_add(ns)));
        ALLOC_BYTES.with(|c| c.set(c.get().wrapping_add(bytes as u64)));
        r
    } else {
        f()
    }
}

unsafe impl GlobalAlloc for TimingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        timed(layout.size(), || unsafe { Jemalloc.alloc(layout) })
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // dealloc cost counts toward pause time but not allocated bytes.
        timed(0, || unsafe { Jemalloc.dealloc(ptr, layout) })
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        timed(layout.size(), || unsafe { Jemalloc.alloc_zeroed(layout) })
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        timed(new_size, || unsafe { Jemalloc.realloc(ptr, layout, new_size) })
    }
}

/// Returns `(value, nanoseconds spent in the allocator, bytes allocated)` on the
/// calling thread during `f`.
///
/// Requires [`TimingAlloc`] to be installed as the `#[global_allocator]`;
/// otherwise the counters stay zero.
pub fn measure_alloc<T>(f: impl FnOnce() -> T) -> (T, u64, u64) {
    ALLOC_NS.with(|c| c.set(0));
    ALLOC_BYTES.with(|c| c.set(0));
    MEASURING.with(|c| c.set(true));
    let r = f();
    MEASURING.with(|c| c.set(false));
    let ns = ALLOC_NS.with(Cell::get);
    let bytes = ALLOC_BYTES.with(Cell::get);
    (r, ns, bytes)
}
