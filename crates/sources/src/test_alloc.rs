//! The unit tests' allocator, jemalloc, and this thread's allocation
//! statistics: what a test measures working memory with (a single-thread
//! runtime keeps everything it measures on the test's thread).

// Reading jemalloc's statistics is a foreign call.
#![allow(unsafe_code)]

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// A u64 statistic of this thread.
fn stat(name: &std::ffi::CStr) -> u64 {
    let mut value: u64 = 0;
    let mut len = std::mem::size_of::<u64>();
    // SAFETY: the named statistic is one u64 written into `value`.
    let rc = unsafe {
        tikv_jemalloc_sys::mallctl(
            name.as_ptr(),
            (&mut value as *mut u64).cast(),
            &mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    assert_eq!(rc, 0, "{name:?}");
    value
}

/// The bytes this thread holds now (allocated less deallocated, each
/// allocation at its size class).
pub(crate) fn held() -> i64 {
    stat(c"thread.allocated") as i64 - stat(c"thread.deallocated") as i64
}

/// The bytes jemalloc gives a request of `n` bytes (its size class).
pub(crate) fn size_class(n: usize) -> usize {
    // SAFETY: `nallocx` only computes a size; it allocates nothing.
    unsafe { tikv_jemalloc_sys::nallocx(n, 0) }
}

/// Reset this thread's allocation peak.
pub(crate) fn reset_peak() {
    // SAFETY: a documented write-only mallctl with no argument.
    let rc = unsafe {
        tikv_jemalloc_sys::mallctl(
            c"thread.peak.reset".as_ptr(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            0,
        )
    };
    assert_eq!(rc, 0, "thread.peak.reset");
}

/// This thread's highest (allocated - deallocated) since the last reset,
/// above what it held at the reset.
pub(crate) fn peak() -> u64 {
    stat(c"thread.peak.read")
}
