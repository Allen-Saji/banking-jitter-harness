//! Banking-stage jitter harness -- the three measurement runs.
//!
//! Kept as one integration binary so the heavy `solana-core` dependency graph
//! links once. The harness modules live in `tests/jitter/` and are pulled in
//! via `#[path]` so they are not each compiled as a separate test binary.
//!
//! This binary installs the timing allocator globally; only `per_phase` turns
//! measurement on (via `banking_jitter::alloc::measure_alloc`), so the other
//! harnesses pay just a thread-local bool check per allocation.

#[global_allocator]
static GLOBAL: banking_jitter::alloc::TimingAlloc = banking_jitter::alloc::TimingAlloc;

#[path = "jitter/common.rs"]
mod common;
#[path = "jitter/contention.rs"]
mod contention;
#[path = "jitter/end_to_end.rs"]
mod end_to_end;
#[path = "jitter/per_phase.rs"]
mod per_phase;
