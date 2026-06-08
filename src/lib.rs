//! Banking-stage jitter harness.
//!
//! Reusable measurement primitives for timing Agave's banking-stage / consumer
//! hot path against Alpenglow Votor's per-round budget, and classifying the
//! timing jitter into the buckets the lab asks for: CPU-scheduling delay, I/O
//! stall, and allocator pause.
//!
//! This crate is split into a small library of host probes plus the three
//! measurement runs that drive real Agave code.
//!
//! Library (pure host probes, no Agave dependencies):
//!   * [`alloc`]      -- timing global allocator (true malloc/free pause time).
//!   * [`proc_probe`] -- Linux `/proc` probes: per-thread schedstat, context
//!                       switches, a `wchan` off-CPU sampler, and page faults.
//!   * [`stats`]      -- percentile summaries for latency vectors.
//!   * [`votor`]      -- the Alpenglow Votor timing budget and its report.
//!
//! Harnesses (drive real Agave code, so they live in `tests/jitter/` and use the
//! `[dev-dependencies]`; one integration binary, see `tests/jitter.rs`):
//!   * `end_to_end`  -- `measure_banking_stage_slot_timing`: end-to-end
//!     send->committed-entry latency through a real `BankingStage`, with
//!     per-worker-thread CPU and run-queue attribution (the part `tokio-console`
//!     cannot see) plus page-fault / context-switch counters.
//!   * `per_phase`   -- `measure_pre_phase_slot_timing`: per-phase timings read
//!     from the `Consumer`, run synchronously so the same-thread probes
//!     (schedstat + true allocator pause time) attribute cleanly.
//!   * `contention`  -- `measure_account_lock_contention`: the real effect of
//!     conflicting transactions (account-lock serialization).
//!
//! The test binary installs [`alloc::TimingAlloc`] as its `#[global_allocator]`,
//! so allocator pause time is measured directly rather than inferred.

pub mod alloc;
pub mod proc_probe;
pub mod stats;
pub mod votor;
