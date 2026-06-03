# Banking Jitter Harness

A measurement harness that times Agave's banking-stage / consumer hot path against
Alpenglow Votor's per-round budget, and classifies the timing jitter into the three
buckets the lab asks for: CPU-scheduling delay, I/O stall, and allocator pressure.

Built as a workspace crate inside Agave, pinned to commit `f8bc56e` (master,
`4.2.0-alpha.0`).

## TL;DR result

- Steady-state banking is ~190 us end to end and ~85 us inside the consumer:
  comfortably inside Votor's 400 ms block budget, with ~1000x headroom.
- All meaningful jitter is a one-time **cold start**. End to end it is ~30 ms on
  the first iteration; the per-phase view shows it lands in `load_execute` and
  `commit` (the CPU + state-write phases), never `record` or `freeze_lock`.
- The large cold-start cost lives in **worker-thread startup** (lazy program
  loading, scheduler spin-up), which same-thread probes cannot see. That blind
  spot is itself a core lesson (see "tokio-console blind spot").

## Votor budget: where the deadline comes from

Source: Alpenglow White Paper v1.1, **Figure 7** (p22, Votor per-round lifecycle).
The numeric bounds are in the abstract, Section 1.5, Table 6, and Definition 17.

> Note: this is **not** Figure 2 (the double-Merkle block-data hierarchy). Calling
> the timeline "Figure 2" is a common mis-citation carried over from an older draft.

| Quantity | Meaning | Value |
|---|---|---|
| `Δ_block` | Normal block-production time. **What banking is judged against.** | 400 ms |
| `δ` | One all-to-all message delay among a >=θ-stake node set (assumed). | 50 ms |
| finalization | `min(δ_80%, 2·δ_60%)` -- fast path vs slow path, min wins. | min(50, 100) = 50 ms |
| liveness ceiling | `Timeout(i) = δ_timeout + Δ_block`, `δ_timeout ≈ 3·δ`. Give-up line, **not** a target. | 550 ms |

The harness grades the banking stage against **`Δ_block = 400 ms`**. The 550 ms
`Timeout` is a liveness ceiling, not the common-case latency goal, so it is reported
for context only, not used as the pass/fail line.

Votor does not run in Agave today (Agave uses TowerBFT); we borrow Votor's budget
number as a deadline/SLA and measure real banking code against it.

## Two harnesses

### 1. `measure_banking_stage_slot_timing` -- end to end

Spins up a real `BankingStage` (CentralSchedulerGreedy, 4 worker threads) plus a real
PoH recorder, sends one transfer per iteration through `non_vote_sender`, and times
until the committed entry arrives on `entry_receiver`. Work happens on crossbeam
worker threads, so this measures wall-clock latency only (microsecond resolution,
busy-yield wait). This is the harness where the dramatic cold start appears.

### 2. `measure_pre_phase_slot_timing` -- per phase + taxonomy

Drives the `Consumer` directly via `process_and_record_transactions`, which runs
**synchronously on the calling thread**, and reads Agave's own
`LeaderExecuteAndCommitTimings` (`load_execute`, `freeze_lock`, `record`, `commit`).
Because the work is on this thread, the jitter-taxonomy probes attribute cleanly.

## Jitter taxonomy: how each bucket is measured

The lab's core skill is proving *which* bucket a spike belongs to, not guessing.
On the synchronous pre-phase harness we snapshot two OS/runtime counters around each
call:

- **CPU-scheduling delay** -- field 2 of `/proc/thread-self/schedstat` is the
  nanoseconds this thread spent *runnable but waiting on a run-queue* (ready, but the
  OS ran something else). The delta around the call is scheduling jitter.
- **Allocator pressure** -- jemalloc's per-thread cumulative "bytes allocated"
  counter (`thread.allocated`, via `tikv-jemalloc-ctl`). The delta is allocation
  *volume*. This is a **light probe**: volume, not pause time. True malloc pause time
  would need a custom `GlobalAlloc` shim (deliberately out of scope).
- **I/O stall** -- the existing `record` phase already isolates the PoH write/channel
  wait; no extra probe needed.

The whole test binary uses jemalloc as its `#[global_allocator]` so the per-thread
counter reflects real allocation through the banking path.

The report prints, for each signal, the cold-start (iter 0) value next to the steady
median. The bucket whose iter-0 value most exceeds its steady baseline is the dominant
cause of the spike.

### Honest finding on the taxonomy

On the isolated consumer (Harness 2), the cold-start spike is mild (~320 us vs ~190
us steady) and the same-thread probes stay near baseline (CPU-sched ~0, allocator only
slightly up). That is a real result: by the time we call the consumer directly, most
heavy init is already warm. The big 30 ms cold start is in **Harness 1's worker-thread
startup**, which same-thread probes structurally cannot observe. Seeing that boundary
is the point -- the correct next tool for the worker spike is `perf` / a flamegraph
across the banking worker threads, not a same-thread counter.

## The tokio-console blind spot

`banking_stage.rs` looks like a tokio program but is a **hybrid**: one manager OS
thread hosts a current-thread tokio runtime (`tokio::select!` over a control channel +
`spawn_blocking` shims), while the real scheduler and N transaction workers are plain
`std::thread`s wired with crossbeam channels.

Consequence: `tokio-console` sees only the manager runtime and its `spawn_blocking`
shims (which sit "busy" forever joining lifelong threads). The actual scheduling /
execution / channel-wait jitter is in OS threads that are **invisible** to
tokio-console. Reaching for tokio-console first -- the natural move -- tells you
nothing about the hot path.

To demonstrate the blind spot yourself:

```bash
# requires the tokio_unstable cfg and a console-subscriber init in the harness
RUSTFLAGS="--cfg tokio_unstable" cargo test -p banking-jitter measure_banking_stage_slot_timing
# connect a separate `tokio-console` process; observe only the idle manager + shims
```

The OS-level probes above (`schedstat`, jemalloc counters) and `perf` are the correct
tools for the worker layer that tokio-console cannot reach.

## Build and run

This crate must live inside an Agave checkout (it uses path dependencies on
`../core`, `../runtime`, `../poh`, etc.).

```bash
# 1. clone + pin
git clone https://github.com/anza-xyz/agave && cd agave
git checkout f8bc56e

# 2. drop this crate in at agave/banking-jitter/ and add it to the workspace
#    members list in the root Cargo.toml:  "banking-jitter",

# 3. apply the 3-line exposure patch (see below)
git apply banking-jitter/agave-expose.patch

# 4. run
cargo test -p banking-jitter -- --nocapture --test-threads=1
```

## The required Agave patch

The per-phase harness needs three Agave symbols that are private at `f8bc56e`. The
patch (`agave-expose.patch`) is three lines, each marked `HARNESS-PATCH`, intended to
be reverted before any upstream work:

- `core/src/banking_stage.rs`: `mod committer;` -> `pub mod committer;`
- `core/src/banking_stage.rs`: `mod consumer;` -> `pub mod consumer;`
- `core/src/banking_stage/consumer.rs`: the `execute_and_commit_timings` field
  `pub(crate)` -> `pub`

The end-to-end harness uses only public APIs and needs no patch.

## Limitations

- Single-transaction batches, so `freeze_lock` (account-lock contention) is always ~0.
  Realistic contention needs conflicting multi-tx batches.
- Harness 2 runs an isolated `Consumer`, not the full multi-threaded scheduler.
- `δ = 50 ms` is an assumption; real one-way delay varies with stake distribution.
- Allocator probe measures allocation volume, not malloc pause time.
- The worker-thread cold start is observed end to end but not yet attributed at the
  thread level (next step: `perf` / per-tid schedstat across banking workers).
