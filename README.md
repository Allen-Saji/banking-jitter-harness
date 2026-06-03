# Banking Jitter Harness

A measurement harness that times Agave's banking-stage / consumer hot path against
Alpenglow Votor's per-round budget, and classifies the timing jitter into the buckets
the lab asks for: CPU-scheduling delay, I/O stall, and allocator pause.

Built as a workspace crate inside Agave, pinned to commit `f8bc56e` (master,
`4.2.0-alpha.0`).

## TL;DR result

- Steady-state banking is ~200 us end to end and ~90 us inside the consumer:
  comfortably inside Votor's 400 ms block budget, with roughly 1000x headroom.
- All meaningful jitter is a one-time **cold start**, ~30 ms on the first end-to-end
  iteration. The harness measures what causes it rather than guessing, and the
  evidence rules out the obvious explanations:
  - Not the allocator: a timing allocator shows 10 us of malloc/free on the cold
    iteration vs 9 us steady, essentially identical.
  - Not Agave CPU: during the 31 ms cold window, all Agave worker threads combined
    use under 1 ms of CPU.
  - Not scheduling (run-queue wait ~0) and not disk (major page faults 0).
  - What is elevated: ~2200 minor page faults (first-touch demand paging) vs ~170
    steady.
- Diagnosis (located, not inferred): an off-CPU `wchan` sampler shows all ~30 banking
  worker threads parked in `futex_wait_queue` for the full cold window with ~0 CPU,
  the manager thread in `ep_poll`, and only PoH ticking (~600 us). The cold start is
  parked-thread wakeup / first-message latency plus first-touch paging, not
  computation, allocation, or scheduling. (The manager sitting in `ep_poll` also
  independently confirms the hybrid tokio-plus-OS-threads architecture below.)

## Votor budget: where the deadline comes from

Source: Alpenglow White Paper v1.1, **Figure 7** (p22, Votor per-round lifecycle).
The numeric bounds are in the abstract, Section 1.5, Table 6, and Definition 17.

| Quantity | Meaning | Value |
|---|---|---|
| `delta_block` | Normal block-production time. **What banking is judged against.** | 400 ms |
| `delta` | One all-to-all message delay among a >=theta-stake node set (assumed). | 50 ms |
| finalization | `min(delta_80, 2*delta_60)`, fast path vs slow path, min wins. | min(50, 100) = 50 ms |
| liveness ceiling | `Timeout(i) = 3*delta + delta_block`. Give-up line, **not** a target. | 550 ms |

The harness grades the banking stage against `delta_block = 400 ms`. The 550 ms
`Timeout` is a liveness ceiling, not the common-case latency goal, so it is reported
for context only. `delta` is an assumption, so the report also sweeps it across
25 / 50 / 100 ms to show how the finalization and liveness numbers move.

Votor does not run in Agave today (Agave uses TowerBFT); the harness borrows Votor's
budget as a deadline and measures real banking code against it.

## Harnesses

### 1. `measure_banking_stage_slot_timing` -- end to end + worker attribution

Drives a real `BankingStage` (CentralSchedulerGreedy, 4 workers) and times send to
committed entry. On the cold iteration and one steady iteration it snapshots **every
Agave thread's `schedstat`** (on-CPU ns, run-queue-wait ns) and **per-thread context
switches** (`/proc/self/task/<tid>/status`), plus process page faults. On the cold
iteration it additionally runs an **off-CPU `wchan` sampler**: a background thread
polls each worker's `/proc/self/task/<tid>/wchan` and tallies where it is parked, so
the residual wakeup wait is located (a futex / channel wait) rather than guessed. This
is the answer to the tokio-console blind spot (below): the OS worker threads it cannot
see are read directly from `/proc`.

### 2. `measure_pre_phase_slot_timing` -- per phase + true allocator pause

Drives the `Consumer` directly via `process_and_record_transactions`, which runs
synchronously on the calling thread, and reads Agave's own
`LeaderExecuteAndCommitTimings` (`load_execute`, `freeze_lock`, `record`, `commit`).
Two same-thread probes wrap the call: run-queue-wait from `schedstat`, and **true
allocator pause time** from the timing allocator (see below).

### 3. `measure_account_lock_contention` -- the real contention signal

Submits a batch of transactions that all write the same two accounts. Account-lock
contention shows up as a drop in committed count (the conflicting subset is serialized
out), **not** as `freeze_lock` time. This corrects an earlier framing: `freeze_lock`
guards the bank freeze RwLock, a different lock that stays ~0 without a concurrent
freezer.

## Jitter taxonomy: how each bucket is measured

- **CPU-scheduling delay** -- field 2 of `schedstat` is the nanoseconds a thread spent
  *runnable but waiting on a run-queue*. Read per-thread (Harness 1) and same-thread
  (Harness 2).
- **Allocator pause (true pause time)** -- the test binary's `#[global_allocator]` is a
  `TimingAlloc` shim that wraps jemalloc. A per-thread flag enables timing only around
  the measured region, so every Agave worker pays just a thread-local bool check during
  setup, while the measured call records real nanoseconds spent in malloc / free /
  realloc. This is the heavy probe: actual pause time, not an allocation-volume proxy.
- **I/O stall** -- isolated by the existing `record` phase (PoH write / channel wait).
- **What the spike is** -- process minor/major page-fault counts from `/proc/self/stat`
  separate demand-paged code (minor faults) from disk (major faults).

## The tokio-console blind spot

`banking_stage.rs` looks like a tokio program but is a **hybrid**: one manager OS thread
hosts a current-thread tokio runtime (`tokio::select!` over a control channel plus
`spawn_blocking` shims), while the real scheduler and N transaction workers are plain
`std::thread`s wired with crossbeam channels.

`tokio-console` therefore sees only the manager runtime and its `spawn_blocking` shims
(which sit "busy" forever joining lifelong threads). The actual scheduling, execution,
and channel-wait jitter is in OS threads that are invisible to it.

This harness defeats the blind spot by reading the worker threads directly from `/proc`
by thread id (`schedstat` per `solBnkTxSched`, `solCoWorker00..03`, `BankingMgr`, etc).
`schedstat` and the timing allocator are thread-agnostic; tokio-console is the thing
that artificially restricts the view. To reproduce the tokio side for contrast:

```bash
RUSTFLAGS="--cfg tokio_unstable" cargo test -p banking-jitter measure_banking_stage_slot_timing
# connect a separate `tokio-console`; observe only the idle manager + shims
```

## Build and run

This crate must live inside an Agave checkout (path dependencies on `../core`,
`../runtime`, `../poh`, etc).

```bash
# 1. clone + pin
git clone https://github.com/anza-xyz/agave && cd agave
git checkout f8bc56e

# 2. drop this crate in at agave/banking-jitter/ and add it to the workspace
#    members list in the root Cargo.toml:  "banking-jitter",

# 3. apply the 3-line exposure patch
git apply banking-jitter/agave-expose.patch

# 4. run (single-threaded so the harnesses do not contend for cores)
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

The end-to-end and contention harnesses use only public APIs.

## Limitations and honest boundaries

- Single-transaction batches in Harnesses 1 and 2, so `freeze_lock` (bank freeze
  RwLock) is ~0; Harness 3 shows the account-lock contention signal instead.
- The `wchan` sampler is opportunistic: it captures a thread's sleep location only
  while blocked (an on-CPU thread reads `wchan` as `0`), and its poll rate is best
  effort. It identifies *where* time is spent parked, not an exact per-park duration.
- `delta = 50 ms` is an assumption (swept across 25/50/100 ms in the report, not
  measured live).
- Harness 2 runs an isolated `Consumer`, not the full multi-threaded scheduler;
  Harness 1 covers the real multi-threaded path end to end.
- Linux-specific: the `/proc` schedstat, status, and wchan probes assume a Linux host.
