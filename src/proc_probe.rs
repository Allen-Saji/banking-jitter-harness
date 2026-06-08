//! Linux `/proc` probes: per-thread scheduling, context switches, off-CPU sleep
//! location, and page faults.
//!
//! These are the thread-agnostic substitutes for the view `tokio-console` cannot
//! give of Agave's OS worker threads. Linux-specific: every reader assumes a
//! Linux `/proc` host.

use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

// field 2 of /proc/thread-self/schedstat: ns this thread spent runnable but
// waiting on a run-queue.
pub fn self_runqueue_wait_ns() -> u64 {
    read_schedstat("/proc/thread-self/schedstat").1
}

// (cpu_ns, runqueue_wait_ns) from a schedstat file. Fields: on-cpu ns,
// run-queue wait ns, timeslices.
fn read_schedstat(path: &str) -> (u64, u64) {
    let s = std::fs::read_to_string(path).unwrap_or_default();
    let mut it = s.split_whitespace();
    let cpu = it.next().and_then(|x| x.parse().ok()).unwrap_or(0);
    let wait = it.next().and_then(|x| x.parse().ok()).unwrap_or(0);
    (cpu, wait)
}

// All threads of this process: tid -> comm name.
pub fn thread_names() -> BTreeMap<u64, String> {
    let mut out = BTreeMap::new();
    if let Ok(rd) = std::fs::read_dir("/proc/self/task") {
        for ent in rd.flatten() {
            if let Ok(tid) = ent.file_name().to_string_lossy().parse::<u64>() {
                let comm = std::fs::read_to_string(format!("/proc/self/task/{tid}/comm"))
                    .unwrap_or_default()
                    .trim()
                    .to_string();
                out.insert(tid, comm);
            }
        }
    }
    out
}

pub fn tid_schedstat(tid: u64) -> (u64, u64) {
    read_schedstat(&format!("/proc/self/task/{tid}/schedstat"))
}

// (voluntary, nonvoluntary) context switches for a specific thread.
pub fn tid_ctxt(tid: u64) -> (u64, u64) {
    let s = std::fs::read_to_string(format!("/proc/self/task/{tid}/status")).unwrap_or_default();
    let grab = |key: &str| {
        s.lines()
            .find(|l| l.starts_with(key))
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|x| x.parse().ok())
            .unwrap_or(0)
    };
    (grab("voluntary_ctxt_switches"), grab("nonvoluntary_ctxt_switches"))
}

// The kernel function a thread is currently blocked in ("0" if on-CPU /
// runnable). For a worker parked on a crossbeam channel this is a futex
// symbol; for one waiting on a pipe/poll it names that instead.
fn tid_wchan(tid: u64) -> String {
    std::fs::read_to_string(format!("/proc/self/task/{tid}/wchan"))
        .unwrap_or_default()
        .trim()
        .to_string()
}

// Off-CPU sampler: while `body` runs, a background thread polls each tid's
// wchan as fast as it can and tallies non-"0" sleep locations. The result
// pins *where* each worker spends its blocked time, which is how the
// residual cold-start wait is attributed without patching spans into the
// worker loop. Returns (body result, tid -> {wchan -> sample count}).
pub fn sample_wchan_during<R>(
    tids: &[u64],
    body: impl FnOnce() -> R,
) -> (R, BTreeMap<u64, BTreeMap<String, u32>>) {
    let running = Arc::new(AtomicBool::new(true));
    let counts: Arc<Mutex<BTreeMap<u64, BTreeMap<String, u32>>>> =
        Arc::new(Mutex::new(BTreeMap::new()));
    let (r2, c2, tids2) = (running.clone(), counts.clone(), tids.to_vec());
    let sampler = std::thread::spawn(move || {
        while r2.load(Ordering::Relaxed) {
            for &t in &tids2 {
                let w = tid_wchan(t);
                if !w.is_empty() && w != "0" {
                    *c2.lock().unwrap().entry(t).or_default().entry(w).or_default() += 1;
                }
            }
            std::hint::spin_loop();
        }
    });
    let res = body();
    running.store(false, Ordering::Relaxed);
    sampler.join().unwrap();
    let counts = Arc::try_unwrap(counts).unwrap().into_inner().unwrap();
    (res, counts)
}

// (minor_faults, major_faults) from /proc/self/stat. comm is parenthesized
// and may contain spaces, so parse after the last ')'.
pub fn proc_faults() -> (u64, u64) {
    let s = std::fs::read_to_string("/proc/self/stat").unwrap_or_default();
    let rest = s.rsplit_once(')').map(|(_, r)| r).unwrap_or(&s);
    let f: Vec<u64> = rest.split_whitespace().map(|x| x.parse().unwrap_or(0)).collect();
    // after ')': index 0 = state(field 3); minflt = field 10 = index 7,
    // majflt = field 12 = index 9.
    (f.get(7).copied().unwrap_or(0), f.get(9).copied().unwrap_or(0))
}
