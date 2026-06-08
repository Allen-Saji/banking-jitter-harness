//! Harness 2: per-phase timings + same-thread taxonomy with true allocator
//! pause time.
//!
//! Drives the `Consumer` directly via `process_and_record_transactions`, which
//! runs synchronously on the calling thread, so the same-thread probes
//! (run-queue wait + true allocator pause) attribute cleanly.

use {
    banking_jitter::{
        alloc::measure_alloc,
        proc_probe::self_runqueue_wait_ns,
        stats::{print_row, summarize},
        votor::print_votor_budget,
    },
    crossbeam_channel::unbounded,
    solana_core::banking_stage::{committer::Committer, consumer::Consumer},
    solana_ledger::genesis_utils::GenesisConfigInfo,
    solana_poh::{record_channels::record_channels, transaction_recorder::TransactionRecorder},
    solana_runtime::bank::Bank,
    solana_system_transaction as system_transaction,
    std::time::Instant,
};

use crate::common::{N_ITERATIONS, create_slow_genesis_config, sanitize_transactions};

#[test]
fn measure_pre_phase_slot_timing() {
    let GenesisConfigInfo { genesis_config, mint_keypair, .. } =
        create_slow_genesis_config(1_000_000_000);
    let (bank, _forks) = Bank::new_with_bank_forks_for_tests(&genesis_config);

    let (record_sender, mut record_receiver) = record_channels(false);
    let recorder = TransactionRecorder::new(record_sender);
    record_receiver.restart(bank.bank_id());
    let (replay_vote_sender, _) = unbounded();
    let committer = Committer::new(None, replay_vote_sender, None);
    let consumer = Consumer::new(committer, recorder, None);

    let mut load_exec = Vec::with_capacity(N_ITERATIONS);
    let mut freeze = Vec::with_capacity(N_ITERATIONS);
    let mut record = Vec::with_capacity(N_ITERATIONS);
    let mut commit = Vec::with_capacity(N_ITERATIONS);
    let mut wall_us = Vec::with_capacity(N_ITERATIONS);
    let mut sched_wait_us = Vec::with_capacity(N_ITERATIONS);
    let mut alloc_pause_us = Vec::with_capacity(N_ITERATIONS);
    let mut alloc_kib = Vec::with_capacity(N_ITERATIONS);

    for _ in 0..N_ITERATIONS {
        let start_hash = bank.last_blockhash();
        let recipient = solana_pubkey::new_rand();
        let tx = system_transaction::transfer(&mint_keypair, &recipient, 1, start_hash);
        let txs = sanitize_transactions(vec![tx]);

        let sched0 = self_runqueue_wait_ns();
        let t0 = Instant::now();
        // Measured region: real allocator nanoseconds on this thread.
        let (output, alloc_ns, alloc_bytes) =
            measure_alloc(|| consumer.process_and_record_transactions(&bank, &txs));
        let wall = t0.elapsed().as_micros() as u64;
        let sched_delta = self_runqueue_wait_ns().saturating_sub(sched0);

        let t = &output.execute_and_commit_transactions_output.execute_and_commit_timings;
        load_exec.push(t.load_execute_us);
        freeze.push(t.freeze_lock_us);
        record.push(t.record_us);
        commit.push(t.commit_us);
        wall_us.push(wall);
        sched_wait_us.push(sched_delta / 1000);
        alloc_pause_us.push(alloc_ns / 1000);
        alloc_kib.push(alloc_bytes / 1024);
    }

    println!("\n=== Harness 2: per-phase + same-thread taxonomy ===");
    println!("  iterations : {N_ITERATIONS}");
    println!("\n  phase timings (iters 1..{N_ITERATIONS}, microseconds):");
    println!("  {:<14} {:>8} {:>8} {:>8} {:>8} {:>8}", "phase", "min", "mean", "p50", "p99", "max");
    for (name, v) in [
        ("load_execute", &load_exec),
        ("freeze_lock", &freeze),
        ("record", &record),
        ("commit", &commit),
        ("wall_total", &wall_us),
    ] {
        print_row(name, &v[1..]);
    }

    println!("\n  jitter taxonomy -- cold start (iter 0) vs steady median:");
    println!("  {:<26} {:>10} {:>12}", "signal", "iter 0", "steady p50");
    for (name, v) in [
        ("CPU-sched wait (us)", &sched_wait_us),
        ("allocator pause (us)", &alloc_pause_us),
        ("allocator volume (KiB)", &alloc_kib),
        ("I/O record phase (us)", &record),
        ("wall total (us)", &wall_us),
    ] {
        println!("  {name:<26} {:>10} {:>12}", v[0], summarize(&v[1..]).p50);
    }
    println!("\n  allocator pause is TRUE time spent in malloc/free (timing GlobalAlloc),");
    println!("  not a volume proxy. The bucket whose iter-0 value most exceeds its");
    println!("  steady p50 dominates the cold-start spike.");

    print_votor_budget();
    println!("\n  caveats: single-tx batches; freeze_lock is the bank freeze RwLock and is");
    println!("  ~0 without a concurrent freezer (see measure_account_lock_contention for");
    println!("  the real contention signal); δ assumption swept above.");
}
