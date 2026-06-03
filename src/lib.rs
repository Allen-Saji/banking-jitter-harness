//! Banking-stage jitter harness.
//!
//! Times Agave's banking-stage / consumer hot path against Alpenglow Votor's
//! per-round budget, and classifies the jitter into the three buckets the lab
//! asks for: CPU-scheduling delay, I/O stall, and allocator pause.
//!
//! Two harnesses:
//!   * `measure_banking_stage_slot_timing` -- end-to-end send->committed-entry
//!     latency through a real `BankingStage` (crossbeam worker threads).
//!   * `measure_pre_phase_slot_timing` -- per-phase timings read straight from
//!     the `Consumer`, run synchronously on the calling thread so the taxonomy
//!     probes (schedstat + jemalloc) attribute cleanly.
//!
//! The whole test binary allocates through jemalloc so the per-thread
//! allocation counter is meaningful.

#[cfg(test)]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(test)]
mod tests {
    use {
        agave_banking_stage_ingress_types::BankingPacketBatch,
        crossbeam_channel::unbounded,
        solana_core::{
            banking_stage::{
                BankingStage, committer::Committer, consumer::Consumer,
                transaction_scheduler::scheduler_controller::SchedulerConfig,
            },
            banking_trace::{BankingTracer, Channels},
            validator::{BlockProductionMethod, SchedulerPacing},
        },
        solana_entry::entry_or_marker::EntryOrMarker,
        solana_ledger::{
            blockstore::Blockstore,
            genesis_utils::{
                GenesisConfigInfo, bootstrap_validator_stake_lamports,
                create_genesis_config_with_leader,
            },
            get_tmp_ledger_path_auto_delete,
        },
        solana_perf::packet::to_packet_batches,
        solana_poh::{
            poh_recorder::create_test_recorder, record_channels::record_channels,
            transaction_recorder::TransactionRecorder,
        },
        solana_runtime::bank::Bank,
        solana_runtime_transaction::runtime_transaction::RuntimeTransaction,
        solana_system_transaction as system_transaction,
        solana_transaction::{Transaction, sanitized::SanitizedTransaction},
        std::{
            num::NonZeroUsize,
            sync::{Arc, atomic::Ordering},
            time::Instant,
        },
        tokio::sync::mpsc,
    };

    // ---- Alpenglow Votor timing budget -----------------------------------
    // Source: Alpenglow White Paper v1.1 -- Figure 7 (p22, Votor per-round
    // lifecycle), with the numeric bounds in the abstract, Section 1.5,
    // Table 6, and Definition 17. (NOT Figure 2, which is the double-Merkle
    // block-data hierarchy -- a common mis-citation from an older draft.)
    //
    // Block-production budget (what the banking stage is judged against):
    const DELTA_BLOCK_MS: u64 = 400; // Δ_block -- normal block time.
    //
    // δ = one all-to-all message delay among a >=θ-stake node set (assumed).
    const DELTA_MS: u64 = 50;
    //
    // Finalization budget (consensus layer, for context only):
    //   fast path = δ_80%       (NotarVote >=80% -> Fast-Finalization Cert)
    //   slow path = 2·δ_60%     (Notar >=60% -> FinalVote >=60% -> Finalization)
    //   finalize  = min(fast, slow)
    // Both run concurrently; min wins. With δ≈50ms: fast=50, slow=100, min=50.
    const FINALIZE_FAST_MS: u64 = DELTA_MS;
    const FINALIZE_SLOW_MS: u64 = 2 * DELTA_MS;
    //
    // Liveness give-up ceiling (NOT the common-case target):
    //   Timeout(i) = δ_timeout + Δ_block,  δ_timeout ≈ 3·δ
    const DELTA_TIMEOUT_MS: u64 = 3 * DELTA_MS;
    const TIMEOUT_CEILING_MS: u64 = DELTA_TIMEOUT_MS + DELTA_BLOCK_MS;

    // Iterations. Warmup (iter 0) is reported separately from steady state.
    const N_ITERATIONS: usize = 100;

    fn create_slow_genesis_config(lamports: u64) -> GenesisConfigInfo {
        let validator_pubkey = solana_pubkey::new_rand();
        let mut info = create_genesis_config_with_leader(
            lamports,
            &validator_pubkey,
            bootstrap_validator_stake_lamports(),
        );
        // Extend ticks so the slot doesn't expire across N iterations.
        info.genesis_config.ticks_per_slot *= 1024;
        info
    }

    // ---- stats helpers ----------------------------------------------------

    struct Summary {
        min: u64,
        mean: u64,
        p50: u64,
        p99: u64,
        max: u64,
    }

    fn summarize(vals: &[u64]) -> Summary {
        if vals.is_empty() {
            return Summary { min: 0, mean: 0, p50: 0, p99: 0, max: 0 };
        }
        let mut s = vals.to_vec();
        s.sort_unstable();
        let pct = |p: f64| -> u64 {
            let rank = (p / 100.0 * (s.len() - 1) as f64).round() as usize;
            s[rank]
        };
        Summary {
            min: s[0],
            mean: s.iter().sum::<u64>() / s.len() as u64,
            p50: pct(50.0),
            p99: pct(99.0),
            max: s[s.len() - 1],
        }
    }

    fn print_row(name: &str, vals: &[u64]) {
        let s = summarize(vals);
        println!(
            "  {name:<14} {:>8} {:>8} {:>8} {:>8} {:>8}",
            s.min, s.mean, s.p50, s.p99, s.max
        );
    }

    // ---- jitter-taxonomy probes (same-thread only) ------------------------

    // CPU-scheduling delay: field 2 of /proc/thread-self/schedstat is the
    // nanoseconds this thread spent *runnable but waiting on a run-queue*
    // (ready to run, but the OS gave the CPU to something else). Delta around
    // a synchronous call = scheduling jitter the thread absorbed.
    fn runqueue_wait_ns() -> u64 {
        std::fs::read_to_string("/proc/thread-self/schedstat")
            .ok()
            .and_then(|s| s.split_whitespace().nth(1).map(str::to_owned))
            .and_then(|f| f.parse().ok())
            .unwrap_or(0)
    }

    // Allocator activity (light probe): jemalloc's per-thread cumulative
    // "bytes allocated" counter. Delta = bytes this thread allocated during the
    // call. This is allocation *volume*, a proxy for allocator pressure -- NOT
    // pause time. True malloc pause time would need a custom GlobalAlloc shim.
    fn thread_allocated() -> u64 {
        use tikv_jemalloc_ctl::thread;
        thread::allocatedp::mib()
            .and_then(|m| m.read())
            .map(|a| a.get())
            .unwrap_or(0)
    }

    // sanitize_transactions is pub(crate) in agave's test module -- not importable.
    fn sanitize_transactions(
        txs: Vec<Transaction>,
    ) -> Vec<RuntimeTransaction<SanitizedTransaction>> {
        txs.into_iter()
            .map(RuntimeTransaction::from_transaction_for_tests)
            .collect()
    }

    fn print_votor_header() {
        println!("\nVotor budget (Alpenglow v1.1, Fig 7 + Table 6 + Def 17):");
        println!("  block production : Δ_block        = {DELTA_BLOCK_MS} ms   <-- banking is judged here");
        println!(
            "  finalization     : min(δ_80%, 2·δ_60%) = min({FINALIZE_FAST_MS}, {FINALIZE_SLOW_MS}) = {} ms   (consensus layer)",
            FINALIZE_FAST_MS.min(FINALIZE_SLOW_MS)
        );
        println!("  liveness ceiling : δ_timeout+Δ_block = {TIMEOUT_CEILING_MS} ms   (give-up line, NOT a target)");
    }

    // ====================================================================
    // Harness 1: end-to-end send -> committed entry latency.
    // Work runs on real crossbeam worker threads; we measure wall-clock only.
    // ====================================================================
    #[test]
    fn measure_banking_stage_slot_timing() {
        agave_logger::setup();

        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_slow_genesis_config(1_000_000_000);

        let (bank, bank_forks) = Bank::new_with_bank_forks_for_tests(&genesis_config);
        let start_hash = bank.last_blockhash();

        let banking_tracer = BankingTracer::new_disabled();
        let Channels {
            non_vote_sender,
            non_vote_receiver,
            tpu_vote_sender,
            tpu_vote_receiver,
            gossip_vote_sender,
            gossip_vote_receiver,
        } = banking_tracer.create_channels();

        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Arc::new(Blockstore::open(ledger_path.path()).expect("open blockstore"));

        let (
            exit,
            poh_recorder,
            _poh_controller,
            transaction_recorder,
            poh_service,
            entry_receiver,
        ) = create_test_recorder(bank, blockstore, None, None);

        let (replay_vote_sender, _replay_vote_receiver) = unbounded();

        let banking_stage = BankingStage::new_num_threads(
            BlockProductionMethod::CentralSchedulerGreedy,
            poh_recorder.clone(),
            transaction_recorder,
            non_vote_receiver,
            tpu_vote_receiver,
            gossip_vote_receiver,
            mpsc::channel(1).1,
            NonZeroUsize::new(4).unwrap(),
            SchedulerConfig {
                scheduler_pacing: SchedulerPacing::Disabled,
            },
            None,
            replay_vote_sender,
            None,
            bank_forks,
            None,
            Arc::default(),
        );

        // Each iteration sends one transfer and times until the committed entry
        // arrives. Microsecond resolution; busy-yield instead of a 1ms sleep so
        // sub-millisecond latencies are visible.
        let mut latency_us: Vec<u64> = Vec::with_capacity(N_ITERATIONS);

        for _ in 0..N_ITERATIONS {
            let recipient = solana_pubkey::new_rand();
            let tx = system_transaction::transfer(&mint_keypair, &recipient, 1, start_hash);
            let batches = to_packet_batches(&[tx], 1);

            let t0 = Instant::now();
            non_vote_sender
                .send(BankingPacketBatch::new(batches))
                .unwrap();

            let elapsed_us = loop {
                if let Ok((_bank, (EntryOrMarker::Entry(entry), _))) = entry_receiver.try_recv() {
                    if !entry.transactions.is_empty() {
                        break t0.elapsed().as_micros() as u64;
                    }
                }
                std::thread::yield_now();
            };
            latency_us.push(elapsed_us);
        }

        // Teardown
        drop(non_vote_sender);
        drop(tpu_vote_sender);
        drop(gossip_vote_sender);
        banking_stage.join().unwrap();
        exit.store(true, Ordering::Relaxed);
        poh_service.join().unwrap();
        drop(poh_recorder);
        for (_bank, (eom, _)) in entry_receiver.iter() {
            let _ = eom.unwrap_entry();
        }

        // ---- report ----
        let cold = latency_us[0];
        let steady = &latency_us[1..];
        let s = summarize(steady);

        println!("\n=== Harness 1: end-to-end (send -> committed entry) ===");
        println!("  iterations : {N_ITERATIONS}  (iter 0 split out as cold start)");
        println!("\n  cold start (iter 0): {} us ({} ms)", cold, cold / 1000);
        println!("\n  steady state (iters 1..{N_ITERATIONS}), microseconds:");
        println!("    min {}  p50 {}  mean {}  p99 {}  max {}", s.min, s.p50, s.mean, s.p99, s.max);
        println!("    jitter (max-min): {} us", s.max - s.min);

        print_votor_header();
        let cold_ok = cold <= DELTA_BLOCK_MS * 1000;
        let steady_ok = s.max <= DELTA_BLOCK_MS * 1000;
        println!(
            "\n  verdict vs Δ_block ({DELTA_BLOCK_MS} ms): cold {}  steady-max {}",
            if cold_ok { "PASS" } else { "OVER" },
            if steady_ok { "PASS" } else { "OVER" }
        );

        assert_eq!(latency_us.len(), N_ITERATIONS);
        assert!(latency_us.iter().all(|&us| us > 0));
    }

    // ====================================================================
    // Harness 2: per-phase timings + jitter taxonomy.
    // Runs synchronously on this thread, so schedstat + jemalloc deltas
    // attribute cleanly to each iteration.
    // ====================================================================
    #[test]
    fn measure_pre_phase_slot_timing() {
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_slow_genesis_config(1_000_000_000);

        let (bank, _forks) = Bank::new_with_bank_forks_for_tests(&genesis_config);

        let (record_sender, mut record_receiver) = record_channels(false);
        let recorder = TransactionRecorder::new(record_sender);
        record_receiver.restart(bank.bank_id());

        let (replay_vote_sender, _) = unbounded();
        let committer = Committer::new(None, replay_vote_sender, None);
        let consumer = Consumer::new(committer, recorder, None);

        // Phase timings (agave's own counters), in microseconds.
        let mut load_exec = Vec::with_capacity(N_ITERATIONS);
        let mut freeze = Vec::with_capacity(N_ITERATIONS);
        let mut record = Vec::with_capacity(N_ITERATIONS);
        let mut commit = Vec::with_capacity(N_ITERATIONS);
        // Jitter-taxonomy probes, per iteration.
        let mut wall_us = Vec::with_capacity(N_ITERATIONS);
        let mut sched_wait_us = Vec::with_capacity(N_ITERATIONS); // CPU-scheduling
        let mut alloc_kib = Vec::with_capacity(N_ITERATIONS); // allocator pressure

        for _ in 0..N_ITERATIONS {
            let start_hash = bank.last_blockhash();
            let recipient = solana_pubkey::new_rand();
            let tx = system_transaction::transfer(&mint_keypair, &recipient, 1, start_hash);
            let txs = sanitize_transactions(vec![tx]);

            // Probe snapshot, run, probe snapshot.
            let sched0 = runqueue_wait_ns();
            let alloc0 = thread_allocated();
            let t0 = Instant::now();
            let output = consumer.process_and_record_transactions(&bank, &txs);
            let wall = t0.elapsed().as_micros() as u64;
            let alloc_delta = thread_allocated().saturating_sub(alloc0);
            let sched_delta = runqueue_wait_ns().saturating_sub(sched0);

            let t = &output
                .execute_and_commit_transactions_output
                .execute_and_commit_timings;

            load_exec.push(t.load_execute_us);
            freeze.push(t.freeze_lock_us);
            record.push(t.record_us);
            commit.push(t.commit_us);
            wall_us.push(wall);
            sched_wait_us.push(sched_delta / 1000); // ns -> us
            alloc_kib.push(alloc_delta / 1024); // bytes -> KiB
        }

        // ---- phase report (steady state) ----
        println!("\n=== Harness 2: per-phase + jitter taxonomy ===");
        println!("  iterations : {N_ITERATIONS}  (iter 0 split out as cold start)");

        let phases = [
            ("load_execute", &load_exec),
            ("freeze_lock", &freeze),
            ("record", &record),
            ("commit", &commit),
            ("wall_total", &wall_us),
        ];

        println!("\n  phase timings (iters 1..{N_ITERATIONS}, microseconds):");
        println!("  {:<14} {:>8} {:>8} {:>8} {:>8} {:>8}", "phase", "min", "mean", "p50", "p99", "max");
        for (name, v) in phases {
            print_row(name, &v[1..]);
        }

        // ---- taxonomy: cold start vs steady ----
        // Attribute the cold-start spike to a bucket by comparing iter 0 to the
        // steady median for each signal.
        println!("\n  jitter taxonomy -- cold start (iter 0) vs steady median:");
        println!("  {:<22} {:>12} {:>12}", "signal", "iter 0", "steady p50");
        let tax = [
            ("CPU-sched wait (us)", &sched_wait_us),
            ("allocator (KiB)", &alloc_kib),
            ("I/O record phase (us)", &record),
            ("wall total (us)", &wall_us),
        ];
        for (name, v) in tax {
            let cold = v[0];
            let p50 = summarize(&v[1..]).p50;
            println!("  {name:<22} {cold:>12} {p50:>12}");
        }

        println!("\n  reading: the bucket whose iter-0 value most exceeds its steady p50");
        println!("  is the dominant cause of the cold-start spike. CPU-sched = the OS");
        println!("  descheduled us; allocator = jemalloc grew the arena; I/O = PoH stall.");

        print_votor_header();
        let cold_wall = wall_us[0];
        let steady_max = summarize(&wall_us[1..]).max;
        println!(
            "\n  verdict vs Δ_block ({DELTA_BLOCK_MS} ms = {} us): cold {}  steady-max {}",
            DELTA_BLOCK_MS * 1000,
            if cold_wall <= DELTA_BLOCK_MS * 1000 { "PASS" } else { "OVER" },
            if steady_max <= DELTA_BLOCK_MS * 1000 { "PASS" } else { "OVER" }
        );

        println!("\n  caveats: single-tx batches (freeze_lock has no contention -> ~0);");
        println!("  isolated Consumer; δ=50ms assumed; allocator probe = volume, not pause time.");
    }
}
