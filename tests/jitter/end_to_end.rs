//! Harness 1: end-to-end latency + per-worker-thread attribution.
//!
//! Drives a real `BankingStage` and times send -> committed entry, attributing
//! the cold-start spike to specific worker threads via `/proc` instead of
//! guessing -- the answer to the `tokio-console` blind spot, which cannot see
//! the crossbeam OS worker threads.

use {
    agave_banking_stage_ingress_types::BankingPacketBatch,
    banking_jitter::{
        proc_probe::{
            proc_faults, sample_wchan_during, thread_names, tid_ctxt, tid_schedstat,
        },
        stats::summarize,
        votor::{DELTA_BLOCK_MS, print_votor_budget},
    },
    crossbeam_channel::unbounded,
    solana_core::{
        banking_stage::{
            BankingStage, transaction_scheduler::scheduler_controller::SchedulerConfig,
        },
        banking_trace::{BankingTracer, Channels},
        validator::{BlockProductionMethod, SchedulerPacing},
    },
    solana_entry::entry_or_marker::EntryOrMarker,
    solana_ledger::{
        blockstore::Blockstore, genesis_utils::GenesisConfigInfo,
        get_tmp_ledger_path_auto_delete,
    },
    solana_perf::packet::to_packet_batches,
    solana_poh::poh_recorder::create_test_recorder,
    solana_runtime::bank::Bank,
    solana_system_transaction as system_transaction,
    std::{
        collections::BTreeMap,
        num::NonZeroUsize,
        sync::{Arc, atomic::Ordering},
        time::Instant,
    },
    tokio::sync::mpsc,
};

use crate::common::{N_ITERATIONS, create_slow_genesis_config};

struct ThreadRow {
    name: String,
    cpu_us: u64,
    wait_us: u64,
    vol: u64,
    nonvol: u64,
    // dominant sleep location during the window: (wchan, samples_there, total_samples)
    parked: Option<(String, u32, u32)>,
}

fn dominant_wchan(
    samples: &BTreeMap<u64, BTreeMap<String, u32>>,
    tid: u64,
) -> Option<(String, u32, u32)> {
    let m = samples.get(&tid)?;
    let total: u32 = m.values().sum();
    let (w, c) = m.iter().max_by_key(|(_, c)| **c)?;
    Some((w.clone(), *c, total))
}

#[test]
fn measure_banking_stage_slot_timing() {
    // With --features tokio-console (and RUSTFLAGS="--cfg tokio_unstable"),
    // start the console subscriber for the whole process. tokio-console will
    // then surface the banking-stage manager's current-thread runtime tasks
    // and its spawn_blocking shims -- but none of the crossbeam OS worker
    // threads, which is the blind spot this harness works around with /proc.
    #[cfg(feature = "tokio-console")]
    console_subscriber::init();
    agave_logger::setup();

    let GenesisConfigInfo { genesis_config, mint_keypair, .. } =
        create_slow_genesis_config(1_000_000_000);
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
    let (exit, poh_recorder, _poh_controller, transaction_recorder, poh_service, entry_receiver) =
        create_test_recorder(bank, blockstore, None, None);
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
        SchedulerConfig { scheduler_pacing: SchedulerPacing::Disabled },
        None,
        replay_vote_sender,
        None,
        bank_forks,
        None,
        Arc::default(),
    );

    // One transfer per iteration; time send -> committed entry. On the cold
    // iteration (0) and a steady iteration we snapshot every Agave thread's
    // schedstat AND per-thread context switches, plus process page faults, so
    // the spike is attributed to threads instead of guessed. On the cold
    // iteration we additionally run an off-CPU wchan sampler to locate where
    // each worker is parked (the residual wakeup wait).
    let mut latency_us: Vec<u64> = Vec::with_capacity(N_ITERATIONS);
    let steady_probe_iter = N_ITERATIONS / 2;
    let mut cold_attr = None;
    let mut steady_attr = None;

    // Agave worker tids, known now that BankingStage::new has spawned them.
    let agave_tids: Vec<u64> = thread_names()
        .iter()
        .filter(|(_, n)| n.starts_with("sol") || *n == "BankingMgr")
        .map(|(t, _)| *t)
        .collect();

    for i in 0..N_ITERATIONS {
        let recipient = solana_pubkey::new_rand();
        let tx = system_transaction::transfer(&mint_keypair, &recipient, 1, start_hash);
        let batches = to_packet_batches(&[tx], 1);

        let probe = i == 0 || i == steady_probe_iter;
        // before-snapshot: tid -> (cpu_ns, wait_ns, vol_cs, nonvol_cs)
        let before: Option<BTreeMap<u64, (u64, u64, u64, u64)>> = probe.then(|| {
            agave_tids
                .iter()
                .map(|&t| {
                    let (c, w) = tid_schedstat(t);
                    let (v, nv) = tid_ctxt(t);
                    (t, (c, w, v, nv))
                })
                .collect()
        });
        let before_faults = probe.then(proc_faults);

        let send_and_wait = || {
            let t0 = Instant::now();
            non_vote_sender.send(BankingPacketBatch::new(batches)).unwrap();
            loop {
                if let Ok((_b, (EntryOrMarker::Entry(entry), _))) = entry_receiver.try_recv() {
                    if !entry.transactions.is_empty() {
                        break t0.elapsed().as_micros() as u64;
                    }
                }
                std::thread::yield_now();
            }
        };

        // Cold iteration: sample wchan during the wait. Steady iterations are
        // ~1ms, too short to sample, so run the wait directly.
        let (elapsed_us, wchan) = if i == 0 {
            let (e, w) = sample_wchan_during(&agave_tids, send_and_wait);
            (e, Some(w))
        } else {
            (send_and_wait(), None)
        };
        latency_us.push(elapsed_us);

        if probe {
            let names = thread_names();
            let before = before.unwrap();
            let mut rows: Vec<ThreadRow> = agave_tids
                .iter()
                .filter_map(|tid| {
                    let name = names.get(tid)?.clone();
                    let (c1, w1) = tid_schedstat(*tid);
                    let (v1, nv1) = tid_ctxt(*tid);
                    let (c0, w0, v0, nv0) = before.get(tid).copied().unwrap_or((c1, w1, v1, nv1));
                    let cpu_us = c1.saturating_sub(c0) / 1000;
                    let wait_us = w1.saturating_sub(w0) / 1000;
                    let vol = v1.saturating_sub(v0);
                    let nonvol = nv1.saturating_sub(nv0);
                    let parked = wchan.as_ref().and_then(|m| dominant_wchan(m, *tid));
                    (cpu_us + wait_us + vol + nonvol > 0 || parked.is_some()).then_some(ThreadRow {
                        name,
                        cpu_us,
                        wait_us,
                        vol,
                        nonvol,
                        parked,
                    })
                })
                .collect();
            rows.sort_by_key(|r| std::cmp::Reverse(r.cpu_us));
            let (mnf, mjf) = proc_faults();
            let (bmnf, bmjf) = before_faults.unwrap();
            let attr = (elapsed_us, rows, (mnf - bmnf, mjf - bmjf));
            if i == 0 { cold_attr = Some(attr) } else { steady_attr = Some(attr) }
        }
    }

    // Keep the banking stage (and its manager runtime) alive long enough for
    // an operator to attach tokio-console and inspect it live.
    #[cfg(feature = "tokio-console")]
    {
        println!("\n  [tokio-console] manager runtime is live; attach `tokio-console` now.");
        println!("  Observe: only the BankingMgr runtime + spawn_blocking shims appear;");
        println!("  the solBnkTxSched / solCoWorker* OS threads do not. Sleeping 60s.");
        std::thread::sleep(std::time::Duration::from_secs(60));
    }

    drop(non_vote_sender);
    drop(tpu_vote_sender);
    drop(gossip_vote_sender);
    banking_stage.join().unwrap();
    exit.store(true, Ordering::Relaxed);
    poh_service.join().unwrap();
    drop(poh_recorder);
    for (_b, (eom, _)) in entry_receiver.iter() {
        let _ = eom.unwrap_entry();
    }

    // ---- report ----
    let cold = latency_us[0];
    let s = summarize(&latency_us[1..]);
    println!("\n=== Harness 1: end-to-end (send -> committed entry) ===");
    println!("  iterations : {N_ITERATIONS}");
    println!("  cold start (iter 0): {cold} us ({} ms)", cold / 1000);
    println!("  steady state (us): min {} p50 {} mean {} p99 {} max {}  jitter {}",
        s.min, s.p50, s.mean, s.p99, s.max, s.max - s.min);

    let print_attr = |label: &str, attr: &(u64, Vec<ThreadRow>, (u64, u64))| {
        let (wall_us, rows, (minf, majf)) = attr;
        println!("\n  [{label}] window {wall_us} us   (process page faults: minor {minf}, major {majf})");
        println!(
            "    {:<16} {:>7} {:>7} {:>7} {:>8}  {}",
            "thread", "cpu_us", "vol_cs", "invol_cs", "wait_us", "parked_in (share of samples)"
        );
        // Active threads (did CPU work or context-switched) first; the rest
        // are summarized so idle background threads do not crowd the table.
        let (active, idle): (Vec<&ThreadRow>, Vec<&ThreadRow>) =
            rows.iter().partition(|r| r.cpu_us > 0 || r.vol > 0 || r.nonvol > 0);
        for r in active.iter().take(8) {
            let parked = match &r.parked {
                Some((w, c, total)) if *total > 0 => format!("{w} ({}%)", c * 100 / total),
                _ => "-".to_string(),
            };
            println!(
                "    {:<16} {:>7} {:>7} {:>7} {:>8}  {}",
                r.name, r.cpu_us, r.vol, r.nonvol, r.wait_us, parked
            );
        }
        if !idle.is_empty() {
            let futex = idle
                .iter()
                .filter(|r| matches!(&r.parked, Some((w, ..)) if w.contains("futex")))
                .count();
            println!(
                "    + {} more threads idle (cpu ~0), {futex} of them parked in futex_wait_queue",
                idle.len()
            );
        }
    };
    println!("\n  --- worker-thread attribution (the tokio-console blind spot, seen) ---");
    if let Some(a) = &cold_attr { print_attr("cold iter 0", a) }
    if let Some(a) = &steady_attr { print_attr("steady iter", a) }
    println!("\n  reading: cpu_us = work done on this thread; vol_cs = times it parked");
    println!("  and was woken (a futex/channel wait); parked_in = where it slept during");
    println!("  the window. A cold window with near-zero worker cpu_us, low runqueue");
    println!("  wait, zero major faults, and workers parked in a futex means the spike is");
    println!("  wakeup/first-message latency, not computation, allocation, or scheduling.");

    print_votor_budget();
    println!("\n  verdict vs Δ_block ({DELTA_BLOCK_MS} ms): cold {}  steady-max {}",
        if cold <= DELTA_BLOCK_MS * 1000 { "PASS" } else { "OVER" },
        if s.max <= DELTA_BLOCK_MS * 1000 { "PASS" } else { "OVER" });

    assert_eq!(latency_us.len(), N_ITERATIONS);
    assert!(latency_us.iter().all(|&us| us > 0));
    assert!(cold_attr.is_some() && steady_attr.is_some());
}
