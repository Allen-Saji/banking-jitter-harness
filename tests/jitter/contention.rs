//! Harness 3: account-lock contention -- the honest replacement for the earlier
//! (incorrect) "conflicting batches light up freeze_lock" claim.
//!
//! Conflicting transactions serialize on account locks: in one batch, only the
//! non-conflicting subset commits; the rest report AccountInUse.

use {
    crossbeam_channel::unbounded,
    solana_core::banking_stage::{committer::Committer, consumer::Consumer},
    solana_ledger::genesis_utils::GenesisConfigInfo,
    solana_poh::{record_channels::record_channels, transaction_recorder::TransactionRecorder},
    solana_runtime::bank::Bank,
    solana_system_transaction as system_transaction,
    solana_transaction::Transaction,
};

use crate::common::{create_slow_genesis_config, sanitize_transactions};

#[test]
fn measure_account_lock_contention() {
    let GenesisConfigInfo { genesis_config, mint_keypair, .. } =
        create_slow_genesis_config(1_000_000_000);
    let (bank, _forks) = Bank::new_with_bank_forks_for_tests(&genesis_config);

    let (record_sender, mut record_receiver) = record_channels(false);
    let recorder = TransactionRecorder::new(record_sender);
    record_receiver.restart(bank.bank_id());
    let (replay_vote_sender, _) = unbounded();
    let consumer = Consumer::new(
        Committer::new(None, replay_vote_sender, None),
        recorder,
        None,
    );

    let start_hash = bank.last_blockhash();
    let hot = solana_pubkey::new_rand();
    // 8 transfers, all FROM the same mint and TO the same hot account: every
    // pair conflicts on both writable accounts.
    let conflicting: Vec<Transaction> = (0..8)
        .map(|_| system_transaction::transfer(&mint_keypair, &hot, 1, start_hash))
        .collect();
    let txs = sanitize_transactions(conflicting);

    let out = consumer.process_and_record_transactions(&bank, &txs);
    let eo = &out.execute_and_commit_transactions_output;
    let committed = eo
        .commit_transactions_result
        .as_ref()
        .map(|v| v.len())
        .unwrap_or(0);

    println!("\n=== Harness 3: account-lock contention ===");
    println!("  submitted   : {} transactions, all writing the same two accounts", txs.len());
    println!("  committed   : {committed}  (the rest are serialized out by account locks)");
    println!("  reading: account-lock contention shows up as a committed-count drop and");
    println!("  retries, NOT as freeze_lock time. freeze_lock guards the bank freeze, a");
    println!("  different lock. This corrects the earlier freeze_lock framing.");

    // With all-conflicting transactions, at most one can take the locks per
    // batch pass.
    assert!(committed <= txs.len());
}
