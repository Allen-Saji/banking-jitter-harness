//! Shared genesis + transaction helpers for the harness runs.

use solana_ledger::genesis_utils::{
    GenesisConfigInfo, bootstrap_validator_stake_lamports, create_genesis_config_with_leader,
};
use solana_runtime_transaction::runtime_transaction::RuntimeTransaction;
use solana_transaction::{Transaction, sanitized::SanitizedTransaction};

pub const N_ITERATIONS: usize = 100;

pub fn create_slow_genesis_config(lamports: u64) -> GenesisConfigInfo {
    let validator_pubkey = solana_pubkey::new_rand();
    let mut info = create_genesis_config_with_leader(
        lamports,
        &validator_pubkey,
        bootstrap_validator_stake_lamports(),
    );
    info.genesis_config.ticks_per_slot *= 1024;
    info
}

pub fn sanitize_transactions(
    txs: Vec<Transaction>,
) -> Vec<RuntimeTransaction<SanitizedTransaction>> {
    txs.into_iter()
        .map(RuntimeTransaction::from_transaction_for_tests)
        .collect()
}
