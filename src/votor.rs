//! Alpenglow Votor timing budget -- the deadline banking is graded against.
//!
//! Source: Alpenglow White Paper v1.1 -- Figure 7 (p22, Votor per-round
//! lifecycle), with the numeric bounds in the abstract, Section 1.5, Table 6,
//! and Definition 17. (NOT Figure 2, the double-Merkle block-data hierarchy --
//! a common mis-citation from an older draft.)

pub const DELTA_BLOCK_MS: u64 = 400; // Δ_block -- block-production budget.
pub const DELTA_MS: u64 = 50; // δ -- one all-to-all message delay (assumed).

pub fn print_votor_budget() {
    let finalize = DELTA_MS.min(2 * DELTA_MS);
    println!("\nVotor budget (Alpenglow v1.1, Fig 7 + Table 6 + Def 17):");
    println!("  block production : Δ_block            = {DELTA_BLOCK_MS} ms   <-- banking judged here");
    println!("  finalization     : min(δ_80, 2·δ_60)  = {finalize} ms    (consensus layer)");
    println!("  liveness ceiling : 3·δ + Δ_block       = {} ms   (give-up line, not a target)", 3 * DELTA_MS + DELTA_BLOCK_MS);
    // delta sweep: the 400ms block budget is fixed, but the finalization and
    // liveness numbers move with the network-delay assumption δ.
    println!("\n  δ sensitivity (finalization = min(δ,2δ)=δ ; liveness = 3δ+400):");
    println!("  {:>8} {:>16} {:>14}", "δ (ms)", "finalize (ms)", "liveness (ms)");
    for d in [25u64, 50, 100] {
        println!("  {:>8} {:>16} {:>14}", d, d, 3 * d + DELTA_BLOCK_MS);
    }
}
