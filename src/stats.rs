//! Percentile summaries for latency vectors.

/// Min / mean / p50 / p99 / max over a slice of samples.
pub struct Summary {
    pub min: u64,
    pub mean: u64,
    pub p50: u64,
    pub p99: u64,
    pub max: u64,
}

pub fn summarize(vals: &[u64]) -> Summary {
    if vals.is_empty() {
        return Summary { min: 0, mean: 0, p50: 0, p99: 0, max: 0 };
    }
    let mut s = vals.to_vec();
    s.sort_unstable();
    let pct = |p: f64| s[(p / 100.0 * (s.len() - 1) as f64).round() as usize];
    Summary {
        min: s[0],
        mean: s.iter().sum::<u64>() / s.len() as u64,
        p50: pct(50.0),
        p99: pct(99.0),
        max: s[s.len() - 1],
    }
}

pub fn print_row(name: &str, vals: &[u64]) {
    let s = summarize(vals);
    println!(
        "  {name:<14} {:>8} {:>8} {:>8} {:>8} {:>8}",
        s.min, s.mean, s.p50, s.p99, s.max
    );
}
