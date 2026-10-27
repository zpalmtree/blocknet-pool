//! Verifier throughput micro-bench (T1 in VERIFY_PERFORMANCE.md).
//!
//! Runs production-size Argon2id hashes through `Argon2PowHasher` (same arena
//! pool and hugepage backing as Stratum) from N threads and prints one JSON
//! line: verifies/s, latency percentiles, arena backing and a digest of every
//! output hash. Inputs are deterministic, so two binaries given the same
//! arguments must print the same `digest`; that is the cross-kernel parity
//! check.
//!
//!     cargo run --release -p pool-common --example verify_bench -- \
//!         --threads 8 --hashes 160 [--warmup 8] [--retain 8] [--seed 1] \
//!         [--kernel avx512]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use pool_common::pow::{Argon2PowHasher, PowHasher};
use sha3::{Digest, Sha3_256};

struct Args {
    threads: usize,
    hashes: usize,
    warmup: usize,
    retain: usize,
    seed: u64,
    kernel: Option<blocknet_pow_kernel::CpuKernel>,
}

fn parse_args() -> Args {
    let mut args = Args {
        threads: 8,
        hashes: 160,
        warmup: 0,
        retain: 0,
        seed: 1,
        kernel: None,
    };
    let mut iter = std::env::args().skip(1);
    while let Some(flag) = iter.next() {
        let value = iter
            .next()
            .unwrap_or_else(|| panic!("{flag} needs a value"));
        if flag == "--kernel" {
            args.kernel = Some(
                value
                    .parse()
                    .unwrap_or_else(|err| panic!("--kernel: {err}")),
            );
            continue;
        }
        let value: u64 = value.parse().unwrap_or_else(|_| panic!("{flag}: {value}"));
        match flag.as_str() {
            "--threads" => args.threads = value as usize,
            "--hashes" => args.hashes = value as usize,
            "--warmup" => args.warmup = value as usize,
            "--retain" => args.retain = value as usize,
            "--seed" => args.seed = value,
            _ => panic!("unknown flag {flag}"),
        }
    }
    if args.warmup == 0 {
        args.warmup = args.threads;
    }
    if args.retain == 0 {
        args.retain = args.threads;
    }
    args
}

/// A header-sized input that differs per job index.
fn job_input(seed: u64, index: usize) -> (Vec<u8>, u64) {
    let mut header = Sha3_256::digest(format!("verify-bench-{seed}-{index}")).to_vec();
    header.extend_from_slice(&Sha3_256::digest(header.as_slice()));
    header.extend_from_slice(&Sha3_256::digest(header.as_slice()));
    (
        header,
        seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ index as u64,
    )
}

fn percentile(sorted: &[f64], pct: usize) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    sorted[((sorted.len() - 1) * pct) / 100]
}

fn main() {
    let args = parse_args();
    let hasher = match args.kernel {
        Some(kernel) => Argon2PowHasher::with_kernel(kernel, args.retain).expect("kernel"),
        None => Argon2PowHasher::with_arena_limit(args.retain),
    };
    let total = args.warmup + args.hashes;
    let next = Arc::new(AtomicUsize::new(0));
    // (index, finished_at, latency_ms, hash)
    let results = Arc::new(Mutex::new(Vec::with_capacity(total)));
    let started_at = Instant::now();
    let workers: Vec<_> = (0..args.threads)
        .map(|_| {
            let hasher = hasher.clone();
            let next = Arc::clone(&next);
            let results = Arc::clone(&results);
            let seed = args.seed;
            std::thread::spawn(move || loop {
                let index = next.fetch_add(1, Ordering::Relaxed);
                if index >= total {
                    break;
                }
                let (header, nonce) = job_input(seed, index);
                let hash_started = Instant::now();
                let hash = hasher.hash(&header, nonce).expect("hash");
                let finished = Instant::now();
                let latency_ms = finished.duration_since(hash_started).as_secs_f64() * 1e3;
                results
                    .lock()
                    .expect("results")
                    .push((index, finished, latency_ms, hash));
            })
        })
        .collect();
    for worker in workers {
        worker.join().expect("worker");
    }

    let mut results = results.lock().expect("results").clone();
    results.sort_by_key(|(index, ..)| *index);
    let mut digest = Sha3_256::new();
    for (_, _, _, hash) in &results {
        digest.update(hash);
    }
    // Steady state: completions after the warmup jobs' last completion.
    let warm_until = results[..args.warmup]
        .iter()
        .map(|(_, finished, ..)| *finished)
        .max()
        .unwrap_or(started_at);
    let measured: Vec<_> = results[args.warmup..]
        .iter()
        .filter(|(_, finished, ..)| *finished > warm_until)
        .collect();
    let last = measured
        .iter()
        .map(|(_, finished, ..)| *finished)
        .max()
        .unwrap_or(warm_until);
    let window_s = last.duration_since(warm_until).as_secs_f64();
    let mut latencies: Vec<f64> = measured.iter().map(|(_, _, ms, _)| *ms).collect();
    latencies.sort_by(f64::total_cmp);
    let mean = latencies.iter().sum::<f64>() / latencies.len().max(1) as f64;
    let stats = hasher.arena_pool_stats();
    println!(
        "{}",
        serde_json::json!({
            "kernel": hasher.kernel_label(),
            "threads": args.threads,
            "retain": args.retain,
            "warmup": args.warmup,
            "measured": latencies.len(),
            "window_s": window_s,
            "verifies_per_s": latencies.len() as f64 / window_s.max(f64::EPSILON),
            "latency_ms": {
                "mean": mean,
                "p50": percentile(&latencies, 50),
                "p95": percentile(&latencies, 95),
                "p99": percentile(&latencies, 99),
                "max": latencies.last().copied().unwrap_or(0.0),
            },
            "arena": {
                "allocations": stats.allocations,
                "cache_hits": stats.cache_hits,
                "hugetlb": stats.hugetlb_allocations,
                "thp_hint": stats.thp_hint_allocations,
                "heap": stats.heap_allocations,
            },
            "seed": args.seed,
            "jobs": total,
            "digest": hex::encode(digest.finalize()),
        })
    );
}
