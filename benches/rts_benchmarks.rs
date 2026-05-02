// RTS2601 Criterion Benchmarks
//
// Three benchmark groups:
//   1. scheduling_drift    — priority scheduling vs FIFO (Component C)
//   2. sync_contention     — Mutex vs RwLock vs Atomic at 1/2/4/8/16 threads (Component D)
//   3. pipeline_comparison — Async vs Threaded pipeline processing p50/p90/p99 (D2)

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, RwLock};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

// ============================================================
// Shared helpers
// ============================================================

fn percentile(sorted: &[Duration], pct: f64) -> Duration {
    if sorted.is_empty() { return Duration::ZERO; }
    let idx = ((pct / 100.0) * sorted.len() as f64) as usize;
    sorted[idx.min(sorted.len() - 1)]
}

// Minimal in-bench event — no dependency on main crate types
#[derive(Clone)]
struct BenchEvent {
    is_bot:      bool,
    domain:      String,
    enqueued_at: Instant,
}

fn make_events(n: usize) -> Vec<BenchEvent> {
    (0..n)
        .map(|i| BenchEvent {
            is_bot:      i % 4 != 0,           // 75% bots, 25% humans (realistic ratio)
            domain:      "en.wikipedia.org".to_string(),
            enqueued_at: Instant::now(),
        })
        .collect()
}

// Inline priority channel used only in benchmarks — mirrors production logic
struct BenchChannel {
    buf:      VecDeque<BenchEvent>,
    capacity: usize,
}

impl BenchChannel {
    fn new(cap: usize) -> Self {
        Self { buf: VecDeque::with_capacity(cap), capacity: cap }
    }

    fn push(&mut self, mut ev: BenchEvent) {
        ev.enqueued_at = Instant::now(); // stamp at channel entry
        if self.buf.len() < self.capacity {
            self.buf.push_back(ev);
            return;
        }
        if ev.is_bot {
            return; // drop bot
        }
        let bot_pos = self.buf.iter().position(|e| e.is_bot);
        match bot_pos {
            Some(p) => { self.buf.remove(p); self.buf.push_back(ev); }
            None    => { self.buf.pop_front(); self.buf.push_back(ev); }
        }
    }

    fn pop(&mut self) -> Option<BenchEvent> {
        self.buf.pop_front()
    }
}

// Minimal leaderboard for benchmark processing
struct BenchLeaderboard {
    counts: std::collections::HashMap<String, u64>,
}

impl BenchLeaderboard {
    fn new() -> Self { Self { counts: std::collections::HashMap::new() } }
    fn update(&mut self, domain: &str) {
        *self.counts.entry(domain.to_string()).or_insert(0) += 1;
    }
}

// ============================================================
// BENCHMARK 1: Scheduling Drift — Priority vs FIFO (Component C)
// ============================================================
fn bench_scheduling_drift(c: &mut Criterion) {
    let mut group = c.benchmark_group("scheduling_drift");

    group.bench_function("priority_human_first", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let mut human_q: VecDeque<Instant> = VecDeque::new();
                let mut bot_q: VecDeque<Instant>   = VecDeque::new();
                for _ in 0..80 { bot_q.push_back(Instant::now()); }
                for _ in 0..20 { human_q.push_back(Instant::now()); }
                while let Some(t) = human_q.pop_front() { total += t.elapsed(); }
                while let Some(t) = bot_q.pop_front()   { total += t.elapsed(); }
            }
            total
        })
    });

    group.bench_function("fifo_no_priority", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let mut queue: VecDeque<Instant> = VecDeque::new();
                for _ in 0..100 { queue.push_back(Instant::now()); }
                while let Some(t) = queue.pop_front() { total += t.elapsed(); }
            }
            total
        })
    });

    group.finish();
}

// ============================================================
// BENCHMARK 2: Sync Contention — Mutex vs RwLock vs Atomic (Component D)
// ============================================================
fn bench_sync_contention(c: &mut Criterion) {
    let mut group = c.benchmark_group("sync_contention");

    for &n_threads in &[1usize, 2, 4, 8, 16] {
        group.bench_with_input(BenchmarkId::new("Mutex", n_threads), &n_threads, |b, &n| {
            b.iter(|| {
                let lb = Arc::new(Mutex::new(BenchLeaderboard::new()));
                let hs: Vec<_> = (0..n).map(|_| {
                    let lb = Arc::clone(&lb);
                    thread::spawn(move || {
                        for _ in 0..1000 { lb.lock().unwrap().update("en.wikipedia.org"); }
                    })
                }).collect();
                for h in hs { h.join().unwrap(); }
            })
        });

        group.bench_with_input(BenchmarkId::new("RwLock", n_threads), &n_threads, |b, &n| {
            b.iter(|| {
                let lb = Arc::new(RwLock::new(BenchLeaderboard::new()));
                let hs: Vec<_> = (0..n).map(|_| {
                    let lb = Arc::clone(&lb);
                    thread::spawn(move || {
                        for _ in 0..1000 { lb.write().unwrap().update("en.wikipedia.org"); }
                    })
                }).collect();
                for h in hs { h.join().unwrap(); }
            })
        });

        group.bench_with_input(BenchmarkId::new("Atomic", n_threads), &n_threads, |b, &n| {
            b.iter(|| {
                let ctr = Arc::new(AtomicU64::new(0));
                let hs: Vec<_> = (0..n).map(|_| {
                    let ctr = Arc::clone(&ctr);
                    thread::spawn(move || {
                        for _ in 0..1000 { ctr.fetch_add(1, Ordering::Relaxed); }
                    })
                }).collect();
                for h in hs { h.join().unwrap(); }
            })
        });
    }

    group.finish();
}

// ============================================================
// BENCHMARK 3 (D2): Pipeline Comparison — Async vs Threaded
//
// Simulates end-to-end event processing for both pipeline models.
//   Async model:    tokio tasks push events; tokio task pops + processes.
//   Threaded model: std::threads push events; std::thread pops + processes.
//
// Measured metric: per-event latency from enqueued_at → leaderboard update.
// Criterion reports min/mean/max with confidence intervals.
// p50/p90/p99 printed to stderr so they appear in bench output.
//
// Both models share identical PriorityChannel + leaderboard logic.
// The ONLY variable is the concurrency primitive (Tokio task vs OS thread).
// ============================================================

const N_EVENTS: usize = 500;

// --- Async pipeline simulation ---
fn run_async_simulation() -> Vec<Duration> {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let channel     = Arc::new(tokio::sync::Mutex::new(BenchChannel::new(200)));
        let leaderboard = Arc::new(tokio::sync::Mutex::new(BenchLeaderboard::new()));
        let events      = make_events(N_EVENTS);

        // Producer task: push all events into channel
        let ch_prod = Arc::clone(&channel);
        let producer = tokio::spawn(async move {
            for ev in events {
                ch_prod.lock().await.push(ev);
                tokio::task::yield_now().await;
            }
        });

        // Consumer task: pop + process, collect per-event latency
        let ch_cons = Arc::clone(&channel);
        let lb_cons = Arc::clone(&leaderboard);
        let consumer = tokio::spawn(async move {
            let mut latencies = Vec::with_capacity(N_EVENTS);
            let mut processed = 0usize;
            loop {
                let ev = ch_cons.lock().await.pop();
                if let Some(e) = ev {
                    let t0 = e.enqueued_at;
                    lb_cons.lock().await.update(&e.domain);
                    latencies.push(t0.elapsed());
                    processed += 1;
                    if processed >= N_EVENTS { break; }
                } else {
                    tokio::task::yield_now().await;
                }
            }
            latencies
        });

        producer.await.unwrap();
        let mut lats = consumer.await.unwrap();
        lats.sort();
        lats
    })
}

// --- Threaded pipeline simulation ---
fn run_threaded_simulation() -> Vec<Duration> {
    let channel     = Arc::new(Mutex::new(BenchChannel::new(200)));
    let leaderboard = Arc::new(Mutex::new(BenchLeaderboard::new()));
    let events      = make_events(N_EVENTS);

    // Producer thread
    let ch_prod = Arc::clone(&channel);
    let producer = thread::spawn(move || {
        for ev in events {
            ch_prod.lock().unwrap().push(ev);
            thread::yield_now();
        }
    });

    // Consumer thread
    let ch_cons = Arc::clone(&channel);
    let lb_cons = Arc::clone(&leaderboard);
    let consumer = thread::spawn(move || {
        let mut latencies = Vec::with_capacity(N_EVENTS);
        let mut processed = 0usize;
        loop {
            let ev = ch_cons.lock().unwrap().pop();
            if let Some(e) = ev {
                let t0 = e.enqueued_at;
                lb_cons.lock().unwrap().update(&e.domain);
                latencies.push(t0.elapsed());
                processed += 1;
                if processed >= N_EVENTS { break; }
            } else {
                thread::yield_now();
            }
        }
        latencies
    });

    producer.join().unwrap();
    let mut lats = consumer.join().unwrap();
    lats.sort();
    lats
}

fn bench_pipeline_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline_comparison");

    // Pre-compute percentiles once and print — these are the D2 proof numbers
    {
        let async_lats    = run_async_simulation();
        let threaded_lats = run_threaded_simulation();

        if !async_lats.is_empty() && !threaded_lats.is_empty() {
            eprintln!("\n========== D2: Pipeline Comparison — Latency Percentiles ==========");
            eprintln!(
                "  {:12}  p50={:>8.2}µs  p90={:>8.2}µs  p99={:>8.2}µs",
                "ASYNC",
                percentile(&async_lats, 50.0).as_nanos() as f64 / 1000.0,
                percentile(&async_lats, 90.0).as_nanos() as f64 / 1000.0,
                percentile(&async_lats, 99.0).as_nanos() as f64 / 1000.0,
            );
            eprintln!(
                "  {:12}  p50={:>8.2}µs  p90={:>8.2}µs  p99={:>8.2}µs",
                "THREADED",
                percentile(&threaded_lats, 50.0).as_nanos() as f64 / 1000.0,
                percentile(&threaded_lats, 90.0).as_nanos() as f64 / 1000.0,
                percentile(&threaded_lats, 99.0).as_nanos() as f64 / 1000.0,
            );
            eprintln!("=====================================================================\n");
        }
    }

    group.bench_function("async_pipeline", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let lats = run_async_simulation();
                total += lats.iter().sum::<Duration>();
            }
            total
        })
    });

    group.bench_function("threaded_pipeline", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let lats = run_threaded_simulation();
                total += lats.iter().sum::<Duration>();
            }
            total
        })
    });

    group.finish();
}

criterion_group!(benches, bench_scheduling_drift, bench_sync_contention, bench_pipeline_comparison);
criterion_main!(benches);
