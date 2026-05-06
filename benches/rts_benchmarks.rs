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
// BENCHMARK 1: Scheduling Drift — Component C priority-after-dequeue (Component C)
//
// Measures scheduling drift = time from dequeue to task completion,
// comparing two strategies:
//
//  priority_comp_c  — bots are rejected when the domain's last edit was human.
//                     Simulates the production Component C check.
//  fifo_no_priority — every event is processed unconditionally (baseline).
//
// Expected result: priority_comp_c has lower per-event latency because some
// bot events are short-circuited early; human edits are never blocked.
// ============================================================
fn bench_scheduling_drift(c: &mut Criterion) {
    let mut group = c.benchmark_group("scheduling_drift");
    group.measurement_time(Duration::from_secs(10));

    // Three domains to exercise the HashMap lookup on different keys
    const DOMAINS: [&str; 3] = ["en.wikipedia.org", "de.wikipedia.org", "fr.wikipedia.org"];

    // Component C: priority-after-dequeue with bot-overwrite protection
    group.bench_function("priority_comp_c", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            // Reset per iter_custom call so state doesn't accumulate across warmup
            let mut last_editor_bot: std::collections::HashMap<String, bool> =
                std::collections::HashMap::new();
            let mut lb = BenchLeaderboard::new();

            for i in 0..iters {
                let is_bot = (i % 4) != 0;                        // 75% bots, 25% human
                let domain = DOMAINS[(i as usize) % DOMAINS.len()];

                // --- dequeue time: scheduling drift starts here ---
                let dequeue_time = Instant::now();

                // Component C check: block bot if last edit on this domain was human
                let last_was_human = last_editor_bot
                    .get(domain)
                    .map(|&bot| !bot)
                    .unwrap_or(false);

                if is_bot && last_was_human {
                    // Rejected — task complete (no leaderboard update)
                    total += dequeue_time.elapsed();
                    continue;
                }

                // Allowed — update leaderboard and record last editor
                lb.update(domain);
                last_editor_bot.insert(domain.to_string(), is_bot);
                total += dequeue_time.elapsed();
            }
            total
        })
    });

    // Baseline: FIFO — process every event unconditionally, no priority check
    group.bench_function("fifo_no_priority", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            let mut lb = BenchLeaderboard::new();

            for i in 0..iters {
                let domain = DOMAINS[(i as usize) % DOMAINS.len()];

                // --- dequeue time ---
                let dequeue_time = Instant::now();
                lb.update(domain);
                total += dequeue_time.elapsed();
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
// Measured metric: scheduling drift = dequeue → leaderboard update complete.
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

        // Consumer task: pop + process, collect per-event scheduling drift
        // (dequeue → leaderboard update complete, i.e. the Component C metric)
        let ch_cons = Arc::clone(&channel);
        let lb_cons = Arc::clone(&leaderboard);
        let consumer = tokio::spawn(async move {
            let mut latencies = Vec::with_capacity(N_EVENTS);
            let mut processed = 0usize;
            loop {
                let ev = ch_cons.lock().await.pop();
                if let Some(e) = ev {
                    let dequeue_time = Instant::now();  // scheduling drift starts at dequeue
                    lb_cons.lock().await.update(&e.domain);
                    latencies.push(dequeue_time.elapsed());
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

    // Consumer thread: measures scheduling drift (dequeue → leaderboard update)
    let ch_cons = Arc::clone(&channel);
    let lb_cons = Arc::clone(&leaderboard);
    let consumer = thread::spawn(move || {
        let mut latencies = Vec::with_capacity(N_EVENTS);
        let mut processed = 0usize;
        loop {
            let ev = ch_cons.lock().unwrap().pop();
            if let Some(e) = ev {
                let dequeue_time = Instant::now();  // scheduling drift starts at dequeue
                lb_cons.lock().unwrap().update(&e.domain);
                latencies.push(dequeue_time.elapsed());
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
            eprintln!("\n===== D2: Pipeline Comparison — Scheduling Drift Percentiles =====");
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
