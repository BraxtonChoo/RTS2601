// RTS2601 Criterion Benchmarks
//
// Benchmark groups:
//   pipeline_comparison — Async vs Threaded scheduling drift p50/p90/p99
//   sync_contention     — Mutex vs RwLock vs Atomic at 1/2/4/8/16 writer threads
//
// NOTE — sync_contention is a controlled scalability experiment, not drawn from
// the live simulation.  The simulation itself uses a single processor thread so
// its leaderboard access is inherently serial; the dashboard SYNC BENCHMARK panel
// shows those real per-event timings.  This benchmark answers a separate question:
// "how does each primitive degrade as concurrent writers increase?" — which is the
// academic comparison required by Component D.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::{Duration, Instant};

// ============================================================
// Shared helpers
// ============================================================

fn percentile(sorted: &[Duration], pct: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let idx = ((pct / 100.0) * sorted.len() as f64) as usize;
    sorted[idx.min(sorted.len() - 1)]
}

// Minimal in-bench event — no dependency on main crate types
#[derive(Clone)]
struct BenchEvent {
    is_bot: bool,
    domain: String,
    enqueued_at: Instant,
}

fn make_events(n: usize) -> Vec<BenchEvent> {
    (0..n)
        .map(|i| BenchEvent {
            is_bot: i % 4 != 0, // 75% bots, 25% humans (realistic ratio)
            domain: "en.wikipedia.org".to_string(),
            enqueued_at: Instant::now(),
        })
        .collect()
}

// Inline priority channel — mirrors production logic, no crate dependency
struct BenchChannel {
    buf: VecDeque<BenchEvent>,
    capacity: usize,
}

impl BenchChannel {
    fn new(cap: usize) -> Self {
        Self {
            buf: VecDeque::with_capacity(cap),
            capacity: cap,
        }
    }

    fn push(&mut self, mut ev: BenchEvent) {
        ev.enqueued_at = Instant::now();
        if self.buf.len() < self.capacity {
            self.buf.push_back(ev);
            return;
        }
        if ev.is_bot {
            return;
        }
        let bot_pos = self.buf.iter().position(|e| e.is_bot);
        match bot_pos {
            Some(p) => {
                self.buf.remove(p);
                self.buf.push_back(ev);
            }
            None => {
                self.buf.pop_front();
                self.buf.push_back(ev);
            }
        }
    }

    fn pop(&mut self) -> Option<BenchEvent> {
        self.buf.pop_front()
    }
}

struct BenchLeaderboard {
    counts: std::collections::HashMap<String, u64>,
}

impl BenchLeaderboard {
    fn new() -> Self {
        Self {
            counts: std::collections::HashMap::new(),
        }
    }
    fn update(&mut self, domain: &str) {
        *self.counts.entry(domain.to_string()).or_insert(0) += 1;
    }
}

// ============================================================
// BENCHMARK: Pipeline Comparison — Async vs Threaded (D2)
//
// Simulates end-to-end event processing for both pipeline models.
//   Async model:    tokio tasks push/pop via tokio::sync::Mutex.
//   Threaded model: std::threads push/pop via std::sync::Mutex.
//
// Measured metric: scheduling drift = dequeue → leaderboard update complete.
// p50/p90/p99 printed to stderr for direct comparison in bench output.
// ============================================================

const N_EVENTS: usize = 500;

fn run_async_simulation() -> Vec<Duration> {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let channel = Arc::new(tokio::sync::Mutex::new(BenchChannel::new(200)));
        let leaderboard = Arc::new(tokio::sync::Mutex::new(BenchLeaderboard::new()));
        let events = make_events(N_EVENTS);

        let ch_prod = Arc::clone(&channel);
        let producer = tokio::spawn(async move {
            for ev in events {
                ch_prod.lock().await.push(ev);
                tokio::task::yield_now().await;
            }
        });

        let ch_cons = Arc::clone(&channel);
        let lb_cons = Arc::clone(&leaderboard);
        let consumer = tokio::spawn(async move {
            let mut latencies = Vec::with_capacity(N_EVENTS);
            let mut processed = 0usize;
            loop {
                let ev = ch_cons.lock().await.pop();
                if let Some(e) = ev {
                    let t = Instant::now();
                    lb_cons.lock().await.update(&e.domain);
                    latencies.push(t.elapsed());
                    processed += 1;
                    if processed >= N_EVENTS {
                        break;
                    }
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

fn run_threaded_simulation() -> Vec<Duration> {
    use std::sync::atomic::{AtomicBool, Ordering};

    let channel = Arc::new(Mutex::new(BenchChannel::new(200)));
    let leaderboard = Arc::new(Mutex::new(BenchLeaderboard::new()));
    let events = make_events(N_EVENTS);
    let done = Arc::new(AtomicBool::new(false));

    let ch_prod = Arc::clone(&channel);
    let done_prod = Arc::clone(&done);
    let producer = thread::spawn(move || {
        for ev in events {
            ch_prod.lock().unwrap().push(ev);
            thread::yield_now();
        }
        done_prod.store(true, Ordering::SeqCst);
    });

    let ch_cons = Arc::clone(&channel);
    let lb_cons = Arc::clone(&leaderboard);
    let done_cons = Arc::clone(&done);
    let consumer = thread::spawn(move || {
        let mut latencies = Vec::with_capacity(N_EVENTS);
        loop {
            let ev = ch_cons.lock().unwrap().pop();
            if let Some(e) = ev {
                let t = Instant::now();
                lb_cons.lock().unwrap().update(&e.domain);
                latencies.push(t.elapsed());
            } else if done_cons.load(Ordering::SeqCst) {
                // Producer is done and channel is empty — we're finished
                break;
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
    group.sample_size(100); // reduce from 100 to 50 samples
    group.measurement_time(Duration::from_secs(5)); // reduce from 5s to 2s

    // Pre-compute percentiles once — these are the D2 proof numbers
    {
        let async_lats = run_async_simulation();
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
                total += run_async_simulation().iter().sum::<Duration>();
            }
            total
        })
    });

    group.bench_function("threaded_pipeline", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                total += run_threaded_simulation().iter().sum::<Duration>();
            }
            total
        })
    });

    group.finish();
}

// ============================================================
// BENCHMARK: Sync Contention — Mutex vs RwLock vs Atomic (Component D)
//
// Each sub-benchmark spawns N writer threads, all updating the same shared
// counter/leaderboard concurrently for 1 000 ops each.
//
// Thread counts: 1 / 2 / 4 / 8 / 16
//   1  thread  → baseline (no contention)
//   16 threads → high contention — gap between primitives widens most here
//
// Expected ranking: Atomic << RwLock < Mutex (gap grows with thread count)
//
// NOTE: This is a controlled microbenchmark.  The live simulation uses a single
// processor thread (serial access), so the dashboard SYNC BENCHMARK panel shows
// the real per-event cost.  This benchmark isolates lock-contention scaling.
// ============================================================

const OPS_PER_THREAD: usize = 1_000;

// Stub leaderboard identical to the production one — avoids a crate dep
struct ContendedLeaderboard {
    counts: std::collections::HashMap<String, u64>,
}

impl ContendedLeaderboard {
    fn new() -> Self {
        Self {
            counts: std::collections::HashMap::new(),
        }
    }
    fn update(&mut self, domain: &str) {
        *self.counts.entry(domain.to_string()).or_insert(0) += 1;
    }
}

fn bench_sync_contention(c: &mut Criterion) {
    let mut group = c.benchmark_group("sync_contention");

    // Print a summary table to stderr once before the timed iterations begin
    // so the numbers appear in `cargo bench` terminal output alongside the
    // Criterion confidence intervals.
    {
        eprintln!("\n===== Component D: Sync Contention — ns/op at increasing thread counts =====");
        eprintln!(
            "  {:>7}  {:>12}  {:>12}  {:>12}",
            "Threads", "Mutex", "RwLock", "Atomic"
        );
        for &n in &[1usize, 2, 4, 8, 16] {
            // Mutex
            let lb = Arc::new(Mutex::new(ContendedLeaderboard::new()));
            let t0 = Instant::now();
            let hs: Vec<_> = (0..n)
                .map(|_| {
                    let lb = Arc::clone(&lb);
                    thread::spawn(move || {
                        for _ in 0..OPS_PER_THREAD {
                            lb.lock().unwrap().update("en.wikipedia.org");
                        }
                    })
                })
                .collect();
            for h in hs {
                h.join().unwrap();
            }
            let mutex_ns = t0.elapsed().as_nanos() as f64 / (n * OPS_PER_THREAD) as f64;

            // RwLock
            let lb = Arc::new(RwLock::new(ContendedLeaderboard::new()));
            let t0 = Instant::now();
            let hs: Vec<_> = (0..n)
                .map(|_| {
                    let lb = Arc::clone(&lb);
                    thread::spawn(move || {
                        for _ in 0..OPS_PER_THREAD {
                            lb.write().unwrap().update("en.wikipedia.org");
                        }
                    })
                })
                .collect();
            for h in hs {
                h.join().unwrap();
            }
            let rwlock_ns = t0.elapsed().as_nanos() as f64 / (n * OPS_PER_THREAD) as f64;

            // Atomic
            let ctr = Arc::new(AtomicU64::new(0));
            let t0 = Instant::now();
            let hs: Vec<_> = (0..n)
                .map(|_| {
                    let ctr = Arc::clone(&ctr);
                    thread::spawn(move || {
                        for _ in 0..OPS_PER_THREAD {
                            ctr.fetch_add(1, Ordering::Relaxed);
                        }
                    })
                })
                .collect();
            for h in hs {
                h.join().unwrap();
            }
            let atomic_ns = t0.elapsed().as_nanos() as f64 / (n * OPS_PER_THREAD) as f64;

            eprintln!(
                "  {:>7}  {:>9.1} ns  {:>9.1} ns  {:>9.1} ns",
                n, mutex_ns, rwlock_ns, atomic_ns
            );
        }
        eprintln!("============================================================================\n");
    }

    // Criterion timed iterations — one group per primitive, parameterised by thread count
    for &n in &[1usize, 2, 4, 8, 16] {
        group.bench_with_input(BenchmarkId::new("Mutex", n), &n, |b, &n| {
            b.iter(|| {
                let lb = Arc::new(Mutex::new(ContendedLeaderboard::new()));
                let handles: Vec<_> = (0..n)
                    .map(|_| {
                        let lb = Arc::clone(&lb);
                        thread::spawn(move || {
                            for _ in 0..OPS_PER_THREAD {
                                lb.lock().unwrap().update("en.wikipedia.org");
                            }
                        })
                    })
                    .collect();
                for h in handles {
                    h.join().unwrap();
                }
            });
        });

        group.bench_with_input(BenchmarkId::new("RwLock", n), &n, |b, &n| {
            b.iter(|| {
                let lb = Arc::new(RwLock::new(ContendedLeaderboard::new()));
                let handles: Vec<_> = (0..n)
                    .map(|_| {
                        let lb = Arc::clone(&lb);
                        thread::spawn(move || {
                            for _ in 0..OPS_PER_THREAD {
                                lb.write().unwrap().update("en.wikipedia.org");
                            }
                        })
                    })
                    .collect();
                for h in handles {
                    h.join().unwrap();
                }
            });
        });

        group.bench_with_input(BenchmarkId::new("Atomic", n), &n, |b, &n| {
            b.iter(|| {
                let counter = Arc::new(AtomicU64::new(0));
                let handles: Vec<_> = (0..n)
                    .map(|_| {
                        let counter = Arc::clone(&counter);
                        thread::spawn(move || {
                            for _ in 0..OPS_PER_THREAD {
                                counter.fetch_add(1, Ordering::Relaxed);
                            }
                        })
                    })
                    .collect();
                for h in handles {
                    h.join().unwrap();
                }
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_pipeline_comparison, bench_sync_contention);
criterion_main!(benches);
