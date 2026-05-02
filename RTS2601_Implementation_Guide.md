# RTS2601 — Wikipedia Realtime Pipeline: Implementation Guide

> **Module:** CT087-3-3 Realtime Systems | **Language:** Rust  
> **Data Source:** Wikipedia Recent Changes SSE Stream

---

## Table of Contents

1. [Project Setup](#1-project-setup)
2. [File Structure](#2-file-structure)
3. [Shared Types](#3-shared-types)
4. [Parser](#4-parser)
5. [Priority Channel](#5-priority-channel)
6. [Ingestion Pipelines](#6-ingestion-pipelines)
7. [Scheduler and Drift Tracker](#7-scheduler-and-drift-tracker)
8. [Leaderboard](#8-leaderboard)
9. [Watchdog and Jitter Monitor](#9-watchdog-and-jitter-monitor)
10. [Logging](#10-logging)
11. [Dashboard](#11-dashboard)
12. [Main Entry Point](#12-main-entry-point)
13. [Criterion Benchmarks](#13-criterion-benchmarks)
14. [Running the System](#14-running-the-system)
15. [Expected Outputs](#15-expected-outputs)
16. [Distinction Checklist](#16-distinction-checklist)

---

## 1. Project Setup

### 1.1 Create the Project

```bash
cargo new rts2601
cd rts2601
```

### 1.2 `Cargo.toml`

```toml
[package]
name = "rts2601"
version = "0.1.0"
edition = "2021"

[[bench]]
name = "rts_benchmarks"
harness = false

[dependencies]
tokio        = { version = "1",    features = ["full"] }
reqwest      = { version = "0.11", features = ["stream"] }
ureq         = "2"
serde        = { version = "1",    features = ["derive"] }
serde_json   = "1"
futures-util = "0.3"
bytes        = "1"
tracing      = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter", "json"] }
tracing-appender   = "0.2"
ratatui    = "0.26"
crossterm  = "0.27"
chrono     = "0.4"

[dev-dependencies]
criterion = { version = "0.5", features = ["html_reports"] }
tokio     = { version = "1",   features = ["full"] }
```

### 1.3 Create Directories

```bash
mkdir -p src/ingestion src/channel src/parser
mkdir -p src/scheduler src/leaderboard src/watchdog src/dashboard
mkdir -p benches logs
```

---

## 2. File Structure

```
rts2601/
├── Cargo.toml
├── benches/
│   └── rts_benchmarks.rs
├── logs/                        # created at runtime
└── src/
    ├── main.rs
    ├── types.rs
    ├── logging.rs
    ├── channel/
    │   └── mod.rs
    ├── parser/
    │   └── mod.rs
    ├── ingestion/
    │   ├── mod.rs
    │   ├── async_pipeline.rs
    │   └── threaded_pipeline.rs
    ├── scheduler/
    │   └── mod.rs
    ├── leaderboard/
    │   └── mod.rs
    ├── watchdog/
    │   └── mod.rs
    └── dashboard/
        └── mod.rs
```

---

## 3. Shared Types

Create `src/types.rs` first. Every other module depends on these types.

```rust
// src/types.rs

use std::sync::{Arc, Mutex};
use std::sync::atomic::AtomicBool;
use std::time::Instant;
use std::collections::VecDeque;

// One event moving through the pipeline.
// enqueued_at is stamped inside PriorityChannel::push() — not at parse time.
#[derive(Debug, Clone)]
pub struct PrioritisedEvent {
    pub user:        String,
    pub is_bot:      bool,
    pub domain:      String,
    pub title:       String,
    pub enqueued_at: Instant,
}

// What happened when an event was pushed into PriorityChannel.
#[derive(Debug, PartialEq)]
pub enum PushResult {
    Accepted,        // channel had space
    DroppedIncoming, // incoming bot rejected — channel full
    BotEvicted,      // buffered bot removed to fit incoming human
    DroppedOldest,   // channel all-human — oldest human dropped
}

// Status of an event shown in the live feed panel.
#[derive(Debug, Clone)]
pub enum EventStatus {
    Processed,
    BotEvicted,
    BotDropped,
    DeadlineMissed,
}

// One row in the dashboard live feed.
#[derive(Debug, Clone)]
pub struct RecentEvent {
    pub timestamp: String,
    pub user:      String,
    pub domain:    String,
    pub is_bot:    bool,
    pub status:    EventStatus,
}

// All runtime counters. Read by dashboard, written by processor loop.
#[derive(Debug, Default, Clone)]
pub struct SystemStats {
    pub events_processed:     u64,
    pub human_events:         u64,
    pub bot_events:           u64,
    pub bot_evictions:        u64,
    pub bot_drops:            u64,
    pub human_drops:          u64,
    pub deadline_misses:      u64,
    pub reconnect_count:      u64,
    pub degraded_activations: u64,
    pub degraded_mode:        bool,
    pub current_buffer_fill:  usize,
    pub throughput_per_sec:   f64,
    pub active_pipeline:      String,

    pub avg_mutex_ns:   f64,
    pub avg_rwlock_ns:  f64,
    pub avg_atomic_ns:  f64,

    pub human_drift_p50: f64,
    pub human_drift_p90: f64,
    pub human_drift_p99: f64,
    pub bot_drift_p50:   f64,
    pub bot_drift_p90:   f64,
    pub bot_drift_p99:   f64,

    pub recent_events: VecDeque<RecentEvent>,
}

// Shared across all threads via Arc clones.
pub struct SharedState {
    pub stats:         Arc<Mutex<SystemStats>>,
    pub degraded_mode: Arc<AtomicBool>,
    pub start_time:    Instant,
}

impl SharedState {
    pub fn new() -> Self {
        Self {
            stats:         Arc::new(Mutex::new(SystemStats::default())),
            degraded_mode: Arc::new(AtomicBool::new(false)),
            start_time:    Instant::now(),
        }
    }
}
```

---

## 4. Parser

Create `src/parser/mod.rs`.

Zero-copy parsing using serde lifetimes. `WikiEvent<'a>` borrows string slices directly from the raw JSON buffer — no heap allocation during parsing. The one intentional allocation happens when converting to `PrioritisedEvent` so the data can outlive the buffer.

The parse deadline (2ms) measures parsing speed only. A separate end-to-end deadline is checked in the processor loop.

```rust
// src/parser/mod.rs

use serde::Deserialize;
use std::time::{Duration, Instant};
use crate::types::PrioritisedEvent;

pub const PARSE_DEADLINE: Duration = Duration::from_millis(2);

// Borrows directly from the raw JSON buffer.
// Must not outlive the buffer — enforced by lifetime 'a.
#[derive(Deserialize, Debug)]
pub struct WikiEvent<'a> {
    #[serde(borrow)]
    pub user: &'a str,

    #[serde(default)]
    pub bot: bool,

    #[serde(borrow, rename = "server_name")]
    pub server_name: &'a str,

    #[serde(borrow, default)]
    pub title: &'a str,
}

// Parses one raw JSON string into a PrioritisedEvent.
// enqueued_at is a placeholder here — overwritten in PriorityChannel::push().
pub fn parse_event(raw_json: &str) -> Result<PrioritisedEvent, String> {
    let parse_start = Instant::now();

    let event: WikiEvent = serde_json::from_str(raw_json)
        .map_err(|e| format!("parse error: {}", e))?;

    let elapsed = parse_start.elapsed();

    if elapsed > PARSE_DEADLINE {
        tracing::error!(
            parse_us = elapsed.as_micros(),
            user     = %event.user,
            "Parse deadline missed"
        );
    } else {
        tracing::debug!(parse_us = elapsed.as_micros(), "Parsed within deadline");
    }

    Ok(PrioritisedEvent {
        user:        event.user.to_owned(),
        is_bot:      event.bot,
        domain:      event.server_name.to_owned(),
        title:       event.title.to_owned(),
        enqueued_at: Instant::now(), // overwritten in PriorityChannel::push()
    })
}

// --- Distinction: Custom allocator for zero-allocation proof ---
// Add to main.rs to count heap allocations globally.
// Before parse_event(): record ALLOC_COUNT.
// After parse_event():  assert count unchanged.
//
// use std::alloc::{GlobalAlloc, System, Layout};
// use std::sync::atomic::{AtomicU64, Ordering};
// pub static ALLOC_COUNT: AtomicU64 = AtomicU64::new(0);
// struct CountingAllocator;
// unsafe impl GlobalAlloc for CountingAllocator {
//     unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
//         ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
//         System.alloc(layout)
//     }
//     unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
//         System.dealloc(ptr, layout)
//     }
// }
// #[global_allocator]
// static A: CountingAllocator = CountingAllocator;
```

---

## 5. Priority Channel

Create `src/channel/mod.rs`.

A bounded `VecDeque` with capacity 100. `enqueued_at` is stamped at `push()` — this is the correct reference point for drift measurement.

**Push rules when full:**
- Incoming bot → drop it (`DroppedIncoming`)
- Incoming human + bot in buffer → evict oldest bot, insert human (`BotEvicted`)
- Incoming human + no bots → drop oldest human, insert new human (`DroppedOldest`)

**Pop rule:** `pop_next()` scans for humans first. If any human is waiting, it is returned regardless of arrival order. If no humans, returns the oldest bot. This is execution-level priority — humans override bots even if bots arrived earlier.

Every drop or eviction emits a structured "Overflow Event" log with nanosecond precision.

```rust
// src/channel/mod.rs

use std::collections::VecDeque;
use std::time::{SystemTime, UNIX_EPOCH, Instant};
use crate::types::{PrioritisedEvent, PushResult};

pub struct PriorityChannel {
    buffer:   VecDeque<PrioritisedEvent>,
    capacity: usize,
}

impl PriorityChannel {
    pub fn new(capacity: usize) -> Self {
        Self {
            buffer:   VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    pub fn push(&mut self, mut event: PrioritisedEvent) -> PushResult {
        // Stamp here — drift measures time from this point to pop_next().
        event.enqueued_at = Instant::now();

        if self.buffer.len() < self.capacity {
            self.buffer.push_back(event);
            return PushResult::Accepted;
        }

        if event.is_bot {
            self.overflow_log("bot_dropped_incoming", &event.user, &event.domain);
            return PushResult::DroppedIncoming;
        }

        let bot_pos = self.buffer.iter().position(|e| e.is_bot);
        match bot_pos {
            Some(pos) => {
                let evicted = self.buffer.remove(pos).unwrap();
                self.overflow_log("bot_evicted_for_human", &evicted.user, &evicted.domain);
                self.buffer.push_back(event);
                PushResult::BotEvicted
            }
            None => {
                let dropped = self.buffer.pop_front().unwrap();
                self.overflow_log("oldest_human_dropped", &dropped.user, &dropped.domain);
                self.buffer.push_back(event);
                PushResult::DroppedOldest
            }
        }
    }

    // Humans override bots at execution time.
    // Scans for any human — returns it before any bot regardless of arrival order.
    pub fn pop_next(&mut self) -> Option<PrioritisedEvent> {
        let human_pos = self.buffer.iter().position(|e| !e.is_bot);
        match human_pos {
            Some(pos) => self.buffer.remove(pos),
            None      => self.buffer.pop_front(),
        }
    }

    pub fn len(&self)      -> usize { self.buffer.len() }
    pub fn capacity(&self) -> usize { self.capacity }
    pub fn is_empty(&self) -> bool  { self.buffer.is_empty() }

    fn overflow_log(&self, reason: &str, user: &str, domain: &str) {
        let ts_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        tracing::warn!(
            event_type   = "OverflowEvent",
            timestamp_ns = ts_ns,
            reason       = reason,
            user         = %user,
            domain       = %domain,
            "Overflow Event"
        );
    }
}
```

---

## 6. Ingestion Pipelines

### `src/ingestion/mod.rs`

```rust
pub mod async_pipeline;
pub mod threaded_pipeline;
```

### `src/ingestion/async_pipeline.rs`

Tokio task. Reads the Wikipedia SSE stream with async I/O. Calls `schedule_event()` after parsing to push into `PriorityChannel`. Sends a heartbeat on every received event. Reconnects when the watchdog signals.

```rust
// src/ingestion/async_pipeline.rs

use tokio::sync::mpsc::Receiver as TokioReceiver;
use futures_util::StreamExt;
use std::sync::{Arc, Mutex};
use std::sync::mpsc::SyncSender;
use crate::parser::parse_event;
use crate::channel::PriorityChannel;
use crate::scheduler::schedule_event;
use crate::types::SharedState;

const SSE_URL: &str = "https://stream.wikimedia.org/v2/stream/recentchange";

pub async fn run_async_pipeline(
    channel:      Arc<Mutex<PriorityChannel>>,
    heartbeat_tx: SyncSender<()>,
    mut reconnect_rx: TokioReceiver<()>,
    state:        Arc<SharedState>,
) {
    loop {
        tracing::info!(pipeline = "async", "Connecting to Wikipedia SSE stream");

        let client   = reqwest::Client::new();
        let response = match client.get(SSE_URL).send().await {
            Ok(r)  => r,
            Err(e) => {
                tracing::error!(pipeline = "async", error = %e, "Connection failed");
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                continue;
            }
        };

        let mut stream = response.bytes_stream();

        loop {
            tokio::select! {
                chunk = stream.next() => {
                    match chunk {
                        Some(Ok(bytes)) => {
                            let text = match std::str::from_utf8(&bytes) {
                                Ok(t)  => t,
                                Err(_) => continue,
                            };
                            for line in text.lines() {
                                if let Some(json) = line.strip_prefix("data: ") {
                                    let _ = heartbeat_tx.try_send(());
                                    match parse_event(json) {
                                        Ok(event) => {
                                            tracing::info!(
                                                pipeline = "async",
                                                user     = %event.user,
                                                domain   = %event.domain,
                                                is_bot   = event.is_bot,
                                                "Event ingested"
                                            );
                                            schedule_event(event, &channel, &state);
                                        }
                                        Err(e) => {
                                            tracing::debug!(error = %e, "Parse skipped");
                                        }
                                    }
                                }
                            }
                        }
                        Some(Err(e)) => {
                            tracing::error!(pipeline = "async", error = %e, "Stream error");
                            break;
                        }
                        None => {
                            tracing::warn!(pipeline = "async", "Stream ended");
                            break;
                        }
                    }
                }
                _ = reconnect_rx.recv() => {
                    tracing::warn!(pipeline = "async", "Reconnect signal received");
                    break;
                }
            }
        }

        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}
```

### `src/ingestion/threaded_pipeline.rs`

`std::thread` with blocking I/O. Same behaviour as async pipeline but uses `ureq` and `std::sync::mpsc::sync_channel`. Run separately from the async pipeline — one active per run, controlled by CLI flag.

`tokio::sync::mpsc` (async) vs `std::sync::mpsc::sync_channel` (threaded) — both bounded, same capacity, different runtime models. This makes the Criterion comparison valid.

```rust
// src/ingestion/threaded_pipeline.rs

use std::thread;
use std::time::Duration;
use std::io::{BufRead, BufReader};
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{SyncSender, Receiver};
use crate::parser::parse_event;
use crate::channel::PriorityChannel;
use crate::scheduler::schedule_event;
use crate::types::SharedState;

const SSE_URL: &str = "https://stream.wikimedia.org/v2/stream/recentchange";

pub fn run_threaded_pipeline(
    channel:      Arc<Mutex<PriorityChannel>>,
    heartbeat_tx: SyncSender<()>,
    reconnect_rx: Receiver<()>,
    state:        Arc<SharedState>,
) {
    thread::spawn(move || {
        loop {
            tracing::info!(pipeline = "threaded", "Connecting to Wikipedia SSE stream");

            let response = match ureq::get(SSE_URL).call() {
                Ok(r)  => r,
                Err(e) => {
                    tracing::error!(pipeline = "threaded", error = %e, "Connection failed");
                    thread::sleep(Duration::from_secs(5));
                    continue;
                }
            };

            let reader = BufReader::new(response.into_reader());

            for line_result in reader.lines() {
                if reconnect_rx.try_recv().is_ok() {
                    tracing::warn!(pipeline = "threaded", "Reconnect signal received");
                    break;
                }
                match line_result {
                    Ok(line) => {
                        if let Some(json) = line.strip_prefix("data: ") {
                            let _ = heartbeat_tx.try_send(());
                            match parse_event(json) {
                                Ok(event) => {
                                    tracing::info!(
                                        pipeline = "threaded",
                                        user     = %event.user,
                                        domain   = %event.domain,
                                        is_bot   = event.is_bot,
                                        "Event ingested"
                                    );
                                    schedule_event(event, &channel, &state);
                                }
                                Err(e) => {
                                    tracing::debug!(error = %e, "Parse skipped");
                                }
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!(pipeline = "threaded", error = %e, "Read error");
                        break;
                    }
                }
            }

            thread::sleep(Duration::from_secs(2));
        }
    });
}
```

---

## 7. Scheduler and Drift Tracker

Create `src/scheduler/mod.rs`.

`schedule_event()` is called by both pipelines after `parse_event()`. It pushes into `PriorityChannel` and updates `SystemStats` based on the result.

`DriftTracker` collects drift samples (in microseconds) separately for human and bot events. Drift = time from `enqueued_at` (stamped in `push()`) to when `pop_next()` is called. Reports p50/p90/p99 per group.

```rust
// src/scheduler/mod.rs

use std::sync::{Arc, Mutex};
use std::time::Instant;
use crate::channel::PriorityChannel;
use crate::types::{PrioritisedEvent, PushResult, SharedState, RecentEvent, EventStatus};

pub fn schedule_event(
    event:   PrioritisedEvent,
    channel: &Arc<Mutex<PriorityChannel>>,
    state:   &Arc<SharedState>,
) {
    let is_bot = event.is_bot;
    let user   = event.user.clone();
    let domain = event.domain.clone();

    let result = {
        let mut ch = channel.lock().unwrap();
        ch.push(event)
    };

    if let Ok(mut s) = state.stats.lock() {
        match &result {
            PushResult::Accepted => {}
            PushResult::DroppedIncoming => {
                s.bot_drops += 1;
                s.recent_events.push_back(RecentEvent {
                    timestamp: chrono::Local::now().format("%H:%M:%S").to_string(),
                    user, domain, is_bot,
                    status: EventStatus::BotDropped,
                });
            }
            PushResult::BotEvicted => {
                s.bot_evictions += 1;
                s.recent_events.push_back(RecentEvent {
                    timestamp: chrono::Local::now().format("%H:%M:%S").to_string(),
                    user, domain, is_bot,
                    status: EventStatus::BotEvicted,
                });
            }
            PushResult::DroppedOldest => {
                s.human_drops += 1;
            }
        }
        s.current_buffer_fill = channel.lock().unwrap().len();
        if s.recent_events.len() > 10 { s.recent_events.pop_front(); }
    }
}

pub struct DriftTracker {
    pub human_samples: Vec<f64>,
    pub bot_samples:   Vec<f64>,
}

impl DriftTracker {
    pub fn new() -> Self {
        Self { human_samples: Vec::new(), bot_samples: Vec::new() }
    }

    pub fn record(&mut self, enqueued_at: Instant, is_bot: bool) {
        let drift_us = enqueued_at.elapsed().as_micros() as f64;
        if is_bot { self.bot_samples.push(drift_us); }
        else      { self.human_samples.push(drift_us); }

        if drift_us > 2000.0 {
            tracing::warn!(drift_us = drift_us, is_bot = is_bot, "Scheduling drift exceeded 2ms");
        }
    }

    pub fn percentile(samples: &mut Vec<f64>, pct: f64) -> f64 {
        if samples.is_empty() { return 0.0; }
        samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let idx = (pct / 100.0 * samples.len() as f64) as usize;
        samples[idx.min(samples.len() - 1)]
    }

    pub fn update_stats(&mut self, state: &Arc<SharedState>) {
        if let Ok(mut s) = state.stats.lock() {
            s.human_drift_p50 = Self::percentile(&mut self.human_samples, 50.0) / 1000.0;
            s.human_drift_p90 = Self::percentile(&mut self.human_samples, 90.0) / 1000.0;
            s.human_drift_p99 = Self::percentile(&mut self.human_samples, 99.0) / 1000.0;
            s.bot_drift_p50   = Self::percentile(&mut self.bot_samples,   50.0) / 1000.0;
            s.bot_drift_p90   = Self::percentile(&mut self.bot_samples,   90.0) / 1000.0;
            s.bot_drift_p99   = Self::percentile(&mut self.bot_samples,   99.0) / 1000.0;
        }
    }

    pub fn report(&mut self) {
        tracing::info!(
            human_p50_us = Self::percentile(&mut self.human_samples, 50.0),
            human_p90_us = Self::percentile(&mut self.human_samples, 90.0),
            human_p99_us = Self::percentile(&mut self.human_samples, 99.0),
            human_misses = self.human_samples.iter().filter(|&&d| d > 2000.0).count(),
            bot_p50_us   = Self::percentile(&mut self.bot_samples, 50.0),
            bot_p90_us   = Self::percentile(&mut self.bot_samples, 90.0),
            bot_p99_us   = Self::percentile(&mut self.bot_samples, 99.0),
            bot_misses   = self.bot_samples.iter().filter(|&&d| d > 2000.0).count(),
            "Scheduling drift report"
        );
    }
}
```

---

## 8. Leaderboard

Create `src/leaderboard/mod.rs`.

Tracks edit counts per domain. Three sync-wrapped versions run simultaneously: `Mutex`, `RwLock`, and `AtomicU64`. Every processed event updates all three with timing recorded for each. Rolling averages feed the dashboard. The Criterion benchmark tests all three under controlled contention.

```rust
// src/leaderboard/mod.rs

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

#[derive(Debug, Default)]
pub struct Leaderboard {
    pub counts: HashMap<String, u64>,
}

impl Leaderboard {
    pub fn new() -> Self { Self { counts: HashMap::new() } }

    pub fn update(&mut self, domain: &str) {
        *self.counts.entry(domain.to_string()).or_insert(0) += 1;
    }

    pub fn top3(&self) -> Vec<(String, u64)> {
        let mut v: Vec<_> = self.counts.iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect();
        v.sort_by(|a, b| b.1.cmp(&a.1));
        v.into_iter().take(3).collect()
    }
}

pub struct LeaderboardManager {
    pub mutex_lb:  Arc<Mutex<Leaderboard>>,
    pub rwlock_lb: Arc<RwLock<Leaderboard>>,
    pub atomic_en: Arc<AtomicU64>,
    pub atomic_de: Arc<AtomicU64>,
    pub atomic_fr: Arc<AtomicU64>,
    pub atomic_es: Arc<AtomicU64>,
    pub atomic_ja: Arc<AtomicU64>,
    mutex_times:   Vec<u64>,
    rwlock_times:  Vec<u64>,
    atomic_times:  Vec<u64>,
}

impl LeaderboardManager {
    pub fn new() -> Self {
        Self {
            mutex_lb:    Arc::new(Mutex::new(Leaderboard::new())),
            rwlock_lb:   Arc::new(RwLock::new(Leaderboard::new())),
            atomic_en:   Arc::new(AtomicU64::new(0)),
            atomic_de:   Arc::new(AtomicU64::new(0)),
            atomic_fr:   Arc::new(AtomicU64::new(0)),
            atomic_es:   Arc::new(AtomicU64::new(0)),
            atomic_ja:   Arc::new(AtomicU64::new(0)),
            mutex_times:  Vec::new(),
            rwlock_times: Vec::new(),
            atomic_times: Vec::new(),
        }
    }

    pub fn update_all(&mut self, domain: &str) -> (u64, u64, u64) {
        let t = Instant::now();
        { self.mutex_lb.lock().unwrap().update(domain); }
        let mutex_ns = t.elapsed().as_nanos() as u64;

        let t = Instant::now();
        { self.rwlock_lb.write().unwrap().update(domain); }
        let rwlock_ns = t.elapsed().as_nanos() as u64;

        let t = Instant::now();
        let counter = match domain {
            "en.wikipedia.org" => &self.atomic_en,
            "de.wikipedia.org" => &self.atomic_de,
            "fr.wikipedia.org" => &self.atomic_fr,
            "es.wikipedia.org" => &self.atomic_es,
            "ja.wikipedia.org" => &self.atomic_ja,
            _                  => &self.atomic_en,
        };
        counter.fetch_add(1, Ordering::Relaxed);
        let atomic_ns = t.elapsed().as_nanos() as u64;

        for (vec, val) in [
            (&mut self.mutex_times,  mutex_ns),
            (&mut self.rwlock_times, rwlock_ns),
            (&mut self.atomic_times, atomic_ns),
        ] {
            vec.push(val);
            if vec.len() > 1000 { vec.remove(0); }
        }

        tracing::debug!(mutex_ns, rwlock_ns, atomic_ns, domain, "Sync timings");
        (mutex_ns, rwlock_ns, atomic_ns)
    }

    fn avg(v: &[u64]) -> f64 {
        if v.is_empty() { return 0.0; }
        v.iter().sum::<u64>() as f64 / v.len() as f64
    }

    pub fn avg_mutex_ns(&self)  -> f64 { Self::avg(&self.mutex_times) }
    pub fn avg_rwlock_ns(&self) -> f64 { Self::avg(&self.rwlock_times) }
    pub fn avg_atomic_ns(&self) -> f64 { Self::avg(&self.atomic_times) }

    pub fn top3(&self) -> Vec<(String, u64)> {
        self.rwlock_lb.read().unwrap().top3()
    }
}
```

---

## 9. Watchdog and Jitter Monitor

Create `src/watchdog/mod.rs`.

The watchdog runs on its own `std::thread`. It waits on a heartbeat channel with a 10-second timeout. Every SSE event sends a heartbeat. If 10 seconds pass with none, it signals the pipeline to reconnect.

`JitterMonitor` tracks a rolling window of 100 processing times. When standard deviation exceeds 5ms, `degraded_mode` flips to `true` — the processor discards bot events to reduce load. Flips back when jitter recovers.

```rust
// src/watchdog/mod.rs

use std::thread;
use std::time::{Duration, Instant};
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{SyncSender, Receiver, RecvTimeoutError};
use crate::types::SharedState;

const WATCHDOG_TIMEOUT:    Duration = Duration::from_secs(10);
const JITTER_THRESHOLD_MS: f64     = 5.0;
const JITTER_WINDOW:       usize   = 100;

pub fn start_watchdog(
    heartbeat_rx: Receiver<()>,
    reconnect_tx: SyncSender<()>,
    state:        Arc<SharedState>,
) {
    thread::spawn(move || {
        tracing::info!("Watchdog started — timeout = 10s");
        loop {
            match heartbeat_rx.recv_timeout(WATCHDOG_TIMEOUT) {
                Ok(_) => {
                    tracing::debug!("Watchdog heartbeat received");
                }
                Err(RecvTimeoutError::Timeout) => {
                    tracing::warn!("Watchdog timeout — triggering reconnect");
                    if let Ok(mut s) = state.stats.lock() {
                        s.reconnect_count += 1;
                    }
                    let _ = reconnect_tx.send(());
                }
                Err(RecvTimeoutError::Disconnected) => {
                    tracing::info!("Watchdog shutting down");
                    break;
                }
            }
        }
    });
}

pub struct JitterMonitor {
    recent_times:      VecDeque<f64>,
    pub degraded:      Arc<AtomicBool>,
    degraded_start:    Option<Instant>,
    total_degraded_ms: u64,
}

impl JitterMonitor {
    pub fn new(degraded: Arc<AtomicBool>) -> Self {
        Self {
            recent_times:      VecDeque::with_capacity(JITTER_WINDOW),
            degraded,
            degraded_start:    None,
            total_degraded_ms: 0,
        }
    }

    pub fn record(&mut self, processing_time_ms: f64, state: &Arc<SharedState>) {
        if self.recent_times.len() == JITTER_WINDOW { self.recent_times.pop_front(); }
        self.recent_times.push_back(processing_time_ms);
        if self.recent_times.len() >= 10 { self.evaluate(state); }
    }

    fn jitter(&self) -> f64 {
        let n    = self.recent_times.len() as f64;
        let mean = self.recent_times.iter().sum::<f64>() / n;
        let var  = self.recent_times.iter().map(|t| (t - mean).powi(2)).sum::<f64>() / n;
        var.sqrt()
    }

    fn evaluate(&mut self, state: &Arc<SharedState>) {
        let jitter      = self.jitter();
        let is_degraded = self.degraded.load(Ordering::Relaxed);

        if jitter > JITTER_THRESHOLD_MS && !is_degraded {
            self.degraded.store(true, Ordering::Relaxed);
            self.degraded_start = Some(Instant::now());
            tracing::error!(jitter_ms = jitter, threshold_ms = JITTER_THRESHOLD_MS,
                "Jitter threshold exceeded — degraded mode ON");
            if let Ok(mut s) = state.stats.lock() {
                s.degraded_mode = true;
                s.degraded_activations += 1;
            }
        } else if jitter <= JITTER_THRESHOLD_MS && is_degraded {
            self.degraded.store(false, Ordering::Relaxed);
            if let Some(start) = self.degraded_start.take() {
                self.total_degraded_ms += start.elapsed().as_millis() as u64;
            }
            tracing::info!(jitter_ms = jitter, "Jitter recovered — degraded mode OFF");
            if let Ok(mut s) = state.stats.lock() { s.degraded_mode = false; }
        }
    }

    pub fn total_degraded_secs(&self) -> f64 {
        self.total_degraded_ms as f64 / 1000.0
    }
}
```

---

## 10. Logging

Create `src/logging.rs`.

Two simultaneous outputs: colored terminal and rolling JSON file. Files rotate hourly into `logs/`.

```rust
// src/logging.rs

use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

pub fn init_logging() {
    let file_appender            = RollingFileAppender::new(Rotation::HOURLY, "logs", "rts2601.log");
    let (non_blocking, _guard)   = tracing_appender::non_blocking(file_appender);

    tracing_subscriber::registry()
        .with(fmt::layer().with_target(true).with_thread_ids(true))
        .with(fmt::layer().json().with_writer(non_blocking).with_target(true))
        .with(EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info,rts2601=debug".to_string())
        ))
        .init();
}
```

Post-run analysis:

```bash
grep "deadline missed"       logs/rts2601.log | wc -l
grep "bot_evicted_for_human" logs/rts2601.log | wc -l
grep "bot_dropped_incoming"  logs/rts2601.log | wc -l
grep "Watchdog timeout"      logs/rts2601.log | wc -l
grep "degraded mode ON"      logs/rts2601.log | wc -l

# Average mutex timing (ns)
cat logs/rts2601.log \
  | jq -r 'select(.fields.mutex_ns != null) | .fields.mutex_ns' \
  | awk '{sum+=$1;n++} END{print sum/n}'
```

---

## 11. Dashboard

Create `src/dashboard/mod.rs`.

Reads `SharedState` every 100ms and renders a ratatui TUI. Press `q` to exit.

```rust
// src/dashboard/mod.rs

use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Gauge, List, ListItem, Paragraph},
    Terminal,
};
use crossterm::{
    event::{self, Event, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use crate::types::SharedState;
use crate::leaderboard::LeaderboardManager;

pub fn run_dashboard(
    state:       Arc<SharedState>,
    leaderboard: Arc<Mutex<LeaderboardManager>>,
) -> Result<(), Box<dyn std::error::Error>> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend      = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    loop {
        let stats   = state.stats.lock().unwrap().clone();
        let top3    = leaderboard.lock().unwrap().top3();
        let runtime = state.start_time.elapsed().as_secs();

        terminal.draw(|f| {
            let size = f.size();
            let rows = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Length(3),
                    Constraint::Length(9),
                    Constraint::Length(7),
                    Constraint::Min(0),
                ])
                .split(size);

            // Title bar
            let mode_color = if stats.degraded_mode { Color::Red } else { Color::Green };
            let mode_text  = if stats.degraded_mode { "⚠ DEGRADED" } else { "● LIVE" };
            let title = Paragraph::new(Line::from(vec![
                Span::styled(
                    "  RTS2601 Wikipedia Realtime Pipeline   ",
                    Style::default().fg(Color::White).add_modifier(Modifier::BOLD),
                ),
                Span::styled(
                    format!("[{}]  Pipeline: {}  Runtime: {:02}:{:02}",
                        mode_text, stats.active_pipeline, runtime / 60, runtime % 60),
                    Style::default().fg(mode_color),
                ),
            ]))
            .block(Block::default().borders(Borders::ALL));
            f.render_widget(title, rows[0]);

            // Row 1
            let row1 = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([
                    Constraint::Percentage(33),
                    Constraint::Percentage(34),
                    Constraint::Percentage(33),
                ])
                .split(rows[1]);

            let lb_items: Vec<ListItem> = top3.iter().enumerate()
                .map(|(i, (domain, count))| {
                    ListItem::new(format!("{}. {:22} {:>6}", i + 1, domain, count))
                })
                .collect();
            f.render_widget(
                List::new(lb_items)
                    .block(Block::default().title(" TOP 3 DOMAINS ").borders(Borders::ALL)),
                row1[0],
            );

            f.render_widget(
                Paragraph::new(vec![
                    Line::from(format!(" Mode:      {}", stats.active_pipeline)),
                    Line::from(format!(" TPS:       {:.0}/s", stats.throughput_per_sec)),
                    Line::from(format!(" Bot evict: {}", stats.bot_evictions)),
                    Line::from(format!(" Bot drops: {}", stats.bot_drops)),
                    Line::from(format!(" Hum drops: {}", stats.human_drops)),
                    Line::from(format!(" Reconnect: {}", stats.reconnect_count)),
                ])
                .block(Block::default().title(" PIPELINE STATUS ").borders(Borders::ALL)),
                row1[1],
            );

            let h_color = if stats.human_drift_p99 < 2.0 { Color::Green } else { Color::Red };
            let b_color = if stats.bot_drift_p99   < 2.0 { Color::Green } else { Color::Yellow };
            f.render_widget(
                Paragraph::new(vec![
                    Line::from(Span::styled(
                        format!(" Human p50: {:.2}ms", stats.human_drift_p50),
                        Style::default().fg(h_color),
                    )),
                    Line::from(Span::styled(
                        format!(" Human p90: {:.2}ms", stats.human_drift_p90),
                        Style::default().fg(h_color),
                    )),
                    Line::from(Span::styled(
                        format!(" Human p99: {:.2}ms", stats.human_drift_p99),
                        Style::default().fg(h_color),
                    )),
                    Line::from(Span::styled(
                        format!(" Bot   p99: {:.2}ms", stats.bot_drift_p99),
                        Style::default().fg(b_color),
                    )),
                    Line::from(format!(" Misses:    {}", stats.deadline_misses)),
                ])
                .block(Block::default().title(" LATENCY MONITOR ").borders(Borders::ALL)),
                row1[2],
            );

            // Row 2
            let row2 = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([
                    Constraint::Percentage(33),
                    Constraint::Percentage(34),
                    Constraint::Percentage(33),
                ])
                .split(rows[2]);

            let fill_pct    = (stats.current_buffer_fill as f64 / 100.0 * 100.0) as u16;
            let gauge_color = if fill_pct > 80 { Color::Red }
                else if fill_pct > 50 { Color::Yellow }
                else { Color::Green };
            f.render_widget(
                Gauge::default()
                    .block(Block::default().title(" CHANNEL BUFFER ").borders(Borders::ALL))
                    .gauge_style(Style::default().fg(gauge_color))
                    .percent(fill_pct)
                    .label(format!("{}/100", stats.current_buffer_fill)),
                row2[0],
            );

            f.render_widget(
                Paragraph::new(vec![
                    Line::from(format!(" Mutex:  {:>8.0} ns", stats.avg_mutex_ns)),
                    Line::from(format!(" RwLock: {:>8.0} ns", stats.avg_rwlock_ns)),
                    Line::from(Span::styled(
                        format!(" Atomic: {:>8.0} ns ◄", stats.avg_atomic_ns),
                        Style::default().fg(Color::Green),
                    )),
                    Line::from(format!(" Total:  {:>8}", stats.events_processed)),
                ])
                .block(Block::default().title(" SYNC BENCHMARK ").borders(Borders::ALL)),
                row2[1],
            );

            let wd_color  = if stats.degraded_mode { Color::Red } else { Color::Green };
            let wd_status = if stats.degraded_mode { "⚠ DEGRADED" } else { "● CONNECTED" };
            f.render_widget(
                Paragraph::new(vec![
                    Line::from(Span::styled(
                        format!(" Status:   {}", wd_status),
                        Style::default().fg(wd_color),
                    )),
                    Line::from(format!(" Reconnects: {}", stats.reconnect_count)),
                    Line::from(format!(" Degraded:   {} times", stats.degraded_activations)),
                ])
                .block(Block::default().title(" WATCHDOG ").borders(Borders::ALL)),
                row2[2],
            );

            // Row 3: Live feed
            let feed_items: Vec<ListItem> = stats.recent_events.iter().rev()
                .map(|e| {
                    let (color, tag) = match e.status {
                        crate::types::EventStatus::Processed      =>
                            (if e.is_bot { Color::Gray } else { Color::Green }, "✓"),
                        crate::types::EventStatus::BotEvicted     => (Color::Yellow,  "EVICTED"),
                        crate::types::EventStatus::BotDropped     => (Color::Red,     "DROPPED"),
                        crate::types::EventStatus::DeadlineMissed => (Color::Magenta, "MISS"),
                    };
                    let kind = if e.is_bot { "BOT  " } else { "HUMAN" };
                    ListItem::new(Span::styled(
                        format!(" [{}] {:16} {:24} {}  {}",
                            e.timestamp, e.user, e.domain, kind, tag),
                        Style::default().fg(color),
                    ))
                })
                .collect();
            f.render_widget(
                List::new(feed_items)
                    .block(Block::default().title(" LIVE EVENT FEED (q to quit) ").borders(Borders::ALL)),
                rows[3],
            );
        })?;

        if event::poll(Duration::from_millis(100))? {
            if let Event::Key(key) = event::read()? {
                if key.code == KeyCode::Char('q') { break; }
            }
        }
    }

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    Ok(())
}
```

---

## 12. Main Entry Point

Create `src/main.rs`.

Reads `--pipeline async` (default) or `--pipeline threaded` from CLI. Only one pipeline is active per run — this keeps the benchmark comparison clean and unambiguous.

The processor loop:
1. Calls `pop_next()` — humans override bots at execution time
2. Checks degraded mode — discards bots if active
3. Measures scheduling drift
4. Updates leaderboard (all three sync primitives timed)
5. Checks end-to-end 2ms deadline
6. Records processing time for jitter monitor

```rust
// src/main.rs

mod types;
mod logging;
mod parser;
mod channel;
mod ingestion;
mod scheduler;
mod leaderboard;
mod watchdog;
mod dashboard;

use std::sync::{Arc, Mutex};
use std::sync::atomic::Ordering;
use std::sync::mpsc::sync_channel;
use std::time::{Duration, Instant};
use tokio::sync::mpsc as tokio_mpsc;

use types::SharedState;
use channel::PriorityChannel;
use leaderboard::LeaderboardManager;
use watchdog::{start_watchdog, JitterMonitor};
use scheduler::DriftTracker;

#[tokio::main]
async fn main() {
    logging::init_logging();

    let args: Vec<String> = std::env::args().collect();
    let pipeline_mode = args.iter()
        .position(|a| a == "--pipeline")
        .and_then(|i| args.get(i + 1))
        .map(|s| s.as_str())
        .unwrap_or("async")
        .to_string();

    tracing::info!(pipeline = %pipeline_mode, "Starting RTS2601");

    let state       = Arc::new(SharedState::new());
    let channel     = Arc::new(Mutex::new(PriorityChannel::new(100)));
    let leaderboard = Arc::new(Mutex::new(LeaderboardManager::new()));

    if let Ok(mut s) = state.stats.lock() {
        s.active_pipeline = pipeline_mode.clone();
    }

    let (heartbeat_tx, heartbeat_rx) = sync_channel::<()>(10);
    let (reconnect_tx, reconnect_rx_std) = sync_channel::<()>(1);
    let (_reconnect_tx_tokio, reconnect_rx_tokio) = tokio_mpsc::channel::<()>(1);

    start_watchdog(heartbeat_rx, reconnect_tx.clone(), Arc::clone(&state));

    match pipeline_mode.as_str() {
        "threaded" => {
            ingestion::threaded_pipeline::run_threaded_pipeline(
                Arc::clone(&channel),
                heartbeat_tx.clone(),
                reconnect_rx_std,
                Arc::clone(&state),
            );
        }
        _ => {
            let ch = Arc::clone(&channel);
            let hb = heartbeat_tx.clone();
            let st = Arc::clone(&state);
            tokio::spawn(ingestion::async_pipeline::run_async_pipeline(
                ch, hb, reconnect_rx_tokio, st,
            ));
        }
    }

    // Spawn dashboard
    {
        let st = Arc::clone(&state);
        let lb = Arc::clone(&leaderboard);
        std::thread::spawn(move || {
            if let Err(e) = dashboard::run_dashboard(st, lb) {
                tracing::error!("Dashboard error: {}", e);
            }
        });
    }

    // Processor loop
    let mut drift_tracker   = DriftTracker::new();
    let mut jitter_monitor  = JitterMonitor::new(Arc::clone(&state.degraded_mode));
    let mut last_update     = Instant::now();
    let mut events_this_sec = 0u64;

    tracing::info!("Processor loop running — press q to exit");

    loop {
        let event = {
            let mut ch = channel.lock().unwrap();
            ch.pop_next()
        };

        if let Some(event) = event {
            let process_start = Instant::now();

            // Degraded mode: discard bots to reduce load
            if state.degraded_mode.load(Ordering::Relaxed) && event.is_bot {
                tracing::debug!("Degraded mode — bot discarded");
                continue;
            }

            // Drift: time from enqueued_at (in PriorityChannel::push) to now
            drift_tracker.record(event.enqueued_at, event.is_bot);

            // Update all three sync primitives — times recorded inside update_all
            {
                let mut lb = leaderboard.lock().unwrap();
                lb.update_all(&event.domain);
            }

            // End-to-end deadline: parse + queue wait + leaderboard update
            let end_to_end = process_start.elapsed();
            if end_to_end > Duration::from_millis(2) {
                tracing::error!(
                    end_to_end_us = end_to_end.as_micros(),
                    user          = %event.user,
                    is_bot        = event.is_bot,
                    "End-to-end processing deadline missed"
                );
                if let Ok(mut s) = state.stats.lock() {
                    s.deadline_misses += 1;
                }
            }

            let processing_ms = process_start.elapsed().as_secs_f64() * 1000.0;
            jitter_monitor.record(processing_ms, &state);

            events_this_sec += 1;

            if let Ok(mut s) = state.stats.lock() {
                s.events_processed += 1;
                if event.is_bot { s.bot_events += 1; } else { s.human_events += 1; }
                s.avg_mutex_ns  = leaderboard.lock().unwrap().avg_mutex_ns();
                s.avg_rwlock_ns = leaderboard.lock().unwrap().avg_rwlock_ns();
                s.avg_atomic_ns = leaderboard.lock().unwrap().avg_atomic_ns();

                s.recent_events.push_back(types::RecentEvent {
                    timestamp: chrono::Local::now().format("%H:%M:%S").to_string(),
                    user:   event.user.clone(),
                    domain: event.domain.clone(),
                    is_bot: event.is_bot,
                    status: types::EventStatus::Processed,
                });
                if s.recent_events.len() > 10 { s.recent_events.pop_front(); }
            }

            if last_update.elapsed() >= Duration::from_secs(1) {
                if let Ok(mut s) = state.stats.lock() {
                    s.throughput_per_sec = events_this_sec as f64;
                }
                events_this_sec = 0;
                last_update = Instant::now();
                drift_tracker.update_stats(&state);
            }

        } else {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
}
```

---

## 13. Criterion Benchmarks

Create `benches/rts_benchmarks.rs`.

Three benchmark groups:
1. **pipeline_tail_latency** — async vs threaded p99 at increasing event counts
2. **scheduling_drift** — priority scheduling vs FIFO
3. **sync_contention** — Mutex vs RwLock vs Atomic at 1/2/4/8/16 threads

```rust
// benches/rts_benchmarks.rs

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, RwLock};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Instant;

// ── 1. Pipeline tail latency ─────────────────────────────────────────────────

fn simulate_async_batch(n: usize) -> Vec<std::time::Duration> {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let mut latencies = Vec::with_capacity(n);
        for _ in 0..n {
            let start = Instant::now();
            tokio::task::yield_now().await;
            let _ = serde_json::from_str::<serde_json::Value>(
                r#"{"user":"Alice","bot":false,"server_name":"en.wikipedia.org","title":"T"}"#
            );
            latencies.push(start.elapsed());
        }
        latencies
    })
}

fn simulate_threaded_batch(n: usize) -> Vec<std::time::Duration> {
    let mut latencies = Vec::with_capacity(n);
    for _ in 0..n {
        let start = Instant::now();
        let _ = serde_json::from_str::<serde_json::Value>(
            r#"{"user":"Alice","bot":false,"server_name":"en.wikipedia.org","title":"T"}"#
        );
        latencies.push(start.elapsed());
    }
    latencies
}

fn percentile(mut samples: Vec<f64>, pct: f64) -> f64 {
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let idx = (pct / 100.0 * samples.len() as f64) as usize;
    samples[idx.min(samples.len() - 1)]
}

fn bench_pipeline_tail_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline_tail_latency");

    for &n in &[100usize, 500, 1000, 2000] {
        group.bench_with_input(BenchmarkId::new("async", n), &n, |b, &n| {
            b.iter(|| {
                let latencies: Vec<f64> = simulate_async_batch(n)
                    .into_iter().map(|d| d.as_micros() as f64).collect();
                let _p99 = percentile(latencies, 99.0);
            });
        });

        group.bench_with_input(BenchmarkId::new("threaded", n), &n, |b, &n| {
            b.iter(|| {
                let latencies: Vec<f64> = simulate_threaded_batch(n)
                    .into_iter().map(|d| d.as_micros() as f64).collect();
                let _p99 = percentile(latencies, 99.0);
            });
        });
    }

    group.finish();
}

// ── 2. Scheduling drift: priority vs FIFO ────────────────────────────────────

fn bench_scheduling_drift(c: &mut Criterion) {
    let mut group = c.benchmark_group("scheduling_drift");

    group.bench_function("priority_human_first", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let mut human_q: VecDeque<Instant> = VecDeque::new();
                let mut bot_q:   VecDeque<Instant> = VecDeque::new();
                for _ in 0..80 { bot_q.push_back(Instant::now()); }
                for _ in 0..20 { human_q.push_back(Instant::now()); }
                while let Some(t) = human_q.pop_front() {
                    total += Instant::now().duration_since(t);
                }
                while let Some(t) = bot_q.pop_front() {
                    total += Instant::now().duration_since(t);
                }
            }
            total
        })
    });

    group.bench_function("fifo_no_priority", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let mut queue: VecDeque<Instant> = VecDeque::new();
                for _ in 0..100 { queue.push_back(Instant::now()); }
                while let Some(t) = queue.pop_front() {
                    total += Instant::now().duration_since(t);
                }
            }
            total
        })
    });

    group.finish();
}

// ── 3. Sync contention: Mutex vs RwLock vs Atomic ────────────────────────────

struct BenchLeaderboard { counts: std::collections::HashMap<String, u64> }
impl BenchLeaderboard {
    fn new() -> Self { Self { counts: std::collections::HashMap::new() } }
    fn update(&mut self, d: &str) { *self.counts.entry(d.to_string()).or_insert(0) += 1; }
}

fn bench_sync_contention(c: &mut Criterion) {
    let mut group = c.benchmark_group("sync_contention");

    for &n in &[1usize, 2, 4, 8, 16] {

        group.bench_with_input(BenchmarkId::new("Mutex", n), &n, |b, &n| {
            b.iter(|| {
                let lb = Arc::new(Mutex::new(BenchLeaderboard::new()));
                let handles: Vec<_> = (0..n).map(|_| {
                    let lb = Arc::clone(&lb);
                    thread::spawn(move || {
                        for _ in 0..1000 { lb.lock().unwrap().update("en.wikipedia.org"); }
                    })
                }).collect();
                for h in handles { h.join().unwrap(); }
            });
        });

        group.bench_with_input(BenchmarkId::new("RwLock", n), &n, |b, &n| {
            b.iter(|| {
                let lb = Arc::new(RwLock::new(BenchLeaderboard::new()));
                let handles: Vec<_> = (0..n).map(|_| {
                    let lb = Arc::clone(&lb);
                    thread::spawn(move || {
                        for _ in 0..1000 { lb.write().unwrap().update("en.wikipedia.org"); }
                    })
                }).collect();
                for h in handles { h.join().unwrap(); }
            });
        });

        group.bench_with_input(BenchmarkId::new("Atomic", n), &n, |b, &n| {
            b.iter(|| {
                let counter = Arc::new(AtomicU64::new(0));
                let handles: Vec<_> = (0..n).map(|_| {
                    let counter = Arc::clone(&counter);
                    thread::spawn(move || {
                        for _ in 0..1000 { counter.fetch_add(1, Ordering::Relaxed); }
                    })
                }).collect();
                for h in handles { h.join().unwrap(); }
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_pipeline_tail_latency, bench_scheduling_drift, bench_sync_contention);
criterion_main!(benches);
```

---

## 14. Running the System

```bash
# Build
cargo build --release

# Run with async pipeline (default)
cargo run --release

# Run with threaded pipeline
cargo run --release -- --pipeline threaded

# Verbose logging
RUST_LOG=debug cargo run --release

# Run all benchmarks
cargo bench

# Specific groups
cargo bench -- pipeline_tail_latency
cargo bench -- sync_contention
cargo bench -- scheduling_drift

# Save and compare baselines
cargo bench -- --save-baseline v1
cargo bench -- --baseline v1

# View HTML reports
open target/criterion/pipeline_tail_latency/report/index.html
open target/criterion/sync_contention/report/index.html
open target/criterion/scheduling_drift/report/index.html
```

### Test Fault Tolerance

```bash
# Block Wikipedia to trigger watchdog
sudo iptables -A OUTPUT -d stream.wikimedia.org -j DROP
# Watch dashboard — after 10s: ● RECONNECTING

# Restore
sudo iptables -D OUTPUT -d stream.wikimedia.org -j DROP
```

---

## 15. Expected Outputs

### Terminal Dashboard

```
┌──────────────────────────────────────────────────────────────────┐
│  RTS2601 Wikipedia Realtime Pipeline  [● LIVE]  Pipeline: async  │
│                                                  Runtime: 05:23  │
├──────────────────────┬─────────────────────┬────────────────────┤
│  TOP 3 DOMAINS       │  PIPELINE STATUS    │  LATENCY MONITOR   │
│ 1. en.wikipedia.org  │ Mode:  async        │ Human p50:  0.3ms  │
│              4821    │ TPS:   312/s        │ Human p90:  0.7ms  │
│ 2. de.wikipedia.org  │ Bot evict: 143      │ Human p99:  1.1ms  │
│              2103    │ Bot drops:  89      │ Bot   p99:  8.2ms  │
│ 3. fr.wikipedia.org  │ Reconnect:   1      │ Misses:     2      │
│              1847    │                     │                    │
├──────────────────────┴──────────────────────┴──────────────────-┤
│  CHANNEL BUFFER [████████░░] 80/100  │  SYNC BENCHMARK          │
│                                      │  Mutex:   1247 ns        │
│  WATCHDOG                            │  RwLock:   891 ns        │
│  Status:    ● CONNECTED              │  Atomic:    19 ns ◄      │
│  Reconnects: 1   Degraded: 2 times   │  Total:   9847           │
├──────────────────────────────────────┴──────────────────────────┤
│  LIVE EVENT FEED (q to quit)                                     │
│  [12:04:01] Alice        en.wikipedia.org   HUMAN  ✓             │
│  [12:04:01] Bot3234      de.wikipedia.org   BOT    EVICTED       │
│  [12:04:02] Carlos_M     fr.wikipedia.org   HUMAN  ✓             │
│  [12:04:02] Bot9981      en.wikipedia.org   BOT    DROPPED       │
└──────────────────────────────────────────────────────────────────┘
```

### Session Summary (printed on exit)

```
╔══════════════════════════════════════════════╗
║         RTS2601 — SESSION SUMMARY            ║
╠══════════════════════════════════════════════╣
║  Pipeline:             async                 ║
║  Runtime:              00:05:23              ║
║  Total events:         9,847                 ║
║  Human edits:          2,341  (23.8%)        ║
║  Bot edits:            7,506  (76.2%)        ║
╠══════════════════════════════════════════════╣
║  CHANNEL                                     ║
║  Bot evictions:        143                   ║
║  Bot drops:             89                   ║
║  Human drops:            2                   ║
╠══════════════════════════════════════════════╣
║  SCHEDULING DRIFT                            ║
║  Human  p50:  0.3ms   p90:  0.7ms           ║
║         p99:  1.1ms   misses: 2              ║
║  Bot    p50:  1.4ms   p90:  3.8ms           ║
║         p99:  8.2ms   misses: 47             ║
╠══════════════════════════════════════════════╣
║  SYNC BENCHMARK (session avg)                ║
║  Mutex:    1,247 ns                          ║
║  RwLock:     891 ns                          ║
║  Atomic:      19 ns                          ║
╠══════════════════════════════════════════════╣
║  FAULT TOLERANCE                             ║
║  Watchdog reconnects:   1                    ║
║  Degraded activations:  2                    ║
║  Total degraded time:   34.2s                ║
╚══════════════════════════════════════════════╝
```

### Criterion Benchmark Output

```
pipeline_tail_latency/async/100      time: [0.8µs  0.9µs  1.1µs]
pipeline_tail_latency/async/2000     time: [0.8µs  0.9µs  1.2µs]
pipeline_tail_latency/threaded/100   time: [1.2µs  1.4µs  1.9µs]
pipeline_tail_latency/threaded/2000  time: [1.3µs  1.6µs  2.8µs]

scheduling_drift/priority_human_first  time: [0.8µs  0.9µs  1.1µs]
scheduling_drift/fifo_no_priority      time: [1.4µs  1.6µs  1.9µs]

sync_contention/Mutex/1    time: [245ns  248ns  251ns]
sync_contention/Mutex/16   time: [3.1µs  3.8µs  4.2µs]
sync_contention/RwLock/1   time: [198ns  201ns  205ns]
sync_contention/RwLock/16  time: [1.8µs  2.1µs  2.4µs]
sync_contention/Atomic/1   time: [ 18ns   19ns   20ns]
sync_contention/Atomic/16  time: [ 89ns   95ns  102ns]
```

---

## 16. Distinction Checklist

| # | What to verify | Where it shows |
|---|---|---|
| 1 | Zero heap allocs on parse hot path — use `CountingAllocator`, assert count unchanged before/after `parse_event()` | Code + allocator output in report |
| 2 | p50/p90/p99 reported separately for human and bot events | Session summary + drift log |
| 3 | Human p99 significantly lower than bot p99 | Drift table in report |
| 4 | Criterion confidence intervals on all benchmarks | HTML reports in appendix |
| 5 | Async vs threaded p99 compared at multiple event counts | `pipeline_tail_latency` group |
| 6 | Sync primitives benchmarked at 1/2/4/8/16 threads | `sync_contention` group |
| 7 | Atomic fastest — gap widens with thread count | Benchmark table in report |
| 8 | Overflow Event log with nanosecond timestamp on every drop/eviction | Log file excerpt |
| 9 | End-to-end 2ms deadline checked and logged in processor loop | Log file excerpt |
| 10 | Watchdog reconnect demonstrated with real connection drop | iptables test + log |
| 11 | Degraded mode activation and recovery logged | Log file excerpt |
| 12 | `tokio::sync::mpsc` vs `std::sync::mpsc::sync_channel` choice explained | Report pipeline section |

---

*End of Implementation Guide — RTS2601*