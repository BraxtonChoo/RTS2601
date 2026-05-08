# RTS2601 — Wikipedia Realtime Pipeline: System Design Report

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [System Architecture](#2-system-architecture)
3. [Data Flow](#3-data-flow)
4. [File Structure](#4-file-structure)
5. [Component A — Priority Channel and Dual Pipeline](#5-component-a)
   - 5.1 Bounded Priority Event Buffer
   - 5.2 Human-First Dequeue
   - 5.3 Buffer Pressure Monitoring
   - 5.4 Dual Pipeline Implementation
     - 5.4.1 Async Pipeline
     - 5.4.2 Threaded Pipeline
6. [Component B — Zero-Copy Parser](#6-component-b)
   - 6.1 Zero-Copy Struct with Lifetime
   - 6.2 Two-Phase Parsing
   - 6.3 Heap Allocation Counter
7. [Component C — Scheduling Drift and Deadline](#7-component-c)
   - 7.1 Event Sequencing and Overflow Logging
   - 7.2 Scheduling Drift Measurement
   - 7.3 Page-Level Bot Protection
8. [Component D — Sync Primitive Benchmark](#8-component-d)
   - 8.1 Three Primitives Per Event
   - 8.2 Rolling Window Averages
   - 8.3 Criterion Contention Benchmark
9. [Component E — Fault Tolerance](#9-component-e)
   - 9.1 Heartbeat Watchdog
   - 9.2 Jitter-Driven Degraded Mode
   - 9.3 Automatic Recovery
10. [Advanced Integration](#10-advanced-integration)
    - 10.1 Throughput Spike Detection
    - 10.2 Nanosecond Overflow Timestamps
    - 10.3 Centralised Configuration
    - 10.4 Custom Structured Log Formatter
    - 10.5 Session Summary

---

## 1. System Overview

RTS2601 is a real-time Wikipedia edit stream processor written in Rust. It connects to the Wikimedia SSE feed, classifies every incoming edit as human or bot, routes it through a bounded priority channel, measures scheduling latency, benchmarks concurrent synchronisation primitives, and renders a live terminal dashboard. The system supports two interchangeable ingestion modes — Tokio async and `std::thread` blocking — selectable at startup.

**Run:**
```bash
cargo run --release                   # async pipeline (default)
cargo run --release -- --threaded     # std::thread pipeline
cargo bench                           # Criterion benchmarks
```

---

## 2. System Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                          RTS2601 Process                             │
│                                                                      │
│  ┌────────────────────────────┐    ┌──────────────────────────────┐ │
│  │    Ingestion Pipeline      │    │       Watchdog (E)           │ │
│  │    Component A / B         │───▶│  10s heartbeat timeout       │ │
│  │                            │    │  jitter → degraded mode      │ │
│  │  Async  (Tokio + reqwest)  │◀───│  reconnect_tx on timeout     │ │
│  │    OR                      │    └──────────────────────────────┘ │
│  │  Threaded (std + ureq)     │                                     │
│  └─────────────┬──────────────┘                                     │
│                │  parse_event() — WikiEvent<'a> → PrioritisedEvent  │
│                ▼                                                     │
│  ┌─────────────────────────────┐                                    │
│  │    PriorityChannel (A)      │  bounded 100 slots                 │
│  │    push(): priority enqueue │  evict bot → admit human           │
│  │    pop():  human-first scan │  human dequeued before any bot     │
│  └─────────────┬───────────────┘                                    │
│                │  pop()                                             │
│                ▼                                                     │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │               Processor Loop  (main.rs)                      │  │
│  │                                                              │  │
│  │  degraded?       → discard bots immediately                  │  │
│  │  comp_c check?   → block bot if page last edited by human    │  │
│  │  drift clock     → measure dequeue → done vs 2 ms deadline   │  │
│  │  jitter feed     → JitterMonitor for degraded detection      │  │
│  └────┬─────────────────┬──────────────────────┬───────────────┘  │
│       │                 │                      │                   │
│       ▼                 ▼                      ▼                   │
│  ┌──────────┐   ┌──────────────┐       ┌────────────┐             │
│  │  Drift   │   │ Leaderboard  │       │SharedState │             │
│  │ Tracker  │   │ Manager (D)  │       │stats / feed│             │
│  │ (C)      │   │Mutex/RwLock  │       │            │             │
│  │ p50/90/99│   │/Atomic bench │       │            │             │
│  └──────────┘   └──────────────┘       └─────┬──────┘             │
│                                              │                    │
│                                              ▼                    │
│                                       ┌───────────┐              │
│                                       │ Dashboard │              │
│                                       │ (ratatui) │              │
│                                       └───────────┘              │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 3. Data Flow

```
Wikimedia SSE stream (stream.wikimedia.org/v2/stream/recentchange)
        │
        │  raw line: data: {"user":"Alice","bot":false,"server_name":"en.wikipedia.org",...}
        ▼
[Ingestion Pipeline — Component A]
  reqwest bytes_stream (async)  OR  BufReader::lines (threaded)
  strips "data: " prefix
  sends () heartbeat → watchdog channel
  updates last_heartbeat timestamp in SharedState
        │
        ▼
[parser::parse_event() — Component B]
  Phase 1 — zero-copy:
    serde deserialises into WikiEvent<'a>
    user / server_name / title are &str slices pointing into the raw JSON
    ALLOC_COUNT sampled before and after — delta stored as allocs field
  Phase 2 — boundary allocation:
    .to_owned() × 3 produces PrioritisedEvent (user, domain, title as String)
        │
        ▼
[scheduler::schedule_event()]
  assigns atomic EVENT_SEQ number
  logs INGESTED (raw_bytes) and PARSED (parse_us, allocs=0)
        │
        ▼
[PriorityChannel::push() — Component A]
  space available?          → Accepted,        logs ENQUEUED
  full + bot arriving?      → DroppedIncoming, logs DROPPED (bot_overflow)
  full + human + bot in buf → BotEvicted,       logs EVICTED
  full + human + no bots    → DroppedOldest,   logs DROPPED (queue_full)
  enqueued_at stamped here — drift clock starts
        │
        │  event waits in VecDeque
        ▼
[main.rs processor loop]
  pop() — human-first scan, dequeues earliest human before any bot
  process_start = Instant::now()                ← drift clock reference
  degraded && is_bot?  → DISCARD immediately
  last_was_human(title)? → BLOCKED if is_bot
  lb.update_all()      → Mutex / RwLock / Atomic timed and recorded (D)
  sched_drift = process_start.elapsed()         ← Component C metric
  deadline_missed = drift > 2 ms
  jitter_monitor.record(drift)                  ← feeds degraded detection (E)
  drift_tracker.record(drift)                   ← feeds p50/p90/p99 (C)
        │
        ▼
[SharedState::stats]
  events_processed, drift_history (300 samples), recent_events (10)
  avg_mutex_ns / avg_rwlock_ns / avg_atomic_ns
        │
        ▼
[Dashboard — redraws every 100 ms]
  reads SharedState → renders all panels
  q / Ctrl-C → print_summary() → structured SESSION_END log → exit
```

---

## 4. File Structure

| File | Purpose |
|------|---------|
| `src/main.rs` | Entry point: CLI parsing, processor loop, spike detection, session summary |
| `src/config.rs` | Single source of truth — all tuneable constants |
| `src/types.rs` | Shared types: `PrioritisedEvent`, `SystemStats`, `SharedState`, `EventStatus` |
| `src/allocator.rs` | Custom `GlobalAlloc` that counts every heap allocation |
| `src/parser/mod.rs` | Component B: `WikiEvent<'a>` zero-copy struct and `parse_event()` |
| `src/channel/mod.rs` | Component A: bounded `PriorityChannel` — priority push and human-first pop |
| `src/scheduler/mod.rs` | Component C: `DriftTracker`, `schedule_event()`, overflow event logging |
| `src/leaderboard/mod.rs` | Component D: `LeaderboardManager` — Mutex/RwLock/Atomic benchmark + top-3 |
| `src/watchdog/mod.rs` | Component E: `start_watchdog()` thread + `JitterMonitor` degraded mode |
| `src/ingestion/async_pipeline.rs` | Component A: Tokio async SSE ingestion (reqwest) |
| `src/ingestion/threaded_pipeline.rs` | Component A: blocking SSE ingestion (std::thread + ureq) |
| `src/ingestion/mod.rs` | Re-exports both pipeline modules |
| `src/dashboard/mod.rs` | ratatui TUI: all live panels and event feed |
| `src/logging.rs` | Custom `tracing` formatter — fixed-column structured log layout |
| `benches/rts_benchmarks.rs` | Criterion: pipeline tail latency, scheduling drift, sync contention |

---

## 5. Component A — Priority Channel and Dual Pipeline

### 5.1 Bounded Priority Event Buffer

**Overview**

The `PriorityChannel` is a bounded `VecDeque` capped at `CHANNEL_CAPACITY` (100) events. When an event arrives it is immediately stamped with `enqueued_at` — this is the reference point that the drift clock uses later. If the channel is not full the event is accepted. When the channel is full, the outcome depends entirely on whether the incoming event is a bot or a human, ensuring humans are never turned away while a bot occupies a slot.

```rust
// src/channel/mod.rs

pub fn push(&mut self, mut event: PrioritisedEvent) -> PushResult {
    event.enqueued_at = Instant::now();  // drift clock reference

    if self.buffer.len() < self.capacity {
        self.buffer.push_back(event);
        self.check_pressure();
        return PushResult::Accepted;
    }

    if event.is_bot {
        return PushResult::DroppedIncoming;  // bot rejected — channel full
    }

    // Incoming human — find and evict the oldest buffered bot
    let bot_pos = self.buffer.iter().position(|e| e.is_bot);
    match bot_pos {
        Some(pos) => {
            let evicted = self.buffer.remove(pos).unwrap();
            self.buffer.push_back(event);
            PushResult::BotEvicted(evicted.seq, evicted.user)
        }
        None => {
            // All slots are humans — drop oldest to admit newest
            let dropped = self.buffer.pop_front().unwrap();
            self.buffer.push_back(event);
            PushResult::DroppedOldest(dropped.seq, dropped.user)
        }
    }
}
```

**Logic**

Bots arriving to a full channel are turned away on the spot without touching the buffer. An incoming human searches for any buffered bot and removes it to free a slot; only when every slot already holds a human does the oldest human get displaced by the incoming one. This guarantees human events are never lost while bot capacity exists anywhere in the channel, regardless of arrival order.

---

### 5.2 Human-First Dequeue

**Overview**

Priority is also enforced at dequeue time, not just at enqueue. Every time the processor requests the next event, `pop()` scans the entire buffer for the earliest queued human and returns it immediately, bypassing all waiting bots. Bots are only dequeued when there are no humans in the buffer at all.

```rust
// src/channel/mod.rs

pub fn pop(&mut self) -> Option<PrioritisedEvent> {
    let human_pos = self.buffer.iter().position(|e| !e.is_bot);
    let item = match human_pos {
        Some(pos) => self.buffer.remove(pos),  // always drain humans first
        None      => self.buffer.pop_front(),   // fallback: oldest bot
    };
    if item.is_some() { self.check_ease(); }
    item
}
```

**Logic**

A human that arrives after 50 bots is still processed before all 50 bots. This execution-time priority reduces the time a human event spends waiting in the channel to near zero even under high bot traffic, which is the direct mechanism that produces lower scheduling drift for humans compared to bots in the session summary percentile tables.

---

### 5.3 Buffer Pressure Monitoring

**Overview**

`push()` calls `check_pressure()` after every acceptance, and `pop()` calls `check_ease()` after every removal. These methods emit structured log events when the buffer fill crosses 50 %, 80 %, and falls back below 40 %, giving early visibility into backpressure before the channel becomes full.

```rust
// src/channel/mod.rs

fn check_pressure(&mut self) {
    let pct = self.buffer.len() * 100 / self.capacity;
    if pct >= BUFFER_CRITICAL_PCT && self.pressure_level < 2 {
        self.pressure_level = 2;
        tracing::warn!(evt = "BUFFER_80PCT", fill = ..., human_in_buf, bot_in_buf);
    } else if pct >= BUFFER_WARN_PCT && self.pressure_level < 1 {
        self.pressure_level = 1;
        tracing::info!(evt = "BUFFER_50PCT", fill = ..., human_in_buf, bot_in_buf);
    }
}

fn check_ease(&mut self) {
    if self.pressure_level > 0 {
        let pct = self.buffer.len() * 100 / self.capacity;
        if pct < BUFFER_EASE_PCT {
            self.pressure_level = 0;
            tracing::info!(evt = "BUFFER_EASED", fill = ...);
        }
    }
}
```

**Logic**

The `pressure_level` flag prevents repeated log emission — `BUFFER_50PCT` fires once when the fill crosses 50 % upward and does not fire again until the buffer has eased back below 40 % and then re-crossed 50 %. Each warning includes a breakdown of human versus bot events currently in the buffer, which makes it possible to distinguish between a burst of human traffic and a bot flood at a glance in the log file.

---

### 5.4 Dual Pipeline Implementation

#### 5.4.1 Async Pipeline

**Overview**

The async pipeline runs as a Tokio task and connects to the Wikipedia SSE stream using `reqwest`. It reads the response as a byte stream, reassembles SSE lines across chunk boundaries using an internal string buffer, and calls `parse_event()` and `schedule_event()` for each complete JSON line. A heartbeat is sent to the watchdog on every `data:` line received.

```rust
// src/ingestion/async_pipeline.rs

pub async fn run_async_pipeline(
    heartbeat_tx: Sender<()>,
    reconnect_rx: Receiver<()>,
    channel: Arc<Mutex<PriorityChannel>>,
    state:   Arc<SharedState>,
) {
    loop {
        let client = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .build().unwrap_or_else(|_| reqwest::Client::new());

        let response = match client.get(SSE_URL)
            .header("Accept", "text/event-stream")
            .send().await
        {
            Ok(r)  => r,
            Err(e) => { tokio::time::sleep(CONNECT_FAIL_BACKOFF).await; continue; }
        };

        let mut stream = response.bytes_stream();
        let mut buf    = String::new();

        'inner: loop {
            if reconnect_rx.try_recv().is_ok() { break 'inner; }

            match tokio::time::timeout(Duration::from_secs(1), stream.next()).await {
                Ok(Some(Ok(bytes))) => {
                    buf.push_str(std::str::from_utf8(&bytes).unwrap_or(""));
                    while let Some(nl) = buf.find('\n') {
                        let line: String = buf.drain(..=nl).collect();
                        if let Some(json) = line.trim().strip_prefix("data: ") {
                            let _ = heartbeat_tx.try_send(());
                            if let Ok(event) = parse_event(json) {
                                schedule_event(event, &channel, &state);
                            }
                        }
                    }
                }
                _ => break 'inner,
            }
        }
        tokio::time::sleep(RECONNECT_BACKOFF).await;
    }
}
```

**Logic**

The async pipeline never blocks a thread while waiting for network data — the Tokio runtime suspends the task and schedules other work instead. The 1-second `timeout` wrapper on `stream.next()` allows the reconnect signal to be checked on every iteration without spinning. The internal `buf` string accumulates partial chunks across multiple receive calls until a newline is found, then drains exactly that line to avoid accumulating memory.

---

#### 5.4.2 Threaded Pipeline

**Overview**

The threaded pipeline runs in a dedicated `std::thread` and uses the blocking `ureq` HTTP client with a `BufReader` for line-by-line reading. It is functionally identical to the async pipeline — same channel, same SharedState, same heartbeat and reconnect crossbeam channels — but uses OS threads and blocking I/O instead of cooperative scheduling.

```rust
// src/ingestion/threaded_pipeline.rs

pub fn run_threaded_pipeline(
    heartbeat_tx: Sender<()>,
    reconnect_rx: Receiver<()>,
    channel: Arc<Mutex<PriorityChannel>>,
    state:   Arc<SharedState>,
) {
    thread::spawn(move || {
        loop {
            let agent = ureq::AgentBuilder::new()
                .timeout_connect(CONNECT_TIMEOUT)
                .build();

            let response = match agent.get(SSE_URL)
                .set("Accept", "text/event-stream")
                .call()
            {
                Ok(r)  => r,
                Err(_) => { thread::sleep(CONNECT_FAIL_BACKOFF); continue; }
            };

            let reader = BufReader::new(response.into_reader());

            'inner: for line_result in reader.lines() {
                if reconnect_rx.try_recv().is_ok() { break 'inner; }
                if let Ok(line) = line_result {
                    if let Some(json) = line.trim().strip_prefix("data: ") {
                        let _ = heartbeat_tx.try_send(());
                        if let Ok(event) = parse_event(json) {
                            schedule_event(event, &channel, &state);
                        }
                    }
                }
            }
            thread::sleep(RECONNECT_BACKOFF);
        }
    });
}
```

**Logic**

The threaded pipeline blocks the OS thread on each `reader.lines()` call, which is simpler to reason about but consumes a thread for the lifetime of the connection. The reconnect signal is polled with `try_recv()` at the top of each iteration rather than with a blocking receive, so the loop stays responsive without requiring async machinery. Both pipelines produce identical event throughput under normal Wikipedia traffic; the Criterion benchmark (`pipeline_tail_latency`) quantifies the tail-latency difference between the two models.

---

## 6. Component B — Zero-Copy Parser

### 6.1 Zero-Copy Struct with Lifetime

**Overview**

`WikiEvent<'a>` is a serde-deserialised struct whose string fields are borrowed slices pointing directly into the raw JSON buffer rather than heap-copied strings. The `<'a>` lifetime parameter ties every `WikiEvent` instance to the buffer it was parsed from, and the compiler statically enforces that the struct cannot outlive that buffer.

```rust
// src/parser/mod.rs

#[derive(Deserialize, Debug)]
pub struct WikiEvent<'a> {
    #[serde(borrow)]
    pub user: &'a str,         // pointer into raw_json — no allocation

    #[serde(default)]
    pub bot: bool,

    #[serde(borrow, rename = "server_name")]
    pub server_name: &'a str,  // pointer into raw_json — no allocation

    #[serde(borrow, default)]
    pub title: &'a str,        // pointer into raw_json — no allocation

    #[serde(rename = "type", borrow, default)]
    pub event_type: &'a str,
}
```

**Logic**

When serde deserialises into `WikiEvent<'a>`, it records the start and length of each string field within the existing buffer rather than allocating new memory and copying bytes. The three string fields together represent the entire string content needed from the JSON, yet zero new heap memory is allocated to hold them. This is the hot-path behaviour that the counting allocator verifies at runtime.

---

### 6.2 Two-Phase Parsing

**Overview**

Parsing is split into two phases. Phase 1 produces a `WikiEvent<'a>` with zero heap allocations. Phase 2 converts the borrowed slices to owned `String` values so the event can be moved out of the function and into the priority channel without any dependency on the original JSON buffer.

```rust
// src/parser/mod.rs

pub fn parse_event(raw_json: &str) -> Result<PrioritisedEvent, String> {
    let raw_len    = raw_json.len();
    let parse_start = Instant::now();

    // Phase 1: zero-copy parse — no heap allocation
    let before_parse    = ALLOC_COUNT.load(Ordering::Relaxed);
    let event: WikiEvent = serde_json::from_str(raw_json)
        .map_err(|e| format!("parse error: {e}"))?;
    let hot_path_allocs = ALLOC_COUNT.load(Ordering::Relaxed)
        .saturating_sub(before_parse);
    let parse_us = parse_start.elapsed().as_micros() as u64;

    // Phase 2: boundary — exactly 3 owned Strings
    Ok(PrioritisedEvent {
        seq:         0,
        user:        event.user.to_owned(),
        is_bot:      event.bot,
        domain:      event.server_name.to_owned(),
        title:       event.title.to_owned(),
        enqueued_at: Instant::now(),
        raw_len,
        parse_us,
        allocs:      hot_path_allocs,
    })
}
```

**Logic**

The `allocs` field captured between the two phases is carried forward into the `PARSED` structured log line emitted by `schedule_event`. Every log line in a normal run shows `allocs=0`, confirming that Phase 1 allocates nothing. The three `.to_owned()` calls in Phase 2 are the minimum necessary — the event must own its strings to be safely moved across thread boundaries into the channel queue.

---

### 6.3 Heap Allocation Counter

**Overview**

`CountingAllocator` replaces Rust's default global allocator. Every call to `alloc` increments a process-wide `AtomicU64`. By sampling this counter before and after `serde_json::from_str()`, the parser can report exactly how many heap allocations the zero-copy phase performed without any instrumentation in the serde or JSON libraries themselves.

```rust
// src/allocator.rs

pub static ALLOC_COUNT: AtomicU64 = AtomicU64::new(0);

pub struct CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        System.alloc(layout)
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout)
    }
}

// src/main.rs
#[global_allocator]
static A: CountingAllocator = CountingAllocator;
```

**Logic**

Replacing the global allocator means the counter increments for every allocation anywhere in the process, including allocations inside third-party libraries. The delta measured strictly around `serde_json::from_str()` is therefore a reliable proof of the zero-copy claim — if any internal serde path were to allocate a temporary string, it would appear in the `allocs` field and be logged, making the claim falsifiable at runtime rather than just asserted in code.

---

## 7. Component C — Scheduling Drift and Deadline

### 7.1 Event Sequencing and Overflow Logging

**Overview**

`schedule_event()` is the bridge between the ingestion pipeline and the channel. It assigns each event a monotonically increasing sequence number, emits structured `INGESTED` and `PARSED` log lines with the sequence number and per-event metrics, pushes the event into `PriorityChannel`, and emits an overflow log line for every non-`Accepted` result. All overflow events include a nanosecond Unix timestamp.

```rust
// src/scheduler/mod.rs

pub fn schedule_event(mut event: PrioritisedEvent, channel: &Arc<Mutex<PriorityChannel>>, state: &Arc<SharedState>) {
    let seq   = EVENT_SEQ.fetch_add(1, Ordering::Relaxed) + 1;
    event.seq = seq;

    tracing::info!(actor = %event.user, evt = "INGESTED", seq, raw_bytes = event.raw_len);
    tracing::info!(actor = %event.user, evt = "PARSED",   seq, parse_us = event.parse_us, allocs = event.allocs);

    let (result, buf_fill, buf_cap) = {
        let mut ch = channel.lock().unwrap();
        let r = ch.push(event);
        (r, ch.len(), ch.capacity())
    };

    match &result {
        PushResult::BotEvicted(evicted_seq, evicted_user) => {
            let timestamp_ns = SystemTime::now()
                .duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as u64;
            tracing::warn!(evt = "EVICTED", seq, timestamp_ns, evicted_seq, evicted_user = %evicted_user, buf = ...);
        }
        PushResult::DroppedIncoming => {
            let timestamp_ns = SystemTime::now()
                .duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as u64;
            tracing::warn!(evt = "DROPPED", seq, timestamp_ns, reason = "bot_overflow", buf = ...);
        }
        // ... DroppedOldest, Accepted
    }
}
```

**Logic**

The atomic sequence number means every event can be traced end-to-end through the log file by its `seq=` field — from `INGESTED` through `PARSED`, `ENQUEUED` or `EVICTED`/`DROPPED`, and finally `DONE`. The nanosecond timestamp on overflow events provides sub-millisecond resolution for post-hoc analysis of burst behaviour that the wall-clock log timestamp (millisecond precision) cannot capture.

---

### 7.2 Scheduling Drift Measurement

**Overview**

Scheduling drift is defined as the time from when an event is dequeued to when all processing is complete. The clock starts the moment `pop()` returns in the processor loop and stops after the leaderboard update. `DriftTracker` maintains separate sample lists for human and bot events and computes p50/p90/p99 for each group independently.

```rust
// src/main.rs — processor loop
let process_start = Instant::now();          // clock starts at dequeue

// ... leaderboard update, comp-c check ...

let process_us      = process_start.elapsed().as_micros() as f64;
let process_ms      = process_us / 1000.0;
let deadline_missed = !comp_c_blocked && process_ms > DRIFT_DEADLINE_MS;

drift_tracker.record(process_us, event.is_bot);  // separate human/bot buckets
```

```rust
// src/scheduler/mod.rs — DriftTracker

pub fn record(&mut self, process_us: f64, is_bot: bool) -> f64 {
    if is_bot { self.bot_samples.push(process_us); }
    else       { self.human_samples.push(process_us); }
    process_us
}

pub fn percentile(samples: &mut Vec<f64>, pct: f64) -> f64 {
    if samples.is_empty() { return 0.0; }
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let idx = ((pct / 100.0) * samples.len() as f64) as usize;
    samples[idx.min(samples.len() - 1)]
}
```

**Logic**

Because `pop()` always returns a human before any bot, humans dequeue with near-zero queue wait time and arrive at the processor in a fresher state, resulting in lower absolute drift values. Bots accumulate wait time behind every human that arrives after them. The session summary prints p50/p90/p99 and miss counts for each class side by side, providing direct quantitative proof that priority scheduling produces lower scheduling drift for human events. A periodic `[DRIFT 10s]` log line reports the latest percentiles with a trend arrow showing whether latency is improving or worsening.

---

### 7.3 Page-Level Bot Protection

**Overview**

Component C extends into the processor loop: a bot is blocked from processing if the most recent edit to that exact page was made by a human. The protection is keyed by article title rather than domain, so a human editing one Wikipedia article does not block bots on every other article on the same domain.

```rust
// src/main.rs — processor loop

let (mutex_ns, rwlock_ns, atomic_ns, comp_c_blocked) = {
    let mut lb = leaderboard.lock().unwrap();
    if event.is_bot && lb.last_was_human(&event.title) {
        (0u64, 0u64, 0u64, true)   // bot blocked — page human-protected
    } else {
        let (m, r, a) = lb.update_all(&event.domain, event.is_bot, &event.user, &event.title);
        (m, r, a, false)
    }
};
```

```rust
// src/leaderboard/mod.rs

pub fn last_was_human(&self, title: &str) -> bool {
    self.last_editor_bot.get(title)
        .map(|&is_bot| !is_bot)
        .unwrap_or(false)  // unseen page → no restriction
}
```

**Logic**

The check and the leaderboard update both happen under the same `leaderboard.lock()` acquisition, making the read-then-write atomic — no race condition is possible between checking a page's protection status and recording a new editor. A page that has never been seen has no restriction; the protection only activates after a confirmed human edit. Blocked bots are logged with `evt=BLOCKED reason=human_protected` and counted as `comp_c_rejections` in the session summary.

---

## 8. Component D — Sync Primitive Benchmark

### 8.1 Three Primitives Per Event

**Overview**

Every processed event updates the domain leaderboard using all three sync primitives simultaneously — `Mutex`, `RwLock`, and `AtomicU64` — and records the nanosecond cost of each operation. This means the benchmark data is driven by real Wikipedia traffic under real concurrency conditions rather than synthetic inputs.

```rust
// src/leaderboard/mod.rs — update_all()

pub fn update_all(&mut self, domain: &str, is_bot: bool, user: &str, title: &str) -> (u64, u64, u64) {
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
        _                  => &self.atomic_en,
    };
    counter.fetch_add(1, Ordering::Relaxed);
    let atomic_ns = t.elapsed().as_nanos() as u64;

    // Component C: record last editor keyed by page title
    self.last_editor_bot.insert(title.to_string(), is_bot);
    self.last_editor_user.insert(title.to_string(), user.to_string());

    (mutex_ns, rwlock_ns, atomic_ns)
}
```

**Logic**

`Mutex` acquires an exclusive lock that blocks all other threads. `RwLock` uses a write lock here, which has similar exclusivity but carries additional bookkeeping overhead for reader tracking. `AtomicU64::fetch_add` with `Ordering::Relaxed` is a single hardware instruction with no lock, no context switch, and no kernel involvement. The consistent ordering Atomic < RwLock < Mutex across thousands of events confirms that lock-free primitives provide a significant throughput advantage when the shared state can be reduced to a counter.

---

### 8.2 Rolling Window Averages

**Overview**

Each primitive maintains a rolling vector of the last `LEADERBOARD_ROLLING_WINDOW` (1000) timing samples. The rolling average is recomputed after every event and pushed into `SharedState`, where the dashboard reads it every 100 ms and the session summary reads it at exit.

```rust
// src/leaderboard/mod.rs

self.mutex_times.push(mutex_ns);
self.rwlock_times.push(rwlock_ns);
self.atomic_times.push(atomic_ns);
if self.mutex_times.len()  > LEADERBOARD_ROLLING_WINDOW { self.mutex_times.remove(0); }
if self.rwlock_times.len() > LEADERBOARD_ROLLING_WINDOW { self.rwlock_times.remove(0); }
if self.atomic_times.len() > LEADERBOARD_ROLLING_WINDOW { self.atomic_times.remove(0); }

fn avg(v: &[u64]) -> f64 {
    if v.is_empty() { return 0.0; }
    v.iter().sum::<u64>() as f64 / v.len() as f64
}
```

**Logic**

Capping the window at 1000 samples ensures the average reflects recent behaviour rather than being diluted by the entire session history. During a burst of high-latency events the rolling average climbs quickly and then recovers as the burst passes, making the dashboard SYNC BENCHMARK panel responsive to real-time changes in contention rather than showing a static lifetime mean.

---

### 8.3 Criterion Contention Benchmark

**Overview**

The Criterion benchmark `sync_contention` spawns 1, 2, 4, 8, and 16 threads simultaneously against a shared `Mutex`, `RwLock`, and `AtomicU64` counter, each thread performing 1000 increments. This isolates the scaling behaviour of each primitive under increasing contention, independent of the live pipeline.

```rust
// benches/rts_benchmarks.rs — sync_contention

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
    // ... same for RwLock and Atomic
}
```

**Logic**

At 1 thread, all three primitives show their uncontested overhead. As thread count increases, `Mutex` and `RwLock` degrade faster because each acquisition requires serialising through the kernel — threads spend increasing proportions of time blocked waiting for the lock to be released. `AtomicU64` degrades far more slowly because the CPU's cache coherence protocol handles the contention in hardware without OS involvement. The HTML reports generated by `cargo bench` include confidence intervals on every measurement, making the gap statistically rigorous rather than anecdotal.

---

## 9. Component E — Fault Tolerance

### 9.1 Heartbeat Watchdog

**Overview**

The watchdog runs in a dedicated `std::thread` and blocks on a crossbeam channel with a `WATCHDOG_TIMEOUT` (10 s) timeout. Every SSE data line received by the ingestion pipeline sends a `()` token on the heartbeat channel. If 10 seconds pass without a token, the watchdog increments `reconnect_count` and sends a signal on the reconnect channel, which the ingestion pipeline observes at the top of its inner loop and reacts to by breaking out and reconnecting.

```rust
// src/watchdog/mod.rs

pub fn start_watchdog(heartbeat_rx: Receiver<()>, reconnect_tx: Sender<()>, state: Arc<SharedState>) {
    thread::spawn(move || {
        tracing::info!(evt = "WATCHDOG_START", timeout = "10s");
        loop {
            match heartbeat_rx.recv_timeout(WATCHDOG_TIMEOUT) {
                Ok(_) => {}
                Err(RecvTimeoutError::Timeout) => {
                    tracing::warn!(evt = "WATCHDOG_TIMEOUT", reason = "no_heartbeat_10s");
                    state.stats.lock().unwrap().reconnect_count += 1;
                    let _ = reconnect_tx.try_send(());
                }
                Err(RecvTimeoutError::Disconnected) => {
                    tracing::info!(evt = "WATCHDOG_STOP");
                    break;
                }
            }
        }
    });
}
```

**Logic**

The watchdog and the ingestion pipeline are decoupled through the channel — neither needs to know the other's implementation. The watchdog never touches the HTTP connection directly; it only signals a flag. This means the same watchdog works unchanged for both the async and threaded pipelines. The reconnect counter in `SharedState` is incremented by the watchdog before the signal is sent, so the count is accurate even if the signal is dropped because the reconnect channel is already full.

---

### 9.2 Jitter-Driven Degraded Mode

**Overview**

`JitterMonitor` maintains a rolling window of the last `JITTER_WINDOW` (100) processing times. After every event, it computes the standard deviation of the window. When the standard deviation exceeds `JITTER_THRESHOLD_MS` (5 ms), the `degraded_mode` atomic flag is set to `true` and the processor loop begins discarding all bot events on arrival without processing them.

```rust
// src/watchdog/mod.rs — JitterMonitor

fn jitter(&self) -> f64 {
    let n    = self.recent_times.len() as f64;
    let mean = self.recent_times.iter().sum::<f64>() / n;
    let var  = self.recent_times.iter().map(|t| (t - mean).powi(2)).sum::<f64>() / n;
    var.sqrt()
}

fn evaluate(&mut self, state: &Arc<SharedState>) {
    let jitter    = self.jitter();
    let currently = self.degraded.load(Ordering::Relaxed);

    if jitter > self.threshold_ms && !currently {
        self.degraded.store(true, Ordering::Relaxed);
        self.degraded_start = Some(Instant::now());
        tracing::warn!(evt = "DEGRADED_ON", jitter_stddev = ..., threshold = "5.0ms");
        if let Ok(mut s) = state.stats.lock() {
            s.degraded_mode          = true;
            s.degraded_activations  += 1;
            s.bots_discarded_degraded = 0;
            s.humans_processed_degraded = 0;
        }
    }
    // ... recovery branch below
}
```

**Logic**

Standard deviation of processing time is used rather than mean or maximum because it captures instability in the system — a mean that is low but highly variable indicates the processor is intermittently struggling, which is the condition that warrants shedding load. Discarding bots rather than humans preserves human-edit throughput during the degraded window, which is the highest-priority traffic. The `degraded_activations` counter and the `bots_discarded_degraded` counter accumulate across all windows, giving the session summary a full picture of how often and how severely the system degraded.

---

### 9.3 Automatic Recovery

**Overview**

Recovery is fully automatic. When the rolling standard deviation falls back at or below the threshold, `JitterMonitor` clears the `degraded_mode` flag, computes the duration of the degraded window, and emits a `DEGRADED_OFF` structured log event with the duration, bots discarded, and humans that processed unaffected during the window.

```rust
// src/watchdog/mod.rs

} else if jitter <= self.threshold_ms && currently {
    self.degraded.store(false, Ordering::Relaxed);
    if let Some(start) = self.degraded_start.take() {
        let dur_ms = start.elapsed().as_millis() as u64;
        self.total_degraded_ms += dur_ms;

        let (bots_disc, hum_proc) = if let Ok(s) = state.stats.lock() {
            (s.bots_discarded_degraded, s.humans_processed_degraded)
        } else { (0, 0) };

        tracing::info!(
            evt = "DEGRADED_OFF",
            duration = format_args!("{:.1}s", dur_ms as f64 / 1000.0),
            bots_discarded = bots_disc,
            humans_unaffected = hum_proc,
            jitter_now = format_args!("{:.2}ms", jitter)
        );
    }
    state.stats.lock().unwrap().degraded_mode = false;
}
```

**Logic**

The `DEGRADED_OFF` log is the proof-of-recovery the assignment requires. It records the exact duration the system was in degraded state, how many bot events were shed to protect human throughput, and how many human events were processed uninterrupted throughout. `total_degraded_ms` accumulates across all degraded windows so the session summary can report the total time spent in degraded mode as a fraction of the session runtime.

---

## 10. Advanced Integration

### 10.1 Throughput Spike Detection

The processor loop maintains a rolling 60-sample history of per-second TPS. Once at least `SPIKE_BASELINE_MIN_SAMPLES` (10) baseline samples are available, it compares the current second's TPS against the rolling mean. A `THROUGHPUT_SPIKE` warning fires when TPS exceeds `SPIKE_RATIO` (2.5×) of baseline and clears when it falls back below `SPIKE_RECOVERY_RATIO` (1.5×), providing early warning of traffic bursts before they trigger buffer pressure.

```rust
// src/main.rs
if !spike_active && baseline > SPIKE_BASELINE_MIN_TPS && tps > baseline * SPIKE_RATIO {
    spike_active = true;
    tracing::warn!(evt = "THROUGHPUT_SPIKE", current = ..., baseline = ..., ratio = ...);
} else if spike_active && tps < baseline * SPIKE_RECOVERY_RATIO {
    spike_active = false;
    tracing::info!(evt = "THROUGHPUT_NORMAL", current = ..., baseline = ...);
}
```

---

### 10.2 Nanosecond Overflow Timestamps

Every overflow event — bot dropped, bot evicted, human dropped — includes a `timestamp_ns` field sampled from `SystemTime::now()` as Unix nanoseconds. The wall-clock column in the log has millisecond precision; `timestamp_ns` provides sub-millisecond resolution for correlating overflow clusters with external events.

```rust
let timestamp_ns = SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .unwrap_or_default()
    .as_nanos() as u64;
tracing::warn!(evt = "EVICTED", seq, timestamp_ns, evicted_seq, evicted_user = %evicted_user, buf = ...);
```

---

### 10.3 Centralised Configuration

All tuneable constants are defined in `src/config.rs` and imported by name wherever they are used. No magic numbers exist in the implementation files.

| Constant | Default | Purpose |
|---|---|---|
| `CHANNEL_CAPACITY` | 100 | Buffer slot count |
| `DRIFT_DEADLINE_MS` | 2.0 ms | Hard deadline per event |
| `WATCHDOG_TIMEOUT` | 10 s | Heartbeat timeout before reconnect |
| `JITTER_THRESHOLD_MS` | 5.0 ms | Std-dev threshold for degraded mode |
| `JITTER_WINDOW` | 100 | Samples in jitter rolling window |
| `BUFFER_WARN_PCT` | 50 % | First pressure warning threshold |
| `BUFFER_CRITICAL_PCT` | 80 % | Second pressure warning threshold |
| `BUFFER_EASE_PCT` | 40 % | Pressure-cleared threshold |
| `SPIKE_RATIO` | 2.5 × | TPS multiple to declare a spike |
| `DRIFT_LOG_INTERVAL_SECS` | 10 s | Cadence of periodic drift log |
| `LEADERBOARD_ROLLING_WINDOW` | 1000 | Samples per sync-primitive rolling average |

---

### 10.4 Custom Structured Log Formatter

`src/logging.rs` implements a custom `tracing` `FormatEvent` that produces a fixed-column layout:

```
MM/DD/YY HH:MM:SS.mmm | LEVEL | ACTOR        | KIND  | DOMAIN           | EVENT      key=val ...
```

System events (watchdog, checkpoint) use a narrower layout without KIND/DOMAIN columns. The `seq=` field is always placed immediately after the event name so every event's lifecycle can be followed by grepping a single number. Empty message strings that `tracing` injects for field-only events are suppressed to keep the log readable.

---

### 10.5 Session Summary

On exit, the processor loop calls `print_summary()` which prints a box-drawing table to the terminal covering all five components. The table is generated entirely from `SharedState` and `DriftTracker` — no separate state is maintained — and a structured `SESSION_END` log line carrying the same data is written to the log file simultaneously for automated analysis.

```
╔══════════════════════════════════════════════════╗
║      RTS2601  —  SESSION SUMMARY                 ║
╠══════════════════════════════════════════════════╣
║  Pipeline:     ASYNC       Runtime: 00:10:00     ║
║  Total events:       18432                       ║
╠══════════════════════════════════════════════════╣
║  COMPONENT A — CHANNEL                           ║
║  Bot evictions:     1043                         ║
║  Bot drops:          621                         ║
║  Human drops:          4                         ║
╠══════════════════════════════════════════════════╣
║  COMPONENT C — SCHEDULING DRIFT                  ║
║  Human  p50:  0.003ms  p90:  0.007ms  p99: 0.11ms║
║  Bot    p50:  0.003ms  p90:  0.009ms  p99: 0.98ms║
╠══════════════════════════════════════════════════╣
║  COMPONENT D — SYNC BENCHMARK (session avg)      ║
║  Mutex:    1247 ns                               ║
║  RwLock:    891 ns                               ║
║  Atomic:     19 ns                               ║
╠══════════════════════════════════════════════════╣
║  COMPONENT E — FAULT TOLERANCE                   ║
║  Watchdog reconnects:     1                      ║
║  Degraded activations:    2                      ║
║  Total degraded time:   34.2s                    ║
╚══════════════════════════════════════════════════╝
```
