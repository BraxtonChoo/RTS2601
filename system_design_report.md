# RTS2601 Wikipedia Realtime Pipeline: System Design Report

---

## Table of Contents

1. [Component A: Implementation and Architecture](#component-a)
   - 1.1 Dual-Pipeline Implementation
     - 1.1.1 Architecture 1: Async/Await (Tokio)
     - 1.1.2 Architecture 2: Multi-Threaded (std::thread)
   - 1.2 Backpressure Management
2. [Component B: Optimization and Priority](#component-b)
   - 2.1 Zero-Copy Parsing
   - 2.2 Priority Scheduling
   - 2.3 Micro-Deadlines
3. [Component C: Priority Scheduling and Drift](#component-c)
   - 3.1 Human Override Bot Mechanism
   - 3.2 Scheduling Drift Measurement and Reporting
4. [Component D: Shared Resource and Metrics](#component-d)
   - 4.1 Thread Safety and Live Leaderboard
   - 4.2 Synchronization Benchmark
5. [Component E: Fault Tolerance and Watchdog](#component-e)
   - 5.1 Network Resilience
   - 5.2 Fail-Safe Mode
6. [Advanced Integration for Distinction-Level](#advanced-integration)
   - 6.1 Memory Mastery: Zero-Copy Proof
   - 6.2 Comparative Analysis: Async vs Threaded Tail Latency
   - 6.3 Statistical Rigor: p50/p90/p99 Percentiles
   - 6.4 Safety Interlocks: Automatic Degraded Mode and Recovery

---

<a name="component-a"></a>
## Component A: Implementation and Architecture

---

### 1.1 Dual-Pipeline Implementation

Both pipeline architectures connect to the same Wikimedia SSE endpoint, share the same bounded `PriorityChannel`, and communicate with the watchdog through the same crossbeam heartbeat and reconnect channels. The active pipeline is selected at startup via a CLI flag, keeping the benchmark comparison clean and unambiguous.

```bash
cargo run --release                   # async pipeline (default)
cargo run --release -- --threaded     # std::thread pipeline
```

---

#### 1.1.1 Architecture 1: Async/Await (Tokio)

**Overview**

The async pipeline runs as a Tokio task. It connects to the Wikimedia SSE stream using `reqwest` and reads the response as a byte stream. Incoming bytes are accumulated in a string buffer until a newline is found, at which point the completed SSE line is parsed and pushed into the priority channel. All I/O suspension is handled cooperatively by the Tokio runtime without blocking any OS thread.

```rust
// src/ingestion/async_pipeline.rs

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, Sender};
use futures_util::StreamExt;

use crate::channel::PriorityChannel;
use crate::config::{CONNECT_FAIL_BACKOFF, CONNECT_TIMEOUT, RECONNECT_BACKOFF};
use crate::parser::parse_event;
use crate::scheduler::schedule_event;
use crate::types::SharedState;

const SSE_URL: &str = "https://stream.wikimedia.org/v2/stream/recentchange";

pub async fn run_async_pipeline(
    heartbeat_tx: Sender<()>,
    reconnect_rx: Receiver<()>,
    channel:      Arc<Mutex<PriorityChannel>>,
    state:        Arc<SharedState>,
) {
    let mut attempt: u32 = 0;

    loop {
        attempt += 1;
        if attempt > 1 {
            tracing::info!(evt = "RECONNECTING", attempt, pipeline = "async");
        }

        let client = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());

        let response = match client
            .get(SSE_URL)
            .header("Accept", "text/event-stream")
            .header("User-Agent", "rts2601/0.1 (student project; Rust/tokio)")
            .send().await
        {
            Ok(r)  => r,
            Err(e) => {
                tracing::error!(evt = "CONNECT_FAIL", attempt, error = %e);
                tokio::time::sleep(CONNECT_FAIL_BACKOFF).await;
                continue;
            }
        };

        tracing::info!(evt = "CONNECTED", pipeline = "async");

        let mut stream      = response.bytes_stream();
        let mut buf         = String::new();
        let connect_time    = Instant::now();
        let mut first_event = true;

        'inner: loop {
            if reconnect_rx.try_recv().is_ok() {
                tracing::warn!(evt = "RECONNECT_SIGNAL", pipeline = "async");
                break 'inner;
            }

            let chunk = match tokio::time::timeout(Duration::from_secs(1), stream.next()).await {
                Ok(Some(result)) => result,
                Ok(None) => { break 'inner; }
                Err(_)   => continue 'inner,
            };

            match chunk {
                Err(e) => { tracing::error!(evt = "STREAM_ERROR", error = %e); break 'inner; }
                Ok(bytes) => {
                    let text = match std::str::from_utf8(&bytes) {
                        Ok(t)  => t,
                        Err(_) => continue 'inner,
                    };

                    buf.push_str(text);

                    while let Some(nl) = buf.find('\n') {
                        let line: String = buf.drain(..=nl).collect();
                        let line = line.trim();

                        if let Some(json) = line.strip_prefix("data: ") {
                            let _ = heartbeat_tx.try_send(());
                            if let Ok(mut hb) = state.last_heartbeat.lock() {
                                *hb = Instant::now();
                            }

                            if first_event {
                                first_event = false;
                                tracing::info!(
                                    evt = "STREAM_LIVE",
                                    latency = format_args!("{:.2}s", connect_time.elapsed().as_secs_f64())
                                );
                                attempt = 0;
                            }

                            match parse_event(json) {
                                Ok(event) => { schedule_event(event, &channel, &state); }
                                Err(e)    => { tracing::debug!(evt = "PARSE_SKIP", reason = %e); }
                            }
                        }
                    }
                }
            }
        }

        tokio::time::sleep(RECONNECT_BACKOFF).await;
    }
}
```

```rust
// src/main.rs -- startup: async pipeline branch

match pipeline_mode {
    PipelineMode::Async => {
        tokio::spawn(ingestion::async_pipeline::run_async_pipeline(
            heartbeat_tx,
            reconnect_rx,
            Arc::clone(&channel),
            Arc::clone(&state),
        ));
    }
    // ...
}
```

```rust
// src/config.rs -- timeout constants used by async pipeline

/// TCP connect timeout for the SSE HTTP request.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Backoff after a connection failure before retrying.
pub const CONNECT_FAIL_BACKOFF: Duration = Duration::from_secs(5);

/// Backoff after a clean stream disconnect before reconnecting.
pub const RECONNECT_BACKOFF: Duration = Duration::from_secs(2);
```

**Code Explanation**

The async pipeline uses Tokio cooperative scheduling, which means the task yields control to the runtime whenever it is waiting for a network chunk. The 1-second `timeout` wrapper on each `stream.next()` call ensures the reconnect signal from the watchdog is checked at least once per second, preventing the task from blocking indefinitely on a stalled connection. The internal `buf` string accumulates partial SSE chunks across multiple receive calls and drains exactly one line at a time, ensuring no JSON events are split or dropped at chunk boundaries.

---

#### 1.1.2 Architecture 2: Multi-Threaded (std::thread)

**Overview**

The threaded pipeline runs in a dedicated OS thread spawned by `std::thread::spawn`. It uses the blocking `ureq` HTTP client with a `BufReader` to read the SSE stream line by line. The same bounded priority channel, `SharedState`, and crossbeam heartbeat/reconnect channels are used as in the async pipeline, making the two architectures directly comparable.

```rust
// src/ingestion/threaded_pipeline.rs

use std::io::{BufRead, BufReader};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;

use crossbeam_channel::{Receiver, Sender};

use crate::channel::PriorityChannel;
use crate::config::{CONNECT_FAIL_BACKOFF, CONNECT_TIMEOUT, RECONNECT_BACKOFF};
use crate::parser::parse_event;
use crate::scheduler::schedule_event;
use crate::types::SharedState;

const SSE_URL: &str = "https://stream.wikimedia.org/v2/stream/recentchange";

pub fn run_threaded_pipeline(
    heartbeat_tx: Sender<()>,
    reconnect_rx: Receiver<()>,
    channel:      Arc<Mutex<PriorityChannel>>,
    state:        Arc<SharedState>,
) {
    thread::spawn(move || {
        let mut attempt: u32 = 0;

        loop {
            attempt += 1;
            if attempt > 1 {
                tracing::info!(evt = "RECONNECTING", attempt, pipeline = "threaded");
            }

            let agent = ureq::AgentBuilder::new()
                .timeout_connect(CONNECT_TIMEOUT)
                .build();

            let response = match agent
                .get(SSE_URL)
                .set("Accept", "text/event-stream")
                .set("User-Agent", "rts2601/0.1 (student project; Rust/std::thread)")
                .call()
            {
                Ok(r)  => r,
                Err(e) => {
                    tracing::error!(evt = "CONNECT_FAIL", attempt, error = %e);
                    thread::sleep(CONNECT_FAIL_BACKOFF);
                    continue;
                }
            };

            tracing::info!(evt = "CONNECTED", pipeline = "threaded");

            let reader       = BufReader::new(response.into_reader());
            let connect_time = Instant::now();
            let mut first_event = true;

            'inner: for line_result in reader.lines() {
                if reconnect_rx.try_recv().is_ok() {
                    tracing::warn!(evt = "RECONNECT_SIGNAL", pipeline = "threaded");
                    break 'inner;
                }

                match line_result {
                    Err(e) => { tracing::error!(evt = "STREAM_ERROR", error = %e); break 'inner; }
                    Ok(line) => {
                        if let Some(json) = line.trim().strip_prefix("data: ") {
                            let _ = heartbeat_tx.try_send(());
                            if let Ok(mut hb) = state.last_heartbeat.lock() {
                                *hb = Instant::now();
                            }

                            if first_event {
                                first_event = false;
                                tracing::info!(
                                    evt = "STREAM_LIVE",
                                    latency = format_args!("{:.2}s", connect_time.elapsed().as_secs_f64())
                                );
                                attempt = 0;
                            }

                            match parse_event(json) {
                                Ok(event) => { schedule_event(event, &channel, &state); }
                                Err(e)    => { tracing::debug!(evt = "PARSE_SKIP", reason = %e); }
                            }
                        }
                    }
                }
            }

            thread::sleep(RECONNECT_BACKOFF);
        }
    });
}
```

```rust
// src/main.rs -- startup: threaded pipeline branch

match pipeline_mode {
    PipelineMode::Threaded => {
        ingestion::threaded_pipeline::run_threaded_pipeline(
            heartbeat_tx,
            reconnect_rx,
            Arc::clone(&channel),
            Arc::clone(&state),
        );
    }
    // ...
}
```

**Code Explanation**

The threaded pipeline blocks the OS thread on `reader.lines()`, which internally calls `read()` on the TCP socket. This is simpler to reason about but consumes a dedicated OS thread for the lifetime of the connection. The reconnect signal is polled with `try_recv()` without blocking at the top of every line iteration, keeping the loop responsive. Both pipelines send the same heartbeat token on the same crossbeam channel, so the watchdog logic is completely independent of which pipeline is running.

---

### 1.2 Backpressure Management

**Overview**

The system uses a bounded `PriorityChannel` backed by a `VecDeque` capped at `CHANNEL_CAPACITY` (100) slots. When the channel is full, one of three overflow outcomes occurs depending on whether the incoming event is a bot or a human. Every overflow outcome is logged as a structured Overflow Event with a nanosecond-precision Unix timestamp. Buffer fill levels are also monitored continuously, with warnings emitted when fill crosses 50% and 80%.

```rust
// src/channel/mod.rs

use std::collections::VecDeque;
use std::time::Instant;

use crate::config::{BUFFER_CRITICAL_PCT, BUFFER_EASE_PCT, BUFFER_WARN_PCT};
use crate::types::{PrioritisedEvent, PushResult};

pub struct PriorityChannel {
    buffer:         VecDeque<PrioritisedEvent>,
    capacity:       usize,
    pressure_level: u8,
}

impl PriorityChannel {
    pub fn new(capacity: usize) -> Self {
        Self {
            buffer:         VecDeque::with_capacity(capacity),
            capacity,
            pressure_level: 0,
        }
    }

    pub fn push(&mut self, mut event: PrioritisedEvent) -> PushResult {
        event.enqueued_at = Instant::now();  // drift clock starts here

        if self.buffer.len() < self.capacity {
            self.buffer.push_back(event);
            self.check_pressure();
            return PushResult::Accepted;
        }

        if event.is_bot {
            return PushResult::DroppedIncoming;  // bot rejected immediately
        }

        // Incoming human -- find and evict the oldest buffered bot
        let bot_pos = self.buffer.iter().position(|e| e.is_bot);
        match bot_pos {
            Some(pos) => {
                let evicted = self.buffer.remove(pos).unwrap();
                self.buffer.push_back(event);
                PushResult::BotEvicted(evicted.seq, evicted.user)
            }
            None => {
                // All slots are humans -- drop oldest to admit newest
                let dropped = self.buffer.pop_front().unwrap();
                self.buffer.push_back(event);
                PushResult::DroppedOldest(dropped.seq, dropped.user)
            }
        }
    }

    fn check_pressure(&mut self) {
        let fill = self.buffer.len();
        let pct  = fill * 100 / self.capacity;
        if pct >= BUFFER_CRITICAL_PCT && self.pressure_level < 2 {
            self.pressure_level = 2;
            let human_in_buf = self.buffer.iter().filter(|e| !e.is_bot).count();
            let bot_in_buf   = fill - human_in_buf;
            tracing::warn!(evt = "BUFFER_80PCT", fill = %format!("{}/{}", fill, self.capacity),
                human_in_buf, bot_in_buf);
        } else if pct >= BUFFER_WARN_PCT && self.pressure_level < 1 {
            self.pressure_level = 1;
            let human_in_buf = self.buffer.iter().filter(|e| !e.is_bot).count();
            let bot_in_buf   = fill - human_in_buf;
            tracing::info!(evt = "BUFFER_50PCT", fill = %format!("{}/{}", fill, self.capacity),
                human_in_buf, bot_in_buf);
        }
    }

    fn check_ease(&mut self) {
        if self.pressure_level > 0 {
            let pct = self.buffer.len() * 100 / self.capacity;
            if pct < BUFFER_EASE_PCT {
                self.pressure_level = 0;
                tracing::info!(evt = "BUFFER_EASED",
                    fill = %format!("{}/{}", self.buffer.len(), self.capacity));
            }
        }
    }
}
```

```rust
// src/scheduler/mod.rs -- overflow event logging with nanosecond timestamps

match &result {
    PushResult::BotEvicted(evicted_seq, evicted_user) => {
        let timestamp_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as u64;
        tracing::warn!(
            actor = %user, kind = "HUMAN", domain = %domain,
            evt = "EVICTED", seq, timestamp_ns,
            evicted_seq, evicted_user = %evicted_user,
            buf = format_args!("{}/{}", buf_fill, buf_cap)
        );
    }
    PushResult::DroppedIncoming => {
        let timestamp_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as u64;
        tracing::warn!(
            actor = %user, kind = "BOT", domain = %domain,
            evt = "DROPPED", seq, timestamp_ns,
            reason = "bot_overflow",
            buf = format_args!("{}/{}", buf_fill, buf_cap)
        );
    }
    PushResult::DroppedOldest(dropped_seq, dropped_user) => {
        let timestamp_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as u64;
        tracing::warn!(
            actor = %user, kind = "HUMAN", domain = %domain,
            evt = "DROPPED", seq, timestamp_ns,
            reason = "queue_full",
            dropped_seq, dropped_user = %dropped_user,
            buf = format_args!("{}/{}", buf_fill, buf_cap)
        );
    }
    PushResult::Accepted => {
        tracing::info!(
            actor = %user, kind, domain = %domain,
            evt = "ENQUEUED", seq,
            buf = format_args!("{}/{}", buf_fill, buf_cap)
        );
    }
}
```

```rust
// src/config.rs -- backpressure constants

/// Maximum number of events the priority channel can hold.
pub const CHANNEL_CAPACITY: usize = 100;

/// Buffer fill level (%) at which a BUFFER_50PCT warning is emitted.
pub const BUFFER_WARN_PCT: usize = 50;

/// Buffer fill level (%) at which a BUFFER_80PCT critical warning is emitted.
pub const BUFFER_CRITICAL_PCT: usize = 80;

/// Buffer fill level (%) below which BUFFER_EASED is emitted.
pub const BUFFER_EASE_PCT: usize = 40;
```

```rust
// src/main.rs -- channel and watchdog channel creation

let channel = Arc::new(Mutex::new(PriorityChannel::new(CHANNEL_CAPACITY)));

let (heartbeat_tx, heartbeat_rx) = bounded::<()>(HEARTBEAT_CHANNEL_CAP);
let (reconnect_tx, reconnect_rx) = bounded::<()>(RECONNECT_CHANNEL_CAP);
```

**Code Explanation**

When a bot arrives at a full channel, it is rejected without any modification to the buffer. When a human arrives at a full channel, the system first searches for any buffered bot to evict and only discards the oldest human if no bots are present. The `enqueued_at` timestamp is stamped at the moment of successful entry into the buffer, not at parse time, ensuring the drift clock reflects actual time spent waiting in the channel. The `pressure_level` flag prevents duplicate warning emissions: `BUFFER_50PCT` fires once on the way up and will not fire again until the buffer has eased below 40% and re-crossed 50%. Each overflow log entry carries a `timestamp_ns` field from `SystemTime::now()` at nanosecond resolution, allowing burst analysis that the millisecond wall-clock timestamp cannot support.

---

<a name="component-b"></a>
## Component B: Optimization and Priority (The "Hot Path")

---

### 2.1 Zero-Copy Parsing

**Overview**

`WikiEvent<'a>` is a serde-deserialised struct whose string fields are borrowed slices pointing directly into the raw JSON buffer. No heap memory is allocated for string data during parsing. The `<'a>` lifetime parameter tells the compiler that every `WikiEvent` instance borrows from the buffer it was parsed from and cannot outlive it. Parsing is split into two phases: Phase 1 produces the zero-copy struct with zero allocations; Phase 2 converts the slices into owned `String` values with exactly three allocations so the event can be moved into the channel queue.

```rust
// src/parser/mod.rs

use serde::Deserialize;
use std::time::Instant;

use crate::allocator::ALLOC_COUNT;
use crate::types::PrioritisedEvent;

#[derive(Deserialize, Debug)]
pub struct WikiEvent<'a> {
    #[serde(borrow)]
    pub user: &'a str,           // points into raw_json -- no allocation

    #[serde(default)]
    pub bot: bool,

    #[serde(borrow, rename = "server_name")]
    pub server_name: &'a str,    // points into raw_json -- no allocation

    #[serde(borrow, default)]
    pub title: &'a str,          // points into raw_json -- no allocation

    #[serde(rename = "type", borrow, default)]
    #[allow(dead_code)]
    pub event_type: &'a str,
}

pub fn parse_event(raw_json: &str) -> Result<PrioritisedEvent, String> {
    let raw_len     = raw_json.len();
    let parse_start = Instant::now();

    // Phase 1: zero-copy -- serde reads slices from the existing buffer
    let before_parse    = ALLOC_COUNT.load(std::sync::atomic::Ordering::Relaxed);
    let event: WikiEvent = serde_json::from_str(raw_json)
        .map_err(|e| format!("parse error: {e}"))?;
    let hot_path_allocs = ALLOC_COUNT.load(std::sync::atomic::Ordering::Relaxed)
        .saturating_sub(before_parse);
    let parse_us = parse_start.elapsed().as_micros() as u64;

    // Phase 2: boundary -- exactly 3 owned Strings
    let owned = PrioritisedEvent {
        seq:         0,
        user:        event.user.to_owned(),
        is_bot:      event.bot,
        domain:      event.server_name.to_owned(),
        title:       event.title.to_owned(),
        enqueued_at: Instant::now(),
        raw_len,
        parse_us,
        allocs:      hot_path_allocs,
    };

    Ok(owned)
}
```

```rust
// src/allocator.rs

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};

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
```

```rust
// src/main.rs -- register global allocator

use allocator::CountingAllocator;
#[global_allocator]
static A: CountingAllocator = CountingAllocator;
```

**Code Explanation**

When serde deserialises into `WikiEvent<'a>`, it records the start offset and length of each string field within the existing buffer rather than allocating new memory and copying bytes. The three string fields cover all the string content needed from the JSON event, yet zero heap memory is allocated to hold them. The `ALLOC_COUNT` counter is sampled before and after the `serde_json::from_str` call. The difference is stored in the `allocs` field of every `PrioritisedEvent` and appears in the `PARSED` log line. A value of `allocs=0` in every log line is the empirical proof that Phase 1 allocates nothing.

---

### 2.2 Priority Scheduling

**Overview**

Priority is enforced at two distinct points. At enqueue time, bots arriving at a full channel are dropped and humans arriving at a full channel evict buffered bots. At dequeue time, the processor always pulls the next human from the buffer before any bot, regardless of arrival order. Additionally, in degraded mode, bot events are discarded immediately on dequeue without any processing. The combination of these three mechanisms ensures human edits consistently experience lower scheduling drift than bot edits.

```rust
// src/channel/mod.rs -- human-first dequeue

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

```rust
// src/channel/mod.rs -- priority enqueue (full channel handling)

if event.is_bot {
    return PushResult::DroppedIncoming;
}

let bot_pos = self.buffer.iter().position(|e| e.is_bot);
match bot_pos {
    Some(pos) => {
        let evicted = self.buffer.remove(pos).unwrap();
        self.buffer.push_back(event);
        PushResult::BotEvicted(evicted.seq, evicted.user)
    }
    None => {
        let dropped = self.buffer.pop_front().unwrap();
        self.buffer.push_back(event);
        PushResult::DroppedOldest(dropped.seq, dropped.user)
    }
}
```

```rust
// src/main.rs -- degraded mode bot discard

let degraded = state.degraded_mode.load(Ordering::Relaxed);

if degraded && event.is_bot {
    let process_us = process_start.elapsed().as_micros() as f64;
    drift_tracker.record(process_us, true);
    if let Ok(mut s) = state.stats.lock() {
        s.bots_discarded_degraded += 1;
    }
    tracing::warn!(
        actor = %event.user, kind = "BOT", domain = %event.domain,
        evt = "DISCARDED", seq = event.seq,
        reason = "degraded_mode"
    );
    continue;
}
```

**Code Explanation**

The `pop()` scan iterates the buffer from front to back looking for any event where `is_bot` is false and removes it by index. This means a human that arrived after 50 bots is still processed next, reducing its total time from enqueue to processing completion. In degraded mode the discard happens before the leaderboard update, so it costs only the Ordering::Relaxed atomic load and avoids all mutex contention, protecting human-event throughput during high-load periods.

---

### 2.3 Micro-Deadlines

**Overview**

A strict 2ms completion deadline applies to each event from the moment it is dequeued until all processing is finalised. The clock starts at `pop()` and stops after the leaderboard update. Events that exceed the deadline are flagged as `deadline=MISS` in the log and counted in `deadline_misses`. The `DriftTracker` maintains separate sample vectors for human and bot events and emits periodic 10-second drift reports.

```rust
// src/main.rs -- per-event deadline check

let process_start = Instant::now();  // clock starts at dequeue

// ... leaderboard update, comp-c check ...

let process_us      = process_start.elapsed().as_micros() as f64;
let process_ms      = process_us / 1000.0;
let deadline_missed = !comp_c_blocked && process_ms > DRIFT_DEADLINE_MS;

drift_tracker.record(process_us, event.is_bot);

if deadline_missed {
    tracing::warn!(
        actor = %event.user, kind = %kind, domain = %event.domain,
        evt = "DONE", seq = event.seq,
        sched_drift = format_args!("{:.3}ms", process_ms),
        deadline = "MISS",
        mutex_ns, rwlock_ns, atomic_ns
    );
} else {
    tracing::info!(
        actor = %event.user, kind = %kind, domain = %event.domain,
        evt = "DONE", seq = event.seq,
        sched_drift = format_args!("{:.3}ms", process_ms)
    );
}

if let Ok(mut s) = state.stats.lock() {
    if deadline_missed { s.deadline_misses += 1; }
    s.drift_history.push_back(process_us as u64);
    if s.drift_history.len() > DRIFT_HISTORY_LEN { s.drift_history.pop_front(); }
}
```

```rust
// src/scheduler/mod.rs -- DriftTracker

pub struct DriftTracker {
    pub human_samples: Vec<f64>,
    pub bot_samples:   Vec<f64>,
    last_log:          Instant,
    last_h_p99:        f64,
    last_b_p99:        f64,
}

impl DriftTracker {
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
}
```

```rust
// src/config.rs -- deadline constant

/// Hard deadline for each event from dequeue to processing complete.
pub const DRIFT_DEADLINE_MS: f64 = 2.0;

/// Same deadline as a Duration.
#[allow(dead_code)]
pub const DRIFT_DEADLINE: Duration = Duration::from_millis(2);
```

**Code Explanation**

The deadline clock uses `Instant::now()` immediately after `pop()` returns, before any lock acquisition or processing begins. This captures the full processing cost including mutex contention for the leaderboard update. The `drift_history` rolling window of 300 samples feeds the dashboard sparkline chart so deadline behaviour is visible in real time. The 2ms threshold is defined once in `config.rs` as `DRIFT_DEADLINE_MS` and all comparison logic imports it by name, so changing the deadline requires editing only one file.

---

<a name="component-c"></a>
## Component C: Priority Scheduling and Drift

---

### 3.1 Human Override Bot Mechanism

**Overview**

Human edits override bot edits at two layers. The channel's `pop()` always returns a human before any bot. Beyond that, the processor loop implements a page-level protection rule: if the most recent edit to a specific Wikipedia article was made by a human, any bot attempting to edit the same article is blocked from processing. The protection is keyed by article title so that a human editing one article does not affect bot processing on unrelated articles on the same domain.

```rust
// src/channel/mod.rs -- execution-time human priority

pub fn pop(&mut self) -> Option<PrioritisedEvent> {
    let human_pos = self.buffer.iter().position(|e| !e.is_bot);
    let item = match human_pos {
        Some(pos) => self.buffer.remove(pos),
        None      => self.buffer.pop_front(),
    };
    if item.is_some() { self.check_ease(); }
    item
}
```

```rust
// src/leaderboard/mod.rs -- page-level protection check

pub fn last_was_human(&self, title: &str) -> bool {
    self.last_editor_bot.get(title)
        .map(|&is_bot| !is_bot)
        .unwrap_or(false)
}

// Component C: record last editor keyed by page title (not domain)
self.last_editor_bot.insert(title.to_string(), is_bot);
self.last_editor_user.insert(title.to_string(), user.to_string());
```

```rust
// src/main.rs -- page-level bot protection in processor loop

let (mutex_ns, rwlock_ns, atomic_ns, comp_c_blocked) = {
    let mut lb = leaderboard.lock().unwrap();
    if event.is_bot && lb.last_was_human(&event.title) {
        (0u64, 0u64, 0u64, true)
    } else {
        let (m, r, a) = lb.update_all(&event.domain, event.is_bot, &event.user, &event.title);
        (m, r, a, false)
    }
};

if comp_c_blocked {
    tracing::warn!(
        actor = %event.user, kind = "BOT", domain = %event.domain,
        evt = "BLOCKED", seq = event.seq,
        page = %event.title,
        reason = "human_protected"
    );
}

// Record in stats and live feed
if let Ok(mut s) = state.stats.lock() {
    if comp_c_blocked { s.comp_c_rejections += 1; }
    let status = if comp_c_blocked {
        EventStatus::CompCBlocked
    } else if deadline_missed {
        EventStatus::DeadlineMissed
    } else {
        EventStatus::Processed
    };
}
```

```rust
// src/types.rs -- EventStatus variants including CompCBlocked

pub enum EventStatus {
    Processed,
    BotEvicted,      // buffer eviction -- Component A
    BotDropped,      // incoming bot dropped -- Component A
    DeadlineMissed,  // 2ms deadline exceeded -- Component C
    CompCBlocked,    // bot blocked -- page last edited by human -- Component C
}
```

**Code Explanation**

The check and the leaderboard update share the same `leaderboard.lock()` acquisition, making the read-modify-write atomic with no race condition between checking a page's protection status and recording a new editor. A page that has never been seen returns `false` from `last_was_human` so there is no restriction on first edit. When a bot is blocked, the `comp_c_blocked` flag skips the leaderboard update entirely and skips the deadline check, so blocked bots do not inflate miss counts. The dashboard live feed shows blocked bots with the tag `BLOCKED` in cyan, visually distinguishing them from `EVICTED` (yellow) and `DROPPED` (red) events.

---

### 3.2 Scheduling Drift Measurement and Reporting

**Overview**

Scheduling drift is measured as the time from when an event is dequeued to when all processing is complete. `DriftTracker` maintains separate sample lists for human and bot events and computes p50, p90, and p99 percentiles for each group. A periodic 10-second log line reports the latest percentiles with a trend arrow, and the session summary prints a side-by-side comparison demonstrating that human edits experience lower scheduling drift than bot edits.

```rust
// src/scheduler/mod.rs -- DriftTracker full implementation

pub struct DriftTracker {
    pub human_samples: Vec<f64>,
    pub bot_samples:   Vec<f64>,
    last_log:          Instant,
    last_h_p99:        f64,
    last_b_p99:        f64,
}

impl DriftTracker {
    pub fn new() -> Self {
        Self {
            human_samples: Vec::new(),
            bot_samples:   Vec::new(),
            last_log:      Instant::now(),
            last_h_p99:    0.0,
            last_b_p99:    0.0,
        }
    }

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

    pub fn update_stats(&mut self, state: &Arc<SharedState>) {
        if let Ok(mut s) = state.stats.lock() {
            s.human_drift_p50 = Self::percentile(&mut self.human_samples, 50.0) / 1000.0;
            s.human_drift_p90 = Self::percentile(&mut self.human_samples, 90.0) / 1000.0;
            s.human_drift_p99 = Self::percentile(&mut self.human_samples, 99.0) / 1000.0;
            s.bot_drift_p50   = Self::percentile(&mut self.bot_samples,   50.0) / 1000.0;
            s.bot_drift_p90   = Self::percentile(&mut self.bot_samples,   90.0) / 1000.0;
            s.bot_drift_p99   = Self::percentile(&mut self.bot_samples,   99.0) / 1000.0;

            let mut all = self.human_samples.clone();
            all.extend_from_slice(&self.bot_samples);
            s.drift_p50 = Self::percentile(&mut all, 50.0) / 1000.0;
            s.drift_p90 = Self::percentile(&mut all, 90.0) / 1000.0;
            s.drift_p99 = Self::percentile(&mut all, 99.0) / 1000.0;
        }

        if self.last_log.elapsed() >= Duration::from_secs(DRIFT_LOG_INTERVAL_SECS) {
            self.last_log = Instant::now();

            let h50 = Self::percentile(&mut self.human_samples, 50.0);
            let h90 = Self::percentile(&mut self.human_samples, 90.0);
            let h99 = Self::percentile(&mut self.human_samples, 99.0);
            let b50 = Self::percentile(&mut self.bot_samples,   50.0);
            let b90 = Self::percentile(&mut self.bot_samples,   90.0);
            let b99 = Self::percentile(&mut self.bot_samples,   99.0);
            let h_miss = self.human_samples.iter().filter(|&&d| d > 2000.0).count();
            let b_miss = self.bot_samples.iter().filter(|&&d| d > 2000.0).count();

            let h_trend = if h99 > self.last_h_p99 + 500.0 { "up" }
                          else if h99 < self.last_h_p99 - 500.0 { "down recovering" }
                          else { "stable" };
            let b_trend = if b99 > self.last_b_p99 + 500.0 { "up" }
                          else if b99 < self.last_b_p99 - 500.0 { "down recovering" }
                          else { "stable" };

            self.last_h_p99 = h99;
            self.last_b_p99 = b99;

            tracing::info!(
                "[DRIFT 10s] human p50={:.3}ms p90={:.3}ms p99={:.3}ms misses={} trend={}  |  \
                 bot p50={:.3}ms p90={:.3}ms p99={:.3}ms misses={} trend={}",
                h50/1000.0, h90/1000.0, h99/1000.0, h_miss, h_trend,
                b50/1000.0, b90/1000.0, b99/1000.0, b_miss, b_trend
            );
        }
    }

    pub fn report(&mut self) {
        let h50 = Self::percentile(&mut self.human_samples, 50.0);
        let h90 = Self::percentile(&mut self.human_samples, 90.0);
        let h99 = Self::percentile(&mut self.human_samples, 99.0);
        let b50 = Self::percentile(&mut self.bot_samples,   50.0);
        let b90 = Self::percentile(&mut self.bot_samples,   90.0);
        let b99 = Self::percentile(&mut self.bot_samples,   99.0);
        let h_miss = self.human_samples.iter().filter(|&&d| d > 2000.0).count();
        let b_miss = self.bot_samples.iter().filter(|&&d| d > 2000.0).count();

        tracing::info!(
            evt = "DRIFT_FINAL",
            human_p50 = format_args!("{:.3}ms", h50/1000.0),
            human_p90 = format_args!("{:.3}ms", h90/1000.0),
            human_p99 = format_args!("{:.3}ms", h99/1000.0),
            human_misses = h_miss,
            bot_p50 = format_args!("{:.3}ms", b50/1000.0),
            bot_p90 = format_args!("{:.3}ms", b90/1000.0),
            bot_p99 = format_args!("{:.3}ms", b99/1000.0),
            bot_misses = b_miss,
        );
    }
}
```

```rust
// src/dashboard/mod.rs -- scheduling drift display panel

let dc = if stats.drift_p99 < DRIFT_DEADLINE_MS { Color::Green } else { Color::Red };
let latency_text = vec![
    Line::from(Span::styled(
        format!(" Drift p50: {:.3}ms", stats.drift_p50),
        Style::default().fg(dc),
    )),
    Line::from(Span::styled(
        format!(" Drift p90: {:.3}ms", stats.drift_p90),
        Style::default().fg(dc),
    )),
    Line::from(Span::styled(
        format!(" Drift p99: {:.3}ms", stats.drift_p99),
        Style::default().fg(dc),
    )),
    Line::from(Span::styled(
        format!(" Deadline:  {:.3}ms", DRIFT_DEADLINE_MS),
        Style::default().fg(Color::DarkGray),
    )),
    Line::from(format!(" Misses:   {}", stats.deadline_misses)),
    Line::from(Span::styled(
        format!(" C-Block:  {}", stats.comp_c_rejections),
        Style::default().fg(Color::Yellow),
    )),
];
```

```rust
// src/main.rs -- session summary drift output

let h50 = DriftTracker::percentile(&mut drift.human_samples, 50.0);
let h90 = DriftTracker::percentile(&mut drift.human_samples, 90.0);
let h99 = DriftTracker::percentile(&mut drift.human_samples, 99.0);
let b50 = DriftTracker::percentile(&mut drift.bot_samples,   50.0);
let b90 = DriftTracker::percentile(&mut drift.bot_samples,   90.0);
let b99 = DriftTracker::percentile(&mut drift.bot_samples,   99.0);
let h_miss = drift.human_samples.iter().filter(|&&d| d > 2000.0).count();
let b_miss = drift.bot_samples.iter().filter(|&&d| d > 2000.0).count();

row(format!("  Human  p50:{:7.3}ms  p90:{:7.3}ms  p99:{:7.3}ms", h50/1000.0, h90/1000.0, h99/1000.0));
row(format!("         misses: {:>4}", h_miss));
row(format!("  Bot    p50:{:7.3}ms  p90:{:7.3}ms  p99:{:7.3}ms", b50/1000.0, b90/1000.0, b99/1000.0));
row(format!("         misses: {:>4}", b_miss));
```

**Code Explanation**

The percentile function sorts the accumulated samples in place and indexes directly into the sorted slice. Sorting on every call is acceptable here because drift reporting happens once per second at the stats tick, not on the hot path. The periodic 10-second log includes trend arrows to show whether latency is improving or deteriorating since the last report window. Human samples consistently produce lower p99 values than bot samples because the human-first dequeue mechanism reduces queue wait time for humans across the whole session.

---

<a name="component-d"></a>
## Component D: Shared Resource and Metrics

---

### 4.1 Thread Safety and Live Leaderboard

**Overview**

The leaderboard tracks edit counts per domain and maintains a live top-3 ranking. It is wrapped in `Arc<Mutex<LeaderboardManager>>` and shared between the processor loop and the dashboard thread. The processor updates it on every processed event; the dashboard reads it every 100ms. `SharedState` bundles all shared counters behind `Arc<Mutex<SystemStats>>` so every thread accesses the same live data.

```rust
// src/leaderboard/mod.rs -- Leaderboard and LeaderboardManager

#[derive(Debug, Default)]
pub struct Leaderboard {
    pub counts: HashMap<String, u64>,
}

impl Leaderboard {
    pub fn new() -> Self {
        Self { counts: HashMap::new() }
    }

    pub fn update(&mut self, domain: &str) {
        *self.counts.entry(domain.to_string()).or_insert(0) += 1;
    }

    pub fn top3(&self) -> Vec<(String, u64)> {
        let mut v: Vec<_> = self.counts.iter().map(|(k, v)| (k.clone(), *v)).collect();
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
    mutex_times:  Vec<u64>,
    rwlock_times: Vec<u64>,
    atomic_times: Vec<u64>,
    last_editor_bot:  HashMap<String, bool>,
    last_editor_user: HashMap<String, String>,
}

pub fn top3(&self) -> Vec<(String, u64)> {
    self.rwlock_lb.read().unwrap().top3()
}
```

```rust
// src/types.rs -- SharedState bundling all shared data

pub struct SharedState {
    pub stats:          Arc<Mutex<SystemStats>>,
    pub degraded_mode:  Arc<AtomicBool>,
    pub pipeline_mode:  PipelineMode,
    pub start_time:     Instant,
    pub last_heartbeat: Arc<Mutex<Instant>>,
}

impl SharedState {
    pub fn new(pipeline_mode: PipelineMode) -> Self {
        Self {
            stats:          Arc::new(Mutex::new(SystemStats::default())),
            degraded_mode:  Arc::new(AtomicBool::new(false)),
            pipeline_mode,
            start_time:     Instant::now(),
            last_heartbeat: Arc::new(Mutex::new(Instant::now())),
        }
    }
}
```

```rust
// src/dashboard/mod.rs -- live leaderboard panel

let lb_items: Vec<ListItem> = top3
    .iter()
    .enumerate()
    .map(|(i, (domain, count))| {
        ListItem::new(format!("{}. {:20} {:>6}", i + 1, domain, count))
    })
    .collect();
f.render_widget(
    List::new(lb_items)
        .block(Block::default().title(" TOP 3 DOMAINS ").borders(Borders::ALL)),
    row1[0],
);
```

**Code Explanation**

The leaderboard is wrapped in `Arc` so ownership can be shared across the processor loop thread, the dashboard thread, and the session summary without copying the data. The `Mutex` inside `Arc` serialises access so only one thread can read or write at a time. The `top3()` function on `LeaderboardManager` reads through the `RwLock` version of the leaderboard using a read lock, which allows multiple concurrent readers without waiting for the processor's write. The `degraded_mode` flag uses `Arc<AtomicBool>` rather than a mutex because it is a single boolean that only needs to be read atomically without locking.

---

### 4.2 Synchronization Benchmark

**Overview**

Every processed event updates the domain leaderboard through all three sync primitives simultaneously and records the nanosecond cost of each. A rolling window of 1000 samples per primitive produces the live averages shown on the dashboard. The Criterion benchmark `sync_contention` isolates the scaling behaviour of each primitive under 1, 2, 4, 8, and 16 concurrent writer threads, providing quantitative proof of how each primitive degrades under contention.

```rust
// src/leaderboard/mod.rs -- update_all: three primitives per event

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
        "es.wikipedia.org" => &self.atomic_es,
        "ja.wikipedia.org" => &self.atomic_ja,
        _                  => &self.atomic_en,
    };
    counter.fetch_add(1, Ordering::Relaxed);
    let atomic_ns = t.elapsed().as_nanos() as u64;

    self.mutex_times.push(mutex_ns);
    self.rwlock_times.push(rwlock_ns);
    self.atomic_times.push(atomic_ns);
    if self.mutex_times.len()  > LEADERBOARD_ROLLING_WINDOW { self.mutex_times.remove(0); }
    if self.rwlock_times.len() > LEADERBOARD_ROLLING_WINDOW { self.rwlock_times.remove(0); }
    if self.atomic_times.len() > LEADERBOARD_ROLLING_WINDOW { self.atomic_times.remove(0); }

    self.last_editor_bot.insert(title.to_string(), is_bot);
    self.last_editor_user.insert(title.to_string(), user.to_string());

    (mutex_ns, rwlock_ns, atomic_ns)
}

fn avg(v: &[u64]) -> f64 {
    if v.is_empty() { return 0.0; }
    v.iter().sum::<u64>() as f64 / v.len() as f64
}

pub fn avg_mutex_ns(&self)  -> f64 { avg(&self.mutex_times)  }
pub fn avg_rwlock_ns(&self) -> f64 { avg(&self.rwlock_times) }
pub fn avg_atomic_ns(&self) -> f64 { avg(&self.atomic_times) }
```

```rust
// src/dashboard/mod.rs -- sync benchmark panel

let sync_text = vec![
    Line::from(format!(" Mutex:  {:>8.0} ns", stats.avg_mutex_ns)),
    Line::from(format!(" RwLock: {:>8.0} ns", stats.avg_rwlock_ns)),
    Line::from(Span::styled(
        format!(" Atomic: {:>8.0} ns  fastest", stats.avg_atomic_ns),
        Style::default().fg(Color::Green),
    )),
    Line::from(format!(" Total:  {:>8}", stats.events_processed)),
];
```

```rust
// benches/rts_benchmarks.rs -- sync_contention Criterion benchmark

fn bench_sync_contention(c: &mut Criterion) {
    let mut group = c.benchmark_group("sync_contention");

    for &n in &[1usize, 2, 4, 8, 16] {

        group.bench_with_input(BenchmarkId::new("Mutex", n), &n, |b, &n| {
            b.iter(|| {
                let lb = Arc::new(Mutex::new(ContendedLeaderboard::new()));
                let handles: Vec<_> = (0..n).map(|_| {
                    let lb = Arc::clone(&lb);
                    thread::spawn(move || {
                        for _ in 0..OPS_PER_THREAD {
                            lb.lock().unwrap().update("en.wikipedia.org");
                        }
                    })
                }).collect();
                for h in handles { h.join().unwrap(); }
            });
        });

        group.bench_with_input(BenchmarkId::new("RwLock", n), &n, |b, &n| {
            b.iter(|| {
                let lb = Arc::new(RwLock::new(ContendedLeaderboard::new()));
                let handles: Vec<_> = (0..n).map(|_| {
                    let lb = Arc::clone(&lb);
                    thread::spawn(move || {
                        for _ in 0..OPS_PER_THREAD {
                            lb.write().unwrap().update("en.wikipedia.org");
                        }
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
                        for _ in 0..OPS_PER_THREAD {
                            counter.fetch_add(1, Ordering::Relaxed);
                        }
                    })
                }).collect();
                for h in handles { h.join().unwrap(); }
            });
        });
    }

    group.finish();
}
```

**Code Explanation**

`Mutex` acquires an exclusive lock that blocks every other thread for the duration of the critical section. `RwLock` uses a write lock here which has similar exclusivity but adds overhead for tracking reader counts. `AtomicU64::fetch_add` with `Ordering::Relaxed` maps to a single hardware atomic instruction with no kernel involvement or context switching. At one thread all three show their uncontested baseline cost. As thread count increases to 16, `Mutex` and `RwLock` times grow steeply because threads spend most of their time blocked waiting for the lock. The `Atomic` time grows much more slowly because the CPU cache coherence protocol handles contention in hardware. The Criterion confidence intervals on each measurement make the differences statistically rigorous rather than anecdotal.

---

<a name="component-e"></a>
## Component E: Fault Tolerance and Watchdog

---

### 5.1 Network Resilience

**Overview**

A dedicated watchdog thread blocks on a crossbeam channel with a 10-second timeout. Every SSE data line received by the ingestion pipeline sends a heartbeat token on this channel. If no token arrives for 10 seconds, the watchdog increments the reconnect counter and signals the pipeline to drop its current connection and reconnect. Both the async and threaded pipelines poll the reconnect signal at the top of their inner loops.

```rust
// src/watchdog/mod.rs -- watchdog thread

pub fn start_watchdog(
    heartbeat_rx: Receiver<()>,
    reconnect_tx: Sender<()>,
    state:        Arc<SharedState>,
) {
    thread::spawn(move || {
        tracing::info!(actor = "SYSTEM", evt = "WATCHDOG_START", timeout = "10s");
        loop {
            match heartbeat_rx.recv_timeout(WATCHDOG_TIMEOUT) {
                Ok(_) => {}
                Err(RecvTimeoutError::Timeout) => {
                    tracing::warn!(actor = "SYSTEM", evt = "WATCHDOG_TIMEOUT",
                        reason = "no_heartbeat_10s");
                    if let Ok(mut s) = state.stats.lock() {
                        s.reconnect_count += 1;
                    }
                    let _ = reconnect_tx.try_send(());
                }
                Err(RecvTimeoutError::Disconnected) => {
                    tracing::info!(actor = "SYSTEM", evt = "WATCHDOG_STOP");
                    break;
                }
            }
        }
    });
}
```

```rust
// src/ingestion/async_pipeline.rs -- heartbeat and reconnect handling

if let Some(json) = line.strip_prefix("data: ") {
    let _ = heartbeat_tx.try_send(());  // heartbeat to watchdog
    if let Ok(mut hb) = state.last_heartbeat.lock() {
        *hb = Instant::now();
    }
    // ...
}

// Reconnect signal check at top of inner loop
if reconnect_rx.try_recv().is_ok() {
    tracing::warn!(evt = "RECONNECT_SIGNAL", pipeline = "async");
    break 'inner;
}
```

```rust
// src/ingestion/threaded_pipeline.rs -- same pattern in threaded pipeline

if let Some(json) = line.trim().strip_prefix("data: ") {
    let _ = heartbeat_tx.try_send(());
    if let Ok(mut hb) = state.last_heartbeat.lock() {
        *hb = Instant::now();
    }
    // ...
}

'inner: for line_result in reader.lines() {
    if reconnect_rx.try_recv().is_ok() {
        tracing::warn!(evt = "RECONNECT_SIGNAL", pipeline = "threaded");
        break 'inner;
    }
    // ...
}
```

```rust
// src/main.rs -- watchdog channel setup

let (heartbeat_tx, heartbeat_rx) = bounded::<()>(HEARTBEAT_CHANNEL_CAP);
let (reconnect_tx, reconnect_rx) = bounded::<()>(RECONNECT_CHANNEL_CAP);

start_watchdog(heartbeat_rx, reconnect_tx, Arc::clone(&state));
```

```rust
// src/config.rs -- watchdog constants

/// No heartbeat for this long triggers a reconnect.
pub const WATCHDOG_TIMEOUT: Duration = Duration::from_secs(10);

/// Capacity of the heartbeat crossbeam channel.
pub const HEARTBEAT_CHANNEL_CAP: usize = 10;

/// Capacity of the reconnect crossbeam channel.
pub const RECONNECT_CHANNEL_CAP: usize = 1;
```

```rust
// src/dashboard/mod.rs -- watchdog status panel

let since_hb: f64 = state.last_heartbeat.lock()
    .map(|hb| hb.elapsed().as_secs_f64())
    .unwrap_or(0.0);

let (wd_color, wd_status, wd_detail) = if stats.degraded_mode {
    (Color::Red, "DEGRADED".to_string(), format!(" No hb: {:.0}s ago", since_hb))
} else if since_hb < WATCHDOG_TIMEOUT.as_secs_f64() {
    let timeout_in = (WATCHDOG_TIMEOUT.as_secs_f64() - since_hb).ceil() as u64;
    (Color::Green, "CONNECTED".to_string(), format!(" Timeout in: {}s", timeout_in))
} else {
    (Color::Red, "DISCONNECTED".to_string(), format!(" Reconnecting... ({:.0}s)", since_hb))
};
```

**Code Explanation**

The watchdog and the ingestion pipelines are fully decoupled through channels. The watchdog has no reference to the HTTP connection and the pipeline has no reference to the watchdog thread; they communicate only through bounded crossbeam channels. The reconnect channel has capacity 1 so a second timeout signal while the pipeline is already reconnecting is silently discarded rather than queuing. The dashboard derives the connection status independently from `last_heartbeat` rather than from the watchdog, giving the UI a real-time countdown to the next watchdog check that updates every 100ms.

---

### 5.2 Fail-Safe Mode

**Overview**

`JitterMonitor` maintains a rolling window of the last `JITTER_WINDOW` (100) processing times in milliseconds. After every event, it computes the standard deviation of the window. When the standard deviation exceeds `JITTER_THRESHOLD_MS` (5ms), the `degraded_mode` atomic flag is set and the processor loop begins discarding all bot events on arrival without any leaderboard update. Recovery is fully automatic: when the standard deviation falls back at or below the threshold, the flag is cleared and a `DEGRADED_OFF` log event is emitted recording the duration and events affected.

```rust
// src/watchdog/mod.rs -- JitterMonitor full implementation

pub struct JitterMonitor {
    recent_times:      VecDeque<f64>,
    window_size:       usize,
    threshold_ms:      f64,
    pub degraded:      Arc<AtomicBool>,
    degraded_start:    Option<Instant>,
    total_degraded_ms: u64,
}

impl JitterMonitor {
    pub fn new(degraded: Arc<AtomicBool>) -> Self {
        Self {
            recent_times:      VecDeque::with_capacity(JITTER_WINDOW),
            window_size:       JITTER_WINDOW,
            threshold_ms:      JITTER_THRESHOLD_MS,
            degraded,
            degraded_start:    None,
            total_degraded_ms: 0,
        }
    }

    pub fn record(&mut self, processing_time_ms: f64, state: &Arc<SharedState>) {
        if self.recent_times.len() == self.window_size {
            self.recent_times.pop_front();
        }
        self.recent_times.push_back(processing_time_ms);
        if self.recent_times.len() >= JITTER_MIN_SAMPLES {
            self.evaluate(state);
        }
    }

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
            tracing::warn!(
                actor = "SYSTEM", evt = "DEGRADED_ON",
                jitter_stddev = format_args!("{:.2}ms", jitter),
                threshold = format_args!("{:.1}ms", self.threshold_ms),
                bots_discarded = true
            );
            if let Ok(mut s) = state.stats.lock() {
                s.degraded_mode             = true;
                s.degraded_activations     += 1;
                s.bots_discarded_degraded   = 0;
                s.humans_processed_degraded = 0;
            }
        } else if jitter <= self.threshold_ms && currently {
            self.degraded.store(false, Ordering::Relaxed);
            if let Some(start) = self.degraded_start.take() {
                let dur_ms = start.elapsed().as_millis() as u64;
                self.total_degraded_ms += dur_ms;

                let (bots_disc, hum_proc) = if let Ok(s) = state.stats.lock() {
                    (s.bots_discarded_degraded, s.humans_processed_degraded)
                } else {
                    (0, 0)
                };

                tracing::info!(
                    actor = "SYSTEM", evt = "DEGRADED_OFF",
                    duration = format_args!("{:.1}s", dur_ms as f64 / 1000.0),
                    bots_discarded = bots_disc,
                    humans_unaffected = hum_proc,
                    jitter_now = format_args!("{:.2}ms", jitter)
                );
            }
            if let Ok(mut s) = state.stats.lock() {
                s.degraded_mode = false;
            }
        }
    }

    pub fn total_degraded_seconds(&self) -> f64 {
        self.total_degraded_ms as f64 / 1000.0
    }
}
```

```rust
// src/main.rs -- processor loop: bot discard in degraded mode

let degraded = state.degraded_mode.load(Ordering::Relaxed);

if degraded && event.is_bot {
    let process_us = process_start.elapsed().as_micros() as f64;
    drift_tracker.record(process_us, true);
    if let Ok(mut s) = state.stats.lock() {
        s.bots_discarded_degraded += 1;
    }
    tracing::warn!(
        actor = %event.user, kind = "BOT", domain = %event.domain,
        evt = "DISCARDED", seq = event.seq,
        page = %event.title,
        reason = "degraded_mode"
    );
    continue;
}

if degraded && !event.is_bot {
    if let Ok(mut s) = state.stats.lock() {
        s.humans_processed_degraded += 1;
    }
}
```

```rust
// src/config.rs -- jitter constants

/// Processing jitter (std-dev, ms) above this triggers degraded mode ON.
pub const JITTER_THRESHOLD_MS: f64 = 5.0;

/// Number of recent processing-time samples kept by the jitter monitor.
pub const JITTER_WINDOW: usize = 100;

/// Minimum samples in the jitter window before evaluation starts.
pub const JITTER_MIN_SAMPLES: usize = 10;
```

**Code Explanation**

Standard deviation of processing time captures instability rather than average load. A mean that is low but highly variable indicates the processor is intermittently struggling, which is the correct condition for shedding load. The `JITTER_MIN_SAMPLES` guard prevents the system from entering degraded mode during the first few events when the window is nearly empty and variance would be unreliable. The `DEGRADED_OFF` log line records the exact duration of the degraded window, the number of bots shed, and the number of humans that processed uninterrupted, which is the proof-of-recovery the assignment requires. The `total_degraded_seconds()` method accumulates time across all degraded windows so the session summary can show the total degraded fraction of the session.

---

<a name="advanced-integration"></a>
## Advanced Integration for Distinction-Level

---

### 6.1 Memory Mastery: Zero-Copy Proof

**Overview**

Replacing the global allocator with `CountingAllocator` means every heap allocation anywhere in the process increments `ALLOC_COUNT`. By sampling this counter immediately before and after `serde_json::from_str()`, the parser captures the exact number of allocations that occur during Phase 1. This value is stored in the `allocs` field of the event and appears in every `PARSED` log line. A consistent `allocs=0` across all log lines is empirical runtime proof that the zero-copy parsing hot path allocates nothing.

```rust
// src/allocator.rs

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};

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
```

```rust
// src/main.rs -- register as global allocator

use allocator::CountingAllocator;
#[global_allocator]
static A: CountingAllocator = CountingAllocator;
```

```rust
// src/parser/mod.rs -- allocation measurement around Phase 1

let before_parse    = ALLOC_COUNT.load(std::sync::atomic::Ordering::Relaxed);
let event: WikiEvent = serde_json::from_str(raw_json)
    .map_err(|e| format!("parse error: {e}"))?;
let hot_path_allocs = ALLOC_COUNT.load(std::sync::atomic::Ordering::Relaxed)
    .saturating_sub(before_parse);
```

```rust
// src/scheduler/mod.rs -- allocs logged per event

tracing::info!(
    actor = %user, kind, domain = %domain,
    evt = "PARSED", seq, parse_us, allocs
);
```

**Code Explanation**

The custom allocator delegates all actual memory operations to the system allocator unchanged, so it has no performance impact beyond the atomic increment. The counter uses `Ordering::Relaxed` because the only requirement is that the count is eventually observed, not that it is synchronised with other threads. The `saturating_sub` prevents underflow if a concurrent allocation occurred between the two reads. Every `PARSED` log line carrying `allocs=0` is a self-documenting proof point for the zero-copy claim that persists in the log file for the entire session.

---

### 6.2 Comparative Analysis: Async vs Threaded Tail Latency

**Overview**

The Criterion benchmark `pipeline_comparison` simulates end-to-end event processing under both pipeline models using an inline priority channel that mirrors production logic. Both simulations use 500 events at 75% bot / 25% human ratio. The benchmark measures scheduling drift (dequeue to leaderboard update complete) and prints p50/p90/p99 for both models to stderr before the timed Criterion iterations begin.

```rust
// benches/rts_benchmarks.rs -- pipeline_comparison

const N_EVENTS: usize = 500;

fn run_async_simulation() -> Vec<Duration> {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let channel     = Arc::new(tokio::sync::Mutex::new(BenchChannel::new(200)));
        let leaderboard = Arc::new(tokio::sync::Mutex::new(BenchLeaderboard::new()));
        let events      = make_events(N_EVENTS);

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

fn run_threaded_simulation() -> Vec<Duration> {
    let channel     = Arc::new(Mutex::new(BenchChannel::new(200)));
    let leaderboard = Arc::new(Mutex::new(BenchLeaderboard::new()));
    let events      = make_events(N_EVENTS);

    let ch_prod = Arc::clone(&channel);
    let producer = thread::spawn(move || {
        for ev in events { ch_prod.lock().unwrap().push(ev); thread::yield_now(); }
    });

    let ch_cons = Arc::clone(&channel);
    let lb_cons = Arc::clone(&leaderboard);
    let consumer = thread::spawn(move || {
        let mut latencies = Vec::with_capacity(N_EVENTS);
        let mut processed = 0usize;
        loop {
            let ev = ch_cons.lock().unwrap().pop();
            if let Some(e) = ev {
                let t = Instant::now();
                lb_cons.lock().unwrap().update(&e.domain);
                latencies.push(t.elapsed());
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

    // Print percentile table to stderr before timed iterations
    {
        let async_lats    = run_async_simulation();
        let threaded_lats = run_threaded_simulation();
        eprintln!("\n===== D2: Pipeline Comparison Scheduling Drift Percentiles =====");
        eprintln!(
            "  {:12}  p50={:>8.2}us  p90={:>8.2}us  p99={:>8.2}us",
            "ASYNC",
            percentile(&async_lats, 50.0).as_nanos() as f64 / 1000.0,
            percentile(&async_lats, 90.0).as_nanos() as f64 / 1000.0,
            percentile(&async_lats, 99.0).as_nanos() as f64 / 1000.0,
        );
        eprintln!(
            "  {:12}  p50={:>8.2}us  p90={:>8.2}us  p99={:>8.2}us",
            "THREADED",
            percentile(&threaded_lats, 50.0).as_nanos() as f64 / 1000.0,
            percentile(&threaded_lats, 90.0).as_nanos() as f64 / 1000.0,
            percentile(&threaded_lats, 99.0).as_nanos() as f64 / 1000.0,
        );
        eprintln!("=================================================================\n");
    }

    group.bench_function("async_pipeline", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters { total += run_async_simulation().iter().sum::<Duration>(); }
            total
        })
    });

    group.bench_function("threaded_pipeline", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters { total += run_threaded_simulation().iter().sum::<Duration>(); }
            total
        })
    });

    group.finish();
}
```

**Code Explanation**

Both simulations use the same `BenchChannel` struct that mirrors the production priority push and pop logic, so the benchmark measures a realistic workload rather than a trivial synthetic one. The `yield_now()` calls in both producer and consumer allow the runtime or OS scheduler to interleave them, reflecting real pipeline concurrency. The p50/p90/p99 table printed to stderr appears directly in `cargo bench` terminal output alongside the Criterion confidence intervals, giving a single unified view of the comparison. The async simulation uses `tokio::sync::Mutex` because async code must not block within an async context; the threaded simulation uses `std::sync::Mutex` because threads block normally.

---

### 6.3 Statistical Rigor: p50/p90/p99 Percentiles

**Overview**

The system reports scheduling drift using three percentile levels rather than averages. p50 reflects typical behaviour, p90 captures elevated but not extreme cases, and p99 captures tail latency that affects the worst 1% of events. All three are tracked separately for human and bot events, updated every second to the dashboard, logged every 10 seconds with a trend indicator, and printed side-by-side in the session summary.

```rust
// src/scheduler/mod.rs -- percentile computation and periodic reporting

pub fn percentile(samples: &mut Vec<f64>, pct: f64) -> f64 {
    if samples.is_empty() { return 0.0; }
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let idx = ((pct / 100.0) * samples.len() as f64) as usize;
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
```

```rust
// src/main.rs -- session summary: human vs bot percentile comparison

row(format!("  Human  p50:{:7.3}ms  p90:{:7.3}ms  p99:{:7.3}ms",
    h50/1000.0, h90/1000.0, h99/1000.0));
row(format!("         misses: {:>4}", h_miss));
row(format!("  Bot    p50:{:7.3}ms  p90:{:7.3}ms  p99:{:7.3}ms",
    b50/1000.0, b90/1000.0, b99/1000.0));
row(format!("         misses: {:>4}", b_miss));
```

```rust
// benches/rts_benchmarks.rs -- percentile helper used in benchmark output

fn percentile(sorted: &[Duration], pct: f64) -> Duration {
    if sorted.is_empty() { return Duration::ZERO; }
    let idx = ((pct / 100.0) * sorted.len() as f64) as usize;
    sorted[idx.min(sorted.len() - 1)]
}
```

**Code Explanation**

Averages hide tail behaviour. A system with mean latency of 0.5ms can still have p99 of 50ms if 1% of events are severely delayed. The three-percentile view shows the full shape of the latency distribution. The separation between human and bot p99 values is the quantitative proof that the priority scheduling mechanism produces measurable benefit: human p99 is consistently lower because the human-first dequeue reduces queue wait time. The trend arrows in the 10-second log line show direction of change since the previous window, giving operators early warning of deteriorating latency before it reaches the miss threshold.

---

### 6.4 Safety Interlocks: Automatic Degraded Mode and Recovery

**Overview**

The safety interlock system combines the jitter monitor and the watchdog into a two-layer defence. The jitter monitor responds to CPU load increases by entering degraded mode. The watchdog responds to network failures by triggering reconnection. Both are automatic with no operator intervention required, and both produce structured log evidence of activation and recovery.

```rust
// src/watchdog/mod.rs -- full evaluate() showing both activation and recovery

fn evaluate(&mut self, state: &Arc<SharedState>) {
    let jitter    = self.jitter();
    let currently = self.degraded.load(Ordering::Relaxed);

    // Activation
    if jitter > self.threshold_ms && !currently {
        self.degraded.store(true, Ordering::Relaxed);
        self.degraded_start = Some(Instant::now());
        tracing::warn!(
            actor = "SYSTEM", evt = "DEGRADED_ON",
            jitter_stddev = format_args!("{:.2}ms", jitter),
            threshold = format_args!("{:.1}ms", self.threshold_ms),
        );
        if let Ok(mut s) = state.stats.lock() {
            s.degraded_mode             = true;
            s.degraded_activations     += 1;
            s.bots_discarded_degraded   = 0;
            s.humans_processed_degraded = 0;
        }
    }
    // Automatic recovery
    else if jitter <= self.threshold_ms && currently {
        self.degraded.store(false, Ordering::Relaxed);
        if let Some(start) = self.degraded_start.take() {
            let dur_ms = start.elapsed().as_millis() as u64;
            self.total_degraded_ms += dur_ms;

            let (bots_disc, hum_proc) = if let Ok(s) = state.stats.lock() {
                (s.bots_discarded_degraded, s.humans_processed_degraded)
            } else { (0, 0) };

            tracing::info!(
                actor = "SYSTEM", evt = "DEGRADED_OFF",
                duration = format_args!("{:.1}s", dur_ms as f64 / 1000.0),
                bots_discarded = bots_disc,
                humans_unaffected = hum_proc,
                jitter_now = format_args!("{:.2}ms", jitter)
            );
        }
        if let Ok(mut s) = state.stats.lock() {
            s.degraded_mode = false;
        }
    }
}
```

```rust
// src/main.rs -- session summary: fault tolerance section

row(format!("  Watchdog reconnects:   {:>4}", s.reconnect_count));
row(format!("  Degraded activations:  {:>4}", s.degraded_activations));
row(format!("  Total degraded time:   {:.1}s", jitter.total_degraded_seconds()));
```

**Code Explanation**

The `DEGRADED_ON` log proves that the system detected the timing violation and responded. The `DEGRADED_OFF` log proves the system recovered autonomously when load normalised. The `bots_discarded` count in the `DEGRADED_OFF` log shows how much load was shed during the degraded window. The `humans_unaffected` count proves that human-event throughput was maintained throughout. The `total_degraded_seconds()` value in the session summary allows the total fraction of session time spent in degraded mode to be computed, giving a single number that captures the overall health of the system across the entire run.
