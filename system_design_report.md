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

Both pipelines connect to the same Wikipedia SSE endpoint, share the same bounded priority channel, and communicate with the watchdog through the same heartbeat and reconnect channels. The active pipeline is selected with a command-line flag at startup.

```bash
cargo run --release                   # async pipeline (default)
cargo run --release -- --threaded     # std::thread pipeline
```

---

#### 1.1.1 Architecture 1: Async/Await (Tokio)

**Overview**

`run_async_pipeline` connects to Wikipedia's live SSE stream as a Tokio task. Its role is to keep the connection alive, send heartbeat signals so the system knows the stream is active, and handle reconnections automatically when the connection drops or fails.

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

**Explanation**

When the function starts, it builds an HTTP client and attempts a connection to the stream. If the connection fails, it logs the error and waits briefly before retrying. Once connected, incoming data is collected in a buffer and scanned line by line for valid SSE content. Each valid event updates the heartbeat timestamp and is passed to the parser and scheduler. A 1-second timeout on each read ensures the reconnect signal is checked regularly, preventing the task from stalling on a silent connection. When an error or reconnect signal arrives, the loop breaks, pauses briefly, and then restarts.

---

#### 1.1.2 Architecture 2: Multi-Threaded (std::thread)

**Overview**

`run_threaded_pipeline` serves the same role as the async pipeline, connecting to the Wikipedia SSE stream, sending heartbeats, and handling reconnections in the same way, but runs as a dedicated OS thread using blocking I/O instead. The stream is read line by line through a `BufReader`, which makes the control flow straightforward to follow.

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

**Explanation**

Unlike the async version, this pipeline blocks the OS thread on each read. The `BufReader` handles incoming bytes and delivers one complete line at a time, so there is no need to manually manage a buffer or scan for newlines. The reconnect signal is checked at the top of every loop iteration, so the pipeline stays responsive even while waiting for the next line. Both pipelines send heartbeats and handle reconnections in exactly the same way, which keeps the watchdog logic consistent regardless of which pipeline is active.

---

### 1.2 Backpressure Management

**Overview**

`PriorityChannel` is a fixed-size queue that holds up to 100 events. When the queue is full, the outcome depends on whether the incoming event is from a bot or a human. Bots are rejected immediately, while humans are prioritised by evicting a buffered bot if one is available. The channel also monitors how full it is, logging warnings at 50% and 80% capacity.

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

**Explanation**

Each event receives a timestamp the moment it enters the queue, which is used later to measure how long it waited before being processed. When the queue reaches 80% full, a warning logs the exact number of human and bot events currently in the buffer. If the queue later drops below 40%, the pressure level resets so the warning can fire again on the next spike, preventing duplicate alerts during a sustained burst. For overflow events, a nanosecond-precision timestamp is recorded alongside the event details. This makes it possible to analyse short bursts of high load that a millisecond-level log would miss.

---

<a name="component-b"></a>
## Component B: Optimization and Priority (The "Hot Path")

---

### 2.1 Zero-Copy Parsing

**Overview**

`parse_event` turns a raw JSON string from the SSE stream into a structured event ready for queuing. It does this in two phases. In the first phase, `WikiEvent<'a>` reads string fields directly from the original buffer without copying any data to new memory. In the second phase, those fields are converted into owned strings so the event can be stored independently of the buffer. A custom memory counter tracks exactly how many allocations occur during the first phase.

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

**Explanation**

By pointing directly into the existing JSON buffer instead of copying strings to new memory, the parser avoids unnecessary allocation on every event. This matters in a high-throughput pipeline where thousands of events arrive per minute. The allocation counter is sampled before and after the parse call, and the difference is stored with the event. A count of zero in the log means Phase 1 truly allocated nothing. The second phase then creates exactly three owned strings, for user, domain, and title, which is the minimum needed to store the event independently of the buffer.

---

### 2.2 Priority Scheduling

**Overview**

Priority is enforced at two points. At the queue level, `pop()` always retrieves a human edit before any bot, regardless of arrival order. At the processing level, when the system enters degraded mode under high load, bot events are discarded immediately after being dequeued, without going through the leaderboard update at all. Together these two rules ensure human edits consistently move through the pipeline faster than bot edits.

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

**Explanation**

The `pop()` function scans the queue from front to back and removes the first human event it finds by index. This means a human that arrived after fifty bots is still processed next. At the enqueue side, when the queue is full, a bot is always rejected or evicted to make room for an incoming human. In degraded mode, the discard happens before any lock is acquired, so it adds almost no overhead even when the system is already under pressure. As a result, human throughput is maintained regardless of load conditions.

---

### 2.3 Micro-Deadlines

**Overview**

Each event has a strict 2ms budget from the moment it is dequeued to the moment all processing is complete. A timer starts immediately after `pop()` and stops after the leaderboard update. Events that exceed the budget are flagged in the log as `deadline=MISS`. The `DriftTracker` collects these times separately for humans and bots and produces p50, p90, and p99 summaries every 10 seconds.

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

**Explanation**

The timer starts before any lock is acquired, so it captures the full cost of processing including any wait for the leaderboard mutex. The 2ms threshold is defined once in `config.rs` and imported by name wherever it is used, so changing the deadline requires editing only one file. Deadline misses are counted separately from Component C blocks: a bot that is blocked by page protection is not counted as a miss, keeping the miss count focused on genuine timing violations. The rolling window of recent drift values feeds the live dashboard chart, giving a real-time view of whether the system is meeting its deadline.

---

<a name="component-c"></a>
## Component C: Priority Scheduling and Drift

---

### 3.1 Human Override Bot Mechanism

**Overview**

Human edits are protected at two levels. At the queue level, `pop()` always returns a human edit before any bot. Beyond this, if the most recent edit to a specific Wikipedia article was made by a human, any bot trying to edit the same article is blocked entirely. This check is per article title rather than per domain, so blocking a bot on one article has no effect on other articles on the same site.

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

**Explanation**

The protection check and the leaderboard update share the same lock acquisition, which means the read and write happen together with no gap where another thread could change the page's protection status in between. A page that has never been seen before returns unprotected, so the first edit to any article always goes through. When a bot is blocked, the leaderboard update is skipped entirely and the event is not counted as a deadline miss. The dashboard live feed shows blocked bots in cyan, visually distinguishing them from evictions in yellow and drops in red.

---

### 3.2 Scheduling Drift Measurement and Reporting

**Overview**

`DriftTracker` measures how long each event takes from the moment it leaves the queue to the moment processing is complete. It stores these times in separate lists for humans and bots, then computes p50, p90, and p99 for each group. Results are pushed to the live dashboard every second and logged every 10 seconds with a trend indicator showing whether latency is improving or getting worse.

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

**Explanation**

Sorting the sample list to compute percentiles happens at reporting time, not during event processing, so it adds no latency to the hot path. The trend indicator compares the current p99 to the previous 10-second window. For example, if p99 rises by more than 500 microseconds, the trend shows "up", giving early warning before latency reaches the miss threshold. Human samples consistently produce lower p99 values than bot samples because the human-first dequeue reduces total queue wait time. The session summary side-by-side comparison is the direct proof that priority scheduling produces a measurable difference in latency.

---

<a name="component-d"></a>
## Component D: Shared Resource and Metrics

---

### 4.1 Thread Safety and Live Leaderboard

**Overview**

The leaderboard tracks edit counts per Wikipedia domain and maintains a live top-3 ranking. It is shared between the processor loop and the dashboard using `Arc<Mutex<>>`, which allows safe access from multiple threads. `SharedState` bundles all other shared data, including counters, flags, and the last heartbeat time, into one place so every thread works with the same live information.

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

**Explanation**

The `Arc` wrapper lets multiple threads hold a reference to the same leaderboard without copying any data. The `Mutex` inside ensures only one thread can read or write at a time, preventing corrupted state from concurrent writes. The dashboard reads the top-3 list through the `RwLock` version using a read lock, which allows multiple threads to read at the same time without waiting for the processor to finish writing. The `degraded_mode` flag uses an atomic boolean instead of a mutex because toggling a single flag does not need the broader protection a mutex provides.

---

### 4.2 Synchronization Benchmark

**Overview**

Every processed event runs the leaderboard update through all three synchronisation methods at once: `Mutex`, `RwLock`, and `AtomicU64`. The time each one takes is recorded in nanoseconds. A rolling average of the last 1000 samples per method is displayed live on the dashboard. The Criterion benchmark `sync_contention` then measures how each method scales from 1 to 16 concurrent writer threads.

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

**Explanation**

At one thread, all three methods show their baseline cost without any contention. As thread count rises to 16, `Mutex` and `RwLock` times increase steeply because threads spend most of their time waiting for the lock to become free. `AtomicU64` scales much better because the hardware handles contention directly in the CPU cache without suspending any thread. For example, at 16 threads the atomic operation can be many times faster than the mutex. Criterion reports a confidence interval for each measurement, which makes the comparison statistically meaningful rather than based on a single run.

---

<a name="component-e"></a>
## Component E: Fault Tolerance and Watchdog

---

### 5.1 Network Resilience

**Overview**

A watchdog thread runs independently from the ingestion pipeline. It waits on a channel for a heartbeat signal from the pipeline, where every valid SSE event causes the pipeline to send a token. If no token arrives for 10 seconds, the watchdog concludes the stream has stalled, increments the reconnect counter, and signals the pipeline to drop its connection and start again.

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

**Explanation**

The watchdog and the ingestion pipeline communicate only through bounded channels, so neither holds a reference to the other. This keeps them fully independent, meaning a change to one side does not affect the other. The reconnect channel has a capacity of one, so if the pipeline is already reconnecting when a second timeout fires, the extra signal is silently discarded rather than queued. The dashboard derives connection status independently from `last_heartbeat`, giving a live countdown to the next watchdog check that updates every 100ms without involving the watchdog thread at all.

---

### 5.2 Fail-Safe Mode

**Overview**

`JitterMonitor` watches for instability in processing times. It keeps the last 100 processing times and computes their standard deviation after every event. When the standard deviation exceeds 5ms, the system enters degraded mode and starts discarding all bot events immediately on dequeue. When the standard deviation drops back to 5ms or below, degraded mode ends automatically and a log entry records how long it lasted and how many events were affected.

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

**Explanation**

Standard deviation captures instability better than an average because it rises when processing times become unpredictable, even if the mean stays low. The minimum sample guard prevents the system from reacting to noise right at startup when the window contains only a few data points. When degraded mode ends, the `DEGRADED_OFF` log records the exact duration, the number of bots discarded, and the number of humans that continued processing uninterrupted. This gives a clear audit trail of every degraded period. In conclusion, the session summary accumulates all degraded windows into a single total, making it easy to see what fraction of the session was spent under stress.

---

<a name="advanced-integration"></a>
## Advanced Integration for Distinction-Level

---

### 6.1 Memory Mastery: Zero-Copy Proof

**Overview**

`CountingAllocator` replaces the standard memory allocator for the entire process. Every heap allocation anywhere in the program increments a global atomic counter. By reading this counter immediately before and after the JSON parse step, the parser reports exactly how many allocations the parsing phase made. This count is stored in every event and written to the log, providing runtime proof of the zero-copy claim.

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

**Explanation**

The custom allocator delegates all actual memory operations to the system allocator unchanged, so it has no impact on performance beyond one atomic increment per allocation. The counter uses relaxed ordering because it only needs to be accurate within the same thread between the two reads, with no cross-thread synchronisation required. For example, across a 10-minute session with tens of thousands of events, every `PARSED` log line carrying `allocs=0` collectively proves the zero-copy design held throughout the entire run.

---

### 6.2 Comparative Analysis: Async vs Threaded Tail Latency

**Overview**

The Criterion benchmark `pipeline_comparison` runs both pipelines through the same workload of 500 events at a 75% bot, 25% human ratio. Each simulation uses an inline priority channel that mirrors the production push and pop logic. The benchmark measures scheduling drift for each pipeline and prints p50, p90, and p99 to the terminal before the timed iterations begin.

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

**Explanation**

Both simulations share the same event generation and the same priority channel logic, so the only variable is whether Tokio tasks or OS threads handle the concurrency. The `yield_now()` calls in both producer and consumer allow the scheduler to interleave them, which reflects real pipeline conditions. The p50/p90/p99 table appears directly alongside the Criterion confidence intervals in the terminal output, giving a single unified view of the comparison. The async simulation uses `tokio::sync::Mutex` instead of `std::sync::Mutex` because blocking inside an async task would prevent other tasks from running.

---

### 6.3 Statistical Rigor: p50/p90/p99 Percentiles

**Overview**

The system tracks scheduling drift using three percentile levels: p50 for typical behaviour, p90 for elevated but not extreme cases, and p99 for tail latency affecting the worst 1% of events. All three are tracked separately for humans and bots, updated to the dashboard every second, logged every 10 seconds with a trend indicator, and printed side-by-side in the session summary.

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

**Explanation**

An average hides tail behaviour. A system with a mean of 0.5ms can still have a p99 of 50ms if a small number of events are severely delayed. By reporting all three percentiles, the report gives a complete picture of the latency distribution. The gap between human p99 and bot p99 is the direct evidence that priority scheduling works: humans experience lower tail latency because they are dequeued first. The trend arrows in the 10-second log give early warning of deteriorating latency, allowing the system state to be assessed even without looking at the dashboard.

---

### 6.4 Safety Interlocks: Automatic Degraded Mode and Recovery

**Overview**

The system uses two independent safety mechanisms that work together. The jitter monitor detects CPU load instability and enters degraded mode to reduce processing pressure. The watchdog detects network failures and triggers reconnection. Both activate and recover automatically with no operator input required, and both produce structured log entries showing exactly when they activated, how long they lasted, and what effect they had.

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

**Explanation**

The two mechanisms address different failure modes without overlap. A network failure triggers the watchdog regardless of CPU load, and a jitter spike triggers degraded mode regardless of network health. When degraded mode activates, the `DEGRADED_ON` log records the measured jitter level and the threshold that was crossed. When it ends, `DEGRADED_OFF` records the duration, the number of bots discarded, and the number of humans processed uninterrupted. Together these two log lines are the evidence that the system responded and recovered correctly. In conclusion, the session summary aggregates all reconnects, degraded activations, and total degraded time into one section, giving a concise fault tolerance audit for the entire session.
