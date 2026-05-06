use std::collections::VecDeque;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::sync::atomic::AtomicBool;
use std::time::Instant;

// ---------------------------------------------------------------------------
// PipelineMode — selected via --pipeline <async|threaded> CLI flag
// ---------------------------------------------------------------------------
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PipelineMode {
    Async,
    Threaded,
}

impl fmt::Display for PipelineMode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PipelineMode::Async    => write!(f, "ASYNC"),
            PipelineMode::Threaded => write!(f, "THREADED"),
        }
    }
}

// ---------------------------------------------------------------------------
// PrioritisedEvent
// ---------------------------------------------------------------------------
// enqueued_at is intentionally set to Instant::now() as a placeholder in
// parse_event(), then OVERRIDDEN inside PriorityChannel::push() to stamp the
// exact moment the event enters the queue. Drift = pop_time − enqueued_at.
// ---------------------------------------------------------------------------
#[derive(Debug, Clone)]
pub struct PrioritisedEvent {
    pub seq:         u64,
    pub user:        String,
    pub is_bot:      bool,
    pub domain:      String,
    #[allow(dead_code)]
    pub title:       String,
    pub enqueued_at: Instant,
}

// ---------------------------------------------------------------------------
// PushResult — returned by PriorityChannel::push()
// ---------------------------------------------------------------------------
#[derive(Debug, PartialEq)]
pub enum PushResult {
    Accepted,
    DroppedIncoming,       // bot rejected — channel full
    BotEvicted(String),    // evicted bot's username
    DroppedOldest(String), // last resort: dropped human's username
}

// ---------------------------------------------------------------------------
// SystemStats — all live counters, read by dashboard and session summary
// ---------------------------------------------------------------------------
#[derive(Debug, Default, Clone)]
pub struct SystemStats {
    pub events_processed:     u64,
    pub human_events:         u64,
    pub bot_events:           u64,
    pub bot_evictions:        u64,
    pub bot_drops:            u64,
    pub human_drops:          u64,
    pub overflow_events:      u64,  // total OverflowEvents logged
    pub deadline_misses:      u64,  // processing time > 2ms
    pub reconnect_count:      u64,
    pub degraded_activations: u64,
    pub degraded_mode:        bool,
    pub current_buffer_fill:  usize,
    pub throughput_per_sec:   f64,

    pub avg_mutex_ns:  f64,
    pub avg_rwlock_ns: f64,
    pub avg_atomic_ns: f64,

    // Degraded-window counters — reset each time degraded mode exits
    pub bots_discarded_degraded:  u64,
    pub humans_processed_degraded: u64,

    pub human_drift_p50: f64,
    pub human_drift_p90: f64,
    pub human_drift_p99: f64,
    pub bot_drift_p50:   f64,
    pub bot_drift_p90:   f64,
    pub bot_drift_p99:   f64,

    // Unified scheduling drift (dequeue → task complete) — used by dashboard
    pub drift_p50: f64,
    pub drift_p90: f64,
    pub drift_p99: f64,

    // Component C: bot packets blocked from overwriting a human's last edit
    pub comp_c_rejections: u64,

    pub recent_events: VecDeque<RecentEvent>,
}

#[derive(Debug, Clone)]
pub struct RecentEvent {
    pub timestamp: String,
    pub user:      String,
    pub domain:    String,
    pub is_bot:    bool,
    pub status:    EventStatus,
}

#[derive(Debug, Clone)]
pub enum EventStatus {
    Processed,
    BotEvicted,
    BotDropped,
    DeadlineMissed,
}

// ---------------------------------------------------------------------------
// SharedState — one struct bundling all Arc<> pointers, cheaply cloned
// ---------------------------------------------------------------------------
pub struct SharedState {
    pub stats:         Arc<Mutex<SystemStats>>,
    pub degraded_mode: Arc<AtomicBool>,
    pub pipeline_mode: PipelineMode,
    pub start_time:    Instant,
}

impl SharedState {
    pub fn new(pipeline_mode: PipelineMode) -> Self {
        Self {
            stats:         Arc::new(Mutex::new(SystemStats::default())),
            degraded_mode: Arc::new(AtomicBool::new(false)),
            pipeline_mode,
            start_time:    Instant::now(),
        }
    }
}
