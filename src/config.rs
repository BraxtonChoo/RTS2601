// src/config.rs — Single source of truth for all tuneable system parameters.
//
// Every magic number in the system lives here.
// To change behaviour, edit this file only — no other file needs touching.

use std::time::Duration;

// ---------------------------------------------------------------------------
// Component A — Priority Channel
// ---------------------------------------------------------------------------

/// Maximum number of events the priority channel can hold.
pub const CHANNEL_CAPACITY: usize = 15;

/// Buffer fill level (%) at which a BUFFER_50PCT warning is emitted.
pub const BUFFER_WARN_PCT: usize = 50;

/// Buffer fill level (%) at which a BUFFER_80PCT critical warning is emitted.
pub const BUFFER_CRITICAL_PCT: usize = 80;

/// Buffer fill level (%) below which BUFFER_EASED is emitted (pressure relieved).
pub const BUFFER_EASE_PCT: usize = 40;

/// Capacity of the heartbeat crossbeam channel (watchdog ← pipeline).
pub const HEARTBEAT_CHANNEL_CAP: usize = 10;

/// Capacity of the reconnect crossbeam channel (watchdog → pipeline).
pub const RECONNECT_CHANNEL_CAP: usize = 1;

// ---------------------------------------------------------------------------
// Component B / C — Scheduling Deadline & Drift
// ---------------------------------------------------------------------------

/// Hard deadline for each event from dequeue to processing complete.
pub const DRIFT_DEADLINE_MS: f64 = 2.0;

/// Same deadline as a Duration — used where a Duration is required.
#[allow(dead_code)]
pub const DRIFT_DEADLINE: Duration = Duration::from_millis(2);

/// Miss-rate threshold (%) above which a DRIFT_ALERT is emitted.
#[allow(dead_code)]
pub const DRIFT_ALERT_PCT: f64 = 25.0;

/// How often (seconds) the drift 10s log line is emitted.
pub const DRIFT_LOG_INTERVAL_SECS: u64 = 10;

/// Rolling window of per-event drift samples kept for the dashboard chart (µs).
pub const DRIFT_HISTORY_LEN: usize = 300;

// ---------------------------------------------------------------------------
// Component D — Leaderboard
// ---------------------------------------------------------------------------

/// Rolling window size for Mutex / RwLock / Atomic timing samples.
pub const LEADERBOARD_ROLLING_WINDOW: usize = 1_000;

// ---------------------------------------------------------------------------
// Component E — Watchdog & Jitter Monitor
// ---------------------------------------------------------------------------

/// No heartbeat for this long → watchdog triggers a reconnect.
pub const WATCHDOG_TIMEOUT: Duration = Duration::from_secs(10);

/// Processing jitter (std-dev, ms) above this → degraded mode ON.
pub const JITTER_THRESHOLD_MS: f64 = 5.0;

/// Number of recent processing-time samples kept by the jitter monitor.
pub const JITTER_WINDOW: usize = 100;

/// Minimum samples in the jitter window before evaluation starts.
pub const JITTER_MIN_SAMPLES: usize = 10;

// ---------------------------------------------------------------------------
// Processor loop
// ---------------------------------------------------------------------------

/// How often the per-second stats tick runs.
pub const STATS_TICK_SECS: u64 = 1;

/// How often a CHECKPOINT log line is emitted.
pub const CHECKPOINT_INTERVAL_SECS: u64 = 30;

/// Max recent events kept for the dashboard live feed.
pub const RECENT_EVENTS_LEN: usize = 10;

/// Rolling window of TPS samples used for spike detection baseline.
pub const TPS_HISTORY_LEN: usize = 60;

/// Minimum baseline TPS before spike detection activates.
pub const SPIKE_BASELINE_MIN_TPS: f64 = 5.0;

/// Minimum samples in the TPS history before spike detection activates.
pub const SPIKE_BASELINE_MIN_SAMPLES: usize = 10;

/// TPS must exceed baseline × this ratio to be declared a spike.
pub const SPIKE_RATIO: f64 = 2.5;

/// TPS must drop below baseline × this ratio for spike to be cleared.
pub const SPIKE_RECOVERY_RATIO: f64 = 1.5;

// ---------------------------------------------------------------------------
// Ingestion pipelines
// ---------------------------------------------------------------------------

/// Backoff after a clean stream disconnect before reconnecting.
pub const RECONNECT_BACKOFF: Duration = Duration::from_secs(2);

/// Backoff after a connection failure before retrying.
pub const CONNECT_FAIL_BACKOFF: Duration = Duration::from_secs(5);

/// TCP connect timeout for the SSE HTTP request.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

// ---------------------------------------------------------------------------
// Dashboard
// ---------------------------------------------------------------------------

/// How often the dashboard redraws and polls for key input.
pub const DASHBOARD_POLL_MS: u64 = 100;

/// Adaptive chart Y-axis floor (µs) — keeps the drift line visible near zero.
pub const CHART_Y_FLOOR_US: u64 = 100;

/// Adaptive chart Y-axis ceiling (µs) — prevents extreme outliers squashing the line.
pub const CHART_Y_CEIL_US: u64 = 10_000;
