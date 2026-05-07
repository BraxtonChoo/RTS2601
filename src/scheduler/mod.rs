// Component C: Scheduler and drift tracker
//
// Scheduling Drift = time from dequeue (pop) to task completion (leaderboard update).
// Expected: ≤ 2ms.  Drift > 2ms → deadline miss.
//
// Queue-wait (secondary metric) = time packet spent inside PriorityChannel
// (enqueued_at → dequeue).  Logged per-event as qwait= but NOT used for the
// 2ms deadline comparison — only scheduling drift counts.

use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::channel::PriorityChannel;
use crate::types::{EventStatus, PrioritisedEvent, PushResult, RecentEvent, SharedState};

static EVENT_SEQ: AtomicU64 = AtomicU64::new(0);

#[allow(dead_code)]
pub const DRIFT_DEADLINE: Duration = Duration::from_millis(2);

// ---------------------------------------------------------------------------
// DriftTracker
// ---------------------------------------------------------------------------
pub struct DriftTracker {
    pub human_samples: Vec<f64>,  // microseconds
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

    // process_us: time from dequeue to task completion (scheduling drift, µs).
    // Pass process_start.elapsed().as_micros() as f64 AFTER all processing is done.
    // Returns the same value so callers can use it for deadline checks / logging.
    pub fn record(&mut self, process_us: f64, is_bot: bool) -> f64 {
        if is_bot {
            self.bot_samples.push(process_us);
        } else {
            self.human_samples.push(process_us);
        }
        process_us
    }

    pub fn percentile(samples: &mut Vec<f64>, pct: f64) -> f64 {
        if samples.is_empty() { return 0.0; }
        samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let idx = ((pct / 100.0) * samples.len() as f64) as usize;
        samples[idx.min(samples.len() - 1)]
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
            actor = "SYSTEM", evt = "DRIFT_FINAL",
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

    // Called every 1 s from main; emits [DRIFT 30s] every 30 s with trend arrow.
    pub fn update_stats(&mut self, state: &Arc<SharedState>) {
        if let Ok(mut s) = state.stats.lock() {
            s.human_drift_p50 = Self::percentile(&mut self.human_samples, 50.0) / 1000.0;
            s.human_drift_p90 = Self::percentile(&mut self.human_samples, 90.0) / 1000.0;
            s.human_drift_p99 = Self::percentile(&mut self.human_samples, 99.0) / 1000.0;
            s.bot_drift_p50   = Self::percentile(&mut self.bot_samples,   50.0) / 1000.0;
            s.bot_drift_p90   = Self::percentile(&mut self.bot_samples,   90.0) / 1000.0;
            s.bot_drift_p99   = Self::percentile(&mut self.bot_samples,   99.0) / 1000.0;

            // Unified scheduling drift across all event types — used by dashboard
            let mut all = self.human_samples.clone();
            all.extend_from_slice(&self.bot_samples);
            s.drift_p50 = Self::percentile(&mut all, 50.0) / 1000.0;
            s.drift_p90 = Self::percentile(&mut all, 90.0) / 1000.0;
            s.drift_p99 = Self::percentile(&mut all, 99.0) / 1000.0;
        }

        if self.last_log.elapsed() >= Duration::from_secs(10) {
            self.last_log = Instant::now();

            let h50 = Self::percentile(&mut self.human_samples, 50.0);
            let h90 = Self::percentile(&mut self.human_samples, 90.0);
            let h99 = Self::percentile(&mut self.human_samples, 99.0);
            let b50 = Self::percentile(&mut self.bot_samples,   50.0);
            let b90 = Self::percentile(&mut self.bot_samples,   90.0);
            let b99 = Self::percentile(&mut self.bot_samples,   99.0);
            let h_miss = self.human_samples.iter().filter(|&&d| d > 2000.0).count();
            let b_miss = self.bot_samples.iter().filter(|&&d| d > 2000.0).count();

            let h_trend = if h99 > self.last_h_p99 + 500.0 { "↑" }
                          else if h99 < self.last_h_p99 - 500.0 { "↓ recovering" }
                          else { "~" };
            let b_trend = if b99 > self.last_b_p99 + 500.0 { "↑" }
                          else if b99 < self.last_b_p99 - 500.0 { "↓ recovering" }
                          else { "~" };

            self.last_h_p99 = h99;
            self.last_b_p99 = b99;

            tracing::info!(
                "[DRIFT 10s] sched_drift(dequeue→done)  human p50={:.3}ms p90={:.3}ms p99={:.3}ms misses={} trend={}  |  bot p50={:.3}ms p90={:.3}ms p99={:.3}ms misses={} trend={}",
                h50/1000.0, h90/1000.0, h99/1000.0, h_miss, h_trend,
                b50/1000.0, b90/1000.0, b99/1000.0, b_miss, b_trend
            );

            // Alert when drift miss rate is high — indicates the processor is falling behind
            let total = self.human_samples.len() + self.bot_samples.len();
            let total_misses = h_miss + b_miss;
            if total > 0 {
                let miss_rate = total_misses as f64 / total as f64 * 100.0;
                if miss_rate > 25.0 {
                    tracing::warn!(
                        "[DRIFT ALERT] {:.1}% of events exceeded 2ms scheduling deadline  human_misses={}  bot_misses={}  — system under high load",
                        miss_rate, h_miss, b_miss
                    );
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// schedule_event — push into PriorityChannel and update stats/feed
// ---------------------------------------------------------------------------
pub fn schedule_event(
    mut event: PrioritisedEvent,
    channel:   &Arc<Mutex<PriorityChannel>>,
    state:     &Arc<SharedState>,
) {
    let seq      = EVENT_SEQ.fetch_add(1, Ordering::Relaxed) + 1;
    event.seq    = seq;

    // Capture all per-event fields before push() moves the value
    let user     = event.user.clone();
    let domain   = event.domain.clone();
    let is_bot   = event.is_bot;
    let raw_len  = event.raw_len;
    let parse_us = event.parse_us;
    let allocs   = event.allocs;
    let kind     = if is_bot { "BOT" } else { "HUMAN" };

    // Component B — raw bytes received from SSE stream
    tracing::info!(
        actor = %user, kind, domain = %domain,
        evt = "INGESTED", seq, raw_bytes = raw_len
    );

    // Component B — zero-copy parse result
    tracing::info!(
        actor = %user, kind, domain = %domain,
        evt = "PARSED", seq, parse_us, allocs
    );

    let (result, buf_fill, buf_cap) = {
        let mut ch = channel.lock().unwrap();
        let r    = ch.push(event);
        let fill = ch.len();
        let cap  = ch.capacity();
        (r, fill, cap)
    };

    match &result {
        PushResult::Accepted => {
            tracing::info!(
                actor = %user, kind, domain = %domain,
                evt = "ENQUEUED", seq,
                buf = format_args!("{}/{}", buf_fill, buf_cap)
            );
        }
        PushResult::BotEvicted(evicted_seq, evicted_user) => {
            // Incoming human admitted; oldest bot displaced — Overflow Event
            let timestamp_ns = SystemTime::now()
                .duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as u64;
            tracing::warn!(
                actor = %user, kind = "HUMAN", domain = %domain,
                evt = "EVICTED", seq, timestamp_ns,
                evicted_seq = evicted_seq, evicted_user = %evicted_user,
                buf = format_args!("{}/{}", buf_fill, buf_cap)
            );
        }
        PushResult::DroppedIncoming => {
            // Incoming bot dropped — channel full — Overflow Event
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
            // All-human queue — oldest human displaced — Overflow Event
            let timestamp_ns = SystemTime::now()
                .duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as u64;
            tracing::warn!(
                actor = %user, kind = "HUMAN", domain = %domain,
                evt = "DROPPED", seq, timestamp_ns,
                reason = "queue_full",
                dropped_seq = dropped_seq, dropped_user = %dropped_user,
                buf = format_args!("{}/{}", buf_fill, buf_cap)
            );
        }
    }

    if let Ok(mut s) = state.stats.lock() {
        let feed_entry: Option<(String, String, bool, EventStatus)> = match &result {
            PushResult::Accepted => None,
            PushResult::DroppedIncoming => {
                s.bot_drops      += 1;
                s.overflow_events += 1;
                Some((user.clone(), domain.clone(), true, EventStatus::BotDropped))
            }
            PushResult::BotEvicted(_, _) => {
                s.bot_evictions  += 1;
                s.overflow_events += 1;
                Some((user.clone(), domain.clone(), true, EventStatus::BotEvicted))
            }
            PushResult::DroppedOldest(_, _) => {
                s.human_drops    += 1;
                s.overflow_events += 1;
                None
            }
        };

        if let Some((u, d, b, status)) = feed_entry {
            let elapsed = state.start_time.elapsed();
            let ts = format!(
                "{:02}:{:02}:{:02}",
                elapsed.as_secs() / 3600,
                (elapsed.as_secs() % 3600) / 60,
                elapsed.as_secs() % 60
            );
            s.recent_events.push_back(RecentEvent { timestamp: ts, user: u, domain: d, is_bot: b, status });
            if s.recent_events.len() > 10 { s.recent_events.pop_front(); }
        }

        s.current_buffer_fill = buf_fill;
    }
}
