// Component E: Watchdog + JitterMonitor
//
// Watchdog: dedicated std::thread; 10s timeout on heartbeat channel.
//   On timeout → sends reconnect signal → ingestion pipeline re-connects.
//
// JitterMonitor: rolling std-dev of processing times.
//   Jitter > 5ms → degraded mode ON  (processor discards bots).
//   Jitter ≤ 5ms → degraded mode OFF (system recovers automatically).
//   D4: recovery proven by logging degraded duration and events discarded on exit.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender};

use crate::types::SharedState;

const WATCHDOG_TIMEOUT:   Duration = Duration::from_secs(10);
const JITTER_THRESHOLD_MS: f64    = 5.0;
const JITTER_WINDOW:       usize  = 100;

// ---------------------------------------------------------------------------
// start_watchdog
// ---------------------------------------------------------------------------
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
                    tracing::warn!(actor = "SYSTEM", evt = "WATCHDOG_TIMEOUT", reason = "no_heartbeat_10s");
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

// ---------------------------------------------------------------------------
// JitterMonitor
// ---------------------------------------------------------------------------
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
        if self.recent_times.len() >= 10 {
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
        let jitter     = self.jitter();
        let currently  = self.degraded.load(Ordering::Relaxed);

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
                s.degraded_mode            = true;
                s.degraded_activations    += 1;
                s.bots_discarded_degraded  = 0;
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
