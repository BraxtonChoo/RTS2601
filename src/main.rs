mod allocator;
mod channel;
mod dashboard;
mod ingestion;
mod leaderboard;
mod logging;
mod parser;
mod scheduler;
mod types;
mod watchdog;

// D1: Counting allocator — proves zero-copy hot path has no hidden heap allocations
use allocator::CountingAllocator;
#[global_allocator]
static A: CountingAllocator = CountingAllocator;

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use crossbeam_channel::bounded;

use channel::PriorityChannel;
use leaderboard::LeaderboardManager;
use scheduler::DriftTracker;
use types::{EventStatus, PipelineMode, RecentEvent, SharedState, DRIFT_HISTORY_LEN};
use watchdog::{start_watchdog, JitterMonitor};

pub const CHANNEL_CAPACITY: usize = 100;

// ---------------------------------------------------------------------------
// CLI flag parsing
// ---------------------------------------------------------------------------
fn parse_pipeline_mode() -> PipelineMode {
    let args: Vec<String> = std::env::args().collect();
    for i in 0..args.len() {
        match args[i].as_str() {
            // --threaded  (shorthand flag)
            "--threaded" => return PipelineMode::Threaded,
            // --async     (shorthand flag)
            "--async"    => return PipelineMode::Async,
            // --pipeline threaded | --pipeline async
            "--pipeline" => {
                if let Some(val) = args.get(i + 1) {
                    return match val.to_lowercase().as_str() {
                        "threaded" => PipelineMode::Threaded,
                        _          => PipelineMode::Async,
                    };
                }
            }
            _ => {}
        }
    }
    PipelineMode::Async
}

// ---------------------------------------------------------------------------
// Session summary — printed on Ctrl-C / q
// ---------------------------------------------------------------------------
fn print_summary(
    state:       &SharedState,
    drift:       &mut DriftTracker,
    jitter:      &JitterMonitor,
    leaderboard: &Arc<Mutex<LeaderboardManager>>,
) {
    let s       = state.stats.lock().unwrap();
    let runtime = state.start_time.elapsed();
    let h       = runtime.as_secs() / 3600;
    let m       = (runtime.as_secs() % 3600) / 60;
    let sec     = runtime.as_secs() % 60;

    let h_pct = if s.events_processed > 0 {
        s.human_events as f64 / s.events_processed as f64 * 100.0
    } else { 0.0 };
    let b_pct = 100.0 - h_pct;

    let h50 = DriftTracker::percentile(&mut drift.human_samples, 50.0);
    let h90 = DriftTracker::percentile(&mut drift.human_samples, 90.0);
    let h99 = DriftTracker::percentile(&mut drift.human_samples, 99.0);
    let b50 = DriftTracker::percentile(&mut drift.bot_samples,   50.0);
    let b90 = DriftTracker::percentile(&mut drift.bot_samples,   90.0);
    let b99 = DriftTracker::percentile(&mut drift.bot_samples,   99.0);
    let h_miss = drift.human_samples.iter().filter(|&&d| d > 2000.0).count();
    let b_miss = drift.bot_samples.iter().filter(|&&d| d > 2000.0).count();

    let top3        = leaderboard.lock().unwrap().top3();
    let lb_avg_mutex  = leaderboard.lock().unwrap().avg_mutex_ns();
    let lb_avg_rwlock = leaderboard.lock().unwrap().avg_rwlock_ns();
    let lb_avg_atomic = leaderboard.lock().unwrap().avg_atomic_ns();

    let miss_rate     = if s.events_processed > 0 { s.deadline_misses as f64 / s.events_processed as f64 * 100.0 } else { 0.0 };
    let _overflow_rate = if s.events_processed > 0 { s.overflow_events as f64 / s.events_processed as f64 * 100.0 } else { 0.0 };

    // Log structured session-end line first (goes to file)
    tracing::info!(
        actor = "SYSTEM", evt = "SESSION_END",
        runtime = format_args!("{:02}:{:02}:{:02}", h, m, sec),
        total = s.events_processed,
        human = format_args!("{}({:.1}%)", s.human_events, h_pct),
        bot = format_args!("{}({:.1}%)", s.bot_events, b_pct),
        deadline_misses = format_args!("{}({:.1}%)", s.deadline_misses, miss_rate),
        overflows = s.overflow_events,
        reconnects = s.reconnect_count,
        degraded_windows = s.degraded_activations
    );

    // Human-readable terminal summary.
    // BW = number of interior characters between the two ║ on every line.
    // Every content line is: ║ {content:<BW-2} ║  (one space margin each side).
    // The separator is ═ repeated BW times.
    // Nothing is hardcoded to a column — format! builds the content string,
    // then it gets padded to exactly BW-2 chars so the right ║ never drifts.
    const BW: usize = 50;
    let sep = || println!("╠{}╣", "═".repeat(BW));
    let row = |content: String| println!("║ {:<width$} ║", content, width = BW - 2);

    println!("\n╔{}╗", "═".repeat(BW));
    row(format!("    RTS2601  —  SESSION SUMMARY"));
    sep();
    row(format!("  Pipeline:     {:<10}  Runtime: {:02}:{:02}:{:02}", state.pipeline_mode, h, m, sec));
    row(format!("  Total events:   {:>8}", s.events_processed));
    row(format!("  Human edits:    {:>8}  ({:.1}%)", s.human_events, h_pct));
    row(format!("  Bot edits:      {:>8}  ({:.1}%)", s.bot_events,   b_pct));
    sep();
    row(format!("  COMPONENT A — CHANNEL"));
    row(format!("  OverflowEvents:  {:>6}", s.overflow_events));
    row(format!("  Bot evictions:   {:>6}", s.bot_evictions));
    row(format!("  Bot drops:       {:>6}", s.bot_drops));
    row(format!("  Human drops:     {:>6}", s.human_drops));
    sep();
    row(format!("  COMPONENT C — SCHEDULING DRIFT"));
    row(format!("  (dequeue -> task complete vs 2ms deadline)"));
    row(format!("  Human  p50:{:7.3}ms  p90:{:7.3}ms  p99:{:7.3}ms", h50/1000.0, h90/1000.0, h99/1000.0));
    row(format!("         misses: {:>4}", h_miss));
    row(format!("  Bot    p50:{:7.3}ms  p90:{:7.3}ms  p99:{:7.3}ms", b50/1000.0, b90/1000.0, b99/1000.0));
    row(format!("         misses: {:>4}", b_miss));
    row(format!("  C-Blocked: {:>6}  (bot->human protected)", s.comp_c_rejections));
    sep();
    row(format!("  COMPONENT D — SYNC BENCHMARK (session avg)"));
    row(format!("  Mutex:   {:>8.0} ns", lb_avg_mutex));
    row(format!("  RwLock:  {:>8.0} ns", lb_avg_rwlock));
    row(format!("  Atomic:  {:>8.0} ns", lb_avg_atomic));
    row(format!("  Top-3 domains:"));
    for (i, (domain, count)) in top3.iter().enumerate() {
        row(format!("    {}. {:<28}  {:>6}", i + 1, domain, count));
    }
    sep();
    row(format!("  COMPONENT E — FAULT TOLERANCE"));
    row(format!("  Watchdog reconnects:   {:>4}", s.reconnect_count));
    row(format!("  Degraded activations:  {:>4}", s.degraded_activations));
    row(format!("  Total degraded time:   {:.1}s", jitter.total_degraded_seconds()));
    sep();
    row(format!("  D1: Deadline misses:   {:>6}", s.deadline_misses));
    println!("╚{}╝\n", "═".repeat(BW));
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
#[tokio::main]
async fn main() {
    logging::init_logging();

    let pipeline_mode = parse_pipeline_mode();

    let state       = Arc::new(SharedState::new(pipeline_mode));
    let channel     = Arc::new(Mutex::new(PriorityChannel::new(CHANNEL_CAPACITY)));
    let leaderboard = Arc::new(Mutex::new(LeaderboardManager::new()));

    tracing::info!(
        actor = "SYSTEM", evt = "SESSION_START",
        pipeline = %pipeline_mode, buffer = CHANNEL_CAPACITY,
        drift_deadline = "2ms", watchdog_timeout = "10s"
    );

    // Watchdog channels
    let (heartbeat_tx, heartbeat_rx) = bounded::<()>(10);
    let (reconnect_tx, reconnect_rx) = bounded::<()>(1);

    start_watchdog(heartbeat_rx, reconnect_tx, Arc::clone(&state));

    match pipeline_mode {
        PipelineMode::Async => {
            tokio::spawn(ingestion::async_pipeline::run_async_pipeline(
                heartbeat_tx,
                reconnect_rx,
                Arc::clone(&channel),
                Arc::clone(&state),
            ));
        }
        PipelineMode::Threaded => {
            ingestion::threaded_pipeline::run_threaded_pipeline(
                heartbeat_tx,
                reconnect_rx,
                Arc::clone(&channel),
                Arc::clone(&state),
            );
        }
    }

    // Ctrl-C / 'q' exit flag — shared with dashboard
    let exit_flag = Arc::new(AtomicBool::new(false));
    {
        let flag = Arc::clone(&exit_flag);
        tokio::spawn(async move {
            let _ = tokio::signal::ctrl_c().await;
            flag.store(true, Ordering::Relaxed);
        });
    }

    // Dashboard
    {
        let state_c = Arc::clone(&state);
        let lb_c    = Arc::clone(&leaderboard);
        let flag_c  = Arc::clone(&exit_flag);
        std::thread::spawn(move || {
            if let Err(e) = dashboard::run_dashboard(state_c, lb_c, flag_c) {
                tracing::error!("Dashboard error: {e}");
            }
        });
    }

    // ---------------------------------------------------------------------------
    // Processor loop
    // ---------------------------------------------------------------------------
    let mut drift_tracker      = DriftTracker::new();
    let mut jitter_monitor     = JitterMonitor::new(Arc::clone(&state.degraded_mode));
    let mut last_stats_tick    = Instant::now();
    let mut last_checkpoint    = Instant::now();
    let mut events_this_second = 0u64;
    let mut total_deadline_misses_checkpoint = 0u64;
    let mut total_overflows_checkpoint       = 0u64;
    let mut total_processed_checkpoint       = 0u64;

    // Throughput spike detection: keep a rolling 60-sample baseline (one per second)
    let mut tps_history: VecDeque<f64> = VecDeque::with_capacity(60);
    let mut spike_active = false;

    loop {
        if exit_flag.load(Ordering::Relaxed) {
            drift_tracker.report();
            print_summary(&state, &mut drift_tracker, &jitter_monitor, &leaderboard);
            std::process::exit(0);
        }

        let event = {
            let mut ch = channel.lock().unwrap();
            ch.pop()
        };

        if let Some(event) = event {
            // Component B/C: timing starts the moment the packet leaves the channel.
            // This is the reference point for scheduling drift (dequeue → task complete).
            let process_start = Instant::now();

            // queue_wait_ms: how long the packet sat inside PriorityChannel before we
            // picked it up.  Logged as qwait= for secondary analysis; NOT used for the
            // 2ms deadline comparison — only scheduling drift (below) is.
            let queue_wait_ms = event.enqueued_at.elapsed().as_secs_f64() * 1000.0;

            let degraded = state.degraded_mode.load(Ordering::Relaxed);

            if degraded && event.is_bot {
                // Degraded mode: immediately discard bots without touching the leaderboard.
                let process_us = process_start.elapsed().as_micros() as f64;
                drift_tracker.record(process_us, true);
                if let Ok(mut s) = state.stats.lock() {
                    s.bots_discarded_degraded += 1;
                }
                tracing::warn!(
                    actor = %event.user, kind = "BOT", domain = %event.domain,
                    evt = "DISCARDED", seq = event.seq,
                    page = %event.title,
                    qwait = format_args!("{:.2}ms", queue_wait_ms),
                    sched_drift = format_args!("{:.3}ms", process_us / 1000.0),
                    reason = "degraded_mode"
                );
                continue;
            }

            if degraded && !event.is_bot {
                if let Ok(mut s) = state.stats.lock() {
                    s.humans_processed_degraded += 1;
                }
            }

            // Component C (priority-after-dequeue): a bot must NOT overwrite a domain
            // whose most recent edit was made by a human.  The check and the update both
            // happen under the same leaderboard lock so no race is possible.
            let (mutex_ns, rwlock_ns, atomic_ns, comp_c_blocked) = {
                let mut lb = leaderboard.lock().unwrap();
                if event.is_bot && lb.last_was_human(&event.domain) {
                    // Human edit is protected — reject this bot without updating counts
                    (0u64, 0u64, 0u64, true)
                } else {
                    // Component D: update all three sync primitives and record last editor
                    let (m, r, a) = lb.update_all(&event.domain, event.is_bot, &event.user);
                    (m, r, a, false)
                }
            };

            // Scheduling drift: actual = dequeue → task complete, expected = 2ms
            let process_us      = process_start.elapsed().as_micros() as f64;
            let process_ms      = process_us / 1000.0;
            let deadline_missed = !comp_c_blocked && process_ms > 2.0;
            let kind            = if event.is_bot { "BOT" } else { "HUMAN" };

            // Record scheduling drift for percentile tracking
            drift_tracker.record(process_us, event.is_bot);

            if comp_c_blocked {
                tracing::warn!(
                    actor = %event.user, kind = "BOT", domain = %event.domain,
                    evt = "BLOCKED", seq = event.seq,
                    page = %event.title,
                    qwait = format_args!("{:.2}ms", queue_wait_ms),
                    sched_drift = format_args!("{:.3}ms", process_ms),
                    reason = "human_protected"
                );
            } else if deadline_missed {
                tracing::warn!(
                    actor = %event.user, kind = %kind, domain = %event.domain,
                    evt = "DONE", seq = event.seq,
                    page = %event.title,
                    qwait = format_args!("{:.2}ms", queue_wait_ms),
                    sched_drift = format_args!("{:.3}ms", process_ms),
                    deadline = "MISS",
                    mutex_ns, rwlock_ns, atomic_ns
                );
            } else {
                tracing::info!(
                    actor = %event.user, kind = %kind, domain = %event.domain,
                    evt = "DONE", seq = event.seq,
                    page = %event.title,
                    qwait = format_args!("{:.2}ms", queue_wait_ms),
                    sched_drift = format_args!("{:.3}ms", process_ms)
                );
            }

            // Component E: feed jitter monitor with actual processing time
            jitter_monitor.record(process_ms, &state);

            events_this_second += 1;

            if let Ok(mut s) = state.stats.lock() {
                s.events_processed += 1;
                if event.is_bot { s.bot_events += 1; } else { s.human_events += 1; }
                if deadline_missed   { s.deadline_misses   += 1; }
                if comp_c_blocked    { s.comp_c_rejections += 1; }

                // Drift sparkline — record every event (including C-blocked; they are near-zero)
                s.drift_history.push_back(process_us as u64);
                if s.drift_history.len() > DRIFT_HISTORY_LEN {
                    s.drift_history.pop_front();
                }
                if !comp_c_blocked {
                    // Only refresh sync benchmark averages when a real update happened
                    s.avg_mutex_ns  = leaderboard.lock().unwrap().avg_mutex_ns();
                    s.avg_rwlock_ns = leaderboard.lock().unwrap().avg_rwlock_ns();
                    s.avg_atomic_ns = leaderboard.lock().unwrap().avg_atomic_ns();
                }

                let elapsed = state.start_time.elapsed();
                let ts = format!(
                    "{:02}:{:02}:{:02}",
                    elapsed.as_secs() / 3600,
                    (elapsed.as_secs() % 3600) / 60,
                    elapsed.as_secs() % 60
                );
                let status = if comp_c_blocked {
                    EventStatus::CompCBlocked
                } else if deadline_missed {
                    EventStatus::DeadlineMissed
                } else {
                    EventStatus::Processed
                };
                s.recent_events.push_back(RecentEvent {
                    timestamp: ts,
                    user:      event.user.clone(),
                    domain:    event.domain.clone(),
                    is_bot:    event.is_bot,
                    status,
                });
                if s.recent_events.len() > 10 { s.recent_events.pop_front(); }
            }

            tracing::debug!(
                pipeline = %pipeline_mode, user = %event.user, domain = %event.domain,
                is_bot = event.is_bot, queue_wait_ms, process_ms,
                comp_c_blocked, mutex_ns, rwlock_ns, atomic_ns,
                deadline_ok = !deadline_missed, "processor event complete"
            );

            // --- Per-second stats tick ---
            if last_stats_tick.elapsed().as_secs() >= 1 {
                let tps = events_this_second as f64;
                if let Ok(mut s) = state.stats.lock() {
                    s.throughput_per_sec = tps;
                }
                events_this_second = 0;
                last_stats_tick    = Instant::now();
                drift_tracker.update_stats(&state);

                // Throughput spike detection — needs ≥10 baseline samples
                tps_history.push_back(tps);
                if tps_history.len() > 60 { tps_history.pop_front(); }
                if tps_history.len() >= 10 {
                    let baseline: f64 = tps_history.iter().sum::<f64>() / tps_history.len() as f64;
                    if !spike_active && baseline > 5.0 && tps > baseline * 2.5 {
                        spike_active = true;
                        tracing::warn!(
                            actor = "SYSTEM", evt = "THROUGHPUT_SPIKE",
                            current = format_args!("{:.0}/s", tps),
                            baseline = format_args!("{:.0}/s", baseline),
                            ratio = format_args!("{:.1}x", tps / baseline)
                        );
                    } else if spike_active && tps < baseline * 1.5 {
                        spike_active = false;
                        tracing::info!(
                            actor = "SYSTEM", evt = "THROUGHPUT_NORMAL",
                            current = format_args!("{:.0}/s", tps),
                            baseline = format_args!("{:.0}/s", baseline)
                        );
                    }
                }
            }

            // --- 30-second checkpoint ---
            if last_checkpoint.elapsed().as_secs() >= 30 {
                last_checkpoint = Instant::now();
                let (processed, misses, overflows, tps, buf, mode_str, preemptions, bot_rejections, human_drops) = {
                    let s = state.stats.lock().unwrap();
                    let mode = if s.degraded_mode { "DEGRADED ←" } else { "NORMAL" };
                    (
                        s.events_processed,
                        s.deadline_misses,
                        s.overflow_events,
                        s.throughput_per_sec,
                        s.current_buffer_fill,
                        mode.to_string(),
                        s.bot_evictions,
                        s.bot_drops,
                        s.human_drops,
                    )
                };
                let new_processed = processed - total_processed_checkpoint;
                let new_misses    = misses    - total_deadline_misses_checkpoint;
                let new_overflows = overflows - total_overflows_checkpoint;
                total_processed_checkpoint       = processed;
                total_deadline_misses_checkpoint = misses;
                total_overflows_checkpoint       = overflows;

                let miss_rate     = if new_processed > 0 { new_misses    as f64 / new_processed as f64 * 100.0 } else { 0.0 };
                let overflow_rate = if new_processed > 0 { new_overflows as f64 / new_processed as f64 * 100.0 } else { 0.0 };

                tracing::info!(
                    actor = "SYSTEM", evt = "CHECKPOINT",
                    processed = new_processed,
                    tps = format_args!("{:.1}/s", tps),
                    miss_rate = format_args!("{:.1}%", miss_rate),
                    overflow_rate = format_args!("{:.1}%", overflow_rate),
                    buf = format_args!("{}/{}", buf, CHANNEL_CAPACITY),
                    mode = %mode_str,
                    preemptions, bot_rejections, human_drops
                );
            }
        } else {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    }
}
