// Component A — Threaded pipeline (std::thread + ureq blocking I/O)

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
                tracing::info!(actor = "SYSTEM", evt = "RECONNECTING", attempt, pipeline = "threaded");
            }

            let agent = ureq::AgentBuilder::new()
                .timeout_connect(CONNECT_TIMEOUT)
                .build();

            let response = match agent
                .get(SSE_URL)
                .set("Accept", "text/event-stream")
                .set("Cache-Control", "no-cache")
                .set("User-Agent", "rts2601/0.1 (student project; Rust/std::thread)")
                .call()
            {
                Ok(r)  => r,
                Err(e) => {
                    tracing::error!(actor = "SYSTEM", evt = "CONNECT_FAIL", attempt, error = %e);
                    thread::sleep(CONNECT_FAIL_BACKOFF);
                    continue;
                }
            };

            tracing::info!(actor = "SYSTEM", evt = "CONNECTED", pipeline = "threaded");

            let reader       = BufReader::new(response.into_reader());
            let connect_time = Instant::now();
            let mut first_event = true;

            'inner: for line_result in reader.lines() {
                if reconnect_rx.try_recv().is_ok() {
                    tracing::warn!(actor = "SYSTEM", evt = "RECONNECT_SIGNAL", pipeline = "threaded");
                    // reconnect_count already incremented by the watchdog before it sent the signal
                    break 'inner;
                }

                match line_result {
                    Err(e) => {
                        tracing::error!(actor = "SYSTEM", evt = "STREAM_ERROR", pipeline = "threaded", error = %e);
                        break 'inner;
                    }
                    Ok(line) => {
                        if let Some(json) = line.trim().strip_prefix("data: ") {
                            let _ = heartbeat_tx.try_send(());
                            if let Ok(mut hb) = state.last_heartbeat.lock() {
                                *hb = Instant::now();
                            }

                            if first_event {
                                first_event = false;
                                tracing::info!(
                                    actor = "SYSTEM", evt = "STREAM_LIVE",
                                    latency = format_args!("{:.2}s", connect_time.elapsed().as_secs_f64())
                                );
                                attempt = 0;
                            }

                            match parse_event(json) {
                                Ok(event) => {
                                    schedule_event(event, &channel, &state);
                                }
                                Err(e) => {
                                    tracing::debug!(
                                        actor = "SYSTEM", evt = "PARSE_SKIP",
                                        reason = %e, raw_bytes = json.len()
                                    );
                                }
                            }
                        }
                    }
                }
            }

            thread::sleep(RECONNECT_BACKOFF);
        }
    });
}
