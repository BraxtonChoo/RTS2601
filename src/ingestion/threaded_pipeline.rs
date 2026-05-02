// Component A — Threaded pipeline (std::thread + ureq blocking I/O)

use std::io::{BufRead, BufReader};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, Sender};

use crate::channel::PriorityChannel;
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
                tracing::info!("[PIPELINE] Reconnecting...  attempt={}", attempt);
            }

            let agent = ureq::AgentBuilder::new()
                .timeout_connect(Duration::from_secs(10))
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
                    tracing::error!("[PIPELINE] Connection failed  attempt={}  error={}", attempt, e);
                    thread::sleep(Duration::from_secs(5));
                    continue;
                }
            };

            tracing::info!("[PIPELINE] Connected to Wikipedia SSE stream  pipeline=threaded");

            let reader       = BufReader::new(response.into_reader());
            let connect_time = Instant::now();
            let mut first_event = true;

            'inner: for line_result in reader.lines() {
                if reconnect_rx.try_recv().is_ok() {
                    tracing::warn!("[PIPELINE] Reconnect signal received — dropping connection");
                    if let Ok(mut s) = state.stats.lock() {
                        s.reconnect_count += 1;
                    }
                    break 'inner;
                }

                match line_result {
                    Err(e) => {
                        tracing::error!("[PIPELINE] Stream error  error={}", e);
                        break 'inner;
                    }
                    Ok(line) => {
                        if let Some(json) = line.trim().strip_prefix("data: ") {
                            let _ = heartbeat_tx.try_send(());

                            if first_event {
                                first_event = false;
                                tracing::info!(
                                    "[PIPELINE] First event received  stream_confirmed_live=true  latency={:.2}s",
                                    connect_time.elapsed().as_secs_f64()
                                );
                                attempt = 0;
                            }

                            match parse_event(json) {
                                Ok(event) => {
                                    tracing::debug!(
                                        "[event] user={}  domain={}  bot={}",
                                        event.user, event.domain, event.is_bot
                                    );
                                    schedule_event(event, &channel, &state);
                                }
                                Err(e) => {
                                    tracing::debug!(
                                        "[PARSE] Skipped  reason=\"{}\"  raw_len={}B",
                                        e, json.len()
                                    );
                                }
                            }
                        }
                    }
                }
            }

            thread::sleep(Duration::from_secs(2));
        }
    });
}
