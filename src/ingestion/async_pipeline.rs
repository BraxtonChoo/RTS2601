// Component A — Async pipeline (Tokio)
// Reads Wikipedia SSE stream using reqwest + futures_util.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, Sender};
use futures_util::StreamExt;

use crate::channel::PriorityChannel;
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
            tracing::info!("[PIPELINE] Reconnecting...  attempt={}", attempt);
        }

        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());

        let response = match client
            .get(SSE_URL)
            .header("Accept", "text/event-stream")
            .header("Cache-Control", "no-cache")
            .header("User-Agent", "rts2601/0.1 (student project; Rust/tokio)")
            .send()
            .await
        {
            Ok(r)  => r,
            Err(e) => {
                tracing::error!("[PIPELINE] Connection failed  attempt={}  error={}", attempt, e);
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        let status = response.status();
        if !status.is_success() {
            tracing::error!("[PIPELINE] HTTP {}  retrying in 5s", status);
            tokio::time::sleep(Duration::from_secs(5)).await;
            continue;
        }

        tracing::info!("[PIPELINE] Connected to Wikipedia SSE stream  pipeline=async");

        let mut stream      = response.bytes_stream();
        let mut buf         = String::new();
        let connect_time    = Instant::now();
        let mut first_event = true;

        'inner: loop {
            if reconnect_rx.try_recv().is_ok() {
                tracing::warn!("[PIPELINE] Reconnect signal received — dropping connection");
                break 'inner;
            }

            let chunk = match tokio::time::timeout(Duration::from_secs(1), stream.next()).await {
                Ok(Some(result)) => result,
                Ok(None) => {
                    tracing::warn!("[PIPELINE] Stream ended unexpectedly");
                    break 'inner;
                }
                Err(_) => continue 'inner,
            };

            match chunk {
                Err(e) => {
                    tracing::error!("[PIPELINE] Stream error  error={}", e);
                    break 'inner;
                }
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

                            if first_event {
                                first_event = false;
                                tracing::info!(
                                    "[PIPELINE] First event received  stream_confirmed_live=true  latency={:.2}s",
                                    connect_time.elapsed().as_secs_f64()
                                );
                                attempt = 0; // reset for next disconnect cycle
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
        }

        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}
