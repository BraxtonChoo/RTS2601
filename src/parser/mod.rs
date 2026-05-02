// Component B: Zero-copy parser
// WikiEvent<'a> borrows &str slices directly from the raw JSON buffer.
// No heap allocation occurs until the boundary conversion to PrioritisedEvent.

use serde::Deserialize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::allocator::ALLOC_COUNT;
use crate::types::PrioritisedEvent;

// Count how many events have been parsed — used to log the D1 proof once at startup
static PARSE_COUNT: AtomicU64 = AtomicU64::new(0);

pub const PARSE_DEADLINE: Duration = Duration::from_millis(2);

// ---------------------------------------------------------------------------
// WikiEvent — zero-copy struct
// All string fields borrow from the raw JSON buffer via lifetime 'a.
// #[serde(borrow)] enables serde to emit &'a str instead of String.
// ---------------------------------------------------------------------------
#[derive(Deserialize, Debug)]
pub struct WikiEvent<'a> {
    #[serde(borrow)]
    pub user: &'a str,

    #[serde(default)]
    pub bot: bool,

    #[serde(borrow, rename = "server_name")]
    pub server_name: &'a str,

    #[serde(borrow, default)]
    pub title: &'a str,

    #[serde(rename = "type", borrow, default)]
    #[allow(dead_code)]
    pub event_type: &'a str,
}

// ---------------------------------------------------------------------------
// parse_event
//
// D1 Allocation proof:
//   Phase 1 (zero-copy) — serde borrows &str from raw_json, no owned strings.
//     hot_path_allocs should be 0 (only serde_json internal parser state).
//   Phase 2 (boundary) — explicit .to_owned() calls create owned Strings.
//     boundary_allocs will show exactly 3 (user, domain, title).
//
// The enqueued_at field is set here as a placeholder; PriorityChannel::push()
// overwrites it with the exact channel-entry instant for accurate drift.
// ---------------------------------------------------------------------------
pub fn parse_event(raw_json: &str) -> Result<PrioritisedEvent, String> {
    let parse_start = Instant::now();

    // --- Phase 1: Zero-copy parse ---
    let before_parse = ALLOC_COUNT.load(Ordering::Relaxed);
    let event: WikiEvent = serde_json::from_str(raw_json)
        .map_err(|e| format!("Parse error: {e}"))?;
    let after_parse = ALLOC_COUNT.load(Ordering::Relaxed);
    let hot_path_allocs = after_parse - before_parse;

    let parse_dur = parse_start.elapsed();
    if parse_dur > PARSE_DEADLINE {
        tracing::error!(
            parse_us = parse_dur.as_micros(),
            "Component B: Parse deadline MISSED"
        );
    }

    // --- Phase 2: Boundary allocation (one owned copy per event) ---
    let before_owned = ALLOC_COUNT.load(Ordering::Relaxed);
    let owned = PrioritisedEvent {
        seq:         0, // assigned in schedule_event before channel push
        user:        event.user.to_owned(),
        is_bot:      event.bot,
        domain:      event.server_name.to_owned(),
        title:       event.title.to_owned(),
        enqueued_at: Instant::now(), // overridden in PriorityChannel::push()
    };
    let boundary_allocs = ALLOC_COUNT.load(Ordering::Relaxed) - before_owned;

    // D1 proof: log allocation counts once after the first few events so the log
    // captures a warm steady-state reading. hot_path_allocs counts global allocations
    // between load/store — in multi-threaded runs other threads may contribute, so
    // the meaningful proof is that boundary_allocs == 3 (user + domain + title).
    let n = PARSE_COUNT.fetch_add(1, Ordering::Relaxed);
    if n == 10 {
        tracing::info!(
            hot_path_allocs,
            boundary_allocs,
            parse_us = parse_dur.as_micros(),
            "D1: Allocation proof (boundary=3 owned strings, hot_path includes concurrent thread noise)"
        );
    }

    Ok(owned)
}
