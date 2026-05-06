// Component B: Zero-copy parser
//
// WikiEvent<'a> borrows &str slices directly from the raw JSON buffer.
// No heap allocation occurs in the "hot path" (Phase 1).
// Phase 2 converts to owned Strings — exactly 3 allocations (user/domain/title).
//
// raw_len, parse_us, allocs are stored in the returned PrioritisedEvent so that
// schedule_event can emit INGESTED and PARSED structured log lines with the
// correct seq number once it has been assigned.

use serde::Deserialize;
use std::time::Instant;

use crate::allocator::ALLOC_COUNT;
use crate::types::PrioritisedEvent;

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
// Returns a PrioritisedEvent with:
//   raw_len  = raw_json.len()
//   parse_us = Phase-1 parse duration in microseconds
//   allocs   = heap allocations during the zero-copy phase (should be 0)
//
// Logging is intentionally absent here — schedule_event has the seq number
// and emits the INGESTED / PARSED structured lines.
// ---------------------------------------------------------------------------
pub fn parse_event(raw_json: &str) -> Result<PrioritisedEvent, String> {
    let raw_len     = raw_json.len();
    let parse_start = Instant::now();

    // Phase 1: zero-copy parse
    let before_parse  = ALLOC_COUNT.load(std::sync::atomic::Ordering::Relaxed);
    let event: WikiEvent = serde_json::from_str(raw_json)
        .map_err(|e| format!("parse error: {e}"))?;
    let hot_path_allocs = ALLOC_COUNT.load(std::sync::atomic::Ordering::Relaxed)
        .saturating_sub(before_parse);
    let parse_us = parse_start.elapsed().as_micros() as u64;

    // Phase 2: boundary allocation — exactly 3 owned Strings
    let owned = PrioritisedEvent {
        seq:         0,                   // assigned in schedule_event
        user:        event.user.to_owned(),
        is_bot:      event.bot,
        domain:      event.server_name.to_owned(),
        title:       event.title.to_owned(),
        enqueued_at: Instant::now(),      // overridden in PriorityChannel::push()
        raw_len,
        parse_us,
        allocs:      hot_path_allocs,
    };

    Ok(owned)
}
