// Component A: Bounded priority channel
// Configured bounded capacity. Human edits have highest priority; bots lowest.
// Emits [CHANNEL] log on every overflow decision and [BUFFER] log when fill
// crosses 50 % / 80 % thresholds and when pressure eases below 40 %.

use std::collections::VecDeque;
use std::time::Instant;

use crate::config::{BUFFER_CRITICAL_PCT, BUFFER_EASE_PCT, BUFFER_WARN_PCT};
use crate::types::{PrioritisedEvent, PushResult};

pub struct PriorityChannel {
    buffer:         VecDeque<PrioritisedEvent>,
    capacity:       usize,
    pressure_level: u8,  // 0 = normal, 1 = ≥50 %, 2 = ≥80 %
}

impl PriorityChannel {
    pub fn new(capacity: usize) -> Self {
        Self {
            buffer:         VecDeque::with_capacity(capacity),
            capacity,
            pressure_level: 0,
        }
    }

    // -------------------------------------------------------------------
    // push — stamps enqueued_at, enforces priority, emits overflow logs
    // -------------------------------------------------------------------
    pub fn push(&mut self, mut event: PrioritisedEvent) -> PushResult {
        event.enqueued_at = Instant::now();

        if self.buffer.len() < self.capacity {
            self.buffer.push_back(event);
            self.check_pressure();
            return PushResult::Accepted;
        }

        // Channel full — compute queue composition for log context
        let humans = self.buffer.iter().filter(|e| !e.is_bot).count();
        let _bots  = self.buffer.len() - humans;
        let _fill  = self.buffer.len();
        let _cap   = self.capacity;

        if event.is_bot {
            return PushResult::DroppedIncoming;
        }

        // Incoming is human — evict oldest buffered bot if one exists
        let bot_pos = self.buffer.iter().position(|e| e.is_bot);
        match bot_pos {
            Some(pos) => {
                let evicted = self.buffer.remove(pos).unwrap();
                let (evicted_seq, evicted_user) = (evicted.seq, evicted.user.clone());
                self.buffer.push_back(event);
                PushResult::BotEvicted(evicted_seq, evicted_user)
            }
            None => {
                let dropped = self.buffer.pop_front().unwrap();
                let (dropped_seq, dropped_user) = (dropped.seq, dropped.user.clone());
                self.buffer.push_back(event);
                PushResult::DroppedOldest(dropped_seq, dropped_user)
            }
        }
    }

    pub fn pop(&mut self) -> Option<PrioritisedEvent> {
        // Execution-time priority: always dequeue a human before any bot,
        // regardless of arrival order.  If no humans are waiting, fall back
        // to the oldest bot (FIFO within each priority class).
        let human_pos = self.buffer.iter().position(|e| !e.is_bot);
        let item = match human_pos {
            Some(pos) => self.buffer.remove(pos),
            None      => self.buffer.pop_front(),
        };
        if item.is_some() {
            self.check_ease();
        }
        item
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    #[allow(dead_code)]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    // Emit structured BUFFER_80PCT / BUFFER_50PCT when fill crosses thresholds upward
    fn check_pressure(&mut self) {
        let fill = self.buffer.len();
        let pct  = fill * 100 / self.capacity;
        if pct >= BUFFER_CRITICAL_PCT && self.pressure_level < 2 {
            self.pressure_level = 2;
            let human_in_buf = self.buffer.iter().filter(|e| !e.is_bot).count();
            let bot_in_buf   = fill - human_in_buf;
            let buf = format!("{}/{}", fill, self.capacity);
            tracing::warn!(
                actor = "SYSTEM", evt = "BUFFER_80PCT",
                fill = %buf, human_in_buf, bot_in_buf
            );
        } else if pct >= BUFFER_WARN_PCT && self.pressure_level < 1 {
            self.pressure_level = 1;
            let human_in_buf = self.buffer.iter().filter(|e| !e.is_bot).count();
            let bot_in_buf   = fill - human_in_buf;
            let buf = format!("{}/{}", fill, self.capacity);
            tracing::info!(
                actor = "SYSTEM", evt = "BUFFER_50PCT",
                fill = %buf, human_in_buf, bot_in_buf
            );
        }
    }

    // Emit BUFFER_EASED when fill drops back below 40 % after pressure
    fn check_ease(&mut self) {
        if self.pressure_level > 0 {
            let fill = self.buffer.len();
            let pct  = fill * 100 / self.capacity;
            if pct < BUFFER_EASE_PCT {
                self.pressure_level = 0;
                let buf = format!("{}/{}", fill, self.capacity);
                tracing::info!(actor = "SYSTEM", evt = "BUFFER_EASED", fill = %buf);
            }
        }
    }
}
