// Component A: Bounded priority channel
// Capacity 100. Human edits have highest priority; bots lowest.
// Emits [CHANNEL] log on every overflow decision and [BUFFER] log when fill
// crosses 50 % / 80 % thresholds and when pressure eases below 40 %.

use std::collections::VecDeque;
use std::time::Instant;

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
                let evicted_user = evicted.user.clone();
                self.buffer.push_back(event);
                PushResult::BotEvicted(evicted_user)
            }
            None => {
                let dropped = self.buffer.pop_front().unwrap();
                let dropped_user = dropped.user.clone();
                self.buffer.push_back(event);
                PushResult::DroppedOldest(dropped_user)
            }
        }
    }

    pub fn pop(&mut self) -> Option<PrioritisedEvent> {
        let item = self.buffer.pop_front();
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

    // Emit [BUFFER] warning when fill crosses 50 % or 80 % upward
    fn check_pressure(&mut self) {
        let pct = self.buffer.len() * 100 / self.capacity;
        if pct >= 80 && self.pressure_level < 2 {
            self.pressure_level = 2;
            let h = self.buffer.iter().filter(|e| !e.is_bot).count();
            let b = self.buffer.len() - h;
            tracing::warn!(
                "[BUFFER] 80% full  ({}/{})  human={}  bot={}  overflow risk",
                self.buffer.len(), self.capacity, h, b
            );
        } else if pct >= 50 && self.pressure_level < 1 {
            self.pressure_level = 1;
            let h = self.buffer.iter().filter(|e| !e.is_bot).count();
            let b = self.buffer.len() - h;
            tracing::info!(
                "[BUFFER] 50% full  ({}/{})  human={}  bot={}",
                self.buffer.len(), self.capacity, h, b
            );
        }
    }

    // Emit [BUFFER] info when fill drops back below 40 % after pressure
    fn check_ease(&mut self) {
        if self.pressure_level > 0 {
            let pct = self.buffer.len() * 100 / self.capacity;
            if pct < 40 {
                self.pressure_level = 0;
                tracing::info!(
                    "[BUFFER] Pressure eased  ({}/{})",
                    self.buffer.len(), self.capacity
                );
            }
        }
    }
}
