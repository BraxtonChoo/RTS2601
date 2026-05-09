// Component D: Leaderboard with three concurrent sync primitives
// All three are updated on every event so timings are directly comparable.
//
// Component C extension: LeaderboardManager tracks the last editor type per title.
// last_was_human(title) lets the processor loop reject bot edits that would
// overwrite a human's most recent update to that title.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use crate::config::LEADERBOARD_ROLLING_WINDOW;

#[derive(Debug, Default)]
pub struct Leaderboard {
    pub counts: HashMap<String, u64>,
}

impl Leaderboard {
    pub fn new() -> Self {
        Self { counts: HashMap::new() }
    }

    pub fn update(&mut self, domain: &str) {
        *self.counts.entry(domain.to_string()).or_insert(0) += 1;
    }

    pub fn top3(&self) -> Vec<(String, u64)> {
        let mut v: Vec<_> = self.counts.iter().map(|(k, v)| (k.clone(), *v)).collect();
        v.sort_by(|a, b| b.1.cmp(&a.1));
        v.into_iter().take(3).collect()
    }
}

// ---------------------------------------------------------------------------
// LeaderboardManager — runs all three sync primitives per event
// ---------------------------------------------------------------------------
pub struct LeaderboardManager {
    pub mutex_lb:  Arc<Mutex<Leaderboard>>,
    pub rwlock_lb: Arc<RwLock<Leaderboard>>,

    // Atomic version: fixed counters for top known domains
    pub atomic_en: Arc<AtomicU64>,
    pub atomic_de: Arc<AtomicU64>,
    pub atomic_fr: Arc<AtomicU64>,
    pub atomic_es: Arc<AtomicU64>,
    pub atomic_ja: Arc<AtomicU64>,

    mutex_times:  Vec<u64>,
    rwlock_times: Vec<u64>,
    atomic_times: Vec<u64>,

    // Component C: track the last editor per domain.
    // last_editor_bot:  true = bot, false = human
    // last_editor_user: username of the last editor (for ALLOWED / BLOCKED logs)
    last_editor_bot:  HashMap<String, bool>,
    last_editor_user: HashMap<String, String>,
}

impl LeaderboardManager {
    pub fn new() -> Self {
        Self {
            mutex_lb:    Arc::new(Mutex::new(Leaderboard::new())),
            rwlock_lb:   Arc::new(RwLock::new(Leaderboard::new())),
            atomic_en:   Arc::new(AtomicU64::new(0)),
            atomic_de:   Arc::new(AtomicU64::new(0)),
            atomic_fr:   Arc::new(AtomicU64::new(0)),
            atomic_es:   Arc::new(AtomicU64::new(0)),
            atomic_ja:   Arc::new(AtomicU64::new(0)),
            mutex_times:      Vec::new(),
            rwlock_times:     Vec::new(),
            atomic_times:     Vec::new(),
            last_editor_bot:  HashMap::new(),
            last_editor_user: HashMap::new(),
        }
    }

    // Component C: returns true when the most recent edit to this specific `title`
    // (article page) was by a human.  Keyed by page title so that a human editing
    // one article does not accidentally block bots on unrelated articles on the
    // same domain.
    // Called under the LeaderboardManager lock so the check + update are atomic.
    pub fn last_was_human(&self, title: &str) -> bool {
        self.last_editor_bot.get(title).map(|&is_bot| !is_bot).unwrap_or(false)
    }

    // Returns (mutex_ns, rwlock_ns, atomic_ns).
    // domain: used for leaderboard counts and atomic counter selection.
    // title:  used as the Component C protection key (page-level, not domain-level).
    // is_bot / user: recorded so last_was_human() works correctly per page.
    pub fn update_all(&mut self, domain: &str, is_bot: bool, user: &str, title: &str) -> (u64, u64, u64) {
        let t = Instant::now();
        { self.mutex_lb.lock().unwrap().update(domain); }
        let mutex_ns = t.elapsed().as_nanos() as u64;

        let t = Instant::now();
        { self.rwlock_lb.write().unwrap().update(domain); }
        let rwlock_ns = t.elapsed().as_nanos() as u64;

        let t = Instant::now();
        let counter = match domain {
            "en.wikipedia.org" => &self.atomic_en,
            "de.wikipedia.org" => &self.atomic_de,
            "fr.wikipedia.org" => &self.atomic_fr,
            "es.wikipedia.org" => &self.atomic_es,
            "ja.wikipedia.org" => &self.atomic_ja,
            _                  => &self.atomic_en,
        };
        counter.fetch_add(1, Ordering::Relaxed);
        let atomic_ns = t.elapsed().as_nanos() as u64;

        // Rolling window of last 1000 samples
        self.mutex_times.push(mutex_ns);
        self.rwlock_times.push(rwlock_ns);
        self.atomic_times.push(atomic_ns);
        if self.mutex_times.len()  > LEADERBOARD_ROLLING_WINDOW { self.mutex_times.remove(0); }
        if self.rwlock_times.len() > LEADERBOARD_ROLLING_WINDOW { self.rwlock_times.remove(0); }
        if self.atomic_times.len() > LEADERBOARD_ROLLING_WINDOW { self.atomic_times.remove(0); }

        // Component C: record last editor keyed by page title (not domain)
        // so protection is scoped to the exact article, not the whole domain.
        self.last_editor_bot.insert(title.to_string(), is_bot);
        self.last_editor_user.insert(title.to_string(), user.to_string());

        tracing::debug!(mutex_ns, rwlock_ns, atomic_ns, domain, title, is_bot, "Component D: Sync timings");
        (mutex_ns, rwlock_ns, atomic_ns)
    }

    pub fn avg_mutex_ns(&self) -> f64 {
        avg(&self.mutex_times)
    }
    pub fn avg_rwlock_ns(&self) -> f64 {
        avg(&self.rwlock_times)
    }
    pub fn avg_atomic_ns(&self) -> f64 {
        avg(&self.atomic_times)
    }

    pub fn top3(&self) -> Vec<(String, u64)> {
        self.rwlock_lb.read().unwrap().top3()
    }
}

fn avg(v: &[u64]) -> f64 {
    if v.is_empty() { return 0.0; }
    v.iter().sum::<u64>() as f64 / v.len() as f64
}
