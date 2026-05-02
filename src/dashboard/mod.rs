use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crossterm::{
    event::{self, Event, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Gauge, List, ListItem, Paragraph},
    Terminal,
};

use crate::leaderboard::LeaderboardManager;
use crate::types::{EventStatus, SharedState};

fn format_runtime(secs: u64) -> String {
    format!("{:02}:{:02}:{:02}", secs / 3600, (secs % 3600) / 60, secs % 60)
}

pub fn run_dashboard(
    state:       Arc<SharedState>,
    leaderboard: Arc<Mutex<LeaderboardManager>>,
    exit_flag:   Arc<AtomicBool>,
) -> Result<(), Box<dyn std::error::Error>> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    loop {
        let stats    = state.stats.lock().unwrap().clone();
        let top3     = leaderboard.lock().unwrap().top3();
        let runtime  = state.start_time.elapsed().as_secs();
        let pipeline = state.pipeline_mode.to_string();

        terminal.draw(|f| {
            let size = f.size();

            let rows = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Length(3),
                    Constraint::Length(9),
                    Constraint::Length(7),
                    Constraint::Min(0),
                ])
                .split(size);

            // --- Title bar ---
            let (status_text, status_color) = if stats.degraded_mode {
                ("⚠ DEGRADED", Color::Red)
            } else {
                ("● LIVE", Color::Green)
            };
            let title = Paragraph::new(Line::from(vec![
                Span::styled(
                    "  RTS2601 Wikipedia Realtime Pipeline   ",
                    Style::default().fg(Color::White).add_modifier(Modifier::BOLD),
                ),
                Span::styled(
                    format!("[{status_text}]  Mode: {pipeline}  Runtime: {}", format_runtime(runtime)),
                    Style::default().fg(status_color),
                ),
            ]))
            .block(Block::default().borders(Borders::ALL));
            f.render_widget(title, rows[0]);

            // --- Row 1: Leaderboard | Pipeline status | Latency monitor ---
            let row1 = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([
                    Constraint::Percentage(33),
                    Constraint::Percentage(34),
                    Constraint::Percentage(33),
                ])
                .split(rows[1]);

            let lb_items: Vec<ListItem> = top3
                .iter()
                .enumerate()
                .map(|(i, (domain, count))| {
                    ListItem::new(format!("{}. {:20} {:>6}", i + 1, domain, count))
                })
                .collect();
            f.render_widget(
                List::new(lb_items)
                    .block(Block::default().title(" TOP 3 DOMAINS ").borders(Borders::ALL)),
                row1[0],
            );

            let pipeline_text = vec![
                Line::from(format!(" Mode:      {pipeline}")),
                Line::from(format!(" TPS:       {:.0}/s", stats.throughput_per_sec)),
                Line::from(format!(" Bot evict: {}", stats.bot_evictions)),
                Line::from(format!(" Bot drops: {}", stats.bot_drops)),
                Line::from(format!(" Hum drops: {}", stats.human_drops)),
                Line::from(format!(" Overflows: {}", stats.overflow_events)),
                Line::from(format!(" Reconnect: {}", stats.reconnect_count)),
            ];
            f.render_widget(
                Paragraph::new(pipeline_text)
                    .block(Block::default().title(" PIPELINE STATUS ").borders(Borders::ALL)),
                row1[1],
            );

            let hc = if stats.human_drift_p99 < 2.0 { Color::Green } else { Color::Red };
            let bc = if stats.bot_drift_p99   < 2.0 { Color::Green } else { Color::Yellow };
            let latency_text = vec![
                Line::from(Span::styled(
                    format!(" Human p50: {:.2}ms", stats.human_drift_p50),
                    Style::default().fg(hc),
                )),
                Line::from(Span::styled(
                    format!(" Human p90: {:.2}ms", stats.human_drift_p90),
                    Style::default().fg(hc),
                )),
                Line::from(Span::styled(
                    format!(" Human p99: {:.2}ms", stats.human_drift_p99),
                    Style::default().fg(hc),
                )),
                Line::from(Span::styled(
                    format!(" Bot   p99: {:.2}ms", stats.bot_drift_p99),
                    Style::default().fg(bc),
                )),
                Line::from(format!(" Misses:   {}", stats.deadline_misses)),
            ];
            f.render_widget(
                Paragraph::new(latency_text)
                    .block(Block::default().title(" LATENCY MONITOR ").borders(Borders::ALL)),
                row1[2],
            );

            // --- Row 2: Buffer gauge | Sync benchmark | Watchdog ---
            let row2 = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([
                    Constraint::Percentage(33),
                    Constraint::Percentage(34),
                    Constraint::Percentage(33),
                ])
                .split(rows[2]);

            let fill_pct = (stats.current_buffer_fill as f64 / 100.0 * 100.0) as u16;
            let gauge_color = if fill_pct > 80 {
                Color::Red
            } else if fill_pct > 50 {
                Color::Yellow
            } else {
                Color::Green
            };
            f.render_widget(
                Gauge::default()
                    .block(Block::default().title(" CHANNEL BUFFER ").borders(Borders::ALL))
                    .gauge_style(Style::default().fg(gauge_color))
                    .percent(fill_pct)
                    .label(format!("{}/100", stats.current_buffer_fill)),
                row2[0],
            );

            let sync_text = vec![
                Line::from(format!(" Mutex:  {:>8.0} ns", stats.avg_mutex_ns)),
                Line::from(format!(" RwLock: {:>8.0} ns", stats.avg_rwlock_ns)),
                Line::from(Span::styled(
                    format!(" Atomic: {:>8.0} ns ◄ fastest", stats.avg_atomic_ns),
                    Style::default().fg(Color::Green),
                )),
                Line::from(format!(" Total:  {:>8}", stats.events_processed)),
            ];
            f.render_widget(
                Paragraph::new(sync_text)
                    .block(Block::default().title(" SYNC BENCHMARK ").borders(Borders::ALL)),
                row2[1],
            );

            let wd_color  = if stats.degraded_mode { Color::Red } else { Color::Green };
            let wd_status = if stats.degraded_mode { "⚠ DEGRADED" } else { "● CONNECTED" };
            let watchdog_text = vec![
                Line::from(Span::styled(
                    format!(" Status: {wd_status}"),
                    Style::default().fg(wd_color),
                )),
                Line::from(format!(" Reconnects: {}", stats.reconnect_count)),
                Line::from(format!(" Degraded:   {} times", stats.degraded_activations)),
            ];
            f.render_widget(
                Paragraph::new(watchdog_text)
                    .block(Block::default().title(" WATCHDOG ").borders(Borders::ALL)),
                row2[2],
            );

            // --- Row 3: Live event feed ---
            let feed_items: Vec<ListItem> = stats
                .recent_events
                .iter()
                .rev()
                .map(|e| {
                    let (color, tag) = match e.status {
                        EventStatus::Processed => {
                            (if e.is_bot { Color::Gray } else { Color::Green }, "✓")
                        }
                        EventStatus::BotEvicted     => (Color::Yellow,  "EVICTED"),
                        EventStatus::BotDropped     => (Color::Red,     "DROPPED"),
                        EventStatus::DeadlineMissed => (Color::Magenta, "MISS"),
                    };
                    let kind = if e.is_bot { "BOT  " } else { "HUMAN" };
                    ListItem::new(Span::styled(
                        format!(
                            " [{}] {:16} {:24} {}  {}",
                            e.timestamp, e.user, e.domain, kind, tag
                        ),
                        Style::default().fg(color),
                    ))
                })
                .collect();
            f.render_widget(
                List::new(feed_items).block(
                    Block::default()
                        .title(" LIVE EVENT FEED (q to quit) ")
                        .borders(Borders::ALL),
                ),
                rows[3],
            );
        })?;

        if event::poll(Duration::from_millis(100))? {
            if let Event::Key(key) = event::read()? {
                if key.code == KeyCode::Char('q') {
                    exit_flag.store(true, Ordering::Relaxed);
                    break;
                }
            }
        }
    }

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    Ok(())
}
