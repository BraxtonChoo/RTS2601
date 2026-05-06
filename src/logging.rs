// Structured log formatter for RTS2601.
//
// Output format — user events:
//   MM/DD/YY HH:MM:SS.mmm | LEVEL | ACTOR        | KIND  | DOMAIN           | EVENT      key=val ...
//
// Output format — system events (no KIND/DOMAIN columns):
//   MM/DD/YY HH:MM:SS.mmm | LEVEL | SYSTEM | EVENT            key=val ...
//
// Call sites use structured tracing fields:
//   actor = "SYSTEM" | %username       (string)
//   kind  = "HUMAN"  | "BOT" | "-"     (string, only for user events)
//   domain = %event.domain             (string, only for user events)
//   evt   = "DONE"   | "ENQUEUED" …    (string — the event name)
//   seq   = event.seq                  (u64, printed first after evt name)
//   … remaining key=val fields in declaration order

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::sync::{Mutex, MutexGuard};

use chrono::Local;
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields, MakeWriter};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

// ── File writer ───────────────────────────────────────────────────────────────

struct FileWriter {
    file: Mutex<File>,
}

impl FileWriter {
    fn new(path: &str) -> io::Result<Self> {
        std::fs::create_dir_all("logs")?;
        Ok(Self {
            file: Mutex::new(
                OpenOptions::new().append(true).create(true).open(path)?,
            ),
        })
    }
}

struct LockedWriter<'a> {
    guard: MutexGuard<'a, File>,
}

impl Write for LockedWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> { self.guard.write(buf) }
    fn flush(&mut self) -> io::Result<()> { self.guard.flush() }
}

impl<'a> MakeWriter<'a> for FileWriter {
    type Writer = LockedWriter<'a>;
    fn make_writer(&'a self) -> LockedWriter<'a> {
        LockedWriter { guard: self.file.lock().unwrap() }
    }
}

// ── Field visitor ─────────────────────────────────────────────────────────────

#[derive(Default)]
struct RtsVisitor {
    actor:   Option<String>,
    kind:    Option<String>,
    domain:  Option<String>,
    evt:     Option<String>,
    seq:     Option<u64>,
    message: Option<String>,
    fields:  Vec<(String, String)>,
}

impl RtsVisitor {
    fn store(&mut self, name: &str, value: String) {
        match name {
            "actor"   => self.actor   = Some(value),
            "kind"    => self.kind    = Some(value),
            "domain"  => self.domain  = Some(value),
            "evt"     => self.evt     = Some(value),
            // suppress empty message strings that tracing emits for field-only events
            "message" => {
                if !value.is_empty() && value != "\"\"" {
                    self.message = Some(value);
                }
            }
            other => self.fields.push((other.to_string(), value)),
        }
    }
}

impl Visit for RtsVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.store(field.name(), value.to_string());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        // %expr uses DisplayValue whose Debug impl calls Display — no surrounding quotes
        self.store(field.name(), format!("{:?}", value));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        if field.name() == "seq" {
            self.seq = Some(value);
        } else {
            self.fields.push((field.name().to_string(), value.to_string()));
        }
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.fields.push((field.name().to_string(), value.to_string()));
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        // Use 2 dp for ms-range values; the call site controls what's passed
        self.fields.push((field.name().to_string(), format!("{:.2}", value)));
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.fields.push((field.name().to_string(), value.to_string()));
    }
}

// ── Custom event formatter ────────────────────────────────────────────────────

pub struct RtsFormat;

impl<S, N> FormatEvent<S, N> for RtsFormat
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        _ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> std::fmt::Result {
        let mut vis = RtsVisitor::default();
        event.record(&mut vis);

        // Wall-clock timestamp with millisecond precision
        let ts = Local::now().format("%m/%d/%y %H:%M:%S%.3f");

        let level = match *event.metadata().level() {
            Level::ERROR => "ERROR",
            Level::WARN  => "WARN ",
            Level::INFO  => "INFO ",
            Level::DEBUG => "DEBUG",
            Level::TRACE => "TRACE",
        };

        let actor = vis.actor.as_deref().unwrap_or("SYSTEM");
        let evt   = vis.evt.as_deref()
            .or_else(|| vis.message.as_deref().filter(|m| !m.is_empty()))
            .unwrap_or("-");

        // Header columns differ for user vs system events
        match (vis.kind.as_deref(), vis.domain.as_deref()) {
            (Some(kind), Some(domain)) => {
                // User event: full column set
                write!(
                    writer,
                    "{} | {} | {:<12} | {:<5} | {:<16} | {:<10}",
                    ts, level, actor, kind, domain, evt
                )?;
            }
            _ => {
                // System event: no KIND / DOMAIN columns
                write!(writer, "{} | {} | {:<6} | {:<16}", ts, level, actor, evt)?;
            }
        }

        // seq always appears immediately after the event name when present
        if let Some(seq) = vis.seq {
            write!(writer, " seq={}", seq)?;
        }

        // Remaining key=val pairs in declaration order
        for (k, v) in &vis.fields {
            write!(writer, " {}={}", k, v)?;
        }

        writeln!(writer)
    }
}

// ── Init ──────────────────────────────────────────────────────────────────────

pub fn init_logging() {
    let writer = FileWriter::new("logs/rts2601.log")
        .expect("could not open logs/rts2601.log");

    tracing_subscriber::registry()
        .with(
            fmt::layer()
                .with_writer(writer)
                .event_format(RtsFormat)
                .with_ansi(false),
        )
        .with(EnvFilter::new(
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "warn,rts2601=info".to_string()),
        ))
        .init();
}
