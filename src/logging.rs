use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::sync::{Mutex, MutexGuard};

use chrono::Local;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::fmt::time::FormatTime;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

struct FileWriter {
    file: Mutex<File>,
}

impl FileWriter {
    fn new(path: &str) -> io::Result<Self> {
        std::fs::create_dir_all("logs")?;
        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(path)?;
        Ok(Self { file: Mutex::new(file) })
    }
}

struct LockedFileWriter<'a> {
    guard: MutexGuard<'a, File>,
}

impl Write for LockedFileWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.guard.write(buf)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.guard.flush()
    }
}

impl<'a> MakeWriter<'a> for FileWriter {
    type Writer = LockedFileWriter<'a>;
    fn make_writer(&'a self) -> Self::Writer {
        LockedFileWriter { guard: self.file.lock().unwrap() }
    }
}

struct LocalTimestamp;

impl FormatTime for LocalTimestamp {
    fn format_time(&self, w: &mut tracing_subscriber::fmt::format::Writer<'_>) -> std::fmt::Result {
        write!(w, "{}", Local::now().format("%d/%m/%y %H:%M:%S"))
    }
}

// stdout layer omitted: ratatui owns the terminal.
// FileWriter uses raw File (no BufWriter) so every write() goes to the OS
// immediately and is visible to any log tail/viewer without waiting for flush.
pub fn init_logging() {
    let writer = FileWriter::new("logs/rts2601.log")
        .expect("could not open logs/rts2601.log");

    tracing_subscriber::registry()
        .with(
            fmt::layer()
                .with_writer(writer)
                .with_timer(LocalTimestamp)
                .with_target(false)
                .with_thread_ids(false)
                .with_ansi(false),
        )
        .with(EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "warn,rts2601=info".to_string()),
        ))
        .init();
}
