//! Volatile bounded diagnostics logging.

use crate::clock::{Clock, SystemClock};
use crate::config::{LOG_MAX_BYTES, LOG_TRUNCATE_BYTES};
use crate::storage::{atomic_write, open_secure_append, read_secure_bounded};
use std::io::Write;
use std::path::{Path, PathBuf};

pub trait LogSink {
    fn log(&mut self, message: &str);
}

pub struct RamLogger {
    path: PathBuf,
}

impl RamLogger {
    #[must_use]
    pub const fn new(path: PathBuf) -> Self {
        Self { path }
    }

    fn append(&self, entry: &str) -> std::io::Result<()> {
        let mut file = open_secure_append(&self.path)?;
        writeln!(file, "{entry}")?;
        let size = file.metadata()?.len();
        drop(file);
        if size > LOG_MAX_BYTES {
            retain_tail(&self.path, LOG_TRUNCATE_BYTES)?;
        }
        Ok(())
    }
}

impl LogSink for RamLogger {
    fn log(&mut self, message: &str) {
        let now = SystemClock.local_date_time();
        let entry = format!(
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}: {message}",
            now.year, now.month, now.day, now.hour, now.minute, now.second
        );
        println!("{entry}");
        let _ = self.append(&entry);
    }
}

fn retain_tail(path: &Path, keep: u64) -> std::io::Result<()> {
    let maximum = LOG_MAX_BYTES.saturating_add(keep);
    let data = read_secure_bounded(path, maximum)?.unwrap_or_default();
    let start = data
        .len()
        .saturating_sub(usize::try_from(keep).unwrap_or(usize::MAX));
    atomic_write(path, &data[start..], false)
}

#[cfg(test)]
mod tests {
    use super::{LogSink, RamLogger, retain_tail};
    use std::fs;
    use std::os::unix::fs::symlink;

    #[test]
    fn tail_rotation_is_bounded() {
        let dir = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let path = dir.path().join("log");
        fs::write(&path, b"0123456789").unwrap_or_else(|_| std::process::abort());
        retain_tail(&path, 4).unwrap_or_else(|_| std::process::abort());
        assert_eq!(fs::read(&path).ok().as_deref(), Some(&b"6789"[..]));
        let mut logger = RamLogger::new(path);
        logger.log("test");
    }

    #[test]
    fn logger_does_not_follow_existing_symlinks() {
        let dir = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let victim = dir.path().join("victim");
        let log = dir.path().join("log");
        fs::write(&victim, b"untouched").unwrap_or_else(|_| std::process::abort());
        symlink(&victim, &log).unwrap_or_else(|_| std::process::abort());

        let mut logger = RamLogger::new(log);
        logger.log("must not reach victim");

        assert_eq!(fs::read(victim).ok().as_deref(), Some(&b"untouched"[..]));
    }
}
