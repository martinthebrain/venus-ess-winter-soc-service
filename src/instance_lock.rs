//! Exclusive process-instance ownership.

use crate::storage::open_secure_lock;
use fs2::FileExt;
use std::fs::File;
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;

#[derive(Debug)]
pub struct InstanceLock {
    _file: File,
}

impl InstanceLock {
    /// Acquire the process lock and retain it until this guard is dropped.
    ///
    /// # Errors
    ///
    /// Returns an error when the secure lock file cannot be opened or another
    /// process already owns the advisory lock.
    pub fn acquire(path: &Path) -> std::io::Result<Self> {
        let mut file = open_secure_lock(path)?;
        FileExt::try_lock_exclusive(&file).map_err(|error| {
            if error.kind() == std::io::ErrorKind::WouldBlock {
                std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    format!("service instance already holds {}", path.display()),
                )
            } else {
                error
            }
        })?;
        file.set_len(0)?;
        file.seek(SeekFrom::Start(0))?;
        writeln!(file, "{}", std::process::id())?;
        file.flush()?;
        Ok(Self { _file: file })
    }
}

#[cfg(test)]
mod tests {
    use super::InstanceLock;

    #[test]
    fn only_one_guard_can_hold_a_lock_file() {
        let directory = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let path = directory.path().join("service.lock");
        let first = InstanceLock::acquire(&path).unwrap_or_else(|_| std::process::abort());
        assert!(InstanceLock::acquire(&path).is_err());
        drop(first);
        assert!(InstanceLock::acquire(&path).is_ok());
    }
}
