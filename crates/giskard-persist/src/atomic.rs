//! Atomic file write utility (spec §5.4).
//!
//! Write to `<file>.tmp-<rand>`, fsync, rename over the target.
//! Guarantees crash consistency: a crash leaves either the old or the new complete file.

use std::io::Write;
use std::path::{Path, PathBuf};
use tokio::fs;

use crate::PersistError;

/// Atomically write `data` to `path`.
///
/// Creates parent directories if needed. Uses a temp file + rename for crash safety.
pub async fn atomic_write(path: &Path, data: &[u8]) -> Result<(), PersistError> {
    // Ensure parent directory exists.
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .await
            .map_err(|e| PersistError::Io(e.to_string()))?;
    }

    // Write to a temp file.
    let rand_suffix: String = std::iter::repeat_with(fastrand::alphanumeric)
        .take(8)
        .collect();
    let tmp_path = PathBuf::from(format!("{}.tmp-{rand_suffix}", path.display()));

    {
        let mut file =
            std::fs::File::create(&tmp_path).map_err(|e| PersistError::Io(e.to_string()))?;
        file.write_all(data)
            .map_err(|e| PersistError::Io(e.to_string()))?;
        file.sync_all()
            .map_err(|e| PersistError::Io(e.to_string()))?;
    }

    // Rename over the target (atomic on POSIX).
    fs::rename(&tmp_path, path)
        .await
        .map_err(|e| PersistError::Io(e.to_string()))?;

    Ok(())
}

/// Atomically write JSON-serializable data.
pub async fn atomic_write_json<T: serde::Serialize>(
    path: &Path,
    value: &T,
) -> Result<(), PersistError> {
    let json =
        serde_json::to_string_pretty(value).map_err(|e| PersistError::Serialize(e.to_string()))?;
    atomic_write(path, json.as_bytes()).await
}

/// Read and parse a JSON file. On parse failure, quarantine it and return `Corrupt`.
pub async fn read_json_or_quarantine<T: serde::de::DeserializeOwned>(
    path: &Path,
) -> Result<Option<T>, PersistError> {
    let data = match fs::read(path).await {
        Ok(data) => data,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(PersistError::Io(e.to_string())),
    };

    match serde_json::from_slice::<T>(&data) {
        Ok(value) => Ok(Some(value)),
        Err(e) => {
            tracing::warn!(path = %path.display(), error = %e, "corrupt JSON file, quarantining");
            let ts = chrono::Utc::now().format("%Y%m%dT%H%M%S");
            let corrupt_path = PathBuf::from(format!("{}.corrupt-{ts}", path.display()));
            if let Err(rename_error) = fs::rename(path, &corrupt_path).await {
                tracing::warn!(
                    path = %path.display(),
                    quarantine_path = %corrupt_path.display(),
                    error = %rename_error,
                    "failed to quarantine corrupt JSON file"
                );
            }
            Err(PersistError::Corrupt(format!("{}: {}", path.display(), e)))
        }
    }
}

/// Read and parse a JSON file, returning `None` if it doesn't exist (no quarantine).
pub async fn read_json<T: serde::de::DeserializeOwned>(
    path: &Path,
) -> Result<Option<T>, PersistError> {
    let data = match fs::read(path).await {
        Ok(data) => data,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(PersistError::Io(e.to_string())),
    };

    match serde_json::from_slice::<T>(&data) {
        Ok(value) => Ok(Some(value)),
        Err(e) => Err(PersistError::Deserialize(format!(
            "{}: {}",
            path.display(),
            e
        ))),
    }
}
