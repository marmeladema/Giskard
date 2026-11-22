//! Where the data directory is: one rule for `giskard-server` and `giskard-admin`, so the admin
//! tool always works on the directory the server uses.

use std::path::PathBuf;

/// The environment variable that names the data directory.
pub const DATA_DIR_ENV: &str = "GISKARD_DATA_DIR";

/// The data directory: [`DATA_DIR_ENV`] when it is set, else `$HOME/.local/share/giskard` (`.`
/// standing in for an unset `HOME`).
pub fn default_data_dir() -> PathBuf {
    if let Ok(dir) = std::env::var(DATA_DIR_ENV) {
        return PathBuf::from(dir);
    }
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
    PathBuf::from(home).join(".local/share/giskard")
}
