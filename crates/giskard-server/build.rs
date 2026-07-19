//! Build script: stamp the binary with a human-readable version (git short hash + dirty flag) and
//! content hashes of the served assets. The version is shown in the UI's Settings panel; the asset
//! hashes name the served script/stylesheet (`/app.<hash>.js`) so an immutable cache can never hand
//! the browser a stale asset after an upgrade — the URL changes exactly when the bytes change.

use std::path::Path;
use std::process::Command;

use sha2::{Digest, Sha256};

fn main() {
    // Content hashes are the cache-correctness-critical part: recompute whenever an asset changes.
    println!("cargo:rerun-if-changed=static/app.js");
    println!("cargo:rerun-if-changed=static/app.css");
    println!("cargo:rerun-if-changed=static/index.html");
    // Best-effort freshness for the git hash: re-run when HEAD moves or the index/refs change. This
    // is inherently approximate (Cargo caches build scripts); `cargo install`/CI always build fresh.
    for rel in [".git/HEAD", ".git/index", ".git/refs/heads"] {
        let path = Path::new("../..").join(rel);
        if path.exists() {
            println!("cargo:rerun-if-changed={}", path.display());
        }
    }

    let git_hash = git_version().unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=GISKARD_GIT_HASH={git_hash}");
    println!(
        "cargo:rustc-env=GISKARD_JS_HASH={}",
        asset_hash("static/app.js")
    );
    println!(
        "cargo:rustc-env=GISKARD_CSS_HASH={}",
        asset_hash("static/app.css")
    );
}

/// `git rev-parse --short HEAD`, suffixed with `-dirty` when tracked files have uncommitted changes.
/// Returns `None` when git or a repository is unavailable (e.g. built from a packaged crate).
fn git_version() -> Option<String> {
    let short = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()?;
    if !short.status.success() {
        return None;
    }
    let mut hash = String::from_utf8(short.stdout).ok()?.trim().to_string();
    if hash.is_empty() {
        return None;
    }
    // Dirty iff the working tree or index differs from HEAD (tracked files only, matching the
    // `git describe --dirty` convention so stray untracked scratch files don't flip the flag).
    let clean = |args: &[&str]| {
        Command::new("git")
            .args(args)
            .status()
            .map(|s| s.success())
            .unwrap_or(true)
    };
    if !clean(&["diff", "--quiet"]) || !clean(&["diff", "--cached", "--quiet"]) {
        hash.push_str("-dirty");
    }
    Some(hash)
}

/// First 8 hex chars of the SHA-256 of a file — a short, filename-safe cache-busting token. A
/// missing file hashes to the digest of empty input rather than failing the build.
fn asset_hash(path: &str) -> String {
    let bytes = std::fs::read(path).unwrap_or_default();
    let digest = Sha256::digest(&bytes);
    digest.iter().take(4).map(|b| format!("{b:02x}")).collect()
}
