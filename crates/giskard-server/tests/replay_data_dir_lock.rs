//! The replay server takes the data-directory lock before it writes anything.
//!
//! This binary is a test fixture, but it is the one that *overwrites* `config.toml` in whatever
//! directory it is given — so pointed at a live data directory it would clobber a real
//! configuration. It therefore takes the same lock the real server does, and the ordering matters:
//! the lock has to come before the write, not after.

use std::process::Command;

#[test]
fn replay_refuses_a_locked_data_directory_without_touching_its_config() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config = tmp.path().join("config.toml");
    let original = b"# a real configuration the replay server must not clobber\n[server]\nbind = \"127.0.0.1:9999\"\n";
    std::fs::write(&config, original).unwrap();

    let held = giskard_persist::DataDirLock::try_acquire(tmp.path())
        .unwrap()
        .expect("stand in for a running giskard-server");

    // An already-bound port, so a regression that let the replay server past the lock fails to bind
    // and exits instead of serving forever — this test can fail, but it cannot hang.
    let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let bind = occupied.local_addr().unwrap().to_string();

    let output = Command::new(env!("CARGO_BIN_EXE_giskard-server-replay"))
        .env("GISKARD_DATA_DIR", tmp.path())
        .env("GISKARD_BIND", &bind)
        .output()
        .unwrap();

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "replay must refuse: {stderr}");
    assert!(
        stderr.contains("another Giskard process is using the data directory"),
        "replay must refuse *because of the lock*, not because of anything later: {stderr}"
    );
    assert_eq!(
        std::fs::read(&config).unwrap(),
        original,
        "the lock is taken before config.toml is rewritten, so a refused run leaves it untouched"
    );

    drop(held);
    assert!(
        giskard_persist::DataDirLock::try_acquire(tmp.path())
            .unwrap()
            .is_some(),
        "the refused run left no lock of its own behind"
    );
}
