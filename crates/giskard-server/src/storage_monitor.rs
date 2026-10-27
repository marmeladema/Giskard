//! Filesystem capacity is process-wide health state, never project or thread identity.
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use giskard_persist::config::StorageMonitorConfig;
use giskard_proto::{StorageFilesystem, StorageStatus};
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;
use tracing::{Instrument, info, warn};

use crate::{AppShutdown, hub::Hub};

const GIB: u64 = 1024 * 1024 * 1024;
const SAMPLE_TIMEOUT: Duration = Duration::from_secs(10);

/// Read-only projection handle. Only the monitor task publishes snapshots.
#[derive(Clone)]
pub struct StorageMonitor {
    status: watch::Receiver<StorageStatus>,
}

impl Default for StorageMonitor {
    fn default() -> Self {
        Self {
            status: watch::channel(initial_status(false)).1,
        }
    }
}

impl StorageMonitor {
    pub fn current(&self) -> StorageStatus {
        self.status.borrow().clone()
    }

    /// Start once, after the server binds and before accepting connections. A disabled monitor
    /// performs no filesystem work. The caller joins this task during graceful shutdown.
    pub fn spawn(
        data_dir: PathBuf,
        config: StorageMonitorConfig,
        hub: Arc<Hub>,
        shutdown: AppShutdown,
    ) -> (Self, JoinHandle<()>) {
        spawn_with_probe(
            data_dir,
            config,
            hub,
            shutdown,
            Arc::new(SystemProbe),
            SAMPLE_TIMEOUT,
        )
    }
}

fn initial_status(enabled: bool) -> StorageStatus {
    StorageStatus {
        revision: 0,
        checked_at: None,
        enabled,
        filesystems: Vec::new(),
        issues: Vec::new(),
    }
}

#[derive(Clone)]
struct Target {
    path: PathBuf,
    threshold: u64,
}

fn targets(data_dir: PathBuf, config: &StorageMonitorConfig) -> Vec<Target> {
    let mut targets = vec![Target {
        path: data_dir,
        threshold: config.minimum_available_gib * GIB,
    }];
    targets.extend(config.additional_filesystems.iter().map(|entry| {
        Target {
            path: entry.path.clone(),
            threshold: entry
                .minimum_available_gib
                .unwrap_or(config.minimum_available_gib)
                * GIB,
        }
    }));
    targets
}

#[derive(Clone, Copy)]
struct Capacity {
    available: u64,
    total: u64,
}

trait Probe: Send + Sync {
    fn capacity(&self, path: &Path) -> Result<Capacity, String>;
}

struct SystemProbe;
impl Probe for SystemProbe {
    fn capacity(&self, path: &Path) -> Result<Capacity, String> {
        let file = std::fs::File::open(path).map_err(|error| error.to_string())?;
        let stats = rustix::fs::fstatvfs(&file).map_err(|error| error.to_string())?;
        let bytes = |blocks| {
            u64::try_from(u128::from(blocks) * u128::from(stats.f_frsize))
                .map_err(|_| "Filesystem capacity exceeds supported byte range".to_string())
        };
        Ok(Capacity {
            available: bytes(stats.f_bavail)?,
            total: bytes(stats.f_blocks)?,
        })
    }
}

fn initial_entry(index: usize, target: &Target) -> StorageFilesystem {
    StorageFilesystem {
        id: format!("path-{index}"),
        paths: vec![target.path.display().to_string()],
        available_bytes: None,
        total_bytes: None,
        minimum_available_bytes: target.threshold,
        low: false,
        error: None,
    }
}

fn apply_reading(entry: &mut StorageFilesystem, reading: Result<Capacity, String>) {
    match reading {
        Ok(capacity) => {
            entry.available_bytes = Some(capacity.available);
            entry.total_bytes = Some(capacity.total);
            let recovery =
                entry
                    .minimum_available_bytes
                    .saturating_add(if entry.low { GIB } else { 0 });
            entry.low = capacity.available < recovery;
            entry.error = None;
        }
        Err(error) => entry.error = Some(error),
    }
}

fn log_changes(previous: &StorageStatus, next: &StorageStatus) {
    for entry in &next.filesystems {
        let prior = previous
            .filesystems
            .iter()
            .find(|prior| prior.id == entry.id);
        if entry.low && !prior.is_some_and(|prior| prior.low) {
            warn!(action = "storage_low", path = ?entry.paths,
                available_bytes = entry.available_bytes, minimum_available_bytes = entry.minimum_available_bytes,
                "filesystem available space is below the configured threshold");
        } else if !entry.low && prior.is_some_and(|prior| prior.low) {
            info!(action = "storage_recovered", path = ?entry.paths,
                available_bytes = entry.available_bytes, "filesystem available space recovered");
        }
        if entry.error.as_deref() != prior.and_then(|prior| prior.error.as_deref()) {
            if let Some(error) = &entry.error {
                warn!(action = "storage_sample_failed", path = ?entry.paths, %error,
                    "could not check filesystem capacity");
            } else {
                info!(action = "storage_sample_recovered", path = ?entry.paths, "filesystem sampling recovered");
            }
        }
    }
}

async fn publish(sender: &watch::Sender<StorageStatus>, hub: &Hub, mut next: StorageStatus) {
    let previous = sender.borrow().clone();
    next.revision = previous.revision + 1;
    log_changes(&previous, &next);
    sender.send_replace(next.clone());
    hub.publish_storage_status(next).await;
}

/// A path owns at most one outstanding OS call. Timeouts report stale state while that call
/// finishes; unrelated paths continue on their own schedules. OS workers own no server state.
async fn monitor_path(
    index: usize,
    target: Target,
    mut entry: StorageFilesystem,
    interval_seconds: u64,
    sample_timeout: Duration,
    probe: Arc<dyn Probe>,
    updates: tokio::sync::mpsc::Sender<(usize, StorageFilesystem)>,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(interval_seconds));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        let job_path = target.path.clone();
        let job_probe = probe.clone();
        let span = tracing::Span::current();
        let (done, mut job) = oneshot::channel();
        if let Err(error) = std::thread::Builder::new()
            .name("giskard-storage".into())
            .spawn(move || {
                let reading = span.in_scope(|| job_probe.capacity(&job_path));
                let _ = done.send(reading);
            })
        {
            apply_reading(
                &mut entry,
                Err(format!("Could not start storage check: {error}")),
            );
        } else {
            let reading = match tokio::time::timeout(sample_timeout, &mut job).await {
                Ok(result) => result,
                Err(_) => {
                    apply_reading(
                        &mut entry,
                        Err("Storage check exceeded 10 seconds; waiting for the filesystem".into()),
                    );
                    if updates.send((index, entry.clone())).await.is_err() {
                        return;
                    }
                    // Never replace a timed-out worker: doing so could accumulate stuck calls.
                    job.await
                }
            };
            apply_reading(
                &mut entry,
                reading.unwrap_or_else(|error| Err(format!("Storage check task failed: {error}"))),
            );
        }
        if updates.send((index, entry.clone())).await.is_err() {
            return;
        }
    }
}

fn spawn_with_probe(
    data_dir: PathBuf,
    config: StorageMonitorConfig,
    hub: Arc<Hub>,
    shutdown: AppShutdown,
    probe: Arc<dyn Probe>,
    sample_timeout: Duration,
) -> (StorageMonitor, JoinHandle<()>) {
    let (sender, status) = watch::channel(initial_status(config.enabled));
    let task = tokio::spawn(async move {
        if !config.enabled { return; }
        let paths = targets(data_dir, &config);
        let mut snapshot = initial_status(true);
        snapshot.filesystems = paths.iter().enumerate().map(|(index, target)| initial_entry(index, target)).collect();
        let (updates, mut incoming) = tokio::sync::mpsc::channel(16);
        let mut workers = tokio::task::JoinSet::new();
        for (index, target) in paths.into_iter().enumerate() {
            workers.spawn(monitor_path(index, target, snapshot.filesystems[index].clone(),
                config.check_interval_seconds, sample_timeout, probe.clone(), updates.clone()).in_current_span());
        }
        drop(updates);
        loop {
            tokio::select! {
                biased;
                () = shutdown.wait() => break,
                result = workers.join_next(), if !workers.is_empty() => {
                    match result {
                        Some(Err(error)) => warn!(action = "storage_monitor_worker_failed", %error, "storage path monitor failed"),
                        Some(Ok(())) => warn!(action = "storage_monitor_worker_stopped", "storage path monitor stopped unexpectedly"),
                        None => {}
                    }
                }
                update = incoming.recv() => {
                    let Some((index, entry)) = update else { break; };
                    snapshot.filesystems[index] = entry;
                    snapshot.checked_at = Some(Utc::now());
                    publish(&sender, &hub, snapshot.clone()).await;
                }
            }
        }
        // Aborting async path tasks drops their OS-worker receivers without joining potentially
        // uninterruptible filesystem calls, keeping runtime shutdown prompt.
        workers.shutdown().await;
        info!(action = "storage_monitor_stopped", "storage monitor stopped");
    }.in_current_span());
    (StorageMonitor { status }, task)
}

#[cfg(test)]
mod tests {
    use super::*;
    use giskard_persist::config::MonitoredFilesystemConfig;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use std::sync::{Condvar, Mutex};
    use tracing_test::traced_test;

    fn entry(path: &str, threshold: u64) -> StorageFilesystem {
        initial_entry(
            0,
            &Target {
                path: path.into(),
                threshold,
            },
        )
    }

    fn reading(available: u64) -> Result<Capacity, String> {
        Ok(Capacity {
            available,
            total: 100 * GIB,
        })
    }

    #[test]
    fn thresholds_are_independent_even_for_the_same_path() {
        let config = StorageMonitorConfig {
            additional_filesystems: vec![MonitoredFilesystemConfig {
                path: "/data".into(),
                minimum_available_gib: Some(20),
            }],
            ..Default::default()
        };
        let targets = targets("/data".into(), &config);
        assert_eq!(targets.len(), 2);
        let mut entries: Vec<_> = targets
            .iter()
            .enumerate()
            .map(|(index, target)| initial_entry(index, target))
            .collect();
        for entry in &mut entries {
            apply_reading(entry, reading(15 * GIB));
        }
        assert!(!entries[0].low);
        assert!(entries[1].low);
        assert_ne!(entries[0].id, entries[1].id);
        assert_eq!(entries[0].minimum_available_bytes, 10 * GIB);
        assert_eq!(entries[1].minimum_available_bytes, 20 * GIB);
    }

    #[test]
    fn missing_path_retains_its_low_reading_and_threshold() {
        let mut missing = entry("/build", 20 * GIB);
        let mut data = entry("/data", 10 * GIB);
        apply_reading(&mut missing, reading(15 * GIB));
        apply_reading(&mut data, reading(15 * GIB));
        apply_reading(&mut missing, Err("No such file or directory".into()));
        assert!(missing.low);
        assert_eq!(missing.available_bytes, Some(15 * GIB));
        assert_eq!(missing.minimum_available_bytes, 20 * GIB);
        assert!(missing.error.is_some());
        assert!(!data.low);
        assert!(data.error.is_none());
        apply_reading(&mut missing, reading(21 * GIB));
        assert!(!missing.low);
        assert!(missing.error.is_none());
    }

    #[test]
    fn threshold_equality_and_recovery_hysteresis() {
        let mut sample = entry("/data", 10 * GIB);
        apply_reading(&mut sample, reading(10 * GIB));
        assert!(!sample.low);
        apply_reading(&mut sample, reading(10 * GIB - 1));
        assert!(sample.low);
        apply_reading(&mut sample, reading(11 * GIB - 1));
        assert!(sample.low);
        apply_reading(&mut sample, reading(11 * GIB));
        assert!(!sample.low);
    }

    #[test]
    fn system_probe_follows_symlinks_and_reports_missing_paths() {
        let directory = tempfile::tempdir().unwrap();
        let link = directory.path().join("link");
        std::os::unix::fs::symlink(directory.path(), &link).unwrap();
        let original = SystemProbe.capacity(directory.path()).unwrap();
        let alias = SystemProbe.capacity(&link).unwrap();
        assert_eq!(original.total, alias.total);
        assert!(original.total > 0);
        assert!(alias.available <= alias.total);
        assert!(
            SystemProbe
                .capacity(&directory.path().join("missing"))
                .is_err()
        );
    }

    struct CountingProbe(AtomicUsize);
    impl Probe for CountingProbe {
        fn capacity(&self, _: &Path) -> Result<Capacity, String> {
            self.0.fetch_add(1, Ordering::SeqCst);
            reading(15 * GIB)
        }
    }

    async fn wait_status(
        monitor: &StorageMonitor,
        condition: impl Fn(&StorageStatus) -> bool,
    ) -> StorageStatus {
        let mut receiver = monitor.status.clone();
        tokio::time::timeout(Duration::from_secs(4), async {
            loop {
                let current = receiver.borrow_and_update().clone();
                if condition(&current) {
                    return current;
                }
                receiver.changed().await.unwrap();
            }
        })
        .await
        .expect("storage status update")
    }

    #[tokio::test]
    async fn disabled_monitor_performs_no_calls() {
        let probe = Arc::new(CountingProbe(AtomicUsize::new(0)));
        let config = StorageMonitorConfig {
            enabled: false,
            ..Default::default()
        };
        let (monitor, task) = spawn_with_probe(
            "/data".into(),
            config,
            Arc::new(Hub::new()),
            AppShutdown::default(),
            probe.clone(),
            Duration::from_millis(20),
        );
        task.await.unwrap();
        assert_eq!(probe.0.load(Ordering::SeqCst), 0);
        assert!(!monitor.current().enabled);
        assert!(monitor.current().filesystems.is_empty());
    }

    #[tokio::test]
    async fn same_filesystem_paths_are_sampled_separately() {
        let probe = Arc::new(CountingProbe(AtomicUsize::new(0)));
        let config = StorageMonitorConfig {
            additional_filesystems: vec![MonitoredFilesystemConfig {
                path: "/data".into(),
                minimum_available_gib: Some(20),
            }],
            ..Default::default()
        };
        let shutdown = AppShutdown::default();
        let (monitor, task) = spawn_with_probe(
            "/data".into(),
            config,
            Arc::new(Hub::new()),
            shutdown.clone(),
            probe.clone(),
            Duration::from_secs(1),
        );
        let status = wait_status(&monitor, |status| {
            status.filesystems.len() == 2
                && status
                    .filesystems
                    .iter()
                    .all(|entry| entry.available_bytes.is_some())
        })
        .await;
        assert_eq!(probe.0.load(Ordering::SeqCst), 2);
        assert!(!status.filesystems[0].low);
        assert!(status.filesystems[1].low);
        shutdown.trigger();
        task.await.unwrap();
    }

    struct BlockingProbe {
        gate: (Mutex<bool>, Condvar),
        blocked_calls: AtomicUsize,
        local_available: AtomicU64,
    }
    impl BlockingProbe {
        fn release(&self) {
            *self.gate.0.lock().unwrap() = true;
            self.gate.1.notify_all();
        }
    }
    impl Probe for BlockingProbe {
        fn capacity(&self, path: &Path) -> Result<Capacity, String> {
            if path == Path::new("/blocked") {
                self.blocked_calls.fetch_add(1, Ordering::SeqCst);
                let mut released = self.gate.0.lock().unwrap();
                while !*released {
                    released = self.gate.1.wait(released).unwrap();
                }
                reading(30 * GIB)
            } else {
                reading(self.local_available.load(Ordering::SeqCst))
            }
        }
    }
    struct ReleaseOnDrop(Arc<BlockingProbe>);
    impl Drop for ReleaseOnDrop {
        fn drop(&mut self) {
            self.0.release();
        }
    }

    #[tokio::test]
    #[traced_test]
    async fn blocked_path_does_not_block_other_checks_or_shutdown() {
        let probe = Arc::new(BlockingProbe {
            gate: (Mutex::new(false), Condvar::new()),
            blocked_calls: AtomicUsize::new(0),
            local_available: AtomicU64::new(9 * GIB),
        });
        let _release = ReleaseOnDrop(probe.clone());
        let shutdown = AppShutdown::default();
        let config = StorageMonitorConfig {
            check_interval_seconds: 1,
            additional_filesystems: vec![MonitoredFilesystemConfig {
                path: "/blocked".into(),
                minimum_available_gib: Some(20),
            }],
            ..Default::default()
        };
        let (monitor, task) = spawn_with_probe(
            "/data".into(),
            config,
            Arc::new(Hub::new()),
            shutdown.clone(),
            probe.clone(),
            Duration::from_millis(20),
        );
        let status = wait_status(&monitor, |status| {
            status.filesystems.len() == 2
                && status.filesystems[0].low
                && status.filesystems[1].error.is_some()
        })
        .await;
        assert_eq!(status.filesystems[1].available_bytes, None);
        probe.local_available.store(11 * GIB, Ordering::SeqCst);
        wait_status(&monitor, |status| {
            status.filesystems[0].available_bytes == Some(11 * GIB)
                && !status.filesystems[0].low
                && status.filesystems[1].error.is_some()
        })
        .await;
        assert_eq!(probe.blocked_calls.load(Ordering::SeqCst), 1);
        assert!(logs_contain("storage_sample_failed"));
        assert!(logs_contain("storage_low"));
        assert!(logs_contain("storage_recovered"));
        shutdown.trigger();
        tokio::time::timeout(Duration::from_millis(250), task)
            .await
            .expect("shutdown must not wait for blocked OS call")
            .unwrap();
        assert_eq!(probe.blocked_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    #[traced_test]
    async fn timed_out_path_recovers_when_original_worker_finishes() {
        let probe = Arc::new(BlockingProbe {
            gate: (Mutex::new(false), Condvar::new()),
            blocked_calls: AtomicUsize::new(0),
            local_available: AtomicU64::new(30 * GIB),
        });
        let _release = ReleaseOnDrop(probe.clone());
        let shutdown = AppShutdown::default();
        let (monitor, task) = spawn_with_probe(
            "/blocked".into(),
            StorageMonitorConfig::default(),
            Arc::new(Hub::new()),
            shutdown.clone(),
            probe.clone(),
            Duration::from_millis(20),
        );
        wait_status(&monitor, |status| {
            status
                .filesystems
                .first()
                .is_some_and(|entry| entry.error.is_some())
        })
        .await;
        probe.release();
        wait_status(&monitor, |status| {
            status.filesystems.first().is_some_and(|entry| {
                entry.error.is_none() && entry.available_bytes == Some(30 * GIB)
            })
        })
        .await;
        assert!(logs_contain("storage_sample_failed"));
        assert!(logs_contain("storage_sample_recovered"));
        assert_eq!(probe.blocked_calls.load(Ordering::SeqCst), 1);
        shutdown.trigger();
        task.await.unwrap();
    }
}
