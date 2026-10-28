//! Filesystem capacity is process-wide health state, never project or thread identity. The
//! monitor publishes into a `watch` channel that `GET /api/status` reads; nothing is pushed.
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use giskard_persist::config::DiskUsageConfig;
use giskard_proto::{DiskUsage, DiskUsagePath};
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;
use tracing::{Instrument, info, warn};

use crate::AppShutdown;

const GIB: u64 = 1024 * 1024 * 1024;
const SAMPLE_TIMEOUT: Duration = Duration::from_secs(10);

/// Read-only projection handle. Only the monitor task publishes snapshots.
#[derive(Clone)]
pub struct DiskUsageMonitor {
    status: watch::Receiver<DiskUsage>,
}

impl Default for DiskUsageMonitor {
    fn default() -> Self {
        Self {
            status: watch::channel(initial_status(&DiskUsageConfig {
                enabled: false,
                ..DiskUsageConfig::default()
            }))
            .1,
        }
    }
}

impl DiskUsageMonitor {
    pub fn current(&self) -> DiskUsage {
        self.status.borrow().clone()
    }

    /// Start once, after the server binds and before accepting connections. A disabled monitor
    /// performs no filesystem work. The caller joins this task during graceful shutdown.
    pub fn spawn(
        data_dir: PathBuf,
        config: DiskUsageConfig,
        shutdown: AppShutdown,
    ) -> (Self, JoinHandle<()>) {
        spawn_with_probe(
            data_dir,
            config,
            shutdown,
            Arc::new(SystemProbe),
            SAMPLE_TIMEOUT,
        )
    }
}

fn initial_status(config: &DiskUsageConfig) -> DiskUsage {
    DiskUsage {
        enabled: config.enabled,
        check_interval_seconds: config.check_interval_seconds,
        checked_at: None,
        paths: Vec::new(),
    }
}

#[derive(Clone)]
struct Target {
    path: PathBuf,
    threshold: u64,
}

fn targets(data_dir: PathBuf, config: &DiskUsageConfig) -> Vec<Target> {
    let mut targets = vec![Target {
        path: data_dir,
        threshold: config.min_available_gib * GIB,
    }];
    targets.extend(config.paths.iter().map(|(path, entry)| Target {
        path: path.clone(),
        threshold: entry.min_available_gib.unwrap_or(config.min_available_gib) * GIB,
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

fn initial_entry(target: &Target) -> DiskUsagePath {
    DiskUsagePath {
        path: target.path.display().to_string(),
        available_bytes: None,
        total_bytes: None,
        min_available_bytes: target.threshold,
        low: false,
        error: None,
    }
}

/// Enter `low` strictly below the threshold; leave it only at threshold plus 1 GiB, so a path
/// hovering around the line does not flap. A failed reading keeps the previous capacity and
/// `low`, because "could not check" must never read as "recovered".
fn apply_reading(entry: &mut DiskUsagePath, reading: Result<Capacity, String>) {
    match reading {
        Ok(capacity) => {
            entry.available_bytes = Some(capacity.available);
            entry.total_bytes = Some(capacity.total);
            let recovery =
                entry
                    .min_available_bytes
                    .saturating_add(if entry.low { GIB } else { 0 });
            entry.low = capacity.available < recovery;
            entry.error = None;
        }
        Err(error) => entry.error = Some(error),
    }
}

/// Entries are positional: both snapshots list the configured paths in the same order.
fn log_changes(previous: &DiskUsage, next: &DiskUsage) {
    for (index, entry) in next.paths.iter().enumerate() {
        let prior = previous.paths.get(index);
        if entry.low && !prior.is_some_and(|prior| prior.low) {
            warn!(action = "disk_usage_low", path = %entry.path,
                available_bytes = entry.available_bytes, min_available_bytes = entry.min_available_bytes,
                "filesystem available space is below the configured threshold");
        } else if !entry.low && prior.is_some_and(|prior| prior.low) {
            info!(action = "disk_usage_recovered", path = %entry.path,
                available_bytes = entry.available_bytes, "filesystem available space recovered");
        }
        if entry.error.as_deref() != prior.and_then(|prior| prior.error.as_deref()) {
            if let Some(error) = &entry.error {
                warn!(action = "disk_usage_check_failed", path = %entry.path, %error,
                    "could not check filesystem capacity");
            } else {
                info!(action = "disk_usage_check_recovered", path = %entry.path, "filesystem sampling recovered");
            }
        }
    }
}

/// A path owns at most one outstanding OS call. Timeouts report stale state while that call
/// finishes; unrelated paths continue on their own schedules. OS workers own no server state.
async fn monitor_path(
    index: usize,
    target: Target,
    mut entry: DiskUsagePath,
    interval_seconds: u64,
    sample_timeout: Duration,
    probe: Arc<dyn Probe>,
    updates: tokio::sync::mpsc::Sender<(usize, DiskUsagePath)>,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(interval_seconds));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        let job_path = target.path.clone();
        let job_probe = probe.clone();
        let span = tracing::Span::current();
        let (done, mut job) = oneshot::channel();
        // A dedicated thread rather than `spawn_blocking`: a filesystem call stuck on a hung
        // mount would otherwise pin one of the runtime's shared blocking workers for good.
        if let Err(error) = std::thread::Builder::new()
            .name("giskard-disk-usage".into())
            .spawn(move || {
                let reading = span.in_scope(|| job_probe.capacity(&job_path));
                let _ = done.send(reading);
            })
        {
            apply_reading(
                &mut entry,
                Err(format!("Could not start disk usage check: {error}")),
            );
        } else {
            let reading = match tokio::time::timeout(sample_timeout, &mut job).await {
                Ok(result) => result,
                Err(_) => {
                    apply_reading(
                        &mut entry,
                        Err(format!(
                            "Disk usage check exceeded {} seconds; waiting for the filesystem",
                            sample_timeout.as_secs_f64()
                        )),
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
                reading
                    .unwrap_or_else(|error| Err(format!("Disk usage check task failed: {error}"))),
            );
        }
        if updates.send((index, entry.clone())).await.is_err() {
            return;
        }
    }
}

fn spawn_with_probe(
    data_dir: PathBuf,
    config: DiskUsageConfig,
    shutdown: AppShutdown,
    probe: Arc<dyn Probe>,
    sample_timeout: Duration,
) -> (DiskUsageMonitor, JoinHandle<()>) {
    let (sender, status) = watch::channel(initial_status(&config));
    let task = tokio::spawn(
        async move {
            if !config.enabled {
                return;
            }
            let targets = targets(data_dir, &config);
            let mut snapshot = initial_status(&config);
            snapshot.paths = targets.iter().map(initial_entry).collect();
            let (updates, mut incoming) = tokio::sync::mpsc::channel(16);
            let mut workers = tokio::task::JoinSet::new();
            for (index, target) in targets.into_iter().enumerate() {
                workers.spawn(
                    monitor_path(
                        index,
                        target,
                        snapshot.paths[index].clone(),
                        config.check_interval_seconds,
                        sample_timeout,
                        probe.clone(),
                        updates.clone(),
                    )
                    .in_current_span(),
                );
            }
            drop(updates);
            loop {
                tokio::select! {
                    biased;
                    () = shutdown.wait() => break,
                    result = workers.join_next(), if !workers.is_empty() => {
                        match result {
                            Some(Err(error)) => warn!(action = "disk_usage_worker_failed", %error, "disk usage path monitor failed"),
                            Some(Ok(())) => warn!(action = "disk_usage_worker_stopped", "disk usage path monitor stopped unexpectedly"),
                            None => {}
                        }
                    }
                    update = incoming.recv() => {
                        let Some((index, entry)) = update else { break; };
                        snapshot.paths[index] = entry;
                        snapshot.checked_at = Some(Utc::now());
                        log_changes(&sender.borrow(), &snapshot);
                        sender.send_replace(snapshot.clone());
                    }
                }
            }
            // Aborting async path tasks drops their OS-worker receivers without joining
            // potentially uninterruptible filesystem calls, keeping runtime shutdown prompt.
            workers.shutdown().await;
            info!(action = "disk_usage_monitor_stopped", "disk usage monitor stopped");
        }
        .in_current_span(),
    );
    (DiskUsageMonitor { status }, task)
}

#[cfg(test)]
mod tests {
    use super::*;
    use giskard_persist::config::DiskUsagePathConfig;

    fn extra_path(
        path: &str,
        min_available_gib: Option<u64>,
    ) -> indexmap::IndexMap<PathBuf, DiskUsagePathConfig> {
        indexmap::IndexMap::from([(
            PathBuf::from(path),
            DiskUsagePathConfig { min_available_gib },
        )])
    }
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use std::sync::{Condvar, Mutex};
    use tracing_test::traced_test;

    fn entry(path: &str, threshold: u64) -> DiskUsagePath {
        initial_entry(&Target {
            path: path.into(),
            threshold,
        })
    }

    fn reading(available: u64) -> Result<Capacity, String> {
        Ok(Capacity {
            available,
            total: 100 * GIB,
        })
    }

    #[test]
    fn thresholds_are_independent_even_for_the_same_path() {
        let config = DiskUsageConfig {
            paths: extra_path("/data", Some(20)),
            ..Default::default()
        };
        let targets = targets("/data".into(), &config);
        assert_eq!(targets.len(), 2);
        let mut entries: Vec<_> = targets.iter().map(initial_entry).collect();
        for entry in &mut entries {
            apply_reading(entry, reading(15 * GIB));
        }
        assert!(!entries[0].low);
        assert!(entries[1].low);
        assert_eq!(entries[0].path, entries[1].path);
        assert_eq!(entries[0].min_available_bytes, 10 * GIB);
        assert_eq!(entries[1].min_available_bytes, 20 * GIB);
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
        assert_eq!(missing.min_available_bytes, 20 * GIB);
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
        monitor: &DiskUsageMonitor,
        condition: impl Fn(&DiskUsage) -> bool,
    ) -> DiskUsage {
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
        .expect("disk usage update")
    }

    #[tokio::test]
    async fn disabled_monitor_performs_no_calls() {
        let probe = Arc::new(CountingProbe(AtomicUsize::new(0)));
        let config = DiskUsageConfig {
            enabled: false,
            check_interval_seconds: 7,
            ..Default::default()
        };
        let (monitor, task) = spawn_with_probe(
            "/data".into(),
            config,
            AppShutdown::default(),
            probe.clone(),
            Duration::from_millis(20),
        );
        task.await.unwrap();
        assert_eq!(probe.0.load(Ordering::SeqCst), 0);
        let status = monitor.current();
        assert!(!status.enabled);
        assert_eq!(status.check_interval_seconds, 7);
        assert_eq!(status.checked_at, None);
        assert!(status.paths.is_empty());
    }

    #[test]
    fn default_handle_reports_monitoring_disabled() {
        let status = DiskUsageMonitor::default().current();
        assert!(!status.enabled);
        assert!(status.paths.is_empty());
        assert_eq!(status.checked_at, None);
    }

    #[tokio::test]
    async fn same_filesystem_paths_are_sampled_separately() {
        let probe = Arc::new(CountingProbe(AtomicUsize::new(0)));
        let config = DiskUsageConfig {
            paths: extra_path("/data", Some(20)),
            ..Default::default()
        };
        let shutdown = AppShutdown::default();
        let (monitor, task) = spawn_with_probe(
            "/data".into(),
            config,
            shutdown.clone(),
            probe.clone(),
            Duration::from_secs(1),
        );
        let status = wait_status(&monitor, |status| {
            status.paths.len() == 2
                && status
                    .paths
                    .iter()
                    .all(|entry| entry.available_bytes.is_some())
        })
        .await;
        assert_eq!(probe.0.load(Ordering::SeqCst), 2);
        assert!(status.checked_at.is_some());
        assert_eq!(status.check_interval_seconds, 60);
        assert!(!status.paths[0].low);
        assert!(status.paths[1].low);
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
        let config = DiskUsageConfig {
            check_interval_seconds: 1,
            paths: extra_path("/blocked", Some(20)),
            ..Default::default()
        };
        let (monitor, task) = spawn_with_probe(
            "/data".into(),
            config,
            shutdown.clone(),
            probe.clone(),
            Duration::from_millis(20),
        );
        let status = wait_status(&monitor, |status| {
            status.paths.len() == 2 && status.paths[0].low && status.paths[1].error.is_some()
        })
        .await;
        assert_eq!(status.paths[1].available_bytes, None);
        assert!(
            status.paths[1]
                .error
                .as_deref()
                .is_some_and(|error| error.starts_with("Disk usage check exceeded 0.02 seconds")),
            "{:?}",
            status.paths[1].error
        );
        probe.local_available.store(11 * GIB, Ordering::SeqCst);
        wait_status(&monitor, |status| {
            status.paths[0].available_bytes == Some(11 * GIB)
                && !status.paths[0].low
                && status.paths[1].error.is_some()
        })
        .await;
        assert_eq!(probe.blocked_calls.load(Ordering::SeqCst), 1);
        assert!(logs_contain("disk_usage_check_failed"));
        assert!(logs_contain("disk_usage_low"));
        assert!(logs_contain("disk_usage_recovered"));
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
            DiskUsageConfig::default(),
            shutdown.clone(),
            probe.clone(),
            Duration::from_millis(20),
        );
        wait_status(&monitor, |status| {
            status
                .paths
                .first()
                .is_some_and(|entry| entry.error.is_some())
        })
        .await;
        probe.release();
        wait_status(&monitor, |status| {
            status.paths.first().is_some_and(|entry| {
                entry.error.is_none() && entry.available_bytes == Some(30 * GIB)
            })
        })
        .await;
        assert!(logs_contain("disk_usage_check_failed"));
        assert!(logs_contain("disk_usage_check_recovered"));
        assert_eq!(probe.blocked_calls.load(Ordering::SeqCst), 1);
        shutdown.trigger();
        task.await.unwrap();
    }
}
