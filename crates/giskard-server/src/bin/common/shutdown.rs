use std::future::Future;
use std::time::Duration;

use axum::Router;
use giskard_server::{AppShutdown, HarnessRegistry};
use tokio::sync::watch;
use tracing::{error, info, warn};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    Running,
    Graceful(&'static str),
    Forced(&'static str),
}

pub enum RunOutcome<T> {
    Completed(T),
    Forced(&'static str),
}

pub fn install_signal_handler() -> watch::Receiver<Phase> {
    let (sender, receiver) = watch::channel(Phase::Running);
    tokio::spawn(async move {
        let signal = next_signal().await;
        info!(signal, "server shutdown signal received");
        sender.send_replace(Phase::Graceful(signal));

        let signal = next_signal().await;
        sender.send_replace(Phase::Forced(signal));
    });
    receiver
}

pub async fn run_until_forced<T>(
    future: impl Future<Output = T>,
    mut shutdown: watch::Receiver<Phase>,
) -> RunOutcome<T> {
    tokio::pin!(future);
    tokio::select! {
        result = &mut future => RunOutcome::Completed(result),
        signal = wait_for_forced(&mut shutdown) => RunOutcome::Forced(signal),
    }
}

pub async fn serve(
    listener: tokio::net::TcpListener,
    app: Router,
    app_shutdown: AppShutdown,
    shutdown: watch::Receiver<Phase>,
    timeout: Duration,
    server_name: &'static str,
) -> Result<(), String> {
    let graceful_rx = shutdown.clone();
    let graceful_app_shutdown = app_shutdown;
    let server = async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                wait_for_graceful(graceful_rx).await;
                graceful_app_shutdown.trigger();
            })
            .await
    };
    tokio::pin!(server);
    tokio::select! {
        result = &mut server => result.map_err(|error| format!("server error: {error}")),
        () = shutdown_timeout(shutdown, timeout) => {
            warn!(
                timeout_ms = timeout.as_millis(),
                server = server_name,
                "HTTP graceful shutdown timed out; stopped waiting for remaining connections"
            );
            Ok(())
        }
    }
}

pub async fn serve_then_shutdown_registry(
    listener: tokio::net::TcpListener,
    app: Router,
    app_shutdown: AppShutdown,
    shutdown: watch::Receiver<Phase>,
    timeout: Duration,
    server_name: &'static str,
    registry: &HarnessRegistry,
) -> Result<(), String> {
    let serve_result = serve(listener, app, app_shutdown, shutdown, timeout, server_name).await;
    let registry_result = registry
        .shutdown()
        .await
        .map_err(|error| format!("registry shutdown failed: {error}"));

    match (serve_result, registry_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Err(serve_error), Err(registry_error)) => Err(format!("{serve_error}; {registry_error}")),
    }
}

async fn wait_for_graceful(mut shutdown: watch::Receiver<Phase>) -> &'static str {
    loop {
        match *shutdown.borrow_and_update() {
            Phase::Graceful(signal) | Phase::Forced(signal) => return signal,
            Phase::Running => {}
        }
        if shutdown.changed().await.is_err() {
            return "signal_handler_closed";
        }
    }
}

async fn wait_for_forced(shutdown: &mut watch::Receiver<Phase>) -> &'static str {
    loop {
        if let Phase::Forced(signal) = *shutdown.borrow_and_update() {
            return signal;
        }
        if shutdown.changed().await.is_err() {
            std::future::pending().await
        }
    }
}

async fn shutdown_timeout(shutdown: watch::Receiver<Phase>, timeout: Duration) {
    wait_for_graceful(shutdown).await;
    tokio::time::sleep(timeout).await;
}

async fn next_signal() -> &'static str {
    let ctrl_c = async {
        match tokio::signal::ctrl_c().await {
            Ok(()) => "ctrl_c",
            Err(error) => {
                error!(%error, "failed to install Ctrl-C shutdown handler");
                std::future::pending().await
            }
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
                "sigterm"
            }
            Err(error) => {
                error!(%error, "failed to install SIGTERM shutdown handler");
                std::future::pending().await
            }
        }
    };

    #[cfg(unix)]
    tokio::select! {
        signal = ctrl_c => signal,
        signal = terminate => signal,
    }
    #[cfg(not(unix))]
    ctrl_c.await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn forced_phase_interrupts_work() {
        let (sender, receiver) = watch::channel(Phase::Running);
        sender.send_replace(Phase::Graceful("ctrl_c"));
        sender.send_replace(Phase::Forced("sigterm"));

        let outcome = run_until_forced(std::future::pending::<()>(), receiver).await;
        assert!(matches!(outcome, RunOutcome::Forced("sigterm")));
    }
}
