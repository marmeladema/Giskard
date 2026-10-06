//! One `claude` operating-system process: argv, spawn, stdout lines, the stderr tail, the exit.
//!
//! Nothing here knows the protocol beyond line framing. The supervisor (`session.rs`) reads the
//! lines itself, so there is no reader task and no waiter table at this layer.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use giskard_core::error::HarnessError;
use giskard_core::ids::{ProjectId, ThreadId};
use giskard_core::model::ModelRef;
use giskard_harness::EnvOverlay;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::process::{Child, ChildStdin, ChildStdout};
use tokio::task::JoinHandle;
use tracing::{Instrument, debug, info, warn};

use crate::log_fields::display_opt;

/// The longest stdout line accepted; a longer one is fatal for its child.
pub(crate) const MAX_STDOUT_LINE_BYTES: usize = 64 * 1024 * 1024;
/// How many stderr lines a `ChildExit` keeps.
pub(crate) const STDERR_TAIL_LINES: usize = 8;
/// How many characters of each kept stderr line survive.
pub(crate) const STDERR_LINE_PREVIEW: usize = 400;
/// How long `wait` lets the stderr drain finish after the process exited. A grandchild that
/// inherited stderr can hold it open past the CLI's own exit.
const STDERR_DRAIN_GRACE: Duration = Duration::from_secs(2);
/// The binary run when the declaration names none.
const DEFAULT_COMMAND: &str = "claude";
/// How long a thread's child may sit idle before it is reaped, when the declaration does not say
/// (`idle_shutdown_secs`). A judgment, not a measurement: long enough that a user reading an answer
/// and replying does not pay a respawn, short enough that a dozen idle threads do not hold
/// gigabytes for an hour.
pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(600);

/// How to launch this instance's children (the declaration's neutral keys plus logging context).
#[derive(Debug, Clone, Default)]
pub struct ClaudeLaunchOptions {
    /// Binary path or name. `None` is `claude` on `PATH`.
    pub command: Option<PathBuf>,
    /// Appended after the adapter's own arguments.
    pub args: Vec<String>,
    /// Applied on every child over the inherited environment.
    pub env: EnvOverlay,
    /// Only reported on log lines.
    pub project_id: Option<ProjectId>,
    /// The `[harnesses.<name>]` key this instance comes from. Only reported on log lines.
    pub declaration: Option<String>,
    /// Reap a thread's child idle this long; it is respawned with `--resume` on the thread's
    /// next turn. `None` never reaps.
    pub idle_timeout: Option<Duration>,
}

impl ClaudeLaunchOptions {
    pub(crate) fn command_display(&self) -> String {
        self.command
            .as_deref()
            .map(|command| command.display().to_string())
            .unwrap_or_else(|| DEFAULT_COMMAND.to_string())
    }
}

/// Which session a child serves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SessionFlag {
    /// `--session-id <uuid>`: a fresh session, or the same-id respawn after a lost transcript.
    Fresh(String),
    /// `--resume <uuid>`.
    Resume(String),
}

/// The permission mode a session child is launched with: the *ceiling* of what its turns may set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LaunchMode {
    /// `--permission-mode bypassPermissions`: every preset, `full_access` included, can be set
    /// per turn. The handshake sets `default` before anything else.
    Bypass,
    /// `--permission-mode manual`: the CLI refused a bypass launch; `full_access` is refused.
    Standard,
}

impl LaunchMode {
    pub fn as_str(self) -> &'static str {
        match self {
            LaunchMode::Bypass => "bypass",
            LaunchMode::Standard => "standard",
        }
    }
}

/// The per-thread part of a session child's argv.
#[derive(Debug, Clone)]
pub(crate) struct SessionArgs {
    pub model: ModelRef,
    pub session: SessionFlag,
    pub launch_mode: LaunchMode,
}

/// The protocol flags every child carries, session or probe.
fn protocol_argv() -> Vec<String> {
    [
        "-p",
        "--input-format",
        "stream-json",
        "--output-format",
        "stream-json",
        "--verbose",
        "--permission-prompt-tool",
        "stdio",
        "--setting-sources",
        "user",
        "--disallowedTools",
        "EnterPlanMode",
        "ExitPlanMode",
        "--include-partial-messages",
        "--forward-subagent-text",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

/// The plan §3.1 invocation for one primary thread. The declaration's `args` come last so an
/// operator can append to, never override, the protocol flags.
pub(crate) fn session_argv(options: &ClaudeLaunchOptions, session: &SessionArgs) -> Vec<String> {
    let mut argv = protocol_argv();
    let mode = match session.launch_mode {
        LaunchMode::Bypass => "bypassPermissions",
        LaunchMode::Standard => "manual",
    };
    argv.extend(["--permission-mode".into(), mode.into()]);
    argv.extend(["--model".into(), session.model.model.clone()]);
    if let Some(effort) = &session.model.reasoning_effort {
        argv.extend(["--effort".into(), effort.0.clone()]);
    }
    match &session.session {
        SessionFlag::Fresh(id) => argv.extend(["--session-id".into(), id.clone()]),
        SessionFlag::Resume(id) => argv.extend(["--resume".into(), id.clone()]),
    }
    // Appended, never `--system-prompt`, which would replace Claude Code's own prompt. Passed on
    // resume too: the CLI snapshots the rendered prompt on a conversation's first request and reuses
    // that record on resume, but renders it afresh after a compaction, from this launch's flags.
    argv.extend([
        "--append-system-prompt".into(),
        giskard_harness::GISKARD_FRONTEND_INSTRUCTIONS.into(),
    ]);
    argv.extend(options.args.iter().cloned());
    argv
}

/// The catalog probe's invocation: no mode, model, effort or session flag, so it leaves no
/// transcript.
pub(crate) fn probe_argv(options: &ClaudeLaunchOptions) -> Vec<String> {
    let mut argv = protocol_argv();
    argv.extend(options.args.iter().cloned());
    argv
}

/// What every log line about one child carries.
#[derive(Debug, Clone, Default)]
pub(crate) struct ChildLogContext {
    pub project_id: Option<ProjectId>,
    pub harness: Option<String>,
    pub thread_id: Option<ThreadId>,
    pub harness_thread_id: Option<String>,
    pub resume: bool,
    /// Live session children when this one is spawned (the probe does not count).
    pub live_children: usize,
}

/// How a child ended.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ChildExit {
    /// `None` when killed by a signal.
    pub code: Option<i32>,
    pub signal: Option<i32>,
    /// The last `STDERR_TAIL_LINES` lines, each cut at `STDERR_LINE_PREVIEW` chars.
    pub stderr_tail: Vec<String>,
}

impl ChildExit {
    /// `code 1`, `signal 9`, or `unknown status`, for messages and logs.
    pub fn describe(&self) -> String {
        match (self.code, self.signal) {
            (Some(code), _) => format!("code {code}"),
            (None, Some(signal)) => format!("signal {signal}"),
            (None, None) => "unknown status".into(),
        }
    }
}

/// The process behind one supervisor, or a scripted stand-in in tests.
#[async_trait]
pub(crate) trait ClaudeChild: Send {
    /// Write one stdin line (the newline is appended here). Fails when stdin is closed.
    async fn write_line(&mut self, line: &str) -> Result<(), HarnessError>;
    /// The next stdout line without its newline; `None` at EOF.
    ///
    /// Must be cancel-safe: the supervisor polls it inside `select!`, and a partial line read
    /// before the future was dropped must be returned by the next call.
    async fn next_line(&mut self) -> Result<Option<String>, HarnessError>;
    /// Close stdin so an idle CLI exits on its own. Idempotent.
    fn close_stdin(&mut self);
    /// Wait for exit after EOF and collect the stderr tail. Idempotent after the first return.
    async fn wait(&mut self) -> ChildExit;
    /// SIGKILL. Idempotent; a child that already exited is not an error.
    fn start_kill(&mut self);
    fn pid(&self) -> Option<u32>;
}

/// The error a line over `MAX_STDOUT_LINE_BYTES` is.
pub(crate) fn overlong_line(bytes: usize) -> HarnessError {
    HarnessError::Protocol(format!(
        "claude wrote a stdout line of more than {bytes} bytes; the limit is \
         {MAX_STDOUT_LINE_BYTES}"
    ))
}

/// Read one line into `buffer`, capped at `cap` bytes. Returns `Ok(true)` for a complete line
/// (or a final unterminated one), `Ok(false)` at EOF with nothing buffered.
///
/// Cancel-safe: every byte taken from the reader is moved into `buffer` before the next await,
/// and `buffer` is only drained by the caller once a line is complete.
async fn read_capped_line<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    buffer: &mut Vec<u8>,
    cap: usize,
) -> Result<bool, HarnessError> {
    loop {
        let available = reader
            .fill_buf()
            .await
            .map_err(|error| HarnessError::Transport(format!("reading claude stdout: {error}")))?;
        if available.is_empty() {
            return Ok(!buffer.is_empty());
        }
        let (taken, complete) = match available.iter().position(|byte| *byte == b'\n') {
            Some(newline) => {
                buffer.extend_from_slice(&available[..newline]);
                (newline + 1, true)
            }
            None => {
                buffer.extend_from_slice(available);
                (available.len(), false)
            }
        };
        reader.consume(taken);
        if buffer.len() > cap {
            return Err(overlong_line(buffer.len()));
        }
        if complete {
            return Ok(true);
        }
    }
}

/// A real `claude` child.
pub(crate) struct SpawnedChild {
    child: Child,
    stdin: Option<BufWriter<ChildStdin>>,
    stdout: BufReader<ChildStdout>,
    buffer: Vec<u8>,
    stderr_tail: Arc<Mutex<VecDeque<String>>>,
    stderr_task: Option<JoinHandle<()>>,
    exit: Option<ChildExit>,
    pid: Option<u32>,
    context: ChildLogContext,
}

/// Start one child. Names only on the log line, never values: the overlay and the extra args may
/// carry credentials.
pub(crate) async fn spawn_child(
    options: &ClaudeLaunchOptions,
    argv: &[String],
    cwd: &Path,
    context: &ChildLogContext,
) -> Result<SpawnedChild, HarnessError> {
    let command = options.command_display();
    let mut process = tokio::process::Command::new(
        options
            .command
            .as_deref()
            .unwrap_or_else(|| Path::new(DEFAULT_COMMAND)),
    );
    // The overlay is applied over the inherited environment, never in place of it.
    process
        .args(argv)
        .envs(
            options
                .env
                .entries()
                .iter()
                .map(|(name, value)| (name, value)),
        )
        .current_dir(cwd)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = process.spawn().map_err(|error| {
        warn!(
            action = "spawn_claude",
            project_id = display_opt(context.project_id),
            harness = display_opt(context.harness.as_deref()),
            thread_id = display_opt(context.thread_id),
            harness_thread_id = display_opt(context.harness_thread_id.as_deref()),
            command = %command,
            cwd = %cwd.display(),
            error = %error,
            "failed to start Claude Code"
        );
        HarnessError::Spawn(format!("failed to start {command}: {error}"))
    })?;
    let pid = child.id();
    info!(
        action = "spawn_claude",
        project_id = display_opt(context.project_id),
        harness = display_opt(context.harness.as_deref()),
        thread_id = display_opt(context.thread_id),
        harness_thread_id = display_opt(context.harness_thread_id.as_deref()),
        command = %command,
        cwd = %cwd.display(),
        resume = context.resume,
        extra_args = options.args.len(),
        env_names = ?options.env.names(),
        pid = display_opt(pid),
        live_children = context.live_children,
        "spawned Claude Code"
    );
    let (Some(stdin), Some(stdout), Some(stderr)) =
        (child.stdin.take(), child.stdout.take(), child.stderr.take())
    else {
        // Unreachable with all three piped, but a child without its pipes is useless: reap it.
        let _ = child.start_kill();
        return Err(HarnessError::Spawn(format!(
            "{command} started without its stdio pipes"
        )));
    };
    let stderr_tail = Arc::new(Mutex::new(VecDeque::with_capacity(STDERR_TAIL_LINES)));
    // The drain logs every line: it runs in the spawner's span, so its lines keep that context.
    let stderr_task = tokio::spawn(
        drain_stderr(
            BufReader::new(stderr),
            stderr_tail.clone(),
            context.clone(),
            pid,
        )
        .in_current_span(),
    );
    Ok(SpawnedChild {
        child,
        stdin: Some(BufWriter::new(stdin)),
        stdout: BufReader::new(stdout),
        buffer: Vec::new(),
        stderr_tail,
        stderr_task: Some(stderr_task),
        exit: None,
        pid,
        context: context.clone(),
    })
}

/// Log every stderr line at `debug` and keep the bounded tail the exit reports.
async fn drain_stderr<R: AsyncBufRead + Unpin>(
    mut stderr: R,
    tail: Arc<Mutex<VecDeque<String>>>,
    context: ChildLogContext,
    pid: Option<u32>,
) {
    let mut line = Vec::new();
    loop {
        line.clear();
        match stderr.read_until(b'\n', &mut line).await {
            Ok(0) => return,
            Ok(_) => {
                let text = strip_ansi(String::from_utf8_lossy(&line).trim_end());
                debug!(
                    target: "giskard_harness_claude::stderr",
                    project_id = display_opt(context.project_id),
                    harness = display_opt(context.harness.as_deref()),
                    thread_id = display_opt(context.thread_id),
                    harness_thread_id = display_opt(context.harness_thread_id.as_deref()),
                    pid = display_opt(pid),
                    line = %text,
                    "claude stderr"
                );
                if text.trim().is_empty() {
                    continue;
                }
                let kept: String = text.chars().take(STDERR_LINE_PREVIEW).collect();
                let mut tail = tail.lock().unwrap_or_else(PoisonError::into_inner);
                if tail.len() == STDERR_TAIL_LINES {
                    tail.pop_front();
                }
                tail.push_back(kept);
            }
            Err(error) => {
                warn!(
                    action = "read_stderr",
                    thread_id = display_opt(context.thread_id),
                    harness_thread_id = display_opt(context.harness_thread_id.as_deref()),
                    pid = display_opt(pid),
                    error = %error,
                    "stopped reading claude stderr"
                );
                return;
            }
        }
    }
}

#[async_trait]
impl ClaudeChild for SpawnedChild {
    async fn write_line(&mut self, line: &str) -> Result<(), HarnessError> {
        let Some(stdin) = self.stdin.as_mut() else {
            return Err(HarnessError::Transport("claude stdin is closed".into()));
        };
        let result = async {
            stdin.write_all(line.as_bytes()).await?;
            stdin.write_all(b"\n").await?;
            stdin.flush().await
        }
        .await;
        result.map_err(|error| HarnessError::Transport(format!("writing claude stdin: {error}")))
    }

    async fn next_line(&mut self) -> Result<Option<String>, HarnessError> {
        if !read_capped_line(&mut self.stdout, &mut self.buffer, MAX_STDOUT_LINE_BYTES).await? {
            return Ok(None);
        }
        let bytes = std::mem::take(&mut self.buffer);
        match String::from_utf8(bytes) {
            Ok(line) => Ok(Some(line)),
            Err(error) => {
                let bytes = error.into_bytes();
                warn!(
                    action = "read_stdout",
                    thread_id = display_opt(self.context.thread_id),
                    harness_thread_id = display_opt(self.context.harness_thread_id.as_deref()),
                    pid = display_opt(self.pid),
                    bytes = bytes.len(),
                    "claude wrote a stdout line that is not UTF-8; converting it lossily"
                );
                Ok(Some(String::from_utf8_lossy(&bytes).into_owned()))
            }
        }
    }

    fn close_stdin(&mut self) {
        if self.stdin.take().is_some() {
            debug!(
                action = "close_stdin",
                thread_id = display_opt(self.context.thread_id),
                harness_thread_id = display_opt(self.context.harness_thread_id.as_deref()),
                pid = display_opt(self.pid),
                "closed claude stdin"
            );
        }
    }

    async fn wait(&mut self) -> ChildExit {
        if let Some(exit) = &self.exit {
            return exit.clone();
        }
        let (code, signal) = match self.child.wait().await {
            Ok(status) => (status.code(), exit_signal(&status)),
            Err(error) => {
                warn!(
                    action = "wait_child",
                    thread_id = display_opt(self.context.thread_id),
                    harness_thread_id = display_opt(self.context.harness_thread_id.as_deref()),
                    pid = display_opt(self.pid),
                    error = %error,
                    "could not collect the claude exit status"
                );
                (None, None)
            }
        };
        if let Some(mut task) = self.stderr_task.take() {
            match tokio::time::timeout(STDERR_DRAIN_GRACE, &mut task).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => warn!(
                    action = "read_stderr",
                    thread_id = display_opt(self.context.thread_id),
                    pid = display_opt(self.pid),
                    error = %error,
                    "the claude stderr drain failed"
                ),
                Err(_) => {
                    task.abort();
                    warn!(
                        action = "read_stderr",
                        thread_id = display_opt(self.context.thread_id),
                        pid = display_opt(self.pid),
                        grace_ms = STDERR_DRAIN_GRACE.as_millis(),
                        "claude stderr stayed open after exit; the tail may be incomplete"
                    );
                }
            }
        }
        let stderr_tail = self
            .stderr_tail
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .cloned()
            .collect();
        let exit = ChildExit {
            code,
            signal,
            stderr_tail,
        };
        self.exit = Some(exit.clone());
        exit
    }

    fn start_kill(&mut self) {
        if let Err(error) = self.child.start_kill() {
            debug!(
                action = "kill_child",
                thread_id = display_opt(self.context.thread_id),
                pid = display_opt(self.pid),
                error = %error,
                "claude child could not be killed; it has most likely exited already"
            );
        }
    }

    fn pid(&self) -> Option<u32> {
        self.pid
    }
}

#[cfg(unix)]
fn exit_signal(status: &std::process::ExitStatus) -> Option<i32> {
    use std::os::unix::process::ExitStatusExt;
    status.signal()
}

#[cfg(not(unix))]
fn exit_signal(_status: &std::process::ExitStatus) -> Option<i32> {
    None
}

/// Why a child exited before answering, as far as its own words tell.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExitKind {
    /// `No conversation found with session ID` on stderr or in `result.errors`.
    ResumeMissing,
    /// The CLI refused `--permission-mode bypassPermissions`: as root, or (best effort, the exact
    /// sentence is unverified) when settings disable the mode.
    BypassRefused,
    /// The best-effort authentication match: the unauthenticated shape could not be reproduced,
    /// so this is a substring match on the CLI's own words.
    Unauthenticated,
    Other,
}

const RESUME_MISSING_MARKER: &str = "No conversation found with session ID";
const BYPASS_ROOT_MARKER: &str = "cannot be used with root/sudo privileges";
const AUTHENTICATION_MARKERS: &[&str] = &[
    "not logged in",
    "invalid api key",
    "/login",
    "authentication",
];

pub(crate) fn classify_exit(exit: &ChildExit, last_result_errors: &[String]) -> ExitKind {
    let texts = || exit.stderr_tail.iter().chain(last_result_errors);
    if texts().any(|text| text.contains(RESUME_MISSING_MARKER)) {
        return ExitKind::ResumeMissing;
    }
    if texts().any(|text| is_bypass_refusal(text)) {
        return ExitKind::BypassRefused;
    }
    if texts().any(|text| {
        let text = text.to_ascii_lowercase();
        AUTHENTICATION_MARKERS
            .iter()
            .any(|marker| text.contains(marker))
    }) {
        return ExitKind::Unauthenticated;
    }
    ExitKind::Other
}

fn is_bypass_refusal(text: &str) -> bool {
    if text.contains(BYPASS_ROOT_MARKER) {
        return true;
    }
    let lower = text.to_ascii_lowercase();
    lower.contains("bypasspermissions") && lower.contains("disable")
}

/// The sentence in which the CLI refused a bypass launch, for the `full_access` refusal.
pub(crate) fn bypass_refused_sentence(exit: &ChildExit, result_errors: &[String]) -> String {
    exit.stderr_tail
        .iter()
        .chain(result_errors)
        .find(|text| is_bypass_refusal(text))
        .map(|text| text.trim().to_owned())
        .unwrap_or_else(|| format!("claude exited with {}", exit.describe()))
}

/// The sentence that explains a missing transcript, for the resume-fallback notice.
pub(crate) fn resume_missing_sentence(
    exit: &ChildExit,
    result_errors: &[String],
) -> Option<String> {
    exit.stderr_tail
        .iter()
        .chain(result_errors)
        .find(|text| text.contains(RESUME_MISSING_MARKER))
        .map(|text| text.trim().to_owned())
}

fn strip_ansi(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    let mut chars = value.chars().peekable();
    while let Some(character) = chars.next() {
        if character == '\u{1b}' && chars.peek() == Some(&'[') {
            chars.next();
            for control in chars.by_ref() {
                if !(control.is_ascii_digit() || control == ';') {
                    break;
                }
            }
        } else {
            output.push(character);
        }
    }
    output
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use giskard_core::model::Effort;

    pub(crate) fn fake_claude() -> PathBuf {
        PathBuf::from(format!(
            "{}/tests/fake-claude.sh",
            env!("CARGO_MANIFEST_DIR")
        ))
    }

    fn model(effort: Option<&str>) -> ModelRef {
        ModelRef {
            provider: "anthropic".into(),
            model: "sonnet".into(),
            reasoning_effort: effort.map(|e| Effort(e.into())),
        }
    }

    fn strings(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }

    const PROTOCOL: &[&str] = &[
        "-p",
        "--input-format",
        "stream-json",
        "--output-format",
        "stream-json",
        "--verbose",
        "--permission-prompt-tool",
        "stdio",
        "--setting-sources",
        "user",
        "--disallowedTools",
        "EnterPlanMode",
        "ExitPlanMode",
        "--include-partial-messages",
        "--forward-subagent-text",
    ];

    #[test]
    fn session_argv_is_the_plan_invocation() {
        let options = ClaudeLaunchOptions {
            args: strings(&["--debug", "api"]),
            ..ClaudeLaunchOptions::default()
        };
        let fresh = session_argv(
            &options,
            &SessionArgs {
                model: model(None),
                session: SessionFlag::Fresh("uuid-1".into()),
                launch_mode: LaunchMode::Bypass,
            },
        );
        let mut expected = strings(PROTOCOL);
        expected.extend(strings(&[
            "--permission-mode",
            "bypassPermissions",
            "--model",
            "sonnet",
            "--session-id",
            "uuid-1",
            "--append-system-prompt",
            giskard_harness::GISKARD_FRONTEND_INSTRUCTIONS,
            "--debug",
            "api",
        ]));
        assert_eq!(fresh, expected);

        let resume = session_argv(
            &ClaudeLaunchOptions::default(),
            &SessionArgs {
                model: model(Some("high")),
                session: SessionFlag::Resume("uuid-2".into()),
                launch_mode: LaunchMode::Standard,
            },
        );
        let mut expected = strings(PROTOCOL);
        expected.extend(strings(&[
            "--permission-mode",
            "manual",
            "--model",
            "sonnet",
            "--effort",
            "high",
            "--resume",
            "uuid-2",
            "--append-system-prompt",
            giskard_harness::GISKARD_FRONTEND_INSTRUCTIONS,
        ]));
        assert_eq!(resume, expected);

        let mut expected = strings(PROTOCOL);
        expected.extend(strings(&["--debug", "api"]));
        assert_eq!(probe_argv(&options), expected);

        // No child echoes its prompts: the turn is acknowledged at its `system/init`.
        for argv in [&fresh, &resume, &probe_argv(&options)] {
            assert!(
                !argv.iter().any(|arg| arg == "--replay-user-messages"),
                "{argv:?}"
            );
        }
    }

    #[test]
    fn a_probe_never_becomes_a_session() {
        // The probe rule (AGENTS.md): no flag that names, resumes or shapes a session.
        let options = ClaudeLaunchOptions {
            args: strings(&["--debug", "api"]),
            ..ClaudeLaunchOptions::default()
        };
        let argv = probe_argv(&options);
        for flag in [
            "--session-id",
            "--resume",
            "--model",
            "--permission-mode",
            "--replay-user-messages",
            "--append-system-prompt",
        ] {
            assert!(!argv.iter().any(|arg| arg == flag), "{flag} in {argv:?}");
        }
    }

    #[test]
    fn exits_are_classified_by_the_cli_s_own_words() {
        let exit = |line: &str| ChildExit {
            code: Some(1),
            signal: None,
            stderr_tail: vec![line.into()],
        };
        assert_eq!(
            classify_exit(&exit("No conversation found with session ID: x"), &[]),
            ExitKind::ResumeMissing
        );
        assert_eq!(
            classify_exit(
                &ChildExit::default(),
                &["No conversation found with session ID: x".into()]
            ),
            ExitKind::ResumeMissing
        );
        assert_eq!(
            classify_exit(&exit("Invalid API key · Please run /login"), &[]),
            ExitKind::Unauthenticated
        );
        assert_eq!(
            classify_exit(&exit("Error: Session ID x is already in use."), &[]),
            ExitKind::Other
        );
        let root = "--dangerously-skip-permissions cannot be used with root/sudo privileges for \
                    security reasons";
        assert_eq!(classify_exit(&exit(root), &[]), ExitKind::BypassRefused);
        assert_eq!(bypass_refused_sentence(&exit(root), &[]), root);
        assert_eq!(
            classify_exit(
                &ChildExit::default(),
                &["bypassPermissions mode is disabled by your settings".into()]
            ),
            ExitKind::BypassRefused
        );
        assert_eq!(
            bypass_refused_sentence(&exit("something else"), &[]),
            "claude exited with code 1"
        );
        assert_eq!(exit("").describe(), "code 1");
        assert_eq!(
            ChildExit {
                code: None,
                signal: Some(9),
                stderr_tail: Vec::new()
            }
            .describe(),
            "signal 9"
        );
    }

    #[tokio::test]
    async fn an_overlong_line_is_an_error_and_a_short_one_is_cancel_safe() {
        let input: &[u8] = b"short\n0123456789abcdef\nlast";
        let mut reader = BufReader::with_capacity(4, input);
        let mut buffer = Vec::new();
        assert!(
            read_capped_line(&mut reader, &mut buffer, 10)
                .await
                .unwrap()
        );
        assert_eq!(std::mem::take(&mut buffer), b"short");
        let error = read_capped_line(&mut reader, &mut buffer, 10)
            .await
            .unwrap_err();
        assert!(matches!(error, HarnessError::Protocol(message) if message.contains("limit")));

        let mut reader = BufReader::new(&b"a\nunterminated"[..]);
        let mut buffer = Vec::new();
        assert!(
            read_capped_line(&mut reader, &mut buffer, 100)
                .await
                .unwrap()
        );
        buffer.clear();
        assert!(
            read_capped_line(&mut reader, &mut buffer, 100)
                .await
                .unwrap()
        );
        assert_eq!(buffer, b"unterminated");
        buffer.clear();
        assert!(
            !read_capped_line(&mut reader, &mut buffer, 100)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn a_missing_binary_is_a_spawn_error() {
        let options = ClaudeLaunchOptions {
            command: Some("giskard-no-such-claude".into()),
            ..ClaudeLaunchOptions::default()
        };
        let error = spawn_child(&options, &[], Path::new("."), &ChildLogContext::default())
            .await
            .err()
            .unwrap();
        assert!(
            matches!(&error, HarnessError::Spawn(message) if message.contains("giskard-no-such-claude")),
            "{error}"
        );
    }

    #[tokio::test]
    async fn stderr_is_drained_and_its_tail_is_bounded() {
        let options = ClaudeLaunchOptions {
            command: Some(fake_claude()),
            env: EnvOverlay::new([("FAKE_CLAUDE_STDERR_FLOOD".into(), "1".into())]),
            ..ClaudeLaunchOptions::default()
        };
        let cwd = tempfile::tempdir().unwrap();
        let mut child = spawn_child(&options, &[], cwd.path(), &ChildLogContext::default())
            .await
            .unwrap();
        assert!(child.pid().is_some());
        assert_eq!(child.next_line().await.unwrap(), None);
        let exit = child.wait().await;
        assert_eq!(exit.code, Some(0));
        assert_eq!(exit.stderr_tail.len(), STDERR_TAIL_LINES);
        assert!(
            exit.stderr_tail
                .iter()
                .all(|line| line.chars().count() <= STDERR_LINE_PREVIEW)
        );
        assert!(exit.stderr_tail.last().unwrap().starts_with("50 "));
        // Idempotent after the first return.
        assert_eq!(child.wait().await, exit);
        child.start_kill();
        child.close_stdin();
        child.close_stdin();
        assert!(child.write_line("x").await.is_err());
    }
}
