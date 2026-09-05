# S4 — A `giskard-testkit` crate for the server integration tests

Implementation plan for step 4 of [`design-straightening-review.md`](design-straightening-review.md)
(finding C6). Written against `main` at `36c42e8` (S3 merged); every file and line reference below
was checked against that tree. Re-check them if the branch has moved.

## Goal

Replace the scaffolding that every server integration test binary rebuilds for itself with one
crate, `giskard-testkit`: password hashing, the config baseline, spawning the app on an ephemeral
port, logging in, project and thread setup, the WebSocket handshake, and the replay fixtures. After
S4 a new integration test starts with `TestServer::spawn(factory)` and gets an authenticated
client, a cookie, and a WebSocket in three calls. No test's assertions change; only how each test
reaches the server.

## Scope

S4 is the crate plus the migration of the *scaffolding* in all 21 test files. The fourteen
hand-written `impl AgentHarness` fakes stay where they are, unchanged, and each is handed to the
kit through a factory constructor. Unifying the fakes into one configurable `FakeHarness` is
**S4b**, a separate plan written after S4 lands, for two reasons that the survey behind this plan
made concrete:

- The fakes are not fourteen copies of one thing. Twelve of the fourteen share the same
  `open_thread` shape and `subscribe` shape, but every one of them scripts a different
  `start_turn` (a twelve-branch text dispatcher in `e2e_smoke.rs`, a scripted `TerminateBehavior`
  enum in `interrupt.rs`, four response-behaviour knobs in `server_requests.rs`, a provider
  rewrite in `provider_switch.rs`). Replacing them is behaviour design, not a move, and the tests
  they drive are the M0–M8 safety net.
- What S4 removes is what is actually duplicated: 18 copies of the password hash, 35 app spawns,
  28 hand-rolled WebSocket handshakes, 36 inline cookie extractions, 28 `HarnessFactory` impls.
  The fakes' own boilerplate (their `subscribe` and `open_thread` bodies) is a few hundred lines
  and is better attacked once a shared fake exists to absorb it.

## Non-goals

- No change to any test's assertions, to the messages it sends, or to the fakes' behaviour. The
  diff in each test file is deletions of helpers plus one-for-one replacements at call sites.
- No change to `giskard-server`'s public API. The kit uses exactly what the tests use today:
  `AppState::new`, `build_app`, `HarnessFactory`.
- No change to the unit-test fakes inside `crates/giskard-server/src` (five `impl AgentHarness`
  in `registry.rs`, `registry/driver.rs`, `registry/event_forwarder.rs`), nor to
  `giskard-server-replay`'s `ScriptedHarness`.
- No change to `giskard-harness-replay`. The kit wraps `ReplayHarness`; it does not extend it.
- No Playwright, no `tests/e2e/`.

## Ground truth

| Fact | Where |
| --- | --- |
| 21 integration test files, 24 671 lines, 199 `#[tokio::test]` + 27 `#[test]`; plus `tests/common/mod.rs` (17 lines, `fake_native_model`) and `tests/common/thread_fixture.rs` (50 lines, `persist_primary_thread`) | `crates/giskard-server/tests/` |
| `common/mod.rs:1-6` documents why the shared module stays tiny: it is compiled into each binary that declares `mod common;`, so an item only some binaries use is dead code there, and the workspace denies warnings. A dev-dependency crate has no such constraint | `tests/common/mod.rs` |
| `mod common;` in 9 files; `#[path = "common/thread_fixture.rs"] mod thread_fixture;` in 11 files (the 9 plus `model_refresh.rs:10`, `turn_controls.rs:4`) | grep |
| The tests reach `giskard_server` through six symbols: `AppState`, `AppState::new` (35 call sites), `build_app`, `HarnessFactory` (19 files use the verbatim line `use giskard_server::{AppState, HarnessFactory, build_app};`), `auth::{SESSION_COOKIE, TokenPurpose, sign_token}` (`security.rs` only), `models::discover_models` (`model_refresh.rs` only), and the `worktree` module (`worktree.rs`, `worktree_threads.rs`) | grep; `src/lib.rs:27-28`; `src/app.rs:63-69`, `:112` |
| `HarnessFactory` is a trait object: `async fn create(&self, config: &ProjectConfig, bootstrap: HarnessBootstrap) -> Result<Arc<dyn AgentHarness>, HarnessError>`; `HarnessBootstrap { known_threads: Vec<KnownThreadBinding { harness_thread_id, thread_id }> }` derives `Default` | `src/registry.rs:69-78`; `giskard-harness/src/lib.rs:346-355` |
| 28 `impl HarnessFactory for` blocks in the tests (`e2e_smoke.rs` has 8, `project_models.rs` and `read_only_thread.rs` 2 each, the other 16 files 1 each). Their bodies fall into four shapes: clone a shared `Arc<Harness>` (8), build a fresh harness per call (12, two of which also seed a native-id table from `bootstrap.known_threads`: `e2e_smoke.rs:219`, `worktree_threads.rs:435`), wrap `ReplayHarness::from_fixture(self.fixture.clone())` (5: `diff_accumulation.rs:29-37`, `history_sync.rs:27-35`, `token_ledgers.rs:27-35`, `e2e_smoke.rs:137-145`, `turn_controls.rs:32-40`), or always fail (5: `code_overlay.rs:26` and `thread_lifecycle.rs:26` with `Spawn("dummy")`, `read_only_thread.rs:117` with `Spawn("unknown provider: cloudflare-litellm")`, `security.rs:17` with `Unsupported("no harness in security tests")`, `ui.rs:10` with `Spawn("unused")`) | read |
| Password hashing: 12 `fn generate_password_hash` + 6 `fn password_hash`, all 18 byte-identical apart from the name: `Argon2::default().hash_password(password, &SaltString::generate(&mut OsRng))`. Every login sends `"testpass"` except the deliberate failures (`e2e_smoke.rs:8746` `"wrongpass"`, `security.rs:188, :200` `"wrong"`) | grep; `server_requests.rs:264-273` |
| Config baseline written by every authenticated spawn: `[server] bind = "127.0.0.1:<port or 0>"`, `secure_cookies = false`, `[auth] password_hash = "<hash>"`, `session_days = 30`, then optional extra sections. 8 files interpolate the real port (bind the listener first); 13 write `:0`. `secure_cookies = false` is load-bearing: the cookie is otherwise `Secure` and never returns over plain HTTP. The server reads it through `PersistStore::load_config` from `<data_dir>/config.toml` | `worktree_threads.rs:494-503`; `giskard-persist/src/store.rs:585-590` |
| Session key is `(0..32u8).collect()` at all 35 spawns | grep |
| Every spawn serves on a real `TcpListener` bound to `127.0.0.1:0` with `axum::serve`; no test drives the router in-process | grep for `tower::`/`oneshot(`: only `tokio::sync::oneshot` hits |
| Cookie extraction `headers().get("set-cookie") … split(';').next()` appears 36 times; the WebSocket handshake with the fixed `sec-websocket-key: dGhlIHNhbXBsZSBub25jZQ==` appears 28 times (`e2e_smoke.rs` 12, `worktree_threads.rs` 3, `history_sync.rs` 2, 11 files once); `tokio_tungstenite::connect_async` 28 times | grep |
| Named duplicates: `connect_ws` ×6 (+ `ws_connect` ×1, same body), `ws_text` ×6 (identical), `login`/`login_cookie` ×7, `spawn_test_app` ×3 (`approval_reconnect.rs:218`, `interrupt.rs:339`, `server_requests.rs:279`; same 75-line body, different harness and thread name), `start_server` ×5, `make_fixture` ×5 (three identical: `history_sync.rs:37`, `model_refresh.rs:87`, `token_ledgers.rs:37`; two distinct: `e2e_smoke.rs:1802`, `turn_controls.rs:43`), `make_turn` ×3 (identical but for the model), `seeded_thread` ×2, `git` ×2 (`worktree.rs:14` returns `Output`, `worktree_threads.rs:461` does not), `type TestWs` ×3 (+ `type Ws` in `provider_switch.rs:356`), `wait_for_ws_error` ×3 (+ `wait_for_error` in `interrupt.rs:896`), `wait_for_live_snapshot` ×2, `const TINY_PNG` ×2, `struct DummyFactory` ×2 | grep, anchors in the migration table |
| `reqwest::redirect::Policy::none()` clients: 22 builders (`e2e_smoke.rs` 18, `read_only_thread.rs` 2, `security.rs:85`, `turn_controls.rs` 1). No test relies on a followed redirect | grep `Policy::none` |
| `giskard-harness-replay` is a regular dependency of `giskard-server`; 8 test files use `ReplayHarness`, all through `ReplayFixture::from_events` (no fixture files on disk) | `crates/giskard-server/Cargo.toml:13`; grep |
| `[dev-dependencies]` of `giskard-server`: `tempfile`, `tokio-tungstenite = "0.29"`, `futures-util = "0.3"`. `argon2`, `rand`, `reqwest`, `axum`, `async-trait`, `serde_json`, `chrono` reach the tests through `[dependencies]` | `crates/giskard-server/Cargo.toml:46-49` |
| A dev-dependency cycle (`giskard-server` dev-depends on `giskard-testkit`, which depends on `giskard-server`) is accepted by cargo: verified on this tree with a probe crate exposing a `giskard_server::AppState`-returning function, referenced from a test file; `cargo check -p giskard-server --tests` and `--lib` both pass and `cargo metadata` lists both packages | probe run, then reverted |
| CI: `cargo build --workspace --locked`, `cargo test --workspace --locked`, `cargo clippy --workspace --all-targets --locked -- -D warnings`; the new crate is built and linted by all three | `.github/workflows/ci.yml:41-55` |
| Rule: tests must bind port `0` and pass the still-open listener to the server | `AGENTS.md:38-42` |
| `ThreadHandle::opened(thread, harness_thread_id, workspace_root)`; `EventLog::new()`, `Arc<EventLog>::reader()` | `giskard-harness/src/lib.rs:385`; `event_log.rs:77`, `:157` |
| `PersistStore::create_project(id, name, dir: &str)` | `giskard-persist/src/store.rs:935-940` |

## Design

### D1. Crate layout and dependencies

`crates/giskard-testkit`, a workspace member, one library:

```
src/lib.rs        pub mod auth; pub mod factory; pub mod fixtures; pub mod git; pub mod server; pub mod ws;
                  pub use server::{TestProject, TestServer, TestServerBuilder}; pub use ws::TestWs;
```

`Cargo.toml` dependencies (all `workspace = true` where the workspace declares them):
`giskard-core`, `giskard-harness`, `giskard-harness-replay`, `giskard-persist`, `giskard-proto`,
`giskard-server`, `tokio`, `async-trait`, `serde_json`, `chrono`, `tempfile`, `axum`, `reqwest`
(same feature set as the server's: `default-features = false, features = ["json", "rustls-tls"]`),
`argon2`, `rand`, `futures-util = "0.3"`, `tokio-tungstenite = "0.29"`. Add
`giskard-testkit = { path = "crates/giskard-testkit" }` to `[workspace.dependencies]` and
`giskard-testkit = { workspace = true }` to `giskard-server`'s `[dev-dependencies]`. Keep
`tempfile`, `tokio-tungstenite`, and `futures-util` in the server's dev-dependencies: tests still
name `TempDir`, `Message`, and `StreamExt` directly.

Why a crate and not `tests/common`: the dead-code constraint in `common/mod.rs:1-6`. A kit with
a dozen helpers, each used by a subset of 21 binaries, cannot live in a per-binary module under
`-D warnings` without `allow` attributes.

Why one crate that depends on `giskard-server`, and not a server-free kit plus a `tests/common`
spawner: the spawner is the biggest duplicate (35 sites) and the one every binary wants; splitting
it back into `common` reintroduces the constraint above. The cycle is legal and verified.

### D2. `auth`

```rust
pub const PASSWORD: &str = "testpass";
pub fn password_hash(password: &str) -> String;           // the 18-copy body, verbatim
pub fn session_key() -> Vec<u8>;                          // (0..32u8).collect()
pub async fn login(client: &reqwest::Client, base: &str) -> String;
    // POST {base}/api/login {"password": PASSWORD}; asserts 200; returns the cookie up to the first ';'
pub async fn login_with(client: &reqwest::Client, base: &str, password: &str)
    -> reqwest::Response;                                  // no assertion; for the wrong-password tests
```

### D3. `factory`

Four constructors, each returning `Arc<dyn HarnessFactory>`; no test declares a factory struct
afterwards:

```rust
pub fn shared(harness: Arc<dyn AgentHarness>) -> Arc<dyn HarnessFactory>;   // every create returns this Arc
pub fn fixture(fixture: ReplayFixture) -> Arc<dyn HarnessFactory>;          // ReplayHarness::from_fixture(fixture.clone())
pub fn failing(error: HarnessError) -> Arc<dyn HarnessFactory>;             // every create returns error.clone()
pub fn from_fn<F>(f: F) -> Arc<dyn HarnessFactory>
where F: Fn(&ProjectConfig, HarnessBootstrap) -> Result<Arc<dyn AgentHarness>, HarnessError> + Send + Sync + 'static;
```

`shared` takes `Arc<dyn AgentHarness>`; a test holding `Arc<ApprovalHarness>` passes
`harness.clone()` and unsized coercion does the rest. `from_fn` covers the fresh-per-create
factories and the two that read `bootstrap.known_threads`; the closure body is the old `create`
body.

### D4. `server`

```rust
pub struct TestServer {
    pub state: AppState,
    pub addr: SocketAddr,
    pub base: String,                 // "http://127.0.0.1:<port>"
    pub client: reqwest::Client,      // redirect policy: none
    pub cookie: String,               // "" when spawned unauthenticated
    data_dir: Option<TempDir>,        // None when started over an existing directory
}

pub struct TestProject { pub id: ProjectId, pub dir: TempDir }

impl TestServer {
    pub async fn spawn(factory: Arc<dyn HarnessFactory>) -> Self;          // builder(factory).start()
    pub fn builder(factory: Arc<dyn HarnessFactory>) -> TestServerBuilder;
    pub fn data_dir(&self) -> &Path;
    pub fn store(&self) -> &Arc<PersistStore>;
    pub fn url(&self, path: &str) -> String;                                // format!("{base}{path}")
    pub async fn login(&self) -> String;                                    // a fresh cookie
    pub async fn create_project(&self, name: &str) -> TestProject;          // store.create_project into a new TempDir
    pub async fn create_project_in(&self, name: &str, dir: &Path) -> ProjectId;   // store.create_project, caller-owned dir
    pub async fn create_project_via_api(&self, name: &str, dir: &str) -> ProjectId; // POST /api/projects, asserts 200
    pub async fn register_thread(&self, project: ProjectId, harness_thread_id: &str) -> ThreadId;
        // persist_primary_thread(store, project, ThreadId::new(), harness_thread_id, fake_native_model())
        // then POST /api/projects/{project}/threads {"thread_id"}; asserts 200 and that the echoed id matches
    pub async fn ws(&self) -> TestWs;                                       // ws::connect(self.addr, &self.cookie)
    pub async fn ws_with_cookie(&self, cookie: &str) -> TestWs;
}

pub struct TestServerBuilder { /* factory, extra_config, authenticated, data_dir, seed */ }

impl TestServerBuilder {
    pub fn config(self, extra: &str) -> Self;        // appended after the baseline, as every extra_config is today
    pub fn unauthenticated(self) -> Self;            // no config.toml, no login; cookie stays ""
    pub fn data_dir(self, path: &Path) -> Self;      // start over an existing data directory (restart tests)
    pub fn seed<F, Fut>(self, f: F) -> Self          // runs against the store before AppState::new
    where F: FnOnce(Arc<PersistStore>) -> Fut + Send + 'static, Fut: Future<Output = ()> + Send;
    pub async fn start(self) -> TestServer;
}
```

`start` does, in this order: bind `127.0.0.1:0` and read the port; create the data `TempDir`
(unless `data_dir` was given); unless `unauthenticated`, write `config.toml` with the baseline
below and the real port; build the store; run `seed` if given; `AppState::new(store, factory,
auth::session_key())`; `build_app`; `tokio::spawn(axum::serve(listener, app))`; build the client
with `Policy::none()`; unless `unauthenticated`, `auth::login`.

The baseline, byte-for-byte what `e2e_smoke.rs:2106-2118` and the others write, with the real
port in place of `0`:

```toml
[server]
bind = "127.0.0.1:{port}"
secure_cookies = false

[auth]
password_hash = "{hash}"
session_days = 30

{extra}
```

Binding first is the form `worktree_threads.rs:494-496` and `model_refresh.rs:866-870` already
argue for; the tests that write `:0` today never read the value back, so they are unaffected.

`seed` exists for the two files that write a thread into the store *before* the app exists
(`provider_switch.rs:298-305`, `read_only_thread.rs:239-242`). The store is path-based and reads
lazily, so seeding after `start` through `server.store()` would also work, but `seed` keeps the
order those tests chose and removes the question.

### D5. `ws`

```rust
pub type TestWs = WebSocketStream<MaybeTlsStream<TcpStream>>;
pub async fn connect(addr: SocketAddr, cookie: &str) -> TestWs;   // the seven-header handshake, verbatim from server_requests.rs:361-376
pub fn text(msg: &ClientMessage) -> Message;                      // ws_text
pub async fn send(ws: &mut TestWs, msg: &ClientMessage);          // ws.send(text(msg)).await.unwrap()
pub async fn recv_until<T>(ws: &mut TestWs, mut pick: impl FnMut(ServerMessage) -> Option<T>) -> Option<T>;
    // 5 s overall deadline, 1 s per frame, text frames only, frames that do not parse as
    // ServerMessage are skipped; None at the deadline
pub async fn next_matching(ws: &mut TestWs, pred: impl Fn(&serde_json::Value) -> bool) -> Option<serde_json::Value>;
    // provider_switch.rs:382-400, verbatim
pub async fn expect_error(ws: &mut TestWs) -> ErrorInfo;                     // recv_until on ServerMessage::Error, panics "websocket error not observed"
pub async fn expect_error_for(ws: &mut TestWs, action: &str, code: &str) -> ErrorInfo;
    // as expect_error, filtered on error.action == Some(action) && error.code == code; panics "websocket error {code}/{action} was not observed"
pub async fn expect_live_snapshot(ws: &mut TestWs) -> LiveTurnSnapshot;      // panics "live turn snapshot not observed"
```

`recv_until` is the skeleton the ~40 bespoke `wait_for_*` helpers share (`let deadline = …
from_secs(5)` appears 105 times in the tests). S4 migrates only the duplicated ones
(`wait_for_ws_error` ×3, `wait_for_error` ×1, `wait_for_live_snapshot` ×2); the file-local ones
stay as they are and may adopt `recv_until` in S4b. One behavioural note: `interrupt.rs:896`
`unwrap`s a frame that fails to parse, the kit skips it; no test sends an unparsable frame.

### D6. `fixtures`

```rust
pub fn fake_native_model() -> ModelRef;                                   // common/mod.rs:11, verbatim
pub async fn persist_primary_thread(store, project_id, thread_id, harness_thread_id, model) -> ThreadId;  // common/thread_fixture.rs:13, verbatim
pub fn completed_turn_fixture() -> ReplayFixture;                          // the identical make_fixture (history_sync.rs:37): "th_tok", one AgentMessage "done", TokenUsage::new(100, 50)
pub fn completed_turn(text: &str, model: ModelRef) -> Turn;                // make_turn with the model as a parameter (history_sync.rs:83-111)
pub fn orphaned_thread(project_id, thread_id, model: ModelRef, git_workspace: Option<ThreadGitWorkspace>) -> ThreadFile;
    // seeded_thread (provider_switch.rs:189-217): title "Orphaned thread", harness_thread_id "harness-{thread_id}", context_window 131_072
pub const TINY_PNG: &[u8];                                                 // code_overlay.rs:15 / thread_lifecycle.rs:15
```

The two distinct `make_fixture`s (`e2e_smoke.rs:1802`, `turn_controls.rs:43`) and
`code_overlay.rs:148 thread_file` stay local: they are different fixtures, not copies.

### D7. `git`

```rust
pub fn run(dir: &Path, args: &[&str]) -> std::process::Output;   // worktree.rs:14-31, verbatim (the returning variant)
pub fn init_repo_with_commit(dir: &Path);                        // worktree_threads.rs:510-513: init -b main, README.md, add, commit
```

## Migration

Each row: what the file deletes (anchor on the base tree), what replaces it. Every file also
drops its `use giskard_server::{AppState, HarnessFactory, build_app};` line where nothing else
needs those names, and its `argon2`/`rand` imports. "S" = `let server = TestServer::…`.

| File | Deletes | Replacement |
| --- | --- | --- |
| `approval_reconnect.rs` | `mod common` :9, `thread_fixture` :11, `TestWs` :36, `ApprovalFactory` :188-:201, `generate_password_hash` :203, `ws_text` :214, `spawn_test_app` :218-:298, `connect_ws` :300, `wait_for_ws_error` :585, `wait_for_live_snapshot` :638 | `factory::shared(harness.clone())`; S `spawn` + `create_project("proj")` + `register_thread(pid, "approval_thread")`; `server.ws()`; `ws::text`; `ws::expect_error`; `ws::expect_live_snapshot`. Keeps `ApprovalHarness`, four bespoke waits |
| `code_overlay.rs` | `TINY_PNG` :15, `DummyFactory` :23-:34, `generate_password_hash` :36, `start_server` :48 | `fixtures::TINY_PNG`; `factory::failing(HarnessError::Spawn("dummy".into()))`; S `builder(..).config(<its extra sections>).start()` + `create_project("viz-test")`, then the same seeded files. Keeps `thread_file`, `command_turn`, `command_output_url` |
| `diff_accumulation.rs` | `DiffFactory` :24-:37, `generate_password_hash` :161, inline spawn around `AppState::new` :203, `mod common` :408, `thread_fixture` :410 | `factory::fixture(make_diff_fixture())`; S. Keeps `make_diff_fixture` |
| `e2e_smoke.rs` | `mod common` :1, `thread_fixture` :3, `TestFactory` :132-:145, `NoMcpFactory` :147/:164, `UnsupportedCompactionFactory` :148/:175, `SlowCompactionFactory` :149/:186, `HeldCompactionFactory` :150/:197, `SlowStartFactory` :153/:208, `ActivityFactory` :156/:219, `CountingOpenFactory` :159/:235, `generate_password_hash` :2082, the ten `start_*` :2093-:2270, `login_cookie` :2272, `connect_ws` :2291, `create_project_and_thread` :2313, `create_project_only` :2347, `wait_for_ws_error` :2454, 12 inline handshakes, 13 inline cookie extractions, 18 `Policy::none()` clients | `factory::fixture`, `factory::from_fn` (fresh `::default()` harnesses; `ActivityFactory`'s closure keeps its `native_routes` seeding), `factory::shared`; S `builder(factory).config(extra).start()` at the former `start_*` call sites (keep one-line local wrappers where a name is used many times); `server.client`/`server.cookie`/`server.ws()`; `create_project_via_api("thread-actions", "/tmp/thread-actions")` + `register_thread(pid, "th_test")`; `ws::expect_error_for`. Keeps all six fakes, `make_fixture` :1802, every other local helper |
| `history_sync.rs` | `DiffFactory` :22-:35, `make_fixture` :37, `make_turn` :83, `password_hash` :113, `login` :124, `ws_connect` :145, three inline spawns (`AppState::new` :179, :325, :473), `mod common` :574, `thread_fixture` :576 | `factory::fixture(fixtures::completed_turn_fixture())`; `fixtures::completed_turn(text, fake_native_model())`; S; `server.ws()` |
| `interrupt.rs` | `mod common` :4, `thread_fixture` :6, `TestWs` :37, `InterruptFactory` :293-:306, `generate_password_hash` :308, `ws_text` :319, `spawn_test_app` :339-:421, `connect_ws` :423, `wait_for_error` :896 | `TestApp` :323 becomes `{ server: TestServer, harness: Arc<InterruptHarness>, thread_id }` with `connect_ws` delegating to `server.ws()`; `factory::shared`; S + `create_project("proj")` + `register_thread(pid, "interrupt_thread")`; `ws::expect_error_for`. Keeps `InterruptHarness`, `TerminateBehavior`, five bespoke waits, the two shared test bodies |
| `model_refresh.rs` | `thread_fixture` :10, `DiffFactory` :29-:65, `create_project` :68, `make_fixture` :87, `password_hash` :133, `login` :144, `ephemeral_listener` :871, ten inline spawns (`AppState::new` :213 … :1362) | `factory::from_fn` building `ReplayHarness::from_fixture(..).with_providers(..)…` as the old `create` did; `create_project_via_api("discovery", "/tmp")`; `fixtures::completed_turn_fixture`; S `builder(factory).config(<per-test providers>).start()`. Keeps the mock providers and `discover_*` helpers |
| `override_propagation.rs` | `mod common` :7, `thread_fixture` :9, `CapFactory` :154-:180, `generate_password_hash` :182, `ws_text` :193, inline spawn (`AppState::new` :232) | `factory::from_fn` building `CapturingHarness::with_requests(..)` per call; S. Keeps `CapturingHarness`, `wait_for_capture` |
| `project_models.rs` | `CatalogFactory` :38-:56, `FailingCatalogFactory` :59-:76, `password_hash` :90, `login` :101, `connect_ws` :122, the spawn half of `spawn_project` :147 | `factory::from_fn` for both catalog shapes; `spawn_project` keeps the mock provider and becomes: start mock → build factory → S `builder(factory).config(PROVIDERS).start()` → `create_project_via_api("proj", "/tmp/giskard-project-models-test")`. Keeps `harness_providers`, `catalog_model` |
| `provider_switch.rs` | `SwitchFactory` :115-:134, `password_hash` :136, `make_turn` :163, `seeded_thread` :189, local `struct TestServer` :219, `start_server`/`_with_worktree`/`_inner` :232-:335, `login_cookie` :337, `Ws` :356, `connect_ws` :359, `send_msg` :373, `next_matching` :382 | `factory::from_fn` building a fresh `SwitchHarness` sharing the two `Arc`s; `fixtures::completed_turn(text, dead_model())`; `fixtures::orphaned_thread(pid, tid, dead_model(), git_workspace)`; a local `struct Fixture { server: TestServer, pid, tid, opened_workspace_roots, _proj_dir, _worktree_dir }` built by one `start(report_provider, seed_worktree)` that uses `builder(..).config(NEW_PROVIDER_TOML).seed(|store| …save_thread + append_turn…).start()`; `ws::send`; `ws::next_matching`. Keeps `SwitchHarness`, `dead_model`, `new_model` |
| `read_only_thread.rs` | `FailingFactory` :22/:117-:127, `AttachFailsFactory` :26/:100-:114, `password_hash` :129, `make_turn` :149, `seeded_thread` :175, the spawn half of `open_read_only_thread` :207 | `factory::failing(HarnessError::Spawn("unknown provider: cloudflare-litellm".into()))`; `factory::from_fn` building `AttachFails { inner }`; `fixtures::completed_turn(text, orphaned_model())`; `fixtures::orphaned_thread(pid, tid, orphaned_model(), None)`; `open_read_only_thread(factory)` keeps its signature and body-shape but uses `builder(factory).seed(..).start()` and `server.client`/`server.cookie`. Keeps `AttachFails`, `orphaned_model` |
| `running_tasks.rs` | `mod common` :4, `thread_fixture` :6, `ToolFactory` :164-:175, `generate_password_hash` :177, `ws_text` :188, inline spawn (`AppState::new` :206) | `factory::from_fn(\|_, _\| Ok(Arc::new(ToolHarness::new())))`; S. Keeps `ToolHarness` |
| `security.rs` | `NoHarnessFactory` :14-:27, `generate_password_hash` :29, `session_key` :40, `start_server` :54-:83, `client` :85, `login` :92 | `factory::failing(HarnessError::Unsupported("no harness in security tests".into()))`; S `builder(..).config(extra).start()` and `server.base`/`server.client`; `auth::session_key`; `auth::login`. The `set-cookie` reads at :224, :288, :315, :329 assert cookie attributes and stay. Keeps `attr_after` |
| `server_requests.rs` | `mod common` :3, `thread_fixture` :5, `TestWs` :33, `ServerRequestFactory` :249-:262, `generate_password_hash` :264, `ws_text` :275, `spawn_test_app` :279-:359, `connect_ws` :361, `wait_for_ws_error` :968, `wait_for_live_snapshot` :1118 | `factory::shared`; S + `create_project("proj")` + `register_thread(pid, "server_request_thread")`; `server.ws()`; `ws::text`; `ws::expect_error`; `ws::expect_live_snapshot`. Keeps `ServerRequestHarness`, five bespoke waits, `server_request_rows` |
| `thread_lifecycle.rs` | `TINY_PNG` :15, `DummyFactory` :23-:34, `generate_password_hash` :36, `start_server` :47 | as `code_overlay.rs` |
| `token_ledgers.rs` | `DiffFactory` :22-:35, `make_fixture` :37, `password_hash` :83, `login` :94, inline spawn (`AppState::new` :131), `mod common` :240, `thread_fixture` :242 | `factory::fixture(fixtures::completed_turn_fixture())`; S |
| `turn_controls.rs` | `thread_fixture` :4, `TestFactory` :27-:40, `generate_password_hash` :126, `start_server` :137-:180, `ws_text` :182 | `factory::fixture(make_fixture())`; S `builder(..).config(<its plan + provider sections>).start()`. Keeps `make_fixture` :43, `poll_thread` |
| `ui.rs` | `NoFactory` :8-:18, `start_ui_server` :20-:30 | S `builder(factory::failing(HarnessError::Spawn("unused".into()))).unauthenticated().start()`; the three tests read `server.addr.port()`. Nothing else changes |
| `worktree.rs` | `git` :14-:31 | `git::run`. Keeps `stdout`, `repo_with_commit`, `git_status` |
| `worktree_threads.rs` | `RecordingFactory` :432-:448, `generate_password_hash` :450, `git` :461, the spawn half of `start` :491-:568, the spawn half of `restart` :1704-:1739, 3 inline handshakes, 2 inline cookie extractions | `factory::from_fn` seeding `native_routes` from the bootstrap; `Harnessed` :479 keeps its name and becomes `{ server: TestServer, project: TestProject, harness, project_id }`; `start(git_repo)` = S + `create_project("worktree-test")` + `git::init_repo_with_commit(project.dir.path())` when asked; `restart` = `builder(factory).data_dir(server.data_dir()).start()`; `server.ws()`, `server.cookie`. Keeps `RecordingHarness`, `wait_for_subagent`, `branch_exists`, `paths_in` |
| `replay_data_dir_lock.rs` | nothing | untouched |
| `tests/common/` | both files | deleted |

Two things the table implies and the implementer should not undo:

- A test that today builds its own `reqwest::Client` (default policy) and takes a cookie keeps
  working with `server.client` (policy none): none of them follows a redirect. The 22 builders
  that already set `Policy::none()` become `server.client`.
- `TestProject` and `TestServer` own their `TempDir`s. Tests that destructured a tuple into
  `_tmp` bind the struct instead; the data directory must outlive the server exactly as before.

## Every site that changes

| File | Change |
| --- | --- |
| `Cargo.toml` | `crates/giskard-testkit` in `members`; `giskard-testkit` in `[workspace.dependencies]` |
| `crates/giskard-testkit/{Cargo.toml, src/lib.rs, src/auth.rs, src/factory.rs, src/fixtures.rs, src/git.rs, src/server.rs, src/ws.rs}` | new |
| `crates/giskard-server/Cargo.toml` | `giskard-testkit = { workspace = true }` under `[dev-dependencies]` |
| `crates/giskard-server/tests/common/` | deleted |
| 20 of the 21 test files | per the migration table |
| `AGENTS.md` "Build & Test" | one sentence: integration tests build their server through `giskard-testkit`'s `TestServer`, which binds port 0 for them |
| `docs/design-straightening-review.md` | mark C6 (step 4) landed |
| `Cargo.lock` | regenerated by cargo |

## Tests

The existing 226 integration tests are the specification; every one must pass unchanged in what
it asserts. The kit gets its own unit tests in `crates/giskard-testkit/src/*.rs`, small and
without a server:

1. `auth::password_hash` output verifies with `Argon2::default().verify_password` for `PASSWORD`
   and rejects `"wrong"`.
2. `factory::failing` returns the given error from `create`; `factory::shared` returns the same
   `Arc` (pointer-equal) from two `create` calls; `factory::from_fn` receives the bootstrap it was
   given.
3. `fixtures::completed_turn_fixture` starts with `ThreadOpened { harness_thread_id: "th_tok" }`
   and ends with `TurnCompleted` carrying `TokenUsage::new(100, 50)`.

And one integration test in the kit's own `tests/smoke.rs`: `TestServer::spawn(factory::failing(..))`
logs in, `create_project_via_api` returns an id the store can load, `ws()` connects and the first
frame parses as a `ServerMessage`. This is the only test that exercises `start` end to end outside
the server's suite.

## Order of work

1. Create the crate with D2, D3, D5, D6, D7 and the unit tests; add the workspace and
   dev-dependency wiring. `cargo test -p giskard-testkit`.
2. Add D4 and the smoke test. `cargo test -p giskard-testkit`.
3. Migrate one small file end to end (`running_tasks.rs`, one test, 191 lines of preamble) and
   run it ten times. Then `thread_lifecycle.rs` and `code_overlay.rs` (the `failing` + config
   shape), `security.rs` (`unauthenticated` is not needed there, but `auth::login` and cookie
   attributes are), `ui.rs` (`unauthenticated`), `worktree_threads.rs` (`data_dir`, git),
   `provider_switch.rs` and `read_only_thread.rs` (`seed`). Each file: migrate, `cargo test -p
   giskard-server --test <name>`, clippy on that target.
4. The remaining files in any order; `e2e_smoke.rs` last.
5. Delete `tests/common/`. `cargo test --workspace`, `cargo clippy --workspace --all-targets --
   -D warnings`, `cargo fmt --check`. Run the full server integration suite three times.

Expected size: about 550 lines added in the kit, about 2 400 deleted across the test files.

## Exit checks

Validated on the base tree; the baseline is given for each. All are run from the repository
root.

```sh
T=crates/giskard-server/tests
# 18 → 0
grep -cE "^fn (generate_password_hash|password_hash)\(" $T/*.rs | awk -F: '{s+=$2} END{print s}'
# 35 → 0
grep -o "AppState::new(" $T/*.rs | wc -l
# 28 → 0
grep -o "impl HarnessFactory for" $T/*.rs | wc -l
# 28 → 0
grep -o "dGhlIHNhbXBsZSBub25jZQ==" $T/*.rs | wc -l
# 28 → 0
grep -o "connect_async" $T/*.rs | wc -l
# 36 → 4, all in security.rs (:224, :288, :315, :329 on the base tree)
grep -c 'get("set-cookie")' $T/*.rs | grep -v ':0$'
# 9 → 0 and 11 → 0
grep -l "^mod common;" $T/*.rs | wc -l; grep -l "common/thread_fixture.rs" $T/*.rs | wc -l
# directory gone
test ! -e $T/common && echo gone
# 7 → 0 (connect_ws + ws_connect), 6 → 0 (ws_text), 7 → 0 (login/login_cookie)
grep -cE "^async fn (connect_ws|ws_connect)\(" $T/*.rs | awk -F: '{s+=$2} END{print s}'
grep -c "^fn ws_text(" $T/*.rs | awk -F: '{s+=$2} END{print s}'
grep -cE "^async fn (login|login_cookie)\(" $T/*.rs | awk -F: '{s+=$2} END{print s}'
# 4 → 0 ("type TestWs" ×3 + "type Ws")
grep -cE "^type (TestWs|Ws) " $T/*.rs | awk -F: '{s+=$2} END{print s}'
# 7 → 0: the "th_tok" fixture is only built by the kit
grep -o '"th_tok"' $T/*.rs | wc -l
# 3 → 0, 2 → 0, 2 → 0
grep -c "^fn make_turn(" $T/*.rs | awk -F: '{s+=$2} END{print s}'
grep -c "^fn seeded_thread(" $T/*.rs | awk -F: '{s+=$2} END{print s}'
grep -c "^fn git(" $T/*.rs | awk -F: '{s+=$2} END{print s}'
# 2 → 0
grep -c "^const TINY_PNG" $T/*.rs | awk -F: '{s+=$2} END{print s}'
# 5 → 2 (e2e_smoke.rs:1802 and turn_controls.rs:43 are distinct fixtures and stay)
grep -c "^fn make_fixture(" $T/*.rs | awk -F: '{s+=$2} END{print s}'
# 14 → 14: no fake was touched (13 plain + read_only_thread.rs's fully-qualified impl)
grep -oE "impl (giskard_harness::)?AgentHarness for" $T/*.rs | wc -l
# 226 → 226: no test added or removed in the server suite
grep -cE "^\s*#\[(tokio::)?test" $T/*.rs | awk -F: '{s+=$2} END{print s}'
```

## Pitfalls

- `secure_cookies = false` must be in the baseline. Without it every login "succeeds" and every
  authenticated request then fails with 401, in all 20 files at once.
- Bind before writing the config, and pass the bound listener to `axum::serve`; never bind twice.
- Keep the `TempDir`s alive: `TestServer` and `TestProject` own them; a test that lets the struct
  drop mid-test deletes the data directory under the running server, and the failure shows up as
  a persistence error unrelated to the test.
- `factory::shared` needs `Arc<dyn AgentHarness>`; pass `harness.clone()` and let coercion happen
  at the call, or the closure in `from_fn` will not type-check against the trait object.
- `from_fn` closures for `ActivityFactory` and `RecordingFactory` must keep their
  `bootstrap.known_threads` seeding; drop it and the reattach tests fail on unknown native ids.
- `e2e_smoke.rs` uses `Arc<AppState>` and passes `&state` to helpers taking `&AppState`;
  `&server.state` satisfies them. Do not wrap `state` in a second `Arc`.
- The `security.rs` tests that assert `Secure`, `HttpOnly`, `Max-Age`, or an absent cookie must
  keep reading `set-cookie` themselves; `auth::login` returns only the name=value pair.
- `provider_switch.rs` names its local struct `TestServer`; rename it (the table says `Fixture`)
  before importing the kit's, or the two collide.
- A test that spawns two servers (`worktree_threads.rs` restart, `history_sync.rs` ×3) gets two
  `TestServer`s; nothing in the kit is process-global.

## Stop rules

Stop and re-cut if the diff:

- changes an assertion, a sent message, a config section a test writes, or any fake's `impl AgentHarness`;
- adds `pub` items to `giskard-server` or changes `AppState::new`/`build_app`/`HarnessFactory`;
- adds an `allow` attribute anywhere;
- leaves a `fn generate_password_hash`, an `impl HarnessFactory`, or a hand-rolled handshake in
  any test file;
- puts a helper into the kit that only one test file uses (that helper stays local);
- touches `crates/giskard-harness-replay` or the unit-test fakes under `crates/giskard-server/src`.
