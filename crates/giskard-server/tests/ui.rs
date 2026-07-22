//! Smoke test: the desktop UI page is served at `/`, is public, and is self-contained (§13).

use std::sync::Arc;

use giskard_persist::store::ProjectConfig;
use giskard_server::{AppState, HarnessFactory, build_app};

struct NoFactory;
#[async_trait::async_trait]
impl HarnessFactory for NoFactory {
    async fn create(
        &self,
        _c: &ProjectConfig,
    ) -> Result<Arc<dyn giskard_harness::AgentHarness>, giskard_core::HarnessError> {
        Err(giskard_core::HarnessError::Spawn("unused".into()))
    }
}

#[tokio::test]
async fn index_page_is_served_and_public() {
    let port = 19300;
    let tmp = tempfile::TempDir::new().unwrap();
    let store = Arc::new(giskard_persist::PersistStore::new(tmp.path().to_path_buf()));
    let state = AppState::new(store, Arc::new(NoFactory), (0..32u8).collect());
    let app = build_app(state);
    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{port}"))
        .await
        .unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    // No cookie: the page and its same-origin assets must load without authentication. The UI's
    // script/stylesheet are separate assets (CSP: `script-src 'self'`) served under content-hashed
    // URLs (cache-busting); the served HTML points at the current ones. The favicon is same-origin
    // app chrome.
    let index_html = reqwest::get(format!("http://127.0.0.1:{port}/"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let js_url = between(&index_html, "<script src=\"", "\"").to_string();
    let css_url = between(&index_html, "<link rel=\"stylesheet\" href=\"", "\"").to_string();
    assert!(
        js_url.starts_with("/app.") && js_url.ends_with(".js"),
        "script is served from a content-hashed URL: {js_url}"
    );
    assert!(
        css_url.starts_with("/app.") && css_url.ends_with(".css"),
        "stylesheet is served from a content-hashed URL: {css_url}"
    );

    let mut body = index_html.clone();
    for path in ["/favicon.svg".to_string(), js_url, css_url] {
        body.push('\n');
        body.push_str(
            &reqwest::get(format!("http://127.0.0.1:{port}{path}"))
                .await
                .unwrap()
                .text()
                .await
                .unwrap(),
        );
    }
    assert!(body.contains("<title>Giskard</title>"));
    assert!(
        body.contains(
            r#"<img class="sidebar-logo" src="/favicon.svg" width="24" height="24" alt="" aria-hidden="true" />"#
        ),
        "sidebar shows the Giskard icon"
    );
    assert!(body.contains("/api/ws"), "app talks to the WS endpoint");
    assert!(
        body.contains("/api/ws-ticket"),
        "app fetches a WS auth ticket"
    );
    assert!(
        body.contains("ws.onmessage = (m) => {\n    if (state.ws !== ws) return;"),
        "stale websocket frames from a replaced connection are ignored"
    );
    assert!(
        body.contains("function serverMessageThreadId(msg)")
            && body.contains("function isThreadScopedServerMessage(msg)")
            && body.contains("function isCurrentThreadServerMessage(msg)")
            && body.contains("if (!isCurrentThreadServerMessage(msg)) return;"),
        "thread-scoped server messages are gated by the active thread before rendering"
    );
    assert!(
        body.contains(
            "case \"token_update\":\n      if (msg.scope === \"thread\") renderTokens(msg.ledger);"
        ),
        "only thread-scoped token updates render into the thread usage menu"
    );
    assert!(body.contains("send_input"), "composer wired to SendInput");
    assert!(
        body.contains("id=\"stopBtn\""),
        "UI exposes a stop button for live turns"
    );
    assert!(
        body.contains("type:\"interrupt\""),
        "stop button sends the Interrupt client message"
    );
    assert!(
        body.contains("msg.action===\"interrupt\""),
        "interrupt errors are handled explicitly"
    );
    assert!(
        body.contains("state.interruptPending = false"),
        "interrupt errors re-enable the stop control"
    );
    assert!(
        body.contains("activeTurn"),
        "UI tracks active turns for interrupt controls"
    );
    assert!(
        body.contains("renderApprovalRequest"),
        "UI renders approval requests as actionable transcript cards"
    );
    assert!(
        body.contains("case \"approval_resolved\"")
            && body.contains("resolveApprovalRequest(msg.request_id, msg.decision)")
            && body.contains("function resolveApprovalRequest(id, decision)")
            && body.contains("closeApprovalNotification(tid, id)")
            && body.contains("const msg = entry ? entry.msg : approvalRowById(id)")
            && body.contains("state.pendingApprovals.delete(id)"),
        "approval decisions broadcast from another tab resolve pending approval cards"
    );
    assert!(
        body.contains("type:\"approval_decision\""),
        "approval cards send approval decisions over the WebSocket"
    );
    assert!(
        body.contains("accept_for_session"),
        "approval cards expose session-scoped approval"
    );
    assert!(
        body.contains("addApprovalButton(actions, id, \"accept_for_session\", \"Session\", \"session\", available)")
            && body.contains("button.session {")
            && body.contains("button.session:hover"),
        "approval cards style session separately from the default cancel button"
    );
    assert!(
        body.contains("id=\"approvalSel\""),
        "approval policy is exposed as a selector"
    );
    assert!(
        body.contains("<label for=\"approvalSel\">Approvals</label>"),
        "approval policy selector has a visible label"
    );
    assert!(
        body.contains(">Ask first</option>") && body.contains(">Auto approve</option>"),
        "approval policy selector uses action-oriented option labels"
    );
    assert!(
        body.contains("type:\"set_approval_policy\", thread_id: state.threadId, policy"),
        "approval policy changes target the active thread"
    );
    assert!(
        body.contains("setApprovalPolicy(s.approval_policy || \"ask\")"),
        "thread state hydrates the approval policy selector"
    );
    assert!(
        !body.contains("id=\"pmApproval\""),
        "approval policy is not configured at project creation"
    );
    assert!(
        body.contains("pendingApprovals"),
        "UI de-duplicates pending approval cards"
    );
    assert!(
        body.contains("answeredApprovals")
            && body.contains("approvalStateKey(request)")
            && body.contains("const answered = state.answeredApprovals.get(stateKey)")
            && body.contains("JSON.stringify(requestOrId.kind")
            && body.contains("String(requestOrId.reason")
            && body.contains("JSON.stringify(requestOrId.metadata")
            && body.contains("state.answeredApprovals.set")
            && body.contains("request: entry.request"),
        "UI remembers answered approvals and matches them only to the replayed request"
    );
    assert!(
        !body.contains("renderAnsweredApprovalDecisionsForThread")
            && !body.contains("renderApprovalRequest(answered.request)")
            && body.contains("state.renderedApprovalStateKeys"),
        "UI does not append answered approval cards outside transcript replay order"
    );
    assert!(
        body.contains("applyApprovalDecision")
            && body.contains(".approval-actions")
            && body.contains("approval-result")
            && body.contains("approvalDecisionClass"),
        "UI renders answered approvals as decision cards instead of disabled prompts"
    );
    assert!(
        body.contains(
            "msg.querySelectorAll(\".approval-actions\").forEach(el => el.remove());\n  const title = msg.querySelector(\".approval-title\");"
        ),
        "resolved approvals keep the row copy button enabled"
    );
    assert!(
        body.contains("decision-decline") && body.contains("decision-cancel"),
        "UI styles declined and cancelled approvals distinctly from accepted approvals"
    );
    assert!(
        body.contains("if (decision===\"accept\") return \"decision-accept\"")
            && body.contains("if (decision===\"accept_for_session\") return \"decision-session\"")
            && body.contains("if (decision===\"decline\") return \"decision-decline\"")
            && body.contains("if (decision===\"cancel\") return \"decision-cancel\""),
        "UI maps every approval decision to an explicit resolved-card class"
    );
    assert!(
        body.contains("if (decision===\"accept_for_session\") return \"Session\"")
            && body.contains("return decision.charAt(0).toUpperCase() + decision.slice(1)"),
        "UI labels session approvals explicitly and labels other decisions predictably"
    );
    assert!(
        body.contains(".msg.approval.resolved.decision-accept")
            && body.contains(".msg.approval.resolved.decision-session")
            && body.contains(".msg.approval.resolved.decision-decline")
            && body.contains(".msg.approval.resolved.decision-cancel"),
        "UI has distinct resolved approval styles for accept, session, decline, and cancel"
    );
    assert!(
        body.contains("renderApprovalMetadata(body, request.metadata || [])"),
        "approval cards render structured approval metadata"
    );
    assert!(
        body.contains("approval-meta-row"),
        "approval metadata is rendered as dedicated rows"
    );
    assert!(
        body.contains("return kind.command || \"(empty command)\"")
            && !body.contains("const cwd = kind.cwd ? `\\n${kind.cwd}` : \"\""),
        "command approval cwd is rendered as metadata, not as a second detail line"
    );
    assert!(
        body.contains("item.kind === \"path\" && item.source_link"),
        "approval path metadata only links explicit source paths"
    );
    assert!(
        body.contains("if (item.kind === \"host\") return approvalHostValue(item)"),
        "approval host metadata is routed through the host formatter"
    );
    assert!(
        body.contains("if (item.kind === \"text\") return String(item.value || \"\")"),
        "approval text metadata is rendered from the value field"
    );
    assert!(
        body.contains("if (item.protocol) value += `${item.protocol}://`;"),
        "approval host metadata renders the protocol"
    );
    assert!(
        body.contains(
            "if (item.port !== undefined && item.port !== null) value += `:${item.port}`;"
        ),
        "approval host metadata renders the port"
    );
    assert!(
        body.contains("if (item.target) value += ` (${item.target})`;"),
        "approval host metadata renders the target URL/detail"
    );
    assert!(
        body.contains("body.textContent = value"),
        "plain approval metadata stays visible without source-overlay behavior"
    );
    assert!(
        body.contains("pendingServerRequests"),
        "UI de-duplicates pending server request cards"
    );
    assert!(
        body.contains("renderServerRequest"),
        "UI renders non-approval server requests as transcript cards"
    );
    assert!(
        body.contains("case \"server_request_received\""),
        "UI consumes server request events"
    );
    assert!(
        body.contains("case \"server_request_resolved\""),
        "UI consumes server request resolution events"
    );
    assert!(
        body.contains("pending_server_requests"),
        "UI replays pending server requests from live snapshots"
    );
    assert!(
        body.contains("type:\"server_request_response\""),
        "server request cards send responses over the WebSocket"
    );
    assert!(
        body.contains("item/tool/call"),
        "UI has first-class dynamic tool-call request handling"
    );
    assert!(
        body.contains("item/tool/requestUserInput"),
        "UI has first-class user-input tool request handling"
    );
    assert!(
        body.contains("mcpServer/elicitation/request"),
        "UI has first-class MCP elicitation handling"
    );
    assert!(
        body.contains("if (!fields) return {}"),
        "MCP elicitation cards with no schema fields submit empty content without a JSON editor"
    );
    assert!(
        body.contains("appendJsonPreviewIfMeaningful(body, p.arguments)"),
        "dynamic tool-call requests suppress empty argument previews"
    );
    assert!(
        body.contains("appendJsonPreviewIfMeaningful(body, request.params)"),
        "unknown and unsupported server requests suppress empty params previews"
    );
    assert!(
        body.contains("hasMeaningfulJson(kind) ? JSON.stringify(kind) : \"\""),
        "unknown approval kinds suppress empty JSON detail"
    );
    assert!(
        body.contains("if (questions.length) body.append(fields)"),
        "user-input request cards do not append empty field containers"
    );
    assert!(
        !body.contains("Content JSON"),
        "MCP elicitation cards should not show a confusing empty content section"
    );
    assert!(
        body.contains("account/chatgptAuthTokens/refresh"),
        "UI explicitly handles ChatGPT auth refresh requests"
    );
    assert!(
        body.contains("attestation/generate"),
        "UI explicitly handles client attestation requests"
    );
    assert!(
        body.contains("renderUnsupportedServerRequest"),
        "known unsupported server requests use an explicit error response"
    );
    assert!(
        body.contains("Giskard cannot refresh ChatGPT auth tokens."),
        "auth refresh requests cannot accidentally return empty success"
    );
    assert!(
        body.contains("Giskard cannot generate client attestation tokens."),
        "attestation requests cannot accidentally return empty success"
    );
    assert!(
        body.contains("Return Empty Result"),
        "UI exposes an intentional unknown-request fallback"
    );
    assert!(
        body.contains("resetResolvingServerRequests"),
        "server request response errors re-enable pending cards"
    );
    assert!(
        body.contains("awaitingInitialThreadState"),
        "UI distinguishes initial thread snapshots from metadata refreshes"
    );
    assert!(
        body.contains("contextUsed"),
        "UI tracks context gauge usage separately from cumulative token totals"
    );
    assert!(
        body.contains("updateGaugeFromUsage(ev.usage)"),
        "turn completion updates the context gauge from per-turn usage"
    );
    assert!(
        body.contains("updateGaugeFromTurns(turns)"),
        "initial history updates the context gauge from the latest persisted turn"
    );
    assert!(
        body.contains("input tokens are the best available proxy for current context occupancy"),
        "UI documents the selected Codex context-usage source"
    );
    assert!(
        !body.contains("updateGauge(t.total"),
        "cumulative token totals must not drive the context gauge"
    );
    assert!(
        body.contains("id=\"usageBtn\"") && body.contains("id=\"usageMenu\""),
        "context usage is exposed as a header button with a popover menu"
    );
    assert!(
        body.contains("id=\"effortSel\"")
            && body.contains("EFFORT_OPTIONS")
            && body.contains("supports_reasoning_effort")
            && body.contains("reasoning_effort:effort")
            && body.contains("unset.textContent = \"Default\""),
        "reasoning models expose a thread-header effort selector"
    );
    assert!(
        body.contains("function modelOptionLabel")
            && body.contains("`${name} [${m.provider}]`")
            && body.contains("o.textContent = modelOptionLabel(m)"),
        "model choices show their provider next to the model name"
    );
    assert!(
        body.contains("function modelProviderLocked(provider)")
            && body.contains("o.disabled = locked")
            && body.contains("Create a new thread to use"),
        "provider-bound threads gray out models from other providers"
    );
    assert!(
        body.contains("if (state.threadReadOnly) return false;"),
        "read-only threads unlock the picker so a replacement provider can be selected"
    );
    assert!(
        body.contains("state.threadReadOnly = true;") && body.contains("thread_read_only"),
        "the thread_read_only warning marks the thread read-only in client state"
    );
    assert!(
        body.contains("Thread resumed under provider"),
        "a confirmed provider switch clears the read-only state and announces the new provider"
    );
    assert!(
        body.contains("id=\"readOnlyBanner\"") && body.contains("function updateReadOnlyBanner"),
        "read-only threads show a persistent banner instead of only a transient toast"
    );
    assert!(
        body.contains("Read-only thread — pick a model above to reactivate it."),
        "the composer is disabled with an actionable placeholder on read-only threads"
    );
    assert!(
        body.contains("state.pendingModelBeforeSelect")
            && body.contains("if (msg.action===\"select_model\")")
            && body.contains("state.currentModel = state.pendingModelBeforeSelect"),
        "failed async model selection restores the previous selected model"
    );
    assert!(
        body.contains("id=\"compactBtn\"")
            && body.contains("function compactContext")
            && body.contains("type:\"compact_context\", thread_id: state.threadId")
            && body.contains("state.compactPending ? \"Compacting")
            && body.contains("msg.action===\"compact_context\"")
            && body.contains("isContextCompactionItem"),
        "context menu exposes a manual context compaction action with pending/error recovery"
    );
    assert!(
        body.contains("function renderUsageMenu")
            && body.contains("Current Context")
            && body.contains("Cumulative Tokens"),
        "context menu renders context occupancy and cumulative token totals"
    );
    assert!(
        body.contains("state.tokenLedger = led && led.total ? led : null")
            && body.contains("renderTokenStats(state.tokenLedger)"),
        "token updates populate the context usage menu"
    );
    assert!(
        !body.contains("id=\"ctxTokens\"") && !body.contains("$(\"ctxTokens\")"),
        "cumulative token totals are not rendered as a permanent right-column section"
    );
    assert!(
        body.contains("id=\"settingsBtn\"")
            && body.contains("id=\"settingsMenu\"")
            && body.contains("id=\"appearanceSel\"")
            && body.contains("function toggleSettingsMenu"),
        "appearance is exposed from the sidebar settings popover"
    );
    assert!(
        body.contains("PROJECT_COLLAPSE_KEY = \"giskard.collapsedProjects\"")
            && body.contains("collapsedProjects:new Set(loadCollapsedProjects())")
            && body.contains("className = \"project-toggle\"")
            && body.contains("aria-expanded")
            && body.contains("setProjectCollapsed(p.id, !state.collapsedProjects.has(p.id))")
            && body.contains(".project-threads[hidden]")
            && body.contains("localStorage.setItem(PROJECT_COLLAPSE_KEY"),
        "project rows can collapse and persist their thread-list visibility"
    );
    assert!(
        body.contains("className = \"project-menu-btn\"")
            && body.contains("Remove project")
            && body.contains("Remove project from Giskard?")
            && body.contains("Your source directory will not be touched:")
            && body.contains("Remove from Giskard")
            && body.contains(
                "including\n          its saved threads, transcripts, and project metadata"
            )
            && body.contains("openRemoveProjectModal(p)")
            && body.contains("api(\"DELETE\", `/api/projects/${project.id}`)")
            && body.contains("Project removed from Giskard.")
            && body.contains("function clearProjectView(pid)")
            && body.contains("clearStoredLastThreadForProject(pid)")
            && body.contains("querySelectorAll(\".thread-menu, .project-menu\")"),
        "project rows expose a remove action with source-directory-safe confirmation"
    );
    assert!(
        body.contains("overflow:visible")
            && body.contains("z-index:40")
            && body.contains("z-index:70"),
        "settings popover is not clipped behind the thread column"
    );
    assert!(
        body.contains("grid-template-columns:var(--col-left,260px) 6px minmax(360px,1fr)")
            && !body.contains("id=\"resizeRight\"")
            && !body.contains("id=\"btnInfo\"")
            && !body.contains("col ctx")
            && !body.contains("drawer-right")
            && !body.contains("giskard.colRight"),
        "the permanent right column and right mobile drawer are removed"
    );
    assert!(
        body.contains("scrollbar-color:var(--scroll-thumb) var(--scroll-track)")
            && body.contains("scrollbar-gutter:stable")
            && body.contains("#transcript::-webkit-scrollbar")
            && body.contains("#transcript::-webkit-scrollbar-thumb:hover")
            && body.matches("--scroll-track:#").count() >= 4
            && body.matches("--scroll-thumb:#").count() >= 4
            && body.matches("--scroll-thumb-hover:#").count() >= 4
            && !body.contains("body::-webkit-scrollbar")
            && !body.contains("*::-webkit-scrollbar"),
        "the transcript scrollbar is scoped and appearance-aware"
    );
    assert!(
        body.contains(
            "const shouldResetTranscript = state.awaitingInitialThreadState || state.awaitingThreadResync"
        ) && body.contains("state.awaitingThreadResync = false")
            && body.contains(
                "if (shouldResetTranscript) {\n    resetTranscriptForAuthoritativeSnapshot();\n  }"
            ),
        "UI only clears the transcript for initial snapshots or subscribe resyncs"
    );
    assert!(
        body.contains("function resetTranscriptForAuthoritativeSnapshot()")
            && body.contains("state.pendingUserEl = null")
            && body.contains("state.pendingUserText = null")
            && body.contains("state.oldestTurnId = null")
            && body.contains("state.hasMoreHistory = false")
            && body.contains("setTurnActive(false);"),
        "subscribe resync clears transient browser-only state before replaying history"
    );
    assert!(
        body.contains("beginRenameThread(el, pid, t.id)"),
        "thread rename starts from the sidebar row title next to the actions menu"
    );
    assert!(
        body.contains("`/api/projects/${pid}/threads/${tid}/title`"),
        "thread rename is persisted through the server API"
    );
    assert!(
        body.contains("if (state.threadId === tid) setThreadTitle(savedTitle)"),
        "renaming the open thread updates the header/mobile title after save"
    );
    assert!(
        body.contains("updateThreadRowTitle(s.id || s.thread_id || state.threadId, s.title)"),
        "thread_state title broadcasts update the sidebar row as well as the header"
    );
    assert!(
        !body.contains(
            "// History now arrives separately via `history_page` (H6); clear the transcript ready for it.\n  $(\"transcript\").innerHTML=\"\";\n  resetRenderState();"
        ),
        "thread_state metadata broadcasts must not unconditionally clear the transcript"
    );
    assert!(
        body.contains("id=\"tasksBtn\"") && body.contains("id=\"tasksMenu\""),
        "thread header exposes a running-task summary button and menu"
    );
    assert!(
        body.contains("function renderTasksButton")
            && body.contains("taskButtonState")
            && body.contains("$(\"tasksCount\").textContent = String(count)"),
        "task button shows the current running-task count and state"
    );
    assert!(
        body.contains(".tasks-btn.state-idle")
            && body.contains(".tasks-btn.state-running")
            && body.contains(".tasks-btn.state-stopping"),
        "task button has distinct visual states for idle, running, and stop-requested tasks"
    );
    assert!(
        body.contains("function renderTaskCards")
            && body.contains("renderTaskCards($(\"tasksCommandList\"), commandTasks")
            && body.contains("renderTaskCards($(\"tasksToolList\"), toolTasks"),
        "running-task cards render inside the task menu"
    );
    assert!(
        body.contains("tasks-section-title")
            && body.contains("Commands</div>")
            && body.contains("Tools</div>")
            && body.contains("No running commands.")
            && body.contains("No running tools."),
        "task menu separates running commands from running tools"
    );
    assert!(
        body.contains("const summaryHtml = count")
            && body.contains(": \"\";")
            && body.contains("${summaryHtml}"),
        "empty task menus should not duplicate the no-running-tasks empty state"
    );
    assert!(
        body.contains("taskGroupsById:new Map()")
            && body.contains("taskGroupsByItemId:new Map()")
            && body.contains("expandedTaskDetails:new Map()"),
        "UI tracks transcript task groups and selected task details by item id"
    );
    assert!(
        body.contains("function expandedTaskDetailIds")
            && body.contains("ids = new Set(ids ? [ids] : [])"),
        "task groups track a set of expanded task details per group"
    );
    assert!(
        body.contains("if (kind===\"tool_call\") msg.dataset.toolItemId = key")
            && body.contains("else msg.dataset.commandItemId = key"),
        "task groups give every nested task detail row a selectable item id"
    );
    assert!(
        body.contains("entry.append(row, msg)")
            && body.contains("group.list.append(entry)")
            && body.contains("item.entry.classList.toggle(\"expanded\""),
        "clicking a task summary expands the detail inline inside that task entry"
    );
    assert!(
        body.contains("if (detailIds.has(key))")
            && body.contains("detailIds.delete(key)")
            && body.contains("clearTaskSelection();"),
        "clicking an expanded task summary collapses that task detail again"
    );
    assert!(
        body.contains("head.title = \"Show or hide the task rows\"")
            && body.contains("head.setAttribute(\"aria-label\", \"Show or hide the task rows\")")
            && body.contains("if (state.expandedTaskGroups.has(groupId)) {")
            && body.contains("state.expandedTaskGroups.delete(groupId);")
            && body.contains("state.expandedTaskGroups.add(groupId);"),
        "the task group summary toggles the visibility of the task rows, not every row's detail"
    );
    let toggle_group = between(
        &body,
        "function toggleTaskGroup(",
        "function taskVisualStateFromElement(",
    );
    assert!(
        !toggle_group.contains("detailIds"),
        "toggling a task group's row visibility preserves each row's expanded detail state"
    );
    assert!(
        toggle_group.contains("state.selectedCommandId = null;")
            && toggle_group.contains("renderRunningCommands();"),
        "collapsing a task group clears the selected row and refreshes the running-task panel"
    );
    assert!(
        body.contains("wireTaskGroupItemRow(row, group.id, key)")
            && body.contains("function wireTaskGroupItemRow(row, groupId, itemId)")
            && body.contains("selectTaskGroupItem(groupId, key);")
            && !body.contains("selectCommand(key);"),
        "in-thread task summary clicks toggle inline details without scrolling the transcript, \
         including after item-id migration"
    );
    assert!(
        body.contains(".task-group-body { white-space:normal; font-size:12px; }")
            && body.contains(".task-group-item-status { color:var(--muted); font-size:11px;")
            && body.contains(".task-group-entry > .msg .role { font-size:10px; }"),
        "task group summaries and details use tighter typography than normal transcript rows"
    );
    assert!(
        body.contains(".task-group-list[hidden] { display:none; }"),
        "collapsing a task group actually hides its rows (the flex display must not defeat [hidden])"
    );
    assert!(
        body.contains("function appendBubble")
            && body.contains("function bubble(cls, role)")
            && body.contains("breakTaskGroup();")
            && body.contains("function taskBubble"),
        "normal transcript rows close active task groups while task rows append inside them"
    );
    assert!(
        body.contains("if (allTerminal) state.expandedTaskGroups.delete(group.id)")
            && body.contains("else state.expandedTaskGroups.add(group.id)")
            && body.contains("manuallyToggledTaskGroups"),
        "task groups auto-expand while running and auto-collapse after terminal states"
    );
    assert!(
        body.contains(
            "taskBubble(key, \"command_execution\", \"cmd running-command\", \"command\")"
        ) && body.contains(
            "taskBubble(key, \"tool_call\", \"tool running-tool state-running\", \"tool\")"
        ),
        "live command and tool starts render inside transcript task groups"
    );
    assert!(
        body.contains("? taskBubble(key, p.kind, classForPayload(p), roleForPayload(p))")
            && body.contains("isTaskPayloadKind(p.kind)"),
        "persisted command/tool items are grouped even when they are singletons"
    );
    assert!(
        body.contains("expandedTaskDetailIds(group.id).add(key)")
            && body.contains("(task ? task.entry : msg).scrollIntoView"),
        "header task-menu selection expands inline detail before scrolling to the task entry"
    );
    assert!(
        body.contains("const prevTaskGroup = state.activeTaskGroup")
            && body.contains("state.activeTaskGroup = prevTaskGroup")
            && body.contains("active.el.parentElement === target")
            && body.contains("function renderPersistedTurn(turn) {\n  breakTaskGroup();"),
        "history rendering preserves live grouping state and breaks groups at turn boundaries"
    );
    assert!(
        body.contains("renderItemBody(toolBody, {")
            && body.contains("name:cmd.command || \"tool\"")
            && body.contains("state.streamElsByItemId.set(key, toolBody)"),
        "running tool snapshots create grouped transcript rows when no stream row exists yet"
    );
    assert!(
        !body.contains("id=\"ctxCommands\""),
        "running tasks are not rendered as a permanent right-column section"
    );
    assert!(
        body.contains("case \"running_tasks\""),
        "UI consumes server-owned running task snapshots"
    );
    assert!(
        body.contains("taskTitleText") && body.contains("cmd.kind === \"tool\""),
        "UI shows tool calls as running tasks alongside commands"
    );
    assert!(
        body.contains("function stopTask"),
        "UI can stop a running task (command terminate or tool turn-interrupt)"
    );
    assert!(
        body.contains("commandBodyElsByItemId"),
        "UI tracks running command transcript rows after finalization"
    );
    assert!(
        body.contains("commandIsRunningStatus"),
        "UI keeps in-progress completed command items visible"
    );
    assert!(
        body.contains("setInterval(updateRunningCommandDurations, 1000)"),
        "UI refreshes running command durations once per second"
    );
    assert!(
        body.contains("terminalCommandStatus"),
        "UI renders terminal command status with elapsed duration"
    );
    assert!(
        body.contains("commandStopRequestedByItemId"),
        "UI remembers stop requests until terminal command status arrives"
    );
    assert!(
        body.contains("stop requested after"),
        "UI labels pending command termination as a stop request"
    );
    assert!(
        body.contains("(stop requested)"),
        "UI annotates terminal command status after a stop request"
    );
    assert!(
        body.contains("No longer tracked"),
        "UI has a non-terminal fallback for stale stopped command snapshots"
    );
    assert!(
        !body.contains("terminatedByUser && p.status"),
        "UI must not rewrite successful terminal status to terminated"
    );
    assert!(
        body.contains("commandVisualStateFromStatus"),
        "UI maps command statuses to visual states"
    );
    assert!(
        body.contains("commandStateSymbol"),
        "UI renders command state symbols"
    );
    assert!(
        body.contains("cmd-symbol"),
        "UI includes a non-color command state cue"
    );
    assert!(
        body.contains("state-running"),
        "UI styles running command state"
    );
    assert!(
        body.contains("state-succeeded"),
        "UI styles succeeded command state"
    );
    assert!(
        body.contains("state-failed"),
        "UI styles failed command state"
    );
    assert!(
        body.contains("state-terminated"),
        "UI styles terminated command state"
    );
    assert!(
        body.contains("s===\"completed\" || s===\"succeeded\" || s===\"success\""),
        "UI maps successful command statuses to succeeded state"
    );
    assert!(
        body.contains("s===\"failed\" || s===\"error\""),
        "UI maps failed command statuses to failed state"
    );
    assert!(
        body.contains("s===\"terminated\" || s===\"declined\""),
        "UI maps terminated and declined command statuses to terminated state"
    );
    assert!(
        body.contains("if (stateName===\"succeeded\") return \"✓\""),
        "UI renders the succeeded command symbol"
    );
    assert!(
        body.contains("if (stateName===\"failed\") return \"✕\""),
        "UI renders the failed command symbol"
    );
    assert!(
        body.contains("if (stateName===\"terminated\") return \"■\""),
        "UI renders the terminated command symbol"
    );
    assert!(
        body.contains("return \"●\""),
        "UI renders the running command symbol"
    );
    assert!(
        !body.contains(".cmd-status::before"),
        "command state symbols are DOM content, not CSS-only decoration"
    );
    assert!(
        body.contains("startedAtMs"),
        "UI tracks command start timestamps for elapsed duration"
    );
    assert!(
        body.contains("selectCommand"),
        "running command summary can select transcript command rows"
    );
    assert!(
        body.contains("type:\"terminate_command\""),
        "running command controls send terminate requests"
    );
    assert!(
        body.contains("running-command"),
        "transcript command rows expose running command state"
    );
    assert!(
        body.contains("function scheduleWsReconnect(reason)")
            && body.contains("WS_RECONNECT_BASE_MS")
            && body.contains("WS_RECONNECT_MAX_MS"),
        "UI reconnects the WebSocket with bounded backoff"
    );
    assert!(
        body.contains("visibilitychange")
            && body.contains("window.addEventListener(\"online\"")
            && body.contains("window.addEventListener(\"offline\""),
        "UI reconnects on tab focus and network recovery"
    );
    assert!(
        body.contains("id=\"wsStatusBadge\"")
            && body.contains("function renderWsStatus()")
            && body.contains("state-reconnecting")
            && body.contains("state-draft")
            && body.contains("case \"draft\": return \"Draft\"")
            && body.contains("el.hidden = !state.threadId && !isDraftThread()"),
        "UI exposes persistent connection status without toast spam"
    );
    assert!(
        body.contains("setWsStatus(\"draft\", \"Draft thread. Send a message to create it.\")"),
        "draft threads use a neutral status rather than disconnected"
    );
    assert!(
        body.contains("header.thr button.badge:disabled"),
        "disabled header buttons share the same muted style"
    );
    assert!(
        body.contains("Connecting to agent"),
        "UI exposes WS connecting state"
    );
    assert!(
        body.contains("Reconnecting to agent"),
        "UI exposes WS reconnecting state"
    );
    assert!(
        body.contains(
            "$(\"sendBtn\").disabled = readOnly || state.activeTurn || (!ready && !draft)"
        ) && body.contains("if (isDraftThread()) {")
            && body.contains("startDraftThread(text);"),
        "UI allows draft first sends without a WebSocket, blocks existing-thread sends until open, \
         and blocks sends on read-only threads"
    );
    assert!(
        body.contains("inputDrafts:new Map()")
            && body.contains("function composerDraftKey()")
            && body.contains("return `thread:${state.threadId}`")
            && body.contains("return `draft:${state.draftThread.projectId}`")
            && body.contains("function saveComposerDraft()")
            && body.contains("function restoreComposerDraft()")
            && body.contains("$(\"input\").addEventListener(\"input\", saveComposerDraft);"),
        "pending composer text is cached per existing thread and per project draft"
    );
    assert!(
        body.contains(
            "state.awaitingIncrementalResync = true;\n      state.awaitingThreadResync = false;\n      send({ type:\"subscribe\", thread_id: state.threadId, since: state.newestPersistedTurnId });"
        ) && body.contains("state.awaitingThreadResync = true;\n      state.awaitingIncrementalResync = false;\n      send({ type:\"subscribe\", thread_id: state.threadId });")
            && body.contains("awaitingThreadResync:false")
            && body.contains("awaitingIncrementalResync:false"),
        "reconnect asks for an incremental delta when it has a cursor, else a full authoritative resync"
    );
    assert!(
        body.contains("const WS_BACKGROUND_CLOSE_GRACE_MS = 10000")
            && body.contains(
                "ws._giskardBackgroundedAt = document.visibilityState === \"hidden\" ? Date.now() : 0"
            )
            && body.contains("if (state.ws) state.ws._giskardBackgroundedAt = Date.now()")
            && body.contains("state.ws._giskardResumedAt = Date.now()")
            && body.contains(
                "Date.now() - resumedAt < WS_BACKGROUND_CLOSE_GRACE_MS"
            ),
        "recently backgrounded sockets do not toast for lifecycle disconnects on resume"
    );
    let onerror_start = body
        .find("ws.onerror = () => {")
        .expect("websocket onerror handler exists");
    let onclose_start = body
        .find("ws.onclose = (ev) => {")
        .expect("websocket onclose handler exists");
    let onerror = &body[onerror_start..onclose_start];
    assert!(
        onerror.contains("recordWsProblem(\"WebSocket connection failed. Reconnecting...\")")
            && !onerror.contains("surfaceWsProblem"),
        "websocket onerror updates status without directly emitting a notice"
    );
    assert!(
        body.contains("surfaceWsProblem(message, \"warning\")")
            && body.contains("abnormalForegroundClose"),
        "foreground connection failures remain visible from the close path"
    );
    assert!(
        body.contains("Invalid WebSocket message from server"),
        "UI surfaces malformed WS frames"
    );
    assert!(
        body.contains("renderedHarnessItemIds"),
        "UI de-duplicates rendered harness items"
    );
    assert!(
        body.contains("streamElsByItemId"),
        "UI tracks streamed items independently"
    );
    assert!(
        body.contains("renderedItemIds"),
        "UI de-duplicates completed items by Giskard item id"
    );
    assert!(
        body.contains("finalizeStreamedItem"),
        "UI finalizes streamed agent messages instead of duplicating them"
    );
    assert!(
        body.contains("hasVisiblePayload"),
        "UI has an explicit visibility gate for transcript items"
    );
    assert!(
        body.contains("p.kind===\"file_change\""),
        "UI has explicit file-change transcript handling"
    );
    assert!(
        body.contains("renderFileChange"),
        "UI renders file-change items"
    );
    assert!(
        body.contains("mergeFileChangeWithPrevious")
            && body.contains("mergeFileChangePayload")
            && body.contains("body.parentElement._fileChangePayload"),
        "UI merges consecutive file-change transcript rows"
    );
    assert!(
        body.contains("file-change-status")
            && body.contains("status.className = \"badge file-change-status\"")
            && body.contains("status.textContent = c.status")
            && body.contains("status:(c && c.status) || (p && p.status)")
            && !body.contains("status:p && p.status")
            && !body.contains("status:incoming.status || current.status")
            && !body.contains("status.textContent = \"status: \" + c.status")
            && !body.contains("status.textContent = \"status: \" + normalized.status"),
        "UI renders file-change statuses as per-entry badges"
    );
    assert!(
        body.contains("openDiffOverlay")
            && body.contains("diff-open")
            && body.contains("markdownCodeFence(\"diff\", diff)")
            && body.contains("/render"),
        "UI opens file-change diffs in the server-rendered source overlay"
    );
    assert!(
        body.contains("renderToolBody"),
        "UI renders tool-call items"
    );
    assert!(
        body.contains("toolPayloadsByItemId"),
        "UI tracks tool-call payloads by item"
    );
    assert!(
        body.contains(".msg.tool.state-succeeded") && body.contains(".msg.tool.state-failed"),
        "UI styles succeeded and failed tool-call rows distinctly"
    );
    assert!(
        body.contains("function toolVisualStateFromStatus")
            && body.contains("if (error) return \"failed\"")
            && body.contains("if (s===\"completed\" || s===\"succeeded\" || s===\"success\") return \"succeeded\"")
            && body.contains("if (s===\"failed\" || s===\"error\") return \"failed\""),
        "UI maps tool-call success and failure statuses to distinct visual states"
    );
    assert!(
        body.contains("toolStatusLabel(p.status, p.error, msg, stateName)")
            && body.contains("terminalCommandStatus(error && !status ? \"failed\" : status"),
        "tool-call rows use the same terminal status wording as command rows"
    );
    assert!(
        body.contains("meta.className = \"meta cmd-meta\"")
            && body
                .contains("appendCommandMetaPart(meta, commandStatusNode(statusLabel, stateName))"),
        "tool-call status is rendered in the same meta row position as command status"
    );
    assert!(
        body.contains("if (body && payload && commandIsRunningStatus(payload.status)) renderItemBody(body, payload)"),
        "running tool-call transcript durations refresh on the shared running-task timer"
    );
    assert!(
        !body.contains("wireToolRowToggle") && !body.contains("toggleToolOutput"),
        "the tool input/output row no longer carries a collapse toggle"
    );
    assert!(
        !body.contains("expandedToolOutputs") && !body.contains("manuallyToggledToolOutputs"),
        "tool input/output expansion state is no longer tracked"
    );
    assert!(
        body.contains("Tool data · ${label}"),
        "visible tool rows summarize their input/output with line/byte stats"
    );
    assert!(
        !body.contains("tool-io-toggle"),
        "tool-call input/output must not use a separate toggle button"
    );
    assert!(
        body.contains("startToolCall"),
        "UI renders pending tool-call starts before completion"
    );
    assert!(
        body.contains("ev.item.kind===\"tool_call\" && ev.item.tool"),
        "UI recognizes tool-call start metadata"
    );
    assert!(
        body.contains("appendToolProgress"),
        "UI appends tool progress deltas to the pending tool row"
    );
    assert!(
        body.contains("payload.output = current ? current + \"\\n\" + chunk : chunk")
            && body.contains("renderItemBody(body, payload)"),
        "UI preserves tool progress through collapsed-row re-renders"
    );
    assert!(
        body.contains("resetTerminatingToolTasks"),
        "failed tool-task interrupts roll back the stop-request state"
    );
    assert!(
        body.contains("toolVisualStateFromStatus"),
        "UI gives tool-call rows lifecycle state styling"
    );
    assert!(
        body.contains("renderActivity"),
        "UI renders generic activity items"
    );
    assert!(
        body.contains("function isImageViewActivity")
            && body.contains("p.title === \"Image viewed\"")
            && body.contains("function renderImageViewActivity")
            && body.contains("projectFileUrl(\"image\", path)")
            && body.contains("activity-image-preview"),
        "UI renders ImageView activity rows as inline image previews"
    );
    assert!(
        body.contains("function planFromActivity")
            && body.contains("completed:\"done\", inProgress:\"doing\", pending:\"todo\"")
            && body.contains("class=\"plan-step"),
        "plan steps are detected by shape and rendered as a status checklist"
    );
    assert!(
        body.contains("id=\"planCard\"")
            && body.contains("function renderPlanCard")
            && body.contains("if (isPlanItem(item)) {")
            && body.contains("if (!fromHistory) updatePlanCard(item);"),
        "plan updates drive a card above the composer instead of a transcript row"
    );
    assert!(
        body.contains("function currentPlanStepIndex")
            && body.contains("`${idx + 1}/${steps.length}`")
            && body.contains("planCardCurrent")
            && body.contains("if (!plan || idx === null) { card.hidden = true; return; }"),
        "collapsed plan card shows current/total and the current step, and hides when finished"
    );
    assert!(
        body.contains("clearPlanCard();   // the plan ends with its turn")
            && body.contains("clearPlanCard();   // a new turn starts a fresh plan"),
        "the plan card is cleared when the turn starts and ends"
    );
    assert!(
        body.contains("visibleActivityMetadata")
            && body.contains("if (isContextCompactionPayload(p)) return null"),
        "UI hides protocol-only context compaction metadata from old persisted activity rows"
    );
    assert!(
        body.contains("/linkify"),
        "UI calls the server-side path linkifier"
    );
    assert!(
        body.contains("renderCommandOutputBlock"),
        "UI renders command output through the collapsible output block"
    );
    assert!(
        body.contains("second collapse level"),
        "command output has no second collapse level; the snippet shows whenever the row is visible"
    );
    assert!(
        body.contains("inlineOutputPreview"),
        "command output is shown as a bounded latest-lines preview inline"
    );
    assert!(
        body.contains("INLINE_PREVIEW_LINES"),
        "UI has a line threshold for the inline output preview"
    );
    assert!(
        !body.contains("toggleCommandOutput") && !body.contains("wireCommandRowToggle"),
        "the command output row no longer carries a collapse toggle"
    );
    assert!(
        !body.contains("expandedCommandOutputs") && !body.contains("manuallyToggledCommandOutputs"),
        "command output expansion state is no longer tracked"
    );
    assert!(
        !body.contains("cmd-toggle"),
        "command output must not use a separate arrow-style toggle button"
    );
    assert!(
        body.contains("grid-template-columns:78px minmax(0,1fr)"),
        "IDE transcript grid cannot be widened by command output"
    );
    assert!(
        body.contains("grid-template-columns:66px minmax(0,1fr)"),
        "terminal transcript grid cannot be widened by command output"
    );
    assert!(
        body.contains("overflow-x:hidden"),
        "the transcript column suppresses horizontal overflow"
    );
    assert!(
        body.contains("overflow-wrap:anywhere"),
        "long command output can wrap instead of widening the row"
    );
    assert!(
        body.contains("`Output · ${label}`"),
        "visible command rows summarize their output with line/byte stats"
    );
    assert!(
        body.contains("if (opts.linkify) renderLinkedText(out, preview.text)"),
        "UI linkifies the command output preview only when the output block is expanded"
    );
    assert!(
        body.contains("makeOutputOverlayButton") && body.contains("openOutputOverlay"),
        "command/tool rows expose an affordance to open full output in the overlay card"
    );
    assert!(
        body.contains("LIVE_OUTPUT_TRUNCATED_MARKER") && body.contains("output-overlay-banner"),
        "the output overlay flags reconnect-truncated snapshots with a visible banner"
    );
    assert!(
        body.contains("No data available.") && body.contains("No output to show."),
        "the output overlay handles a missing item with an empty-state message"
    );
    assert!(
        body.contains("Waiting for output…") && body.contains("No output."),
        "the output overlay distinguishes a running item awaiting output from an empty one"
    );
    assert!(
        body.contains("requestAnimationFrame") && body.contains("cancelOutputOverlayRefresh"),
        "streaming overlay refreshes are coalesced to one repaint per frame"
    );
    assert!(
        body.contains("function mergeRunningOutput(prev, next)")
            && body.contains("return prev.length >= next.length ? prev : next;")
            && body.contains("output:mergeRunningOutput(existing && existing.output, info.output)"),
        "a running-command snapshot keeps the fuller delta-accumulated output instead of the \
         server's capped tail"
    );
    assert!(
        body.contains("renderMarkdown(body, p.text"),
        "UI renders completed agent/reasoning text as Markdown through the server"
    );
    assert!(
        body.contains(
            "p.kind===\"agent_message\" || p.kind===\"reasoning\" || p.kind===\"user_message\""
        ) && !body.contains("if (p.kind===\"user_message\") body.textContent"),
        "user messages render as Markdown like agent text, not as plain text"
    );
    assert!(
        body.contains("/render`, { text }"),
        "UI requests server-rendered Markdown for agent text"
    );
    assert!(
        body.contains("renderItemBodyForItem(pendingBody, item, turnId);")
            && !body.contains("state.pendingUserEl.querySelector(\".body\").textContent = p.text;"),
        "completed optimistic user messages must use the Markdown rendering path"
    );
    let append_stream = between(
        &body,
        "function appendStream(turnId, text, itemId, deltaType) {",
        "function finalizeStreamedItem(item, turnId) {",
    );
    assert!(
        append_stream
            .contains("let body = key ? state.streamElsByItemId.get(key) : state.streamEl;")
            && !append_stream.contains("state.streamElsByItemId.get(key) || state.streamEl"),
        "identified streamed items must not share the global fallback row"
    );
    assert!(
        body.contains("function wireCodeCopy")
            && body.contains("wireCodeCopy(el)")
            && body.contains(".code-block-head")
            && body.contains("code.textContent"),
        "rendered code blocks get a copy-to-clipboard button reading the raw source"
    );
    for code_rule in [".md pre code {", ".md .code-block pre code {"] {
        let rule = between(&body, code_rule, "}");
        assert!(
            rule.contains("min-width:max-content") && rule.contains("background:#0b0e13"),
            "markdown code (`{code_rule}`) carries the background and grows to the widest line, so \
             long lines that scroll horizontally stay on the dark background on mobile"
        );
    }
    assert!(
        body.contains("navigator.clipboard.writeText")
            && body.contains("document.execCommand(\"copy\")"),
        "clipboard copy has an async-API path with a legacy execCommand fallback"
    );
    assert!(
        body.contains("function attachRowCopy")
            && body.contains("btn.className = \"row-copy\"")
            && body.contains("el.dataset.copyText != null")
            && body.contains("msg.dataset.copyText = p.text"),
        "transcript rows get a copy button that prefers the raw message source"
    );
    assert!(
        body.contains("function revealRowCopy")
            && body.contains(".msg.copy-revealed")
            && body.contains("@media (hover:none)")
            && body.contains("@media (hover:hover)"),
        "the row copy button reveals on hover (pointer) and on tap (touch)"
    );
    assert!(
        body.contains("openCodeOverlay"),
        "UI opens a source overlay for linked paths"
    );
    assert!(
        body.contains("openCodeOverlay(value, targetLine)"),
        "UI passes linkified line targets into the source overlay"
    );
    assert!(
        body.contains("displayPathForProject(c.path)")
            && body.contains("makePathLink(c.path || \"\", displayPathForProject(c.path), null)"),
        "UI displays project-local file-change paths relatively while preserving source targets"
    );
    assert!(
        body.contains("return value === \"/\" ? value : value.replace")
            && body.contains("if (root === \"/\") return value.slice(1);"),
        "project-root path display keeps / as a valid root"
    );
    assert!(
        body.contains("projectFileUrl(\"highlight\""),
        "UI requests server-side syntax highlighted source"
    );
    assert!(
        body.contains("id=\"codeSourceToggle\"")
            && body.contains("function renderMarkdownCodeOverlay")
            && body.contains("function showSourceCodeOverlay")
            && body.contains("isMarkdownSourcePath(path, res.language)")
            && body.contains("`/api/projects/${projectId}/raw?path=${encodeURIComponent(path)}`")
            && body.contains("`/api/projects/${projectId}/render`, { text: source }")
            && body.contains(
                "showCodeOverlayWarning(\"Markdown preview unavailable; showing source.\")"
            )
            && body.contains(".code-overlay-warning"),
        "Markdown files in the source overlay render as Markdown with a source toggle and visible fallback"
    );
    assert!(
        body.contains("projectFileUrl(\"raw\""),
        "UI exposes raw file downloads"
    );
    assert!(
        body.contains("projectFileUrl(\"image\""),
        "UI exposes confined image preview URLs"
    );
    assert!(
        body.contains("codeDownload"),
        "UI has a download button for source files"
    );
    assert!(
        body.contains("code-line-nos"),
        "UI renders source line numbers in the code overlay"
    );
    assert!(
        body.contains(".code-table")
            && between(&body, ".code-table {", "}").contains("min-width:max-content")
            && between(&body, ".code-table {", "}").contains("background:#0b0e13"),
        "the code table grows to the widest line and carries the background, so long lines that \
         scroll horizontally stay on the dark background (a background on the scroll container \
         alone can fail to cover horizontally-scrolled content on mobile browsers)"
    );
    assert!(
        body.contains("scrollToCodeLine"),
        "UI scrolls the code overlay to a requested line"
    );
    assert!(
        body.contains("view.clientHeight / 2"),
        "UI centers requested source lines in the code overlay"
    );
    assert!(
        body.contains("live_turn_snapshot"),
        "UI replays active-turn snapshots on reconnect"
    );
    assert!(
        body.contains("itemKindsByItemId"),
        "UI tracks item kinds for streamed deltas"
    );
    assert!(
        body.contains("addItem(ev.item, ev.turn)"),
        "UI renders completed events with full item metadata"
    );
    assert!(
        body.contains("addItem(it, turn.id, true)"),
        "UI replays persisted history with full item metadata"
    );
    assert!(
        body.contains("id=\"mcpBtn\""),
        "thread header exposes an MCP status/menu button"
    );
    assert!(
        body.contains("/mcp/reload"),
        "UI can request an MCP config reload"
    );
    assert!(
        body.contains("/mcp/oauth-login"),
        "UI can start MCP OAuth login when supported"
    );
    assert!(
        body.contains("renderMcpServerCard"),
        "UI renders per-server MCP details"
    );
    assert!(
        body.contains("mcpAuthTone"),
        "UI maps MCP auth status to visual state separately from server availability"
    );
    assert!(
        body.contains("No auth"),
        "Codex unsupported auth status is not shown as a failed MCP server"
    );
    assert!(
        body.contains("Resource templates"),
        "expanded MCP details label resource template lists"
    );
    assert!(
        body.contains("data-mcp-login"),
        "MCP servers that need auth render an authenticate action"
    );
    // Self-contained: no external script/style hosts. SVG XML namespaces are allowed to use
    // `http://` identifiers; asset-bearing `src`/`href` attributes must stay same-origin.
    assert!(!body.contains(r#"src="http"#), "no external script refs");
    assert!(
        !body.contains(r#"href="http"#),
        "no external stylesheet refs"
    );
    assert!(!body.contains("cdn"), "no CDN references");
}

#[test]
fn browser_isolates_global_and_project_model_catalogs() {
    let body = app_js();

    let start_app = between(
        body,
        "async function startApp() {",
        "// The global (no-project)",
    );
    assert!(
        start_app
            .contains("state.globalModels = (await api(\"GET\",\"/api/models\")).models || []")
            && !start_app.contains("state.models = (await api(\"GET\",\"/api/models\""),
        "startup keeps the no-project catalog separate from the active project catalog"
    );

    let modal_models = between(
        body,
        "function populateModalModels() {",
        "function openProjectModal() {",
    );
    assert!(
        modal_models.contains("for (const m of state.globalModels)")
            && modal_models.contains("if (!state.globalModels.length)")
            && !modal_models.contains("state.models"),
        "the new-project modal only exposes the global model catalog"
    );

    let prepare_catalog = between(
        body,
        "function prepareProjectModelCatalog(pid) {",
        "async function loadProjectModels(pid, opts) {",
    );
    assert_order(
        prepare_catalog,
        "state.models = [];",
        "renderModelSelect();",
    );
    assert!(
        prepare_catalog.contains("if (state.modelsProject === pid) return;")
            && prepare_catalog.contains("state.modelsProject = null;")
            && prepare_catalog.contains("updateComposerControls();"),
        "switching projects invalidates and disables the previous project catalog immediately"
    );

    let load_catalog = between(
        body,
        "async function loadProjectModels(pid, opts) {",
        "// Reload re-runs discovery",
    );
    assert!(
        load_catalog.contains("pid === state.projectId")
            && load_catalog.contains("state.models = res.models;")
            && load_catalog.contains("state.modelsProject = pid;")
            && !load_catalog.contains("populateModalModels();"),
        "project responses stay scoped to the active project and cannot replace modal options"
    );

    let open_draft = between(
        body,
        "function openDraftThread(pid, defaultModel) {",
        "/* ---------- thread view + websocket ---------- */",
    );
    assert_order(
        open_draft,
        "state.currentModel = normalizeDraftModel(defaultModel);",
        "prepareProjectModelCatalog(pid);",
    );
    assert_order(
        open_draft,
        "prepareProjectModelCatalog(pid);",
        "loadProjectModels(pid);",
    );

    let open_thread = between(
        body,
        "async function openThread(pid, tid, title, opts) {",
        "function clearWsReconnectTimer()",
    );
    assert_order(
        open_thread,
        "state.currentModel = null;",
        "prepareProjectModelCatalog(pid);",
    );
    assert_order(
        open_thread,
        "prepareProjectModelCatalog(pid);",
        "loadProjectModels(pid);",
    );

    let composer_controls = between(
        body,
        "function updateComposerControls() {",
        "function setTurnActive(active) {",
    );
    assert!(
        composer_controls.contains("const modelCatalogReady = projectModelCatalogReady();")
            && composer_controls.matches("!modelCatalogReady").count() == 3,
        "model, picker, and effort controls remain disabled until the active catalog is ready"
    );

    let render_models = between(
        body,
        "function renderModelSelect() {",
        "function modelOptionLabel(m) {",
    );
    assert!(
        render_models.contains("o.value = modelKey(state.currentModel);")
            && render_models.contains("o.textContent = modelOptionLabel(state.currentModel);")
            && render_models.contains("(loading models...)"),
        "catalog invalidation preserves the current model display while hiding old options"
    );

    let thread_state = between(
        body,
        "function renderThreadState(s) {",
        "function resetTranscriptForAuthoritativeSnapshot()",
    );
    assert!(
        thread_state.contains("if (projectModelCatalogReady()) syncModelControls();")
            && thread_state.contains("else renderModelSelect();"),
        "a current model received before the catalog replaces the generic loading placeholder"
    );
}

#[tokio::test]
async fn version_meta_and_immutable_asset_caching() {
    let port = 19301;
    let tmp = tempfile::TempDir::new().unwrap();
    let store = Arc::new(giskard_persist::PersistStore::new(tmp.path().to_path_buf()));
    let state = AppState::new(store, Arc::new(NoFactory), (0..32u8).collect());
    let app = build_app(state);
    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{port}"))
        .await
        .unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    // The index is served no-cache so it always revalidates and points at the current asset URLs.
    let index_resp = reqwest::get(format!("http://127.0.0.1:{port}/"))
        .await
        .unwrap();
    assert_eq!(
        index_resp.headers().get("cache-control").unwrap(),
        "no-cache"
    );
    let index_html = index_resp.text().await.unwrap();

    // The running build is exposed via a CSP-safe meta tag and surfaced in the Settings panel.
    let version = between(
        &index_html,
        "<meta name=\"giskard-version\" content=\"",
        "\"",
    )
    .to_string();
    assert!(
        !version.is_empty() && version != "__GISKARD_VERSION__",
        "the build version is stamped into the served HTML: {version:?}"
    );
    assert!(
        index_html.contains("id=\"giskardVersion\""),
        "the Settings panel has a version row"
    );

    // Asset URLs are content-hashed and served immutably, so a browser cache can't shadow an upgrade.
    let js_url = between(&index_html, "<script src=\"", "\"").to_string();
    let js_resp = reqwest::get(format!("http://127.0.0.1:{port}{js_url}"))
        .await
        .unwrap();
    assert!(
        js_resp
            .headers()
            .get("cache-control")
            .unwrap()
            .to_str()
            .unwrap()
            .contains("immutable"),
        "hashed assets are cached immutably"
    );
    assert!(
        js_resp
            .text()
            .await
            .unwrap()
            .contains("function initVersionLabel()"),
        "the hashed URL serves the app script"
    );

    // The old unhashed path no longer serves the asset — nothing can pin a stale /app.js.
    let stale = reqwest::get(format!("http://127.0.0.1:{port}/app.js"))
        .await
        .unwrap();
    assert!(
        !stale.status().is_success(),
        "the unhashed /app.js must not serve content (got {})",
        stale.status()
    );
}

#[test]
fn browser_resubscribe_replaces_transient_transcript_state() {
    let body = app_js();
    let onopen = between(body, "ws.onopen = () => {", "  };\n  ws.onmessage");
    assert_order(
        onopen,
        "state.awaitingThreadResync = true;",
        "send({ type:\"subscribe\", thread_id: state.threadId });",
    );

    let render_thread_state = between(
        body,
        "function renderThreadState(s) {",
        "function resetTranscriptForAuthoritativeSnapshot() {",
    );
    assert!(render_thread_state.contains(
        "const shouldResetTranscript = state.awaitingInitialThreadState || state.awaitingThreadResync"
    ));
    assert!(render_thread_state.contains("state.awaitingInitialThreadState = false;"));
    assert!(render_thread_state.contains("state.awaitingThreadResync = false;"));
    assert!(render_thread_state.contains(
        "if (shouldResetTranscript) {\n    resetTranscriptForAuthoritativeSnapshot();\n  }"
    ));
    assert!(
        !render_thread_state.contains("$(\"transcript\").innerHTML=\"\""),
        "metadata-only ThreadState updates must not clear the transcript directly"
    );
    assert!(
        !render_thread_state.contains("resetRenderState();"),
        "metadata-only ThreadState updates must not reset rendered item de-dupe state directly"
    );

    let reset = between(
        body,
        "function resetTranscriptForAuthoritativeSnapshot() {",
        "const MODE_LABELS",
    );
    for expected in [
        "$(\"transcript\").innerHTML=\"\";",
        "state.pendingUserEl = null;",
        "state.pendingUserText = null;",
        "state.pendingOlder = false;",
        "state.loadingHistory = false;",
        "state.oldestTurnId = null;",
        "state.hasMoreHistory = false;",
        "state.interruptPending = false;",
        "state.compactPending = false;",
        "resetRenderState();",
        "setTurnActive(false);",
    ] {
        assert!(
            reset.contains(expected),
            "authoritative resync reset is missing `{expected}`"
        );
    }
    assert_order(
        reset,
        "$(\"transcript\").innerHTML=\"\";",
        "resetRenderState();",
    );
    assert_order(reset, "resetRenderState();", "setTurnActive(false);");
}

#[test]
fn browser_marks_turn_active_when_send_is_accepted() {
    let body = app_js();
    let send_input = between(
        body,
        "function sendInput() {",
        "$(\"sendBtn\").onclick = sendInput;",
    );
    assert_order(
        send_input,
        "if (!send({ type:\"send_input\", thread_id: state.threadId, text }))",
        "setTurnActive(true);",
    );
    assert_order(
        send_input,
        "setTurnActive(true);",
        "state.pendingUserEl = msgEl;",
    );

    let error_case = between(body, "case \"error\":", "      break;\n  }");
    assert!(
        error_case.contains(
            "if (msg.action===\"send_input\") {\n        setTurnActive(msg.code === \"thread_turn_active\");\n      }"
        ),
        "send_input errors must reconcile optimistic active-turn state"
    );
}

#[test]
fn browser_scopes_unsent_composer_text_to_thread() {
    let body = app_js();

    let open_draft = between(
        body,
        "function openDraftThread(pid, defaultModel) {",
        "/* ---------- thread view + websocket ---------- */",
    );
    assert_order(open_draft, "saveComposerDraft();", "state.projectId = pid;");
    assert_order(
        open_draft,
        "state.draftThread = { projectId:pid",
        "restoreComposerDraft();",
    );

    let open_thread = between(
        body,
        "async function openThread(pid, tid, title, opts) {",
        "function clearWsReconnectTimer()",
    );
    assert_order(
        open_thread,
        "saveComposerDraft();",
        "state.projectId = pid; state.threadId = tid;",
    );
    assert_order(
        open_thread,
        "state.draftThread = null;",
        "restoreComposerDraft();",
    );

    let send_input = between(
        body,
        "function sendInput() {",
        "$(\"sendBtn\").onclick = sendInput;",
    );
    assert_order(
        send_input,
        "const draftKey = composerDraftKey();",
        "clearComposerDraft(draftKey);",
    );

    let start_draft = between(
        body,
        "async function startDraftThread(text) {",
        "function interruptTurn()",
    );
    assert_order(
        start_draft,
        "const draftKey = composerDraftKey();",
        "clearComposerDraft(draftKey);",
    );
    assert_order(
        start_draft,
        "clearComposerDraft(draftKey);",
        "state.draftThread = null;",
    );
}

#[test]
fn browser_keeps_appended_transcript_rows_anchored_to_bottom() {
    let body = app_js();
    assert!(
        body.contains("const TRANSCRIPT_BOTTOM_STICKY_PX = 96;"),
        "UI defines a bottom-stickiness threshold for transcript rows"
    );
    assert!(
        body.contains("function transcriptShouldStickToBottom()")
            && body.contains("function keepTranscriptAtBottom(shouldStick)")
            && body.contains("requestAnimationFrame(scrollTranscriptToBottom)")
            && body.contains("function keepTranscriptRowAnchored(el)"),
        "UI has shared sticky-scroll helpers for expanded transcript rows"
    );

    let append = between(
        body,
        "function appendBubble(cls, role) {",
        "function bubble(cls, role)",
    );
    assert_order(
        append,
        "const followBottom = transcriptShouldStickToBottom();",
        "t.append(el);",
    );
    assert_order(
        append,
        "t.append(el);",
        "keepTranscriptAtBottom(followBottom);",
    );
    assert!(
        append.contains("if (followBottom) el.dataset.followBottom = \"true\";"),
        "new rows remember whether they should stay anchored after content expands"
    );

    let markdown = between(
        body,
        "function renderMarkdown(el, text) {",
        "// Wire the server-emitted",
    );
    assert!(
        markdown.contains("keepTranscriptRowAnchored(el);"),
        "async markdown rendering keeps newly appended rows visible"
    );
    assert!(
        markdown.contains("el._markdownRenderKey = cacheKey;")
            && markdown.contains("if (el._markdownRenderKey !== cacheKey) return;")
            && !markdown.contains("!el.isConnected"),
        "detached history rows must render while stale responses cannot overwrite newer text"
    );
    assert!(
        markdown.contains("projectId !== state.projectId || threadId !== state.threadId"),
        "Markdown responses must remain scoped to the active project and thread"
    );
    let linkify = between(
        body,
        "function renderLinkedText(el, text) {",
        "function applyLinkedText",
    );
    assert!(
        linkify.contains("keepTranscriptRowAnchored(el);"),
        "async path linkification keeps newly appended rows visible"
    );
}

#[test]
fn browser_applies_cached_markdown_to_detached_history_rows() {
    let body = app_js();
    let history = between(
        body,
        "function renderHistoryPage(msg) {",
        "function renderHistoryDelta(msg) {",
    );
    assert_order(
        history,
        "const container = document.createElement(\"div\");",
        "for (const turn of turns) renderPersistedTurn(turn);",
    );
    assert_order(
        history,
        "for (const turn of turns) renderPersistedTurn(turn);",
        "while (container.firstChild) t.insertBefore(container.firstChild, anchor);",
    );

    let markdown = between(
        body,
        "function renderMarkdown(el, text) {",
        "// Wire the server-emitted",
    );
    assert!(
        markdown.contains("if (state.markdownCache.has(cacheKey)) {")
            && markdown.contains("apply(state.markdownCache.get(cacheKey));")
            && !markdown.contains("!el.isConnected"),
        "a synchronous Markdown cache hit must apply before detached history is inserted"
    );
    assert_order(
        markdown,
        "el._markdownRenderKey = cacheKey;",
        "if (!text.trim() || !projectId) return;",
    );
}

#[test]
fn browser_stamps_transcript_rows_with_their_turn_id() {
    let body = app_js();

    // Every top-level row is stamped at the single creation site with the turn it belongs to.
    let append = between(
        body,
        "function appendBubble(cls, role) {",
        "function bubble(cls, role)",
    );
    assert!(
        append.contains("el.dataset.turn = state.currentRenderTurnId || \"pending\";"),
        "appendBubble stamps each row with the current turn id (or 'pending' before turn_started)"
    );

    // Persisted turns stamp their rows with the turn's own id, saved/restored around rendering.
    let persisted = between(
        body,
        "function renderPersistedTurn(turn) {",
        "// Load older history",
    );
    assert_order(
        persisted,
        "const prevRenderTurnId = state.currentRenderTurnId;",
        "state.currentRenderTurnId = turn.id;",
    );
    assert_order(
        persisted,
        "for (const it of items) addItem(it, turn.id, true);",
        "state.currentRenderTurnId = prevRenderTurnId;",
    );

    // A starting live turn adopts its id and upgrades the optimistic "pending" rows to it.
    let started = between(body, "case \"turn_started\":", "case \"item_started\":");
    assert!(started.contains("state.currentRenderTurnId = ev.turn;"));
    assert!(
        started.contains("document.querySelectorAll('.msg[data-turn=\"pending\"]')"),
        "turn_started re-stamps optimistic pending rows with the real turn id"
    );

    // A completing turn advances the high-water cursor and stops stamping rows to it.
    let completed = between(
        body,
        "case \"turn_completed\":",
        "case \"approval_requested\":",
    );
    assert!(completed.contains("if (ev.turn) state.newestPersistedTurnId = ev.turn;"));
    assert!(completed.contains("state.currentRenderTurnId = null;"));

    // The initial history page seeds the high-water cursor; older pages must not lower it.
    let history = between(
        body,
        "function renderHistoryPage(msg) {",
        "function maybeAutoFillHistory",
    );
    assert!(history.contains(
        "if (!older && turns.length) state.newestPersistedTurnId = turns[turns.length - 1].id;"
    ));
}

#[test]
fn browser_tracks_rendered_items_by_scoped_dom_identity() {
    let body = app_js();

    let scoped_item = between(
        body,
        "function scopedItemKey(turnId, itemId) {",
        "function scopedHarnessKey(turnId, harnessItemId) {",
    );
    assert!(scoped_item.contains("return turn && item ? `${turn}:${item}` : \"\";"));
    let scoped_harness = between(
        body,
        "function scopedHarnessKey(turnId, harnessItemId) {",
        "function identityTokens(value) {",
    );
    assert!(scoped_harness.contains("return turn && harness ? `${turn}:${harness}` : \"\";"));

    let register = between(
        body,
        "function registerRenderedItemBody(body, item, turnId) {",
        "function isRenderedItem(item, turnId) {",
    );
    for expected in [
        "if (turnId) row.dataset.turn = idKey(turnId);",
        "row.dataset.item = addIdentityToken(row.dataset.item, item.id);",
        "row.dataset.harnessItem = addIdentityToken(row.dataset.harnessItem, item.harness_item_id);",
        "if (keys.itemKey) state.renderedItemBodyByKey.set(keys.itemKey, body);",
        "if (keys.harnessKey) state.renderedItemBodyByKey.set(keys.harnessKey, body);",
    ] {
        assert!(
            register.contains(expected),
            "rendered item body registration is missing `{expected}`"
        );
    }

    let add_item = between(
        body,
        "function addItem(item, turnId, fromHistory) {",
        "function resetRenderState()",
    );
    for expected in [
        "const visible = hasVisiblePayload(p);",
        "const existing = renderedItemBody(item, turnId);",
        "const harnessExisting = renderedHarnessItemBody(item, turnId);",
        "if (existing && harnessExisting && existing!==harnessExisting) {",
        "Giskard item identity invariant violated; refusing conflicting item upsert.",
        "if (!existing && harnessExisting) {",
        "Giskard item identity invariant violated; refusing harness-only item upsert.",
        "if (!visible) {\n        markRenderedItem(item, turnId);\n        return;\n      }",
        "registerRenderedItemBody(existing, item, turnId);",
        "registerRenderedItemBody(pendingBody, item, turnId);",
        "registerRenderedItemBody(body, item, turnId);",
        "registerRenderedItemBody(mergedRow.querySelector(\".body\"), item, turnId);",
    ] {
        assert!(
            add_item.contains(expected),
            "addItem must register scoped DOM identity: `{expected}`"
        );
    }
    assert_order(
        add_item,
        "registerRenderedItemBody(existing, item, turnId);",
        "renderItemBodyForItem(existing, item, turnId);",
    );
    assert!(
        add_item.contains("now-visible upsert fall through")
            && add_item.contains("if (state.pendingUserEl && !state.pendingUserEl.isConnected)"),
        "visible upserts without a body render normally, and detached pending rows are ignored"
    );
    assert!(
        !add_item.contains("state.renderedItemBodyByKey.set(key, body)"),
        "new and upserted rows must not rely on item-id-only body lookup"
    );

    assert!(!body.contains("function moveTaskStateKey("));
    assert!(!body.contains("function moveTaskGroupItem("));
    assert!(!body.contains("function taskKeyForBody("));

    let merge = between(
        body,
        "function fileChangeContributionKey(item, turnId) {",
        "function renderFileChange(body, p)",
    );
    for expected in [
        "return keys.itemKey;",
        "if (!row._fileChangePayloadsByItemKey) row._fileChangePayloadsByItemKey = new Map();",
        "row._fileChangePayloadsByItemKey.set(key, normalizedFileChangePayload(p));",
        "const contributions = Array.from(row._fileChangePayloadsByItemKey.values());",
        "if (!fileChangeContributionKey(item, turnId)) return null;",
        "prev.dataset.turn!==idKey(turnId)",
        "renderFileChangeContribution(body, p, item, turnId);",
        "return prev;",
    ] {
        assert!(
            merge.contains(expected),
            "file-change upsert must preserve merged item contributions: `{expected}`"
        );
    }
    assert!(
        add_item.contains("mergeFileChangeWithPrevious(p, item, turnId)"),
        "merged file changes must retain the incoming scoped item identity"
    );

    let finalize = between(
        body,
        "function finalizeStreamedItem(item, turnId) {",
        "function addItem(item, turnId, fromHistory)",
    );
    assert!(
        finalize.contains("renderItemBodyForItem(body, item, turnId);"),
        "stream completion must use contribution-aware file-change rendering"
    );
}

#[test]
fn browser_incremental_resync_reconciles_in_flight_turn() {
    let body = app_js();

    // Reconnect asks for a delta when it has a cursor, else a full resync.
    let onopen = between(body, "ws.onopen = () => {", "ws.onmessage = (m) => {");
    assert!(onopen.contains(
        "send({ type:\"subscribe\", thread_id: state.threadId, since: state.newestPersistedTurnId });"
    ));
    assert!(onopen.contains("state.awaitingIncrementalResync = true;"));

    // A delta keeps the transcript: repaint the in-flight turn, append new turns, advance the cursor.
    let delta = between(
        body,
        "function renderHistoryDelta(msg) {",
        "function reconcileInFlightTurn()",
    );
    assert_order(
        delta,
        "reconcileInFlightTurn();",
        "for (const turn of turns) renderPersistedTurn(turn);",
    );
    assert!(delta.contains("state.newestPersistedTurnId = turns[turns.length - 1].id;"));
    assert!(delta.contains("t.appendChild(container.firstChild)"));

    // Reconcile removes only the in-flight turn's rows (by data-turn) and the optimistic pending rows.
    let reconcile = between(
        body,
        "function reconcileInFlightTurn() {",
        "function removeTurnRows(turnId)",
    );
    assert!(reconcile.contains("removeTurnRows(\"pending\");"));
    assert!(reconcile.contains("state.activeTurn && state.currentRenderTurnId != null"));
    assert_order(
        reconcile,
        "if (liveId) removeTurnRows(liveId);",
        "rebuildRenderTrackingFromDom();",
    );
    assert!(reconcile.contains("state.pendingUserEl = null;"));
    assert!(reconcile.contains("state.pendingUserText = null;"));
    assert!(reconcile.contains("setTurnActive(false);"));
    let remove = between(
        body,
        "function removeTurnRows(turnId) {",
        "function maybeAutoFillHistory",
    );
    assert!(remove.contains("if (el.dataset.turn === turnId) el.remove();"));

    // Rebuild uses the surviving DOM as source of truth so removed live rows cannot leave stale
    // de-dupe sets or body maps pointing at detached elements.
    assert!(body.contains("rebuildRenderTrackingFromDom();"));
    let rebuild = between(
        body,
        "function rebuildRenderTrackingFromDom() {",
        "function removeTurnRows(turnId)",
    );
    for expected in [
        "const renderedBodies = new Map();",
        "for (const row of t.querySelectorAll(\".msg\"))",
        "const turn = row.dataset.turn || \"\";",
        "for (const itemId of identityTokens(row.dataset.item))",
        "const key = scopedItemKey(turn, itemId);",
        "for (const harnessItemId of identityTokens(row.dataset.harnessItem))",
        "const key = scopedHarnessKey(turn, harnessItemId);",
        "state.renderedItemIds = renderedItems;",
        "state.renderedHarnessItemIds = renderedHarnessItems;",
        "state.renderedItemBodyByKey = renderedBodies;",
        "state.streamElsByItemId = new Map();",
        "state.itemKindsByItemId = new Map();",
    ] {
        assert!(
            rebuild.contains(expected),
            "incremental resync rebuild is missing `{expected}`"
        );
    }
    for expected in [
        "state.commandPayloadsByItemId",
        "state.endedCommandsByItemId",
        "state.runningCommands",
        "state.toolPayloadsByItemId",
        "state.taskGroupsByItemId",
        "pruneKeySet(state.commandStopRequestedByItemId, liveTaskIds);",
        "state.pendingApprovals.delete(id)",
        "state.pendingServerRequests.delete(id)",
        "state.renderedApprovalStateKeys = approvalKeys;",
    ] {
        assert!(
            rebuild.contains(expected),
            "incremental resync cleanup is missing `{expected}`"
        );
    }

    // Delta is dispatched, and a full page arriving mid-resync is treated as a stale-cursor rebuild.
    assert!(body.contains("case \"history_delta\": renderHistoryDelta(msg); break;"));
    let page = between(
        body,
        "function renderHistoryPage(msg) {",
        "const older = state.pendingOlder;",
    );
    assert_order(
        page,
        "if (state.awaitingIncrementalResync) {",
        "resetTranscriptForAuthoritativeSnapshot();",
    );

    // Running-tasks is the last resync frame; it restores bottom-stick if the user was pinned there.
    assert!(body.contains(
        "if (state.resyncStickBottom) { state.resyncStickBottom = false; keepTranscriptAtBottom(true); }"
    ));
}

#[test]
fn browser_scopes_running_command_completion_identity_to_turn() {
    let body = app_js();

    let command_from_item = between(
        body,
        "function commandFromItem(item, p, turnId, key, existing) {",
        "function commandBodyFor(id) {",
    );
    assert!(command_from_item.contains("id:existing ? existing.id : key,"));
    assert!(command_from_item.contains("turnId:existing ? existing.turnId : idKey(turnId),"));
    assert!(
        !command_from_item.contains("idKey(item && item.id)"),
        "completed commands must not fall back to a bare item id"
    );

    let finish = between(
        body,
        "function finishRunningCommand(item, turnId) {",
        "function renderRunningCommandSnapshot(commands) {",
    );
    for expected in [
        "const key = scopedItemKey(turnId, item && item.id);",
        "if (!key) return;",
        "commandFromItem(item, p, turnId, key, state.runningCommands.get(key))",
    ] {
        assert!(
            finish.contains(expected),
            "finishRunningCommand must use scoped command identity: `{expected}`"
        );
    }

    let finalize = between(
        body,
        "function finalizeStreamedItem(item, turnId) {",
        "function addItem(item, turnId, fromHistory)",
    );
    assert!(finalize.contains("finishRunningCommand(item, turnId);"));
    let add_item = between(
        body,
        "function addItem(item, turnId, fromHistory) {",
        "function resetRenderState()",
    );
    assert!(add_item.contains("finishRunningCommand(item, turnId);"));
    assert!(
        !add_item.contains("finishRunningCommand(item);"),
        "addItem must pass the owning turn into command completion"
    );

    let snapshot = between(
        body,
        "function renderRunningCommandSnapshot(commands) {",
        "function renderEndedCommandBody(body, cmd, status, opts)",
    );
    for expected in [
        "const key = scopedItemKey(info.turn_id, info.item_id);",
        "const snapshotItem = { id:info.item_id, harness_item_id:info.harness_item_id || \"\" };",
        "const existing = state.runningCommands.get(key);",
        "let body = commandBodyFor(key);",
        "registerRenderedItemBody(toolBody, snapshotItem, info.turn_id);",
        "registerRenderedItemBody(body, snapshotItem, info.turn_id);",
    ] {
        assert!(
            snapshot.contains(expected),
            "running task snapshots must keep scoped DOM identity: `{expected}`"
        );
    }

    let start = between(
        body,
        "function startRunningCommand(item, turnId) {",
        "function commandFromParts(parts) {",
    );
    assert!(start.contains("const key = scopedItemKey(turnId, item.id);"));
    assert!(start.contains("const existing = state.runningCommands.get(key);"));
    assert!(start.contains("let body = commandBodyFor(key);"));
    assert!(start.contains("registerRenderedItemBody(body, item, turnId);"));
}

#[test]
fn browser_websocket_lifecycle_errors_are_not_toasted_directly() {
    let body = app_js();

    let onerror = between(body, "ws.onerror = () => {", "  };\n  ws.onclose");
    assert!(onerror.contains("ws._giskardHadError = true;"));
    assert!(onerror.contains("recordWsProblem(\"WebSocket connection failed. Reconnecting...\")"));
    assert!(
        !onerror.contains("surfaceWsProblem") && !onerror.contains("notice("),
        "WebSocket.onerror must update status only; close decides whether a notice is actionable"
    );

    let onclose = between(body, "ws.onclose = (ev) => {", "function setWsStatus");
    for expected in [
        "const backgroundedAt = Number(ws._giskardBackgroundedAt) || 0;",
        "const resumedAt = Number(ws._giskardResumedAt) || 0;",
        "Date.now() - resumedAt < WS_BACKGROUND_CLOSE_GRACE_MS",
        "const backgrounded = recentlyBackgrounded || document.visibilityState === \"hidden\";",
        "const abnormalForegroundClose = ev.code === 1006 || ev.code === 1008 || ev.code === 1011;",
        "if (!backgrounded && (ws._giskardHadError || abnormalForegroundClose))",
        "surfaceWsProblem(message, \"warning\");",
        "scheduleWsReconnect(message);",
    ] {
        assert!(
            onclose.contains(expected),
            "WebSocket close handler is missing `{expected}`"
        );
    }
    assert_order(onclose, "const backgrounded =", "if (!backgrounded");
    assert_order(
        onclose,
        "surfaceWsProblem(message, \"warning\");",
        "scheduleWsReconnect(message);",
    );

    let visibility = between(
        body,
        "document.addEventListener(\"visibilitychange\", () => {",
        "});\nwindow.addEventListener(\"online\"",
    );
    assert!(visibility.contains("if (state.ws) state.ws._giskardBackgroundedAt = Date.now();"));
    assert!(visibility.contains("state.ws._giskardResumedAt = Date.now();"));
    assert_order(
        visibility,
        "document.visibilityState === \"hidden\"",
        "state.ws._giskardResumedAt = Date.now();",
    );
    assert_order(
        visibility,
        "state.ws._giskardResumedAt = Date.now();",
        "reconnectIfNeeded(\"tab visible\");",
    );
}

#[test]
fn browser_backgrounded_socket_recovery_restores_foreground_errors() {
    let body = app_js();

    let recover = between(
        body,
        "function markWsForegroundRecovered(ws) {",
        "function scheduleWsReconnect",
    );
    assert!(recover.contains("if (!ws || document.visibilityState !== \"visible\") return;"));
    assert!(recover.contains("ws._giskardBackgroundedAt = 0;"));
    assert!(recover.contains("ws._giskardResumedAt = 0;"));

    let onopen = between(body, "ws.onopen = () => {", "  };\n  ws.onmessage");
    assert_order(
        onopen,
        "setWsStatus(\"open\", \"Connected to agent.\");",
        "markWsForegroundRecovered(ws);",
    );
    assert_order(
        onopen,
        "markWsForegroundRecovered(ws);",
        "state.awaitingThreadResync = true;",
    );

    let onmessage = between(body, "ws.onmessage = (m) => {", "  };\n  ws.onerror");
    assert_order(
        onmessage,
        "markWsForegroundRecovered(ws);",
        "handleServer(JSON.parse(m.data));",
    );
}

#[test]
fn browser_diagnostics_panel_is_exposed_from_settings() {
    let body = app_js();

    assert!(body.contains("browserDiagnostics:[]"));
    assert!(body.contains("BROWSER_DIAGNOSTIC_VERSION"));
    assert!(body.contains("browser-diagnostics-v1"));
    assert!(body.contains("BROWSER_DIAGNOSTIC_LIMIT"));
    assert!(body.contains("function browserDiagnosticsSnapshot()"));
    assert!(body.contains("function notificationDebugSnapshot()"));
    assert!(body.contains("last_approval_decision: lastNotificationDiagnostic"));
    assert!(body.contains("recent_approval_decisions: recentNotificationDiagnostics"));
    assert!(body.contains("function isApprovalNotificationDecision(entry)"));
    assert!(body.contains("detail.kind === \"approval\""));
    assert!(body.contains("function lastNotificationDiagnostic(predicate)"));
    assert!(body.contains("function recentNotificationDiagnostics(predicate, limit)"));
    assert!(body.contains("function recordBrowserDiagnostic(category, reason, detail)"));
    assert!(body.contains("category: category || \"browser\""));
    assert!(body.contains("function recordNotificationDiagnostic(reason, detail)"));
    assert!(body.contains("recordBrowserDiagnostic(\"notification\", reason, detail);"));
    assert!(body.contains("function renderBrowserDiagnosticsPanel(snapshot, reveal)"));
    assert!(body.contains("log.textContent = lines.join(\"\\n\");"));
    assert!(body.contains("`lastApproval: ${lastApproval ? lastApproval.reason : \"none\"}`"));
    assert!(body.contains("`dedupMs: ${snapshot.dedup_window_ms}`"));
    assert!(body.contains("recentApprovals:"));
    assert!(body.contains("recentBrowserEvents:"));
    assert!(body.contains("visible=${entry.visibility} focused=${entry.focused}"));
    assert!(body.contains("`approvalSource: ${approvalDetail.source || \"none\"}`"));
    assert!(body.contains("window.giskardBrowserDiagnostics = browserDiagnosticsSnapshot;"));
    assert!(body.contains("window.giskardNotificationDebug = notificationDebugSnapshot;"));
    assert!(body.contains("function showBrowserDiagnostics()"));
    assert!(body.contains("function copyBrowserDiagnostics()"));
    assert!(body.contains("await navigator.clipboard.writeText(text);"));
    assert!(body.contains("function clearBrowserDiagnostics()"));
    assert!(body.contains("state.browserDiagnostics = [];"));
    assert!(body.contains("ws_status_changed"));
    assert!(body.contains("recordBrowserDiagnostic(\"websocket\", \"ws_status_changed\""));

    let index = include_str!("../static/index.html");
    assert!(index.contains("id=\"browserDiagnosticsBtn\""));
    assert!(index.contains("id=\"browserDiagnosticsPanel\""));
    assert!(index.contains("id=\"browserDiagnosticsLog\""));
    assert!(index.contains("id=\"copyBrowserDiagnosticsBtn\""));
    assert!(index.contains("id=\"clearBrowserDiagnosticsBtn\""));
    assert!(index.contains("id=\"testNotificationBtn\""));
    assert!(index.contains("class=\"browser-diagnostics\""));
    assert!(index.contains("Browser diagnostics"));
    assert!(index.contains("Test notification"));
    assert!(!index.contains("notificationDebugBtn"));
    assert!(!index.contains("notificationDebugPanel"));

    let css = include_str!("../static/app.css");
    assert!(css.contains(".browser-diagnostics"));
    assert!(css.contains(".browser-diagnostics-actions"));
}

#[test]
fn sidebar_activity_notifications_target_approval_rows() {
    let body = app_js();

    assert!(body.contains("threadActivity:new Map()"));
    assert!(body.contains("pendingApprovalFocus:null"));
    assert!(body.contains("notifiedApprovals:new Map()"));
    assert!(body.contains("approvalNotifications:new Map()"));
    assert!(body.contains("lastNotificationPromptNoticeAt:0"));
    assert!(body.contains("NOTIFICATION_DEDUP_MS"));
    assert!(body.contains("function handleThreadActivity(msg)"));
    assert!(body.contains("if (msg && msg.type === \"thread_activity\")"));
    assert!(body.contains("function renderThreadActivityIndicator(tid)"));
    assert!(body.contains("activity.kind === \"approval_requested\""));
    assert!(body.contains(
        "if (activity.kind === \"approval_requested\") maybeNotifyApproval(tid, activity);"
    ));
    assert!(body.contains("server_request_id: msg.server_request_id || null"));
    assert!(body.contains("activity.active_turn && activity.kind !== \"approval_requested\""));
    assert!(body.contains("else if (activity.active_turn) status.textContent = \"o\""));
    assert!(!body.contains("else if (activity.active_turn) status.textContent = \">\""));
    assert!(body.contains("function setActiveThreadActivity(kind, activeTurn, summary, extra)"));
    assert!(body.contains("source: \"active_thread_event\""));
    assert!(body.contains("setActiveThreadActivity(\"progress\", true, \"Turn running\")"));
    assert!(
        body.contains("setActiveThreadActivity(\"turn_completed\", false, \"Turn completed\")")
    );
    assert!(body.contains("ACTIVE_THREAD_COMPLETED_MARK_MS = 2500"));
    assert!(body.contains("function clearActiveThreadActivityLater(tid, kind)"));
    assert!(body.contains("clearApprovalThreadActivity(tid, id)"));
    assert!(body.contains("clearServerRequestThreadActivity(tid, id)"));
    assert!(!body.contains("!!activity && !current &&"));
    assert!(body.contains("function initNotificationSettings()"));
    assert!(body.contains("function notificationPermissionButtons()"));
    assert!(body.contains("document.querySelectorAll(\".notify-permission-btn\")"));
    assert!(body.contains("async function requestNotificationPermission()"));
    assert!(body.contains("await Notification.requestPermission()"));
    assert!(body.contains("Browser notifications require HTTPS or localhost."));
    assert!(body.contains("function refreshNotificationButton()"));
    assert!(body.contains("label = \"Approval notifications enabled\""));
    assert!(body.contains("label = \"Notifications blocked by browser\""));
    assert!(body.contains("label = \"Notifications require HTTPS or localhost\""));
    assert!(body.contains("function maybeNoticeNotificationPermission()"));
    assert!(body.contains("Notification.permission !== \"default\""));
    assert!(body.contains("Enable approval notifications from the sidebar alert button."));
    assert!(body.contains(
        "const notificationKey = approvalNotificationKey(tid, activity && activity.approval_id);"
    ));
    assert!(body.contains("pruneNotificationDedup(now);"));
    assert!(body.contains("const notifiedAt = state.notifiedApprovals.get(notificationKey);"));
    assert!(body.contains("now - notifiedAt < NOTIFICATION_DEDUP_MS"));
    assert!(body.contains("state.notifiedApprovals.set(notificationKey, now);"));
    assert!(body.contains("trackApprovalNotification(notificationKey, result.notification);"));
    assert!(body.contains("function pruneNotificationDedup(now)"));
    assert!(body.contains("function approvalNotificationKey(tid, approvalId)"));
    assert!(body.contains("const approvalKey = approvalId === undefined || approvalId === null"));
    assert!(body.contains("String(approvalId)"));
    assert!(body.contains("if (!threadKey || !approvalKey) return \"\";"));
    assert!(body.contains("return `${threadKey}:${approvalKey}`;"));
    assert!(body.contains("function trackApprovalNotification(key, notification)"));
    assert!(body.contains("notifications = new Set();"));
    assert!(body.contains("notifications.add(notification);"));
    assert!(body.contains("function untrackApprovalNotification(key, notification)"));
    assert!(body.contains("notifications.delete(notification);"));
    assert!(
        body.contains("if (notifications.size === 0) state.approvalNotifications.delete(key);")
    );
    assert!(body.contains("function closeApprovalNotification(tid, approvalId)"));
    assert!(body.contains("const notifications = state.approvalNotifications.get(key);"));
    assert!(body.contains("state.approvalNotifications.delete(key);"));
    assert!(body.contains("for (const notification of notifications)"));
    assert!(body.contains("approval_notification_closed"));
    assert!(!body.contains("closeAllApprovalNotifications"));
    assert!(
        body.contains(
            "const notificationTag = approvalNotificationTag(tid, activity.approval_id);"
        )
    );
    assert!(body.contains("return `giskard-approval-${tid}-${approvalId}`;"));
    assert!(body.contains("tag: notificationTag"));
    assert!(body.contains("function notifyApprovalRequest(request, tid, opts)"));
    assert!(body.contains("function handleIncomingApprovalRequest(request, tid, opts)"));
    assert!(body.contains(
        "if (opts.notify !== false) notifyApprovalRequest(request, tid, { source: opts.source });"
    ));
    assert!(body.contains("source: \"agent_event_approval_requested\""));
    assert!(body.contains("source: \"server_message_approval_request\""));
    assert!(body.contains("source: \"live_turn_snapshot_pending_approval\""));
    assert!(body.contains("source: \"thread_activity\""));
    assert!(body.contains("approval_notify_received"));
    assert!(body.contains("approval_notify_suppressed_visible_current_thread"));
    assert!(body.contains("approval_notify_constructor_failed"));
    assert!(body.contains("approval_notify_created"));
    assert!(body.contains("async function showAppNotification(title, options, diagnosticDetail)"));
    // Prefer the service worker (works on Android); fall back to the Notification constructor.
    assert!(body.contains("const reg = await notificationRegistration();"));
    assert!(body.contains("await reg.showNotification(title, options);"));
    assert!(body.contains("const notification = new Notification(title, options);"));
    assert!(body.contains("browser_notification_created"));
    assert!(body.contains("browser_notification_show"));
    assert!(body.contains("browser_notification_error"));
    assert!(body.contains("browser_notification_close"));
    assert!(body.contains("notification.onshow = () => recordNotificationDiagnostic"));
    assert!(body.contains("notification.onerror = () => recordNotificationDiagnostic"));
    assert!(body.contains("notification.onclose = () => {"));
    assert!(body.contains("diagnosticDetail.kind === \"approval\""));
    assert!(
        body.contains(
            "approvalNotificationKey(diagnosticDetail.tid, diagnosticDetail.approval_id),"
        )
    );
    assert!(body.contains("untrackApprovalNotification("));
    assert!(body.contains(
        "recordNotificationDiagnostic(\"browser_notification_close\", diagnosticDetail);"
    ));
    assert!(body.contains("function isApprovalNotificationDecision(entry)"));
    assert!(body.contains("detail.kind === \"approval\""));
    assert!(body.contains("last_approval_decision: lastNotificationDiagnostic"));
    assert!(body.contains("recent_approval_decisions: recentNotificationDiagnostics"));
    assert!(body.contains("`lastApproval: ${lastApproval ? lastApproval.reason : \"none\"}`"));
    assert!(body.contains("function sendTestNotification()"));
    assert!(body.contains("Giskard test notification"));
    assert!(body.contains("test_notify_created"));
    assert!(body.contains("test_notify_constructor_failed"));
    assert!(body.contains("requireInteraction: true"));
    assert!(!body.contains("approval_notify_skipped_invalid_activity"));
    assert!(body.contains("approval_notify_skipped_invalid_call"));
    assert!(body.contains("`lastApproval: ${lastApproval ? lastApproval.reason : \"none\"}`"));
    assert!(!body.contains("notifyApprovalRequest(ev.request"));
    assert!(!body.contains("notifyApprovalRequest(msg.request"));
    assert!(!body.contains("notifyApprovalRequest(snap.pending_approval"));
    assert!(body.contains("await showAppNotification(\"Giskard: approval needed\""));
    assert!(body.contains("const focused = document.hasFocus ? document.hasFocus() : true;"));
    assert!(body.contains(
        "if (document.visibilityState === \"visible\" && focused && String(tid) === String(state.threadId))"
    ));
    // Desktop notifications wire their click; service-worker clicks arrive via the worker postMessage.
    assert!(body.contains(
        "result.notification.onclick = () => handleNotificationClick({ threadId: tid, approvalId: activity.approval_id });"
    ));
    assert!(body.contains("closeApprovalNotification(data.threadId, data.approvalId);"));
    assert!(body.contains("openThread(meta.pid, tid, meta.title, { focusApprovalId:approvalId })"));
    assert!(body.contains("function approvalRowById(id)"));
    assert!(body.contains("row.scrollIntoView({ block:\"center\", behavior:\"smooth\" });"));
    assert!(body.contains("row.classList.add(\"approval-target\")"));

    let index = include_str!("../static/index.html");
    assert!(index.contains("id=\"notifyTopBtn\""));
    assert!(index.contains("class=\"notify-permission-btn\""));
    assert!(index.contains("id=\"notifyBtn\""));
    assert!(index.contains("Enable approval notifications"));

    let css = include_str!("../static/app.css");
    assert!(css.contains(".sidebar-head"));
    assert!(css.contains(".sidebar-logo"));
    assert!(css.contains("flex:0 0 24px"));
    assert!(css.contains("max-width:24px"));
    assert!(css.contains(".notify-permission-btn"));
}

#[test]
fn browser_uses_service_worker_for_android_notifications() {
    let body = app_js();

    // The service worker is registered on startup, and its click messages are dispatched.
    assert!(body.contains("initServiceWorkerNotifications();"));
    let init = between(
        body,
        "function initServiceWorkerNotifications() {",
        "async function notificationRegistration()",
    );
    assert!(init.contains("navigator.serviceWorker.register(\"/sw.js\")"));
    assert!(init.contains("navigator.serviceWorker.addEventListener(\"message\""));
    assert!(init.contains("data.type === \"giskard-notification-click\""));
    assert!(init.contains("handleNotificationClick(data.notification || {})"));

    // Readiness waits (bounded) for the worker so the first notification isn't lost to the race.
    let ready = between(
        body,
        "async function notificationRegistration() {",
        "function handleNotificationClick(data)",
    );
    assert!(ready.contains("navigator.serviceWorker.ready"));
    assert!(ready.contains("typeof reg.showNotification === \"function\""));

    // Resolution closes service-worker notifications by tag (no Notification object is held).
    let close = between(
        body,
        "function closeApprovalNotification(tid, approvalId) {",
        "function maybeNoticeNotificationPermission()",
    );
    assert!(close.contains("reg.getNotifications({ tag })"));

    // The service worker handles the click where `new Notification()` can't exist (Android).
    let sw = include_str!("../static/sw.js");
    assert!(sw.contains("addEventListener(\"notificationclick\""));
    assert!(sw.contains("self.clients.matchAll"));
    assert!(sw.contains("client.postMessage({ type: \"giskard-notification-click\""));
    assert!(sw.contains("self.skipWaiting()"));
}

#[tokio::test]
async fn service_worker_is_served_from_root_no_cache() {
    let port = 19302;
    let tmp = tempfile::TempDir::new().unwrap();
    let store = Arc::new(giskard_persist::PersistStore::new(tmp.path().to_path_buf()));
    let state = AppState::new(store, Arc::new(NoFactory), (0..32u8).collect());
    let app = build_app(state);
    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{port}"))
        .await
        .unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    // The worker must be reachable unauthenticated, at a stable root path, served no-cache and with
    // a JS content type — otherwise the browser refuses to register it.
    let resp = reqwest::get(format!("http://127.0.0.1:{port}/sw.js"))
        .await
        .unwrap();
    assert!(resp.status().is_success());
    assert_eq!(resp.headers().get("cache-control").unwrap(), "no-cache");
    assert!(
        resp.headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap()
            .contains("javascript"),
        "service worker needs a JavaScript content type"
    );
    assert!(resp.text().await.unwrap().contains("notificationclick"));
}

/// The UI script, as served at `/app.js`. The page's JS is a separate same-origin asset so the
/// Content-Security-Policy can enforce `script-src 'self'` (no inline execution).
fn app_js() -> &'static str {
    include_str!("../static/app.js")
}

#[test]
fn browser_applies_harness_context_window_updates_to_the_gauge() {
    let source = app_js();
    assert!(source.contains("case \"context_window_updated\":"));
    assert!(source.contains("ev.model.provider === state.currentModel.provider"));
    assert!(source.contains("ev.model.model === state.currentModel.model"));
    assert!(source.contains("updateGauge(state.contextUsed, ev.context_window);"));
}

#[test]
fn browser_displays_root_project_file_changes_relatively() {
    let source = app_js();
    let display = between(
        source,
        "function trimTrailingSlash(path) {",
        "function fileChangeEntries(p) {",
    );
    assert!(
        display.contains(r#"return value === "/" ? value : value.replace(/\/+$/,"");"#),
        "trailing slash trimming must preserve / as a valid project root"
    );
    assert!(
        display.contains("if (root === \"/\") return value.slice(1);"),
        "absolute files under a / project root should display without the leading slash"
    );
}

#[test]
fn browser_surfaces_markdown_overlay_fallback_warning() {
    let source = app_js();
    let markdown_overlay = between(
        source,
        "async function renderMarkdownCodeOverlay(path, highlightRes, projectId, requestId) {",
        "function showMarkdownCodeOverlay() {",
    );
    assert!(
        markdown_overlay
            .contains("showCodeOverlayWarning(\"Markdown preview unavailable; showing source.\")")
            && markdown_overlay.contains("setCodeSourceToggle(false);"),
        "Markdown preview fallback should be visible while keeping source fallback usable"
    );
    assert!(
        markdown_overlay.contains(
            "if (!rendered || typeof rendered.html !== \"string\") {\n        throw new Error(\"Markdown renderer returned an invalid response\");\n      }"
        ) && markdown_overlay
            .contains("if (typeof html !== \"string\") throw new Error(\"Markdown renderer returned an invalid response\");"),
        "invalid Markdown render payloads should take the visible fallback path"
    );
    let warning = between(
        source,
        "function showCodeOverlayWarning(message) {",
        "function showMarkdownCodeOverlay() {",
    );
    assert!(
        warning.contains("view.prepend(banner);")
            && warning.contains("banner.className = \"code-overlay-warning\"")
            && warning.contains("if (existing) existing.remove();"),
        "overlay warning should be rendered as a visible, deduplicated banner"
    );
}

#[test]
fn browser_scopes_source_overlay_async_results_to_open_request() {
    let source = app_js();
    let open = between(
        source,
        "async function openCodeOverlay(path, line) {",
        "async function renderMarkdownCodeOverlay",
    );
    assert!(
        open.contains("const requestId = Math.random().toString(36).slice(2);")
            && open.contains("$(\"codeOverlay\").dataset.requestId = requestId;")
            && open.contains("if (!codeOverlayRequestMatches(path, projectId, requestId)) return;")
            && open.contains("await renderMarkdownCodeOverlay(path, res, projectId, requestId);"),
        "source overlay opens should stamp and validate a per-open request token"
    );
    let markdown_overlay = between(
        source,
        "async function renderMarkdownCodeOverlay(path, highlightRes, projectId, requestId) {",
        "function showCodeOverlayWarning(message) {",
    );
    assert!(
        markdown_overlay
            .matches("codeOverlayRequestMatches(path, projectId, requestId)")
            .count()
            >= 3
            && markdown_overlay.contains("state.codeOverlaySource = { path, line, requestId,"),
        "Markdown overlay async raw/render completions should reject stale request tokens"
    );
}

fn between<'a>(haystack: &'a str, start: &str, end: &str) -> &'a str {
    let start_index = haystack
        .find(start)
        .unwrap_or_else(|| panic!("start marker not found: {start}"));
    let content_start = start_index + start.len();
    let end_index = haystack[content_start..]
        .find(end)
        .map(|offset| content_start + offset)
        .unwrap_or_else(|| panic!("end marker not found after {start}: {end}"));
    &haystack[content_start..end_index]
}

fn assert_order(haystack: &str, first: &str, second: &str) {
    let first_index = haystack
        .find(first)
        .unwrap_or_else(|| panic!("first marker not found: {first}"));
    let second_index = haystack
        .find(second)
        .unwrap_or_else(|| panic!("second marker not found: {second}"));
    assert!(
        first_index < second_index,
        "`{first}` should appear before `{second}`"
    );
}
