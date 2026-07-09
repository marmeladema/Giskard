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

    // No cookie: the page must load without authentication.
    let body = reqwest::get(format!("http://127.0.0.1:{port}/"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(body.contains("<title>Giskard</title>"));
    assert!(body.contains("/api/ws"), "app talks to the WS endpoint");
    assert!(
        body.contains("/api/ws-ticket"),
        "app fetches a WS auth ticket"
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
        body.contains("type:\"approval_decision\""),
        "approval cards send approval decisions over the WebSocket"
    );
    assert!(
        body.contains("accept_for_session"),
        "approval cards expose session-scoped approval"
    );
    assert!(
        body.contains("id=\"approvalSel\""),
        "approval policy is a thread-header selector"
    );
    assert!(
        body.contains("<span class=\"thread-control-label\">Approvals</span>"),
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
        body.contains("renderApprovalMetadata(body, request.metadata || [])"),
        "approval cards render structured approval metadata"
    );
    assert!(
        body.contains("approval-meta-row"),
        "approval metadata is rendered as dedicated rows"
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
        body.contains("if (initialThreadState)"),
        "UI only clears the transcript for the initial thread snapshot"
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
        body.contains("head.title = \"Expand or collapse all task details\"")
            && body.contains("const allExpanded = itemIds.length > 0")
            && body.contains("if (!allExpanded) {")
            && body.contains("for (const id of itemIds) detailIds.add(id);")
            && body.contains("itemIds.includes(state.selectedCommandId)"),
        "the task group header expands all details and collapses them when all are open"
    );
    assert!(
        body.contains(
            "row.onclick = (e) => { e.stopPropagation(); selectTaskGroupItem(group.id, key); }"
        ) && !body.contains("selectCommand(key);"),
        "in-thread task summary clicks toggle inline details without scrolling the transcript"
    );
    assert!(
        body.contains(".task-group-body { white-space:normal; font-size:12px; }")
            && body.contains(".task-group-item-status { color:var(--muted); font-size:11px;")
            && body.contains(".task-group-entry > .msg .role { font-size:10px; }"),
        "task group summaries and details use tighter typography than normal transcript rows"
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
        body.contains("pendingClientMsgs"),
        "messages are queued while WS connects"
    );
    assert!(
        body.contains("Connecting to agent"),
        "UI exposes WS connecting state"
    );
    assert!(
        body.contains("WebSocket closed"),
        "UI surfaces WS close failures"
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
        body.contains("renderToolBody"),
        "UI renders tool-call items"
    );
    assert!(
        body.contains("toolPayloadsByItemId") && body.contains("expandedToolOutputs"),
        "UI tracks tool-call payloads and expansion state by item"
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
        body.contains("wireToolRowToggle"),
        "the transcript tool row owns the input/output toggle handler"
    );
    assert!(
        body.contains("function isToolIoExpanded")
            && body.contains("if (phase === \"completed\") return false"),
        "completed tool-call input/output is collapsed by default"
    );
    assert!(
        body.contains("toggleToolOutput"),
        "tool rows can toggle collapsed input/output"
    );
    assert!(
        body.contains("Tool data collapsed"),
        "collapsed tool-call rows summarize hidden input/output"
    );
    assert!(
        !body.contains("tool-io-toggle"),
        "tool-call collapse must not use a separate toggle button"
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
        body.contains("if (phase === \"completed\") return false"),
        "completed command output is collapsed by default"
    );
    assert!(
        body.contains("commandOutputShouldAutoCollapse"),
        "running command output can auto-collapse when it grows large"
    );
    assert!(
        body.contains("COMMAND_AUTO_COLLAPSE_LINES"),
        "UI has a line threshold for running command auto-collapse"
    );
    assert!(
        body.contains("expandedCommandOutputs"),
        "UI tracks command output expansion state by item"
    );
    assert!(
        body.contains("manuallyToggledCommandOutputs"),
        "UI preserves manual command output toggles while the thread is open"
    );
    assert!(
        body.contains("toggleCommandOutput"),
        "command rows can toggle collapsed output"
    );
    assert!(
        body.contains("wireCommandRowToggle"),
        "the transcript command row owns the output toggle handler"
    );
    assert!(
        body.contains("e.target.closest(\"button,a,input,select,textarea\")"),
        "nested controls inside command rows do not trigger collapse"
    );
    assert!(
        !body.contains("cmd-toggle"),
        "command output collapse must not use a separate arrow-style button"
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
        body.contains("Output collapsed"),
        "collapsed command rows summarize hidden output"
    );
    assert!(
        body.contains("if (opts.linkify) renderLinkedText(out, output)"),
        "UI linkifies command output only when the output block is expanded"
    );
    assert!(
        body.contains("renderMarkdown(body, p.text"),
        "UI renders completed agent/reasoning text as Markdown through the server"
    );
    assert!(
        body.contains("/render`, { text }"),
        "UI requests server-rendered Markdown for agent text"
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
        body.contains("makePathLink(c.path"),
        "UI routes structured file-change paths into the source overlay"
    );
    assert!(
        body.contains("projectFileUrl(\"highlight\""),
        "UI requests server-side syntax highlighted source"
    );
    assert!(
        body.contains("projectFileUrl(\"raw\""),
        "UI exposes raw file downloads"
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
        body.contains("addItem(ev.item)"),
        "UI renders completed events with full item metadata"
    );
    assert!(
        body.contains("addItem(it)"),
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
    // Self-contained: no external script/style hosts.
    assert!(!body.contains("http://"), "no external http asset refs");
    assert!(!body.contains("cdn"), "no CDN references");
}
