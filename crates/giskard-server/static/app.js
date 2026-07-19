"use strict";
const $ = (id) => document.getElementById(id);
const PROJECT_COLLAPSE_KEY = "giskard.collapsedProjects";
const WS_RECONNECT_BASE_MS = 600;
const WS_RECONNECT_MAX_MS = 8000;
const WS_PROBLEM_NOTICE_INTERVAL_MS = 30000;
const WS_BACKGROUND_CLOSE_GRACE_MS = 10000;
const TRANSCRIPT_BOTTOM_STICKY_PX = 96;
// History is paginated by turn on the server, but a turn can hold an arbitrary number of items, so a
// turn count is a poor proxy for screen height. On open we render the live turn first, then top up
// persisted history in small batches until the transcript holds roughly this many viewports of
// scrollback — measuring pixels the server can't see. `clientHeight` makes this adapt to phone vs
// desktop for free. The cap stops pathologically tiny turns from paging forever.
const HISTORY_FILL_SCREENS = 2;
const HISTORY_FILL_BATCH = 5;
const HISTORY_FILL_MAX_TURNS = 200;
const PICKER_TYPEAHEAD_RESET_MS = 1000;
const NOTIFICATION_PROMPT_NOTICE_INTERVAL_MS = 30000;
const BROWSER_DIAGNOSTIC_LIMIT = 120;
const NOTIFICATION_DEDUP_MS = 15000;
const ACTIVE_THREAD_COMPLETED_MARK_MS = 2500;
const BROWSER_DIAGNOSTIC_VERSION = "browser-diagnostics-v1";
let state = {
  projectId:null, threadId:null, mode:"build", ws:null, wsStatus:"closed", wsConnectId:0,
  wsReconnectTimer:null, wsReconnectAttempt:0, wsStatusDetail:"WebSocket disconnected",
  wsLastProblem:"", wsLastProblemNotice:"", wsLastProblemNoticeAt:0,
  draftThread:null, firstTurnStartingThreadId:null, inputDrafts:new Map(),
  // Per-turn DOM identity (foundation for incremental reconnect): `currentRenderTurnId` is the turn
  // whose rows are being stamped right now (a persisted turn being rendered, or the live turn being
  // streamed); `newestPersistedTurnId` is the id of the newest turn known to have completed — the
  // high-water mark a future resync will use as its "give me turns after this" cursor.
  currentRenderTurnId:null, newestPersistedTurnId:null,
  models:[], pendingModelBeforeSelect:null, streamEl:null, streamItemId:null, pendingUserEl:null, pendingUserText:null,
  streamElsByItemId:new Map(), renderedItemIds:new Set(), renderedHarnessItemIds:new Set(), itemKindsByItemId:new Map(),
  pendingApprovals:new Map(), answeredApprovals:new Map(), renderedApprovalStateKeys:new Set(), pendingServerRequests:new Map(),
  runningCommands:new Map(), commandBodyElsByItemId:new Map(), commandMsgElsByItemId:new Map(), commandStopRequestedByItemId:new Set(), selectedCommandId:null,
  commandPayloadsByItemId:new Map(), endedCommandsByItemId:new Map(), expandedCommandOutputs:new Set(), manuallyToggledCommandOutputs:new Set(),
  toolPayloadsByItemId:new Map(), toolBodyElsByItemId:new Map(), expandedToolOutputs:new Set(), manuallyToggledToolOutputs:new Set(),
  activeTaskGroup:null, taskGroupSeq:0, taskItemSeq:0, taskGroupsById:new Map(), taskGroupsByItemId:new Map(),
  expandedTaskGroups:new Set(), manuallyToggledTaskGroups:new Set(), expandedTaskDetails:new Map(),
  linkifyCache:new Map(), markdownCache:new Map(), codePath:null, codeLine:null, activeTurn:false, interruptPending:false, compactPending:false,
  awaitingInitialThreadState:false, awaitingThreadResync:false, awaitingIncrementalResync:false, resyncStickBottom:false, contextWindow:0, contextUsed:null, tokenLedger:null, approvalPolicy:"ask", currentModel:null,
  mcpServers:[], mcpCapabilities:{ status:false, reload:false, oauth_login:false }, mcpLoading:false, mcpError:null, expandedMcps:new Set(),
  threadReadOnly:false, readOnlyProvider:null, readOnlyMessage:null,
  pickerTypeahead:"", pickerTypeaheadTimer:null, pickerSelectedRow:null,
  currentPlan:null, planExpanded:localStorage.getItem("giskard.planExpanded")==="1",
  threadActivity:new Map(), pendingApprovalFocus:null, notifiedApprovals:new Map(), approvalNotifications:new Map(), browserDiagnostics:[],
  lastNotificationPromptNoticeAt:0, swRegistration:null,
  collapsedProjects:new Set(loadCollapsedProjects()), pendingRemoveProject:null
};
const COMMAND_AUTO_COLLAPSE_LINES = 120;
const COMMAND_AUTO_COLLAPSE_BYTES = 16 * 1024;
const THREAD_TITLE_MAX = 120;
const EFFORT_OPTIONS = [
  { value:"minimal", label:"Minimal" },
  { value:"low", label:"Low" },
  { value:"medium", label:"Medium" },
  { value:"high", label:"High" },
  { value:"xhigh", label:"Extra High" }
];
setInterval(updateRunningCommandDurations, 1000);

async function api(method, path, body) {
  const opts = { method, headers:{} };
  if (body !== undefined) { opts.headers["Content-Type"]="application/json"; opts.body=JSON.stringify(body); }
  const r = await fetch(path, opts);
  if (!r.ok) throw new Error((await r.text()) || r.status);
  const ct = r.headers.get("content-type")||"";
  return ct.includes("json") ? r.json() : r.text();
}

/* ---------- auth ---------- */
$("loginForm").onsubmit = async (e) => {
  e.preventDefault();
  try {
    const res = await api("POST","/api/login",{ password:$("pw").value });
    if (res && res.ok === false) { $("loginErr").textContent="Wrong password."; return; }
    startApp();
  } catch (err) { $("loginErr").textContent = "Login failed: "+err.message; }
};

async function startApp() {
  $("login").style.display="none";
  $("app").classList.add("open");
  initServiceWorkerNotifications();
  initNotificationSettings();
  try { state.models = (await api("GET","/api/models")).models || []; } catch { state.models=[]; }
  renderModelSelect();
  await loadProjects();
  refreshModels();   // background: merge in any provider /v1/models discovery (§8.3)
}

// Re-pull the model list, merging each `model_listing` provider's /v1/models over the static list,
// then re-render the pickers. Best-effort; on failure the current list stays.
let _refreshingModels = false;
async function refreshModels(opts) {
  opts = opts || {};
  if (_refreshingModels) return;
  _refreshingModels = true;
  const btn = $("refreshModels"); if (btn) btn.disabled = true;
  try {
    const res = await api("POST","/api/models/refresh");
    if (res && Array.isArray(res.models)) {
      state.models = res.models;
      renderModelSelect();
      populateModalModels();
    }
    // Surface per-provider discovery failures (e.g. a 401 from a misconfigured api_key) so they
    // aren't silent. Suppressed on the modal-open auto-refresh to avoid duplicate toasts.
    if (opts.announce !== false && res && Array.isArray(res.warnings)) {
      for (const w of res.warnings) notice(`Model discovery — ${w.provider}: ${w.message}`, "warning");
    }
  } catch (e) {
    notice("Could not refresh models: "+e.message, "warning");
  } finally {
    _refreshingModels = false;
    if (btn) btn.disabled = false;
  }
}
$("refreshModels").onclick = () => refreshModels();

function initNotificationSettings() {
  const buttons = notificationPermissionButtons();
  if (!buttons.length) {
    recordNotificationDiagnostic("init_no_buttons");
    return;
  }
  if (!("Notification" in window)) {
    for (const btn of buttons) {
      setNotificationButtonState(btn, "Notifications unavailable", true);
    }
    recordNotificationDiagnostic("init_unsupported", { button_count:buttons.length });
    return;
  }
  refreshNotificationButton();
  for (const btn of buttons) btn.onclick = requestNotificationPermission;
  recordNotificationDiagnostic("init_ready", { button_count:buttons.length });
}

function notificationPermissionButtons() {
  return Array.from(document.querySelectorAll(".notify-permission-btn"));
}

// Register the notification service worker (see sw.js). Required on Chrome for Android, where
// `new Notification()` throws — notifications must be shown via registration.showNotification() and
// their clicks arrive as a postMessage from the worker. Best-effort: a non-secure context (plain
// http over a LAN IP) has no service worker, and we fall back to the Notification constructor.
function initServiceWorkerNotifications() {
  if (!("serviceWorker" in navigator)) {
    recordNotificationDiagnostic("sw_unsupported");
    return;
  }
  navigator.serviceWorker.addEventListener("message", (event) => {
    const data = event && event.data;
    if (data && data.type === "giskard-notification-click") {
      handleNotificationClick(data.notification || {});
    }
  });
  navigator.serviceWorker.register("/sw.js").then((reg) => {
    state.swRegistration = reg;
    recordNotificationDiagnostic("sw_registered", { scope: reg && reg.scope });
  }).catch((e) => {
    recordNotificationDiagnostic("sw_register_failed", { error: e && e.message ? e.message : String(e) });
  });
}

// The service-worker registration once it can show notifications, or null to fall back to the
// Notification constructor. Waits briefly for an in-flight registration so the first notification
// after startup isn't lost to the race.
async function notificationRegistration() {
  if (state.swRegistration && state.swRegistration.active) return state.swRegistration;
  if (!("serviceWorker" in navigator)) return null;
  try {
    const reg = await Promise.race([
      navigator.serviceWorker.ready,
      new Promise((resolve) => setTimeout(() => resolve(null), 1500)),
    ]);
    if (reg && typeof reg.showNotification === "function") {
      state.swRegistration = reg;
      return reg;
    }
  } catch {}
  return null;
}

// A notification was clicked — delivered by the service worker as a postMessage, or by the desktop
// Notification's onclick. Approval notifications jump to the pending approval.
function handleNotificationClick(data) {
  if (data && data.threadId && data.approvalId) {
    recordNotificationDiagnostic("approval_notification_clicked", {
      tid: data.threadId,
      approval_id: data.approvalId
    });
    closeApprovalNotification(data.threadId, data.approvalId);
    focusApprovalTarget(data.threadId, data.approvalId);
  }
}

async function requestNotificationPermission() {
  recordNotificationDiagnostic("permission_request_click");
  if (!("Notification" in window)) {
    recordNotificationDiagnostic("permission_request_unsupported");
    return;
  }
  if (Notification.permission === "granted") {
    recordNotificationDiagnostic("permission_request_already_granted");
    return;
  }
  if (!window.isSecureContext) {
    recordNotificationDiagnostic("permission_request_insecure_context");
    notice("Browser notifications require HTTPS or localhost.", "warning");
    return;
  }
  try {
    const permission = await Notification.requestPermission();
    recordNotificationDiagnostic("permission_request_resolved", { permission });
  } catch (e) {
    recordNotificationDiagnostic("permission_request_failed", { error: e && e.message ? e.message : String(e) });
    notice("Notification permission request failed: " + e.message, "warning");
  }
  refreshNotificationButton();
}

function setNotificationButtonState(btn, label, disabled) {
  if (!btn) return;
  if (btn.id === "notifyTopBtn") {
    btn.textContent = "!";
    btn.title = label;
    btn.setAttribute("aria-label", label);
    btn.hidden = label === "Approval notifications enabled" || label === "Notifications unavailable";
  } else {
    btn.textContent = label;
    btn.title = label;
  }
  btn.disabled = !!disabled;
}

function refreshNotificationButton() {
  const buttons = notificationPermissionButtons();
  if (!buttons.length || !("Notification" in window)) return;
  let label = "Enable approval notifications";
  let disabled = false;
  if (!window.isSecureContext) {
    label = "Notifications require HTTPS or localhost";
    disabled = true;
  } else if (Notification.permission === "granted") {
    label = "Approval notifications enabled";
    disabled = true;
  } else if (Notification.permission === "denied") {
    label = "Notifications blocked by browser";
    disabled = true;
  }
  for (const btn of buttons) {
    setNotificationButtonState(btn, label, disabled);
  }
  recordNotificationDiagnostic("permission_button_refreshed", { label, disabled, button_count:buttons.length });
}

function notificationPermissionState() {
  if (!("Notification" in window)) return "unsupported";
  return Notification.permission;
}

function browserDiagnosticsSnapshot() {
  const diagnostics = state.browserDiagnostics.slice();
  return {
    version: BROWSER_DIAGNOSTIC_VERSION,
    permission: notificationPermissionState(),
    secure_context: !!window.isSecureContext,
    visibility: document.visibilityState,
    focused: document.hasFocus ? document.hasFocus() : null,
    thread_id: state.threadId || null,
    ws_status: state.wsStatus,
    notified_count: state.notifiedApprovals.size,
    dedup_window_ms: NOTIFICATION_DEDUP_MS,
    button_count: notificationPermissionButtons().length,
    last_approval_decision: lastNotificationDiagnostic(isApprovalNotificationDecision),
    recent_approval_decisions: recentNotificationDiagnostics(isApprovalNotificationDecision, 6),
    diagnostics
  };
}

function notificationDebugSnapshot() {
  return browserDiagnosticsSnapshot();
}

function isApprovalNotificationDecision(entry) {
  const reason = entry && entry.reason ? entry.reason : "";
  const detail = entry && entry.detail ? entry.detail : {};
  return reason === "approval_notify_received" ||
    reason.startsWith("approval_notify_suppressed_") ||
    reason === "approval_notify_constructor_failed" ||
    reason === "approval_notify_created" ||
    (reason === "browser_notification_created" && detail.kind === "approval") ||
    (reason.startsWith("browser_notification_") && detail.kind === "approval");
}

function lastNotificationDiagnostic(predicate) {
  for (let i = state.browserDiagnostics.length - 1; i >= 0; i--) {
    const entry = state.browserDiagnostics[i];
    if (!predicate || predicate(entry)) return entry;
  }
  return null;
}

function recentNotificationDiagnostics(predicate, limit) {
  const recent = [];
  for (let i = state.browserDiagnostics.length - 1; i >= 0 && recent.length < limit; i--) {
    const entry = state.browserDiagnostics[i];
    if (!predicate || predicate(entry)) recent.push(entry);
  }
  return recent.reverse();
}

function recordBrowserDiagnostic(category, reason, detail) {
  const entry = {
    at: new Date().toISOString(),
    category: category || "browser",
    reason,
    detail: detail || {},
    permission: notificationPermissionState(),
    secure_context: !!window.isSecureContext,
    visibility: document.visibilityState,
    focused: document.hasFocus ? document.hasFocus() : null,
    thread_id: state.threadId || null,
    ws_status: state.wsStatus
  };
  state.browserDiagnostics.push(entry);
  if (state.browserDiagnostics.length > BROWSER_DIAGNOSTIC_LIMIT) {
    state.browserDiagnostics.splice(0, state.browserDiagnostics.length - BROWSER_DIAGNOSTIC_LIMIT);
  }
  console.info("[Giskard browser diagnostics]", entry);
  renderBrowserDiagnosticsPanel();
}

function recordNotificationDiagnostic(reason, detail) {
  recordBrowserDiagnostic("notification", reason, detail);
}

function showBrowserDiagnostics() {
  const snapshot = browserDiagnosticsSnapshot();
  console.info("[Giskard browser diagnostics] snapshot", snapshot);
  if (console.table) console.table(snapshot.diagnostics);
  renderBrowserDiagnosticsPanel(snapshot, true);
}

function renderBrowserDiagnosticsPanel(snapshot, reveal) {
  const panel = $("browserDiagnosticsPanel");
  if (!panel) return;
  const log = $("browserDiagnosticsLog");
  if (!log) return;
  snapshot = snapshot || browserDiagnosticsSnapshot();
  const last = snapshot.diagnostics[snapshot.diagnostics.length - 1];
  const lastApproval = snapshot.last_approval_decision;
  const approvalDetail = lastApproval && lastApproval.detail ? lastApproval.detail : {};
  const lines = [
    `version: ${snapshot.version}`,
    `permission: ${snapshot.permission}`,
    `secure: ${snapshot.secure_context}`,
    `visibility: ${snapshot.visibility}`,
    `focused: ${snapshot.focused}`,
    `thread: ${snapshot.thread_id || "none"}`,
    `ws: ${snapshot.ws_status}`,
    `dedupMs: ${snapshot.dedup_window_ms}`,
    `lastApproval: ${lastApproval ? lastApproval.reason : "none"}`,
    `approvalSource: ${approvalDetail.source || "none"}`,
    `approvalId: ${approvalDetail.approval_id || "none"}`,
    `last: ${last ? last.reason : "none"}`
  ];
  const recent = snapshot.recent_approval_decisions || [];
  if (recent.length) {
    lines.push("recentApprovals:");
    for (const entry of recent) {
      const detail = entry.detail || {};
      const suffix = detail.age_ms !== undefined ? ` age=${detail.age_ms}ms` : "";
      lines.push(`- ${entry.reason} source=${detail.source || "none"} id=${detail.approval_id || "none"} visible=${entry.visibility} focused=${entry.focused}${suffix}`);
    }
  }
  const latest = snapshot.diagnostics.slice(-20);
  if (latest.length) {
    lines.push("recentBrowserEvents:");
    for (const entry of latest) {
      const detail = entry.detail || {};
      const fields = [];
      if (detail.source) fields.push(`source=${detail.source}`);
      if (detail.approval_id !== undefined && detail.approval_id !== null) fields.push(`approval=${detail.approval_id}`);
      if (detail.status) fields.push(`status=${detail.status}`);
      if (detail.error) fields.push(`error=${detail.error}`);
      lines.push(`- ${entry.at} ${entry.category}:${entry.reason} visible=${entry.visibility} focused=${entry.focused}${fields.length ? " " + fields.join(" ") : ""}`);
    }
  }
  log.textContent = lines.join("\n");
  if (reveal || !panel.hidden) panel.hidden = false;
}

async function copyBrowserDiagnostics() {
  const snapshot = browserDiagnosticsSnapshot();
  const text = JSON.stringify(snapshot, null, 2);
  try {
    await navigator.clipboard.writeText(text);
    notice("Browser diagnostics copied.", "info");
  } catch (e) {
    console.info("[Giskard browser diagnostics] copy fallback", text);
    notice("Could not copy diagnostics; logged them to the console.", "warning");
  }
}

function clearBrowserDiagnostics() {
  state.browserDiagnostics = [];
  renderBrowserDiagnosticsPanel(browserDiagnosticsSnapshot(), true);
}

window.giskardBrowserDiagnostics = browserDiagnosticsSnapshot;
window.giskardNotificationDebug = notificationDebugSnapshot;
const browserDiagnosticsBtn = $("browserDiagnosticsBtn");
if (browserDiagnosticsBtn) browserDiagnosticsBtn.onclick = showBrowserDiagnostics;
const copyBrowserDiagnosticsBtn = $("copyBrowserDiagnosticsBtn");
if (copyBrowserDiagnosticsBtn) copyBrowserDiagnosticsBtn.onclick = copyBrowserDiagnostics;
const clearBrowserDiagnosticsBtn = $("clearBrowserDiagnosticsBtn");
if (clearBrowserDiagnosticsBtn) clearBrowserDiagnosticsBtn.onclick = clearBrowserDiagnostics;
const testNotificationBtn = $("testNotificationBtn");
if (testNotificationBtn) testNotificationBtn.onclick = sendTestNotification;

async function sendTestNotification() {
  if (!("Notification" in window)) {
    recordNotificationDiagnostic("test_notify_unsupported");
    notice("Browser notifications are unavailable.", "warning");
    return;
  }
  if (Notification.permission !== "granted") {
    recordNotificationDiagnostic("test_notify_suppressed_permission");
    notice("Notification permission is not granted.", "warning");
    return;
  }
  const tag = `giskard-test-${Date.now()}`;
  let result;
  try {
    result = await showAppNotification("Giskard test notification", {
      body: "Browser notification display test.",
      tag,
      renotify: true,
      requireInteraction: true,
      data: { test:true }
    }, {
      kind: "test",
      tag
    });
  } catch (e) {
    recordNotificationDiagnostic("test_notify_constructor_failed", {
      tag,
      error: e && e.message ? e.message : String(e)
    });
    notice("Test notification failed: " + e.message, "warning");
    return;
  }
  if (result) recordNotificationDiagnostic("test_notify_created", { tag, via: result.via });
}

/* ---------- projects & threads ---------- */
async function loadProjects() {
  const { projects } = await api("GET","/api/projects");
  const box = $("projects"); box.innerHTML="";
  state.projectNames = {};   // id → name, for the mobile "project / thread" breadcrumb
  const pending = [];
  for (const p of projects) {
    state.projectNames[p.id] = p.name;
    const d = document.createElement("div"); d.className="proj";
    d.dataset.pid = p.id;
    const collapsed = state.collapsedProjects.has(p.id);
    d.classList.toggle("collapsed", collapsed);
    const name = document.createElement("div"); name.className="name";
    const toggle = document.createElement("button");
    toggle.type = "button"; toggle.className = "project-toggle";
    toggle.setAttribute("aria-label", collapsed ? "Expand project" : "Collapse project");
    toggle.setAttribute("aria-expanded", String(!collapsed));
    toggle.textContent = collapsed ? ">" : "v";
    toggle.title = collapsed ? "Expand project" : "Collapse project";
    toggle.onclick = (e) => {
      e.stopPropagation();
      setProjectCollapsed(p.id, !state.collapsedProjects.has(p.id));
    };
    const label = document.createElement("button");
    label.type = "button"; label.className = "project-name";
    label.textContent = p.name; label.title = p.name;
    label.onclick = () => setProjectCollapsed(p.id, !state.collapsedProjects.has(p.id));
    const add = document.createElement("button"); add.className="project-add"; add.textContent="+";
    add.title="New thread";
    add.onclick = (e) => {
      e.stopPropagation();
      setProjectCollapsed(p.id, false);
      newThread(p.id);
    };
    const menuBtn = document.createElement("button");
    menuBtn.type = "button"; menuBtn.className = "project-menu-btn";
    menuBtn.textContent = "..."; menuBtn.title = "Project actions";
    menuBtn.setAttribute("aria-label", "Project actions");
    const menu = document.createElement("div"); menu.className = "project-menu"; menu.hidden = true;
    const remove = document.createElement("button");
    remove.type = "button"; remove.textContent = "Remove project"; remove.className = "danger";
    remove.onclick = (e) => {
      e.stopPropagation();
      closeThreadMenus();
      openRemoveProjectModal(p);
    };
    menu.append(remove);
    menuBtn.onclick = (e) => {
      e.stopPropagation();
      const wasHidden = menu.hidden;
      closeThreadMenus();
      menu.hidden = !wasHidden;
    };
    name.append(toggle, label, add, menuBtn, menu); d.append(name);
    const threads = document.createElement("div");
    threads.id = "threads-"+p.id; threads.className = "project-threads";
    threads.hidden = collapsed;
    d.append(threads);
    box.append(d);
    pending.push(loadThreads(p.id));
  }
  await Promise.all(pending);
  restoreLastThread();
}

// Reopen the thread that was last active in this browser (H: persisted client-side, not on the
// server). Silently ignored if it no longer exists (e.g. deleted since).
function restoreLastThread() {
  if (state.threadId) return;   // already viewing a thread
  let last;
  try { last = JSON.parse(localStorage.getItem("giskard.lastThread") || "null"); } catch { last = null; }
  if (!last || !last.pid || !last.tid) return;
  const el = document.querySelector(`.thread[data-tid="${last.tid}"]`);
  if (!el) { localStorage.removeItem("giskard.lastThread"); return; }
  openThread(last.pid, last.tid, currentThreadTitle(el), { silent:true });
}

async function loadThreads(pid) {
  const box = $("threads-"+pid); if (!box) return;
  try {
    const { threads } = await api("GET",`/api/projects/${pid}/threads`);
    box.innerHTML="";
    for (const t of threads.filter(t => !t.archived)) box.append(threadRow(pid, t));
    const archived = threads.filter(t => t.archived);
    if (archived.length) {
      const label = document.createElement("div");
      label.className = "thread-section-label";
      label.textContent = "Archived";
      box.append(label);
      for (const t of archived) box.append(threadRow(pid, t));
    }
  } catch {}
}

function loadCollapsedProjects() {
  try {
    const ids = JSON.parse(localStorage.getItem(PROJECT_COLLAPSE_KEY) || "[]");
    return Array.isArray(ids) ? ids.filter(Boolean) : [];
  } catch {
    return [];
  }
}

function saveCollapsedProjects() {
  try {
    localStorage.setItem(PROJECT_COLLAPSE_KEY, JSON.stringify([...state.collapsedProjects]));
  } catch {}
}

function setProjectCollapsed(pid, collapsed) {
  if (!pid) return;
  if (collapsed) state.collapsedProjects.add(pid);
  else state.collapsedProjects.delete(pid);
  saveCollapsedProjects();
  const project = document.querySelector(`.proj[data-pid="${pid}"]`);
  if (!project) return;
  project.classList.toggle("collapsed", collapsed);
  const threads = $("threads-"+pid);
  if (threads) threads.hidden = collapsed;
  const toggle = project.querySelector(".project-toggle");
  if (toggle) {
    toggle.textContent = collapsed ? ">" : "v";
    toggle.title = collapsed ? "Expand project" : "Collapse project";
    toggle.setAttribute("aria-label", toggle.title);
    toggle.setAttribute("aria-expanded", String(!collapsed));
  }
  closeThreadMenus();
}

function threadRow(pid, t) {
  const row = document.createElement("div"); row.className="thread-row";
  const title = t.title || t.id.slice(0,8);
  const el = document.createElement("div"); el.className="thread mono";
  applyThreadTitleToElement(el, pid, t.id, title);

  const menuBtn = document.createElement("button");
  menuBtn.type = "button"; menuBtn.className = "thread-menu-btn";
  menuBtn.textContent = "..."; menuBtn.title = "Thread actions";
  menuBtn.setAttribute("aria-label", "Thread actions");

  const menu = document.createElement("div"); menu.className = "thread-menu"; menu.hidden = true;
  const rename = document.createElement("button");
  rename.type = "button"; rename.textContent = "Rename";
  rename.onclick = async (e) => {
    e.stopPropagation();
    closeThreadMenus();
    beginRenameThread(el, pid, t.id);
  };
  const archive = document.createElement("button");
  archive.type = "button"; archive.textContent = t.archived ? "Unarchive" : "Archive";
  archive.onclick = async (e) => {
    e.stopPropagation();
    closeThreadMenus();
    await setThreadArchived(pid, t.id, !t.archived);
  };
  const del = document.createElement("button");
  del.type = "button"; del.textContent = "Delete"; del.className = "danger";
  del.onclick = async (e) => {
    e.stopPropagation();
    closeThreadMenus();
    await deleteThread(pid, t.id, currentThreadTitle(el));
  };
  menu.append(rename, archive, del);

  menuBtn.onclick = (e) => {
    e.stopPropagation();
    const wasHidden = menu.hidden;
    closeThreadMenus();
    menu.hidden = !wasHidden;
  };
  row.append(el, menuBtn, menu);
  return row;
}

function applyThreadTitleToElement(el, pid, tid, title) {
  el.innerHTML = "";
  const status = document.createElement("span");
  status.className = "thread-status";
  status.setAttribute("aria-hidden", "true");
  const label = document.createElement("span");
  label.className = "thread-title";
  label.textContent = title;
  el.append(status, label);
  el.title = title;
  el.dataset.title = title;
  el.dataset.pid = pid;
  el.dataset.tid = tid;
  el.onclick = () => openThread(pid, tid, title);
  renderThreadActivityIndicator(tid);
}

function updateThreadRowTitle(tid, title) {
  if (!tid || !title) return;
  document.querySelectorAll(".thread").forEach((el) => {
    if (el.dataset.tid !== tid) return;
    const pid = el.dataset.pid || state.projectId;
    applyThreadTitleToElement(el, pid, tid, title);
  });
}

function currentThreadTitle(el) {
  const title = (el.dataset && el.dataset.title ? el.dataset.title : el.textContent || "").trim();
  return title || (el.dataset.tid || "").slice(0,8);
}

function threadRowForId(tid) {
  if (!tid) return null;
  return document.querySelector(`.thread[data-tid="${tid}"]`);
}

function threadMetaForId(tid) {
  const el = threadRowForId(tid);
  if (!el) return null;
  return {
    pid: el.dataset.pid || "",
    tid: el.dataset.tid || tid,
    title: currentThreadTitle(el)
  };
}

function clearThreadActivity(tid) {
  if (!tid) return;
  const activity = state.threadActivity.get(String(tid));
  if (activity && activity.active_turn) {
    activity.unread = false;
    activity.approval_id = null;
    activity.kind = "progress";
    state.threadActivity.set(String(tid), activity);
  } else {
    state.threadActivity.delete(String(tid));
  }
  renderThreadActivityIndicator(tid);
}

function renderThreadActivityIndicator(tid) {
  const el = threadRowForId(tid);
  if (!el) return;
  const status = el.querySelector(".thread-status");
  if (!status) return;
  const activity = state.threadActivity.get(String(tid));
  const visible = !!activity && (activity.unread || activity.active_turn || activity.approval_id || activity.kind === "turn_completed" || activity.kind === "error");
  el.classList.toggle("has-activity", visible);
  el.classList.toggle("activity-approval", visible && activity && activity.kind === "approval_requested");
  el.classList.toggle("activity-error", visible && activity && activity.kind === "error");
  el.classList.toggle("activity-running", visible && activity && activity.active_turn && activity.kind !== "approval_requested");
  if (!visible) {
    status.textContent = "";
    status.title = "";
    return;
  }
  if (activity.kind === "approval_requested") status.textContent = "!";
  else if (activity.kind === "error") status.textContent = "x";
  else if (activity.active_turn) status.textContent = "o";
  else status.textContent = "*";
  status.title = activity.summary || "Thread activity";
}

function renderAllThreadActivityIndicators() {
  document.querySelectorAll(".thread").forEach(el => renderThreadActivityIndicator(el.dataset.tid));
}

function setThreadActivity(tid, activity) {
  if (!tid || !activity) return;
  const key = String(tid);
  state.threadActivity.set(key, activity);
  renderThreadActivityIndicator(key);
}

function setActiveThreadActivity(kind, activeTurn, summary, extra) {
  if (!state.threadId) return;
  const tid = String(state.threadId);
  setThreadActivity(tid, Object.assign({
    kind,
    active_turn: !!activeTurn,
    approval_id: null,
    server_request_id: null,
    summary: summary || "",
    source: "active_thread_event",
    unread: false
  }, extra || {}));
  if (kind === "turn_completed" && !activeTurn) {
    clearActiveThreadActivityLater(tid, kind);
  }
}

function clearActiveThreadActivityLater(tid, kind) {
  const key = String(tid || "");
  if (!key) return;
  setTimeout(() => {
    const activity = state.threadActivity.get(key);
    if (!activity || activity.source !== "active_thread_event" || activity.kind !== kind || activity.active_turn) return;
    state.threadActivity.delete(key);
    renderThreadActivityIndicator(key);
  }, ACTIVE_THREAD_COMPLETED_MARK_MS);
}

function clearApprovalThreadActivity(tid, approvalId) {
  if (!tid || !approvalId) return;
  const key = String(tid);
  const activity = state.threadActivity.get(key);
  if (!activity || String(activity.approval_id || "") !== String(approvalId)) return;
  activity.approval_id = null;
  if (activity.active_turn) {
    activity.kind = "progress";
    activity.summary = "Turn running";
    activity.unread = state.threadId ? String(state.threadId) !== key : activity.unread;
    state.threadActivity.set(key, activity);
  } else {
    state.threadActivity.delete(key);
  }
  renderThreadActivityIndicator(key);
}

function clearServerRequestThreadActivity(tid, requestId) {
  if (!tid || !requestId) return;
  const key = String(tid);
  const activity = state.threadActivity.get(key);
  if (!activity || String(activity.server_request_id || "") !== String(requestId)) return;
  activity.server_request_id = null;
  if (activity.active_turn) {
    activity.kind = "progress";
    activity.summary = "Turn running";
    activity.unread = state.threadId ? String(state.threadId) !== key : activity.unread;
    state.threadActivity.set(key, activity);
  } else {
    state.threadActivity.delete(key);
  }
  renderThreadActivityIndicator(key);
}

function normalizeThreadTitleInput(value) {
  return (value || "").trim().replace(/\s+/g, " ");
}

function beginRenameThread(el, pid, tid) {
  const currentTitle = currentThreadTitle(el);
  const input = document.createElement("input");
  input.type = "text";
  input.className = "thread-title-input mono";
  input.value = currentTitle;
  input.maxLength = THREAD_TITLE_MAX;
  input.dataset.tid = tid;
  input.setAttribute("aria-label", "Thread name");

  let finished = false;
  const restore = (title) => {
    applyThreadTitleToElement(el, pid, tid, title);
    input.replaceWith(el);
  };
  const cancel = () => {
    if (finished) return;
    finished = true;
    restore(currentTitle);
  };
  const commit = async () => {
    if (finished) return;
    const nextTitle = normalizeThreadTitleInput(input.value);
    if (!nextTitle) {
      notice("Thread name cannot be empty.", "error");
      input.focus();
      input.select();
      return;
    }
    if (nextTitle === currentTitle) {
      finished = true;
      restore(currentTitle);
      return;
    }
    finished = true;
    input.disabled = true;
    try {
      const updated = await renameThread(pid, tid, nextTitle);
      const savedTitle = updated && updated.title ? updated.title : nextTitle;
      restore(savedTitle);
      if (state.threadId === tid) setThreadTitle(savedTitle);
    } catch (e) {
      finished = false;
      input.disabled = false;
      notice("Rename thread failed: " + e.message, "error");
      input.focus();
      input.select();
    }
  };

  input.onkeydown = (e) => {
    e.stopPropagation();
    if (e.key === "Enter") {
      e.preventDefault();
      commit();
    } else if (e.key === "Escape") {
      e.preventDefault();
      cancel();
    }
  };
  input.onclick = (e) => e.stopPropagation();
  input.onblur = cancel;

  el.replaceWith(input);
  input.focus();
  input.select();
}

function closeThreadMenus() {
  document.querySelectorAll(".thread-menu, .project-menu").forEach(m => m.hidden = true);
}

document.addEventListener("click", closeThreadMenus);

async function setThreadArchived(pid, tid, archived) {
  try {
    await api("POST", `/api/projects/${pid}/threads/${tid}/archive`, { archived });
    if (state.threadId === tid && archived) clearThreadView(tid);
    await loadThreads(pid);
  } catch (e) {
    notice((archived ? "Archive" : "Unarchive") + " thread failed: " + e.message, "error");
  }
}

async function renameThread(pid, tid, title) {
  return api("PATCH", `/api/projects/${pid}/threads/${tid}/title`, { title });
}

async function deleteThread(pid, tid, title) {
  if (!confirm(`Delete thread "${title}"? This also deletes the Codex thread.`)) return;
  try {
    await api("DELETE", `/api/projects/${pid}/threads/${tid}`);
    clearThreadView(tid);
    await loadThreads(pid);
  } catch (e) {
    notice("Delete thread failed: " + e.message, "error");
  }
}

function clearThreadView(tid) {
  if (state.threadId !== tid) return;
  saveComposerDraft();
  try { localStorage.removeItem("giskard.lastThread"); } catch {}
  clearWsReconnectTimer();
  const ws = state.ws;
  state.ws = null;
  if (ws) {
    ws._giskardExpectedClose = true;
    try { ws.close(); } catch {}
  }
  state.projectId = null; state.threadId = null;
  state.draftThread = null;
  state.firstTurnStartingThreadId = null;
  state.pendingUserEl = null; state.pendingUserText = null;
  state.compactPending = false;
  state.currentModel = null;
  $("effortControl").hidden = true;
  setTurnActive(false);
  state.awaitingInitialThreadState = false;
  state.awaitingThreadResync = false;
  resetRenderState();
  $("thrHeader").style.display="none"; $("composer").style.display="none";
  $("pickerBar").style.display="none"; closeModelPicker(); closeTurnPicker();
  $("transcript").innerHTML="";
  restoreComposerDraft();
  setWsStatus("closed", "No thread selected.");
}

function clearStoredLastThreadForProject(pid) {
  try {
    const last = JSON.parse(localStorage.getItem("giskard.lastThread") || "null");
    if (last && last.pid === pid) localStorage.removeItem("giskard.lastThread");
  } catch {
    localStorage.removeItem("giskard.lastThread");
  }
}

function clearProjectView(pid) {
  clearStoredLastThreadForProject(pid);
  if (state.projectId !== pid) return;
  clearThreadView(state.threadId);
}

/* ---------- new-project modal + directory picker ---------- */
$("newProj").onclick = () => openProjectModal();

function populateModalModels() {
  const sel = $("pmModel"); if (!sel) return;
  const prev = sel.value;
  sel.innerHTML = "";
  for (const m of state.models) {
    const o = document.createElement("option");
    o.value = `${m.provider}/${m.model}`; o.textContent = modelOptionLabel(m);
    o.dataset.provider = m.provider; o.dataset.model = m.model;
    sel.append(o);
  }
  if (!state.models.length) { const o=document.createElement("option"); o.textContent="(no models configured)"; sel.append(o); }
  if (prev) sel.value = prev;
}
function openProjectModal() {
  closeDrawers();
  $("pmErr").textContent = "";
  populateModalModels();
  $("projectModal").classList.add("open");
  // Start browsing where we last were, falling back to the filesystem root.
  browsePicker(localStorage.getItem("giskard.lastBrowse") || "/");
  refreshModels({ announce:false });   // pull discovered models; startup already announced failures
}
function closeProjectModal() { $("projectModal").classList.remove("open"); }
$("pmCancel").onclick = closeProjectModal;
$("projectModal").addEventListener("click", (e) => { if (e.target === $("projectModal")) closeProjectModal(); });
$("projectModal").addEventListener("keydown", handleProjectModalKeydown);

function basename(p) { const s = String(p).replace(/\/+$/,""); const i = s.lastIndexOf("/"); return i>=0 ? s.slice(i+1) : s; }
function parentOf(p) { const s = String(p).replace(/\/+$/,""); const i = s.lastIndexOf("/"); return i>0 ? s.slice(0,i) : "/"; }

async function browsePicker(path) {
  let res;
  try { res = await api("GET", `/api/browse?path=${encodeURIComponent(path)}`); }
  catch (e) { $("pmErr").textContent = "Cannot open folder: "+e.message; return; }
  state.pickerDir = res.path;
  localStorage.setItem("giskard.lastBrowse", res.path);
  $("pmPath").textContent = res.path;
  // Prefill the project name from the current folder's basename (still editable).
  $("pmName").value = basename(res.path) || res.path;
  $("pmErr").textContent = "";

  resetPickerTypeahead();
  clearPickerSelection();
  const list = $("pmList"); list.tabIndex = 0; list.innerHTML = "";
  if (res.path !== "/") {
    const up = document.createElement("div"); up.className = "direntry";
    up.dataset.nav = "up";
    up.innerHTML = `<span class="ic">↰</span><span>..</span>`;
    up.onclick = () => browsePicker(parentOf(res.path));
    list.append(up);
  }
  for (const e of res.entries) {
    const row = document.createElement("div");
    row.className = "direntry" + (e.is_dir ? "" : " file");
    row.dataset.name = e.name;
    row.dataset.isDir = String(e.is_dir);
    row.innerHTML = `<span class="ic">${e.is_dir ? "📁" : "📄"}</span><span>${escapeHtml(e.name)}</span>`;
    if (e.is_dir) {
      const child = res.path.replace(/\/+$/,"") + "/" + e.name;
      row.dataset.path = child;
      row.onclick = () => browsePicker(child);
    } else {
      row.onclick = () => selectPickerRow(row);
    }
    list.append(row);
  }
  list.focus({ preventScroll:true });
}

function clearPickerSelection() {
  if (state.pickerSelectedRow) state.pickerSelectedRow.classList.remove("selected");
  state.pickerSelectedRow = null;
}

function selectPickerRow(row) {
  clearPickerSelection();
  state.pickerSelectedRow = row;
  row.classList.add("selected");
  row.scrollIntoView({ block:"nearest" });
}

function resetPickerTypeahead() {
  state.pickerTypeahead = "";
  if (state.pickerTypeaheadTimer) clearTimeout(state.pickerTypeaheadTimer);
  state.pickerTypeaheadTimer = null;
}

function schedulePickerTypeaheadReset() {
  if (state.pickerTypeaheadTimer) clearTimeout(state.pickerTypeaheadTimer);
  state.pickerTypeaheadTimer = setTimeout(resetPickerTypeahead, PICKER_TYPEAHEAD_RESET_MS);
}

function activeElementAcceptsText() {
  const el = document.activeElement;
  if (!el) return false;
  const tag = el.tagName;
  return tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT" || tag === "BUTTON" || el.isContentEditable;
}

function handleProjectModalKeydown(e) {
  if (!$("projectModal").classList.contains("open")) return;
  if (activeElementAcceptsText()) return;

  if (e.key === "Enter" && state.pickerSelectedRow && state.pickerSelectedRow.dataset.path) {
    e.preventDefault();
    browsePicker(state.pickerSelectedRow.dataset.path);
    return;
  }

  if (e.key.length !== 1 || e.ctrlKey || e.metaKey || e.altKey) return;
  const char = e.key.toLocaleLowerCase();
  if (char.trim() === "") return;
  e.preventDefault();
  state.pickerTypeahead += char;
  schedulePickerTypeaheadReset();

  const prefix = state.pickerTypeahead;
  const rows = Array.from($("pmList").querySelectorAll(".direntry[data-name]"));
  const match = rows.find(row => row.dataset.name.toLocaleLowerCase().startsWith(prefix));
  if (match) selectPickerRow(match);
}

$("pmNewFolder").onclick = async () => {
  const name = prompt("New folder name:"); if (!name) return;
  try {
    const res = await api("POST","/api/browse/mkdir",{ parent: state.pickerDir, name });
    await browsePicker(res.path);   // navigate into the folder we just created
  } catch (e) { $("pmErr").textContent = "Create folder failed: "+e.message; }
};

$("pmCreate").onclick = async () => {
  const dir = state.pickerDir;
  const name = $("pmName").value.trim();
  if (!dir) { $("pmErr").textContent = "Pick a folder first."; return; }
  if (!name) { $("pmErr").textContent = "Enter a project name."; return; }
  const opt = $("pmModel").selectedOptions[0];
  const model = opt && opt.dataset.model
    ? { provider: opt.dataset.provider, model: opt.dataset.model, reasoning_effort:null }
    : { provider:"openai", model:"gpt-5.5", reasoning_effort:null };
  try {
    await api("POST","/api/projects",{ name, dir, default_model:model });
    closeProjectModal();
    await loadProjects();
  } catch (e) { $("pmErr").textContent = "Create project failed: "+e.message; }
};

function openRemoveProjectModal(project) {
  closeDrawers();
  state.pendingRemoveProject = project;
  $("removeProjectErr").textContent = "";
  $("removeProjectName").textContent = project.name || "this project";
  $("removeProjectDir").textContent = project.dir || "(unknown source directory)";
  $("removeProjectModal").classList.add("open");
  $("removeProjectConfirm").focus();
}

function closeRemoveProjectModal() {
  $("removeProjectModal").classList.remove("open");
  state.pendingRemoveProject = null;
}

$("removeProjectCancel").onclick = closeRemoveProjectModal;
$("removeProjectModal").addEventListener("click", (e) => {
  if (e.target === $("removeProjectModal")) closeRemoveProjectModal();
});

$("removeProjectConfirm").onclick = async () => {
  const project = state.pendingRemoveProject;
  if (!project || !project.id) return;
  const btn = $("removeProjectConfirm");
  btn.disabled = true;
  $("removeProjectErr").textContent = "";
  try {
    await api("DELETE", `/api/projects/${project.id}`);
    clearProjectView(project.id);
    closeRemoveProjectModal();
    await loadProjects();
    notice("Project removed from Giskard.");
  } catch (e) {
    $("removeProjectErr").textContent = "Remove project failed: " + e.message;
  } finally {
    btn.disabled = false;
  }
};

async function newThread(pid) {
  try {
    const project = await api("GET", `/api/projects/${pid}`);
    openDraftThread(pid, project && project.default_model);
  } catch (e) { alert("New thread failed: "+e.message); }
}

function fallbackDraftModel() {
  const first = state.models && state.models[0];
  if (first && first.provider && first.model) {
    return { provider:first.provider, model:first.model, reasoning_effort:null };
  }
  return { provider:"openai", model:"gpt-5.5", reasoning_effort:null };
}

function normalizeDraftModel(model) {
  if (model && model.provider && model.model) {
    return {
      provider:String(model.provider),
      model:String(model.model),
      reasoning_effort:model.reasoning_effort || null
    };
  }
  return fallbackDraftModel();
}

function isDraftThread() {
  return !!state.draftThread && !state.threadId;
}

function composerDraftKey() {
  if (state.threadId) return `thread:${state.threadId}`;
  if (isDraftThread() && state.draftThread.projectId) return `draft:${state.draftThread.projectId}`;
  return "";
}

function saveComposerDraft() {
  const input = $("input");
  if (!input) return;
  const key = composerDraftKey();
  if (!key) return;
  const value = input.value || "";
  if (value) state.inputDrafts.set(key, value);
  else state.inputDrafts.delete(key);
}

function restoreComposerDraft() {
  const input = $("input");
  if (!input) return;
  const key = composerDraftKey();
  input.value = key ? (state.inputDrafts.get(key) || "") : "";
}

function clearComposerDraft(key) {
  if (key) state.inputDrafts.delete(key);
  const input = $("input");
  if (input && composerDraftKey() === key) input.value = "";
}

function openDraftThread(pid, defaultModel) {
  saveComposerDraft();
  clearWsReconnectTimer();
  const oldWs = state.ws;
  state.ws = null;
  if (oldWs) {
    oldWs._giskardExpectedClose = true;
    try { oldWs.close(); } catch {}
  }

  state.projectId = pid;
  state.threadId = null;
  state.draftThread = { projectId:pid, title:"New thread" };
  state.firstTurnStartingThreadId = null;
  state.pendingUserEl = null;
  state.pendingUserText = null;
  state.compactPending = false;
  state.currentModel = normalizeDraftModel(defaultModel);
  state.mcpServers = []; state.mcpError = null; state.expandedMcps = new Set();
  state.mcpCapabilities = { status:false, reload:false, oauth_login:false };
  $("tasksMenu").hidden = true;
  $("mcpMenu").hidden = true;
  $("usageMenu").hidden = true;
  renderMcpButton();
  setMode("build");
  setApprovalPolicy("ask");
  setTurnActive(false);
  state.historyLoaded = false; state.oldestTurnId = null; state.hasMoreHistory = false;
  state.loadingHistory = false; state.pendingOlder = false; state.autoFilledTurns = 0;
  state.currentRenderTurnId = null; state.newestPersistedTurnId = null;
  state.contextUsed = null; state.contextWindow = 0; state.tokenLedger = null;
  updateGauge(null, 0);
  state.awaitingInitialThreadState = false;
  state.awaitingThreadResync = false;
  resetRenderState();
  document.querySelectorAll(".thread").forEach(e => e.classList.remove("active"));
  $("thrHeader").style.display="flex"; $("composer").style.display="flex";
  $("pickerBar").style.display="flex";
  setThreadTitle("New thread");
  $("transcript").className=""; $("transcript").innerHTML=""; $("notices").innerHTML="";
  setWsStatus("draft", "Draft thread. Send a message to create it.");
  syncModelControls();
  closeDrawers();
  restoreComposerDraft();
  $("input").focus();
}

/* ---------- thread view + websocket ---------- */
async function openThread(pid, tid, title, opts) {
  opts = opts || {};
  saveComposerDraft();
  if (!opts.firstTurnStarting) state.firstTurnStartingThreadId = null;
  if (opts.focusApprovalId) {
    state.pendingApprovalFocus = {
      threadId:String(tid),
      approvalId:String(opts.focusApprovalId),
      attempts:0
    };
  }
  let res;
  try {
    res = await api("POST",`/api/projects/${pid}/threads`,{ thread_id:tid, resume:null });
    tid = res.thread_id || tid;
  } catch (e) {
    if (opts.silent) { localStorage.removeItem("giskard.lastThread"); return; }
    alert("Open thread failed: "+e.message);
    return;
  }

  // Remember this thread so a browser reload resumes it (client-side only).
  try { localStorage.setItem("giskard.lastThread", JSON.stringify({ pid, tid })); } catch {}

  clearThreadActivity(tid);
  state.projectId = pid; state.threadId = tid; state.pendingUserEl = null; state.pendingUserText = null;
  state.threadReadOnly = false; state.readOnlyProvider = null; state.readOnlyMessage = null;
  updateReadOnlyBanner();
  state.draftThread = null;
  state.compactPending = false;
  state.currentModel = null;
  $("effortControl").hidden = true;
  state.mcpServers = []; state.mcpError = null; state.expandedMcps = new Set();
  state.mcpCapabilities = { status:false, reload:false, oauth_login:false };
  $("tasksMenu").hidden = true;
  $("mcpMenu").hidden = true;
  $("usageMenu").hidden = true;
  renderMcpButton();
  loadMcpServers({ announce:false });
  setTurnActive(false);
  state.historyLoaded = false; state.oldestTurnId = null; state.hasMoreHistory = false;
  state.loadingHistory = false; state.pendingOlder = false; state.autoFilledTurns = 0;
  state.currentRenderTurnId = null; state.newestPersistedTurnId = null;
  state.contextUsed = null; state.contextWindow = 0; state.tokenLedger = null;
  updateGauge(null, 0);
  state.awaitingInitialThreadState = true;
  state.awaitingThreadResync = false;
  state.awaitingIncrementalResync = false; state.resyncStickBottom = false;
  resetRenderState();
  document.querySelectorAll(".thread").forEach(e => e.classList.toggle("active", e.dataset.tid===tid));
  renderAllThreadActivityIndicators();
  $("thrHeader").style.display="flex"; $("composer").style.display="flex";
  $("pickerBar").style.display="flex";
  setThreadTitle(title || tid.slice(0,8));
  $("transcript").className=""; $("transcript").innerHTML=""; $("notices").innerHTML="";
  closeDrawers();   // on mobile, reveal the transcript after picking a thread
  restoreComposerDraft();
  if (res.warning) {
    // A read-only open (provider removed from config) shows a persistent banner and unlocks the
    // model picker so the user can rescue the thread by selecting a configured model; other
    // warnings stay transient toasts.
    if (res.warning.code === "thread_read_only") {
      state.threadReadOnly = true;
      state.readOnlyMessage = res.warning.message || "This thread is read-only.";
      updateReadOnlyBanner();
      syncModelOptionAvailability();
      updateComposerControls();
    } else {
      notice(res.warning.message || "warning", res.warning.severity || "warning");
    }
  }
  connectWs();
  schedulePendingApprovalFocus();
}

function clearWsReconnectTimer() {
  if (state.wsReconnectTimer) {
    clearTimeout(state.wsReconnectTimer);
    state.wsReconnectTimer = null;
  }
}
function wsIsOpen() {
  return !!(state.ws && state.ws.readyState === WebSocket.OPEN);
}
function wsStatusLabel(status) {
  switch (status) {
    case "open": return "Connected";
    case "draft": return "Draft";
    case "connecting": return "Connecting";
    case "reconnecting": return "Reconnecting...";
    default: return "Disconnected";
  }
}
function renderWsStatus() {
  const el = $("wsStatusBadge");
  if (!el) return;
  el.hidden = !state.threadId && !isDraftThread();
  el.className = `badge ws-badge state-${state.wsStatus}`;
  el.textContent = wsStatusLabel(state.wsStatus);
  el.title = state.wsStatusDetail || wsStatusLabel(state.wsStatus);
}
function recordWsProblem(message) {
  state.wsLastProblem = message || "";
  if (message) state.wsStatusDetail = message;
  renderWsStatus();
}
function surfaceWsProblem(message, severity) {
  recordWsProblem(message);
  if (!message || document.visibilityState === "hidden") return;
  const now = Date.now();
  if (state.wsLastProblemNotice !== message ||
      now - state.wsLastProblemNoticeAt > WS_PROBLEM_NOTICE_INTERVAL_MS) {
    state.wsLastProblemNotice = message;
    state.wsLastProblemNoticeAt = now;
    notice(message, severity || "warning");
  }
}
function markWsForegroundRecovered(ws) {
  if (!ws || document.visibilityState !== "visible") return;
  ws._giskardBackgroundedAt = 0;
  ws._giskardResumedAt = 0;
}
function scheduleWsReconnect(reason) {
  if (!state.threadId) {
    setWsStatus("closed", "No thread selected.");
    return;
  }
  if (navigator.onLine === false) {
    setWsStatus("reconnecting", "Network is offline. Reconnect will resume when the network returns.");
    surfaceWsProblem("Network is offline. Reconnect will resume when the network returns.", "warning");
    return;
  }
  clearWsReconnectTimer();
  const attempt = state.wsReconnectAttempt++;
  const delay = Math.min(WS_RECONNECT_MAX_MS, WS_RECONNECT_BASE_MS * Math.pow(2, attempt));
  const jitter = Math.floor(Math.random() * 200);
  const message = reason || `Connection lost. Reconnecting in ${Math.ceil((delay + jitter) / 1000)}s.`;
  setWsStatus("reconnecting", message);
  state.wsReconnectTimer = setTimeout(() => {
    state.wsReconnectTimer = null;
    connectWs({ reconnect:true });
  }, delay + jitter);
}
async function connectWs(opts) {
  opts = opts || {};
  if (!state.threadId) {
    setWsStatus("closed", "No thread selected.");
    return;
  }
  clearWsReconnectTimer();
  const oldWs = state.ws;
  state.ws = null;
  if (oldWs) {
    oldWs._giskardExpectedClose = true;
    try { oldWs.close(); } catch {}
  }
  const connectId = ++state.wsConnectId;
  if (!opts.reconnect) {
    state.wsReconnectAttempt = 0;
    state.wsLastProblem = "";
    state.wsLastProblemNotice = "";
    state.wsLastProblemNoticeAt = 0;
  }
  setWsStatus(opts.reconnect ? "reconnecting" : "connecting", opts.reconnect ? "Reconnecting to agent..." : "Connecting to agent...");
  const proto = location.protocol==="https:" ? "wss" : "ws";
  let ticket;
  try {
    ticket = (await api("GET","/api/ws-ticket")).ticket;
  } catch (e) {
    if (connectId !== state.wsConnectId) return;
    const message = "WebSocket authorization failed: "+e.message;
    setWsStatus("reconnecting", message);
    surfaceWsProblem(message, "error");
    scheduleWsReconnect(message);
    return;
  }
  if (connectId !== state.wsConnectId) return;
  const ws = new WebSocket(`${proto}://${location.host}/api/ws?ticket=${encodeURIComponent(ticket)}`);
  state.ws = ws;
  ws._giskardBackgroundedAt = document.visibilityState === "hidden" ? Date.now() : 0;
  ws.onopen = () => {
    if (state.ws !== ws) return;
    state.wsReconnectAttempt = 0;
    state.wsLastProblem = "";
    setWsStatus("open", "Connected to agent.");
    markWsForegroundRecovered(ws);
    // Incremental resync: if we already have persisted history rendered, ask only for the turns
    // after our newest one (`since`). The server replies with a HistoryDelta and we keep the
    // immutable completed-turn DOM, repainting only the in-flight turn. Without a cursor (nothing
    // rendered yet) fall back to a full resync that rewrites the transcript.
    if (state.newestPersistedTurnId) {
      state.awaitingIncrementalResync = true;
      state.awaitingThreadResync = false;
      send({ type:"subscribe", thread_id: state.threadId, since: state.newestPersistedTurnId });
    } else {
      state.awaitingThreadResync = true;
      state.awaitingIncrementalResync = false;
      send({ type:"subscribe", thread_id: state.threadId });
    }
  };
  ws.onmessage = (m) => {
    if (state.ws !== ws) return;
    markWsForegroundRecovered(ws);
    try {
      handleServer(JSON.parse(m.data));
    } catch (e) {
      notice("Invalid WebSocket message from server: "+e.message, "error");
    }
  };
  ws.onerror = () => {
    if (state.ws !== ws) return;
    ws._giskardHadError = true;
    recordWsProblem("WebSocket connection failed. Reconnecting...");
  };
  ws.onclose = (ev) => {
    if (state.ws !== ws) return;
    state.ws = null;
    if (ws._giskardExpectedClose) return;
    const reason = ev.reason ? ` ${ev.reason}` : "";
    const code = ev.code ? ` (${ev.code})` : "";
    const message = ws._giskardHadError
      ? `WebSocket connection failed${code}.${reason} Reconnecting...`
      : `Connection lost${code}.${reason} Reconnecting...`;
    const backgroundedAt = Number(ws._giskardBackgroundedAt) || 0;
    const resumedAt = Number(ws._giskardResumedAt) || 0;
    const recentlyBackgrounded =
      backgroundedAt > 0 &&
      (resumedAt === 0 || Date.now() - resumedAt < WS_BACKGROUND_CLOSE_GRACE_MS);
    const backgrounded = recentlyBackgrounded || document.visibilityState === "hidden";
    const abnormalForegroundClose = ev.code === 1006 || ev.code === 1008 || ev.code === 1011;
    if (!backgrounded && (ws._giskardHadError || abnormalForegroundClose)) {
      surfaceWsProblem(message, "warning");
    }
    scheduleWsReconnect(message);
  };
}
function setWsStatus(status, detail) {
  const nextDetail = detail || wsStatusLabel(status);
  const changed = state.wsStatus !== status || state.wsStatusDetail !== nextDetail;
  state.wsStatus = status;
  state.wsStatusDetail = nextDetail;
  if (changed) recordBrowserDiagnostic("websocket", "ws_status_changed", { status, detail:nextDetail });
  renderWsStatus();
  updateComposerControls();
}
/// Persistent banner above the composer while a thread is read-only; hidden otherwise.
function updateReadOnlyBanner() {
  const banner = $("readOnlyBanner");
  if (!banner) return;
  banner.hidden = !state.threadReadOnly;
  banner.textContent = state.threadReadOnly ? (state.readOnlyMessage || "This thread is read-only.") : "";
}

function updateComposerControls() {
  const ready = state.wsStatus==="open";
  const draft = isDraftThread();
  const hasThreadSurface = !!state.threadId || draft;
  const readOnly = state.threadReadOnly && !draft;
  $("sendBtn").disabled = readOnly || state.activeTurn || (!ready && !draft);
  $("sendBtn").title = readOnly ? "Read-only thread — pick a model from a configured provider to reactivate it." : "";
  $("stopBtn").hidden = !state.activeTurn || draft;
  $("stopBtn").disabled = !ready || state.interruptPending;
  $("stopBtn").textContent = state.interruptPending ? "Stopping…" : "Stop";
  $("modelSel").disabled = !hasThreadSurface || (!ready && !draft);
  $("modelPickerBtn").disabled = !hasThreadSurface || (!ready && !draft);
  $("effortSel").disabled = !hasThreadSurface || (!ready && !draft);
  const compactBtn = $("compactBtn");
  if (compactBtn) {
    compactBtn.disabled = !state.threadId || draft || state.activeTurn || state.compactPending || !ready;
    compactBtn.textContent = state.compactPending ? "Compacting..." : "Compact context";
  }
  $("input").disabled = !hasThreadSurface || readOnly;
  $("input").placeholder =
    readOnly ? "Read-only thread — pick a model above to reactivate it." :
    state.activeTurn ? "Agent is running… draft the next message here." :
    draft ? "Message the agent…  (Enter to send, Shift+Enter for newline)" :
    state.wsStatus==="open" ? "Message the agent…  (Enter to send, Shift+Enter for newline)" :
    state.wsStatus==="connecting" ? "Connecting to agent…" :
    state.wsStatus==="reconnecting" ? "Reconnecting to agent… keep drafting here." :
    "Disconnected from agent.";
  $("approvalSel").disabled = !hasThreadSurface || (!ready && !draft);
  $("modeSel").disabled = !hasThreadSurface || (!ready && !draft);
  $("turnPickerBtn").disabled = !hasThreadSurface || (!ready && !draft);
}
function setTurnActive(active) {
  state.activeTurn = active;
  if (!active) state.interruptPending = false;
  updateComposerControls();
}
function send(obj) {
  if (wsIsOpen()) {
    state.ws.send(JSON.stringify(obj));
    return true;
  }
  return false;
}
function reconnectIfNeeded(reason) {
  if (!state.threadId || wsIsOpen()) return;
  state.wsReconnectAttempt = 0;
  connectWs({ reconnect:true, reason });
}
document.addEventListener("visibilitychange", () => {
  if (document.visibilityState === "hidden") {
    if (state.ws) state.ws._giskardBackgroundedAt = Date.now();
    return;
  }
  if (state.ws && state.ws._giskardBackgroundedAt) {
    state.ws._giskardResumedAt = Date.now();
  }
  reconnectIfNeeded("tab visible");
});
window.addEventListener("online", () => {
  reconnectIfNeeded("network online");
});
window.addEventListener("offline", () => {
  if (!state.threadId || state.wsStatus === "closed") return;
  clearWsReconnectTimer();
  setWsStatus("reconnecting", "Network is offline. Reconnect will resume when the network returns.");
  surfaceWsProblem("Network is offline. Reconnect will resume when the network returns.", "warning");
});
function failPendingUserMessage(text) {
  if (!state.pendingUserEl) return;
  state.pendingUserEl.classList.remove("pending");
  state.pendingUserEl.classList.add("failed");
  state.pendingUserEl = null;
  state.pendingUserText = null;
  if (text) notice(text, "error");
}

function serverMessageThreadId(msg) {
  if (!msg) return null;
  if (msg.thread_id !== undefined && msg.thread_id !== null) return String(msg.thread_id);
  if (msg.error && msg.error.thread_id !== undefined && msg.error.thread_id !== null) {
    return String(msg.error.thread_id);
  }
  if (msg.state && msg.state.thread_id !== undefined && msg.state.thread_id !== null) {
    return String(msg.state.thread_id);
  }
  return null;
}
function isThreadScopedServerMessage(msg) {
  if (!msg) return false;
  switch (msg.type) {
    case "thread_state":
    case "history_page":
    case "history_delta":
    case "live_turn_snapshot":
    case "running_tasks":
    case "event":
    case "approval_request":
    case "approval_resolved":
      return true;
    case "token_update":
      return msg.scope === "thread";
    case "error":
      return serverMessageThreadId(msg) !== null;
    default:
      return false;
  }
}
function isCurrentThreadServerMessage(msg) {
  if (!isThreadScopedServerMessage(msg)) return true;
  const messageThreadId = serverMessageThreadId(msg);
  if (!messageThreadId || !state.threadId) return false;
  return messageThreadId === String(state.threadId);
}

function handleServer(msg) {
  if (msg && msg.type === "thread_activity") {
    handleThreadActivity(msg);
    return;
  }
  if (!isCurrentThreadServerMessage(msg)) return;
  switch (msg.type) {
    case "thread_state": renderThreadState(msg.state); break;
    case "history_page": renderHistoryPage(msg); break;
    case "history_delta": renderHistoryDelta(msg); break;
    case "live_turn_snapshot": renderLiveTurnSnapshot(msg); break;
    case "running_tasks":
      renderRunningCommandSnapshot(msg.tasks || []);
      // Running tasks is the last message of a resync; if the user was pinned to the bottom before
      // the in-flight turn was repainted, restore that now that everything has re-rendered.
      if (state.resyncStickBottom) { state.resyncStickBottom = false; keepTranscriptAtBottom(true); }
      break;
    case "event": handleEvent(msg.agent_event); break;
    case "token_update":
      if (msg.scope === "thread") renderTokens(msg.ledger);
      break;
    case "approval_request":
      handleIncomingApprovalRequest(msg.request, msg.thread_id || state.threadId, {
        source: "server_message_approval_request"
      });
      break;
    case "approval_resolved":
      resolveApprovalRequest(msg.request_id, msg.decision);
      break;
    case "error":
      if (msg.code === "thread_read_only") {
        state.threadReadOnly = true;
        state.readOnlyMessage = msg.message || state.readOnlyMessage || "This thread is read-only.";
        if (!state.readOnlyProvider && state.currentModel) {
          state.readOnlyProvider = state.currentModel.provider;
        }
        updateReadOnlyBanner();
        syncModelOptionAvailability();
        updateComposerControls();
        break;   // the persistent banner replaces the transient toast
      }
      if (msg.action==="select_model") {
        if (state.pendingModelBeforeSelect) {
          state.currentModel = state.pendingModelBeforeSelect;
          state.pendingModelBeforeSelect = null;
          syncModelControls();
        }
      }
      if (msg.action==="send_input" && state.pendingUserEl) {
        failPendingUserMessage(null);
      }
      if (msg.action==="send_input") {
        setTurnActive(msg.code === "thread_turn_active");
      }
      if (msg.action==="interrupt") {
        state.interruptPending = false;
        resetTerminatingToolTasks();
        updateComposerControls();
      }
      if (msg.action==="compact_context") {
        state.compactPending = false;
        updateComposerControls();
      }
      if (msg.action==="terminate_command") resetTerminatingCommand(msg.process_id);
      if (msg.action==="server_request_response") resetResolvingServerRequests();
      notice(msg.message||"error", msg.severity||"error");
      break;
  }
}

function handleThreadActivity(msg) {
  const tid = msg && msg.thread_id !== undefined && msg.thread_id !== null ? String(msg.thread_id) : "";
  if (!tid) return;
  const current = state.threadId && String(state.threadId) === tid;
  const prior = state.threadActivity.get(tid) || {};
  const activity = {
    kind: msg.kind || "progress",
    active_turn: !!msg.active_turn,
    approval_id: msg.approval_id || null,
    server_request_id: msg.server_request_id || null,
    summary: msg.summary || "",
    source: "thread_activity",
    unread: !current
  };
  if (activity.kind === "turn_completed") {
    activity.active_turn = false;
    activity.approval_id = null;
    activity.unread = !current;
  } else if (activity.kind === "approval_requested") {
    activity.unread = !current;
  } else if (!activity.active_turn && prior.unread && activity.kind !== "error") {
    activity.unread = true;
  }
  state.threadActivity.set(tid, activity);
  renderThreadActivityIndicator(tid);
  if (activity.kind === "approval_requested") maybeNotifyApproval(tid, activity);
}

async function maybeNotifyApproval(tid, activity) {
  const notificationKey = approvalNotificationKey(tid, activity && activity.approval_id);
  if (!activity || activity.kind !== "approval_requested" || !notificationKey) {
    recordNotificationDiagnostic("approval_notify_skipped_invalid_call", { tid, activity });
    return;
  }
  recordNotificationDiagnostic("approval_notify_received", {
    tid,
    approval_id: activity.approval_id,
    source: activity.source || "unknown",
    summary: activity.summary || ""
  });
  const focused = document.hasFocus ? document.hasFocus() : true;
  if (document.visibilityState === "visible" && focused && String(tid) === String(state.threadId)) {
    recordNotificationDiagnostic("approval_notify_suppressed_visible_current_thread", {
      tid,
      approval_id: activity.approval_id,
      source: activity.source || "unknown"
    });
    return;
  }
  if (!("Notification" in window)) {
    recordNotificationDiagnostic("approval_notify_suppressed_unsupported", {
      tid,
      approval_id: activity.approval_id,
      source: activity.source || "unknown"
    });
    return;
  }
  if (Notification.permission !== "granted") {
    recordNotificationDiagnostic("approval_notify_suppressed_permission", {
      tid,
      approval_id: activity.approval_id,
      source: activity.source || "unknown"
    });
    maybeNoticeNotificationPermission();
    return;
  }
  const now = Date.now();
  pruneNotificationDedup(now);
  const notifiedAt = state.notifiedApprovals.get(notificationKey);
  if (notifiedAt && now - notifiedAt < NOTIFICATION_DEDUP_MS) {
    recordNotificationDiagnostic("approval_notify_suppressed_duplicate", {
      tid,
      approval_id: activity.approval_id,
      source: activity.source || "unknown",
      age_ms: now - notifiedAt
    });
    return;
  }
  const meta = threadMetaForId(tid);
  const title = meta && meta.title ? meta.title : tid.slice(0,8);
  // Stable per-approval tag: it dedups at the OS level and lets us close the notification by tag on
  // the service-worker path (where we never hold a Notification object) when the approval resolves.
  const notificationTag = approvalNotificationTag(tid, activity.approval_id);
  let result;
  try {
    result = await showAppNotification("Giskard: approval needed", {
      body: activity.summary ? `${title}: ${activity.summary}` : title,
      tag: notificationTag,
      renotify: true,
      requireInteraction: true,
      data: { threadId:tid, approvalId:activity.approval_id }
    }, {
      kind: "approval",
      tid,
      approval_id: activity.approval_id,
      source: activity.source || "unknown",
      tag: notificationTag
    });
  } catch (e) {
    recordNotificationDiagnostic("approval_notify_constructor_failed", {
      tid,
      approval_id: activity.approval_id,
      source: activity.source || "unknown",
      error: e && e.message ? e.message : String(e)
    });
    console.warn("Giskard notification failed", e);
    return;
  }
  if (!result) return;
  state.notifiedApprovals.set(notificationKey, now);
  // Desktop (constructor) notifications are tracked so we can close them on resolution and dispatch
  // their click; service-worker notifications are closed by tag and click via the worker postMessage.
  if (result.via === "constructor" && result.notification) {
    trackApprovalNotification(notificationKey, result.notification);
    result.notification.onclick = () => handleNotificationClick({ threadId: tid, approvalId: activity.approval_id });
  }
  recordNotificationDiagnostic("approval_notify_created", {
    tid,
    approval_id: activity.approval_id,
    source: activity.source || "unknown",
    title,
    tag: notificationTag,
    via: result.via
  });
}

// Show an OS notification, preferring the service worker (the only path that works on Chrome for
// Android) and falling back to the `Notification` constructor on desktop / where no worker is
// active. Returns a descriptor: `{ via: "service_worker", tag }` (clicks arrive via postMessage,
// closing is by tag) or `{ via: "constructor", notification }` (wire onclick / track for closing).
// Rejects if the fallback constructor throws (e.g. on Android with no worker) so callers can log it.
async function showAppNotification(title, options, diagnosticDetail) {
  diagnosticDetail = diagnosticDetail || {};
  const reg = await notificationRegistration();
  if (reg) {
    await reg.showNotification(title, options);
    recordNotificationDiagnostic("browser_notification_created", { ...diagnosticDetail, via: "service_worker" });
    return { via: "service_worker", tag: options && options.tag };
  }
  const notification = new Notification(title, options);
  recordNotificationDiagnostic("browser_notification_created", { ...diagnosticDetail, via: "constructor" });
  notification.onshow = () => recordNotificationDiagnostic("browser_notification_show", diagnosticDetail);
  notification.onerror = () => recordNotificationDiagnostic("browser_notification_error", diagnosticDetail);
  notification.onclose = () => {
    if (diagnosticDetail.kind === "approval") {
      untrackApprovalNotification(
        approvalNotificationKey(diagnosticDetail.tid, diagnosticDetail.approval_id),
        notification
      );
    }
    recordNotificationDiagnostic("browser_notification_close", diagnosticDetail);
  };
  return { via: "constructor", notification };
}

function pruneNotificationDedup(now) {
  now = now || Date.now();
  for (const [key, notifiedAt] of state.notifiedApprovals) {
    if (now - notifiedAt >= NOTIFICATION_DEDUP_MS) state.notifiedApprovals.delete(key);
  }
}

function approvalNotificationKey(tid, approvalId) {
  const threadKey = tid === undefined || tid === null ? "" : String(tid);
  const approvalKey = approvalId === undefined || approvalId === null ? "" : String(approvalId);
  if (!threadKey || !approvalKey) return "";
  return `${threadKey}:${approvalKey}`;
}

// Stable OS notification tag for an approval — used to show, dedup, and (on the service-worker
// path) close the notification without holding a Notification object.
function approvalNotificationTag(tid, approvalId) {
  return `giskard-approval-${tid}-${approvalId}`;
}

function trackApprovalNotification(key, notification) {
  if (!key || !notification) return;
  let notifications = state.approvalNotifications.get(key);
  if (!notifications) {
    notifications = new Set();
    state.approvalNotifications.set(key, notifications);
  }
  notifications.add(notification);
}

function untrackApprovalNotification(key, notification) {
  const notifications = state.approvalNotifications.get(key);
  if (!notifications) return;
  notifications.delete(notification);
  if (notifications.size === 0) state.approvalNotifications.delete(key);
}

function closeApprovalNotification(tid, approvalId) {
  const key = approvalNotificationKey(tid, approvalId);
  if (!key) return;
  // Service-worker notifications aren't held as objects: fetch them back by tag and close them.
  const reg = state.swRegistration;
  if (reg && typeof reg.getNotifications === "function") {
    const tag = approvalNotificationTag(tid, approvalId);
    reg.getNotifications({ tag })
      .then((ns) => ns.forEach((n) => { try { n.close(); } catch {} }))
      .catch(() => {});
  }
  // Desktop (constructor) notifications are tracked objects.
  const notifications = state.approvalNotifications.get(key);
  if (!notifications) return;
  state.approvalNotifications.delete(key);
  for (const notification of notifications) {
    try {
      notification.close();
      recordNotificationDiagnostic("approval_notification_closed", {
        tid,
        approval_id: approvalId
      });
    } catch (e) {
      recordNotificationDiagnostic("approval_notification_close_failed", {
        tid,
        approval_id: approvalId,
        error: e && e.message ? e.message : String(e)
      });
    }
  }
}

function maybeNoticeNotificationPermission() {
  if (Notification.permission !== "default") return;
  const now = Date.now();
  if (now - state.lastNotificationPromptNoticeAt < NOTIFICATION_PROMPT_NOTICE_INTERVAL_MS) return;
  state.lastNotificationPromptNoticeAt = now;
  notice("Enable approval notifications from the sidebar alert button.", "warning");
}

async function focusApprovalTarget(tid, approvalId) {
  window.focus();
  const meta = threadMetaForId(tid);
  if (!meta || !meta.pid) {
    notice("Approval thread is not in the current project list.", "warning");
    return;
  }
  if (String(state.threadId) !== String(tid)) {
    await openThread(meta.pid, tid, meta.title, { focusApprovalId:approvalId });
  } else {
    state.pendingApprovalFocus = {
      threadId:String(tid),
      approvalId:String(approvalId),
      attempts:0
    };
    schedulePendingApprovalFocus();
  }
}

function notifyApprovalRequest(request, tid, opts) {
  opts = opts || {};
  if (!request || !request.id || !tid) {
    recordNotificationDiagnostic("incoming_approval_skipped_invalid_request", {
      tid,
      request_id: request && request.id ? String(request.id) : null
    });
    return;
  }
  maybeNotifyApproval(String(tid), {
    kind:"approval_requested",
    active_turn:true,
    approval_id:String(request.id),
    server_request_id:null,
    summary:approvalTitle(request),
    source:opts.source || "incoming_approval_request",
    unread:false
  });
}

function handleIncomingApprovalRequest(request, tid, opts) {
  opts = opts || {};
  recordNotificationDiagnostic("incoming_approval_request", {
    tid,
    request_id: request && request.id ? String(request.id) : null,
    source: opts.source || "unknown",
    notify: opts.notify !== false
  });
  if (opts.notify !== false) notifyApprovalRequest(request, tid, { source: opts.source });
  setThreadActivity(tid, {
    kind:"approval_requested",
    active_turn:true,
    approval_id:request && request.id ? String(request.id) : null,
    server_request_id:null,
    summary:approvalTitle(request),
    source:opts.source || "incoming_approval_request",
    unread:state.threadId ? String(state.threadId) !== String(tid) : true
  });
  renderApprovalRequest(request);
}

function renderThreadState(s) {
  if (!s) return;
  const shouldResetTranscript = state.awaitingInitialThreadState || state.awaitingThreadResync;
  // An incremental resync keeps the transcript. Remember whether the viewport was pinned to the
  // bottom now, before the in-flight turn is repainted, so we can restore that afterwards.
  if (state.awaitingIncrementalResync) state.resyncStickBottom = transcriptShouldStickToBottom();
  state.awaitingInitialThreadState = false;
  state.awaitingThreadResync = false;
  setMode(s.mode || "build");
  setApprovalPolicy(s.approval_policy || "ask");
  if (s.current_model) {
    state.currentModel = s.current_model;
    state.pendingModelBeforeSelect = null;
    if (state.threadReadOnly) {
      if (!state.readOnlyProvider) {
        state.readOnlyProvider = s.current_model.provider;
      } else if (s.current_model.provider !== state.readOnlyProvider) {
        // The verified cold-resume switch landed: the thread is live again under the new
        // provider, so normal provider-lock rules apply from here on.
        state.threadReadOnly = false;
        state.readOnlyProvider = null;
        state.readOnlyMessage = null;
        updateReadOnlyBanner();
        updateComposerControls();
        notice(`Thread resumed under provider ${s.current_model.provider}.`);
      }
    }
    syncModelControls();
  }
  if (s.title) {
    updateThreadRowTitle(s.id || s.thread_id || state.threadId, s.title);
    setThreadTitle(s.title);
  }
  if (s.tokens) renderTokens(s.tokens);
  updateGauge(state.contextUsed, s.context_window || 0);
  if (shouldResetTranscript) {
    resetTranscriptForAuthoritativeSnapshot();
  }
}
function resetTranscriptForAuthoritativeSnapshot() {
  const keepFirstTurnActive =
    state.firstTurnStartingThreadId &&
    state.threadId &&
    String(state.firstTurnStartingThreadId) === String(state.threadId);
  // Subscribe/resubscribe snapshots replay persisted history and any live turn from the server.
  // Clear transient browser-only rows first so fallback error/user bubbles cannot be appended
  // again, and so a missed turn_completed while suspended does not leave stale active-turn UI.
  $("transcript").innerHTML="";
  state.pendingUserEl = null;
  state.pendingUserText = null;
  state.pendingOlder = false;
  state.loadingHistory = false;
  state.oldestTurnId = null;
  state.hasMoreHistory = false;
  state.autoFilledTurns = 0;
  // The transcript is being rebuilt from an authoritative snapshot, so drop the in-flight stamp;
  // the incoming history page re-establishes the persisted high-water mark.
  state.currentRenderTurnId = null;
  state.interruptPending = false;
  state.compactPending = false;
  resetRenderState();
  if (keepFirstTurnActive) updateComposerControls();
  else setTurnActive(false);
}
const MODE_LABELS = { build:"Build", plan:"Plan" };
const APPROVAL_LABELS = { ask:"Ask first", auto:"Auto approve", read_only:"Read only" };
// Summarise "mode · approvals" on the turn chip below the composer.
function updateTurnButton() {
  const btn = $("turnPickerBtn"); if (!btn) return;
  const mode = MODE_LABELS[state.mode] || "Build";
  const appr = APPROVAL_LABELS[state.approvalPolicy] || "Ask first";
  btn.querySelector(".mp-label").textContent = `${mode} · ${appr}`;
}
function setMode(mode) {
  state.mode = mode === "plan" ? "plan" : "build";
  $("modeSel").value = state.mode;
  updateTurnButton();
}
function setApprovalPolicy(policy) {
  state.approvalPolicy = policy || "ask";
  $("approvalSel").value = state.approvalPolicy;
  updateTurnButton();
}

// Render the most recent page of persisted history (H6), oldest-first. Older pages are available
// via LoadHistory { before: oldestTurnId } when has_more is set (wired to the "Load older" button).
function renderHistoryPage(msg) {
  // A full page arriving while we expected a resync delta means the server couldn't honor our
  // cursor (stale/unknown turn) and fell back to a full snapshot. It is sent history-first, so we
  // still own the transcript here: rebuild it from scratch, then render this as a normal initial
  // page (the live turn appends afterwards).
  if (state.awaitingIncrementalResync) {
    state.awaitingIncrementalResync = false;
    state.resyncStickBottom = false;
    resetTranscriptForAuthoritativeSnapshot();
  }
  // `older` marks a page fetched *above* what's already shown: a scroll-up LoadHistory or an
  // open-time autofill top-up. The first (initial) page is the only one that is not `older`.
  const older = state.pendingOlder;
  state.pendingOlder = false;
  state.loadingHistory = false;
  state.hasMoreHistory = !!msg.has_more;
  const turns = msg.turns || [];
  if (turns.length) state.oldestTurnId = turns[0].id;   // turns are oldest-first
  // High-water cursor: the initial page ends at the newest persisted turn. Older pages are, by
  // definition, further back, so they must never lower this.
  if (!older && turns.length) state.newestPersistedTurnId = turns[turns.length - 1].id;
  state.autoFilledTurns = (state.autoFilledTurns || 0) + turns.length;
  // The gauge tracks current context occupancy. When a live turn is active it (and the thread_state
  // aggregates) own the gauge, so a staler value from the newest persisted turn must not clobber it.
  if (!older && !state.activeTurn) updateGaugeFromTurns(turns);

  // Every page renders into a detached container and is prepended above existing content, so the
  // live turn (rendered first, at the bottom) and any already-loaded history stay in place.
  const container = document.createElement("div");
  const prev = state.renderTarget;
  const prevTaskGroup = state.activeTaskGroup;
  state.renderTarget = container;
  state.activeTaskGroup = null;
  for (const turn of turns) renderPersistedTurn(turn);
  state.renderTarget = prev;
  state.activeTaskGroup = prevTaskGroup;

  const t = $("transcript");
  const heightBefore = t.scrollHeight;
  const anchor = t.firstChild;   // insert before current top-most content (or append if empty)
  while (container.firstChild) t.insertBefore(container.firstChild, anchor);
  if (older) {
    // Preserve the viewport so it doesn't jump while older content is inserted (infinite scroll).
    t.scrollTop += t.scrollHeight - heightBefore;
  } else {
    // Initial page: reveal the newest content (the live turn, or the last persisted turn).
    t.scrollTop = t.scrollHeight;
  }

  maybeAutoFillHistory();
}

// Incremental resync: the server sent only the turns that completed while we were disconnected
// (history-first, before the live snapshot). Completed turns are immutable, so we keep the existing
// transcript, repaint just the in-flight turn, and append these new turns at the bottom.
function renderHistoryDelta(msg) {
  state.awaitingIncrementalResync = false;
  const turns = msg.turns || [];

  // Repaint the in-flight turn: remove the rows of the turn that was still running when we
  // disconnected (and any optimistic pre-turn rows). The live snapshot that follows re-renders its
  // current state; a turn that completed while we were away arrives below as a delta turn instead.
  reconcileInFlightTurn();

  // Append the completed-since turns at the bottom — they are newer than everything we kept, and no
  // live turn is in the DOM yet (it arrives next).
  if (turns.length) {
    const container = document.createElement("div");
    const prev = state.renderTarget;
    const prevTaskGroup = state.activeTaskGroup;
    state.renderTarget = container;
    state.activeTaskGroup = null;
    for (const turn of turns) renderPersistedTurn(turn);
    state.renderTarget = prev;
    state.activeTaskGroup = prevTaskGroup;
    const t = $("transcript");
    while (container.firstChild) t.appendChild(container.firstChild);
    state.newestPersistedTurnId = turns[turns.length - 1].id;   // advance the resume cursor
    updateGaugeFromTurns(turns);   // a live snapshot, if any, overrides this next
  }

  // If the user was pinned to the bottom before the repaint, keep them there. When the turn is
  // still running the live snapshot re-applies this after it renders; for an idle thread (no live
  // snapshot) this is the final position.
  if (state.resyncStickBottom) keepTranscriptAtBottom(true);
}

// Remove the in-flight turn's DOM on an incremental resync. Completed turns are immutable and stay
// put; only the turn that was running when we disconnected can have changed, and the optimistic
// "pending" rows (a user bubble sent but never confirmed) are transient. Matching is by the
// per-turn `data-turn` stamp; removing a task-group wrapper takes its nested rows with it.
function reconcileInFlightTurn() {
  removeTurnRows("pending");
  const liveId = state.activeTurn && state.currentRenderTurnId != null
    ? String(state.currentRenderTurnId)
    : null;
  if (liveId) removeTurnRows(liveId);
  // Drop streaming bookkeeping tied to the wiped rows so the live snapshot rebuilds cleanly; the
  // snapshot re-activates the turn if it is still running.
  state.currentRenderTurnId = null;
  setTurnActive(false);
  state.streamEl = null;
  state.streamItemId = null;
  state.streamElsByItemId = new Map();
  breakTaskGroup();
}
function removeTurnRows(turnId) {
  const t = $("transcript");
  if (!t) return;
  // Snapshot into an array first: removing a task-group wrapper also detaches its nested `.msg`
  // children, and calling remove() on an already-detached node is a harmless no-op.
  for (const el of Array.from(t.querySelectorAll(".msg"))) {
    if (el.dataset.turn === turnId) el.remove();
  }
}

// After each history page lands, keep topping up (oldest-first, in small batches) until the
// transcript holds ~HISTORY_FILL_SCREENS viewports of scrollback, we run out of history, or we hit
// the safety cap. This reuses the scroll-up LoadHistory path, so pages arrive as `older` and are
// prepended without moving the viewport. Measuring pixels here is deliberate: only the browser
// knows how tall rendered turns are, so the server cannot page by screen.
function maybeAutoFillHistory() {
  if (state.renderTarget) return;   // never while rendering into a detached container
  const t = $("transcript");
  if (!t || !state.threadId) return;
  if (!state.hasMoreHistory || state.loadingHistory || !state.oldestTurnId) return;
  if ((state.autoFilledTurns || 0) >= HISTORY_FILL_MAX_TURNS) return;
  if (t.scrollHeight >= t.clientHeight * HISTORY_FILL_SCREENS) return;
  state.loadingHistory = true;
  state.pendingOlder = true;
  if (!send({ type:"load_history", thread_id: state.threadId, before: state.oldestTurnId, limit: HISTORY_FILL_BATCH })) {
    state.loadingHistory = false;
    state.pendingOlder = false;
  }
}

// Render one persisted turn from history: its items, plus the user message and any failure that
// aren't captured as items. A turn that failed before producing output (e.g. a quota rejection)
// has no user_message item, so we render `user_input` directly; its `status.message` then explains
// why the message got no agent response — the record that a transient toast used to lose.
function renderPersistedTurn(turn) {
  breakTaskGroup();
  // Stamp this turn's rows with its id while rendering. Save/restore so rendering an older page
  // (prepended above) can't leave a stale id set for whatever renders next.
  const prevRenderTurnId = state.currentRenderTurnId;
  state.currentRenderTurnId = turn.id;
  const items = turn.items || [];
  const hasUserItem = items.some(it => ((it.payload||it).kind) === "user_message");
  const inputText = turn.user_input && turn.user_input.text;
  if (!hasUserItem && inputText) {
    renderItemBody(bubble("user","you"), { kind:"user_message", text: inputText });
  }
  for (const it of items) addItem(it, true);
  const st = turn.status;
  if (st && (st.kind==="failed" || st.kind==="interrupted")) {
    errorBubble(st.message || (st.kind==="interrupted" ? "Turn interrupted." : "Turn failed."));
  }
  state.currentRenderTurnId = prevRenderTurnId;
  breakTaskGroup();
}

// Load older history when the user scrolls near the top (H4/H6 infinite scroll).
function onTranscriptScroll() {
  const t = $("transcript");
  if (t.scrollTop < 80 && state.hasMoreHistory && !state.loadingHistory && state.oldestTurnId && state.threadId) {
    state.loadingHistory = true;
    state.pendingOlder = true;
    send({ type:"load_history", thread_id: state.threadId, before: state.oldestTurnId });
  }
}

function handleEvent(ev) {
  switch (ev.kind) {
    case "turn_started":
      state.firstTurnStartingThreadId = null;
      breakTaskGroup();
      state.streamEl = null;
      state.streamItemId = null;
      state.streamElsByItemId.clear();
      state.itemKindsByItemId.clear();
      clearPlanCard();   // a new turn starts a fresh plan
      // The live turn id equals its eventual persisted id, so adopt it now for row stamping and
      // upgrade any optimistic "pending" rows (the user bubble sent before the turn started).
      state.currentRenderTurnId = ev.turn;
      if (ev.turn) {
        document.querySelectorAll('.msg[data-turn="pending"]').forEach(m => { m.dataset.turn = ev.turn; });
      }
      setTurnActive(true);
      setActiveThreadActivity("progress", true, "Turn running");
      break;
    case "item_started":
      if (ev.item) {
        state.itemKindsByItemId.set(idKey(ev.item.id), ev.item.kind);
        if (ev.item.kind==="command_execution" && ev.item.command) {
          startRunningCommand(ev.item, ev.turn);
        } else if (ev.item.kind==="tool_call" && ev.item.tool) {
          startToolCall(ev.item, ev.turn);
        }
      }
      break;
    case "item_delta":
      if (ev.delta && ev.delta.type==="text") {
        const kind = state.itemKindsByItemId.get(idKey(ev.item_id));
        if (kind==="tool_call") appendToolProgress(ev.item_id, ev.delta.text);
        else appendStream(ev.delta.text, ev.item_id, ev.delta.type);
      }
      else if (ev.delta && ev.delta.type==="command_output") {
        if (!appendRunningCommandOutput(ev.item_id, ev.delta.chunk)) {
          appendStream(ev.delta.chunk, ev.item_id, ev.delta.type);
        }
      }
      break;
    case "item_completed":
      if (!finalizeStreamedItem(ev.item)) addItem(ev.item);
      if (isContextCompactionItem(ev.item)) finishCompactPending();
      break;
    case "turn_completed":
      state.firstTurnStartingThreadId = null;
      // This turn is now persisted; advance the high-water cursor and stop stamping rows to it.
      if (ev.turn) state.newestPersistedTurnId = ev.turn;
      state.currentRenderTurnId = null;
      updateGaugeFromUsage(ev.usage);
      state.streamEl=null;
      state.streamItemId=null;
      state.streamElsByItemId.clear();
      state.itemKindsByItemId.clear();
      detachRunningCommands();
      finishCompactPending();
      clearPlanCard();   // the plan ends with its turn
      setTurnActive(false);
      setActiveThreadActivity("turn_completed", false, "Turn completed");
      breakTaskGroup();
      break;
    case "approval_requested":
      handleIncomingApprovalRequest(ev.request, ev.thread || state.threadId, {
        source: "agent_event_approval_requested"
      });
      break;
    case "server_request_received":
      setActiveThreadActivity("server_request_received", true, "Waiting for input", {
        server_request_id:ev.request && ev.request.id ? String(ev.request.id) : null
      });
      renderServerRequest(ev.request);
      break;
    case "server_request_resolved": resolveServerRequest(ev.request_id); break;
    // Render errors as a persistent transcript entry (tied to the turn/message that caused them)
    // rather than a toast that vanishes — so looking back at a thread explains why a message got
    // no agent response. The matching failed turn is also persisted server-side (§7.1).
    case "error":
      if (state.firstTurnStartingThreadId) {
        state.firstTurnStartingThreadId = null;
        setTurnActive(false);
      }
      setActiveThreadActivity("error", false, errorText(ev.error));
      failPendingUserMessage(null);   // resolve the optimistic bubble to a failed state
      errorBubble(errorText(ev.error));
      break;
    // A non-fatal advisory: show it as a warning, and do NOT fail the pending message — otherwise
    // the optimistic user bubble is cleared early and the real user_message item renders a duplicate.
    case "notice":
      noticeBubble(ev.message || "");
      break;
  }
}

// A persistent, transcript-anchored error entry (respects renderTarget for history prepends).
function errorBubble(message) {
  bubble("error","error").textContent = message || "error";
}
// A non-alarming, transcript-anchored warning entry.
function noticeBubble(message) {
  if (!message) return;
  bubble("notice","warning").textContent = message;
}
function errorText(e) {
  if (!e) return "error";
  if (typeof e === "string") return e;
  return e.message || e.detail || JSON.stringify(e);
}

function renderLiveTurnSnapshot(snap) {
  if (snap && snap.turn_id) {
    state.firstTurnStartingThreadId = null;
    // Adopt the live turn id so its rows stamp correctly even if the accumulated events don't lead
    // with a turn_started (the turn_started handler will confirm the same id).
    state.currentRenderTurnId = snap.turn_id;
    setTurnActive(true);
    setActiveThreadActivity("progress", true, "Turn running");
  }
  for (const ev of (snap.accumulated||[])) handleEvent(ev);
  if (snap.pending_approval) {
    handleIncomingApprovalRequest(snap.pending_approval, snap.thread_id || state.threadId, {
      source: "live_turn_snapshot_pending_approval"
    });
  }
  for (const request of (snap.pending_server_requests || [])) {
    setActiveThreadActivity("server_request_received", true, "Waiting for input", {
      server_request_id:request && request.id ? String(request.id) : null
    });
    renderServerRequest(request);
  }
}

function renderApprovalRequest(request) {
  if (!request || !request.id) return;
  const id = String(request.id);
  const stateKey = approvalStateKey(request);
  if (state.pendingApprovals.has(id) || state.renderedApprovalStateKeys.has(stateKey)) return;
  const answered = state.answeredApprovals.get(stateKey);
  const body = bubble("approval","approval");
  const msg = body.parentElement;
  msg.dataset.approvalId = id;
  msg.dataset.approvalStateKey = stateKey;
  if (state.threadId) msg.dataset.threadId = state.threadId;
  state.renderedApprovalStateKeys.add(stateKey);
  if (!answered) state.pendingApprovals.set(id, { msg, request, stateKey });

  const title = document.createElement("div");
  title.className = "approval-title";
  title.textContent = approvalTitle(request);
  body.append(title);

  const reason = (request.reason || "").trim();
  if (reason) {
    const reasonEl = document.createElement("div");
    reasonEl.textContent = reason;
    body.append(reasonEl);
  }

  const detail = approvalDetail(request);
  if (detail) {
    const detailEl = document.createElement("div");
    detailEl.className = "approval-detail";
    detailEl.textContent = detail;
    body.append(detailEl);
  }
  renderApprovalMetadata(body, request.metadata || []);

  if (answered) {
    applyApprovalDecision(msg, answered.decision);
    return;
  }

  const actions = document.createElement("div");
  actions.className = "approval-actions";
  const available = new Set(request.available || []);
  addApprovalButton(actions, id, "accept", "Accept", "primary", available);
  addApprovalButton(actions, id, "accept_for_session", "Session", "session", available);
  addApprovalButton(actions, id, "decline", "Decline", "danger", available);
  addApprovalButton(actions, id, "cancel", "Cancel", "", available);
  body.append(actions);
  schedulePendingApprovalFocus();
}

function approvalRowById(id) {
  if (!id) return null;
  const target = String(id);
  return Array.from(document.querySelectorAll("[data-approval-id]"))
    .find(el => String(el.dataset.approvalId) === target) || null;
}

function schedulePendingApprovalFocus() {
  const pending = state.pendingApprovalFocus;
  if (!pending || !pending.approvalId) return;
  if (!state.threadId || String(state.threadId) !== String(pending.threadId)) return;
  const row = approvalRowById(pending.approvalId);
  if (row) {
    state.pendingApprovalFocus = null;
    row.scrollIntoView({ block:"center", behavior:"smooth" });
    row.classList.add("approval-target");
    row.setAttribute("tabindex", "-1");
    row.focus({ preventScroll:true });
    setTimeout(() => row.classList.remove("approval-target"), 5000);
    return;
  }
  pending.attempts = (pending.attempts || 0) + 1;
  if (pending.attempts > 40) {
    state.pendingApprovalFocus = null;
    notice("Approval is no longer pending.", "warning");
    return;
  }
  setTimeout(schedulePendingApprovalFocus, 150);
}

function approvalTitle(request) {
  const kind = request.kind || {};
  if (kind.kind==="command_execution") return "Run command?";
  if (kind.kind==="file_change") return "Apply file changes?";
  if (kind.kind==="permission") return "Grant permissions?";
  if (kind.kind==="mcp_tool_call") return "Run MCP tool?";
  return "Approval required";
}
function approvalDetail(request) {
  const kind = request.kind || {};
  if (kind.kind==="command_execution") {
    return kind.command || "(empty command)";
  }
  if (kind.kind==="file_change") return [kind.change, kind.path].filter(Boolean).join(" ");
  if (kind.kind==="permission") return kind.detail || "";
  if (kind.kind==="mcp_tool_call") {
    const server = kind.server ? `${kind.server}:` : "";
    return `${server}${kind.tool_name || ""}`;
  }
  return hasMeaningfulJson(kind) ? JSON.stringify(kind) : "";
}
function renderApprovalMetadata(body, metadata) {
  if (!Array.isArray(metadata) || !metadata.length) return;
  const list = document.createElement("div");
  list.className = "approval-metadata";
  let added = false;
  for (const item of metadata) {
    const row = approvalMetadataRow(item || {});
    if (!row) continue;
    list.append(row);
    added = true;
  }
  if (added) body.append(list);
}
function approvalMetadataRow(item) {
  const labelText = item.label || approvalMetadataDefaultLabel(item.kind);
  const value = approvalMetadataValue(item);
  if (!value) return null;
  const row = document.createElement("div");
  row.className = "approval-meta-row";
  const label = document.createElement("div");
  label.className = "approval-meta-label";
  label.textContent = labelText;
  const body = document.createElement("div");
  body.className = "approval-meta-value";
  if (item.kind === "path" && item.source_link) body.append(makePathLink(item.path || "", value, null));
  else body.textContent = value;
  row.append(label, body);
  return row;
}
function approvalMetadataDefaultLabel(kind) {
  if (kind === "path") return "Path";
  if (kind === "host") return "Host";
  return "Detail";
}
function approvalMetadataValue(item) {
  if (item.kind === "path") return String(item.path || "");
  if (item.kind === "host") return approvalHostValue(item);
  if (item.kind === "text") return String(item.value || "");
  return "";
}
function approvalHostValue(item) {
  const host = String(item.host || "");
  if (!host) return "";
  let value = "";
  if (item.protocol) value += `${item.protocol}://`;
  value += host;
  if (item.port !== undefined && item.port !== null) value += `:${item.port}`;
  if (item.target) value += ` (${item.target})`;
  return value;
}
function addApprovalButton(container, id, decision, label, cls, available) {
  if (available.size && !available.has(decision)) return;
  const btn = document.createElement("button");
  btn.type = "button";
  if (cls) btn.className = cls;
  btn.textContent = label;
  btn.onclick = () => respondApproval(id, decision);
  container.append(btn);
}
function respondApproval(id, decision) {
  const entry = state.pendingApprovals.get(id);
  if (!entry) return;
  const msg = entry.msg;
  msg.querySelectorAll("button").forEach(btn => btn.disabled = true);
  if (!send({ type:"approval_decision", request_id:id, decision })) {
    msg.querySelectorAll("button").forEach(btn => btn.disabled = false);
    notice(`Approval response not sent: WebSocket is ${state.wsStatus}.`, "error");
    return;
  }
  resolveApprovalRequest(id, decision);
}
function resolveApprovalRequest(id, decision) {
  if (id === undefined || id === null || String(id) === "") return;
  id = String(id);
  const entry = state.pendingApprovals.get(id);
  const msg = entry ? entry.msg : approvalRowById(id);
  const tid = entry && entry.request && entry.request.thread_id
    ? entry.request.thread_id
    : (msg && msg.dataset.threadId ? msg.dataset.threadId : state.threadId);
  closeApprovalNotification(tid, id);
  clearApprovalThreadActivity(tid, id);
  if (entry) {
    state.answeredApprovals.set(entry.stateKey || (msg && msg.dataset.approvalStateKey) || approvalStateKey(id), {
      request: entry.request,
      decision
    });
  }
  state.pendingApprovals.delete(id);
  if (msg) applyApprovalDecision(msg, decision);
}
function approvalStateKey(requestOrId) {
  if (requestOrId && typeof requestOrId === "object") {
    return [
      state.threadId || "",
      String(requestOrId.id || ""),
      JSON.stringify(requestOrId.kind || {}),
      String(requestOrId.reason || ""),
      JSON.stringify(requestOrId.metadata || [])
    ].join("\n");
  }
  return `${state.threadId || ""}\n${String(requestOrId || "")}`;
}
function applyApprovalDecision(msg, decision) {
  if (!msg) return;
  msg.classList.add("resolved");
  msg.classList.remove("decision-accept", "decision-session", "decision-decline", "decision-cancel");
  msg.classList.add(approvalDecisionClass(decision));
  msg.querySelectorAll(".approval-actions").forEach(el => el.remove());
  const title = msg.querySelector(".approval-title");
  if (title && !title.dataset.baseTitle) title.dataset.baseTitle = title.textContent || "";
  if (title) title.textContent = `${approvalDecisionLabel(decision)}: ${title.dataset.baseTitle || "Approval"}`;
  const body = msg.querySelector(".body");
  if (!body) return;
  let status = body.querySelector(".approval-result");
  if (!status) {
    status = document.createElement("div");
    status.className = "approval-result";
    body.append(status);
  }
  status.textContent = `Decision: ${approvalDecisionLabel(decision)}`;
}
function approvalDecisionClass(decision) {
  if (decision==="accept") return "decision-accept";
  if (decision==="accept_for_session") return "decision-session";
  if (decision==="decline") return "decision-decline";
  if (decision==="cancel") return "decision-cancel";
  return "decision-cancel";
}
function approvalDecisionLabel(decision) {
  if (decision==="accept_for_session") return "Session";
  return decision.charAt(0).toUpperCase() + decision.slice(1);
}
function renderServerRequest(request) {
  if (!request || !request.id) return;
  const id = String(request.id);
  if (state.pendingServerRequests.has(id)) return;
  const body = bubble("server-request","request");
  const msg = body.parentElement;
  msg.dataset.serverRequestId = id;
  if (state.threadId) msg.dataset.threadId = state.threadId;
  state.pendingServerRequests.set(id, { msg, request });

  const title = document.createElement("div");
  title.className = "server-request-title";
  title.textContent = serverRequestTitle(request);
  body.append(title);

  const prompt = serverRequestPrompt(request);
  if (prompt) {
    const promptEl = document.createElement("div");
    promptEl.textContent = prompt;
    body.append(promptEl);
  }

  const detail = serverRequestDetail(request);
  if (detail) {
    const detailEl = document.createElement("div");
    detailEl.className = "server-request-detail";
    detailEl.textContent = detail;
    body.append(detailEl);
  }

  const method = String(request.method || "");
  if (method === "item/tool/requestUserInput") renderToolUserInputRequest(body, id, request);
  else if (method === "mcpServer/elicitation/request") renderMcpElicitationRequest(body, id, request);
  else if (method === "item/tool/call") renderDynamicToolCallRequest(body, id, request);
  else if (method === "account/chatgptAuthTokens/refresh") {
    renderUnsupportedServerRequest(body, id, request, "Giskard cannot refresh ChatGPT auth tokens.");
  }
  else if (method === "attestation/generate") {
    renderUnsupportedServerRequest(body, id, request, "Giskard cannot generate client attestation tokens.");
  }
  else renderUnknownServerRequest(body, id, request);
}
function resolveServerRequest(id) {
  id = String(id || "");
  const entry = state.pendingServerRequests.get(id);
  if (!entry) return;
  const tid = entry.msg && entry.msg.dataset.threadId ? entry.msg.dataset.threadId : state.threadId;
  clearServerRequestThreadActivity(tid, id);
  entry.msg.classList.add("resolved");
  entry.msg.querySelectorAll("button,input,select,textarea").forEach(el => el.disabled = true);
  const body = entry.msg.querySelector(".body");
  if (body && !entry.msg.dataset.resolvedLabel) {
    const status = document.createElement("div");
    status.className = "meta";
    status.textContent = "Resolved";
    body.append(status);
  }
  state.pendingServerRequests.delete(id);
}
function resetResolvingServerRequests() {
  for (const { msg } of state.pendingServerRequests.values()) {
    if (msg.dataset.resolving !== "true") continue;
    msg.dataset.resolving = "false";
    msg.querySelectorAll("button,input,select,textarea").forEach(el => el.disabled = false);
    const status = msg.querySelector(".server-request-sent");
    if (status) status.remove();
  }
}
function serverRequestTitle(request) {
  const method = String(request.method || "");
  if (method === "item/tool/call") return "Tool call needs a browser response";
  if (method === "item/tool/requestUserInput") return "Agent needs your answer";
  if (method === "mcpServer/elicitation/request") return "MCP server needs input";
  return "Codex server request";
}
function serverRequestPrompt(request) {
  const p = objectValue(request.params);
  return stringValue(p.message) || stringValue(p.reason) || stringValue(p.prompt) || "";
}
function serverRequestDetail(request) {
  const method = String(request.method || "");
  const p = objectValue(request.params);
  if (method === "item/tool/call") {
    const ns = stringValue(p.namespace);
    const name = stringValue(p.tool) || "tool";
    return `${ns ? ns + ":" : ""}${name}`;
  }
  if (method === "item/tool/requestUserInput") {
    const n = Array.isArray(p.questions) ? p.questions.length : 0;
    return n ? `${n} question${n===1 ? "" : "s"}` : "";
  }
  if (method === "mcpServer/elicitation/request") return stringValue(p.url) || stringValue(p.serverName);
  return method;
}
function renderDynamicToolCallRequest(body, id, request) {
  const p = objectValue(request.params);
  appendJsonPreviewIfMeaningful(body, p.arguments);
  const actions = serverRequestActions();
  addServerRequestButton(actions, id, "Fail Tool Call", "danger", () => ({
    kind:"result",
    value:{
      success:false,
      contentItems:[{ type:"inputText", text:"Tool call rejected from Giskard." }]
    }
  }));
  addServerRequestButton(actions, id, "Success Empty", "", () => ({
    kind:"result",
    value:{ success:true, contentItems:[] }
  }));
  body.append(actions);
}
function renderToolUserInputRequest(body, id, request) {
  const p = objectValue(request.params);
  const questions = Array.isArray(p.questions) ? p.questions.map(objectValue).filter(Boolean) : [];
  const fields = document.createElement("div");
  fields.className = "server-request-fields";
  for (const q of questions) fields.append(toolQuestionField(q));
  if (questions.length) body.append(fields);
  const actions = serverRequestActions();
  addServerRequestButton(actions, id, "Continue", "primary", () => ({
    kind:"result",
    value:{ answers: collectToolQuestionAnswers(fields) }
  }));
  addServerRequestButton(actions, id, "Cancel", "", () => ({
    kind:"error",
    code:-32000,
    message:"User input request cancelled."
  }));
  body.append(actions);
}
function toolQuestionField(q) {
  const field = document.createElement("div");
  field.className = "server-request-field server-request-question";
  field.dataset.questionId = stringValue(q.id);
  const label = document.createElement("label");
  label.textContent = stringValue(q.header) || stringValue(q.question) || stringValue(q.id) || "Question";
  field.append(label);
  const prompt = stringValue(q.question);
  if (prompt && prompt !== label.textContent) {
    const hint = document.createElement("div");
    hint.className = "meta";
    hint.textContent = prompt;
    field.append(hint);
  }
  const options = Array.isArray(q.options) ? q.options.map(objectValue).filter(Boolean) : [];
  if (options.length) {
    const select = document.createElement("select");
    select.className = "server-request-answer";
    for (const option of options) {
      const opt = document.createElement("option");
      opt.value = stringValue(option.label);
      opt.textContent = stringValue(option.label);
      select.append(opt);
    }
    if (q.isOther === true) {
      const opt = document.createElement("option");
      opt.value = "__other__";
      opt.textContent = "Other";
      select.append(opt);
    }
    field.append(select);
    const desc = document.createElement("div");
    desc.className = "meta";
    const updateDesc = () => {
      const chosen = options.find(option => stringValue(option.label) === select.value);
      desc.textContent = chosen ? stringValue(chosen.description) : "";
    };
    select.onchange = updateDesc;
    updateDesc();
    field.append(desc);
    if (q.isOther === true) {
      const other = document.createElement("input");
      other.className = "server-request-other";
      other.placeholder = "Other answer";
      field.append(other);
    }
  } else {
    const input = document.createElement("input");
    input.className = "server-request-answer";
    input.type = q.isSecret === true ? "password" : "text";
    field.append(input);
  }
  return field;
}
function collectToolQuestionAnswers(fields) {
  const result = {};
  fields.querySelectorAll(".server-request-question").forEach(field => {
    const id = field.dataset.questionId || "";
    if (!id) return;
    const answerEl = field.querySelector(".server-request-answer");
    const otherEl = field.querySelector(".server-request-other");
    let value = answerEl ? answerEl.value : "";
    if (value === "__other__") value = otherEl ? otherEl.value : "";
    result[id] = { answers: value ? [value] : [] };
  });
  return result;
}
function renderMcpElicitationRequest(body, id, request) {
  const p = objectValue(request.params);
  const url = safeHttpUrl(stringValue(p.url));
  if (url) {
    const a = document.createElement("a");
    a.href = url;
    a.target = "_blank";
    a.rel = "noopener noreferrer";
    a.textContent = url;
    body.append(a);
  }
  const fields = renderMcpSchemaFields(body, p.requestedSchema);
  const actions = serverRequestActions();
  addServerRequestButton(actions, id, "Continue", "primary", () => ({
    kind:"result",
    value:{ action:"accept", content: collectMcpElicitationContent(fields) }
  }));
  addServerRequestButton(actions, id, "Decline", "danger", () => ({
    kind:"result",
    value:{ action:"decline" }
  }));
  addServerRequestButton(actions, id, "Cancel", "", () => ({
    kind:"result",
    value:{ action:"cancel" }
  }));
  body.append(actions);
}
function renderMcpSchemaFields(body, schemaValue) {
  const schema = objectValue(schemaValue);
  const properties = objectValue(schema.properties);
  if (!properties || !Object.keys(properties).length) {
    return null;
  }
  const fields = document.createElement("div");
  fields.className = "server-request-fields";
  for (const [key, raw] of Object.entries(properties)) {
    const prop = objectValue(raw) || {};
    const field = document.createElement("div");
    field.className = "server-request-field server-request-mcp-field";
    field.dataset.fieldKey = key;
    field.dataset.fieldType = stringValue(prop.type) || "string";
    const label = document.createElement("label");
    label.textContent = stringValue(prop.title) || key;
    field.append(label);
    let input;
    if (prop.type === "boolean") {
      input = document.createElement("input");
      input.type = "checkbox";
    } else if (prop.enum && Array.isArray(prop.enum)) {
      input = document.createElement("select");
      for (const value of prop.enum) {
        const opt = document.createElement("option");
        opt.value = String(value);
        opt.textContent = String(value);
        input.append(opt);
      }
    } else {
      input = document.createElement("input");
      input.type = prop.type === "number" || prop.type === "integer" ? "number" : "text";
    }
    input.className = "server-request-mcp-value";
    field.append(input);
    if (prop.description) {
      const desc = document.createElement("div");
      desc.className = "meta";
      desc.textContent = stringValue(prop.description);
      field.append(desc);
    }
    fields.append(field);
  }
  body.append(fields);
  return fields;
}
function collectMcpElicitationContent(fields) {
  if (!fields) return {};
  const textarea = fields.querySelector(".server-request-json-content");
  if (textarea) {
    try { return JSON.parse(textarea.value || "{}"); }
    catch (e) {
      notice("MCP content JSON is invalid: "+e.message, "error");
      throw e;
    }
  }
  const content = {};
  fields.querySelectorAll(".server-request-mcp-field").forEach(field => {
    const key = field.dataset.fieldKey || "";
    if (!key) return;
    const type = field.dataset.fieldType || "string";
    const input = field.querySelector(".server-request-mcp-value");
    if (!input) return;
    if (input.type === "checkbox") content[key] = input.checked;
    else if (type === "number" || type === "integer") {
      const n = Number(input.value);
      content[key] = Number.isFinite(n) ? n : null;
    } else content[key] = input.value;
  });
  return content;
}
function renderUnknownServerRequest(body, id, request) {
  appendJsonPreviewIfMeaningful(body, request.params);
  const actions = serverRequestActions();
  addServerRequestButton(actions, id, "Return Empty Result", "primary", () => ({
    kind:"result",
    value:{}
  }));
  addServerRequestButton(actions, id, "Reject", "danger", () => ({
    kind:"error",
    code:-32000,
    message:`Giskard rejected server request ${request.method || ""}.`
  }));
  body.append(actions);
}
function renderUnsupportedServerRequest(body, id, request, message) {
  appendJsonPreviewIfMeaningful(body, request.params);
  const actions = serverRequestActions();
  addServerRequestButton(actions, id, "Report Unsupported", "danger", () => ({
    kind:"error",
    code:-32000,
    message:message || `Giskard does not support server request ${request.method || ""}.`
  }));
  body.append(actions);
}
function serverRequestActions() {
  const actions = document.createElement("div");
  actions.className = "server-request-actions";
  return actions;
}
function addServerRequestButton(container, id, label, cls, buildResponse) {
  const btn = document.createElement("button");
  btn.type = "button";
  if (cls) btn.className = cls;
  btn.textContent = label;
  btn.onclick = () => {
    let response;
    try { response = buildResponse(); }
    catch { return; }
    respondServerRequest(id, response, label);
  };
  container.append(btn);
}
function respondServerRequest(id, response, label) {
  const entry = state.pendingServerRequests.get(String(id));
  if (!entry) return;
  entry.msg.dataset.resolving = "true";
  entry.msg.querySelectorAll("button,input,select,textarea").forEach(el => el.disabled = true);
  if (!send({ type:"server_request_response", request_id:String(id), response })) {
    entry.msg.dataset.resolving = "false";
    entry.msg.querySelectorAll("button,input,select,textarea").forEach(el => el.disabled = false);
    notice(`Server request response not sent: WebSocket is ${state.wsStatus}.`, "error");
    return;
  }
  const body = entry.msg.querySelector(".body");
  const status = document.createElement("div");
  status.className = "meta server-request-sent";
  status.textContent = `Sent: ${label}`;
  if (body) body.append(status);
}
function objectValue(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : null;
}
function stringValue(value) {
  return typeof value === "string" ? value : "";
}
function safeHttpUrl(value) {
  if (!value) return "";
  try {
    const parsed = new URL(value);
    const protocol = parsed.protocol.toLowerCase();
    return protocol === "http:" || protocol === "https:" ? parsed.toString() : "";
  } catch { return ""; }
}

// Where new bubbles are appended: the live transcript, or a detached container when rendering an
// older history page for prepending (infinite scroll).
function renderTarget() { return state.renderTarget || $("transcript"); }
function transcriptShouldStickToBottom() {
  if (state.renderTarget) return false;
  const t = $("transcript");
  return t ? (t.scrollHeight - t.scrollTop - t.clientHeight) <= TRANSCRIPT_BOTTOM_STICKY_PX : false;
}
function scrollTranscriptToBottom() {
  if (state.renderTarget) return;
  const t = $("transcript");
  if (t) t.scrollTop = t.scrollHeight;
}
function keepTranscriptAtBottom(shouldStick) {
  if (!shouldStick || state.renderTarget) return;
  scrollTranscriptToBottom();
  requestAnimationFrame(scrollTranscriptToBottom);
}
function keepTranscriptRowAnchored(el) {
  const msg = el && el.closest ? el.closest(".msg") : null;
  keepTranscriptAtBottom(!!(msg && msg.dataset.followBottom === "true"));
}
function appendBubble(cls, role) {
  const followBottom = transcriptShouldStickToBottom();
  const el = document.createElement("div"); el.className="msg "+cls;
  // Tag every transcript row with the turn it belongs to. Persisted and live turns supply a real id
  // via `currentRenderTurnId`; optimistic rows created before `turn_started` (the pending user
  // bubble) are marked "pending" and upgraded to the real id when the turn actually starts. This is
  // the sole creation site for top-level rows, so this one stamp covers messages, task-group
  // wrappers, and command/tool bubbles; nested task-detail panels ride along inside their wrapper.
  el.dataset.turn = state.currentRenderTurnId || "pending";
  const r = document.createElement("div"); r.className="role"; r.textContent=role;
  const body = document.createElement("div"); body.className="body";
  el.append(r, body);
  // The task-group container is a wrapper, not a message; its child rows get their own buttons.
  if (!cls.includes("task-group")) attachRowCopy(el);
  if (followBottom) el.dataset.followBottom = "true";
  const t = renderTarget();
  t.append(el);
  keepTranscriptAtBottom(followBottom);
  return body;
}
// Give a transcript row a small copy button. It copies the row's raw source when we have it
// (`dataset.copyText`, set for Markdown messages so they paste back as Markdown), otherwise the
// rendered text. On touch devices the button is revealed by tapping the row (see revealRowCopy).
function attachRowCopy(el) {
  const btn = document.createElement("button");
  btn.type = "button";
  btn.className = "row-copy";
  btn.textContent = "Copy";
  btn.title = "Copy this message";
  btn.setAttribute("aria-label", "Copy this message");
  let resetTimer = 0;
  btn.onclick = async (e) => {
    e.stopPropagation();
    const raw = el.dataset.copyText != null
      ? el.dataset.copyText
      : (el.querySelector(".body") ? el.querySelector(".body").textContent : "");
    const ok = await copyToClipboard(raw);
    btn.textContent = ok ? "Copied" : "Failed";
    btn.classList.toggle("ok", ok);
    btn.classList.toggle("err", !ok);
    clearTimeout(resetTimer);
    resetTimer = setTimeout(() => { btn.textContent = "Copy"; btn.classList.remove("ok", "err"); }, 1500);
  };
  el.append(btn);
  // Touch reveal: a tap on the row (not on a link/button/other control) shows this row's button.
  el.addEventListener("click", (e) => {
    if (e.target.closest("button, a, input, select, textarea")) return;
    revealRowCopy(el);
  });
}
function revealRowCopy(el) {
  document.querySelectorAll(".msg.copy-revealed").forEach(m => { if (m !== el) m.classList.remove("copy-revealed"); });
  el.classList.toggle("copy-revealed");
}
// A tap away from any row dismisses the revealed copy button on touch devices.
document.addEventListener("click", (e) => {
  if (e.target.closest(".msg")) return;
  document.querySelectorAll(".msg.copy-revealed").forEach(m => m.classList.remove("copy-revealed"));
});
function bubble(cls, role) {
  breakTaskGroup();
  return appendBubble(cls, role);
}
function isTaskPayloadKind(kind) {
  return kind==="command_execution" || kind==="tool_call";
}
function breakTaskGroup() {
  state.activeTaskGroup = null;
}
function currentTaskGroup() {
  const target = renderTarget();
  const active = state.activeTaskGroup;
  if (active && active.target === target && active.el.parentElement === target) return active;
  return createTaskGroup(target);
}
function createTaskGroup(target) {
  const groupId = "task-group-" + (++state.taskGroupSeq);
  const body = appendBubble("tasks task-group state-running expanded", "tasks");
  body.classList.add("task-group-body");
  const el = body.parentElement;
  el.dataset.taskGroupId = groupId;

  const head = document.createElement("div");
  head.className = "task-group-head";
  head.tabIndex = 0;
  head.setAttribute("role", "button");
  head.title = "Expand or collapse all task details";
  head.setAttribute("aria-label", "Expand or collapse all task details");
  const caret = document.createElement("span");
  caret.className = "task-group-caret";
  const title = document.createElement("div");
  title.className = "task-group-title";
  const status = document.createElement("div");
  status.className = "task-group-status";
  head.append(caret, title, status);
  const list = document.createElement("div");
  list.className = "task-group-list";
  body.append(head, list);

  const group = {
    id:groupId, target, el, body, head, caret, title, status, list,
    items:new Map(), itemOrder:[]
  };
  state.taskGroupsById.set(groupId, group);
  state.activeTaskGroup = group;
  state.expandedTaskGroups.add(groupId);
  head.onclick = (e) => {
    if (e.defaultPrevented || e.target.closest("button,a,input,select,textarea")) return;
    toggleTaskGroup(groupId);
  };
  head.onkeydown = (e) => {
    if (e.key === "Enter" || e.key === " ") {
      e.preventDefault();
      toggleTaskGroup(groupId);
    }
  };
  syncTaskGroupState(group);
  return group;
}
function taskBubble(itemId, kind, cls, role) {
  let key = idKey(itemId);
  if (!key) key = "anonymous-task-" + (++state.taskItemSeq);
  const existingGroup = state.taskGroupsByItemId.get(key);
  const existing = existingGroup && existingGroup.items.get(key);
  if (existing) return existing.body;

  const group = currentTaskGroup();
  const entry = document.createElement("div");
  entry.className = "task-group-entry";
  const row = document.createElement("div");
  row.className = "task-group-item state-running";
  row.tabIndex = 0;
  row.setAttribute("role", "button");
  const symbol = document.createElement("span");
  symbol.className = "task-group-item-symbol";
  const title = document.createElement("span");
  title.className = "task-group-item-title mono";
  const status = document.createElement("span");
  status.className = "task-group-item-status";
  row.append(symbol, title, status);
  row.onclick = (e) => { e.stopPropagation(); selectTaskGroupItem(group.id, key); };
  row.onkeydown = (e) => {
    if (e.key === "Enter" || e.key === " ") {
      e.preventDefault();
      selectTaskGroupItem(group.id, key);
    }
  };

  const msg = document.createElement("div");
  msg.className = "msg " + cls;
  msg.hidden = true;
  if (kind==="tool_call") msg.dataset.toolItemId = key;
  else msg.dataset.commandItemId = key;
  const roleEl = document.createElement("div");
  roleEl.className = "role";
  roleEl.textContent = role;
  const body = document.createElement("div");
  body.className = "body";
  msg.append(roleEl, body);

  entry.append(row, msg);
  const task = { id:key, kind, entry, row, symbol, title, status, msg, body };
  group.items.set(key, task);
  group.itemOrder.push(key);
  state.taskGroupsByItemId.set(key, group);
  group.list.append(entry);
  syncTaskGroupItem(key);
  return body;
}
function selectTaskGroupItem(groupId, itemId) {
  const key = idKey(itemId);
  const group = state.taskGroupsById.get(groupId);
  if (!group || !key) return;
  state.manuallyToggledTaskGroups.add(groupId);
  state.expandedTaskGroups.add(groupId);
  const detailIds = expandedTaskDetailIds(groupId);
  if (detailIds.has(key)) {
    detailIds.delete(key);
    if (state.selectedCommandId === key) state.selectedCommandId = null;
    clearTaskSelection();
    syncTaskGroupState(group);
    renderRunningCommands();
    return;
  }
  detailIds.add(key);
  state.selectedCommandId = key;
  clearTaskSelection();
  const msg = state.commandMsgElsByItemId.get(key);
  if (msg) msg.classList.add("selected");
  syncTaskGroupState(group);
  syncTaskGroupItem(key);
  renderRunningCommands();
}
function expandedTaskDetailIds(groupId) {
  let ids = state.expandedTaskDetails.get(groupId);
  if (!(ids instanceof Set)) {
    ids = new Set(ids ? [ids] : []);
    state.expandedTaskDetails.set(groupId, ids);
  }
  return ids;
}
function toggleTaskGroup(groupId) {
  const group = state.taskGroupsById.get(groupId);
  if (!group) return;
  state.manuallyToggledTaskGroups.add(groupId);
  state.expandedTaskGroups.add(groupId);
  const detailIds = expandedTaskDetailIds(groupId);
  const itemIds = group.itemOrder.slice();
  const allExpanded = itemIds.length > 0 && itemIds.every(id => detailIds.has(id));
  detailIds.clear();
  if (!allExpanded) {
    for (const id of itemIds) detailIds.add(id);
  } else if (itemIds.includes(state.selectedCommandId)) {
    state.selectedCommandId = null;
    clearTaskSelection();
  }
  syncTaskGroupState(group);
}
function taskVisualStateFromElement(msg) {
  if (!msg) return "running";
  if (msg.classList.contains("state-failed")) return "failed";
  if (msg.classList.contains("state-terminated")) return "terminated";
  if (msg.classList.contains("state-succeeded")) return "succeeded";
  return "running";
}
function taskStatusText(msg) {
  const text = msg && (msg.querySelector(".cmd-status span:last-child") || msg.querySelector(".cmd-status"));
  return text ? text.textContent.trim() : "";
}
function syncTaskGroupItem(itemId) {
  const key = idKey(itemId);
  const group = state.taskGroupsByItemId.get(key);
  const task = group && group.items.get(key);
  if (!task) return;
  const stateName = taskVisualStateFromElement(task.msg);
  task.row.className = `task-group-item state-${stateName}` + (state.selectedCommandId===key ? " selected" : "");
  task.symbol.className = `task-group-item-symbol cmd-symbol state-${stateName}`;
  task.symbol.textContent = commandStateSymbol(stateName);
  const title = task.msg.querySelector(".cmd-title");
  task.title.textContent = title ? title.textContent.trim() : (task.kind==="tool_call" ? "tool" : "command");
  task.status.textContent = taskStatusText(task.msg);
  syncTaskGroupState(group);
}
function syncTaskGroupState(group) {
  if (!group) return;
  const items = group.itemOrder.map(id => group.items.get(id)).filter(Boolean);
  const count = items.length;
  const runningCount = items.filter(item => taskVisualStateFromElement(item.msg)==="running").length;
  const failedCount = items.filter(item => taskVisualStateFromElement(item.msg)==="failed").length;
  const terminatedCount = items.filter(item => taskVisualStateFromElement(item.msg)==="terminated").length;
  const commandCount = items.filter(item => item.kind !== "tool_call").length;
  const toolCount = items.filter(item => item.kind === "tool_call").length;
  const allTerminal = count > 0 && runningCount === 0;
  if (!state.manuallyToggledTaskGroups.has(group.id)) {
    if (allTerminal) state.expandedTaskGroups.delete(group.id);
    else state.expandedTaskGroups.add(group.id);
  }
  const stateName = runningCount ? "running" : failedCount ? "failed" : terminatedCount ? "terminated" : "succeeded";
  const expanded = state.expandedTaskGroups.has(group.id);
  group.el.classList.remove("state-running", "state-succeeded", "state-failed", "state-terminated", "expanded", "collapsed");
  group.el.classList.add(`state-${stateName}`, expanded ? "expanded" : "collapsed");
  group.caret.textContent = expanded ? "▾" : "▸";
  group.head.setAttribute("aria-expanded", expanded ? "true" : "false");
  group.title.textContent = `${count} task${count === 1 ? "" : "s"} · ${commandCount} command${commandCount === 1 ? "" : "s"} · ${toolCount} tool${toolCount === 1 ? "" : "s"}`;
  const statusLabel = runningCount ? `${runningCount} running` :
    failedCount ? `${failedCount} failed` :
    terminatedCount ? `${terminatedCount} terminated` : "succeeded";
  group.status.replaceChildren(commandStatusNode(statusLabel, stateName));
  group.list.hidden = !expanded;
  const detailIds = expandedTaskDetailIds(group.id);
  for (const item of items) {
    item.msg.hidden = !(expanded && detailIds.has(item.id));
    item.row.classList.toggle("selected", state.selectedCommandId===item.id);
    item.entry.classList.toggle("expanded", expanded && detailIds.has(item.id));
  }
}
function removeTaskGroupItem(itemId) {
  const key = idKey(itemId);
  const group = state.taskGroupsByItemId.get(key);
  const task = group && group.items.get(key);
  if (!task) return false;
  task.entry.remove();
  group.items.delete(key);
  group.itemOrder = group.itemOrder.filter(id => id !== key);
  state.taskGroupsByItemId.delete(key);
  state.commandBodyElsByItemId.delete(key);
  state.commandMsgElsByItemId.delete(key);
  state.toolBodyElsByItemId.delete(key);
  const detailIds = expandedTaskDetailIds(group.id);
  detailIds.delete(key);
  if (!group.itemOrder.length) {
    group.el.remove();
    state.taskGroupsById.delete(group.id);
    state.expandedTaskGroups.delete(group.id);
    state.manuallyToggledTaskGroups.delete(group.id);
    state.expandedTaskDetails.delete(group.id);
    if (state.activeTaskGroup === group) state.activeTaskGroup = null;
  } else {
    syncTaskGroupState(group);
  }
  return true;
}
function startRunningCommand(item, turnId) {
  const key = idKey(item.id); if (!key) return;
  const command = item.command || {};
  const existing = state.runningCommands.get(key);
  const cmd = commandFromParts({
    id:key,
    turnId:idKey(turnId),
    harnessItemId:item.harness_item_id || "",
    command:command.command || "",
    cwd:command.cwd || "",
    status:command.status || "in_progress",
    processId:command.process_id || "",
    startedAtMs:normalizeTimestampMs(command.started_at_ms, existing ? existing.startedAtMs : Date.now()),
    output:existing ? existing.output : "",
    afterTurn:existing ? existing.afterTurn : false,
    terminating:existing ? existing.terminating : false
  });
  state.runningCommands.set(key, cmd);
  let body = commandBodyFor(key);
  if (!body) body = taskBubble(key, "command_execution", "cmd running-command", "command");
  state.streamElsByItemId.set(key, body);
  state.commandBodyElsByItemId.set(key, body);
  state.commandMsgElsByItemId.set(key, body.parentElement);
  renderCommandBody(body, cmd);
  renderRunningCommands();
}
function commandFromParts(parts) {
  return {
    id:parts.id,
    kind:parts.kind === "tool" ? "tool" : "command",
    turnId:parts.turnId || "",
    harnessItemId:parts.harnessItemId || "",
    command:parts.command || "",
    cwd:parts.cwd || "",
    server:parts.server || "",
    status:parts.status || "in_progress",
    processId:parts.processId || "",
    startedAtMs:normalizeTimestampMs(parts.startedAtMs, Date.now()),
    output:parts.output || "",
    afterTurn:!!parts.afterTurn,
    terminating:!!parts.terminating
  };
}
// Right-panel label for a running task: a shell command shows "$ cmd"; a tool shows "server:tool".
function taskTitleText(cmd) {
  if (cmd.kind === "tool") return (cmd.server ? cmd.server + ":" : "") + (cmd.command || "tool");
  return "$ " + (cmd.command || "(command)");
}
function commandFromItem(item, p, existing) {
  return commandFromParts({
    id:idKey(item && item.id),
    turnId:existing ? existing.turnId : "",
    harnessItemId:(item && item.harness_item_id) || (existing && existing.harnessItemId) || "",
    command:p.command || "",
    cwd:p.cwd || "",
    status:p.status || "in_progress",
    processId:p.process_id || (existing && existing.processId) || "",
    startedAtMs:existing ? existing.startedAtMs : Date.now(),
    output:p.output || (existing && existing.output) || "",
    afterTurn:existing ? existing.afterTurn : false,
    terminating:existing ? existing.terminating : false
  });
}
function commandBodyFor(id) {
  return state.commandBodyElsByItemId.get(id) || state.streamElsByItemId.get(id);
}
function toolBodyFor(id) {
  return state.toolBodyElsByItemId.get(id) || state.streamElsByItemId.get(id);
}
function commandOutputStats(output) {
  const text = String(output || "");
  let lineCount = text ? 1 : 0;
  for (let i = 0; i < text.length; i++) {
    if (text.charCodeAt(i) === 10 && i < text.length - 1) lineCount++;
  }
  let bytes = text.length;
  try { bytes = new TextEncoder().encode(text).length; } catch {}
  return { chars:text.length, bytes, lineCount };
}
function commandOutputStatsLabel(stats, phase) {
  if (!stats.chars) return phase === "running" ? "No output yet" : "No output";
  const lineWord = stats.lineCount === 1 ? "line" : "lines";
  return `${stats.lineCount.toLocaleString()} ${lineWord} · ${formatBytes(stats.bytes)}`;
}
function commandOutputShouldAutoCollapse(stats) {
  return stats.lineCount > COMMAND_AUTO_COLLAPSE_LINES ||
    stats.bytes > COMMAND_AUTO_COLLAPSE_BYTES;
}
function commandOutputPhaseForId(id) {
  const cmd = state.runningCommands.get(id);
  if (cmd && commandIsRunningStatus(cmd.status)) return "running";
  return "completed";
}
function commandOutputForId(id) {
  const cmd = state.runningCommands.get(id);
  if (cmd) return cmd.output || "";
  const ended = state.endedCommandsByItemId.get(id);
  if (ended && ended.command) return ended.command.output || "";
  const payload = state.commandPayloadsByItemId.get(id);
  return payload ? payload.output || "" : "";
}
function isCommandOutputExpanded(itemId, phase, output, stats) {
  const key = idKey(itemId);
  if (key && state.manuallyToggledCommandOutputs.has(key)) {
    return state.expandedCommandOutputs.has(key);
  }
  if (phase === "completed") return false;
  return !commandOutputShouldAutoCollapse(stats || commandOutputStats(output));
}
function makeCommandHead() {
  const head = document.createElement("div");
  head.className = "cmd-head";
  return { head };
}
function wireCommandRowToggle(msg, itemId, expanded) {
  const key = idKey(itemId);
  if (!key) return;
  msg.classList.add("toggleable");
  msg.title = expanded ? "Collapse command output" : "Show command output";
  msg.tabIndex = 0;
  msg.setAttribute("role", "button");
  msg.setAttribute("aria-expanded", expanded ? "true" : "false");
  msg.onclick = (e) => {
    if (e.defaultPrevented || e.target.closest("button,a,input,select,textarea")) return;
    toggleCommandOutput(key);
  };
  msg.onkeydown = (e) => {
    if (e.key === "Enter" || e.key === " ") {
      e.preventDefault();
      toggleCommandOutput(key);
    }
  };
}
function toggleCommandOutput(itemId) {
  const key = idKey(itemId);
  if (!key) return;
  const phase = commandOutputPhaseForId(key);
  const output = commandOutputForId(key);
  const expanded = isCommandOutputExpanded(key, phase, output);
  state.manuallyToggledCommandOutputs.add(key);
  if (expanded) state.expandedCommandOutputs.delete(key);
  else state.expandedCommandOutputs.add(key);
  rerenderCommandRow(key);
}
function rerenderCommandRow(id) {
  const body = commandBodyFor(id);
  if (!body) return;
  const cmd = state.runningCommands.get(id);
  if (cmd) { renderCommandBody(body, cmd); return; }
  const ended = state.endedCommandsByItemId.get(id);
  if (ended) { renderEndedCommandBody(body, ended.command, ended.status, ended.opts); return; }
  const payload = state.commandPayloadsByItemId.get(id);
  if (payload) renderItemBody(body, payload);
}
function rerenderToolRow(id) {
  const body = toolBodyFor(id);
  const payload = state.toolPayloadsByItemId.get(id);
  if (body && payload) renderItemBody(body, payload);
}
function clearRowToggle(msg) {
  msg.classList.remove("toggleable", "collapsed", "expanded");
  msg.removeAttribute("title");
  msg.removeAttribute("tabindex");
  msg.removeAttribute("role");
  msg.removeAttribute("aria-expanded");
  msg.onclick = null;
  msg.onkeydown = null;
}
function renderCommandOutputBlock(body, opts) {
  const itemId = idKey(opts.itemId);
  const phase = opts.phase || "completed";
  const output = String(opts.output || "");
  const stats = commandOutputStats(output);
  const expanded = isCommandOutputExpanded(itemId, phase, output, stats);
  const msg = body.parentElement;
  msg.classList.toggle("collapsed", !expanded);
  msg.classList.toggle("expanded", expanded);
  wireCommandRowToggle(msg, itemId, expanded);

  const summary = document.createElement("div");
  summary.className = "meta cmd-output-summary";
  const label = commandOutputStatsLabel(stats, phase);
  summary.textContent = !stats.chars ? label :
    expanded ? `Output · ${label}` : `Output collapsed · ${label}`;
  body.append(summary);

  if (!expanded || !stats.chars) return;
  const out = document.createElement("pre");
  out.className = "out";
  body.append(out);
  if (opts.linkify) renderLinkedText(out, output);
  else out.textContent = output;
}
function renderCommandBody(body, cmd) {
  const msg = body.parentElement;
  const stateName = commandVisualStateFromCommand(cmd);
  msg.className = `msg cmd running-command state-${stateName}`;
  if (state.selectedCommandId === cmd.id) msg.classList.add("selected");
  msg.dataset.commandItemId = cmd.id;
  msg.dataset.commandStartedAtMs = String(cmd.startedAtMs || Date.now());
  body.replaceChildren();

  const { head } = makeCommandHead();
  const title = document.createElement("div");
  title.className = "cmd-title mono";
  title.textContent = "$ " + (cmd.command || "(command)");
  const status = commandStatusNode(commandStatusLabel(cmd), stateName);
  const actions = document.createElement("div");
  actions.className = "cmd-actions";
  const term = document.createElement("button");
  term.className = "danger";
  term.textContent = cmd.terminating ? "Stop requested" : "Stop";
  term.disabled = cmd.terminating || !cmd.processId;
  term.title = cmd.processId ? "Ask Codex to stop this running command" : "No process id available";
  term.onclick = (e) => { e.stopPropagation(); terminateCommand(cmd.id); };
  actions.append(term);
  head.append(title, status, actions);
  body.append(head);

  if (cmd.cwd) {
    const cwd = document.createElement("div");
    cwd.className = "meta mono";
    cwd.textContent = cmd.cwd;
    body.append(cwd);
  }
  renderCommandOutputBlock(body, { itemId:cmd.id, output:cmd.output || "", phase:"running" });
  syncTaskGroupItem(cmd.id);
}
function commandStatusLabel(cmd) {
  const elapsed = formatDuration(Date.now() - (cmd.startedAtMs || Date.now()));
  if (cmd.terminating) return `stop requested after ${elapsed}`;
  if (cmd.afterTurn) return `still running for ${elapsed}`;
  return commandIsRunningStatus(cmd.status) ? `running for ${elapsed}` : (cmd.status || "running");
}
function commandVisualStateFromCommand(cmd) {
  if (!cmd) return "running";
  if (commandIsRunningStatus(cmd.status)) return "running";
  return commandVisualStateFromStatus(cmd.status);
}
function commandVisualStateFromStatus(status) {
  const s = commandStatusKey(status);
  if (s==="completed" || s==="succeeded" || s==="success") return "succeeded";
  if (s==="failed" || s==="error") return "failed";
  if (s==="terminated" || s==="declined" || s==="canceled" || s==="cancelled" || s==="interrupted" || s==="unknown") return "terminated";
  if (commandIsRunningStatus(status)) return "running";
  return s ? "failed" : "running";
}
function commandStateSymbol(stateName) {
  if (stateName==="succeeded") return "✓";
  if (stateName==="failed") return "✕";
  if (stateName==="terminated") return "■";
  return "●";
}
function commandStatusNode(label, stateName) {
  const status = document.createElement("span");
  const visualState = stateName || "running";
  status.className = `cmd-status state-${visualState}`;
  const symbol = document.createElement("span");
  symbol.className = `cmd-symbol state-${visualState}`;
  symbol.textContent = commandStateSymbol(visualState);
  const text = document.createElement("span");
  text.textContent = label || "";
  status.append(symbol, text);
  return status;
}
function appendCommandMetaPart(meta, part) {
  if (meta.childNodes.length) meta.append(document.createTextNode(" · "));
  if (part instanceof Node) meta.append(part);
  else meta.append(document.createTextNode(part));
}
function commandStatusKey(status) {
  return String(status || "").toLowerCase().replace(/-/g, "_");
}
function commandIsRunningStatus(status) {
  const s = commandStatusKey(status);
  return s==="in_progress" || s==="inprogress" || s==="running";
}
function normalizeTimestampMs(value, fallback) {
  const n = Number(value);
  return Number.isFinite(n) && n > 0 ? n : fallback;
}
function formatDuration(ms) {
  const total = Math.max(0, Math.round(Number(ms || 0) / 1000));
  const hours = Math.floor(total / 3600);
  const minutes = Math.floor((total % 3600) / 60);
  const seconds = total % 60;
  const parts = [];
  if (hours) parts.push(`${hours} ${hours===1 ? "hour" : "hours"}`);
  if (minutes) parts.push(`${minutes} ${minutes===1 ? "minute" : "minutes"}`);
  if (!hours && (!minutes || seconds)) parts.push(`${seconds} ${seconds===1 ? "second" : "seconds"}`);
  return parts.join(" ");
}
function terminalCommandStatus(status, durationMs, opts) {
  const s = commandStatusKey(status);
  const label = s==="completed" ? "Succeeded" :
    s==="failed" ? "Failed" :
    s==="declined" ? "Declined" :
    s==="interrupted" ? "Interrupted" :
    s==="terminated" ? "Terminated" :
    s==="unknown" ? "No longer tracked" :
    status ? String(status) : "Finished";
  const text = durationMs === null || durationMs === undefined ? label : `${label} after ${formatDuration(durationMs)}`;
  return opts && opts.stopRequested ? `${text} (stop requested)` : text;
}
function updateRunningCommandDurations() {
  if (!state.runningCommands.size) return;
  for (const cmd of state.runningCommands.values()) {
    if (cmd.kind === "tool") {
      const body = toolBodyFor(cmd.id);
      const payload = state.toolPayloadsByItemId.get(cmd.id);
      if (body && payload && commandIsRunningStatus(payload.status)) renderItemBody(body, payload);
      continue;
    }
    const body = commandBodyFor(cmd.id);
    if (body) renderCommandBody(body, cmd);
  }
  renderRunningCommands();
}
function appendRunningCommandOutput(itemId, chunk) {
  const key = idKey(itemId);
  const cmd = state.runningCommands.get(key);
  if (!cmd) return false;
  cmd.output = (cmd.output || "") + (chunk || "");
  if (cmd.output.length > 8000) cmd.output = cmd.output.slice(-8000);
  const body = commandBodyFor(key);
  if (body) renderCommandBody(body, cmd);
  renderRunningCommands();
  return true;
}
function detachRunningCommands() {
  for (const cmd of state.runningCommands.values()) {
    if (commandIsRunningStatus(cmd.status)) {
      cmd.afterTurn = true;
    }
  }
  renderRunningCommands();
}
function finishRunningCommand(item) {
  const key = idKey(item && item.id);
  if (!key) return;
  const p = item && item.payload;
  if (p && p.kind==="command_execution" && commandIsRunningStatus(p.status)) {
    const cmd = commandFromItem(item, p, state.runningCommands.get(key));
    state.runningCommands.set(key, cmd);
    state.endedCommandsByItemId.delete(key);
    let body = commandBodyFor(key);
    if (body) {
      state.commandBodyElsByItemId.set(key, body);
      state.commandMsgElsByItemId.set(key, body.parentElement);
      renderCommandBody(body, cmd);
    }
  } else {
    state.runningCommands.delete(key);
    state.endedCommandsByItemId.delete(key);
  }
  renderRunningCommands();
}
function renderRunningCommandSnapshot(commands) {
  const seen = new Set();
  for (const info of commands) {
    const key = idKey(info.item_id);
    if (!key) continue;
    seen.add(key);
    const existing = state.runningCommands.get(key);
    const cmd = commandFromParts({
      id:key,
      kind:info.kind,
      turnId:idKey(info.turn_id),
      harnessItemId:info.harness_item_id || "",
      command:info.command || "",
      cwd:info.cwd || "",
      server:info.server || "",
      status:info.status || "in_progress",
      processId:info.process_id || "",
      startedAtMs:normalizeTimestampMs(info.started_at_ms, existing ? existing.startedAtMs : Date.now()),
      output:info.output || "",
      afterTurn:!!info.after_turn,
      terminating:info.terminating !== undefined ? !!info.terminating : !!(existing && existing.terminating)
    });
    if (cmd.terminating) state.commandStopRequestedByItemId.add(key);
    state.runningCommands.set(key, cmd);
    // Snapshots can arrive before replayed live items, so both task kinds can create transcript
    // rows here. Later item events reuse the same body by item id and finalize it in place.
    if (cmd.kind === "tool") {
      let toolBody = toolBodyFor(key);
      if (!toolBody) {
        toolBody = taskBubble(key, "tool_call", "tool running-tool state-running", "tool");
        state.streamElsByItemId.set(key, toolBody);
        state.toolBodyElsByItemId.set(key, toolBody);
        toolBody.parentElement.dataset.toolItemId = key;
        toolBody.parentElement.dataset.toolStartedAtMs = String(cmd.startedAtMs || Date.now());
        renderItemBody(toolBody, {
          kind:"tool_call",
          name:cmd.command || "tool",
          input:null,
          output:cmd.output || null,
          server:cmd.server || null,
          status:cmd.status || "in_progress",
          error:null
        });
      }
      state.commandMsgElsByItemId.set(key, toolBody.parentElement);
    } else {
      let body = commandBodyFor(key);
      if (!body) body = taskBubble(key, "command_execution", "cmd running-command", "command");
      state.commandBodyElsByItemId.set(key, body);
      state.commandMsgElsByItemId.set(key, body.parentElement);
      renderCommandBody(body, cmd);
    }
  }

  for (const [id, cmd] of Array.from(state.runningCommands.entries())) {
    if (seen.has(id)) continue;
    state.runningCommands.delete(id);
    // Tool transcript rows are owned by the item stream; only command rows get the ended-body
    // rewrite when a stop was requested.
    if (cmd.kind !== "tool") {
      const body = commandBodyFor(id);
      const stopRequested = cmd.terminating || state.commandStopRequestedByItemId.has(id);
      if (body && stopRequested) {
        renderEndedCommandBody(body, cmd, "unknown", { stopRequested:true });
      }
    }
    state.commandStopRequestedByItemId.delete(id);
  }
  renderRunningCommands();
}
function renderEndedCommandBody(body, cmd, status, opts) {
  state.endedCommandsByItemId.set(cmd.id, { command:cmd, status, opts:opts || {} });
  const msg = body.parentElement;
  const stateName = commandVisualStateFromStatus(status);
  msg.className = `msg cmd state-${stateName}`;
  if (state.selectedCommandId === cmd.id) msg.classList.add("selected");
  msg.dataset.commandItemId = cmd.id;
  msg.dataset.commandStartedAtMs = String(cmd.startedAtMs || Date.now());
  body.replaceChildren();
  const { head } = makeCommandHead();
  const title = document.createElement("div");
  title.className = "cmd-title mono";
  title.textContent = "$ " + (cmd.command || "(command)");
  const meta = document.createElement("div");
  meta.className = "meta cmd-meta";
  const durationMs = Date.now() - (cmd.startedAtMs || Date.now());
  if (cmd.cwd) appendCommandMetaPart(meta, `cwd: ${cmd.cwd}`);
  if (status) {
    appendCommandMetaPart(meta, commandStatusNode(terminalCommandStatus(status, durationMs, opts), stateName));
  }
  head.append(title);
  body.append(head);
  if (meta.childNodes.length) body.append(meta);
  renderCommandOutputBlock(body, { itemId:cmd.id, output:cmd.output || "", phase:"completed" });
  syncTaskGroupItem(cmd.id);
}
function renderRunningCommands() {
  const cmds = Array.from(state.runningCommands.values());
  renderTasksButton(cmds);
  if (!$("tasksMenu").hidden) renderTasksMenu(cmds);
}
function renderTasksButton(cmds) {
  cmds = cmds || Array.from(state.runningCommands.values());
  const btn = $("tasksBtn");
  const count = cmds.length;
  const stateName = taskButtonState(cmds);
  btn.className = `badge tasks-btn state-${stateName}`;
  btn.disabled = !state.threadId;
  btn.title = count ? `${count} running task${count === 1 ? "" : "s"}` : "No running tasks";
  $("tasksCount").textContent = String(count);
}
function taskButtonState(cmds) {
  if (!cmds.length) return "idle";
  if (cmds.some(cmd => cmd.terminating)) return "stopping";
  return "running";
}
function renderTasksMenu(cmds) {
  cmds = cmds || Array.from(state.runningCommands.values());
  const menu = $("tasksMenu");
  const count = cmds.length;
  const commandTasks = cmds.filter(cmd => cmd.kind !== "tool");
  const toolTasks = cmds.filter(cmd => cmd.kind === "tool");
  const summaryHtml = count
    ? `<div class="tasks-summary">${count} running task${count === 1 ? "" : "s"} · ${commandTasks.length} commands · ${toolTasks.length} tools</div>`
    : "";
  const sectionsHtml = count ? `
    <div class="tasks-section">
      <div class="tasks-section-title">Commands</div>
      <div id="tasksCommandList"></div>
    </div>
    <div class="tasks-section">
      <div class="tasks-section-title">Tools</div>
      <div id="tasksToolList"></div>
    </div>` : `<div id="tasksList"></div>`;
  menu.innerHTML = `
    <div class="tasks-head">
      <strong>Tasks</strong>
      <button id="tasksClose" type="button">Close</button>
    </div>
    ${summaryHtml}
    ${sectionsHtml}`;
  $("tasksClose").onclick = () => { $("tasksMenu").hidden = true; };
  if (count) {
    renderTaskCards($("tasksCommandList"), commandTasks, "No running commands.");
    renderTaskCards($("tasksToolList"), toolTasks, "No running tools.");
  } else {
    renderTaskCards($("tasksList"), cmds, "No running tasks.");
  }
}
function renderTaskCards(box, cmds, emptyText) {
  if (!cmds.length) {
    box.className = "muted";
    box.textContent = emptyText || "No running tasks.";
    return;
  }
  box.className = "cmd-summary";
  box.replaceChildren();
  for (const cmd of cmds) {
    const stateName = commandVisualStateFromCommand(cmd);
    const row = document.createElement("div");
    row.className = `cmd-card state-${stateName}` + (state.selectedCommandId===cmd.id ? " selected" : "");
    row.tabIndex = 0;
    row.setAttribute("role", "button");
    row.onclick = () => selectCommand(cmd.id);
    row.onkeydown = (e) => {
      if (e.key==="Enter" || e.key===" ") { e.preventDefault(); selectCommand(cmd.id); }
    };
    const title = document.createElement("div");
    title.className = "cmd-title mono";
    title.textContent = taskTitleText(cmd);
    const meta = document.createElement("div");
    meta.className = "meta cmd-meta";
    appendCommandMetaPart(meta, commandStatusNode(commandStatusLabel(cmd), stateName));
    if (cmd.kind !== "tool" && cmd.cwd) appendCommandMetaPart(meta, cmd.cwd);
    const actions = document.createElement("div");
    actions.className = "cmd-actions";
    const term = document.createElement("button");
    term.className = "danger";
    term.textContent = cmd.terminating ? "Stop requested" : "Stop";
    // Commands stop by process id; tools have no process, so stopping interrupts the owning turn.
    term.disabled = cmd.terminating || (cmd.kind !== "tool" && !cmd.processId);
    term.title = cmd.kind === "tool" ? "Interrupt the turn running this tool call" : (cmd.processId ? "Ask Codex to stop this running command" : "No process id available");
    term.onclick = (e) => { e.stopPropagation(); stopTask(cmd.id); };
    actions.append(term);
    row.append(title, meta, actions);
    box.append(row);
  }
}
function toggleTasksMenu() {
  const menu = $("tasksMenu");
  menu.hidden = !menu.hidden;
  if (!menu.hidden) {
    $("mcpMenu").hidden = true;
    $("usageMenu").hidden = true;
    renderTasksMenu();
  }
}
$("tasksBtn").onclick = (e) => { e.stopPropagation(); toggleTasksMenu(); };
$("tasksMenu").onclick = (e) => e.stopPropagation();
document.addEventListener("click", (e) => {
  const menu = $("tasksMenu");
  if (menu.hidden) return;
  if (e.target.closest && e.target.closest(".tasks-wrap")) return;
  menu.hidden = true;
});
// A tool task can't be stopped individually (Codex has no per-call cancel), so stopping it
// interrupts the owning turn; commands terminate by process id.
function stopTask(id) {
  const cmd = state.runningCommands.get(id);
  if (!cmd || cmd.terminating) return;
  if (cmd.kind === "tool") {
    cmd.terminating = true;
    state.commandStopRequestedByItemId.add(id);
    renderRunningCommands();
    if (!send({ type:"interrupt", thread_id: state.threadId })) {
      cmd.terminating = false;
      state.commandStopRequestedByItemId.delete(id);
      renderRunningCommands();
      notice(`Interrupt not sent: WebSocket is ${state.wsStatus}.`, "error");
    }
    return;
  }
  terminateCommand(id);
}
function clearTaskSelection() {
  document.querySelectorAll(".msg.selected").forEach(el => el.classList.remove("selected"));
  document.querySelectorAll(".task-group-item.selected").forEach(el => el.classList.remove("selected"));
}
function selectCommand(id) {
  const key = idKey(id);
  state.selectedCommandId = key;
  clearTaskSelection();
  const group = state.taskGroupsByItemId.get(key);
  if (group) {
    state.manuallyToggledTaskGroups.add(group.id);
    state.expandedTaskGroups.add(group.id);
    expandedTaskDetailIds(group.id).add(key);
    syncTaskGroupState(group);
  }
  const msg = state.commandMsgElsByItemId.get(key);
  if (msg) {
    msg.classList.add("selected");
    const task = group && group.items.get(key);
    (task ? task.entry : msg).scrollIntoView({ block:"center", behavior:"smooth" });
  }
  if (group) syncTaskGroupItem(key);
  renderRunningCommands();
}
function terminateCommand(id) {
  const cmd = state.runningCommands.get(id);
  if (!cmd || !cmd.processId || cmd.terminating) return;
  cmd.terminating = true;
  state.commandStopRequestedByItemId.add(id);
  const body = commandBodyFor(id);
  if (body) renderCommandBody(body, cmd);
  renderRunningCommands();
  if (!send({ type:"terminate_command", thread_id: state.threadId, process_id: cmd.processId })) {
    cmd.terminating = false;
    state.commandStopRequestedByItemId.delete(id);
    if (body) renderCommandBody(body, cmd);
    renderRunningCommands();
    notice(`Terminate not sent: WebSocket is ${state.wsStatus}.`, "error");
  }
}
function resetTerminatingCommand(processId) {
  for (const cmd of state.runningCommands.values()) {
    if (!cmd.terminating) continue;
    // Scope the optimistic rollback to the command the failed request targeted. Only fall back
    // to clearing every pending stop request when the server didn't identify a process id.
    if (processId && cmd.processId !== processId) continue;
    cmd.terminating = false;
    state.commandStopRequestedByItemId.delete(cmd.id);
    const body = commandBodyFor(cmd.id);
    if (body) renderCommandBody(body, cmd);
  }
  renderRunningCommands();
}
function resetTerminatingToolTasks() {
  for (const cmd of state.runningCommands.values()) {
    if (cmd.kind !== "tool" || !cmd.terminating) continue;
    cmd.terminating = false;
    state.commandStopRequestedByItemId.delete(cmd.id);
  }
  renderRunningCommands();
}
function startToolCall(item, turnId) {
  const key = idKey(item.id); if (!key) return;
  const tool = item.tool || {};
  let body = state.streamElsByItemId.get(key);
  if (!body) body = taskBubble(key, "tool_call", "tool running-tool state-running", "tool");
  state.streamElsByItemId.set(key, body);
  state.toolBodyElsByItemId.set(key, body);
  state.commandMsgElsByItemId.set(key, body.parentElement);
  body.parentElement.dataset.toolItemId = key;
  body.parentElement.dataset.toolStartedAtMs = String(normalizeTimestampMs(tool.started_at_ms, Date.now()));
  renderItemBody(body, {
    kind:"tool_call",
    name:tool.name || "tool",
    input:tool.input,
    output:null,
    server:tool.server || null,
    status:tool.status || "in_progress",
    error:null
  });
}
function appendToolProgress(itemId, text) {
  const key = idKey(itemId);
  let body = key ? state.streamElsByItemId.get(key) : null;
  if (!body) {
    body = taskBubble(key, "tool_call", "tool running-tool state-running", "tool");
    if (key) {
      state.streamElsByItemId.set(key, body);
      state.toolBodyElsByItemId.set(key, body);
      state.commandMsgElsByItemId.set(key, body.parentElement);
      body.parentElement.dataset.toolItemId = key;
    }
    renderItemBody(body, {
      kind:"tool_call",
      name:"tool",
      input:null,
      output:null,
      server:null,
      status:"in_progress",
      error:null
    });
  }
  const chunk = String(text || "");
  if (key) {
    const payload = state.toolPayloadsByItemId.get(key);
    if (payload) {
      const current = typeof payload.output === "string" ? payload.output : "";
      payload.output = current ? current + "\n" + chunk : chunk;
      state.toolPayloadsByItemId.set(key, payload);
      renderItemBody(body, payload);
      $("transcript").scrollTop = $("transcript").scrollHeight;
      return;
    }
  }
  let progress = body.querySelector(".tool-progress");
  if (!progress) {
    progress = document.createElement("div");
    progress.className = "meta tool-progress";
    body.append(progress);
  }
  progress.textContent += (progress.textContent ? "\n" : "") + chunk;
  $("transcript").scrollTop = $("transcript").scrollHeight;
}
function appendStream(text, itemId, deltaType) {
  const key = idKey(itemId);
  if (key && state.renderedItemIds.has(key)) return;
  let body = key ? state.streamElsByItemId.get(key) : state.streamEl;
  if (!body) {
    const kind = key ? state.itemKindsByItemId.get(key) : null;
    if (key && (isTaskPayloadKind(kind) || deltaType==="command_output")) {
      const taskKind = kind==="tool_call" ? "tool_call" : "command_execution";
      body = taskBubble(key, taskKind, classForStream(taskKind, deltaType), roleForStream(taskKind, deltaType));
      if (taskKind==="command_execution") {
        state.commandBodyElsByItemId.set(key, body);
        state.commandMsgElsByItemId.set(key, body.parentElement);
        body.parentElement.dataset.commandItemId = key;
      } else {
        state.toolBodyElsByItemId.set(key, body);
        state.commandMsgElsByItemId.set(key, body.parentElement);
        body.parentElement.dataset.toolItemId = key;
      }
    } else {
      body = bubble(classForStream(kind, deltaType), roleForStream(kind, deltaType));
    }
    if (key) state.streamElsByItemId.set(key, body);
  }
  state.streamEl = body;
  state.streamItemId = key || null;
  const kind = key ? state.itemKindsByItemId.get(key) : null;
  if (deltaType==="command_output" || kind==="command_execution") {
    let out = body.querySelector("pre.out");
    if (!out) {
      out = document.createElement("pre");
      out.className = "out";
      body.append(out);
    }
    out.append(document.createTextNode(text));
  } else {
    body.textContent += text;
  }
  $("transcript").scrollTop = $("transcript").scrollHeight;
}
function finalizeStreamedItem(item) {
  if (!item || !item.payload) return false;
  const key = idKey(item.id);
  const body = (key && (state.streamElsByItemId.get(key) || state.commandBodyElsByItemId.get(key) || state.toolBodyElsByItemId.get(key))) ||
    (state.streamItemId===key ? state.streamEl : null);
  if (!hasVisiblePayload(item.payload)) {
    if (body && !body.textContent.trim()) {
      if (key && !removeTaskGroupItem(key)) body.parentElement.remove();
    }
    markRenderedItem(item);
    if (state.streamEl === body) {
      state.streamEl = null;
      state.streamItemId = null;
    }
    return true;
  }
  if (!body) return false;
  if (item.payload.kind==="command_execution" && key) {
    state.commandBodyElsByItemId.set(key, body);
    state.commandMsgElsByItemId.set(key, body.parentElement);
    body.parentElement.dataset.commandItemId = key;
  }
  if (item.payload.kind==="tool_call" && key) {
    state.toolBodyElsByItemId.set(key, body);
    state.commandMsgElsByItemId.set(key, body.parentElement);
    body.parentElement.dataset.toolItemId = key;
  }
  renderItemBody(body, item.payload);
  if (item.payload.kind==="command_execution") finishRunningCommand(item);
  markRenderedItem(item);
  if (state.streamEl === body) {
    state.streamEl = null;
    state.streamItemId = null;
  }
  $("transcript").scrollTop = $("transcript").scrollHeight;
  return true;
}
function addItem(item, fromHistory) {
  const p = item && item.payload ? item.payload : item;
  if (!p) return;
  // Plan updates are shown in the pinned plan card above the composer, not as transcript rows. Live
  // updates drive the card; persisted (history) plan rows are simply dropped — a finished plan's
  // card has already disappeared, so there is nothing to replay.
  if (isPlanItem(item)) {
    if (!fromHistory) updatePlanCard(item);
    return;
  }
  if (isRenderedItem(item)) return;
  if (!hasVisiblePayload(p)) {
    markRenderedItem(item);
    return;
  }
  if (p.kind==="user_message") {
    if (state.pendingUserEl && p.text===state.pendingUserText) {
      state.pendingUserEl.classList.remove("pending");
      state.pendingUserEl.querySelector(".body").textContent = p.text;
      state.pendingUserEl = null;
      state.pendingUserText = null;
      markRenderedItem(item);
      return;
    }
    renderItemBody(bubble("user","you"), p);
  }
  else {
    if (p.kind==="file_change" && mergeFileChangeWithPrevious(p)) {
      markRenderedItem(item);
      return;
    }
    const key = idKey(item && item.id);
    const body = isTaskPayloadKind(p.kind)
      ? taskBubble(key, p.kind, classForPayload(p), roleForPayload(p))
      : bubble(classForPayload(p), roleForPayload(p));
    if (p.kind==="command_execution") {
      if (key) {
        state.commandBodyElsByItemId.set(key, body);
        state.commandMsgElsByItemId.set(key, body.parentElement);
        body.parentElement.dataset.commandItemId = key;
      }
    }
    if (p.kind==="tool_call") {
      if (key) {
        state.toolBodyElsByItemId.set(key, body);
        state.commandMsgElsByItemId.set(key, body.parentElement);
        body.parentElement.dataset.toolItemId = key;
      }
    }
    renderItemBody(body, p);
  }
  if (p.kind==="command_execution") finishRunningCommand(item);
  markRenderedItem(item);
}
function resetRenderState() {
  clearPlanCard();   // dropping/switching threads clears any pinned plan
  state.streamEl = null;
  state.streamItemId = null;
  state.streamElsByItemId = new Map();
  state.renderedItemIds = new Set();
  state.renderedHarnessItemIds = new Set();
  state.itemKindsByItemId = new Map();
  state.pendingApprovals = new Map();
  state.renderedApprovalStateKeys = new Set();
  state.pendingServerRequests = new Map();
  state.runningCommands = new Map();
  state.commandBodyElsByItemId = new Map();
  state.commandMsgElsByItemId = new Map();
  state.commandStopRequestedByItemId = new Set();
  state.commandPayloadsByItemId = new Map();
  state.endedCommandsByItemId = new Map();
  state.expandedCommandOutputs = new Set();
  state.manuallyToggledCommandOutputs = new Set();
  state.toolPayloadsByItemId = new Map();
  state.toolBodyElsByItemId = new Map();
  state.expandedToolOutputs = new Set();
  state.manuallyToggledToolOutputs = new Set();
  state.activeTaskGroup = null;
  state.taskGroupSeq = 0;
  state.taskItemSeq = 0;
  state.taskGroupsById = new Map();
  state.taskGroupsByItemId = new Map();
  state.expandedTaskGroups = new Set();
  state.manuallyToggledTaskGroups = new Set();
  state.expandedTaskDetails = new Map();
  state.selectedCommandId = null;
  renderRunningCommands();
}
function idKey(id) {
  return id === undefined || id === null ? "" : String(id);
}
function isRenderedItem(item) {
  const itemId = idKey(item && item.id);
  const harnessItemId = item && item.harness_item_id;
  return (itemId && state.renderedItemIds.has(itemId)) ||
    (harnessItemId && state.renderedHarnessItemIds.has(harnessItemId));
}
function markRenderedItem(item) {
  const itemId = idKey(item && item.id);
  if (itemId) {
    state.renderedItemIds.add(itemId);
    state.streamElsByItemId.delete(itemId);
    state.itemKindsByItemId.delete(itemId);
  }
  if (item && item.harness_item_id) state.renderedHarnessItemIds.add(item.harness_item_id);
}
function hasVisiblePayload(p) {
  if (!p || !p.kind) return false;
  if (p.kind==="command_execution") return Boolean((p.command||"").trim() || (p.output||"").trim());
  if (p.kind==="agent_message" || p.kind==="reasoning" || p.kind==="user_message") return Boolean((p.text||"").trim());
  if (p.kind==="file_change") return Boolean((p.path||"").trim() || (p.changes||[]).length || p.status);
  if (p.kind==="tool_call") return Boolean((p.name||"").trim() || (p.server||"").trim() || p.status || p.error || p.input || p.output);
  if (p.kind==="activity") return Boolean((p.title||"").trim() || (p.detail||"").trim() || visibleActivityMetadata(p));
  return false;
}
function renderItemBody(body, p) {
  const msg = body.parentElement;
  msg.className = "msg " + classForPayload(p);
  msg.querySelector(".role").textContent = roleForPayload(p);
  clearRowToggle(msg);
  // Markdown messages keep their raw source so the row copy button yields Markdown, not rendered
  // text; other rows fall back to the rendered text.
  if (p.kind==="agent_message" || p.kind==="reasoning" || p.kind==="user_message") {
    msg.dataset.copyText = p.text || "";
  } else {
    delete msg.dataset.copyText;
  }
  body.replaceChildren();
  if (p.kind==="command_execution") {
    const itemId = msg.dataset.commandItemId || "";
    if (itemId) {
      state.commandPayloadsByItemId.set(itemId, p);
      state.commandBodyElsByItemId.set(itemId, body);
      state.commandMsgElsByItemId.set(itemId, msg);
    }
    const stopRequested = !!(itemId && (state.commandStopRequestedByItemId.has(itemId) || (state.runningCommands.get(itemId) && state.runningCommands.get(itemId).terminating)));
    const displayStatus = p.status;
    const stateName = commandVisualStateFromStatus(displayStatus);
    msg.classList.add(`state-${stateName}`);
    if (commandIsRunningStatus(displayStatus)) msg.classList.add("running-command");
    const startedAtMs = normalizeTimestampMs(msg.dataset.commandStartedAtMs, null);
    const durationMs = normalizeCommandDuration(p.duration_ms, startedAtMs);
    const outputPhase = commandIsRunningStatus(displayStatus) ? "running" : "completed";
    const { head } = makeCommandHead();
    const cmd = document.createElement("div");
    cmd.className = "cmd-title mono";
    cmd.textContent = "$ " + (p.command || "");
    const meta = document.createElement("div");
    meta.className = "meta cmd-meta";
    if (p.cwd) appendCommandMetaPart(meta, `cwd: ${p.cwd}`);
    if (displayStatus && !commandIsRunningStatus(displayStatus)) {
      appendCommandMetaPart(meta, commandStatusNode(terminalCommandStatus(displayStatus, durationMs, { stopRequested }), stateName));
    }
    if (displayStatus && commandIsRunningStatus(displayStatus)) {
      appendCommandMetaPart(meta, commandStatusNode(`status: ${displayStatus}`, stateName));
    }
    if (p.exit_code!==undefined && p.exit_code!==null) appendCommandMetaPart(meta, `exit: ${p.exit_code}`);
    head.append(cmd);
    body.append(head);
    if (meta.childNodes.length) body.append(meta);
    renderCommandOutputBlock(body, {
      itemId,
      output:p.output || "",
      phase:outputPhase,
      linkify:true
    });
  } else if (p.kind==="agent_message" || p.kind==="reasoning" || p.kind==="user_message") {
    // User messages get the same server-rendered, sanitized Markdown as agent text, so pasted code
    // fences, lists and emphasis format the same on both sides of the conversation.
    renderMarkdown(body, p.text || "");
  } else if (p.kind==="file_change") {
    renderFileChange(body, p);
  } else if (p.kind==="tool_call") {
    const stateName = toolVisualStateFromStatus(p.status, p.error);
    msg.classList.add(`state-${stateName}`);
    if (stateName==="running") msg.classList.add("running-tool");
    renderToolBody(body, p);
  } else if (p.kind==="activity") {
    body.innerHTML = renderActivity(p);
  }
  const taskItemId = msg.dataset.commandItemId || msg.dataset.toolItemId || "";
  if (taskItemId) syncTaskGroupItem(taskItemId);
}
function normalizeCommandDuration(durationMs, startedAtMs) {
  const provided = Number(durationMs);
  if (Number.isFinite(provided) && provided >= 0) return provided;
  const started = Number(startedAtMs);
  if (Number.isFinite(started) && started > 0) return Date.now() - started;
  return null;
}
function classForPayload(p) {
  if (p.kind==="user_message") return "user";
  if (p.kind==="reasoning") return "reasoning";
  if (p.kind==="command_execution") return "cmd";
  if (p.kind==="file_change") return "file";
  if (p.kind==="tool_call") return "tool";
  if (p.kind==="activity") return "activity";
  return "agent";
}
function roleForPayload(p) {
  if (p.kind==="user_message") return "you";
  if (p.kind==="reasoning") return "reasoning";
  if (p.kind==="command_execution") return "command";
  if (p.kind==="file_change") return "files";
  if (p.kind==="tool_call") return "tool";
  if (p.kind==="activity") return "activity";
  return "agent";
}
function classForStream(kind, deltaType) {
  if (deltaType==="command_output" || kind==="command_execution") return "cmd";
  if (kind==="reasoning") return "reasoning";
  if (kind==="file_change") return "file";
  if (kind==="tool_call") return "tool";
  if (kind==="activity") return "activity";
  return "agent";
}
function roleForStream(kind, deltaType) {
  if (deltaType==="command_output" || kind==="command_execution") return "command";
  if (kind==="reasoning") return "reasoning";
  if (kind==="file_change") return "files";
  if (kind==="tool_call") return "tool";
  if (kind==="activity") return "activity";
  return "agent";
}
// Agent/reasoning text is Markdown. Render it to sanitized HTML on the server (which also embeds
// path-link buttons), then inject it. The raw text is shown as-is until the render resolves, so
// streaming stays readable and a failed request degrades to plain text.
function renderMarkdown(el, text) {
  text = String(text || "");
  el.classList.remove("md");
  el.textContent = text;
  if (!text.trim() || !state.projectId) return;

  const projectId = state.projectId;
  const cacheKey = projectId + "\n" + text;
  const apply = (html) => {
    if (!el.isConnected || projectId !== state.projectId) return;
    if (typeof html !== "string") return;
    el.innerHTML = html;
    el.classList.add("md");
    wirePathLinks(el);
    wireCodeCopy(el);
    keepTranscriptRowAnchored(el);
  };

  if (state.markdownCache.has(cacheKey)) {
    apply(state.markdownCache.get(cacheKey));
    return;
  }

  api("POST", `/api/projects/${projectId}/render`, { text })
    .then((res) => {
      const html = res && typeof res.html === "string" ? res.html : "";
      state.markdownCache.set(cacheKey, html);
      apply(html);
    })
    .catch((e) => {
      console.warn("Giskard markdown render failed; keeping plain text fallback.", e);
    });
}
// Wire the server-emitted `.path-link` buttons (they arrive with data attributes but no handler).
function wirePathLinks(el) {
  el.querySelectorAll("button.path-link[data-path]").forEach((btn) => {
    const path = btn.dataset.path || "";
    const line = normalizeLine(btn.dataset.line, null);
    btn.title = line ? `Open source at line ${line}` : "Open source";
    btn.onclick = (e) => {
      e.stopPropagation();
      openCodeOverlay(path, line);
    };
  });
}
// Copy text to the clipboard, falling back to a hidden-textarea + execCommand when the async
// Clipboard API is unavailable (e.g. the app served over plain HTTP, a non-secure context).
async function copyToClipboard(text) {
  try {
    if (navigator.clipboard && window.isSecureContext) {
      await navigator.clipboard.writeText(text);
      return true;
    }
  } catch (e) { /* fall through to the legacy path */ }
  try {
    const ta = document.createElement("textarea");
    ta.value = text;
    ta.setAttribute("readonly", "");
    ta.style.position = "fixed";
    ta.style.top = "-1000px";
    ta.style.opacity = "0";
    document.body.appendChild(ta);
    ta.select();
    const ok = document.execCommand("copy");
    ta.remove();
    return ok;
  } catch (e) {
    return false;
  }
}
// Add a "Copy" button to each rendered code block's header so the raw (un-highlighted) source can
// be lifted straight into an editor or shell. The button reads textContent off the <code>, which
// strips the syntax-highlight markup and yields the original text.
function wireCodeCopy(el) {
  el.querySelectorAll(".code-block").forEach((block) => {
    const head = block.querySelector(".code-block-head");
    const code = block.querySelector("pre code") || block.querySelector("pre");
    if (!head || !code || head.querySelector(".code-copy")) return;
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "code-copy";
    btn.textContent = "Copy";
    btn.title = "Copy code to clipboard";
    let resetTimer = 0;
    btn.onclick = async (e) => {
      e.stopPropagation();
      const ok = await copyToClipboard(code.textContent);
      btn.textContent = ok ? "Copied" : "Failed";
      btn.classList.toggle("ok", ok);
      btn.classList.toggle("err", !ok);
      clearTimeout(resetTimer);
      resetTimer = setTimeout(() => {
        btn.textContent = "Copy";
        btn.classList.remove("ok", "err");
      }, 1500);
    };
    head.appendChild(btn);
  });
}
function renderLinkedText(el, text) {
  text = String(text || "");
  el.textContent = text;
  if (!text.trim() || !state.projectId) return;

  const projectId = state.projectId;
  const cacheKey = projectId + "\n" + text;
  const apply = (links) => {
    if (!el.isConnected || projectId !== state.projectId) return;
    applyLinkedText(el, text, links || []);
    keepTranscriptRowAnchored(el);
  };

  if (state.linkifyCache.has(cacheKey)) {
    apply(state.linkifyCache.get(cacheKey));
    return;
  }

  api("POST", `/api/projects/${projectId}/linkify`, { text })
    .then((res) => {
      const links = Array.isArray(res.links) ? res.links : [];
      state.linkifyCache.set(cacheKey, links);
      apply(links);
    })
    .catch((e) => {
      console.warn("Giskard linkification failed; keeping plain text fallback.", e);
    });
}
function applyLinkedText(el, text, links) {
  const sorted = links.slice().sort((a, b) => (a.start || 0) - (b.start || 0));
  const frag = document.createDocumentFragment();
  let pos = 0;
  let added = false;
  for (const link of sorted) {
    const start = byteOffsetToIndex(text, Number(link.start) || 0);
    const end = byteOffsetToIndex(text, Number(link.end) || 0);
    if (!link.path || start < pos || end <= start) continue;
    frag.append(document.createTextNode(text.slice(pos, start)));
    frag.append(makePathLink(link.path, text.slice(start, end), link.line));
    pos = end;
    added = true;
  }
  if (!added) return;
  frag.append(document.createTextNode(text.slice(pos)));
  el.replaceChildren(frag);
}
function byteOffsetToIndex(text, offset) {
  let bytes = 0;
  for (let i = 0; i < text.length;) {
    if (bytes >= offset) return i;
    const code = text.codePointAt(i);
    const step = code > 0xffff ? 2 : 1;
    bytes += code <= 0x7f ? 1 : code <= 0x7ff ? 2 : code <= 0xffff ? 3 : 4;
    i += step;
  }
  return text.length;
}
function makePathLink(path, label, line) {
  const value = String(path || "");
  if (!value) return document.createTextNode(label || "");
  const btn = document.createElement("button");
  btn.type = "button";
  btn.className = "path-link";
  btn.textContent = label || value;
  const targetLine = normalizeLine(line, null);
  btn.title = targetLine ? `Open source at line ${targetLine}` : "Open source";
  btn.onclick = (e) => {
    e.stopPropagation();
    openCodeOverlay(value, targetLine);
  };
  return btn;
}
function fileChangeEntries(p) {
  if (p && p.changes && p.changes.length) return p.changes;
  return [{ path:p && p.path, change:p && p.change, diff:p && p.diff }];
}
function normalizedFileChangePayload(p) {
  const entries = fileChangeEntries(p).map(c => ({
    path:c && c.path,
    change:c && c.change,
    diff:c && c.diff,
    status:(c && c.status) || (p && p.status)
  }));
  const first = entries[0] || {};
  return {
    kind:"file_change",
    path:first.path || (p && p.path) || "",
    change:first.change || (p && p.change) || "modified",
    changes:entries
  };
}
function mergeFileChangePayload(existing, next) {
  const current = normalizedFileChangePayload(existing);
  const incoming = normalizedFileChangePayload(next);
  return {
    kind:"file_change",
    path:current.path,
    change:current.change,
    changes:current.changes.concat(incoming.changes)
  };
}
function mergeFileChangeWithPrevious(p) {
  breakTaskGroup();
  const target = renderTarget();
  const prev = target && target.lastElementChild;
  if (
    !prev ||
    !prev.classList ||
    !prev.classList.contains("file") ||
    !prev._fileChangePayload
  ) return false;
  const body = prev.querySelector(".body");
  if (!body) return false;
  const merged = mergeFileChangePayload(prev._fileChangePayload, p);
  renderItemBody(body, merged);
  keepTranscriptRowAnchored(prev);
  return true;
}
function renderFileChange(body, p) {
  const normalized = normalizedFileChangePayload(p);
  body.parentElement._fileChangePayload = normalized;
  const changes = normalized.changes;
  const title = document.createElement("div");
  title.textContent = `File change${changes.length===1 ? "" : "s"}`;
  const list = document.createElement("ul");
  list.className = "item-list";
  for (const c of changes) {
    const li = document.createElement("li");
    li.className = "file-change-entry";
    const row = document.createElement("div");
    row.className = "file-change-row";
    const kind = document.createElement("span");
    kind.className = "mono";
    kind.textContent = c.change || "modified";
    row.append(kind, document.createTextNode(" "), makePathLink(c.path || "", c.path || "", null));
    if (c.diff) {
      row.append(document.createTextNode(" "));
      const diffBtn = document.createElement("button");
      diffBtn.type = "button";
      diffBtn.className = "diff-open";
      diffBtn.textContent = "View diff";
      diffBtn.title = "Open rendered diff";
      diffBtn.onclick = (e) => {
        e.stopPropagation();
        openDiffOverlay(c.path || "File change", c.diff);
      };
      row.append(diffBtn);
    }
    if (c.status) {
      const status = document.createElement("span");
      status.className = "badge file-change-status";
      status.textContent = c.status;
      row.append(document.createTextNode(" "), status);
    }
    li.append(row);
    list.append(li);
  }
  body.append(title, list);
}
// Tool calls (esp. MCP results) can return very large input/output payloads. Render them with the
// same row-owned collapse model as command output: running rows start expanded while small, large
// running payloads auto-collapse, and completed payloads collapse by default.
function renderToolBody(body, p) {
  const stateName = toolVisualStateFromStatus(p.status, p.error);
  const msg = body.parentElement;
  const itemId = idKey(msg.dataset.toolItemId);
  if (itemId) {
    state.toolPayloadsByItemId.set(itemId, p);
    state.toolBodyElsByItemId.set(itemId, body);
    state.commandMsgElsByItemId.set(itemId, msg);
  }
  const head = document.createElement("div");
  head.className = "cmd-head";
  const title = document.createElement("div");
  title.className = "cmd-title mono";
  title.textContent = `${p.server ? p.server + ":" : ""}${p.name || "tool"}`;
  head.append(title);
  body.append(head);

  const statusLabel = toolStatusLabel(p.status, p.error, msg, stateName);
  if (statusLabel) {
    const meta = document.createElement("div");
    meta.className = "meta cmd-meta";
    appendCommandMetaPart(meta, commandStatusNode(statusLabel, stateName));
    body.append(meta);
  }

  renderToolIoBlocks(body, {
    itemId,
    phase:stateName === "running" ? "running" : "completed",
    blocks:toolIoBlocks(p)
  });
  if (p.error) {
    const err = document.createElement("div");
    err.className = "meta";
    err.textContent = "error: " + p.error;
    body.append(err);
  }
}
function toolIoBlocks(p) {
  const blocks = [];
  const input = toolIoText(p.input);
  const output = toolIoText(p.output);
  if (input) blocks.push({ label:"Input", text:input });
  if (output) blocks.push({ label:"Output", text:output });
  return blocks;
}
function toolIoText(value) {
  if (!hasMeaningfulJson(value)) return "";
  return jsonPreview(value);
}
function toolIoStats(blocks) {
  return commandOutputStats(blocks.map(block => `${block.label}\n${block.text}`).join("\n\n"));
}
function isToolIoExpanded(itemId, phase, blocks, stats) {
  const key = idKey(itemId);
  if (key && state.manuallyToggledToolOutputs.has(key)) {
    return state.expandedToolOutputs.has(key);
  }
  if (phase === "completed") return false;
  return !commandOutputShouldAutoCollapse(stats || toolIoStats(blocks));
}
function wireToolRowToggle(msg, itemId, expanded) {
  const key = idKey(itemId);
  if (!key) return;
  msg.classList.add("toggleable");
  msg.title = expanded ? "Collapse tool input/output" : "Show tool input/output";
  msg.tabIndex = 0;
  msg.setAttribute("role", "button");
  msg.setAttribute("aria-expanded", expanded ? "true" : "false");
  msg.onclick = (e) => {
    if (e.defaultPrevented || e.target.closest("button,a,input,select,textarea")) return;
    toggleToolOutput(key);
  };
  msg.onkeydown = (e) => {
    if (e.key === "Enter" || e.key === " ") {
      e.preventDefault();
      toggleToolOutput(key);
    }
  };
}
function toggleToolOutput(itemId) {
  const key = idKey(itemId);
  if (!key) return;
  const payload = state.toolPayloadsByItemId.get(key);
  if (!payload) return;
  const blocks = toolIoBlocks(payload);
  if (!blocks.length) return;
  const stateName = toolVisualStateFromStatus(payload.status, payload.error);
  const phase = stateName === "running" ? "running" : "completed";
  const expanded = isToolIoExpanded(key, phase, blocks);
  state.manuallyToggledToolOutputs.add(key);
  if (expanded) state.expandedToolOutputs.delete(key);
  else state.expandedToolOutputs.add(key);
  rerenderToolRow(key);
}
function renderToolIoBlocks(body, opts) {
  const blocks = opts.blocks || [];
  if (!blocks.length) {
    clearRowToggle(body.parentElement);
    return;
  }
  const itemId = idKey(opts.itemId);
  const phase = opts.phase || "completed";
  const stats = toolIoStats(blocks);
  const expanded = isToolIoExpanded(itemId, phase, blocks, stats);
  const msg = body.parentElement;
  msg.classList.toggle("collapsed", !expanded);
  msg.classList.toggle("expanded", expanded);
  wireToolRowToggle(msg, itemId, expanded);

  const summary = document.createElement("div");
  summary.className = "meta cmd-output-summary";
  const label = commandOutputStatsLabel(stats, phase);
  summary.textContent = expanded ? `Tool data · ${label}` : `Tool data collapsed · ${label}`;
  body.append(summary);

  if (!expanded) return;
  for (const block of blocks) {
    const section = document.createElement("div");
    section.className = "tool-io";
    const heading = document.createElement("div");
    heading.className = "meta";
    heading.textContent = block.label;
    const pre = document.createElement("pre");
    pre.className = "out";
    pre.textContent = block.text;
    section.append(heading, pre);
    body.append(section);
  }
}
function toolVisualStateFromStatus(status, error) {
  if (error) return "failed";
  const s = commandStatusKey(status);
  if (s==="completed" || s==="succeeded" || s==="success") return "succeeded";
  if (s==="failed" || s==="error") return "failed";
  if (s==="terminated" || s==="declined" || s==="canceled" || s==="cancelled" || s==="interrupted" || s==="unknown") return "terminated";
  if (commandIsRunningStatus(status)) return "running";
  return s ? "failed" : "running";
}
function toolStatusLabel(status, error, msg, stateName) {
  const startedAtMs = normalizeTimestampMs(msg && msg.dataset.toolStartedAtMs, null);
  if (stateName === "running") {
    return startedAtMs ? `running for ${formatDuration(Date.now() - startedAtMs)}` : "running";
  }
  const durationMs = toolTerminalDurationMs(msg, startedAtMs);
  return terminalCommandStatus(error && !status ? "failed" : status, durationMs, null);
}
function toolTerminalDurationMs(msg, startedAtMs) {
  if (!msg || !startedAtMs) return null;
  const stored = normalizeTimestampMs(msg.dataset.toolDurationMs, null);
  if (stored !== null) return stored;
  const durationMs = Date.now() - startedAtMs;
  msg.dataset.toolDurationMs = String(durationMs);
  return durationMs;
}
function renderActivity(p) {
  if (isImageViewActivity(p)) return renderImageViewActivity(p);
  const detail = p.detail ? `<div>${escapeHtml(p.detail)}</div>` : "";
  const metadata = visibleActivityMetadata(p);
  const meta = metadata ? `<pre class="out">${escapeHtml(jsonPreview(metadata))}</pre>` : "";
  return `<div>${escapeHtml(p.title||"Activity")}</div>${detail}${meta}`;
}
function isImageViewActivity(p) {
  return !!(p && p.kind === "activity" && p.title === "Image viewed" && imageViewPath(p));
}
function imageViewPath(p) {
  const detail = String((p && p.detail) || "").trim();
  if (detail) return detail;
  const md = p && p.metadata;
  return md && typeof md.path === "string" ? md.path.trim() : "";
}
function renderImageViewActivity(p) {
  const path = imageViewPath(p);
  const src = projectFileUrl("image", path);
  return [
    `<div class="activity-image-title">${escapeHtml(p.title || "Image viewed")}</div>`,
    `<a class="activity-image-link" href="${escapeAttr(src)}" target="_blank" rel="noopener" title="Open image">`,
    `<img class="activity-image-preview" src="${escapeAttr(src)}" alt="${escapeAttr(path)}" loading="lazy" decoding="async">`,
    `</a>`,
    `<div class="activity-image-caption">${escapeHtml(path)}</div>`
  ].join("");
}
// A plan-update activity carries its steps as a `[{ step, status }]` metadata array (status is one
// of "pending" | "inProgress" | "completed"). Detect it by shape so the check is independent of the
// activity title.
function planFromActivity(p) {
  const md = p && p.metadata;
  if (!Array.isArray(md) || !md.length) return null;
  const ok = md.every(it => it && typeof it === "object"
    && typeof it.step === "string" && typeof it.status === "string");
  return ok ? md : null;
}
function isPlanItem(item) {
  const p = item && (item.payload || item);
  return !!(p && p.kind === "activity" && planFromActivity(p));
}
const PLAN_STEP_STATES = { completed:"done", inProgress:"doing", pending:"todo" };
const PLAN_STEP_ICONS = { done:"✓", doing:"◐", todo:"○" };
// The "current" step is the one being worked on: the first in-progress step, or the first pending
// step if none is in progress. Returns null once every step is completed (the plan is finished).
function currentPlanStepIndex(steps) {
  const doing = steps.findIndex(s => s.status === "inProgress");
  if (doing !== -1) return doing;
  const pending = steps.findIndex(s => s.status !== "completed");
  return pending === -1 ? null : pending;
}
// The plan activity `detail` is "explanation\n<status>: <step>\n…"; strip the trailing step lines
// (which duplicate the checklist) to isolate the agent's explanation.
function planExplanation(p, steps) {
  const stepLines = steps.map(s => `${s.status}: ${s.step}`);
  const lines = String(p && p.detail || "").split("\n");
  for (let i = stepLines.length - 1; i >= 0 && lines.length; i--) {
    if (lines[lines.length - 1] === stepLines[i]) lines.pop();
    else break;
  }
  return lines.join("\n").trim();
}
function renderPlanSteps(steps) {
  const items = steps.map(s => {
    const cls = PLAN_STEP_STATES[s.status] || "todo";
    return `<li class="plan-step ${cls}"><span class="plan-step-icon" aria-hidden="true">${PLAN_STEP_ICONS[cls]}</span><span class="plan-step-text">${escapeHtml(s.step)}</span></li>`;
  }).join("");
  return `<ul class="plan-steps">${items}</ul>`;
}

/* ---------- plan card (pinned above the composer) ---------- */
// Take a live plan-update activity and reflect it in the card. A plan whose steps are all completed
// is finished, so the card is cleared instead of shown.
function updatePlanCard(item) {
  const p = item && (item.payload || item);
  const steps = planFromActivity(p);
  if (!steps) return;
  if (currentPlanStepIndex(steps) === null) { clearPlanCard(); return; }
  state.currentPlan = { steps, explanation: planExplanation(p, steps) };
  renderPlanCard();
}
function clearPlanCard() {
  state.currentPlan = null;
  renderPlanCard();
}
function setPlanExpanded(expanded) {
  state.planExpanded = !!expanded;
  localStorage.setItem("giskard.planExpanded", state.planExpanded ? "1" : "0");
  renderPlanCard();
}
function renderPlanCard() {
  const card = $("planCard");
  if (!card) return;
  const plan = state.currentPlan;
  const idx = plan ? currentPlanStepIndex(plan.steps) : null;
  if (!plan || idx === null) { card.hidden = true; return; }
  const steps = plan.steps;
  $("planCardCount").textContent = `${idx + 1}/${steps.length}`;
  $("planCardCurrent").textContent = steps[idx].step;
  const body = $("planCardBody");
  const expl = plan.explanation ? `<div class="plan-explanation">${escapeHtml(plan.explanation)}</div>` : "";
  body.innerHTML = expl + renderPlanSteps(steps);
  card.classList.toggle("expanded", state.planExpanded);   // CSS rotates the caret when expanded
  body.hidden = !state.planExpanded;
  $("planCardToggle").setAttribute("aria-expanded", state.planExpanded ? "true" : "false");
  card.hidden = false;
}
$("planCardToggle").onclick = () => setPlanExpanded(!state.planExpanded);
function visibleActivityMetadata(p) {
  if (!p || !p.metadata) return null;
  if (isContextCompactionPayload(p)) return null;
  return p.metadata;
}
function isContextCompactionPayload(p) {
  if (!p || p.kind !== "activity") return false;
  const metadata = p.metadata || {};
  if (metadata.type === "contextCompaction") return true;
  if (metadata.threadId && metadata.turnId && String(p.title || "").toLowerCase().includes("context compact")) return true;
  const title = String(p.title || "").toLowerCase();
  return title.includes("context compaction") || title.includes("context compacted");
}
function isContextCompactionItem(item) {
  const payload = item && (item.payload || item);
  return isContextCompactionPayload(payload);
}
function finishCompactPending() {
  if (!state.compactPending) return;
  state.compactPending = false;
  updateComposerControls();
}
function appendJsonPreviewIfMeaningful(body, value) {
  if (!hasMeaningfulJson(value)) return;
  const pre = document.createElement("pre");
  pre.className = "out";
  pre.textContent = jsonPreview(value);
  body.append(pre);
}
function hasMeaningfulJson(value) {
  if (value === undefined || value === null) return false;
  if (typeof value === "string") return value.trim() !== "";
  if (Array.isArray(value)) return value.length > 0;
  if (typeof value === "object") return Object.keys(value).length > 0;
  return true;
}
function jsonPreview(v) {
  try { return typeof v==="string" ? v : JSON.stringify(v, null, 2); }
  catch { return String(v); }
}

/* ---------- MCP servers ---------- */
function mcpCounts() {
  const servers = state.mcpServers || [];
  const tools = servers.reduce((n, s) => n + ((s.tools || []).length), 0);
  const resources = servers.reduce((n, s) => n + ((s.resources || []).length) + ((s.resource_templates || []).length), 0);
  const needsAuth = servers.filter(s => s.auth_status === "not_logged_in").length;
  return { servers:servers.length, tools, resources, needsAuth };
}
function mcpOverallState() {
  if (state.mcpError) return "err";
  if (state.mcpLoading) return "";
  const counts = mcpCounts();
  if (!counts.servers) return "";
  if (counts.needsAuth) return "warn";
  return "ok";
}
function renderMcpButton() {
  const dot = $("mcpDot");
  dot.className = "mcp-dot";
  const visual = mcpOverallState();
  if (visual) dot.classList.add(visual);
  $("mcpCount").textContent = String((state.mcpServers || []).length);
  const caps = state.mcpCapabilities || {};
  $("mcpBtn").disabled = !state.projectId || (!caps.status && !state.mcpLoading && !state.mcpError && !(state.mcpServers || []).length);
  if (!$("mcpMenu").hidden) renderMcpMenu();
}
async function loadMcpServers(opts) {
  opts = opts || {};
  if (!state.projectId || state.mcpLoading) return;
  const projectId = state.projectId;
  state.mcpLoading = true;
  state.mcpError = null;
  renderMcpButton();
  try {
    const res = await api("GET", `/api/projects/${projectId}/mcp`);
    if (state.projectId !== projectId) return;
    state.mcpServers = Array.isArray(res.servers) ? res.servers : [];
    state.mcpCapabilities = res.capabilities || { status:true, reload:false, oauth_login:false };
  } catch (e) {
    if (state.projectId !== projectId) return;
    state.mcpError = e.message || "Could not load MCP servers.";
    if (opts.announce !== false) notice("Could not load MCP servers: "+state.mcpError, "warning");
  } finally {
    if (state.projectId === projectId) {
      state.mcpLoading = false;
      renderMcpButton();
    }
  }
}
async function reloadMcpServers() {
  if (!state.projectId || state.mcpLoading) return;
  const caps = state.mcpCapabilities || {};
  if (!caps.reload) { await loadMcpServers(); return; }
  try {
    await api("POST", `/api/projects/${state.projectId}/mcp/reload`, {});
    await loadMcpServers();
    notice("MCP servers reloaded.", "info");
  } catch (e) {
    notice("Could not reload MCP servers: "+e.message, "error");
  }
}
async function startMcpOauthLogin(name) {
  if (!state.projectId || !name) return;
  try {
    const res = await api("POST", `/api/projects/${state.projectId}/mcp/oauth-login`, { name });
    if (!res.authorization_url) {
      notice(`No OAuth URL returned for ${name}.`, "error");
      return;
    }
    window.open(res.authorization_url, "_blank", "noopener");
  } catch (e) {
    notice(`Could not start MCP login for ${name}: ${e.message}`, "error");
  }
}
function toggleMcpMenu() {
  const menu = $("mcpMenu");
  menu.hidden = !menu.hidden;
  if (!menu.hidden) {
    $("tasksMenu").hidden = true;
    $("usageMenu").hidden = true;
    renderMcpMenu();
    loadMcpServers({ announce:false });
  }
}
function renderMcpMenu() {
  const menu = $("mcpMenu");
  const counts = mcpCounts();
  const caps = state.mcpCapabilities || {};
  const rows = (state.mcpServers || []).map(renderMcpServerCard).join("");
  const reloadLabel = caps.reload ? "Reload" : "Refresh";
  const body = state.mcpError
    ? `<div class="meta">Error: ${escapeHtml(state.mcpError)}</div>`
    : caps.status === false
      ? `<div class="muted">MCP status is not supported by this harness.</div>`
    : state.mcpLoading && !state.mcpServers.length
      ? `<div class="muted">Loading MCP servers...</div>`
      : rows || `<div class="muted">No MCP servers reported by Codex.</div>`;
  menu.innerHTML = `
    <div class="mcp-head">
      <strong>MCP Servers</strong>
      <button id="mcpRefresh" type="button">${reloadLabel}</button>
      <button id="mcpClose" type="button">Close</button>
    </div>
    <div class="mcp-summary">${counts.servers} servers · ${counts.tools} tools · ${counts.resources} resources${counts.needsAuth ? ` · ${counts.needsAuth} need auth` : ""}</div>
    <div class="mcp-list">${body}</div>`;
  $("mcpRefresh").onclick = reloadMcpServers;
  $("mcpClose").onclick = () => { $("mcpMenu").hidden = true; };
  menu.querySelectorAll("[data-mcp-toggle]").forEach(btn => {
    btn.onclick = () => {
      const name = btn.dataset.mcpToggle;
      if (state.expandedMcps.has(name)) state.expandedMcps.delete(name);
      else state.expandedMcps.add(name);
      renderMcpMenu();
    };
  });
  menu.querySelectorAll("[data-mcp-login]").forEach(btn => {
    btn.onclick = () => startMcpOauthLogin(btn.dataset.mcpLogin);
  });
}
function renderMcpServerCard(server) {
  const name = server.name || "(unnamed)";
  const expanded = state.expandedMcps.has(name);
  const tools = server.tools || [];
  const resources = server.resources || [];
  const templates = server.resource_templates || [];
  const auth = mcpAuthLabel(server.auth_status);
  const chipClass = mcpAuthTone(server.auth_status);
  const login = server.auth_status === "not_logged_in" && (state.mcpCapabilities || {}).oauth_login
    ? `<button type="button" data-mcp-login="${escapeAttr(name)}">Authenticate</button>` : "";
  const detail = expanded ? `
    <div class="mcp-card-detail">
      ${server.server_info && server.server_info.description ? `<div>${escapeHtml(server.server_info.description)}</div>` : ""}
      <div class="meta">${tools.length} tools · ${resources.length + templates.length} resources</div>
      ${mcpListSection("Tools", tools.map(mcpToolName))}
      ${mcpListSection("Resources", resources.map(mcpResourceName))}
      ${mcpListSection("Resource templates", templates.map(mcpTemplateName))}
      <div class="mcp-actions">${login}</div>
    </div>` : "";
  return `<div class="mcp-card">
    <button class="mcp-card-top" type="button" data-mcp-toggle="${escapeAttr(name)}">
      <span class="mcp-dot ${chipClass}"></span>
      <span class="mcp-name mono">${escapeHtml(name)}</span>
      <span class="mcp-chip ${chipClass}">${auth}</span>
      <span class="mcp-chip">${tools.length} tools</span>
      <span class="mcp-chip">${resources.length + templates.length} resources</span>
      <span>${expanded ? "⌃" : "⌄"}</span>
    </button>
    ${detail}
  </div>`;
}
function mcpListSection(title, entries) {
  const filtered = (entries || []).filter(Boolean);
  if (!filtered.length) return "";
  return `<div class="mcp-section">
    <div class="mcp-section-title">${escapeHtml(title)}</div>
    <pre class="out">${escapeHtml(filtered.join("\n"))}</pre>
  </div>`;
}
function mcpToolName(tool) {
  return tool.title || tool.name;
}
function mcpResourceName(resource) {
  return resource.title || resource.name || resource.uri;
}
function mcpTemplateName(template) {
  return template.title || template.name || template.uri_template;
}
function mcpAuthTone(status) {
  if (status === "not_logged_in") return "warn";
  return "ok";
}
function mcpAuthLabel(status) {
  if (status === "not_logged_in") return "Needs auth";
  if (status === "bearer_token") return "Bearer token";
  if (status === "oauth") return "OAuth";
  if (status === "unsupported") return "No auth";
  return status || "Unknown";
}
$("mcpBtn").onclick = (e) => { e.stopPropagation(); toggleMcpMenu(); };
$("mcpMenu").onclick = (e) => e.stopPropagation();
document.addEventListener("click", (e) => {
  const menu = $("mcpMenu");
  if (menu.hidden) return;
  if (e.target.closest && e.target.closest(".mcp-wrap")) return;
  menu.hidden = true;
});

/* ---------- source overlay ---------- */
function projectFileUrl(kind, path) {
  return `/api/projects/${state.projectId}/${kind}?path=${encodeURIComponent(path)}`;
}
function diffStats(diff) {
  let added = 0;
  let removed = 0;
  const lines = String(diff || "").split(/\r?\n/);
  for (const line of lines) {
    if (line.startsWith("+++") || line.startsWith("---")) continue;
    if (line.startsWith("+")) added += 1;
    else if (line.startsWith("-")) removed += 1;
  }
  return { added, removed, lines: lines.length };
}
function markdownCodeFence(language, text) {
  text = String(text || "");
  const longest = (text.match(/`+/g) || []).reduce((max, run) => Math.max(max, run.length), 0);
  const fence = "`".repeat(Math.max(3, longest + 1));
  return `${fence}${language || ""}\n${text}${text.endsWith("\n") ? "" : "\n"}${fence}`;
}
async function openCodeOverlay(path, line) {
  if (!state.projectId || !path) return;
  state.codePath = path;
  state.codeLine = normalizeLine(line, null);
  $("codeOverlay").classList.add("open");
  delete $("codeOverlay").dataset.requestId;
  $("codePath").textContent = state.codeLine ? `${path}#${state.codeLine}` : path;
  $("codeMeta").textContent = "Loading…";
  $("codeView").innerHTML = `<div class="code-empty">Loading source…</div>`;
  $("codeDownload").disabled = false;

  const projectId = state.projectId;
  try {
    const res = await api("GET", projectFileUrl("highlight", path));
    if (state.codePath !== path || state.projectId !== projectId) return;
    const bits = [];
    if (res.language) bits.push(res.language);
    if (res.file_size !== undefined) bits.push(formatBytes(res.file_size));
    if (res.total_lines) bits.push(`${res.total_lines.toLocaleString()} lines`);
    $("codeMeta").textContent = bits.join(" · ") || "Source file";
    if (res.is_binary) {
      $("codeView").innerHTML = `<div class="code-empty">Binary file. Download to inspect it.</div>`;
    } else if (!res.html) {
      $("codeView").innerHTML = `<div class="code-empty">Preview unavailable for this file size. Download to inspect it.</div>`;
    } else {
      renderCodeHtml(res, state.codeLine);
    }
  } catch (e) {
    if (state.codePath !== path || state.projectId !== projectId) return;
    $("codeMeta").textContent = "Could not load source";
    $("codeView").innerHTML = `<div class="code-empty">${escapeHtml(e.message || "Could not load file.")}</div>`;
    $("codeDownload").disabled = true;
  }
}
async function openDiffOverlay(path, diff) {
  diff = String(diff || "");
  if (!state.projectId || !diff.trim()) return;
  state.codePath = null;
  state.codeLine = null;
  $("codeOverlay").classList.add("open");
  $("codePath").textContent = `Diff: ${path || "File change"}`;
  const requestId = Math.random().toString(36).slice(2);
  $("codeOverlay").dataset.requestId = requestId;
  const stats = diffStats(diff);
  $("codeMeta").textContent = `Rendering diff... +${stats.added} -${stats.removed} · ${stats.lines.toLocaleString()} lines`;
  $("codeView").innerHTML = `<div class="code-empty">Rendering diff...</div>`;
  $("codeDownload").disabled = true;

  const projectId = state.projectId;
  const markdown = markdownCodeFence("diff", diff);
  const cacheKey = projectId + "\n" + markdown;
  const apply = (html) => {
    if (!$("codeOverlay").classList.contains("open") || $("codeOverlay").dataset.requestId !== requestId || state.codePath !== null || state.projectId !== projectId) return;
    $("codeMeta").textContent = `Rendered diff · +${stats.added} -${stats.removed} · ${stats.lines.toLocaleString()} lines`;
    $("codeView").innerHTML = `<div class="diff-overlay md">${html}</div>`;
    wireCodeCopy($("codeView"));
  };

  try {
    if (state.markdownCache.has(cacheKey)) {
      apply(state.markdownCache.get(cacheKey));
      return;
    }
    const res = await api("POST", `/api/projects/${projectId}/render`, { text: markdown });
    const html = res && typeof res.html === "string" ? res.html : "";
    state.markdownCache.set(cacheKey, html);
    apply(html);
  } catch (e) {
    if (!$("codeOverlay").classList.contains("open") || $("codeOverlay").dataset.requestId !== requestId || state.codePath !== null || state.projectId !== projectId) return;
    console.warn("Giskard diff render failed; hiding raw diff preview.", e);
    $("codeMeta").textContent = "Could not render diff";
    $("codeView").innerHTML = `<div class="code-empty">Could not render diff preview.</div>`;
  }
}
function closeCodeOverlay() {
  $("codeOverlay").classList.remove("open");
  delete $("codeOverlay").dataset.requestId;
  state.codePath = null;
  state.codeLine = null;
}
function renderCodeHtml(res, targetLine) {
  const totalLines = Number(res.total_lines) || 0;
  const line = normalizeLine(targetLine, totalLines);
  const table = document.createElement("div");
  table.className = "code-table";

  const gutter = document.createElement("div");
  gutter.className = "code-line-nos";
  for (let i = 1; i <= totalLines; i++) {
    const row = document.createElement("div");
    row.className = "code-line-no" + (line === i ? " focused" : "");
    row.dataset.line = String(i);
    row.textContent = String(i);
    gutter.append(row);
  }

  const source = document.createElement("div");
  source.className = "code-source";
  source.innerHTML = res.html;

  table.append(gutter, source);
  $("codeView").replaceChildren(table);
  if (line) requestAnimationFrame(() => requestAnimationFrame(() => scrollToCodeLine(line)));
}
function scrollToCodeLine(line) {
  const view = $("codeView");
  const row = view.querySelector(`.code-line-no[data-line="${line}"]`);
  if (!row) return;
  const rowRect = row.getBoundingClientRect();
  const viewRect = view.getBoundingClientRect();
  const target = view.scrollTop + (rowRect.top - viewRect.top) - (view.clientHeight / 2) + (rowRect.height / 2);
  const max = Math.max(0, view.scrollHeight - view.clientHeight);
  view.scrollTop = Math.max(0, Math.min(max, target));
}
function normalizeLine(value, max) {
  const n = Number(value);
  if (!Number.isFinite(n) || n < 1) return null;
  const line = Math.trunc(n);
  return max && max > 0 ? Math.min(line, max) : line;
}
function formatBytes(value) {
  const n = Number(value) || 0;
  if (n >= 1024 * 1024) return (n / (1024 * 1024)).toFixed(1) + " MiB";
  if (n >= 1024) return (n / 1024).toFixed(1) + " KiB";
  return n.toLocaleString() + " B";
}
$("codeClose").onclick = closeCodeOverlay;
$("codeDownload").onclick = () => {
  if (state.projectId && state.codePath) window.location.href = projectFileUrl("raw", state.codePath);
};
$("codeOverlay").addEventListener("click", (e) => { if (e.target === $("codeOverlay")) closeCodeOverlay(); });

/* ---------- composer + controls ---------- */
function sendInput() {
  const ta = $("input"); const text = ta.value.trim(); if (!text || (!state.threadId && !isDraftThread())) return;
  if (state.activeTurn) {
    notice("Wait for the current turn to finish, or stop it first.", "warning");
    return;
  }
  if (isDraftThread()) {
    startDraftThread(text);
    return;
  }
  if (!wsIsOpen()) {
    notice(`Message not sent: WebSocket is ${state.wsStatus}.`, "warning");
    reconnectIfNeeded("send requested while disconnected");
    return;
  }
  const draftKey = composerDraftKey();
  const body = bubble("user pending","you");
  body.textContent = text;
  const msgEl = body.parentElement;
  if (!send({ type:"send_input", thread_id: state.threadId, text })) {
    msgEl.classList.remove("pending");
    msgEl.classList.add("failed");
    notice(`Message not sent: WebSocket is ${state.wsStatus}.`, "error");
    return;
  }
  setTurnActive(true);
  state.pendingUserEl = msgEl;
  state.pendingUserText = text;
  clearComposerDraft(draftKey);
}
$("sendBtn").onclick = sendInput;
$("input").addEventListener("keydown", (e) => { if (e.key==="Enter" && !e.shiftKey) { e.preventDefault(); sendInput(); } });
$("input").addEventListener("input", saveComposerDraft);

async function startDraftThread(text) {
  const pid = state.projectId;
  if (!pid || !isDraftThread()) return;
  if (!state.currentModel || !state.currentModel.provider || !state.currentModel.model) {
    notice("Choose a model before sending the first message.", "error");
    return;
  }

  const draftKey = composerDraftKey();
  const body = bubble("user pending","you");
  body.textContent = text;
  const msgEl = body.parentElement;
  state.pendingUserEl = msgEl;
  state.pendingUserText = text;
  setTurnActive(true);

  try {
    const res = await api("POST", `/api/projects/${pid}/threads/start`, {
      text,
      model_ref: state.currentModel,
      mode: state.mode || "build",
      approval_policy: state.approvalPolicy || "ask"
    });
    const tid = res && res.thread_id;
    if (!tid) throw new Error("new thread response did not include thread_id");
    state.firstTurnStartingThreadId = String(tid);
    clearComposerDraft(draftKey);
    state.draftThread = null;
    await loadThreads(pid);
    await openThread(pid, tid, "New thread", { firstTurnStarting:true });
    state.firstTurnStartingThreadId = String(tid);
    setTurnActive(true);
    if (res.warning) notice(res.warning.message || "warning", res.warning.severity || "warning");
  } catch (e) {
    msgEl.classList.remove("pending");
    msgEl.classList.add("failed");
    state.pendingUserEl = null;
    state.pendingUserText = null;
    setTurnActive(false);
    notice("Message not sent: " + e.message, "error");
  }
}

function interruptTurn() {
  if (!state.threadId || !state.activeTurn || state.interruptPending) return;
  state.interruptPending = true;
  updateComposerControls();
  if (!send({ type:"interrupt", thread_id: state.threadId })) {
    state.interruptPending = false;
    updateComposerControls();
    notice(`Interrupt not sent: WebSocket is ${state.wsStatus}.`, "error");
  }
}
$("stopBtn").onclick = interruptTurn;

function compactContext() {
  if (!state.threadId || state.compactPending) return;
  if (state.activeTurn) {
    notice("Wait for the current turn to finish before compacting context.", "warning");
    return;
  }
  state.compactPending = true;
  updateComposerControls();
  if (!send({ type:"compact_context", thread_id: state.threadId })) {
    state.compactPending = false;
    updateComposerControls();
    notice(`Compaction not started: WebSocket is ${state.wsStatus}.`, "error");
  }
}

$("modeSel").onchange = () => {
  const previous = state.mode || "build";
  const mode = $("modeSel").value === "plan" ? "plan" : "build";
  if (isDraftThread()) {
    setMode(mode);
    return;
  }
  if (!state.threadId) {
    setMode(previous);
    return;
  }
  if (send({ type:"switch_mode", thread_id: state.threadId, mode })) {
    setMode(mode);
  } else {
    setMode(previous);
    notice(`Mode not changed: WebSocket is ${state.wsStatus}.`, "error");
  }
};

$("approvalSel").onchange = () => {
  const previous = state.approvalPolicy || "ask";
  const policy = $("approvalSel").value || "ask";
  if (isDraftThread()) {
    setApprovalPolicy(policy);
    return;
  }
  if (!state.threadId) {
    setApprovalPolicy(previous);
    return;
  }
  if (send({ type:"set_approval_policy", thread_id: state.threadId, policy })) {
    setApprovalPolicy(policy);
  } else {
    setApprovalPolicy(previous);
    notice(`Approval policy not changed: WebSocket is ${state.wsStatus}.`, "error");
  }
};

function renderModelSelect() {
  const sel = $("modelSel");
  const prev = state.currentModel ? modelKey(state.currentModel) : sel.value;
  sel.innerHTML="";
  for (const m of state.models) {
    const o = document.createElement("option");
    o.value = modelKey(m);
    o.textContent = modelOptionLabel(m);
    o.dataset.provider=m.provider;
    o.dataset.model=m.model;
    o.dataset.supportsReasoningEffort = m.supports_reasoning_effort ? "true" : "false";
    sel.append(o);
  }
  if (!state.models.length) { const o=document.createElement("option"); o.textContent="(no models configured)"; sel.append(o); }
  if (prev) sel.value = prev;   // preserve the current selection across a refresh
  syncModelOptionAvailability();
  syncEffortControl();
  sel.onchange = sendSelectedModel;
}
function modelOptionLabel(m) {
  if (!m) return "Model";
  const name = m.display_name || m.model || "Model";
  return m.provider ? `${name} [${m.provider}]` : name;
}
function modelKey(m) {
  return m && m.provider && m.model ? `${m.provider}/${m.model}` : "";
}
function findModelDescriptor(provider, model) {
  return state.models.find(m => m.provider === provider && m.model === model) || null;
}
function effortOptionsForModel(desc) {
  if (!desc || !desc.supports_reasoning_effort) return [];
  if (Array.isArray(desc.reasoning_efforts) && desc.reasoning_efforts.length) {
    const known = new Map(EFFORT_OPTIONS.map(o => [o.value, o]));
    return desc.reasoning_efforts
      .map(e => known.get(String(e)) || { value:String(e), label:String(e) })
      .filter(o => o.value);
  }
  return EFFORT_OPTIONS;
}
function syncModelControls() {
  if (state.currentModel) setModel(modelKey(state.currentModel));
  syncModelOptionAvailability();
  syncEffortControl();
}
function modelProviderLocked(provider) {
  if (state.threadReadOnly) return false;
  return !!state.threadId &&
    !isDraftThread() &&
    !!state.currentModel &&
    !!state.currentModel.provider &&
    provider !== state.currentModel.provider;
}
function syncModelOptionAvailability() {
  const sel = $("modelSel"); if (!sel) return;
  const lockedProvider = state.currentModel && state.currentModel.provider;
  for (const o of sel.options) {
    if (!o.dataset || !o.dataset.provider) continue;
    const locked = modelProviderLocked(o.dataset.provider);
    o.disabled = locked;
    o.title = locked
      ? `This thread is bound to provider ${lockedProvider}. Create a new thread to use ${o.dataset.provider}.`
      : "";
  }
}
// Summarise the current model (and effort, when set) on the picker chip below the composer.
function updateModelButton() {
  const btn = $("modelPickerBtn"); if (!btn) return;
  const label = btn.querySelector(".mp-label");
  const m = state.currentModel;
  if (!m || !m.model) { label.textContent = "Model"; return; }
  const desc = findModelDescriptor(m.provider, m.model);
  let txt = modelOptionLabel(desc || m);
  // Models that support reasoning effort always show it — "Default" when left unset — so the chip
  // reflects the same two settings the popover holds. Models without an effort concept show nothing.
  if (effortOptionsForModel(desc).length) {
    const eff = EFFORT_OPTIONS.find(o => o.value === m.reasoning_effort);
    txt += " · " + (m.reasoning_effort ? (eff ? eff.label : m.reasoning_effort) : "Default");
  }
  label.textContent = txt;
}
function syncEffortControl() {
  updateModelButton();
  const control = $("effortControl");
  const sel = $("effortSel");
  const model = selectedModelFromControl();
  const desc = model ? findModelDescriptor(model.provider, model.model) : null;
  const efforts = effortOptionsForModel(desc);
  sel.innerHTML = "";
  if (!efforts.length) {
    control.hidden = true;
    return;
  }
  control.hidden = false;
  const unset = document.createElement("option");
  unset.value = "";
  unset.textContent = "Default";
  sel.append(unset);
  for (const effort of efforts) {
    const o = document.createElement("option");
    o.value = effort.value;
    o.textContent = effort.label;
    sel.append(o);
  }
  sel.value = state.currentModel && modelKey(state.currentModel) === modelKey(model)
    ? (state.currentModel.reasoning_effort || "")
    : "";
  sel.onchange = sendSelectedEffort;
}
function selectedModelFromControl() {
  const opt = $("modelSel").selectedOptions[0];
  if (!opt || !opt.dataset.model) return null;
  return { provider:opt.dataset.provider, model:opt.dataset.model };
}
function sendSelectedModel() {
  const model = selectedModelFromControl();
  if (!model) {
    syncEffortControl();
    return;
  }
  const previous = state.currentModel;
  if (modelProviderLocked(model.provider)) {
    syncModelControls();
    notice(`Create a new thread to use models from provider ${model.provider}.`, "warning");
    return;
  }
  const next = { provider:model.provider, model:model.model, reasoning_effort:null };
  state.currentModel = next;
  syncEffortControl();
  if (isDraftThread()) return;
  if (!state.threadId) return;
  state.pendingModelBeforeSelect = previous ? { ...previous } : null;
  if (!send({ type:"select_model", thread_id: state.threadId, model_ref:next })) {
    state.pendingModelBeforeSelect = null;
    state.currentModel = previous;
    syncModelControls();
    notice(`Model not changed: WebSocket is ${state.wsStatus}.`, "error");
  }
}
function sendSelectedEffort() {
  const model = selectedModelFromControl();
  if (!model) return;
  const previous = state.currentModel;
  const effort = $("effortSel").value || null;
  const next = { provider:model.provider, model:model.model, reasoning_effort:effort };
  state.currentModel = next;
  syncEffortControl();
  if (isDraftThread()) return;
  if (!state.threadId) return;
  state.pendingModelBeforeSelect = previous ? { ...previous } : null;
  if (!send({ type:"select_model", thread_id: state.threadId, model_ref:next })) {
    state.pendingModelBeforeSelect = null;
    state.currentModel = previous;
    syncModelControls();
    notice(`Reasoning effort not changed: WebSocket is ${state.wsStatus}.`, "error");
  }
}
function setModel(key) { const sel=$("modelSel"); for (const o of sel.options) if (o.value===key) { o.selected=true; break; } }

/* ---------- mobile drawers ---------- */
// The thread name lives in the sidebar (highlighted) and, on mobile where the sidebar is hidden,
// in the top bar as a "project / thread" breadcrumb that makes the current project clear.
function setThreadTitle(t) {
  const proj = state.projectNames && state.projectNames[state.projectId];
  $("mbTitle").innerHTML = proj
    ? `<span class="crumb">${escapeHtml(proj)}</span> / ${escapeHtml(t)}`
    : escapeHtml(t);
}
function closeDrawers() { $("app").classList.remove("drawer-left"); }
function toggleDrawer(side) {
  if (side !== "left") return;
  $("app").classList.toggle("drawer-left");
}
$("btnMenu").onclick = () => toggleDrawer("left");
$("backdrop").onclick = closeDrawers;
document.addEventListener("keydown", (e) => {
  if (e.key!=="Escape") return;
  if ($("codeOverlay").classList.contains("open")) closeCodeOverlay();
  else {
    closeSettingsMenu();
    closeModelPicker();
    closeTurnPicker();
    closeDrawers();
  }
});

function renderTokens(led) {
  state.tokenLedger = led && led.total ? led : null;
  if (!$("usageMenu").hidden) renderUsageMenu();
}
function renderTokenStats(led) {
  if (!led || !led.total) return `<div class="muted">No token usage recorded for this thread yet.</div>`;
  const t = led.total;
  const tile = (label, val) =>
    `<div class="stat"><div class="statlabel">${label}</div><div class="statval">${Number(val || 0).toLocaleString()}</div></div>`;
  return `<div class="stats">${tile("Cumulative Input", t.input)}${tile("Output", t.output)}${tile("Total", t.total)}</div>`;
}
function updateGauge(used, window) {
  state.contextWindow = window || state.contextWindow || 0;
  if (used !== undefined && used !== null) state.contextUsed = used;
  const w = state.contextWindow;
  const u = state.contextUsed;
  $("gauge").textContent = w
    ? `${u === null ? "…" : fmt(u)} / ${fmt(w)}`
    : `${u === null ? "…" : fmt(u)} tokens`;
  $("usageBtn").disabled = !state.threadId;
  if (!$("usageMenu").hidden) renderUsageMenu();
}
function updateGaugeFromTurns(turns) {
  if (!turns.length) {
    updateGauge(null, state.contextWindow);
    return;
  }
  const latest = turns[turns.length - 1];
  updateGaugeFromUsage(latest && latest.usage);
}
function updateGaugeFromUsage(usage) {
  if (!usage) return;
  // Codex currently exposes `last.input_tokens` rather than a dedicated context-used field;
  // input tokens are the best available proxy for current context occupancy (spec §10.3).
  const used = Number.isFinite(usage.input) ? usage.input : usage.total;
  if (Number.isFinite(used)) updateGauge(used, state.contextWindow);
}
function fmt(n) { return n>=1000 ? (n/1000).toFixed(1)+"k" : String(n); }
function usagePercent() {
  if (!state.contextWindow || state.contextUsed === null) return null;
  return Math.max(0, Math.min(100, (state.contextUsed / state.contextWindow) * 100));
}
function toggleUsageMenu() {
  const menu = $("usageMenu");
  menu.hidden = !menu.hidden;
  if (!menu.hidden) {
    $("tasksMenu").hidden = true;
    $("mcpMenu").hidden = true;
    renderUsageMenu();
  }
}
function renderUsageMenu() {
  const menu = $("usageMenu");
  const pct = usagePercent();
  const used = state.contextUsed === null ? "…" : fmt(state.contextUsed);
  const window = state.contextWindow ? fmt(state.contextWindow) : "unknown";
  const pctLabel = pct === null ? "unknown" : `${pct.toFixed(1)}%`;
  const meterWidth = pct === null ? 0 : pct;
  menu.innerHTML = `
    <div class="usage-head">
      <strong>Context Usage</strong>
      <button id="usageClose" type="button">Close</button>
    </div>
    <div class="usage-section">
      <div class="usage-section-title">Current Context</div>
      <div class="usage-line"><span class="muted">Used</span><span class="mono">${escapeHtml(used)} / ${escapeHtml(window)}</span></div>
      <div class="usage-meter" aria-hidden="true"><span style="width:${meterWidth}%"></span></div>
      <div class="usage-line"><span class="muted">Window filled</span><span class="mono">${escapeHtml(pctLabel)}</span></div>
    </div>
    <div class="usage-section">
      <div class="usage-section-title">Actions</div>
      <button id="compactBtn" class="btn" type="button" title="Compact this thread's Codex context">Compact context</button>
    </div>
    <div class="usage-section">
      <div class="usage-section-title">Cumulative Tokens</div>
      ${renderTokenStats(state.tokenLedger)}
    </div>`;
  $("usageClose").onclick = () => { $("usageMenu").hidden = true; };
  $("compactBtn").onclick = compactContext;
  updateComposerControls();
}
$("usageBtn").onclick = (e) => { e.stopPropagation(); toggleUsageMenu(); };
$("usageMenu").onclick = (e) => e.stopPropagation();
document.addEventListener("click", (e) => {
  const menu = $("usageMenu");
  if (menu.hidden) return;
  if (e.target.closest && e.target.closest(".usage-wrap")) return;
  menu.hidden = true;
});
function notice(text, severity) {
  const cls = severity===true || severity==="error" ? " err" : severity==="warning" ? " warn" : "";
  const el = document.createElement("div"); el.className="notice"+cls; el.textContent=text;
  $("notices").prepend(el); setTimeout(()=>el.remove(), 8000);
}
function escapeHtml(s){ return String(s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;"); }
function escapeAttr(s){ return escapeHtml(s).replace(/"/g,"&quot;").replace(/'/g,"&#39;"); }

/* Infinite scroll: load older history when the transcript is scrolled near the top. */
$("transcript").addEventListener("scroll", onTranscriptScroll);

/* ---------- resizable side columns (persisted client-side) ---------- */
function initResizers() {
  const app = $("app");
  const savedL = localStorage.getItem("giskard.colLeft");
  if (savedL) app.style.setProperty("--col-left", savedL);
  setupResizer($("resizeLeft"), "--col-left", "giskard.colLeft", true, 260);
}
function setupResizer(handle, cssVar, storeKey, isLeft, fallback) {
  handle.addEventListener("mousedown", (e) => {
    e.preventDefault();
    handle.classList.add("active");
    const app = $("app");
    const startX = e.clientX;
    const startW = parseInt(getComputedStyle(app).getPropertyValue(cssVar)) || fallback;
    const onMove = (ev) => {
      // Left handle grows the left column as you drag right; right handle grows the right
      // column as you drag left. Clamp so neither side can crowd out the center transcript.
      const delta = ev.clientX - startX;
      const w = Math.max(180, Math.min(560, isLeft ? startW + delta : startW - delta));
      app.style.setProperty(cssVar, w + "px");
    };
    const onUp = () => {
      handle.classList.remove("active");
      localStorage.setItem(storeKey, getComputedStyle(app).getPropertyValue(cssVar).trim());
      document.removeEventListener("mousemove", onMove);
      document.removeEventListener("mouseup", onUp);
      document.body.style.userSelect = "";
    };
    document.body.style.userSelect = "none";
    document.addEventListener("mousemove", onMove);
    document.addEventListener("mouseup", onUp);
  });
}
initResizers();

/* ---------- settings menu ---------- */
function closeSettingsMenu() {
  $("settingsMenu").hidden = true;
}
function toggleSettingsMenu() {
  const menu = $("settingsMenu");
  menu.hidden = !menu.hidden;
}
$("settingsBtn").onclick = (e) => { e.stopPropagation(); toggleSettingsMenu(); };
$("settingsMenu").onclick = (e) => e.stopPropagation();
$("settingsClose").onclick = closeSettingsMenu;

// Show the running build (git short hash, stamped into the served HTML by the server via a
// CSP-safe <meta> tag) so it's easy to confirm which Giskard version — and which cached assets —
// are live. Click to copy.
function initVersionLabel() {
  const el = $("giskardVersion");
  if (!el) return;
  const meta = document.querySelector('meta[name="giskard-version"]');
  const version = (meta && meta.content && meta.content.trim()) || "unknown";
  el.textContent = version;
  el.onclick = async (e) => {
    e.stopPropagation();
    const ok = await copyToClipboard(version);
    const prev = el.textContent;
    el.textContent = ok ? "Copied" : version;
    setTimeout(() => { el.textContent = prev; }, 1200);
  };
}
initVersionLabel();

/* ---------- turn + model pickers (below the composer) ---------- */
function closeModelPicker() { $("modelPickerMenu").hidden = true; }
function closeTurnPicker() { $("turnPickerMenu").hidden = true; }
// Only one picker open at a time: opening one closes the other.
$("modelPickerBtn").onclick = (e) => {
  e.stopPropagation();
  closeTurnPicker();
  const menu = $("modelPickerMenu");
  menu.hidden = !menu.hidden;
};
$("turnPickerBtn").onclick = (e) => {
  e.stopPropagation();
  closeModelPicker();
  const menu = $("turnPickerMenu");
  menu.hidden = !menu.hidden;
};
$("modelPickerMenu").onclick = (e) => e.stopPropagation();
$("turnPickerMenu").onclick = (e) => e.stopPropagation();
document.addEventListener("click", (e) => {
  const menu = $("modelPickerMenu");
  if (menu.hidden) return;
  if (e.target.closest && e.target.closest("#modelPicker")) return;
  menu.hidden = true;
});
document.addEventListener("click", (e) => {
  const menu = $("turnPickerMenu");
  if (menu.hidden) return;
  if (e.target.closest && e.target.closest("#turnPicker")) return;
  menu.hidden = true;
});
document.addEventListener("click", (e) => {
  const menu = $("settingsMenu");
  if (menu.hidden) return;
  if (e.target.closest && e.target.closest(".sidebar-settings")) return;
  menu.hidden = true;
});

/* ---------- appearance theme (persisted client-side) ---------- */
const APPEARANCES = ["ide","bubbles","terminal"];
function applyAppearance(a) {
  if (!APPEARANCES.includes(a)) a = "ide";
  document.documentElement.setAttribute("data-appearance", a);
  localStorage.setItem("giskard.appearance", a);
  const sel = $("appearanceSel"); if (sel) sel.value = a;
}
$("appearanceSel").onchange = () => applyAppearance($("appearanceSel").value);
applyAppearance(localStorage.getItem("giskard.appearance") || "ide");

/* Try to enter the app directly if already authenticated. */
(async () => { try { await api("GET","/api/projects"); startApp(); } catch {} })();
