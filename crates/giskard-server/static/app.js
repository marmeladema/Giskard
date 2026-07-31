"use strict";
const $ = (id) => document.getElementById(id);
const PROJECT_COLLAPSE_KEY = "giskard.collapsedProjects";
const WS_RECONNECT_BASE_MS = 600;
const WS_RECONNECT_MAX_MS = 8000;
const WS_PROBLEM_NOTICE_INTERVAL_MS = 30000;
const WS_BACKGROUND_CLOSE_GRACE_MS = 10000;
const WS_FOREGROUND_PROBE_TIMEOUT_MS = 1200;
const TRANSCRIPT_BOTTOM_STICKY_PX = 96;
// Whether the composer is on a touch/coarse-pointer device (mobile keyboard). On touch there's no
// Shift modifier reachable while typing, so the newline key fires a plain Enter; the composer must
// let it insert a newline rather than sending the half-typed message. Detected once at load and
// cached: it does not change mid-session.
//
// `pointer: coarse` is the standard media query for "no precise pointer" and is what a phone
// reports. The maxTouchPoints fallback is only used when the primary pointer is not already known
// to be fine+hover-capable, so a touch-enabled laptop (e.g. a Surface, which reports
// maxTouchPoints > 0 but has a fine, hover-capable trackpad/mouse as the primary pointer) keeps
// desktop Enter-sends behavior.
const COMPOSER_IS_TOUCH = (() => {
  try {
    if (window.matchMedia && window.matchMedia("(pointer: coarse)").matches) return true;
  } catch { /* matchMedia unsupported; fall through */ }
  // Only fall back to maxTouchPoints when we don't have a positive desktop signal. A fine, hover-
  // capable primary pointer means a desktop-class pointing device is present; treat it as desktop
  // regardless of whether the screen also happens to accept touch.
  let finePointer = false;
  let hoverCapable = false;
  try {
    if (window.matchMedia) {
      finePointer = window.matchMedia("(pointer: fine)").matches;
      hoverCapable = window.matchMedia("(hover: hover)").matches;
    }
  } catch { /* matchMedia unsupported; assume unknown */ }
  if (finePointer && hoverCapable) return false;
  try {
    if (navigator.maxTouchPoints && navigator.maxTouchPoints > 0) return true;
  } catch { /* navigator.maxTouchPoints unavailable */ }
  return false;
})();
// Hint text shown in the composer placeholder and the draft-empty state, adapting to touch vs
// desktop so the user knows how Enter behaves on their keyboard.
const COMPOSER_HINT = COMPOSER_IS_TOUCH
  ? "Tap Send to send"
  : "Enter to send, Shift+Enter for newline";
// Keep the app shell sized to the visible area above the on-screen keyboard.
//
// The bars (`#mobileBar`, `header.thr`) and the transcript are flex children of `.center`, a
// column sized to 100vh / 100dvh. On Android and Firefox the `interactive-widget=resizes-content`
// viewport meta (see index.html) makes 100dvh shrink when the keyboard opens, so the flex reflows
// and the bars stay pinned with no JS. But iOS Safari does NOT support interactive-widget: it
// overlays the keyboard without resizing the layout, then offsets the layout viewport to reveal
// the focused composer — a visual-viewport scroll that pushes the top bars off-screen, which
// position:sticky cannot counter (sticky sticks within a scroll container, not against a
// layout-viewport shift).
//
// The portable fix is to drive the shell height from window.visualViewport.height (the visible
// region above the keyboard) via a --app-height CSS variable: the whole app fits the visible area,
// so Safari has nothing to scroll away and the bars stay put. visualViewport exists on all modern
// mobile browsers (including iOS Safari 13+); where it's missing we leave the 100vh/100dvh CSS as
// the fallback. We listen on the visualViewport (not window) resize because the keyboard shrinks
// the visual viewport without firing a window resize on iOS.
//
// The visual viewport can also PAN: when iOS Safari shifts the layout viewport to reveal the
// focused composer, visualViewport.offsetTop becomes non-zero (the layout viewport's top edge
// moves down relative to the visible region). The shell is anchored to the layout-viewport top
// (top:0), so without compensation the bars would sit above the visible area and get clipped.
// We mirror offsetTop into --app-top and the CSS translates the shell by it, keeping the bars
// aligned with the visible region's top edge. (resize alone doesn't cover the pan; the scroll
// event is what fires when offsetTop changes, so both events run apply.)
(function syncAppHeight() {
  const vv = window.visualViewport;
  if (!vv) return; // older browser; fall back to the 100vh/100dvh CSS
  const transcript = document.getElementById("transcript");
  let transcriptScrollIntent = 0;
  if (transcript) {
    const recordScrollIntent = () => { transcriptScrollIntent += 1; };
    // scrollTop can change as a consequence of the flex reflow itself, so it cannot distinguish a
    // reader gesture from browser layout. Record the input events that initiate manual scrolling.
    transcript.addEventListener("wheel", recordScrollIntent, { passive:true });
    transcript.addEventListener("touchmove", recordScrollIntent, { passive:true });
    transcript.addEventListener("pointerdown", recordScrollIntent, { passive:true });
    transcript.addEventListener("keydown", recordScrollIntent);
  }
  const apply = () => {
    // Resizing the shell also shrinks #transcript, which is its own scroll container. Browsers
    // preserve that element's scrollTop, not its distance from the bottom, so a transcript that
    // was following the latest row would otherwise appear to jump backwards as the keyboard
    // opened: the newest rows remain below the shortened scrollport and look keyboard-covered.
    // Capture the existing bottom-following intent before changing the height and restore it after
    // flex layout has reflowed. Leave readers who deliberately scrolled up exactly where they are.
    const followTranscriptBottom = !!(transcript
      && transcript.scrollHeight - transcript.scrollTop - transcript.clientHeight
        <= TRANSCRIPT_BOTTOM_STICKY_PX);
    const scrollIntentAtResize = transcriptScrollIntent;
    // Use px (not vh) so it tracks the live visual viewport, not the layout viewport. Round to
    // avoid sub-pixel jitter from the fractional heights visualViewport reports. offsetTop is
    // the layout-viewport top's offset from the visual-viewport top (the pan); 0 when the
    // layout viewport isn't shifted (the common case, including desktop and Android).
    document.documentElement.style.setProperty("--app-height", Math.round(vv.height) + "px");
    document.documentElement.style.setProperty("--app-top", Math.round(vv.offsetTop) + "px");
    if (followTranscriptBottom) {
      requestAnimationFrame(() => {
        // A reader can scroll between the viewport event and this frame. Only restore the bottom
        // anchor if no newer manual scroll began in the meantime.
        if (transcriptScrollIntent === scrollIntentAtResize) {
          transcript.scrollTop = transcript.scrollHeight;
        }
      });
    }
  };
  apply();
  vv.addEventListener("resize", apply);
  // The layout can change (scroll bar appearing, orientation) without a resize; scroll is the
  // reliable signal that the visible region moved — and, crucially, the event that fires when
  // visualViewport.offsetTop changes (the iOS layout-viewport pan).
  vv.addEventListener("scroll", apply);
})();
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
// Debounce for re-fetching thread lists after activity arrives for a thread the browser has never
// seen (a sub-agent the server just materialized). Short enough that the sidebar catches up while
// the child is still blocked, long enough that a burst of child events costs one refresh.
const STALE_THREAD_LIST_REFRESH_MS = 400;
// Refresh attempts spent on any one unresolved thread id before giving up on it.
const STALE_THREAD_LIST_REFRESH_MAX_ATTEMPTS = 3;
const BROWSER_DIAGNOSTIC_VERSION = "browser-diagnostics-v1";
const MAX_ATTACHMENTS_PER_MESSAGE = 8;
const MAX_ATTACHMENT_BYTES = 25 * 1024 * 1024;
const MAX_TOTAL_ATTACHMENT_BYTES = 25 * 1024 * 1024;
const MAX_ATTACHMENT_NAME_BYTES = 255;
const MAX_ATTACHMENT_MIME_BYTES = 127;
const THREAD_DELETE_TIMEOUT_MS = 30000;
let state = {
  projectId:null, threadId:null, mode:"build", ws:null, wsStatus:"closed", wsConnectId:0,
  wsReconnectTimer:null, wsReconnectAttempt:0, wsStatusDetail:"WebSocket disconnected",
  wsLastProblem:"", wsLastProblemNotice:"", wsLastProblemNoticeAt:0,
  wsProbeTimer:null, wsProbeToken:0, wsProbeSocket:null,
  draftThread:null, firstTurnStartingThreadId:null, inputDrafts:new Map(),
  // Per-turn DOM identity (foundation for incremental reconnect): `currentRenderTurnId` is the turn
  // whose rows are being stamped right now (a persisted turn being rendered, or the live turn being
  // streamed); `newestPersistedTurnId` is the id of the newest turn known to have completed — the
  // high-water mark a future resync will use as its "give me turns after this" cursor.
  currentRenderTurnId:null, newestPersistedTurnId:null,
  globalModels:[], models:[], modelsProject:null, modelsLoadingProject:null, pendingModelBeforeSelect:null, streamEl:null, streamItemId:null, pendingUserEl:null, pendingUserText:null,
  streamElsByItemId:new Map(), renderedItemIds:new Set(), renderedHarnessItemIds:new Set(), renderedItemBodyByKey:new Map(), itemKindsByItemId:new Map(),
  pendingApprovals:new Map(), answeredApprovals:new Map(), answeredApprovalsById:new Map(), renderedApprovalStateKeys:new Set(), pendingServerRequests:new Map(), answeredServerRequests:new Set(),
  runningCommands:new Map(), commandBodyElsByItemId:new Map(), commandMsgElsByItemId:new Map(), commandStopRequestedByItemId:new Set(), selectedCommandId:null,
  commandPayloadsByItemId:new Map(), endedCommandsByItemId:new Map(),
  toolPayloadsByItemId:new Map(), toolBodyElsByItemId:new Map(),
  activeTaskGroup:null, taskGroupSeq:0, taskItemSeq:0, taskGroupsById:new Map(), taskGroupsByItemId:new Map(),
  expandedTaskGroups:new Set(), manuallyToggledTaskGroups:new Set(), expandedTaskDetails:new Map(),
  linkifyCache:new Map(), markdownCache:new Map(), codePath:null, codeLine:null, codeOverlaySource:null, outputOverlay:null, activeTurn:false, interruptPending:false, compactPending:false,
  awaitingInitialThreadState:false, awaitingThreadResync:false, awaitingIncrementalResync:false, resyncStickBottom:false, contextWindow:0, contextUsed:null, permissionPreset:"ask_first", currentModel:null,
  pendingLiveSnapshotReconcile:false,
  gitStatus:null, gitLoading:false, gitError:null, gitRequestSeq:0,
  gitExpanded:false, gitRepoByProject:new Map(), gitResizeTimer:null, gitBodyHtml:null, gitDiffPending:false, gitRefreshTimer:null,
  mcpServers:[], mcpCapabilities:{ status:false, reload:false, oauth_login:false }, mcpLoading:false, mcpError:null, expandedMcps:new Set(),
  threadReadOnly:false, readOnlyProvider:null, readOnlyMessage:null,
  pickerTypeahead:"", pickerTypeaheadTimer:null, pickerSelectedRow:null,
  currentPlan:null, planExpanded:localStorage.getItem("giskard.planExpanded")==="1",
  threadActivity:new Map(), pendingWaitingFocus:null, notifiedRequests:new Map(), bootstrapNotifiedRequests:new Set(), waitingNotifications:new Map(), browserDiagnostics:[],
  subagentImports:new Map(), projectThreads:new Map(), threadIndex:new Map(),
  lastNotificationPromptNoticeAt:0, swRegistration:null, pendingAttachments:[],
  attachmentGeneration:0, pendingAttachmentOperations:new Map(),
  collapsedProjects:new Set(loadCollapsedProjects()), pendingRemoveProject:null,
  pendingRemoveThread:null, removeThreadRequestSeq:0, projectDirs:{}
};
let attachmentIngestQueue = Promise.resolve();
const activeAttachmentReaders = new Set();
// The inline transcript row shows only a compact preview of a command's/tool's output — the most
// recent lines (its live "progress"), capped to whichever of these limits is hit first. The full
// text is always one click away in the output overlay.
const INLINE_PREVIEW_LINES = 7;
const INLINE_PREVIEW_BYTES = 2 * 1024;
const THREAD_TITLE_MAX = 120;
const EFFORT_OPTIONS = [
  { value:"minimal", label:"Minimal" },
  { value:"low", label:"Low" },
  { value:"medium", label:"Medium" },
  { value:"high", label:"High" },
  { value:"xhigh", label:"Extra High" }
];
setInterval(updateRunningCommandDurations, 1000);

async function api(method, path, body, options) {
  const opts = { method, headers:{} };
  const timeoutMs = options && Number(options.timeoutMs) > 0 ? Number(options.timeoutMs) : 0;
  let timeoutId = null;
  let timedOut = false;
  let timeoutPromise = null;
  if (body !== undefined) { opts.headers["Content-Type"]="application/json"; opts.body=JSON.stringify(body); }
  if (timeoutMs && typeof AbortController === "function") {
    const controller = new AbortController();
    opts.signal = controller.signal;
    timeoutId = setTimeout(() => {
      timedOut = true;
      controller.abort();
    }, timeoutMs);
  } else if (timeoutMs) {
    timeoutPromise = new Promise((_, reject) => {
      timeoutId = setTimeout(() => {
        timedOut = true;
        reject(new Error(`Request timed out after ${Math.round(timeoutMs / 1000)} seconds.`));
      }, timeoutMs);
    });
  }
  try {
    const fetchPromise = fetch(path, opts);
    if (timeoutPromise) fetchPromise.catch(() => {});
    const r = timeoutPromise ? await Promise.race([fetchPromise, timeoutPromise]) : await fetchPromise;
    if (!r.ok) {
      const err = new Error((await r.text()) || `HTTP ${r.status}`);
      err.status = r.status;
      throw err;
    }
    const ct = r.headers.get("content-type")||"";
    return ct.includes("json") ? r.json() : r.text();
  } catch (e) {
    if (timedOut) throw new Error(`Request timed out after ${Math.round(timeoutMs / 1000)} seconds.`);
    throw e;
  } finally {
    if (timeoutId !== null) clearTimeout(timeoutId);
  }
}
function apiFailureMessage(e) {
  const msg = e && e.message ? e.message : String(e);
  if (e && e.status === 401) {
    return "401 unauthorized. Log in again. If you are using plain HTTP, set server.secure_cookies = false and restart Giskard.";
  }
  if (msg === "Failed to fetch" || e instanceof TypeError) {
    return `${msg}. The browser could not reach Giskard for this API request. Check that the server is still running and that you are using the same URL you logged in with.`;
  }
  return msg;
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
  try { state.globalModels = (await api("GET","/api/models")).models || []; } catch { state.globalModels=[]; }
  renderModelSelect();
  await loadProjects();
  refreshModels();   // background: merge in any provider /v1/models discovery (§8.3)
}

// The global (no-project) model list: configured models merged with each `model_listing`
// provider's /v1/models discovery. Used for the startup baseline and the new-project modal. Once a
// project is open its per-project list (with harness names) is authoritative, so this does not
// clobber it. Best-effort; on failure the current list stays.
let _refreshingModels = false;
async function refreshModels(opts) {
  opts = opts || {};
  if (_refreshingModels) return;
  _refreshingModels = true;
  const btn = $("refreshModels"); if (btn) btn.disabled = true;
  try {
    const res = await api("POST","/api/models/refresh");
    if (res && Array.isArray(res.models)) {
      state.globalModels = res.models;
      populateModalModels();
    }
    // Surface per-provider discovery failures (e.g. a 401 from a misconfigured api_key) so they
    // aren't silent. Suppressed on the modal-open auto-refresh to avoid duplicate toasts.
    if (opts.announce !== false && res && Array.isArray(res.warnings)) {
      for (const w of res.warnings) notice(`Model discovery — ${w.source}: ${w.message}`, "warning");
    }
  } catch (e) {
    notice("Could not refresh models: "+e.message, "warning");
  } finally {
    _refreshingModels = false;
    if (btn) btn.disabled = false;
  }
}

// The per-project model list is authoritative when a project is open: configured models + each
// provider's /v1/models discovery + the project harness's (Codex) friendly names, all resolved
// server-side. Loaded once per project (not per thread switch — the list is the same across a
// project's threads) unless opts.force is set (the "Reload models" button). opts.announce surfaces
// discovery warnings. Best-effort; on failure the current list stays. Guards against a stale
// project's response landing after a project switch.
let _loadingProjectModels = false;
let _pendingProjectModelLoad = null;
function projectModelCatalogReady() {
  return !!state.projectId &&
    state.modelsProject === state.projectId &&
    state.modelsLoadingProject !== state.projectId;
}
function prepareProjectModelCatalog(pid) {
  if (state.modelsProject === pid) return;
  state.models = [];
  state.modelsProject = null;
  closeModelPicker();
  renderModelSelect();
  updateComposerControls();
}
async function loadProjectModels(pid, opts) {
  opts = opts || {};
  if (!pid) return;
  // A load is in flight: remember the latest requested project instead of dropping it, so switching
  // A→B while A is loading still fetches B's authoritative list once A settles.
  if (_loadingProjectModels) { _pendingProjectModelLoad = { pid, opts }; return; }
  if (!opts.force && pid === state.modelsProject) return;   // already loaded for this project
  _loadingProjectModels = true;
  if (pid === state.projectId) {
    state.modelsLoadingProject = pid;
    updateComposerControls();
  }
  const btn = $("refreshModels"); if (btn) btn.disabled = true;
  try {
    const res = await api("GET", `/api/projects/${pid}/models`);
    if (res && Array.isArray(res.models) && pid === state.projectId) {
      state.models = res.models;
      state.modelsProject = pid;
      renderModelSelect();
      updateModelButton();
    }
    // Only surface warnings/errors while `pid` is still the active project — a switch mid-request
    // must not misattribute the previous project's discovery failures to the new one.
    if (opts.announce && res && Array.isArray(res.warnings) && pid === state.projectId) {
      for (const w of res.warnings) notice(`Model discovery — ${w.source}: ${w.message}`, "warning");
    }
  } catch (e) {
    // Always surfaced for the active project, unlike the per-source discovery warnings above: those
    // are noise outside an explicit reload, but a hard failure leaves the picker with no options at
    // all. On a draft that means the project's default model is the only one available, and the
    // user cannot pick another — they need to know why rather than find an empty list.
    if (pid === state.projectId) {
      notice("Could not load this project's models: " + e.message, "warning");
    }
  } finally {
    _loadingProjectModels = false;
    if (state.modelsLoadingProject === pid) state.modelsLoadingProject = null;
    updateComposerControls();
    if (btn) btn.disabled = false;
    const pending = _pendingProjectModelLoad;
    _pendingProjectModelLoad = null;
    if (pending && pending.pid === state.projectId) {
      void loadProjectModels(pending.pid, pending.opts);
    }
  }
}
// Reload re-runs discovery and re-pulls this project's harness names for the current project.
$("refreshModels").onclick = () => loadProjectModels(state.projectId, { force:true, announce:true });

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
// Notification's onclick. The click jumps to whatever the thread is waiting on — an approval card
// or a server-request card.
function handleNotificationClick(data) {
  if (data && data.threadId && data.requestId) {
    recordNotificationDiagnostic("waiting_notify_clicked", {
      tid: data.threadId,
      request_id: data.requestId
    });
    closeWaitingNotification(data.threadId, data.requestId);
    focusWaitingRequest(data.threadId, data.requestId);
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
    btn.hidden = label === "Notifications enabled" || label === "Notifications unavailable";
  } else {
    btn.textContent = label;
    btn.title = label;
  }
  btn.disabled = !!disabled;
}

function refreshNotificationButton() {
  const buttons = notificationPermissionButtons();
  if (!buttons.length || !("Notification" in window)) return;
  let label = "Enable notifications";
  let disabled = false;
  if (!window.isSecureContext) {
    label = "Notifications require HTTPS or localhost";
    disabled = true;
  } else if (Notification.permission === "granted") {
    label = "Notifications enabled";
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
    notified_count: state.notifiedRequests.size,
    dedup_window_ms: NOTIFICATION_DEDUP_MS,
    button_count: notificationPermissionButtons().length,
    last_waiting_notification: lastNotificationDiagnostic(isWaitingNotificationDiagnostic),
    recent_waiting_notifications: recentNotificationDiagnostics(isWaitingNotificationDiagnostic, 6),
    diagnostics
  };
}

function notificationDebugSnapshot() {
  return browserDiagnosticsSnapshot();
}

function isWaitingNotificationDiagnostic(entry) {
  const reason = entry && entry.reason ? entry.reason : "";
  const detail = entry && entry.detail ? entry.detail : {};
  return reason.startsWith("waiting_notify_") ||
    (reason.startsWith("browser_notification_") && detail.kind === "waiting_request");
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

function browserNowMs() {
  return (window.performance && typeof window.performance.now === "function")
    ? window.performance.now()
    : Date.now();
}
function elapsedMsSince(startMs) {
  return Number.isFinite(startMs) ? Math.max(0, Math.round(browserNowMs() - startMs)) : null;
}
function wsReconnectDiagnostics(ws) {
  return ws && ws._giskardReconnectDiagnostics ? ws._giskardReconnectDiagnostics : null;
}
function reconnectDiagnosticBase(metrics) {
  if (!metrics) return {};
  return {
    connect_id: metrics.connectId,
    reconnect: !!metrics.reconnect,
    reason: metrics.reason || null,
    cursor: metrics.cursor || null,
    elapsed_ms: elapsedMsSince(metrics.startedAtMs)
  };
}
function recordReconnectDiagnostic(ws, reason, detail) {
  const metrics = wsReconnectDiagnostics(ws);
  if (!metrics) return;
  recordBrowserDiagnostic("websocket", reason, {
    ...reconnectDiagnosticBase(metrics),
    ...(detail || {})
  });
}
function recordReconnectMessageReceived(ws, msgType) {
  const metrics = wsReconnectDiagnostics(ws);
  if (!metrics) return;
  if (metrics.resyncComplete) return;
  if (!metrics.firstMessageAtMs) {
    metrics.firstMessageAtMs = browserNowMs();
    recordReconnectDiagnostic(ws, "ws_resync_first_message", { message_type:msgType });
  }
  recordReconnectDiagnostic(ws, "ws_resync_message_received", { message_type:msgType });
}
function recordReconnectMessageRendered(ws, msgType, startedAtMs, msg) {
  const metrics = wsReconnectDiagnostics(ws);
  if (!metrics) return;
  if (metrics.resyncComplete) return;
  const detail = {
    message_type:msgType,
    duration_ms: elapsedMsSince(startedAtMs)
  };
  if (msg && Array.isArray(msg.turns)) detail.turn_count = msg.turns.length;
  if (msgType === "live_turn_snapshot" && msg) {
    detail.accumulated_events = Array.isArray(msg.accumulated) ? msg.accumulated.length : 0;
  }
  if (msgType === "running_tasks" && msg) {
    detail.task_count = Array.isArray(msg.tasks) ? msg.tasks.length : 0;
  }
  recordReconnectDiagnostic(ws, "ws_resync_message_rendered", detail);
}
function reconnectResyncComplete(metrics, msgType) {
  if (!metrics) return false;
  if (metrics.subscribeMode === "incremental") return msgType === "running_tasks";
  if (metrics.subscribeMode === "full") return msgType === "history_page";
  return false;
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
  const lastWaiting = snapshot.last_waiting_notification;
  const waitingDetail = lastWaiting && lastWaiting.detail ? lastWaiting.detail : {};
  const lines = [
    `version: ${snapshot.version}`,
    `permission: ${snapshot.permission}`,
    `secure: ${snapshot.secure_context}`,
    `visibility: ${snapshot.visibility}`,
    `focused: ${snapshot.focused}`,
    `thread: ${snapshot.thread_id || "none"}`,
    `ws: ${snapshot.ws_status}`,
    `dedupMs: ${snapshot.dedup_window_ms}`,
    `lastRequest: ${lastWaiting ? lastWaiting.reason : "none"}`,
    `requestSource: ${waitingDetail.source || "none"}`,
    `requestId: ${waitingDetail.request_id || "none"}`,
    `last: ${last ? last.reason : "none"}`
  ];
  const recent = snapshot.recent_waiting_notifications || [];
  if (recent.length) {
    lines.push("recentRequests:");
    for (const entry of recent) {
      const detail = entry.detail || {};
      const suffix = detail.age_ms !== undefined ? ` age=${detail.age_ms}ms` : "";
      lines.push(`- ${entry.reason} source=${detail.source || "none"} id=${detail.request_id || "none"} visible=${entry.visibility} focused=${entry.focused}${suffix}`);
    }
  }
  const latest = snapshot.diagnostics.slice(-20);
  if (latest.length) {
    lines.push("recentBrowserEvents:");
    for (const entry of latest) {
      const detail = entry.detail || {};
      const fields = [];
      if (detail.source) fields.push(`source=${detail.source}`);
      if (detail.reason) fields.push(`reason=${detail.reason}`);
      if (detail.request_id !== undefined && detail.request_id !== null) fields.push(`request=${detail.request_id}`);
      if (detail.status) fields.push(`status=${detail.status}`);
      if (detail.mode) fields.push(`mode=${detail.mode}`);
      if (detail.message_type) fields.push(`message=${detail.message_type}`);
      if (detail.elapsed_ms !== undefined && detail.elapsed_ms !== null) fields.push(`elapsed=${detail.elapsed_ms}ms`);
      if (detail.duration_ms !== undefined && detail.duration_ms !== null) fields.push(`duration=${detail.duration_ms}ms`);
      if (detail.backgrounded !== undefined && detail.backgrounded !== null) fields.push(`backgrounded=${detail.backgrounded}`);
      if (detail.backgrounded_ms !== undefined && detail.backgrounded_ms !== null) fields.push(`backgrounded=${detail.backgrounded_ms}ms`);
      if (detail.timeout_ms !== undefined && detail.timeout_ms !== null) fields.push(`timeout=${detail.timeout_ms}ms`);
      if (detail.turn_count !== undefined && detail.turn_count !== null) fields.push(`turns=${detail.turn_count}`);
      if (detail.accumulated_events !== undefined && detail.accumulated_events !== null) fields.push(`events=${detail.accumulated_events}`);
      if (detail.task_count !== undefined && detail.task_count !== null) fields.push(`tasks=${detail.task_count}`);
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
  state.projectDirs = {};     // id → workspace root, for display-only relative file-change paths
  const pending = [];
  for (const p of projects) {
    state.projectNames[p.id] = p.name;
    state.projectDirs[p.id] = p.dir || "";
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
  const meta = knownProjectThreads(last.pid).find(t => String(t.id) === String(last.tid));
  if (!meta) { localStorage.removeItem("giskard.lastThread"); return; }
  openThread(last.pid, last.tid, el ? currentThreadTitle(el) : (meta.title || "Thread"), { silent:true });
}

async function loadThreads(pid) {
  const box = $("threads-"+pid); if (!box) return false;
  try {
    const { threads } = await api("GET",`/api/projects/${pid}/threads`);
    rememberProjectThreads(pid, threads);
    box.innerHTML="";
    appendThreadRows(box, pid, threads.filter(t => !t.archived && !isManagedSubagentThread(t, threads)));
    const archived = threads.filter(t => t.archived && !isManagedSubagentThread(t, threads));
    if (archived.length) {
      const label = document.createElement("div");
      label.className = "thread-section-label";
      label.textContent = "Archived";
      box.append(label);
      appendThreadRows(box, pid, archived);
    }
    // Rebuilding the rows discards the selection highlight with the old DOM. Callers that reload as
    // part of opening a thread re-derive it themselves, but a reload triggered by anything else
    // (say, catching up on a sub-agent the server just materialized) would otherwise leave the list
    // with no visibly selected thread.
    syncActiveThreadHighlight();
    return true;
  } catch {
    return false;
  }
}

function rememberProjectThreads(pid, threads) {
  if (!pid || !Array.isArray(threads)) return;
  const projectId = String(pid);
  const normalized = threads.map(t => Object.assign({}, t, {
    id:String(t.id),
    parent_thread_id:t.parent_thread_id ? String(t.parent_thread_id) : null,
    spawned_by_turn_id:t.spawned_by_turn_id ? String(t.spawned_by_turn_id) : null
  }));
  state.projectThreads.set(projectId, normalized);
  reindexProjectThreads(projectId, normalized);

  // Link results are browser-local accelerators only. Discard them when the authoritative thread
  // list reloads; a later click resolves the trusted item coordinates idempotently on the server.
  const projectPrefix = `${projectId}:`;
  for (const key of Array.from(state.subagentImports.keys())) {
    if (key.startsWith(projectPrefix)) state.subagentImports.delete(key);
  }
  renderParentThreadButton();
  renderSubagentsButton();
}

function knownProjectThreads(pid) {
  return state.projectThreads.get(String(pid || state.projectId)) || [];
}

// Thread id → { pid, thread }, rebuilt whenever a project's list is remembered. Activity hoisting
// resolves ids constantly — once per activity entry, then once per ancestor hop, then again for
// every sidebar row — so scanning each project's array per lookup is quadratic in thread count for
// a single repaint. The entries hold the same objects as `projectThreads`, so in-place edits (a
// renamed thread) stay visible through both.
function reindexProjectThreads(pid, threads) {
  const projectId = String(pid);
  for (const [tid, entry] of state.threadIndex) {
    if (entry.pid === projectId) state.threadIndex.delete(tid);
  }
  for (const thread of threads) state.threadIndex.set(String(thread.id), { pid:projectId, thread });
}

function appendThreadRows(box, pid, threads) {
  const byParent = new Map();
  const ids = new Set(threads.map(t => String(t.id)));
  const roots = [];
  for (const t of threads) {
    const parent = t.parent_thread_id ? String(t.parent_thread_id) : "";
    if (parent && ids.has(parent)) {
      if (!byParent.has(parent)) byParent.set(parent, []);
      byParent.get(parent).push(t);
    } else {
      roots.push(t);
    }
  }
  const rendered = new Set();
  const appendOne = (t) => {
    const id = String(t.id);
    if (rendered.has(id)) return;
    rendered.add(id);
    box.append(threadRow(pid, t));
    for (const child of byParent.get(id) || []) appendOne(child);
  };
  for (const t of roots) appendOne(t);
  // A corrupted parent cycle has no root and would otherwise vanish; keep every visible thread
  // rendered so malformed records stay reachable for repair or deletion.
  for (const t of threads) appendOne(t);
}

// Hide only sub-agents whose ownership chain is complete and terminates at a primary root.
// Dangling, malformed, and cyclic metadata stays in the main sidebar as a recovery path.
function isManagedSubagentThread(t, threads) {
  if (!t || t.kind !== "subagent" || !t.parent_thread_id) return false;
  const byId = new Map((threads || []).map(thread => [String(thread.id), thread]));
  const seen = new Set();
  let current = t;
  while (current) {
    const id = String(current.id || "");
    if (!id || seen.has(id)) return false;
    seen.add(id);
    const parentId = current.parent_thread_id ? String(current.parent_thread_id) : "";
    // `ThreadKind::Primary` is the serde default and may be omitted from summaries.
    if (!parentId) return !current.kind || current.kind === "primary";
    if (current.kind !== "subagent") return false;
    current = byId.get(parentId);
    if (!current) return false;
  }
  return false;
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
  markSidebarRowActive(el, !!state.threadId && String(t.id) === String(state.threadId));

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
  updateKnownThreadTitle(tid, title);
}

function updateKnownThreadTitle(tid, title) {
  const key = String(tid || "");
  if (!key || !title) return;
  for (const threads of state.projectThreads.values()) {
    const thread = threads.find(t => String(t.id) === key);
    if (thread) thread.title = title;
  }
  renderSubagentsButton();
}

function currentThreadTitle(el) {
  const title = (el.dataset && el.dataset.title ? el.dataset.title : el.textContent || "").trim();
  return title || (el.dataset.tid || "").slice(0,8);
}

function threadRowForId(tid) {
  if (!tid) return null;
  return document.querySelector(`.thread[data-tid="${tid}"]`);
}

// Resolve a thread's project, display title, and ownership. The sidebar row is the fast path, but
// managed sub-agent threads are deliberately never rendered there, so fall back to the authoritative
// per-project thread lists. Without that fallback anything keyed off a thread id — approval
// notification naming, click-to-focus, the ancestor activity badge — silently no-ops for a
// sub-agent, which is exactly the thread the user most needs to be told about.
function threadMetaForId(tid) {
  if (!tid) return null;
  const known = knownThreadMeta(tid);
  const el = threadRowForId(tid);
  if (!el) return known;
  return {
    pid: el.dataset.pid || (known && known.pid) || "",
    tid: el.dataset.tid || String(tid),
    title: currentThreadTitle(el),
    kind: (known && known.kind) || "primary",
    parent_thread_id: (known && known.parent_thread_id) || null
  };
}

// Look a thread up across every cached project list. Managed sub-agents are present here even though
// `loadThreads` filters them out of the sidebar.
function knownThreadMeta(tid) {
  const key = String(tid || "");
  if (!key) return null;
  const entry = state.threadIndex.get(key);
  if (!entry) return null;
  const thread = entry.thread;
  return {
    pid: entry.pid,
    tid: key,
    title: threadDisplayTitle(thread),
    kind: thread.kind || "primary",
    parent_thread_id: thread.parent_thread_id ? String(thread.parent_thread_id) : null
  };
}

function threadDisplayTitle(thread) {
  if (!thread) return "";
  if (thread.kind === "subagent") return subagentDisplayName(thread);
  const title = String(thread.title || "").trim();
  return title || String(thread.id || "").slice(0,8);
}

// True when `tid` sits anywhere under `ancestorId` in the ownership tree. Bounded by a seen-set so
// corrupted parent metadata (a cycle) cannot spin.
function threadDescendsFrom(tid, ancestorId) {
  const target = String(ancestorId || "");
  if (!target) return false;
  const seen = new Set([String(tid || "")]);
  let meta = knownThreadMeta(tid);
  while (meta && meta.parent_thread_id) {
    const parentId = meta.parent_thread_id;
    if (parentId === target) return true;
    if (seen.has(parentId)) return false;
    seen.add(parentId);
    meta = knownThreadMeta(parentId);
  }
  return false;
}

// The sidebar row that must display `tid`'s activity. A managed sub-agent has no row of its own, so
// its activity is hoisted to the nearest ancestor that does — otherwise a child blocked on an
// approval produces no visible signal anywhere in the sidebar.
function activityHostThreadId(tid) {
  const key = String(tid || "");
  if (!key) return null;
  if (threadRowForId(key)) return key;
  const seen = new Set([key]);
  let meta = knownThreadMeta(key);
  while (meta && meta.parent_thread_id) {
    const parentId = meta.parent_thread_id;
    if (seen.has(parentId)) return null;
    seen.add(parentId);
    if (threadRowForId(parentId)) return parentId;
    meta = knownThreadMeta(parentId);
  }
  return null;
}

// The server materializes a sub-agent thread on its own, so activity can arrive for a thread the
// browser has never listed. Refresh the cached lists once per burst so naming, ancestor badges, and
// the sub-agents menu can resolve it; without this the first approval from a brand-new child is
// still anonymous.
let staleThreadListRefreshTimer = null;
// Attempts already spent per unresolved thread id. Some ids never resolve — trailing activity from
// a deleted thread, or one the server does not list — and an ungated retry would re-fetch every
// project list on every event for the rest of that turn. A few attempts still cover the case this
// exists for: a child whose activity beats its own persistence by a moment.
const staleThreadRefreshAttempts = new Map();

function noteUnresolvedThread(tid) {
  const key = String(tid || "");
  if (!key) return;
  const attempts = staleThreadRefreshAttempts.get(key) || 0;
  if (attempts >= STALE_THREAD_LIST_REFRESH_MAX_ATTEMPTS) return;
  staleThreadRefreshAttempts.set(key, attempts + 1);
  scheduleStaleThreadListRefresh();
}

function scheduleStaleThreadListRefresh() {
  if (staleThreadListRefreshTimer) return;
  staleThreadListRefreshTimer = setTimeout(() => {
    staleThreadListRefreshTimer = null;
    refreshKnownThreadLists();
  }, STALE_THREAD_LIST_REFRESH_MS);
}

async function refreshKnownThreadLists() {
  const ids = Array.from(state.projectThreads.keys());
  await Promise.all(ids.map(pid => loadThreads(pid)));
  // Ids this refresh resolved get their budget back, so a thread that later disappears and returns
  // is not permanently barred from triggering one.
  for (const tid of Array.from(staleThreadRefreshAttempts.keys())) {
    if (knownThreadMeta(tid)) staleThreadRefreshAttempts.delete(tid);
  }
  renderAllThreadActivityIndicators();
  renderSubagentsButton();
}

function knownThreadForId(pid, tid) {
  const key = String(tid || "");
  if (!key) return null;
  return knownProjectThreads(pid).find(thread => String(thread.id) === key) || null;
}

function activeParentThread() {
  if (!state.projectId || !state.threadId) return null;
  const current = knownThreadForId(state.projectId, state.threadId);
  const parentId = current && current.parent_thread_id ? String(current.parent_thread_id) : "";
  if (!parentId) return null;
  const parent = knownThreadForId(state.projectId, parentId);
  return {
    id:parentId,
    title:(parent && parent.title) || "Parent thread"
  };
}

function renderParentThreadButton() {
  const btn = $("parentThreadBtn");
  if (!btn) return;
  const parent = activeParentThread();
  btn.hidden = !parent;
  btn.disabled = !parent;
  btn.dataset.parentThreadId = parent ? parent.id : "";
  const label = parent ? `Back to parent thread: ${parent.title}` : "Back to parent thread";
  btn.title = label;
  btn.setAttribute("aria-label", label);
}

async function openParentThread() {
  const parent = activeParentThread();
  if (!parent) return;
  const btn = $("parentThreadBtn");
  btn.disabled = true;
  try {
    await openThread(state.projectId, parent.id, parent.title);
  } finally {
    renderParentThreadButton();
  }
}

$("parentThreadBtn").onclick = openParentThread;

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
  renderSubagentsButton();
}

// A thread is *waiting on the user* when it cannot proceed until the user answers something. Codex
// splits that into approvals and server requests — and already blurs the line itself, since MCP tool
// approvals arrive as `requestUserInput` and get promoted to approval cards — but to the person
// looking at the sidebar they are one state: you are being asked for something.
function activityWaitsOnUser(activity) {
  if (!activity) return false;
  return activity.kind === "approval_requested" || activity.kind === "server_request_received";
}

// The id of whatever the thread is waiting for, whichever kind it is.
function waitingRequestId(activity) {
  if (!activity) return null;
  return activity.approval_id || activity.server_request_id || null;
}

// Urgency order used when one row has to represent both its own state and its hidden sub-agents'.
// A blocked child must never be masked by a merely-running parent, so waiting-on-the-user outranks
// error outranks running.
function threadActivityRank(activity) {
  if (!activity) return -1;
  if (activityWaitsOnUser(activity)) return 3;
  if (activity.kind === "error") return 2;
  return activity.active_turn ? 1 : 0;
}

// The activity a sidebar row must display: its own, or the most urgent one belonging to a hidden
// managed sub-agent that resolves to this row. `origin` names the descendant when the winning state
// came from one, so the row can say which sub-agent it is reporting.
// Resolve every activity entry's host row once. Each resolution walks an ownership chain, so a full
// repaint that re-resolves them per row costs rows × activities × depth; sharing this makes the
// chain walking once-per-activity for the whole repaint.
function activityHostIndex() {
  const hosts = new Map();
  for (const tid of state.threadActivity.keys()) hosts.set(tid, activityHostThreadId(tid));
  return hosts;
}

function effectiveThreadActivity(tid, hosts) {
  const key = String(tid);
  let activity = state.threadActivity.get(key) || null;
  let origin = null;
  for (const [otherId, other] of state.threadActivity) {
    if (otherId === key) continue;
    if (threadActivityRank(other) <= threadActivityRank(activity)) continue;
    const host = hosts ? hosts.get(otherId) : activityHostThreadId(otherId);
    if (host !== key) continue;
    activity = other;
    origin = otherId;
  }
  return { activity, origin };
}

function threadActivityTooltip(activity, origin) {
  const summary = (activity && activity.summary) || "Thread activity";
  if (!origin) return summary;
  const meta = threadMetaForId(origin);
  const name = (meta && meta.title) || String(origin).slice(0,8);
  return `${name}: ${summary}`;
}

function renderThreadActivityIndicator(tid, hosts) {
  const el = threadRowForId(tid);
  if (!el) {
    // A managed sub-agent has no row. Repaint the ancestor that stands in for it instead; that call
    // takes the branch below, so this cannot recurse further.
    const host = (hosts && hosts.get(String(tid))) || activityHostThreadId(tid);
    if (host && host !== String(tid)) renderThreadActivityIndicator(host, hosts);
    return;
  }
  const status = el.querySelector(".thread-status");
  if (!status) return;
  const { activity, origin } = effectiveThreadActivity(tid, hosts);
  const visible = !!activity && (activity.unread || activity.active_turn || activity.approval_id || activity.kind === "turn_completed" || activity.kind === "error");
  el.classList.toggle("has-activity", visible);
  el.classList.toggle("activity-waiting", visible && activityWaitsOnUser(activity));
  el.classList.toggle("activity-error", visible && activity && activity.kind === "error");
  el.classList.toggle("activity-running", visible && activity && activity.active_turn && !activityWaitsOnUser(activity));
  el.classList.toggle("activity-subagent", visible && !!origin);
  if (!visible) {
    status.textContent = "";
    status.title = "";
    return;
  }
  if (activityWaitsOnUser(activity)) status.textContent = "!";
  else if (activity.kind === "error") status.textContent = "x";
  else if (activity.active_turn) status.textContent = "o";
  else status.textContent = "*";
  status.title = threadActivityTooltip(activity, origin);
}

function renderAllThreadActivityIndicators() {
  const hosts = activityHostIndex();
  document.querySelectorAll(".thread").forEach(el => renderThreadActivityIndicator(el.dataset.tid, hosts));
}

// Mark a sidebar row as the selected one (or not). `aria-current` mirrors the visual state for
// assistive tech and gives the CSS a stable hook.
function markSidebarRowActive(el, active) {
  if (!el) return;
  el.classList.toggle("active", active);
  if (active) el.setAttribute("aria-current", "true");
  else el.removeAttribute("aria-current");
}

// Derive the sidebar selection from thread/draft state rather than tracking it imperatively.
// Rows are rebuilt whenever the list reloads, so any highlight set by hand goes stale on the next
// re-render; recomputing from the single source of truth keeps persisted threads and draft project
// rows mutually exclusive.
function syncActiveThreadHighlight() {
  const tid = state.threadId ? String(state.threadId) : null;
  const draftPid = isDraftThread() && state.projectId ? String(state.projectId) : null;
  document.querySelectorAll(".thread").forEach(el =>
    markSidebarRowActive(el, tid !== null && String(el.dataset.tid) === tid));
  document.querySelectorAll(".project-name").forEach(el => {
    const project = el.closest(".proj");
    markSidebarRowActive(el, draftPid !== null && project && String(project.dataset.pid) === draftPid);
  });
}

function setThreadActivity(tid, activity) {
  if (!tid || !activity) return;
  const key = String(tid);
  state.threadActivity.set(key, activity);
  renderThreadActivityIndicator(key);
  renderSubagentsButton();
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
    renderSubagentsButton();
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
  renderSubagentsButton();
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
  renderSubagentsButton();
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

function threadDescendantIds(pid, tid) {
  const childrenByParent = new Map();
  for (const thread of knownProjectThreads(pid)) {
    const parentId = thread.parent_thread_id ? String(thread.parent_thread_id) : "";
    if (!parentId) continue;
    if (!childrenByParent.has(parentId)) childrenByParent.set(parentId, []);
    childrenByParent.get(parentId).push(String(thread.id));
  }
  const rootId = String(tid);
  const seen = new Set([rootId]);
  const descendants = [];
  const pending = [...(childrenByParent.get(rootId) || [])];
  while (pending.length) {
    const childId = pending.pop();
    if (!childId || seen.has(childId)) continue;
    seen.add(childId);
    descendants.push(childId);
    pending.push(...(childrenByParent.get(childId) || []));
  }
  return descendants;
}

async function deleteThread(pid, tid, title) {
  const descendants = threadDescendantIds(pid, tid);
  const cascade = descendants.length
    ? `, its ${descendants.length} linked sub-agent thread${descendants.length === 1 ? "" : "s"}, and all corresponding Codex threads`
    : " and its corresponding Codex thread";
  // Open the modal instead of a native confirm() so the user gets the same confirmation card
  // style as project removal. The cascade description is mirrored into the dialog content.
  openRemoveThreadModal(pid, tid, title, cascade);
}

function openRemoveThreadModal(pid, tid, title, cascade) {
  const opener = threadRowForId(tid)?.closest(".thread-row")?.querySelector(".thread-menu-btn")
    || (document.activeElement instanceof HTMLElement ? document.activeElement : null);
  closeDrawers();
  closeThreadMenus();
  state.pendingRemoveThread = {
    pid,
    tid,
    title,
    descendants: threadDescendantIds(pid, tid),
    opener,
    deleting:false,
    requestSeq:0,
  };
  setRemoveThreadDeleting(false);
  $("removeThreadErr").textContent = "";
  $("removeThreadName").textContent = title || "this thread";
  $("removeThreadCascade").textContent = cascade || "";
  $("removeThreadModal").classList.add("open");
  $("removeThreadConfirm").focus();
}

function setRemoveThreadDeleting(deleting) {
  const modal = $("removeThreadModal");
  $("removeThreadConfirm").disabled = deleting;
  $("removeThreadCancel").disabled = deleting;
  modal.setAttribute("aria-busy", deleting ? "true" : "false");
}

function removeThreadFallbackFocus(pending) {
  const rowButton = pending && pending.tid
    ? threadRowForId(pending.tid)?.closest(".thread-row")?.querySelector(".thread-menu-btn")
    : null;
  return rowButton
    || (state.threadId && threadRowForId(state.threadId)?.closest(".thread-row")?.querySelector(".thread-menu-btn"))
    || $("newProj")
    || $("btnMenu");
}

function restoreRemoveThreadFocus(pending) {
  const target = pending && pending.opener && pending.opener.isConnected
    ? pending.opener
    : removeThreadFallbackFocus(pending);
  if (target && target.isConnected && typeof target.focus === "function") target.focus();
}

function closeRemoveThreadModal(options) {
  const force = options && options.force;
  const restoreFocus = !options || options.restoreFocus !== false;
  const pending = state.pendingRemoveThread;
  if (pending && pending.deleting && !force) return false;
  $("removeThreadModal").classList.remove("open");
  setRemoveThreadDeleting(false);
  state.pendingRemoveThread = null;
  if (restoreFocus) restoreRemoveThreadFocus(pending);
  return true;
}

$("removeThreadCancel").onclick = closeRemoveThreadModal;
$("removeThreadModal").addEventListener("click", (e) => {
  if (e.target === $("removeThreadModal")) closeRemoveThreadModal();
});

$("removeThreadConfirm").onclick = async () => {
  const pending = state.pendingRemoveThread;
  if (!pending || !pending.pid || !pending.tid) return;
  const { pid, tid, descendants } = pending;
  pending.deleting = true;
  pending.requestSeq = ++state.removeThreadRequestSeq;
  const requestSeq = pending.requestSeq;
  setRemoveThreadDeleting(true);
  $("removeThreadErr").textContent = "";
  try {
    await api("DELETE", `/api/projects/${pid}/threads/${tid}`, undefined, {
      timeoutMs: THREAD_DELETE_TIMEOUT_MS,
    });
    // The server cascades to descendants it discovered itself, which can include children this
    // client never listed. Decide from the refreshed authoritative list whether the active view
    // was deleted; keep the pre-request set as a fallback in case the sidebar reload fails
    // (loadThreads swallows its own errors and leaves the cached list stale).
    const deletedIds = new Set([String(tid), ...descendants]);
    const threadsRefreshed = await loadThreads(pid);
    // Read the live view only after the awaits: the user may have navigated to another thread
    // or another project meanwhile, and an unrelated active view must never be cleared.
    const activeThread = state.threadId ? String(state.threadId) : null;
    const sameProject = String(state.projectId || "") === String(pid);
    let openedDraft = false;
    if (
      activeThread && sameProject &&
      (deletedIds.has(activeThread) ||
        (threadsRefreshed && !knownThreadForId(pid, activeThread)))
    ) {
      // The active thread was just deleted (or cascaded away). Rather than leave the user staring
      // at an empty view still titled with the deleted thread's name, drop into a fresh draft in
      // the same project so the composer is ready for the next conversation. `openDraftThread`
      // tears down the WebSocket, clears `state.threadId`, and resets the title to "New thread".
      // The explicit `giskard.lastThread` removal above prevents a reload from resurrecting the
      // deleted thread (`openDraftThread` itself never touches that key; without this, reload
      // would self-heal via `restoreLastThread` failing to find the thread, but clearing it now
      // avoids the transient stale entry).
      try { localStorage.removeItem("giskard.lastThread"); } catch {}
      openDraftThread(pid);
      applyProjectDefaultModel(pid, state.draftThread);
      openedDraft = true;
    }
    pending.deleting = false;
    // Skip focus restoration only when we just opened a draft: `openDraftThread` already focuses
    // the composer input, and restoring focus to the deleted thread's row button would yank it
    // away from the input the user is now expected to type into.
    closeRemoveThreadModal({ force:true, restoreFocus: !openedDraft });
  } catch (e) {
    if (state.pendingRemoveThread === pending && pending.requestSeq === requestSeq) {
      $("removeThreadErr").textContent = "Delete thread failed: " + apiFailureMessage(e);
    }
  } finally {
    if (state.pendingRemoveThread === pending && pending.requestSeq === requestSeq) {
      pending.deleting = false;
      setRemoveThreadDeleting(false);
    }
  }
}

function clearThreadView(tid) {
  if (state.threadId !== tid) return;
  saveComposerDraft();
  try { localStorage.removeItem("giskard.lastThread"); } catch {}
  clearWsReconnectTimer();
  clearWsProbeTimer();
  const ws = state.ws;
  state.ws = null;
  if (ws) {
    ws._giskardExpectedClose = true;
    try { ws.close(); } catch {}
  }
  state.projectId = null; state.threadId = null;
  renderParentThreadButton();
  state.draftThread = null;
  state.firstTurnStartingThreadId = null;
  state.pendingUserEl = null; state.pendingUserText = null;
  state.compactPending = false;
  state.currentModel = null;
  $("effortControl").hidden = true;
  setTurnActive(false);
  state.awaitingInitialThreadState = false;
  state.awaitingThreadResync = false;
  state.awaitingIncrementalResync = false;
  state.resyncStickBottom = false;
  state.pendingLiveSnapshotReconcile = false;
  resetGitState();
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
  for (const m of state.globalModels) {
    const o = document.createElement("option");
    o.value = `${m.provider}/${m.model}`; o.textContent = modelOptionLabel(m);
    o.dataset.provider = m.provider; o.dataset.model = m.model;
    sel.append(o);
  }
  if (!state.globalModels.length) { const o=document.createElement("option"); o.textContent="(no models configured)"; sel.append(o); }
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
  catch (e) { $("pmErr").textContent = "Cannot open folder: "+apiFailureMessage(e); return; }
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
  } catch (e) { $("pmErr").textContent = "Create folder failed: "+apiFailureMessage(e); }
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
    const { id } = await api("POST","/api/projects",{ name, dir, default_model:model });
    closeProjectModal();
    await loadProjects();
    // Land on the new project's draft view rather than leaving the previously
    // selected thread on screen. `newThread` opens the draft synchronously and
    // resolves the project's default model in the background (LT6–LT9), the same
    // path the per-project "+" button uses.
    newThread(id);
  } catch (e) { $("pmErr").textContent = "Create project failed: "+apiFailureMessage(e); }
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

// Open the draft immediately, then resolve the project's default model in the background.
//
// Fetching the project first left the *previous* thread on screen for a network round-trip, with
// its composer visible and editable. Anything typed in that window was destroyed when
// `openDraftThread` finally ran and reset the composer, and the Send that followed found an empty
// box and returned silently — so the click read as "nothing happened" and the message was gone.
// Nothing about drawing a draft needs the project record; only the model does.
function newThread(pid) {
  openDraftThread(pid);
  applyProjectDefaultModel(pid, state.draftThread);
}

async function applyProjectDefaultModel(pid, draft) {
  let project = null;
  let failure = null;
  try {
    project = await api("GET", `/api/projects/${pid}`);
  } catch (e) {
    failure = "the project could not be loaded: " + e.message;
  }
  // Only apply the default if this is still the same untouched draft. The draft is interactive from
  // the moment it opens, so while this was in flight the user may have sent it, switched away,
  // opened another one — or picked a model themselves. Their choice wins: the project default is a
  // starting point, not an override, and replacing it would run the turn on a model they did not
  // pick. A pinned draft already left `modelLoading` false, so there is nothing to settle here.
  if (!draft || state.draftThread !== draft || draft.modelPinned) return;

  const model = project && project.default_model;
  if (!failure && (!model || !model.provider || !model.model)) {
    failure = "this project has no default model";
  }
  draft.modelLoading = false;
  draft.modelError = failure;
  if (!failure) state.currentModel = normalizeDraftModel(model);
  syncModelControls();
  updateComposerControls();
  if (failure) notice("Cannot start a thread here — " + failure + ".", "error");
}

// Remember that the user chose this draft's model, so a late project default cannot replace it —
// and that the draft now has an authoritative model, so it can be sent.
function pinDraftModel() {
  if (!state.draftThread) return;
  state.draftThread.modelPinned = true;
  state.draftThread.modelLoading = false;
  state.draftThread.modelError = null;
  // Picking a model is what resolves a draft that was waiting on — or failed to get — its default,
  // so the Send control has to be re-evaluated here; nothing else on this path does it.
  updateComposerControls();
}

// A draft cannot send until its model has resolved to a real one.
//
// While the project's default is in flight `state.currentModel` is null rather than a placeholder:
// starting the first turn on a fallback would bind the thread to the wrong provider, and switching
// a started thread across providers is not allowed, so it is not recoverable (LT7).
function draftModelUnresolved() {
  if (!isDraftThread()) return false;
  return !state.currentModel || !state.currentModel.provider || !state.currentModel.model;
}

// Why the first send is unavailable: still resolving, or resolved to nothing usable.
function draftModelUnavailableReason() {
  const draft = state.draftThread;
  if (draft && draft.modelError) {
    return `Cannot start a thread here — ${draft.modelError}. Pick a model to continue.`;
  }
  return "Loading this project's model…";
}

// Null for anything that is not a real model, never a stand-in. A draft with no model simply
// cannot be sent (LT7); inventing one here is how a thread ends up bound to the wrong provider.
function normalizeDraftModel(model) {
  if (!model || !model.provider || !model.model) return null;
  return {
    provider:String(model.provider),
    model:String(model.model),
    reasoning_effort:model.reasoning_effort || null
  };
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
  // Last: clearing the attachments refreshes the Send button, and it has to see the restored text
  // rather than the outgoing thread's. Callers happen to refresh again afterwards (a draft when its
  // project's default model lands, a thread when its socket reports status), so the order here is
  // not currently observable — but the button's answer should not depend on that.
  clearPendingAttachments();
}

function clearComposerDraft(key) {
  if (key) state.inputDrafts.delete(key);
  const input = $("input");
  if (input && composerDraftKey() === key) input.value = "";
}

// The transcript of a brand-new (unsent) thread is empty, which left users unsure what the view is
// or what to do. Fill it with a centered explainer: what a draft thread is, and that sending the
// first message creates it. Cleared as soon as a real row is appended (see startDraftThread) or the
// user opens another thread (openThread rebuilds the transcript).
function renderDraftPlaceholder() {
  $("transcript").innerHTML =
    '<div class="draft-empty"><div class="draft-empty-inner">' +
    '<div class="draft-empty-icon" aria-hidden="true">✏️</div>' +
    '<h2 class="draft-empty-title">Start a new thread</h2>' +
    "<p class=\"draft-empty-text\">This is a <strong>draft</strong> — nothing is saved yet. " +
    "Type your first message in the composer below and send it to create the thread and start the conversation.</p>" +
    '<p class="draft-empty-hint">Pick a model and mode above the composer first. ' +
    // The Enter behaviour adapts to touch vs desktop (see COMPOSER_HINT). On touch the newline
    // key inserts a line and the Send button sends; on desktop Enter sends and Shift+Enter adds a line.
    (COMPOSER_IS_TOUCH
      ? "Tap <kbd>Send</kbd> to send · <kbd>Enter</kbd> adds a newline.</p>"
      : "<kbd>Enter</kbd> sends · <kbd>Shift</kbd>+<kbd>Enter</kbd> adds a newline.</p>") +
    "</div></div>";
}
function openDraftThread(pid) {
  saveComposerDraft();
  clearWsReconnectTimer();
  clearWsProbeTimer();
  const oldWs = state.ws;
  state.ws = null;
  if (oldWs) {
    oldWs._giskardExpectedClose = true;
    try { oldWs.close(); } catch {}
  }

  state.projectId = pid;
  state.threadId = null;
  renderParentThreadButton();
  // `modelLoading` until the project's default arrives; `currentModel` stays null until then so a
  // placeholder can never reach `threads/start` (LT7).
  state.draftThread = { projectId:pid, title:"New thread", modelLoading:true, modelError:null };
  state.firstTurnStartingThreadId = null;
  state.pendingUserEl = null;
  state.pendingUserText = null;
  state.compactPending = false;
  state.currentModel = null;
  prepareProjectModelCatalog(pid);
  resetGitState();
  state.mcpServers = []; state.mcpError = null; state.expandedMcps = new Set();
  state.mcpCapabilities = { status:false, reload:false, oauth_login:false };
  $("tasksMenu").hidden = true;
  $("subagentsMenu").hidden = true;
  $("mcpMenu").hidden = true;
  $("usageMenu").hidden = true;
  renderMcpButton();
  renderSubagentsButton();
  loadProjectModels(pid);   // load this project's model list (config + discovery + Codex names)
  setMode("build");
  setPermissionPreset("ask_first");
  setTurnActive(false);
  state.historyLoaded = false; state.oldestTurnId = null; state.hasMoreHistory = false;
  state.loadingHistory = false; state.pendingOlder = false; state.autoFilledTurns = 0;
  state.currentRenderTurnId = null; state.newestPersistedTurnId = null;
  state.contextUsed = null; state.contextWindow = 0; state.tokenLedger = null;
  updateGauge(null, 0);
  state.awaitingInitialThreadState = false;
  state.awaitingThreadResync = false;
  state.awaitingIncrementalResync = false;
  state.resyncStickBottom = false;
  state.pendingLiveSnapshotReconcile = false;
  resetRenderState();
  syncActiveThreadHighlight();   // state.threadId is null for a draft, so this clears any selection
  $("thrHeader").style.display="flex"; $("composer").style.display="flex";
  $("pickerBar").style.display="flex";
  setThreadTitle("New thread");
  $("transcript").className=""; $("notices").innerHTML="";
  renderDraftPlaceholder();
  setWsStatus("draft", "Draft thread. Send a message to create it.");
  loadGitStatus(pid);
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
  if (opts.focusRequestId) {
    state.pendingWaitingFocus = {
      threadId:String(tid),
      requestId:String(opts.focusRequestId),
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
  renderParentThreadButton();
  state.threadReadOnly = false; state.readOnlyProvider = null; state.readOnlyMessage = null;
  updateReadOnlyBanner();
  state.draftThread = null;
  state.compactPending = false;
  state.currentModel = null;
  prepareProjectModelCatalog(pid);
  $("effortControl").hidden = true;
  resetGitState();
  state.mcpServers = []; state.mcpError = null; state.expandedMcps = new Set();
  state.mcpCapabilities = { status:false, reload:false, oauth_login:false };
  $("tasksMenu").hidden = true;
  $("subagentsMenu").hidden = true;
  $("mcpMenu").hidden = true;
  $("usageMenu").hidden = true;
  renderMcpButton();
  renderSubagentsButton();
  loadGitStatus(pid);
  loadMcpServers({ announce:false });
  loadProjectModels(pid);   // load this project's model list (config + discovery + Codex names)
  setTurnActive(false);
  state.historyLoaded = false; state.oldestTurnId = null; state.hasMoreHistory = false;
  state.loadingHistory = false; state.pendingOlder = false; state.autoFilledTurns = 0;
  state.currentRenderTurnId = null; state.newestPersistedTurnId = null;
  state.contextUsed = null; state.contextWindow = 0; state.tokenLedger = null;
  updateGauge(null, 0);
  state.awaitingInitialThreadState = true;
  state.awaitingThreadResync = false;
  state.awaitingIncrementalResync = false; state.resyncStickBottom = false;
  state.pendingLiveSnapshotReconcile = false;
  resetRenderState();
  syncActiveThreadHighlight();
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
  schedulePendingWaitingFocus();
}

function clearWsReconnectTimer() {
  if (state.wsReconnectTimer) {
    clearTimeout(state.wsReconnectTimer);
    state.wsReconnectTimer = null;
  }
}
function clearWsProbeTimer() {
  if (state.wsProbeTimer) {
    clearTimeout(state.wsProbeTimer);
    state.wsProbeTimer = null;
  }
  state.wsProbeSocket = null;
}
function wsIsOpen() {
  return !!(state.ws && state.ws.readyState === WebSocket.OPEN);
}
function wsCanSend() {
  return wsIsOpen() && !state.wsProbeTimer;
}
function wsReadyStateLabel(ws) {
  if (!ws) return "none";
  switch (ws.readyState) {
    case WebSocket.CONNECTING: return "connecting";
    case WebSocket.OPEN: return "open";
    case WebSocket.CLOSING: return "closing";
    case WebSocket.CLOSED: return "closed";
    default: return String(ws.readyState);
  }
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
    connectWs({ reconnect:true, reason:message });
  }, delay + jitter);
}
async function connectWs(opts) {
  opts = opts || {};
  if (!state.threadId) {
    setWsStatus("closed", "No thread selected.");
    return;
  }
  clearWsReconnectTimer();
  clearWsProbeTimer();
  state.wsProbeToken++;
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
  const connectStartedAtMs = browserNowMs();
  const reconnectMetrics = {
    connectId,
    reconnect: !!opts.reconnect,
    reason: opts.reason || null,
    cursor: state.newestPersistedTurnId || null,
    startedAtMs: connectStartedAtMs,
    firstMessageAtMs: 0
  };
  recordBrowserDiagnostic("websocket", "ws_connect_started", reconnectDiagnosticBase(reconnectMetrics));
  let ticket;
  try {
    ticket = (await api("GET","/api/ws-ticket")).ticket;
  } catch (e) {
    if (connectId !== state.wsConnectId) return;
    recordBrowserDiagnostic("websocket", "ws_ticket_failed", {
      ...reconnectDiagnosticBase(reconnectMetrics),
      error:e && e.message ? e.message : String(e)
    });
    const message = "WebSocket authorization failed: "+e.message;
    setWsStatus("reconnecting", message);
    surfaceWsProblem(message, "error");
    scheduleWsReconnect(message);
    return;
  }
  if (connectId !== state.wsConnectId) return;
  recordBrowserDiagnostic("websocket", "ws_ticket_received", reconnectDiagnosticBase(reconnectMetrics));
  const ws = new WebSocket(`${proto}://${location.host}/api/ws?ticket=${encodeURIComponent(ticket)}`);
  state.ws = ws;
  ws._giskardReconnectDiagnostics = reconnectMetrics;
  ws._giskardBackgroundedAt = document.visibilityState === "hidden" ? Date.now() : 0;
  recordReconnectDiagnostic(ws, "ws_socket_created", { ready_state:wsReadyStateLabel(ws) });
  ws.onopen = () => {
    if (state.ws !== ws) return;
    state.wsReconnectAttempt = 0;
    state.wsLastProblem = "";
    setWsStatus("open", "Connected to agent.");
    markWsForegroundRecovered(ws);
    recordReconnectDiagnostic(ws, "ws_socket_open", { ready_state:wsReadyStateLabel(ws) });
    // Incremental resync: if we already have persisted history rendered, ask only for the turns
    // after our newest one (`since`). The server replies with a HistoryDelta and we keep the
    // immutable completed-turn DOM. If a live snapshot follows, the stale live DOM stays visible
    // until the snapshot handler replaces it. Without a cursor (nothing rendered yet) fall back to
    // a full resync that rewrites the transcript.
    if (state.newestPersistedTurnId) {
      state.awaitingIncrementalResync = true;
      state.awaitingThreadResync = false;
      const sent = send({ type:"subscribe", thread_id: state.threadId, since: state.newestPersistedTurnId });
      reconnectMetrics.subscribeMode = "incremental";
      recordReconnectDiagnostic(ws, "ws_subscribe_sent", { mode:"incremental", sent });
    } else {
      state.awaitingThreadResync = true;
      state.awaitingIncrementalResync = false;
      const sent = send({ type:"subscribe", thread_id: state.threadId });
      reconnectMetrics.subscribeMode = "full";
      recordReconnectDiagnostic(ws, "ws_subscribe_sent", { mode:"full", sent });
    }
  };
  ws.onmessage = (m) => {
    if (state.ws !== ws) return;
    markWsForegroundRecovered(ws);
    try {
      handleServer(JSON.parse(m.data), ws);
    } catch (e) {
      notice("Invalid WebSocket message from server: "+e.message, "error");
    }
  };
  ws.onerror = () => {
    if (state.ws !== ws) return;
    ws._giskardHadError = true;
    recordReconnectDiagnostic(ws, "ws_socket_error", { ready_state:wsReadyStateLabel(ws) });
    recordWsProblem("WebSocket connection failed. Reconnecting...");
  };
  ws.onclose = (ev) => {
    if (state.ws !== ws) return;
    clearWsProbeTimer();
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
    recordReconnectDiagnostic(ws, "ws_socket_closed", {
      code:ev.code || null,
      reason:ev.reason || null,
      had_error:!!ws._giskardHadError,
      backgrounded,
      backgrounded_ms:backgroundedAt ? Math.max(0, Date.now() - backgroundedAt) : null
    });
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
  const attachmentsLoading = pendingAttachmentOperationCount() > 0;
  const attachmentInputAllowed = hasThreadSurface && !readOnly && !(draft && state.activeTurn);
  const modelUnresolved = draftModelUnresolved();
  // An empty composer with nothing attached has nothing to send. That was previously a silent
  // early return in `sendInput`: the button looked live, the click did nothing, and no message
  // said why — so a composer emptied unexpectedly (as one used to be by a draft opening mid-typing)
  // read as a dead button. Disabling it puts the state on screen instead.
  const nothingToSend = !$("input").value.trim() && state.pendingAttachments.length === 0;
  $("sendBtn").disabled =
    readOnly || state.activeTurn || attachmentsLoading || modelUnresolved || nothingToSend ||
    !hasThreadSurface || (!ready && !draft);
  // The send arrow and the stop square share one slot: hide the arrow while a turn is running so
  // only the red stop square is visible (no disabled send button alongside it).
  $("sendBtn").hidden = state.activeTurn && !draft;
  $("sendBtn").title = readOnly ? "Read-only thread — pick a model from a configured provider to reactivate it." :
    attachmentsLoading ? "Wait for attached files to finish loading." :
    modelUnresolved ? draftModelUnavailableReason() :
    nothingToSend ? "Type a message, or attach a file, to send." : "Send";
  $("stopBtn").hidden = !state.activeTurn || draft;
  $("stopBtn").disabled = !ready || state.interruptPending;
  // The stop button shows a Unicode black square (■) glyph; the "stopping" state is conveyed via
  // the disabled state + tooltip since the glyph itself carries no text to swap out.
  const stopLabel = state.interruptPending ? "Stopping the current turn…" : "Interrupt the running turn";
  $("stopBtn").title = stopLabel;
  // Keep the accessible name in sync with the title so assistive tech announces "Stopping…"
  // rather than the static markup label while an interrupt is pending.
  $("stopBtn").setAttribute("aria-label", stopLabel);
  $("attachBtn").disabled = !attachmentInputAllowed;
  const modelCatalogReady = projectModelCatalogReady();
  $("modelSel").disabled = !hasThreadSurface || !modelCatalogReady || (!ready && !draft);
  $("modelPickerBtn").disabled = !hasThreadSurface || !modelCatalogReady || (!ready && !draft);
  $("effortSel").disabled = !hasThreadSurface || !modelCatalogReady || (!ready && !draft);
  const compactBtn = $("compactBtn");
  if (compactBtn) {
    compactBtn.disabled = !state.threadId || draft || state.activeTurn || state.compactPending || !ready;
    compactBtn.textContent = state.compactPending ? "Compacting..." : "Compact context";
  }
  $("input").disabled = !hasThreadSurface || readOnly;
  $("input").placeholder =
    readOnly ? "Read-only thread — pick a model above to reactivate it." :
    state.activeTurn ? "Draft your next message…" :
    draft ? `Ask Giskard…  (${COMPOSER_HINT})` :
    state.wsStatus==="open" ? `Ask Giskard…  (${COMPOSER_HINT})` :
    state.wsStatus==="connecting" ? "Connecting to agent…" :
    state.wsStatus==="reconnecting" ? "Reconnecting… keep drafting here." :
    "Disconnected from agent.";
  $("permissionPresetSel").disabled = !hasThreadSurface || (!ready && !draft);
  $("modeSel").disabled = !hasThreadSurface || (!ready && !draft);
  $("turnPickerBtn").disabled = !hasThreadSurface || (!ready && !draft);
}
function setTurnActive(active) {
  state.activeTurn = active;
  if (!active) state.interruptPending = false;
  updateComposerControls();
}
function send(obj) {
  if (wsCanSend()) {
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
function probeWsBeforeReconnect(reason) {
  if (!state.threadId) return;
  const ws = state.ws;
  if (!ws || ws.readyState !== WebSocket.OPEN) {
    reconnectIfNeeded(reason);
    return;
  }
  clearWsProbeTimer();
  const token = ++state.wsProbeToken;
  state.wsProbeSocket = ws;
  ws._giskardProbeStartedAtMs = browserNowMs();
  const backgroundedAt = Number(ws._giskardBackgroundedAt) || 0;
  setWsStatus("reconnecting", "Checking connection...");
  recordBrowserDiagnostic("websocket", "ws_probe_started", {
    reason:reason || "probe requested",
    ready_state:wsReadyStateLabel(ws),
    timeout_ms:WS_FOREGROUND_PROBE_TIMEOUT_MS,
    backgrounded_ms:backgroundedAt ? Math.max(0, Date.now() - backgroundedAt) : null
  });
  try {
    ws.send(JSON.stringify({ type:"ping" }));
  } catch (e) {
    recordBrowserDiagnostic("websocket", "ws_probe_send_failed", {
      reason:reason || "probe requested",
      elapsed_ms: elapsedMsSince(ws._giskardProbeStartedAtMs),
      error:e && e.message ? e.message : String(e)
    });
    connectWs({ reconnect:true, reason });
    return;
  }
  state.wsProbeTimer = setTimeout(() => {
    if (state.ws !== ws || state.wsProbeToken !== token) return;
    clearWsProbeTimer();
    recordBrowserDiagnostic("websocket", "ws_probe_timeout", {
      reason:reason || "probe requested",
      timeout_ms:WS_FOREGROUND_PROBE_TIMEOUT_MS,
      elapsed_ms: elapsedMsSince(ws._giskardProbeStartedAtMs),
      ready_state:wsReadyStateLabel(ws)
    });
    state.wsReconnectAttempt = 0;
    connectWs({ reconnect:true, reason });
  }, WS_FOREGROUND_PROBE_TIMEOUT_MS);
}
function finishWsProbe(ws, reason) {
  if (!state.wsProbeTimer || state.wsProbeSocket !== ws) return;
  recordBrowserDiagnostic("websocket", reason, {
    elapsed_ms: elapsedMsSince(ws._giskardProbeStartedAtMs),
    ready_state:wsReadyStateLabel(ws)
  });
  clearWsProbeTimer();
  markWsForegroundRecovered(ws);
  setWsStatus("open", "Connected to agent.");
}
function handleWsPong(ws) {
  finishWsProbe(ws, "ws_probe_pong");
}
document.addEventListener("visibilitychange", () => {
  if (document.visibilityState === "hidden") {
    if (state.ws) state.ws._giskardBackgroundedAt = Date.now();
    return;
  }
  if (state.ws && state.ws._giskardBackgroundedAt) {
    state.ws._giskardResumedAt = Date.now();
    probeWsBeforeReconnect("tab visible");
    return;
  }
  reconnectIfNeeded("tab visible");
});
window.addEventListener("online", () => {
  probeWsBeforeReconnect("network online");
});
window.addEventListener("offline", () => {
  if (!state.threadId || state.wsStatus === "closed") return;
  clearWsReconnectTimer();
  clearWsProbeTimer();
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

function handleServer(msg, ws) {
  if (msg && msg.type === "pong") {
    handleWsPong(ws);
    return;
  }
  finishWsProbe(ws, "ws_probe_message");
  if (msg && msg.type === "thread_activity_bootstrap") {
    handleThreadActivityBootstrap(msg);
    return;
  }
  if (msg && msg.type === "thread_activity") {
    handleThreadActivity(msg);
    return;
  }
  if (!isCurrentThreadServerMessage(msg)) return;
  const messageType = msg && msg.type ? msg.type : "unknown";
  const renderStartedAtMs = browserNowMs();
  recordReconnectMessageReceived(ws, messageType);
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
  recordReconnectMessageRendered(ws, messageType, renderStartedAtMs, msg);
  const metrics = wsReconnectDiagnostics(ws);
  if (reconnectResyncComplete(metrics, messageType)) {
    recordReconnectDiagnostic(ws, "ws_resync_complete", {});
    metrics.resyncComplete = true;
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
    source: msg.source || "thread_activity",
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
  // Activity for a thread we have never listed means the server materialized it after our last
  // load — almost always a sub-agent. Catch the list up so this activity can be attributed and
  // hoisted onto a visible ancestor.
  if (!knownThreadMeta(tid)) noteUnresolvedThread(tid);
  renderThreadActivityIndicator(tid);
  renderSubagentsButton();
  if (activityWaitsOnUser(activity)) maybeNotifyWaitingRequest(tid, activity);
}

// Undo a notification claim when the notification did not actually reach the user. Both records
// must be released together: leaving the session-scoped one set would silence every later replay of
// a request the user was never shown.
function releaseWaitingNotificationClaim(notificationKey) {
  state.notifiedRequests.delete(notificationKey);
  state.bootstrapNotifiedRequests.delete(notificationKey);
}

// Cross-thread activity the server replayed because we were not connected when it happened. Paints
// the badge exactly like a live event; notification is gated separately, because a reconnect is not
// news — see `maybeNotifyWaitingRequest`.
function handleThreadActivityBootstrap(msg) {
  const activities = (msg && Array.isArray(msg.activities)) ? msg.activities : [];
  if (!activities.length) return;
  recordNotificationDiagnostic("activity_bootstrap_received", { count: activities.length });
  for (const entry of activities) {
    handleThreadActivity(Object.assign({}, entry, { source:"connect_bootstrap" }));
  }
}

async function maybeNotifyWaitingRequest(tid, activity) {
  const notificationKey = waitingNotificationKey(tid, waitingRequestId(activity));
  if (!activityWaitsOnUser(activity) || !notificationKey) {
    recordNotificationDiagnostic("waiting_notify_skipped_invalid_call", { tid, activity });
    return;
  }
  recordNotificationDiagnostic("waiting_notify_received", {
    tid,
    request_id: waitingRequestId(activity),
    source: activity.source || "unknown",
    summary: activity.summary || ""
  });
  const focused = document.hasFocus ? document.hasFocus() : true;
  if (document.visibilityState === "visible" && focused && String(tid) === String(state.threadId)) {
    recordNotificationDiagnostic("waiting_notify_suppressed_visible_current_thread", {
      tid,
      request_id: waitingRequestId(activity),
      source: activity.source || "unknown"
    });
    return;
  }
  if (!("Notification" in window)) {
    recordNotificationDiagnostic("waiting_notify_suppressed_unsupported", {
      tid,
      request_id: waitingRequestId(activity),
      source: activity.source || "unknown"
    });
    return;
  }
  if (Notification.permission !== "granted") {
    recordNotificationDiagnostic("waiting_notify_suppressed_permission", {
      tid,
      request_id: waitingRequestId(activity),
      source: activity.source || "unknown"
    });
    maybeNoticeNotificationPermission();
    return;
  }
  // A replayed request is not news. `notifiedRequests` cannot answer "have we ever alerted for
  // this?" because it is pruned on a 15s window, so a laptop resuming repeatedly would re-alert for
  // the same blocked approval. Track bootstrap alerts separately and permanently for this page
  // session: a reconnect stays silent, while a genuine reload starts a new session and re-alerts.
  if (activity.source === "connect_bootstrap" && state.bootstrapNotifiedRequests.has(notificationKey)) {
    recordNotificationDiagnostic("waiting_notify_suppressed_replay", {
      tid,
      request_id: waitingRequestId(activity)
    });
    return;
  }
  const now = Date.now();
  pruneNotificationDedup(now);
  const notifiedAt = state.notifiedRequests.get(notificationKey);
  if (notifiedAt && now - notifiedAt < NOTIFICATION_DEDUP_MS) {
    recordNotificationDiagnostic("waiting_notify_suppressed_duplicate", {
      tid,
      request_id: waitingRequestId(activity),
      source: activity.source || "unknown",
      age_ms: now - notifiedAt
    });
    return;
  }
  // Claim the dedup key before any await. Two events can describe the same approval (the live
  // activity broadcast and the live-turn snapshot path), and with the refresh below between the
  // check above and the record after the notification, both would clear the gate and notify.
  // Every path that returns without notifying releases it again.
  state.notifiedRequests.set(notificationKey, now);
  state.bootstrapNotifiedRequests.add(notificationKey);
  // A sub-agent's very first approval routinely arrives before the browser has listed the thread
  // the server just materialized. Resolve it now rather than shipping an unattributable id prefix —
  // this notification is the only signal the user gets for a thread with no sidebar row.
  if (!threadMetaForId(tid)) await refreshKnownThreadLists();
  const meta = threadMetaForId(tid);
  const title = waitingNotificationLabel(meta, tid);
  const isSubagent = !!meta && meta.kind === "subagent";
  // Stable per-request tag: it dedups at the OS level and lets us close the notification by tag on
  // the service-worker path (where we never hold a Notification object) when the request resolves.
  const notificationTag = waitingNotificationTag(tid, waitingRequestId(activity));
  // The two kinds share the *state*, not the wording: telling someone an approval is needed when
  // the agent asked them a question sends them looking for a decision that does not exist.
  const needed = activity.kind === "approval_requested" ? "approval" : "input";
  const headline = isSubagent ? `Giskard: sub-agent ${needed} needed` : `Giskard: ${needed} needed`;
  let result;
  try {
    result = await showAppNotification(headline, {
      body: activity.summary ? `${title}: ${activity.summary}` : title,
      tag: notificationTag,
      renotify: true,
      requireInteraction: true,
      data: { threadId:tid, requestId:waitingRequestId(activity) }
    }, {
      kind: "waiting_request",
      tid,
      request_id: waitingRequestId(activity),
      source: activity.source || "unknown",
      tag: notificationTag
    });
  } catch (e) {
    releaseWaitingNotificationClaim(notificationKey);
    recordNotificationDiagnostic("waiting_notify_constructor_failed", {
      tid,
      request_id: waitingRequestId(activity),
      source: activity.source || "unknown",
      error: e && e.message ? e.message : String(e)
    });
    console.warn("Giskard notification failed", e);
    return;
  }
  if (!result) {
    releaseWaitingNotificationClaim(notificationKey);
    return;
  }
  // Desktop (constructor) notifications are tracked so we can close them on resolution and dispatch
  // their click; service-worker notifications are closed by tag and click via the worker postMessage.
  if (result.via === "constructor" && result.notification) {
    trackWaitingNotification(notificationKey, result.notification);
    result.notification.onclick = () => handleNotificationClick({ threadId: tid, requestId: waitingRequestId(activity) });
  }
  recordNotificationDiagnostic("waiting_notify_created", {
    tid,
    request_id: waitingRequestId(activity),
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
    if (diagnosticDetail.kind === "waiting_request") {
      untrackWaitingNotification(
        waitingNotificationKey(diagnosticDetail.tid, diagnosticDetail.request_id),
        notification
      );
    }
    recordNotificationDiagnostic("browser_notification_close", diagnosticDetail);
  };
  return { via: "constructor", notification };
}

function pruneNotificationDedup(now) {
  now = now || Date.now();
  for (const [key, notifiedAt] of state.notifiedRequests) {
    if (now - notifiedAt >= NOTIFICATION_DEDUP_MS) state.notifiedRequests.delete(key);
  }
}

function waitingNotificationKey(tid, requestId) {
  const threadKey = tid === undefined || tid === null ? "" : String(tid);
  const requestKey = requestId === undefined || requestId === null ? "" : String(requestId);
  if (!threadKey || !requestKey) return "";
  return `${threadKey}:${requestKey}`;
}

// Stable OS notification tag for an approval — used to show, dedup, and (on the service-worker
// path) close the notification without holding a Notification object.
function waitingNotificationTag(tid, requestId) {
  return `giskard-waiting-${tid}-${requestId}`;
}

function trackWaitingNotification(key, notification) {
  if (!key || !notification) return;
  let notifications = state.waitingNotifications.get(key);
  if (!notifications) {
    notifications = new Set();
    state.waitingNotifications.set(key, notifications);
  }
  notifications.add(notification);
}

function untrackWaitingNotification(key, notification) {
  const notifications = state.waitingNotifications.get(key);
  if (!notifications) return;
  notifications.delete(notification);
  if (notifications.size === 0) state.waitingNotifications.delete(key);
}

function closeWaitingNotification(tid, requestId) {
  const key = waitingNotificationKey(tid, requestId);
  if (!key) return;
  // Service-worker notifications aren't held as objects: fetch them back by tag and close them.
  const reg = state.swRegistration;
  if (reg && typeof reg.getNotifications === "function") {
    const tag = waitingNotificationTag(tid, requestId);
    reg.getNotifications({ tag })
      .then((ns) => ns.forEach((n) => { try { n.close(); } catch {} }))
      .catch(() => {});
  }
  // Desktop (constructor) notifications are tracked objects.
  const notifications = state.waitingNotifications.get(key);
  if (!notifications) return;
  state.waitingNotifications.delete(key);
  for (const notification of notifications) {
    try {
      notification.close();
      recordNotificationDiagnostic("waiting_notify_closed", {
        tid,
        request_id: requestId
      });
    } catch (e) {
      recordNotificationDiagnostic("waiting_notify_close_failed", {
        tid,
        request_id: requestId,
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
  notice("Enable notifications from the sidebar alert button.", "warning");
}

// Name the thread a notification is about, whichever kind it is waiting on. A sub-agent's own title
// is often generic ("Sub-agent #2"), and it has no sidebar row to fall back on, so qualify it with
// the owning thread — an unattributable "3f2a91bc: Approval requested" says nothing about what is
// blocked.
function waitingNotificationLabel(meta, tid) {
  const fallback = String(tid).slice(0,8);
  if (!meta) return fallback;
  const title = meta.title || fallback;
  if (meta.kind !== "subagent" || !meta.parent_thread_id) return title;
  const parent = threadMetaForId(meta.parent_thread_id);
  return parent && parent.title ? `${title} (in ${parent.title})` : title;
}

async function focusWaitingRequest(tid, requestId) {
  window.focus();
  let meta = threadMetaForId(tid);
  if (!meta || !meta.pid) {
    // The thread may have been materialized after our last list load; one refresh is cheap and is
    // the difference between navigating to the blocked sub-agent and a dead-end notification.
    await refreshKnownThreadLists();
    meta = threadMetaForId(tid);
  }
  if (!meta || !meta.pid) {
    notice("That thread is not in the current project list.", "warning");
    return;
  }
  if (String(state.threadId) !== String(tid)) {
    await openThread(meta.pid, tid, meta.title, { focusRequestId:requestId });
  } else {
    state.pendingWaitingFocus = {
      threadId:String(tid),
      requestId:String(requestId),
      attempts:0
    };
    schedulePendingWaitingFocus();
  }
}

function notifyIncomingApproval(request, tid, opts) {
  opts = opts || {};
  if (!request || !request.id || !tid) {
    recordNotificationDiagnostic("incoming_approval_skipped_invalid_request", {
      tid,
      request_id: request && request.id ? String(request.id) : null
    });
    return;
  }
  maybeNotifyWaitingRequest(String(tid), {
    kind:"approval_requested",
    active_turn:true,
    approval_id:String(request.id),
    server_request_id:null,
    summary:approvalTitle(request),
    source:opts.source || "incoming_approval_request",
    unread:false
  });
}

// Resolve an approval's answer from either the live-session store (keyed by state) or the reload
// snapshot store (keyed by id, since a reload wiped the browser's state-key memory). Returns the
// answered entry ({ decision, ... }) or undefined. Single source of truth for the lookup order.
function answeredApprovalEntry(request) {
  if (!request || request.id === undefined || request.id === null) return undefined;
  return state.answeredApprovals.get(approvalStateKey(request))
    || state.answeredApprovalsById.get(String(request.id));
}
function isApprovalAnswered(request) {
  return !!answeredApprovalEntry(request);
}
function handleIncomingApprovalRequest(request, tid, opts) {
  opts = opts || {};
  // A reconnect snapshot replays already-answered approvals so their cards can be redrawn in the
  // resolved state. Those must not re-arm the waiting-on-you sidebar activity or fire a
  // notification — the user already answered them. Render the resolved card and stop.
  if (isApprovalAnswered(request)) {
    renderApprovalRequest(request);
    return;
  }
  recordNotificationDiagnostic("incoming_approval_request", {
    tid,
    request_id: request && request.id ? String(request.id) : null,
    source: opts.source || "unknown",
    notify: opts.notify !== false
  });
  if (opts.notify !== false) notifyIncomingApproval(request, tid, { source: opts.source });
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
  setPermissionPreset(s.permission_preset || "ask_first");
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
    if (projectModelCatalogReady()) syncModelControls();
    else renderModelSelect();
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
  state.pendingLiveSnapshotReconcile = false;
  resetRenderState();
  if (keepFirstTurnActive) updateComposerControls();
  else setTurnActive(false);
}
const MODE_LABELS = { build:"Build", plan:"Plan" };
const PERMISSION_PRESET_LABELS = {
  ask:"Ask first",
  read_only:"Ask first",
  ask_first:"Ask first",
  auto:"Auto approve",
  auto_approve:"Auto approve",
  full_access:"⚠ Full Access"
};
function normalizePermissionPreset(preset) {
  if (preset === "ask" || preset === "read_only") return "ask_first";
  if (preset === "auto") return "auto_approve";
  if (preset === "full_access") return "full_access";
  return preset || "ask_first";
}
// Summarise "mode · permissions" on the turn chip below the composer.
function updateTurnButton() {
  const btn = $("turnPickerBtn"); if (!btn) return;
  const mode = MODE_LABELS[state.mode] || "Build";
  const preset = PERMISSION_PRESET_LABELS[state.permissionPreset] || "Ask first";
  btn.querySelector(".mp-label").textContent = `${mode} · ${preset}`;
}
function setMode(mode) {
  state.mode = mode === "plan" ? "plan" : "build";
  $("modeSel").value = state.mode;
  updateTurnButton();
}
function setPermissionPreset(preset) {
  state.permissionPreset = normalizePermissionPreset(preset);
  $("permissionPresetSel").value = state.permissionPreset;
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
    state.pendingLiveSnapshotReconcile = false;
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
// transcript and append these new turns. If a live turn is still running, keep its old DOM visible
// until the live snapshot arrives; the snapshot handler removes and recreates the live block
// synchronously, avoiding a visible blank gap between the history-delta and live-snapshot messages.
function renderHistoryDelta(msg) {
  state.awaitingIncrementalResync = false;
  const turns = msg.turns || [];
  const liveId = state.activeTurn && state.currentRenderTurnId != null
    ? String(state.currentRenderTurnId)
    : null;
  const completedLiveTurn = !!(liveId && turns.some(turn => String(turn.id) === liveId));
  const completedPendingTurn = !liveId && !!state.pendingUserEl && turns.length > 0;

  if (completedLiveTurn || completedPendingTurn) {
    // The turn that was live, or still an optimistic pending row, completed while we were
    // disconnected, so there will be no live snapshot to replace it. Remove the stale live rows
    // before appending the persisted turn.
    reconcileInFlightTurn();
    state.pendingLiveSnapshotReconcile = false;
  } else {
    state.pendingLiveSnapshotReconcile = !!liveId || !!state.pendingUserEl;
  }

  // Append completed-since turns. If the old live turn is still visible, insert the persisted turns
  // immediately before that live block so transcript chronology stays correct until the snapshot
  // atomically replaces the live block.
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
    const anchor = !completedLiveTurn ? firstLiveTurnRow(liveId) : null;
    while (container.firstChild) t.insertBefore(container.firstChild, anchor);
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
  // The in-flight turn's rows are gone. Rebuild render bookkeeping from the rows that survived
  // (the DOM is the source of truth) so the live snapshot creates fresh rows for the removed turn
  // instead of deduping against stale maps or updating detached bodies.
  rebuildRenderTrackingFromDom();
  state.pendingUserEl = null;
  state.pendingUserText = null;
  state.currentRenderTurnId = null;
  setTurnActive(false);
  state.streamEl = null;
  state.streamItemId = null;
  breakTaskGroup();
}
function firstLiveTurnRow(liveId) {
  const t = $("transcript");
  if (!t) return null;
  for (const el of Array.from(t.children)) {
    if (!el.classList || !el.classList.contains("msg")) continue;
    if (el.dataset.turn === "pending" || (liveId && el.dataset.turn === liveId)) return el;
  }
  return null;
}
function rebuildRenderTrackingFromDom() {
  const t = $("transcript");
  const attached = (el) => !!(el && t && t.contains(el));
  const renderedItems = new Set();
  const renderedHarnessItems = new Set();
  const renderedBodies = new Map();

  if (t) {
    for (const row of t.querySelectorAll(".msg")) {
      const turn = row.dataset.turn || "";
      const body = row.querySelector(".body");
      for (const itemId of identityTokens(row.dataset.item)) {
        const key = scopedItemKey(turn, itemId);
        if (!key) continue;
        renderedItems.add(key);
        if (body) renderedBodies.set(key, body);
      }
      for (const harnessItemId of identityTokens(row.dataset.harnessItem)) {
        const key = scopedHarnessKey(turn, harnessItemId);
        if (!key) continue;
        renderedHarnessItems.add(key);
        if (body) renderedBodies.set(key, body);
      }
    }
  }

  state.renderedItemIds = renderedItems;
  state.renderedHarnessItemIds = renderedHarnessItems;
  state.renderedItemBodyByKey = renderedBodies;
  state.streamElsByItemId = new Map();
  state.itemKindsByItemId = new Map();

  for (const m of [state.commandBodyElsByItemId, state.commandMsgElsByItemId, state.toolBodyElsByItemId]) {
    for (const [key, el] of Array.from(m)) if (!attached(el)) m.delete(key);
  }

  const liveTaskIds = new Set(state.commandMsgElsByItemId.keys());
  for (const m of [
    state.commandPayloadsByItemId,
    state.endedCommandsByItemId,
    state.runningCommands,
    state.toolPayloadsByItemId,
    state.taskGroupsByItemId
  ]) {
    for (const key of Array.from(m.keys())) if (!liveTaskIds.has(key)) m.delete(key);
  }
  pruneKeySet(state.commandStopRequestedByItemId, liveTaskIds);

  for (const [groupId, group] of Array.from(state.taskGroupsById)) {
    if (!attached(group && group.el)) {
      state.taskGroupsById.delete(groupId);
      state.expandedTaskGroups.delete(groupId);
      state.manuallyToggledTaskGroups.delete(groupId);
      state.expandedTaskDetails.delete(groupId);
      continue;
    }
    group.itemOrder = group.itemOrder.filter(id => liveTaskIds.has(id));
    for (const key of Array.from(group.items.keys())) {
      if (!liveTaskIds.has(key)) group.items.delete(key);
    }
    const detailIds = expandedTaskDetailIds(groupId);
    pruneKeySet(detailIds, liveTaskIds);
    syncTaskGroupState(group);
  }
  if (state.selectedCommandId && !liveTaskIds.has(state.selectedCommandId)) {
    state.selectedCommandId = null;
  }

  for (const [id, entry] of Array.from(state.pendingApprovals)) {
    if (!attached(entry && entry.msg)) state.pendingApprovals.delete(id);
  }
  for (const [id, entry] of Array.from(state.pendingServerRequests)) {
    if (!attached(entry && entry.msg)) state.pendingServerRequests.delete(id);
  }
  const approvalKeys = new Set();
  if (t) {
    for (const row of t.querySelectorAll("[data-approval-state-key]")) {
      if (row.dataset.approvalStateKey) approvalKeys.add(row.dataset.approvalStateKey);
    }
  }
  state.renderedApprovalStateKeys = approvalKeys;
  renderRunningCommands();
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
  const inputText = persistedUserInputDisplayText(turn.user_input);
  const hasAttachments = !!(turn.user_input && (turn.user_input.attachments || []).length);
  if (!hasUserItem && inputText) {
    renderItemBody(bubble("user","you"), { kind:"user_message", text: inputText });
  }
  let replacedUserItem = false;
  for (const it of items) {
    const payload = it.payload || it;
    if (hasAttachments && !replacedUserItem && payload.kind === "user_message") {
      addItem(userMessageItemWithText(it, inputText), turn.id, true);
      replacedUserItem = true;
    } else {
      addItem(it, turn.id, true);
    }
  }
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
      state.pendingLiveSnapshotReconcile = false;
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
      renderLiveTurnUserInput(ev.turn, ev.user_input);
      setTurnActive(true);
      setActiveThreadActivity("progress", true, "Turn running");
      break;
    case "context_window_updated":
      if (ev.model && state.currentModel &&
          ev.model.provider === state.currentModel.provider &&
          ev.model.model === state.currentModel.model &&
          Number.isFinite(ev.context_window) && ev.context_window > 0) {
        updateGauge(state.contextUsed, ev.context_window);
      }
      break;
    case "item_started":
      if (ev.item) {
        const key = scopedItemKey(ev.turn, ev.item.id);
        state.itemKindsByItemId.set(key, ev.item.kind);
        if (ev.item.kind==="command_execution" && ev.item.command) {
          startRunningCommand(ev.item, ev.turn);
        } else if (ev.item.kind==="tool_call" && ev.item.tool) {
          startToolCall(ev.item, ev.turn);
        }
      }
      break;
    case "item_delta":
      if (ev.delta && ev.delta.type==="text") {
        const key = scopedItemKey(ev.turn, ev.item_id);
        const kind = state.itemKindsByItemId.get(key);
        if (kind==="tool_call") appendToolProgress(ev.turn, ev.item_id, ev.delta.text);
        else appendStream(ev.turn, ev.delta.text, ev.item_id, ev.delta.type);
      }
      else if (ev.delta && ev.delta.type==="command_output") {
        if (!appendRunningCommandOutput(ev.turn, ev.item_id, ev.delta.chunk)) {
          appendStream(ev.turn, ev.delta.chunk, ev.item_id, ev.delta.type);
        }
      }
      break;
    case "item_completed":
      if (!finalizeStreamedItem(ev.item, ev.turn)) addItem(ev.item, ev.turn);
      if (isContextCompactionItem(ev.item)) finishCompactPending();
      // Only the live path: replaying history must not poll git for changes long since made.
      if (ev.item && ((ev.item.payload || ev.item).kind) === "file_change") scheduleGitRefresh();
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
      // Shell edits leave no file-change item, so the end of the turn is the catch-all.
      scheduleGitRefresh();
      break;
    case "approval_requested":
      handleIncomingApprovalRequest(ev.request, ev.thread || state.threadId, {
        source: "agent_event_approval_requested"
      });
      break;
    case "server_request_received": {
      const serverRequestId = ev.request && ev.request.id ? String(ev.request.id) : null;
      // A reconnect snapshot replays every ServerRequestReceived, answered ones included. Those must
      // not re-arm the waiting-on-you activity or fire a notification — the user already answered
      // them. Mirrors the approval path's guard in `handleIncomingApprovalRequest`;
      // `renderServerRequest` settles the card into its resolved state from the same answered set.
      if (serverRequestId && state.answeredServerRequests.has(serverRequestId)) {
        renderServerRequest(ev.request);
        break;
      }
      setActiveThreadActivity("server_request_received", true, "Waiting for your input", {
        server_request_id:serverRequestId
      });
      renderServerRequest(ev.request);
      maybeNotifyWaitingRequest(String(ev.thread || state.threadId || ""), {
        kind:"server_request_received",
        active_turn:true,
        approval_id:null,
        server_request_id:serverRequestId,
        summary:"Waiting for your input",
        source:"agent_event_server_request_received",
        unread:false
      });
      break;
    }
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
  if (state.pendingLiveSnapshotReconcile) {
    state.pendingLiveSnapshotReconcile = false;
    reconcileInFlightTurn();
  }
  if (snap && snap.turn_id) {
    state.firstTurnStartingThreadId = null;
    // Adopt the live turn id so its rows stamp correctly even if the accumulated events don't lead
    // with a turn_started (the turn_started handler will confirm the same id).
    state.currentRenderTurnId = snap.turn_id;
    renderLiveTurnUserInput(snap.turn_id, snap.user_input);
    setTurnActive(true);
    setActiveThreadActivity("progress", true, "Turn running");
  }
  // Seed answered approvals before replaying accumulated events: the buffer replays every
  // ApprovalRequested (answered ones included), and this reload wiped the in-memory answered state,
  // so without this the answered cards would render actionable again and re-answering errors.
  for (const answered of (snap.answered_approvals || [])) {
    if (answered && answered.request_id !== undefined && answered.request_id !== null) {
      state.answeredApprovalsById.set(String(answered.request_id), { decision: answered.decision });
    }
  }
  // Same reasoning for server requests: `accumulated` replays every ServerRequestReceived, and a
  // harness's resolved event may be late or never arrive, so without this an answered request
  // renders actionable again and re-answering routes a stale id to the harness.
  for (const answered of (snap.answered_server_requests || [])) {
    if (answered !== undefined && answered !== null) state.answeredServerRequests.add(String(answered));
  }
  for (const ev of (snap.accumulated||[])) handleEvent(ev);
  // Then re-assert what the turn is still waiting on the user for, approvals first. The replay
  // above already drew these cards, but later events speak for the thread too — an `error` in
  // particular overwrites the thread's activity and clears the active turn. Whatever is still
  // outstanding gets the last word, so a turn blocked on an approval that then errored still reads
  // as waiting on the user rather than "errored, idle". A turn can be blocked on several approvals
  // at once, so every unanswered approval is re-armed, not just one.
  for (const approval of outstandingApprovals(snap)) {
    handleIncomingApprovalRequest(approval, snap.thread_id || state.threadId, {
      source: "live_turn_snapshot_outstanding_approval"
    });
  }
  for (const request of outstandingServerRequests(snap)) {
    setActiveThreadActivity("server_request_received", true, "Waiting for your input", {
      server_request_id:request && request.id ? String(request.id) : null
    });
    renderServerRequest(request);
  }
}

// Every approval the replayed turn is still waiting on the user for, oldest first. An approval is
// outstanding unless the user already answered it (named in `snap.answered_approvals`). A re-sent
// id is the same approval with a fresher payload, so the latest occurrence wins and the order is
// the first occurrence of each id.
function outstandingApprovals(snap) {
  const answered = new Set();
  for (const a of (snap.answered_approvals || [])) {
    if (a && a.request_id !== undefined && a.request_id !== null) answered.add(String(a.request_id));
  }
  const order = [];
  const latest = new Map();
  for (const ev of (snap.accumulated || [])) {
    if (!ev || ev.kind !== "approval_requested") continue;
    const request = ev.request;
    const id = request && request.id !== undefined && request.id !== null ? String(request.id) : null;
    if (!id) continue;
    if (!latest.has(id)) order.push(id);
    latest.set(id, request);
  }
  return order
    .map(id => latest.get(id))
    .filter(request => request && !answered.has(String(request.id)));
}

// Every server request the replayed turn is still waiting on the user for, oldest first. A
// request leaves the outstanding set when the user answered it (named in
// `snap.answered_server_requests`) or when the harness closed it (`server_request_resolved` in
// `accumulated`). A re-sent id is the same request with a fresher payload, so the latest
// `server_request_received` wins and the order is the first occurrence of each id.
function outstandingServerRequests(snap) {
  const answered = new Set();
  for (const id of (snap.answered_server_requests || [])) {
    if (id !== undefined && id !== null) answered.add(String(id));
  }
  // A `Map` keeps arrival order (iterating a Map yields entries in insertion order). `set` on
  // receive updates the payload in place when the id is already present, or appends it when it is
  // new; `delete` on resolve drops it. A re-sent id after a resolution re-inserts at the end, so a
  // reopen moves to the back rather than keeping its first-seen position. Mirrors
  // `pending_server_requests` on the server.
  const pending = new Map();
  for (const ev of (snap.accumulated || [])) {
    if (!ev || !ev.kind) continue;
    if (ev.kind === "server_request_received") {
      const request = ev.request;
      const id = request && request.id !== undefined && request.id !== null ? String(request.id) : null;
      if (!id) continue;
      pending.set(id, request);
    } else if (ev.kind === "server_request_resolved") {
      const id = ev.request_id !== undefined && ev.request_id !== null ? String(ev.request_id) : null;
      if (id) pending.delete(id);
    }
  }
  return Array.from(pending.values())
    .filter(request => request && !answered.has(String(request.id)));
}

function renderLiveTurnUserInput(turnId, userInput) {
  const text = persistedUserInputDisplayText(userInput);
  if (!turnId || !text) return;
  const exists = Array.from(document.querySelectorAll(".msg.user")).some(
    row => row.dataset && String(row.dataset.turn || "") === String(turnId)
  );
  if (exists) return;
  const body = bubble("user","you");
  body.parentElement.dataset.liveUserInput = "true";
  markAttachmentUserInput(body.parentElement, userInput && userInput.attachments);
  renderItemBody(body, { kind:"user_message", text });
}

function provisionalUserBodyForTurn(turnId) {
  const target = renderTarget();
  return Array.from(target.querySelectorAll(".msg.user[data-live-user-input='true'] .body")).find(
    body => body.parentElement && String(body.parentElement.dataset.turn || "") === String(turnId || "")
  ) || null;
}

function isSyntheticSubagentPrompt(item) {
  return !!(item && String(item.harness_item_id || "").startsWith("subagent_prompt:"));
}

function placeRowFirstInTurn(row, turnId) {
  const target = row && row.parentElement;
  if (!target || !turnId) return;
  const first = Array.from(target.children).find(candidate =>
    candidate !== row && candidate.classList && candidate.classList.contains("msg") &&
    String(candidate.dataset.turn || "") === String(turnId)
  );
  if (first) target.insertBefore(row, first);
}

function renderApprovalRequest(request) {
  if (!request || !request.id) return;
  const id = String(request.id);
  const stateKey = approvalStateKey(request);
  if (state.pendingApprovals.has(id) || state.renderedApprovalStateKeys.has(stateKey)) return;
  // An answer may be keyed by state (live session) or by id (reload snapshot, where the browser's
  // stateKey memory is gone and the server tells us which ids were already answered).
  const answered = answeredApprovalEntry(request);
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
  schedulePendingWaitingFocus();
}

// A notification click targets whatever the thread is waiting for, so look in both card kinds.
function waitingRequestRowById(id) {
  if (!id) return null;
  const target = String(id);
  return Array.from(document.querySelectorAll("[data-approval-id],[data-server-request-id]"))
    .find(el => String(el.dataset.approvalId || el.dataset.serverRequestId) === target) || null;
}

function schedulePendingWaitingFocus() {
  const pending = state.pendingWaitingFocus;
  if (!pending || !pending.requestId) return;
  if (!state.threadId || String(state.threadId) !== String(pending.threadId)) return;
  const row = waitingRequestRowById(pending.requestId);
  if (row) {
    state.pendingWaitingFocus = null;
    row.scrollIntoView({ block:"center", behavior:"smooth" });
    row.classList.add("waiting-target");
    row.setAttribute("tabindex", "-1");
    row.focus({ preventScroll:true });
    setTimeout(() => row.classList.remove("waiting-target"), 5000);
    return;
  }
  pending.attempts = (pending.attempts || 0) + 1;
  if (pending.attempts > 40) {
    state.pendingWaitingFocus = null;
    notice("That request is no longer pending.", "warning");
    return;
  }
  setTimeout(schedulePendingWaitingFocus, 150);
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
  const msg = entry ? entry.msg : waitingRequestRowById(id);
  const tid = entry && entry.request && entry.request.thread_id
    ? entry.request.thread_id
    : (msg && msg.dataset.threadId ? msg.dataset.threadId : state.threadId);
  closeWaitingNotification(tid, id);
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

  // Built the card, now settle it: a request answered before this page load exists only as a
  // replayed `ServerRequestReceived`, so it must not come back actionable.
  if (state.answeredServerRequests.has(id)) resolveServerRequest(id);
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
  // Stop claiming the thread is waiting on the user the moment they act. An approval clears here
  // because answering it broadcasts `ApprovalResolved`; a server request's resolved event comes from
  // the harness on its own schedule and may never come, so waiting for it would leave the sidebar
  // demanding attention for something already answered.
  const tid = entry.msg.dataset.threadId || state.threadId;
  clearServerRequestThreadActivity(tid, String(id));
  closeWaitingNotification(tid, String(id));
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
  head.title = "Show or hide the task rows";
  head.setAttribute("aria-label", "Show or hide the task rows");
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
  wireTaskGroupItemRow(row, group.id, key);

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
function wireTaskGroupItemRow(row, groupId, itemId) {
  const key = idKey(itemId);
  row.onclick = (e) => { e.stopPropagation(); selectTaskGroupItem(groupId, key); };
  row.onkeydown = (e) => {
    if (e.key === "Enter" || e.key === " ") {
      e.preventDefault();
      selectTaskGroupItem(groupId, key);
    }
  };
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
  // The group summary toggles the visibility of the task rows themselves. Each row is expanded to
  // its detail (the inline command/tool output snippet) separately, by clicking the row. Collapsing
  // the group only hides the rows; their individual expanded/collapsed state is preserved.
  if (state.expandedTaskGroups.has(groupId)) {
    state.expandedTaskGroups.delete(groupId);
    if (group.itemOrder.includes(state.selectedCommandId)) {
      state.selectedCommandId = null;
      clearTaskSelection();
      // Drop the stale highlight from the running-task panel too (clearTaskSelection only touches
      // transcript rows), matching selectTaskGroupItem's selection-change handling.
      renderRunningCommands();
    }
  } else {
    state.expandedTaskGroups.add(groupId);
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
  const key = scopedItemKey(turnId, item.id); if (!key) return;
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
  registerRenderedItemBody(body, item, turnId);
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
// The client accumulates the full running output from command_output deltas, but the server's
// RunningTasks snapshot and replayed item payloads may carry only a capped tail (see MAX_OUTPUT_TAIL
// in running_commands.rs). Keep whichever is longer so those updates never shrink the overlay's full
// log back to the tail during a live session.
function mergeRunningOutput(prev, next) {
  prev = prev || "";
  next = next || "";
  return prev.length >= next.length ? prev : next;
}
function commandFromItem(item, p, turnId, key, existing) {
  return commandFromParts({
    id:existing ? existing.id : key,
    turnId:existing ? existing.turnId : idKey(turnId),
    harnessItemId:(item && item.harness_item_id) || (existing && existing.harnessItemId) || "",
    command:p.command || "",
    cwd:p.cwd || "",
    status:p.status || "in_progress",
    processId:p.process_id || (existing && existing.processId) || "",
    startedAtMs:existing ? existing.startedAtMs : Date.now(),
    output:mergeRunningOutput(existing && existing.output, p.output),
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
function outputByteLen(text) {
  try { return new TextEncoder().encode(text).length; } catch { return text.length; }
}
function tailByLines(text, maxLines) {
  let i = text.length - 1;
  if (i >= 0 && text.charCodeAt(i) === 10) i--; // ignore a single trailing newline when counting
  let count = 0;
  for (; i >= 0; i--) {
    if (text.charCodeAt(i) === 10) {
      count++;
      if (count >= maxLines) return text.slice(i + 1);
    }
  }
  return text;
}
function headByLines(text, maxLines) {
  let count = 0;
  for (let i = 0; i < text.length; i++) {
    if (text.charCodeAt(i) === 10) {
      count++;
      if (count >= maxLines) return text.slice(0, i + 1);
    }
  }
  return text;
}
function clampTailBytes(text, maxBytes) {
  if (outputByteLen(text) <= maxBytes) return text;
  let t = text.length > maxBytes ? text.slice(text.length - maxBytes) : text;
  const nl = t.indexOf("\n"); // drop the partial leading line so the preview starts cleanly
  if (nl >= 0 && nl < t.length - 1) t = t.slice(nl + 1);
  return t;
}
function clampHeadBytes(text, maxBytes) {
  if (outputByteLen(text) <= maxBytes) return text;
  let t = text.slice(0, maxBytes);
  const nl = t.lastIndexOf("\n"); // drop the partial trailing line
  if (nl > 0) t = t.slice(0, nl + 1);
  return t;
}
// Compact the inline preview to the freshest INLINE_PREVIEW_LINES / INLINE_PREVIEW_BYTES. Commands
// and tool output keep the tail (latest progress); tool input keeps the head (the call arguments).
function inlineOutputPreview(text, mode) {
  text = String(text || "");
  const stats = commandOutputStats(text);
  if (stats.lineCount <= INLINE_PREVIEW_LINES && stats.bytes <= INLINE_PREVIEW_BYTES) {
    return { text, truncated: false };
  }
  let preview = mode === "head" ? headByLines(text, INLINE_PREVIEW_LINES) : tailByLines(text, INLINE_PREVIEW_LINES);
  preview = mode === "head" ? clampHeadBytes(preview, INLINE_PREVIEW_BYTES) : clampTailBytes(preview, INLINE_PREVIEW_BYTES);
  return { text: preview, truncated: true };
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
function makeCommandHead() {
  const head = document.createElement("div");
  head.className = "cmd-head";
  return { head };
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
  // The command's output snippet is shown whenever the command row itself is visible — there is no
  // second collapse level. The full log lives in the overlay via the "Open" button.
  const msg = body.parentElement;
  clearRowToggle(msg);

  const summary = document.createElement("div");
  summary.className = "meta cmd-output-summary";
  const label = commandOutputStatsLabel(stats, phase);
  const text = document.createElement("span");
  text.className = "cmd-output-summary-text";
  text.textContent = stats.chars ? `Output · ${label}` : label;
  summary.append(text);
  if (stats.chars || phase === "running") {
    summary.append(makeOutputOverlayButton(itemId, "command"));
  }
  body.append(summary);

  if (!stats.chars) return;
  const preview = inlineOutputPreview(output, "tail");
  if (preview.truncated) {
    const note = document.createElement("div");
    note.className = "meta cmd-output-truncated";
    note.textContent = "Showing the latest output — Open ⤢ for the full log";
    body.append(note);
  }
  const out = document.createElement("pre");
  out.className = "out";
  body.append(out);
  if (opts.linkify) renderLinkedText(out, preview.text);
  else out.textContent = preview.text;
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
  refreshOutputOverlay(cmd.id);
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
function appendRunningCommandOutput(turnId, itemId, chunk) {
  const key = scopedItemKey(turnId, itemId);
  const cmd = state.runningCommands.get(key);
  if (!cmd) return false;
  // Retain the full streamed output (not just a tail): the inline row renders only a bounded
  // preview, but the output overlay shows the complete log as it streams. The server still keeps
  // the authoritative full output and sends it on completion.
  cmd.output = (cmd.output || "") + (chunk || "");
  const body = commandBodyFor(key);
  if (body) renderCommandBody(body, cmd);
  else refreshOutputOverlay(key);
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
function finishRunningCommand(item, turnId) {
  const key = scopedItemKey(turnId, item && item.id);
  if (!key) return;
  const p = item && item.payload;
  if (p && p.kind==="command_execution" && commandIsRunningStatus(p.status)) {
    const cmd = commandFromItem(item, p, turnId, key, state.runningCommands.get(key));
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
    const key = scopedItemKey(info.turn_id, info.item_id);
    if (!key) continue;
    const snapshotItem = { id:info.item_id, harness_item_id:info.harness_item_id || "" };
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
      output:mergeRunningOutput(existing && existing.output, info.output),
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
      registerRenderedItemBody(toolBody, snapshotItem, info.turn_id);
      state.commandMsgElsByItemId.set(key, toolBody.parentElement);
    } else {
      let body = commandBodyFor(key);
      if (!body) body = taskBubble(key, "command_execution", "cmd running-command", "command");
      state.commandBodyElsByItemId.set(key, body);
      state.commandMsgElsByItemId.set(key, body.parentElement);
      registerRenderedItemBody(body, snapshotItem, info.turn_id);
      renderCommandBody(body, cmd);
    }
  }

  for (const [id, cmd] of Array.from(state.runningCommands.entries())) {
    if (seen.has(id)) continue;
    state.runningCommands.delete(id);
    // Tool transcript rows are owned by the item stream; only commands with an explicit stop
    // request get a fallback ended-body rewrite when task tracking disappears.
    if (cmd.kind !== "tool") {
      const body = commandBodyFor(id);
      const stopRequested = cmd.terminating || state.commandStopRequestedByItemId.has(id);
      if (body && stopRequested) {
        renderEndedCommandBody(body, cmd, "unknown", { stopRequested });
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
  refreshOutputOverlay(cmd.id);
}
function renderRunningCommands() {
  const cmds = Array.from(state.runningCommands.values());
  renderTasksButton(cmds);
  if (!$("tasksMenu").hidden) renderTasksMenu(cmds);
}

/* Git status line (above the composer).
 *
 * Status is fetched per thread open, so `gitRepoByProject` remembers only whether a project's
 * workspace is a repository at all: that is what decides whether the row exists, and caching just
 * that much keeps the composer from shifting down when you move between threads of a project
 * already known to be one. The status itself is always refetched. */
const GIT_SECTIONS = [
  { key:"conflicted", label:"Conflicts" },
  { key:"staged", label:"Staged" },
  { key:"unstaged", label:"Not staged" },
  { key:"untracked", label:"Untracked" }
];
/* Below this many characters a branch name is more confusing than absent, so the line stops
   shrinking it and drops whole segments instead. */
const GIT_BRANCH_MIN_CHARS = 10;
/* Directory length in a file row past which leading path segments are dropped. */
const GIT_PATH_DIR_MAX = 34;

function resetGitState() {
  clearTimeout(state.gitRefreshTimer);
  state.gitStatus = null;
  state.gitBodyHtml = null;
  state.gitLoading = false;
  state.gitError = null;
  state.gitExpanded = false;
  state.gitRequestSeq += 1;
  renderGitLine();
}

function gitDirtyCount(status) {
  return status && Array.isArray(status.files) ? status.files.length : 0;
}

function gitBranchName(status) {
  if (!status || !status.is_repository) return "";
  if (status.branch) return status.branch;
  if (status.head) return status.head;
  return status.detached ? "detached" : "HEAD";
}

/* Split a branch name into a dimmable leading path and the segment that identifies it, shortening
   to fit `budget` characters. Segments are shed in order — the prefix first, then the head of the
   tail — because the tail is what distinguishes one branch from another. */
function gitBranchParts(name, budget) {
  name = String(name || "");
  const cut = name.lastIndexOf("/");
  const prefix = cut >= 0 ? name.slice(0, cut + 1) : "";
  const tail = cut >= 0 ? name.slice(cut + 1) : name;
  if (name.length <= budget) return { prefix, tail };
  if (tail.length <= budget) return { prefix: prefix ? "…/" : "", tail };
  const keep = Math.max(GIT_BRANCH_MIN_CHARS, budget - 1);
  return { prefix:"", tail: "…" + tail.slice(tail.length - keep) };
}

/* Character budget for the branch, by viewport tier. The row's real width also depends on the
   sidebar, so this is an approximation with `overflow:hidden` on .git-branch as the backstop —
   deliberately preferred over measuring, which would need a layout pass on every render. */
function gitBranchBudget() {
  const width = window.innerWidth || 1024;
  if (width < 480) return 22;
  if (width < 820) return 34;
  return 60;
}

function gitLineState() {
  if (state.gitError) return "error";
  const status = state.gitStatus;
  if (!status || !status.is_repository) return state.gitLoading ? "loading" : "unavailable";
  if (status.conflicted_count) return "conflicted";
  return status.dirty ? "dirty" : "clean";
}

function gitFileSections(status) {
  const files = status && Array.isArray(status.files) ? status.files : [];
  const sections = { conflicted:[], staged:[], unstaged:[], untracked:[] };
  for (const file of files) {
    if (!file) continue;
    if (file.kind === "unmerged") { sections.conflicted.push(file); continue; }
    if (file.kind === "untracked") { sections.untracked.push(file); continue; }
    // A file edited both in the index and in the worktree is genuinely in two states, so it is
    // listed under each — the same way `git status` reports it twice.
    if (file.index_status && file.index_status !== "unmodified") sections.staged.push(file);
    if (file.worktree_status && file.worktree_status !== "unmodified") sections.unstaged.push(file);
  }
  return sections;
}

/* The line is hidden until the project is known to be a repository, so it never appears and then
   vanishes on a workspace that isn't one. A project already seen this session is known before its
   fetch returns, so from the second thread open onward the row renders at its final height and the
   composer doesn't move. */
function gitLineVisible(stateName) {
  if (!state.projectId) return false;
  if (stateName === "error") return true;
  if (stateName === "unavailable") return false;
  if (stateName === "loading") return state.gitRepoByProject.get(state.projectId) === true;
  return true;
}

function renderGitLine() {
  const line = $("gitLine");
  if (!line) return;
  const status = state.gitStatus;
  const stateName = gitLineState();
  const visible = gitLineVisible(stateName);
  line.hidden = !visible;
  if (!visible) { setGitExpanded(false, { skipRender:true }); return; }

  line.className = `git-line state-${stateName}${state.gitExpanded ? " expanded" : ""}`;
  const loadingFirst = stateName === "loading";
  const expandable = stateName === "dirty" || stateName === "conflicted";
  const dirty = gitDirtyCount(status);

  $("gitIcon").firstElementChild.setAttribute("href", status && status.detached ? "#gi-detached" : "#gi-branch");
  $("gitCaret").hidden = !expandable;

  const branch = $("gitBranch");
  if (loadingFirst) {
    branch.innerHTML = `<span class="git-skeleton" style="width:104px"></span>`;
    branch.removeAttribute("title");
  } else if (stateName === "error") {
    branch.textContent = "Git status unavailable";
    branch.title = state.gitError || "";
  } else {
    const name = gitBranchName(status);
    const parts = gitBranchParts(name, gitBranchBudget());
    // A detached HEAD is named by its commit, with the state itself as a dim note after it — so it
    // reads as "this revision, detached" rather than as a branch called "detached".
    const note = status && status.detached ? `<span class="git-branch-note">detached</span>` : "";
    branch.innerHTML = `${parts.prefix ? `<span class="git-branch-prefix">${escapeHtml(parts.prefix)}</span>` : ""}${escapeHtml(parts.tail)}${note}`;
    branch.title = status && status.detached ? `Detached HEAD at ${name}` : name;
  }

  const sync = $("gitSync");
  const ahead = status && status.ahead ? status.ahead : 0;
  const behind = status && status.behind ? status.behind : 0;
  sync.hidden = loadingFirst || (!ahead && !behind);
  if (!sync.hidden) {
    sync.innerHTML = [
      ahead ? `<span class="git-ahead">↑${ahead}</span>` : "",
      behind ? `<span class="git-behind">↓${behind}</span>` : ""
    ].join("");
    sync.title = [
      ahead ? `${ahead} commit${ahead === 1 ? "" : "s"} ahead of upstream` : "",
      behind ? `${behind} commit${behind === 1 ? "" : "s"} behind upstream` : ""
    ].filter(Boolean).join(" · ");
  }

  const count = $("gitCount");
  const countLabel = gitCountLabel(status, stateName, dirty);
  $("gitSep").hidden = stateName === "error" || !countLabel;
  if (loadingFirst) count.innerHTML = `<span class="git-skeleton" style="width:26px"></span>`;
  else count.textContent = countLabel;

  const diffstat = $("gitDiffstat");
  const added = status && status.added_total ? status.added_total : 0;
  const deleted = status && status.deleted_total ? status.deleted_total : 0;
  diffstat.hidden = !expandable || (!added && !deleted);
  if (!diffstat.hidden) {
    diffstat.innerHTML = gitDiffstatHtml(added, deleted);
    diffstat.title = `${added} line${added === 1 ? "" : "s"} added, ${deleted} removed across the working tree`;
  }

  const toggle = $("gitLineToggle");
  toggle.disabled = !expandable;
  toggle.setAttribute("aria-expanded", state.gitExpanded && expandable ? "true" : "false");
  toggle.title = gitLineTitle(status, stateName, dirty);
  $("gitReviewAll").hidden = !expandable;
  $("gitRefresh").hidden = false;

  if (!expandable && state.gitExpanded) setGitExpanded(false, { skipRender:true });
  $("gitLineBody").hidden = !state.gitExpanded;
  if (state.gitExpanded) renderGitLineBody();
}

function gitCountLabel(status, stateName, dirty) {
  if (stateName === "loading") return "…";
  if (stateName === "error") return "";
  if (stateName === "conflicted") {
    const conflicts = status.conflicted_count;
    const label = `${conflicts} conflict${conflicts === 1 ? "" : "s"}`;
    // The total is only worth showing when there is something beyond the conflicts.
    return conflicts === dirty ? label : `${label} · ${dirty}`;
  }
  return status && status.dirty ? String(dirty) : "clean";
}

function gitLineTitle(status, stateName, dirty) {
  if (stateName === "loading") return "Loading Git status…";
  if (stateName === "error") return "Git status unavailable: " + (state.gitError || "");
  if (stateName === "conflicted") return `${status.conflicted_count} conflicted file${status.conflicted_count === 1 ? "" : "s"} — click to list the changes`;
  if (!status || !status.dirty) return "Working tree clean";
  return `${dirty} changed file${dirty === 1 ? "" : "s"} — click to list them`;
}

function renderGitLineBody() {
  const body = $("gitLineBody");
  const status = state.gitStatus;
  if (!status || !status.is_repository) { body.innerHTML = ""; state.gitBodyHtml = null; return; }
  const sections = gitFileSections(status);
  const html = (GIT_SECTIONS
    .filter(section => sections[section.key].length)
    .map(section => renderGitSection(section, sections[section.key]))
    .join("")) || `<div class="git-empty">No changed files.</div>`;
  // The collapsed line re-renders on refresh and on every viewport tier change, and rebuilding an
  // unchanged list would throw away the reader's scroll position in it.
  if (state.gitBodyHtml === html) return;
  state.gitBodyHtml = html;
  body.innerHTML = html;
  body.querySelectorAll("[data-git-diff]").forEach(row => {
    row.onclick = () => openGitDiff(row.dataset.gitDiff, row.dataset.gitSide);
  });
}

function renderGitSection(section, files) {
  const rows = files.map(file => renderGitFileRow(file, section.key)).join("");
  return `<div class="git-section-title">${escapeHtml(section.label)} <span class="muted">${files.length}</span><span class="git-section-rule"></span></div>${rows}`;
}

function renderGitFileRow(file, sectionKey) {
  const path = String(file.path || "");
  const status = gitRowStatus(file, sectionKey);
  // Untracked files have nothing to diff against, so the row stays inert rather than opening an
  // overlay that would report an empty diff.
  const canDiff = sectionKey !== "untracked";
  const title = file.old_path ? `${file.old_path} → ${path}` : path;
  // The row carries its own side, so a path listed under both Staged and Not staged opens the diff
  // that matches the row's line counts rather than the two concatenated.
  const side = gitSectionSide(sectionKey);
  return `<button type="button" class="git-file" title="${escapeAttr(title)}"${canDiff ? ` data-git-diff="${escapeAttr(path)}" data-git-side="${escapeAttr(side)}"` : " disabled"}>
    <span class="git-file-status status-${escapeAttr(status.kind)}">${escapeHtml(status.code)}</span>
    <span class="git-file-path">${renderGitPath(path)}</span>
    <span class="git-file-stat">${renderGitFileStat(file, sectionKey, canDiff)}</span>
  </button>`;
}

/* A conflict lives in the worktree, so it reads the unstaged side — same as its line counts. */
function gitSectionSide(sectionKey) {
  return sectionKey === "staged" ? "staged" : "unstaged";
}

/* The basename is the identifier and the directory is context, so the directory is dimmed and
   shortened from the left when it is long — the tail of a path is what you are reading. */
function renderGitPath(path) {
  // An untracked directory is reported collapsed, with a trailing slash. Its own name is the
  // identifier, so the slash is set aside before splitting and put back on the name — otherwise
  // the whole entry reads as a directory with no file in it.
  const isDirectory = path.endsWith("/");
  const bare = isDirectory ? path.slice(0, -1) : path;
  const cut = bare.lastIndexOf("/");
  const name = escapeHtml((cut < 0 ? bare : bare.slice(cut + 1)) + (isDirectory ? "/" : ""));
  if (cut < 0) return `<span class="git-file-name">${name}</span>`;
  let dir = bare.slice(0, cut + 1);
  if (dir.length > GIT_PATH_DIR_MAX) {
    const segments = dir.split("/").filter(Boolean);
    while (segments.length > 1 && segments.join("/").length + 2 > GIT_PATH_DIR_MAX) segments.shift();
    const shortened = "…/" + segments.join("/") + "/";
    // A single long directory name can't be shortened by dropping segments — prefixing it would
    // only make it longer — so it is left to the row's own overflow handling.
    if (shortened.length < dir.length) dir = shortened;
  }
  return `<span class="git-file-dir">${escapeHtml(dir)}</span><span class="git-file-name">${name}</span>`;
}

/* Line counts for the side of the file this row represents — a file staged and then modified again
   appears in two sections, and each row reports only its own side rather than repeating a combined
   figure. A conflict lives in the worktree, so it reads the unstaged side. */
function renderGitFileStat(file, sectionKey, canDiff) {
  if (!canDiff) return `<span class="muted">new</span>`;
  // A conflict is diffed against each merge stage, so there is no single count to show; the row's
  // `U` and its section already say what it is.
  if (sectionKey === "conflicted") return "";
  const staged = sectionKey === "staged";
  const added = staged ? file.staged_added : file.unstaged_added;
  const deleted = staged ? file.staged_deleted : file.unstaged_deleted;
  if (typeof added !== "number" && typeof deleted !== "number") return `<span class="muted">bin</span>`;
  return gitDiffstatHtml(added || 0, deleted || 0);
}

function gitDiffstatHtml(added, deleted) {
  return `<span class="git-added${added ? "" : " git-zero"}">+${added}</span><span class="git-deleted${deleted ? "" : " git-zero"}">−${deleted}</span>`;
}

function gitRowStatus(file, sectionKey) {
  if (sectionKey === "untracked") return { code:"?", kind:"untracked" };
  if (sectionKey === "conflicted") return { code:"U", kind:"unmerged" };
  const name = sectionKey === "staged" ? file.index_status : file.worktree_status;
  return { code:gitStatusCode(name), kind:name || "modified" };
}

function gitStatusCode(status) {
  if (status === "modified") return "M";
  if (status === "added") return "A";
  if (status === "deleted") return "D";
  if (status === "renamed") return "R";
  if (status === "copied") return "C";
  if (status === "typechange") return "T";
  if (status === "unmerged") return "U";
  if (status === "untracked") return "?";
  return "•";
}

async function loadGitStatus(pid) {
  pid = pid || state.projectId;
  if (!pid) return;
  const requestSeq = ++state.gitRequestSeq;
  state.gitLoading = true;
  state.gitError = null;
  renderGitLine();
  try {
    const res = await api("GET", `/api/projects/${pid}/git/status`);
    if (requestSeq !== state.gitRequestSeq || state.projectId !== pid) return;
    state.gitStatus = res;
    state.gitError = res && res.error ? res.error : null;
    state.gitRepoByProject.set(pid, !!(res && res.is_repository));
  } catch (e) {
    if (requestSeq !== state.gitRequestSeq || state.projectId !== pid) return;
    state.gitStatus = null;
    state.gitError = apiFailureMessage(e) || "Could not load git status.";
  } finally {
    if (requestSeq === state.gitRequestSeq && state.projectId === pid) {
      state.gitLoading = false;
      renderGitLine();
    }
  }
}

/* The working tree moves while the agent works, so the line follows it rather than showing what
   was true when the thread was opened. A completed turn always schedules a refresh; a file-change
   item schedules one too, so the counts move during the turn instead of only at the end. Both are
   coalesced, so a turn touching twenty files still costs one request, and nothing is scheduled for
   a project whose workspace is not a repository. */
const GIT_REFRESH_DEBOUNCE_MS = 1200;

function scheduleGitRefresh() {
  if (!state.projectId || $("gitLine").hidden) return;
  clearTimeout(state.gitRefreshTimer);
  state.gitRefreshTimer = setTimeout(() => {
    if (state.projectId && !$("gitLine").hidden) loadGitStatus(state.projectId);
  }, GIT_REFRESH_DEBOUNCE_MS);
}

function setGitExpanded(expanded, opts) {
  expanded = !!expanded;
  if (state.gitExpanded === expanded) {
    if (!(opts && opts.skipRender)) renderGitLine();
    return;
  }
  state.gitExpanded = expanded;
  if (!(opts && opts.skipRender)) renderGitLine();
}

/* Both diff openers capture the project up front and drop the response if the user has moved on,
   the same guard `loadGitStatus` uses — otherwise switching projects mid-request opens an overlay
   holding the previous project's diff. */
async function openGitDiff(path, side) {
  const pid = state.projectId;
  if (!pid || !path || state.gitDiffPending) return;
  const query = `path=${encodeURIComponent(path)}${side ? `&side=${encodeURIComponent(side)}` : ""}`;
  state.gitDiffPending = true;
  try {
    const res = await api("GET", `/api/projects/${pid}/git/diff?${query}`);
    if (state.projectId !== pid) return;
    if (!res || res.is_empty || !String(res.diff || "").trim()) {
      notice(`No ${side === "staged" ? "staged " : ""}diff available for ${path}.`, "warning");
      return;
    }
    openDiffOverlay(side === "staged" ? `${path} (staged)` : path, res.diff);
  } catch (e) {
    if (state.projectId !== pid) return;
    notice("Could not load git diff: " + apiFailureMessage(e), "error");
  } finally {
    state.gitDiffPending = false;
  }
}

async function openGitWorkingDiff() {
  const pid = state.projectId;
  if (!pid || state.gitDiffPending) return;
  state.gitDiffPending = true;
  try {
    const res = await api("GET", `/api/projects/${pid}/git/diff`);
    if (state.projectId !== pid) return;
    if (!res || res.is_empty || !String(res.diff || "").trim()) {
      notice("No tracked changes to review.", "warning");
      return;
    }
    openDiffOverlay("Working tree", res.diff);
  } catch (e) {
    if (state.projectId !== pid) return;
    notice("Could not load git diff: " + apiFailureMessage(e), "error");
  } finally {
    state.gitDiffPending = false;
  }
}

$("gitLineToggle").onclick = () => setGitExpanded(!state.gitExpanded);
$("gitRefresh").onclick = () => loadGitStatus(state.projectId);
$("gitReviewAll").onclick = () => openGitWorkingDiff();
/* The branch's character budget is width-tiered, so re-render the collapsed line when the viewport
   crosses a tier. Debounced because this also fires continuously while dragging a window edge. */
window.addEventListener("resize", () => {
  clearTimeout(state.gitResizeTimer);
  state.gitResizeTimer = setTimeout(() => { if (!$("gitLine").hidden) renderGitLine(); }, 150);
});

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

function subagentThreadsForActiveProject() {
  const activeThreadId = state.threadId ? String(state.threadId) : "";
  const threads = knownProjectThreads(state.projectId);
  return threads.filter(t =>
    isManagedSubagentThread(t, threads) && String(t.parent_thread_id || "") === activeThreadId
  );
}

// Everything a card reports about the subtree it owns, from one walk of the activity map. A card
// stands for its whole subtree — a nested grandchild is not listed separately, since the menu lists
// direct children of the open thread — so an unreported descendant is reported nowhere.
//
// `running` and `waitingOnUser` are separate flags rather than reads of the winning activity: a
// request for the user also sets `active_turn`, so a blocked child would otherwise be
// indistinguishable from a busy one, and an erroring descendant outranks a running sibling without
// cancelling it. `activity`
// is the ranked winner that names the card's state, with `origin` set when it belongs to a
// descendant, so the summary can describe that descendant rather than this thread.
function subagentSubtreeState(threadId) {
  const key = String(threadId);
  let running = false;
  let waitingOnUser = false;
  let activity = null;
  let origin = null;
  for (const [otherId, other] of state.threadActivity) {
    if (otherId !== key && !threadDescendsFrom(otherId, key)) continue;
    if (other.active_turn) running = true;
    if (activityWaitsOnUser(other)) waitingOnUser = true;
    if (threadActivityRank(other) <= threadActivityRank(activity)) continue;
    activity = other;
    origin = otherId === key ? null : otherId;
  }
  return { running, waitingOnUser, activity, origin };
}

function subagentCounts() {
  const agents = subagentThreadsForActiveProject();
  let running = 0;
  let waitingOnUser = 0;
  for (const thread of agents) {
    const subtree = subagentSubtreeState(thread.id);
    if (subtree.running) running += 1;
    if (subtree.waitingOnUser) waitingOnUser += 1;
  }
  return { total:agents.length, running, waitingOnUser };
}

function renderSubagentsButton() {
  const btn = $("subagentsBtn");
  if (!btn) return;
  const counts = subagentCounts();
  const stateName = counts.waitingOnUser ? "waiting" : (counts.running ? "running" : "idle");
  btn.className = `badge subagents-btn state-${stateName}`;
  btn.disabled = !state.projectId || (!counts.total && !counts.running);
  const runningLabel = `${counts.running} running sub-agent${counts.running === 1 ? "" : "s"} · ${counts.total} total`;
  btn.title = counts.total
    ? (counts.waitingOnUser
        ? `${counts.waitingOnUser} sub-agent${counts.waitingOnUser === 1 ? "" : "s"} waiting on you · ${runningLabel}`
        : runningLabel)
    : "No sub-agents";
  $("subagentsCount").textContent = String(counts.waitingOnUser || counts.running || counts.total);
  if (!$("subagentsMenu").hidden) renderSubagentsMenu();
}

function renderSubagentsMenu() {
  const menu = $("subagentsMenu");
  const agents = subagentThreadsForActiveProject();
  const counts = subagentCounts();
  const rows = agents.map(renderSubagentCard).join("");
  const waitingSummary = counts.waitingOnUser ? `${counts.waitingOnUser} waiting on you · ` : "";
  const summary = agents.length
    ? `<div class="subagents-summary${counts.waitingOnUser ? " waiting" : ""}">${waitingSummary}${counts.running} running · ${counts.total} total</div>`
    : "";
  menu.innerHTML = `
    <div class="subagents-head">
      <strong>Sub-agents</strong>
      <button id="subagentsClose" type="button">Close</button>
    </div>
    ${summary}
    <div class="subagents-list">${rows || `<div class="muted">No sub-agents for this thread yet.</div>`}</div>`;
  $("subagentsClose").onclick = () => { $("subagentsMenu").hidden = true; };
  menu.querySelectorAll("[data-subagent-thread-id]").forEach(btn => {
    btn.onclick = () => {
      const tid = btn.dataset.subagentThreadId;
      const meta = threadMetaForId(tid) || agents.find(t => String(t.id) === String(tid));
      openThread(state.projectId, tid, (meta && meta.title) || "Sub-agent");
    };
  });
}

function renderSubagentCard(thread) {
  const { running, waitingOnUser, activity, origin } = subagentSubtreeState(thread.id);
  const stateLabel = waitingOnUser
    ? "Waiting on you"
    : (running ? "Running" : (activity && activity.kind === "error" ? "Error" : "Idle"));
  const parent = thread.parent_thread_id ? knownProjectThreads(state.projectId).find(t => String(t.id) === String(thread.parent_thread_id)) : null;
  const parentLabel = parent && parent.title ? `Parent: ${parent.title}` : "Parent thread";
  const summary = activity && activity.summary
    ? threadActivityTooltip(activity, origin)
    : parentLabel;
  const active = state.threadId && String(state.threadId) === String(thread.id);
  const name = subagentDisplayName(thread);
  const stateClass = waitingOnUser ? " waiting" : (running ? " running" : "");
  return `<button type="button" class="subagent-card${active ? " active" : ""}${waitingOnUser ? " waiting" : ""}" data-subagent-thread-id="${escapeAttr(thread.id)}">
    <span class="subagent-card-title">
      <span class="subagent-card-name">${escapeHtml(name)}</span>
      <span class="subagent-card-state${stateClass}">${stateLabel}</span>
    </span>
    <span class="subagent-card-meta">${escapeHtml(summary)}</span>
  </button>`;
}

function subagentDisplayName(thread) {
  const title = String((thread && thread.title) || "").trim();
  const name = title.replace(/^Sub-agent:\s*/i, "").trim();
  return name && !subagentNameLooksLikeId(name) ? name : "Sub-agent";
}

function subagentNameLooksLikeId(name) {
  return /^[0-9a-f]{8,}(?:-[0-9a-f]{4,})+$/i.test(String(name || "").trim());
}

function toggleSubagentsMenu() {
  const menu = $("subagentsMenu");
  menu.hidden = !menu.hidden;
  if (!menu.hidden) {
    $("tasksMenu").hidden = true;
    $("mcpMenu").hidden = true;
    $("usageMenu").hidden = true;
    renderSubagentsMenu();
  }
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
    $("subagentsMenu").hidden = true;
    $("mcpMenu").hidden = true;
    $("usageMenu").hidden = true;
    renderTasksMenu();
  }
}
$("tasksBtn").onclick = (e) => { e.stopPropagation(); toggleTasksMenu(); };
$("tasksMenu").onclick = (e) => e.stopPropagation();
$("subagentsBtn").onclick = (e) => { e.stopPropagation(); toggleSubagentsMenu(); };
$("subagentsMenu").onclick = (e) => e.stopPropagation();
document.addEventListener("click", (e) => {
  const menu = $("subagentsMenu");
  if (menu.hidden) return;
  if (e.target.closest && e.target.closest(".subagents-wrap")) return;
  menu.hidden = true;
});
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
  const key = scopedItemKey(turnId, item.id); if (!key) return;
  const tool = item.tool || {};
  let body = state.streamElsByItemId.get(key);
  if (!body) body = taskBubble(key, "tool_call", "tool running-tool state-running", "tool");
  state.streamElsByItemId.set(key, body);
  state.toolBodyElsByItemId.set(key, body);
  state.commandMsgElsByItemId.set(key, body.parentElement);
  body.parentElement.dataset.toolItemId = key;
  body.parentElement.dataset.toolStartedAtMs = String(normalizeTimestampMs(tool.started_at_ms, Date.now()));
  registerRenderedItemBody(body, item, turnId);
  renderItemBody(body, {
    kind:"tool_call",
    name:tool.name || "tool",
    input:tool.input,
    output:null,
    server:tool.server || null,
    status:tool.status || "in_progress",
    metadata:tool.metadata || null,
    subagent:tool.subagent || null,
    error:null
  });
}
function appendToolProgress(turnId, itemId, text) {
  const key = scopedItemKey(turnId, itemId);
  let body = state.streamElsByItemId.get(key);
  if (!body) {
    body = taskBubble(key, "tool_call", "tool running-tool state-running", "tool");
    state.streamElsByItemId.set(key, body);
    state.toolBodyElsByItemId.set(key, body);
    state.commandMsgElsByItemId.set(key, body.parentElement);
    body.parentElement.dataset.toolItemId = key;
    renderItemBody(body, {
      kind:"tool_call",
      name:"tool",
      input:null,
      output:null,
      server:null,
      status:"in_progress",
      metadata:null,
      subagent:null,
      error:null
    });
  }
  registerRenderedItemBody(body, { id:itemId }, turnId);
  const chunk = String(text || "");
  const payload = state.toolPayloadsByItemId.get(key);
  if (payload) {
    const current = typeof payload.output === "string" ? payload.output : "";
    payload.output = current ? current + "\n" + chunk : chunk;
    state.toolPayloadsByItemId.set(key, payload);
    renderItemBody(body, payload);
    $("transcript").scrollTop = $("transcript").scrollHeight;
    return;
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
function appendStream(turnId, text, itemId, deltaType) {
  const key = scopedItemKey(turnId, itemId);
  if (key && state.renderedItemIds.has(key)) return;
  // Identified items always own distinct rows. The global stream fallback exists only for legacy
  // deltas without an item identity; using it for a new key can merge interleaved items.
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
  if (key) registerRenderedItemBody(body, { id:itemId }, turnId);
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
function finalizeStreamedItem(item, turnId) {
  if (!item || !item.payload) return false;
  const key = scopedItemKey(turnId, item.id);
  const body = state.streamElsByItemId.get(key) ||
    state.commandBodyElsByItemId.get(key) ||
    state.toolBodyElsByItemId.get(key) ||
    renderedItemBody(item, turnId) ||
    (state.streamItemId===key ? state.streamEl : null);
  if (!hasVisiblePayload(item.payload)) {
    if (body && !body.textContent.trim()) {
      if (!removeTaskGroupItem(key)) body.parentElement.remove();
    }
    markRenderedItem(item, turnId);
    if (state.streamEl === body) {
      state.streamEl = null;
      state.streamItemId = null;
    }
    return true;
  }
  if (!body) return false;
  if (item.payload.kind==="command_execution") {
    state.commandBodyElsByItemId.set(key, body);
    state.commandMsgElsByItemId.set(key, body.parentElement);
    body.parentElement.dataset.commandItemId = key;
  }
  if (item.payload.kind==="tool_call") {
    state.toolBodyElsByItemId.set(key, body);
    state.commandMsgElsByItemId.set(key, body.parentElement);
    body.parentElement.dataset.toolItemId = key;
  }
  renderItemBodyForItem(body, item, turnId);
  registerRenderedItemBody(body, item, turnId);
  if (item.payload.kind==="command_execution") finishRunningCommand(item, turnId);
  markRenderedItem(item, turnId);
  if (state.streamEl === body) {
    state.streamEl = null;
    state.streamItemId = null;
  }
  $("transcript").scrollTop = $("transcript").scrollHeight;
  return true;
}
function addItem(item, turnId, fromHistory) {
  const p = item && item.payload ? item.payload : item;
  if (!p) return;
  // Plan updates are shown in the pinned plan card above the composer, not as transcript rows. Live
  // updates drive the card; persisted (history) plan rows are simply dropped — a finished plan's
  // card has already disappeared, so there is nothing to replay.
  if (isPlanItem(item)) {
    if (!fromHistory) updatePlanCard(item);
    return;
  }
  const key = scopedItemKey(turnId, item && item.id);
  const hasPreservedUserDisplay = p.kind === "user_message" &&
    ((state.pendingUserEl && preservesUserInputDisplay(state.pendingUserEl)) ||
     !!provisionalUserBodyForTurn(turnId));
  const visible = hasVisiblePayload(p) || hasPreservedUserDisplay;
  if (isRenderedItem(item, turnId)) {
    // Upsert: a repeated item id within the same turn refreshes the existing row.
    const existing = renderedItemBody(item, turnId);
    const harnessExisting = renderedHarnessItemBody(item, turnId);
    if (existing && harnessExisting && existing!==harnessExisting) {
      console.error("Giskard item identity invariant violated; refusing conflicting item upsert.", {
        turnId:idKey(turnId),
        itemId:idKey(item && item.id),
        harnessItemId:idKey(item && item.harness_item_id)
      });
      return;
    }
    if (!existing && harnessExisting) {
      console.error("Giskard item identity invariant violated; refusing harness-only item upsert.", {
        turnId:idKey(turnId),
        itemId:idKey(item && item.id),
        harnessItemId:idKey(item && item.harness_item_id)
      });
      return;
    }
    if (existing) {
      if (!visible) {
        markRenderedItem(item, turnId);
        return;
      }
      registerRenderedItemBody(existing, item, turnId);
      if (!(p.kind === "user_message" && preservesUserInputDisplay(existing))) {
        renderItemBodyForItem(existing, item, turnId);
      }
      if (p.kind==="user_message" && isSyntheticSubagentPrompt(item)) {
        placeRowFirstInTurn(existing.parentElement, turnId);
      }
      if (p.kind==="command_execution") finishRunningCommand(item, turnId);
      markRenderedItem(item, turnId);
      return;
    }
    if (!visible) {
      markRenderedItem(item, turnId);
      return;
    }
    // A previous invisible item can mark this identity rendered without creating a body. Let the
    // now-visible upsert fall through and create its first transcript row.
  } else if (!visible) {
    markRenderedItem(item, turnId);
    return;
  }
  if (p.kind==="user_message") {
    if (state.pendingUserEl && !state.pendingUserEl.isConnected) {
      state.pendingUserEl = null;
      state.pendingUserText = null;
    }
    if (state.pendingUserEl &&
        (p.text===state.pendingUserText || preservesUserInputDisplay(state.pendingUserEl))) {
      state.pendingUserEl.classList.remove("pending");
      const pendingBody = state.pendingUserEl.querySelector(".body");
      if (!preservesUserInputDisplay(pendingBody)) {
        renderItemBodyForItem(pendingBody, item, turnId);
      }
      registerRenderedItemBody(pendingBody, item, turnId);
      state.pendingUserEl = null;
      state.pendingUserText = null;
      markRenderedItem(item, turnId);
      return;
    }
    const provisionalBody = provisionalUserBodyForTurn(turnId);
    if (provisionalBody) {
      delete provisionalBody.parentElement.dataset.liveUserInput;
      if (!preservesUserInputDisplay(provisionalBody)) {
        renderItemBodyForItem(provisionalBody, item, turnId);
      }
      registerRenderedItemBody(provisionalBody, item, turnId);
      placeRowFirstInTurn(provisionalBody.parentElement, turnId);
      markRenderedItem(item, turnId);
      return;
    }
    const body = bubble("user","you");
    renderItemBody(body, p);
    registerRenderedItemBody(body, item, turnId);
    if (isSyntheticSubagentPrompt(item)) placeRowFirstInTurn(body.parentElement, turnId);
  }
  else {
    if (p.kind==="file_change") {
      const mergedRow = mergeFileChangeWithPrevious(p, item, turnId);
      if (mergedRow) {
        registerRenderedItemBody(mergedRow.querySelector(".body"), item, turnId);
        markRenderedItem(item, turnId);
        return;
      }
    }
    const body = isTaskPayloadKind(p.kind)
      ? taskBubble(key, p.kind, classForPayload(p), roleForPayload(p))
      : bubble(classForPayload(p), roleForPayload(p));
    if (p.kind==="command_execution") {
      state.commandBodyElsByItemId.set(key, body);
      state.commandMsgElsByItemId.set(key, body.parentElement);
      body.parentElement.dataset.commandItemId = key;
    }
    if (p.kind==="tool_call") {
      state.toolBodyElsByItemId.set(key, body);
      state.commandMsgElsByItemId.set(key, body.parentElement);
      body.parentElement.dataset.toolItemId = key;
    }
    renderItemBodyForItem(body, item, turnId);
    registerRenderedItemBody(body, item, turnId);
  }
  if (p.kind==="command_execution") finishRunningCommand(item, turnId);
  markRenderedItem(item, turnId);
}
function resetRenderState() {
  clearPlanCard();   // dropping/switching threads clears any pinned plan
  state.streamEl = null;
  state.streamItemId = null;
  state.streamElsByItemId = new Map();
  state.renderedItemIds = new Set();
  state.renderedHarnessItemIds = new Set();
  state.renderedItemBodyByKey = new Map();
  state.itemKindsByItemId = new Map();
  state.pendingApprovals = new Map();
  state.answeredApprovalsById = new Map();
  state.renderedApprovalStateKeys = new Set();
  state.pendingServerRequests = new Map();
  state.answeredServerRequests = new Set();
  state.runningCommands = new Map();
  state.commandBodyElsByItemId = new Map();
  state.commandMsgElsByItemId = new Map();
  state.commandStopRequestedByItemId = new Set();
  state.commandPayloadsByItemId = new Map();
  state.endedCommandsByItemId = new Map();
  state.toolPayloadsByItemId = new Map();
  state.toolBodyElsByItemId = new Map();
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
function scopedItemKey(turnId, itemId) {
  const turn = idKey(turnId);
  const item = idKey(itemId);
  return turn && item ? `${turn}:${item}` : "";
}
function scopedHarnessKey(turnId, harnessItemId) {
  const turn = idKey(turnId);
  const harness = idKey(harnessItemId);
  return turn && harness ? `${turn}:${harness}` : "";
}
function identityTokens(value) {
  return String(value || "").split(" ").filter(Boolean);
}
function addIdentityToken(existing, token) {
  const value = idKey(token);
  if (!value) return existing || "";
  const tokens = new Set(identityTokens(existing));
  tokens.add(value);
  return Array.from(tokens).join(" ");
}
function pruneKeySet(set, allowed) {
  for (const key of Array.from(set)) if (!allowed.has(key)) set.delete(key);
}
function itemIdentityKeys(item, turnId) {
  return {
    itemKey: scopedItemKey(turnId, item && item.id),
    harnessKey: scopedHarnessKey(turnId, item && item.harness_item_id)
  };
}
function renderedItemBody(item, turnId) {
  const keys = itemIdentityKeys(item, turnId);
  return (keys.itemKey && state.renderedItemBodyByKey.get(keys.itemKey)) || null;
}
function renderedHarnessItemBody(item, turnId) {
  const keys = itemIdentityKeys(item, turnId);
  return (keys.harnessKey && state.renderedItemBodyByKey.get(keys.harnessKey)) || null;
}
function registerRenderedItemBody(body, item, turnId) {
  if (!body || !item) return;
  const row = body.parentElement;
  if (!row) return;
  if (turnId) row.dataset.turn = idKey(turnId);
  const keys = itemIdentityKeys(item, turnId);
  if (item && item.id) row.dataset.item = addIdentityToken(row.dataset.item, item.id);
  if (item && item.harness_item_id) {
    row.dataset.harnessItem = addIdentityToken(row.dataset.harnessItem, item.harness_item_id);
  }
  if (keys.itemKey) state.renderedItemBodyByKey.set(keys.itemKey, body);
  if (keys.harnessKey) state.renderedItemBodyByKey.set(keys.harnessKey, body);
}
function isRenderedItem(item, turnId) {
  const keys = itemIdentityKeys(item, turnId);
  return (keys.itemKey && state.renderedItemIds.has(keys.itemKey)) ||
    (keys.harnessKey && state.renderedHarnessItemIds.has(keys.harnessKey));
}
function markRenderedItem(item, turnId) {
  const keys = itemIdentityKeys(item, turnId);
  if (keys.itemKey) {
    state.renderedItemIds.add(keys.itemKey);
    state.streamElsByItemId.delete(keys.itemKey);
    state.itemKindsByItemId.delete(keys.itemKey);
  }
  if (keys.harnessKey) {
    state.renderedHarnessItemIds.add(keys.harnessKey);
  }
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
    attachSubagentLinkActions(body, p);
  }
  const taskItemId = msg.dataset.commandItemId || msg.dataset.toolItemId || "";
  if (taskItemId) {
    syncTaskGroupItem(taskItemId);
    refreshOutputOverlay(taskItemId);
  }
}
function renderItemBodyForItem(body, item, turnId) {
  const p = item && item.payload ? item.payload : item;
  if (p && p.kind==="file_change") {
    renderFileChangeContribution(body, p, item, turnId);
    return;
  }
  renderItemBody(body, p);
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
  const projectId = state.projectId;
  const threadId = state.threadId;
  const cacheKey = (projectId || "") + "\n" + text;
  el._markdownRenderKey = cacheKey;
  if (!text.trim() || !projectId) return;

  const apply = (html) => {
    if (el._markdownRenderKey !== cacheKey) return;
    if (projectId !== state.projectId || threadId !== state.threadId) return;
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
function currentProjectDir() {
  return (state.projectDirs && state.projectId && state.projectDirs[state.projectId]) || "";
}
function trimTrailingSlash(path) {
  const value = String(path || "");
  return value === "/" ? value : value.replace(/\/+$/,"");
}
function displayPathForProject(path) {
  const value = String(path || "");
  if (!value) return "";
  if (!value.startsWith("/")) return value;
  const root = trimTrailingSlash(currentProjectDir());
  if (!root || !root.startsWith("/")) return value;
  if (value === root) return ".";
  if (root === "/") return value.slice(1);
  const prefix = root + "/";
  return value.startsWith(prefix) ? value.slice(prefix.length) : value;
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
function fileChangeContributionKey(item, turnId) {
  const keys = itemIdentityKeys(item, turnId);
  return keys.itemKey;
}
function renderFileChangeContribution(body, p, item, turnId) {
  const row = body && body.parentElement;
  const key = fileChangeContributionKey(item, turnId);
  if (!row || !key) {
    renderFileChange(body, p);
    return;
  }
  if (!row._fileChangePayloadsByItemKey) row._fileChangePayloadsByItemKey = new Map();
  row._fileChangePayloadsByItemKey.set(key, normalizedFileChangePayload(p));
  const contributions = Array.from(row._fileChangePayloadsByItemKey.values());
  const merged = contributions.reduce((current, next) => (
    current ? mergeFileChangePayload(current, next) : next
  ), null);
  renderFileChange(body, merged || p);
}
function mergeFileChangeWithPrevious(p, item, turnId) {
  if (!fileChangeContributionKey(item, turnId)) return null;
  breakTaskGroup();
  const target = renderTarget();
  const prev = target && target.lastElementChild;
  if (
    !prev ||
    !prev.classList ||
    !prev.classList.contains("file") ||
    prev.dataset.turn!==idKey(turnId) ||
    !prev._fileChangePayload
  ) return null;
  const body = prev.querySelector(".body");
  if (!body) return null;
  renderFileChangeContribution(body, p, item, turnId);
  keepTranscriptRowAnchored(prev);
  return prev;
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
    row.append(kind, document.createTextNode(" "), makePathLink(c.path || "", displayPathForProject(c.path), null));
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
  attachSubagentLinkActions(body, p);
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
function renderToolIoBlocks(body, opts) {
  const blocks = opts.blocks || [];
  if (!blocks.length) {
    clearRowToggle(body.parentElement);
    return;
  }
  const itemId = idKey(opts.itemId);
  const phase = opts.phase || "completed";
  const stats = toolIoStats(blocks);
  // Like command output, the tool input/output preview is shown whenever the tool row is visible —
  // there is no second collapse level. The full input/output lives in the overlay via "Open".
  const msg = body.parentElement;
  clearRowToggle(msg);

  const summary = document.createElement("div");
  summary.className = "meta cmd-output-summary";
  const label = commandOutputStatsLabel(stats, phase);
  const text = document.createElement("span");
  text.className = "cmd-output-summary-text";
  text.textContent = stats.chars ? `Tool data · ${label}` : label;
  summary.append(text);
  if (stats.chars || phase === "running") {
    summary.append(makeOutputOverlayButton(itemId, "tool"));
  }
  body.append(summary);

  let anyTruncated = false;
  for (const block of blocks) {
    const section = document.createElement("div");
    section.className = "tool-io";
    const heading = document.createElement("div");
    heading.className = "meta";
    heading.textContent = block.label;
    // Input keeps its head (the call arguments); output keeps its tail (latest result/progress).
    const preview = inlineOutputPreview(block.text, block.label === "Input" ? "head" : "tail");
    if (preview.truncated) anyTruncated = true;
    const pre = document.createElement("pre");
    pre.className = "out";
    pre.textContent = preview.text;
    section.append(heading, pre);
    body.append(section);
  }
  if (anyTruncated) {
    const note = document.createElement("div");
    note.className = "meta cmd-output-truncated";
    note.textContent = "Preview trimmed — Open ⤢ for the full input/output";
    body.append(note);
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
  if (subagentLinkInfo(p)) return renderSubagentActivity(p);
  const detail = p.detail ? `<div>${escapeHtml(p.detail)}</div>` : "";
  const metadata = visibleActivityMetadata(p);
  const meta = metadata ? `<pre class="out">${escapeHtml(jsonPreview(metadata))}</pre>` : "";
  return `<div>${escapeHtml(p.title||"Activity")}</div>${detail}${meta}`;
}
function subagentLinkInfo(p) {
  const link = p && p.subagent;
  if (!link) return null;
  return {
    agentPath:String(link.path || ""),
    title:p.title || "Sub-agent"
  };
}
function renderSubagentActivity(p) {
  const info = subagentLinkInfo(p);
  if (!info) return "";
  const detail = p.detail ? `<div>${escapeHtml(p.detail)}</div>` : "";
  return [
    `<div>${escapeHtml(info.title)}</div>`,
    detail,
    `<button type="button" class="subagent-open-btn" data-agent-path="${escapeAttr(info.agentPath)}">Open linked thread</button>`
  ].join("");
}
function attachSubagentLinkActions(body, p) {
  const info = subagentLinkInfo(p);
  if (!info) return;
  let btn = body.querySelector(".subagent-open-btn");
  if (!btn) {
    btn = document.createElement("button");
    btn.type = "button";
    btn.className = "subagent-open-btn";
    btn.textContent = "Open linked thread";
    body.append(btn);
  }
  const parentTid = state.threadId;
  btn.dataset.agentPath = info.agentPath;
  btn.onclick = () => openSubagentThreadFromActivity(btn, info, {
    focus:true,
    parentTid,
    itemId:subagentActivityItemId(btn)
  });
}

function validGiskardItemId(value) {
  const itemId = value === undefined || value === null ? "" : String(value).trim();
  return /^[0-7][0-9A-HJKMNP-TV-Z]{25}$/i.test(itemId) ? itemId : null;
}

function subagentActivityItemId(btn) {
  const row = btn && btn.closest ? btn.closest(".msg") : null;
  const ids = identityTokens(row && row.dataset ? row.dataset.item : null);
  return ids.map(validGiskardItemId).find(Boolean) || null;
}
function subagentImportKey(pid, parentTid, itemId) {
  return [pid || "", parentTid || "", itemId || ""].join(":");
}
async function importSubagentThread(parentTid, itemId) {
  const pid = state.projectId;
  const res = await api(
    "POST",
    `/api/projects/${pid}/threads/${parentTid}/subagent-links/${itemId}/open`
  );
  return { threadId:res.thread_id, title:res.title || "Sub-agent" };
}
async function openSubagentThreadFromActivity(btn, info, opts) {
  opts = opts || {};
  const itemId = validGiskardItemId(opts.itemId);
  if (!state.projectId || !opts.parentTid || !info || !itemId) return;
  btn.dataset.linkItemId = itemId;
  const key = subagentImportKey(state.projectId, opts.parentTid, itemId);
  let imported = state.subagentImports.get(key);
  if (!imported || !imported.threadId) {
    btn.disabled = true;
    btn.textContent = "Opening...";
    try {
      const result = await importSubagentThread(opts.parentTid, itemId);
      imported = { status:"ready", threadId:result.threadId, title:result.title };
      state.subagentImports.set(key, imported);
      await loadThreads(state.projectId);
    } catch (e) {
      btn.disabled = false;
      btn.textContent = "Open linked thread";
      notice("Open linked thread failed: " + apiFailureMessage(e), "error");
      return;
    }
  }
  await openThread(state.projectId, imported.threadId, imported.title || "Sub-agent");
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
    $("subagentsMenu").hidden = true;
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
function isMarkdownSourcePath(path, language) {
  const lower = String(path || "").toLowerCase();
  return lower.endsWith(".md") || lower.endsWith(".markdown") || String(language || "").toLowerCase() === "markdown";
}
function setCodeSourceToggle(visible, label) {
  const btn = $("codeSourceToggle");
  btn.hidden = !visible;
  btn.disabled = !visible;
  if (visible) btn.textContent = label || "Source";
}
function codeOverlayRequestMatches(path, projectId, requestId) {
  return state.codePath === path &&
    state.projectId === projectId &&
    $("codeOverlay").dataset.requestId === requestId;
}
async function openCodeOverlay(path, line) {
  if (!state.projectId || !path) return;
  const requestId = Math.random().toString(36).slice(2);
  state.codePath = path;
  state.codeLine = normalizeLine(line, null);
  state.codeOverlaySource = null;
  $("codeOverlay").classList.add("open");
  $("codeOverlay").dataset.requestId = requestId;
  $("codePath").textContent = state.codeLine ? `${path}#${state.codeLine}` : path;
  $("codeMeta").textContent = "Loading…";
  $("codeView").innerHTML = `<div class="code-empty">Loading source…</div>`;
  $("codeDownload").disabled = false;
  setCodeSourceToggle(false);

  const projectId = state.projectId;
  try {
    const res = await api("GET", projectFileUrl("highlight", path));
    if (!codeOverlayRequestMatches(path, projectId, requestId)) return;
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
      if (isMarkdownSourcePath(path, res.language)) {
        await renderMarkdownCodeOverlay(path, res, projectId, requestId);
      }
    }
  } catch (e) {
    if (!codeOverlayRequestMatches(path, projectId, requestId)) return;
    $("codeMeta").textContent = "Could not load source";
    $("codeView").innerHTML = `<div class="code-empty">${escapeHtml(e.message || "Could not load file.")}</div>`;
    $("codeDownload").disabled = true;
    setCodeSourceToggle(false);
  }
}
async function renderMarkdownCodeOverlay(path, highlightRes, projectId, requestId) {
  const line = state.codeLine;
  try {
    const source = await api("GET", `/api/projects/${projectId}/raw?path=${encodeURIComponent(path)}`);
    if (!codeOverlayRequestMatches(path, projectId, requestId)) return;
    const cacheKey = projectId + "\n" + String(source || "");
    let html;
    if (state.markdownCache.has(cacheKey)) {
      html = state.markdownCache.get(cacheKey);
    } else {
      const rendered = await api("POST", `/api/projects/${projectId}/render`, { text: source });
      if (!rendered || typeof rendered.html !== "string") {
        throw new Error("Markdown renderer returned an invalid response");
      }
      html = rendered.html;
      state.markdownCache.set(cacheKey, html);
    }
    if (typeof html !== "string") throw new Error("Markdown renderer returned an invalid response");
    if (!codeOverlayRequestMatches(path, projectId, requestId)) return;
    state.codeOverlaySource = { path, line, requestId, highlightRes, markdownHtml:html, rendered:true };
    showMarkdownCodeOverlay();
  } catch (e) {
    console.warn("Giskard markdown file render failed; keeping highlighted source.", e);
    if (codeOverlayRequestMatches(path, projectId, requestId)) {
      showCodeOverlayWarning("Markdown preview unavailable; showing source.");
      setCodeSourceToggle(false);
    }
  }
}
function showCodeOverlayWarning(message) {
  const view = $("codeView");
  const existing = view.querySelector(".code-overlay-warning");
  if (existing) existing.remove();
  const banner = document.createElement("div");
  banner.className = "code-overlay-warning";
  banner.textContent = message;
  view.prepend(banner);
}
function showMarkdownCodeOverlay() {
  const source = state.codeOverlaySource;
  if (!source || !source.rendered) return;
  const view = $("codeView");
  view.innerHTML = `<div class="code-markdown md">${source.markdownHtml}</div>`;
  wirePathLinks(view);
  wireCodeCopy(view);
  setCodeSourceToggle(true, "Source");
}
function showSourceCodeOverlay() {
  const source = state.codeOverlaySource;
  if (!source) return;
  renderCodeHtml(source.highlightRes, source.line);
  setCodeSourceToggle(true, "Rendered");
}
async function openDiffOverlay(path, diff) {
  diff = String(diff || "");
  if (!state.projectId || !diff.trim()) return;
  state.codePath = null;
  state.codeLine = null;
  state.codeOverlaySource = null;
  $("codeOverlay").classList.add("open");
  setCodeSourceToggle(false);
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
  state.codeOverlaySource = null;
  state.outputOverlay = null;
  setCodeSourceToggle(false);
  cancelOutputOverlayRefresh();
}

/* ---------- command / tool output overlay ----------
   Reuses the #codeOverlay modal (same head/close/escape/backdrop plumbing as the source and diff
   views) to show a command's or tool's full output in a large scrollable card instead of an inline
   collapsible block. While the underlying command is still running, the card live-updates: every
   render path that refreshes the inline row also calls refreshOutputOverlay, so the modal tracks the
   command's state and streaming output in place. */
// Stable core of the server's live_buffer truncation marker (see live_buffer.rs). When a client
// reconnects mid-command, the server can only replay a compacted head+tail snapshot with this
// marker spliced into the output; the full log is restored on completion. We detect the marker to
// show a clear "truncated" banner in the overlay rather than relying on the reader spotting it.
const LIVE_OUTPUT_TRUNCATED_MARKER = "command output truncated in the live reconnect snapshot";
function outputHasTruncationMarker(blocks) {
  return blocks.some((b) => String(b.text || "").includes(LIVE_OUTPUT_TRUNCATED_MARKER));
}
function makeOutputOverlayButton(itemId, kind) {
  const btn = document.createElement("button");
  btn.type = "button";
  btn.className = "output-overlay-btn";
  btn.textContent = "Open ⤢";
  btn.title = kind === "tool" ? "Open tool input/output in a full card" : "Open command output in a full card";
  btn.onclick = (e) => {
    e.stopPropagation();
    openOutputOverlay(itemId, kind);
  };
  return btn;
}
function commandLabelForId(id) {
  const cmd = state.runningCommands.get(id);
  if (cmd) return cmd.command || "";
  const ended = state.endedCommandsByItemId.get(id);
  if (ended && ended.command) return ended.command.command || "";
  const payload = state.commandPayloadsByItemId.get(id);
  return payload ? payload.command || "" : "";
}
function outputOverlayModel(itemId, kind) {
  const key = idKey(itemId);
  if (!key) return null;
  if (kind === "tool") {
    const payload = state.toolPayloadsByItemId.get(key);
    if (!payload) return null;
    const blocks = toolIoBlocks(payload);
    const stateName = toolVisualStateFromStatus(payload.status, payload.error);
    const running = stateName === "running";
    return {
      kind,
      title: taskTitleText({ kind: "tool", server: payload.server || "", command: payload.name || "tool" }),
      phase: running ? "running" : "completed",
      running,
      stateLabel: running ? "Running" : (stateName === "succeeded" ? "Completed" : stateName === "failed" ? "Failed" : "Ended"),
      blocks,
      stats: toolIoStats(blocks),
      linkify: false,
      truncated: outputHasTruncationMarker(blocks),
      error: payload.error || ""
    };
  }
  const phase = commandOutputPhaseForId(key);
  const running = phase === "running";
  const output = commandOutputForId(key);
  const cmd = state.runningCommands.get(key);
  const ended = state.endedCommandsByItemId.get(key);
  const payload = state.commandPayloadsByItemId.get(key);
  let stateLabel = running ? "Running" : "Completed";
  if (!running) {
    const status = (ended && ended.status) || (payload && payload.status) || (cmd && cmd.status) || "";
    const stateName = commandVisualStateFromStatus(status);
    stateLabel = stateName === "succeeded" ? "Completed" : stateName === "failed" ? "Failed" :
      stateName === "terminated" ? "Stopped" : "Completed";
  }
  const blocks = output ? [{ label: "Output", text: output }] : [];
  return {
    kind: "command",
    title: "$ " + (commandLabelForId(key) || "(command)"),
    phase,
    running,
    stateLabel,
    blocks,
    stats: commandOutputStats(output),
    linkify: true,
    truncated: outputHasTruncationMarker(blocks),
    error: ""
  };
}
function openOutputOverlay(itemId, kind) {
  const key = idKey(itemId);
  if (!key) return;
  // Take over the shared overlay from any source/diff view: null codePath + no requestId means the
  // async highlight/diff callbacks bail instead of overwriting our content.
  state.codePath = null;
  state.codeLine = null;
  state.codeOverlaySource = null;
  delete $("codeOverlay").dataset.requestId;
  state.outputOverlay = { itemId: key, kind };
  cancelOutputOverlayRefresh();
  $("codeOverlay").classList.add("open");
  $("codeDownload").disabled = false;
  setCodeSourceToggle(false);
  // Clear any leftover source/diff content so the first render's scroll-pin check starts from an
  // empty view (and a running command opens scrolled to its streaming tail).
  $("codeView").replaceChildren();
  renderOutputOverlay();
}
function renderOutputOverlay() {
  const ov = state.outputOverlay;
  if (!ov) return;
  const model = outputOverlayModel(ov.itemId, ov.kind);
  if (!model) {
    $("codePath").textContent = ov.kind === "tool" ? "Tool" : "Command";
    $("codeMeta").textContent = "No data available.";
    $("codeView").innerHTML = `<div class="code-empty">No output to show.</div>`;
    $("codeDownload").disabled = true;
    return;
  }
  $("codePath").textContent = model.title;
  const statLabel = commandOutputStatsLabel(model.stats, model.phase);
  const spinner = model.running ? "⟳ " : "";
  const truncSuffix = model.truncated ? " · truncated" : "";
  $("codeMeta").textContent = model.stats.chars
    ? `${spinner}${model.stateLabel} · ${statLabel}${truncSuffix}`
    : `${spinner}${model.stateLabel} · ${model.running ? "no output yet" : "no output"}`;
  $("codeDownload").disabled = !model.stats.chars;

  const view = $("codeView");
  // Preserve the reader's scroll unless they're pinned to the bottom, in which case follow the
  // streaming tail like a terminal.
  const pinned = view.scrollHeight - view.scrollTop - view.clientHeight < 32;
  const prevScroll = view.scrollTop;

  const wrap = document.createElement("div");
  wrap.className = "output-overlay";
  if (model.truncated) {
    const banner = document.createElement("div");
    banner.className = "output-overlay-banner";
    banner.textContent = model.running
      ? "⚠ Truncated reconnect snapshot — the middle of the output was dropped when this session reconnected. The full log will appear when the command finishes."
      : "⚠ Truncated — the middle of this output was dropped in a reconnect snapshot and could not be recovered.";
    wrap.append(banner);
  }
  if (!model.blocks.length) {
    const empty = document.createElement("div");
    empty.className = "code-empty";
    empty.textContent = model.running ? "Waiting for output…" : "No output.";
    wrap.append(empty);
  } else {
    for (const block of model.blocks) {
      const section = document.createElement("div");
      section.className = "output-overlay-block";
      if (model.blocks.length > 1 || block.label !== "Output") {
        const heading = document.createElement("div");
        heading.className = "meta output-overlay-heading";
        heading.textContent = block.label;
        section.append(heading);
      }
      const pre = document.createElement("pre");
      pre.className = "out";
      // Linkify only for completed commands: while streaming, re-linkifying on every delta would
      // hammer the link-resolution endpoint, so we show plain text until the command settles.
      if (model.linkify && !model.running) renderLinkedText(pre, block.text);
      else pre.textContent = block.text;
      section.append(pre);
      wrap.append(section);
    }
  }
  if (model.error) {
    const err = document.createElement("div");
    err.className = "meta output-overlay-error";
    err.textContent = "error: " + model.error;
    wrap.append(err);
  }
  view.replaceChildren(wrap);
  // Follow the tail when pinned; otherwise hold the reader's place so a delta doesn't yank them to
  // the top. New output is appended at the end, so the earlier lines keep their offset.
  view.scrollTop = pinned ? view.scrollHeight : prevScroll;
}
let outputOverlayRaf = 0;
function cancelOutputOverlayRefresh() {
  if (!outputOverlayRaf) return;
  (window.cancelAnimationFrame || clearTimeout)(outputOverlayRaf);
  outputOverlayRaf = 0;
}
function refreshOutputOverlay(itemId) {
  const ov = state.outputOverlay;
  if (!ov) return;
  if (idKey(itemId) !== ov.itemId) return;
  if (!$("codeOverlay").classList.contains("open")) return;
  // Coalesce bursts of streaming deltas into at most one repaint per frame: rebuilding the whole
  // <pre> from the full retained output on every chunk would be O(n²) over a chatty command.
  if (outputOverlayRaf) return;
  const schedule = window.requestAnimationFrame || ((cb) => setTimeout(cb, 16));
  outputOverlayRaf = schedule(() => {
    outputOverlayRaf = 0;
    if (!state.outputOverlay || !$("codeOverlay").classList.contains("open")) return;
    renderOutputOverlay();
  });
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
$("codeSourceToggle").onclick = () => {
  const source = state.codeOverlaySource;
  if (!source) return;
  source.rendered = !source.rendered;
  if (source.rendered) showMarkdownCodeOverlay();
  else showSourceCodeOverlay();
};
$("codeDownload").onclick = () => {
  if (state.outputOverlay) { downloadOutputOverlay(); return; }
  if (state.projectId && state.codePath) window.location.href = projectFileUrl("raw", state.codePath);
};
function downloadOutputOverlay() {
  const ov = state.outputOverlay;
  if (!ov) return;
  const model = outputOverlayModel(ov.itemId, ov.kind);
  if (!model || !model.blocks.length) return;
  const text = model.blocks.length === 1 && model.blocks[0].label === "Output"
    ? model.blocks[0].text
    : model.blocks.map((b) => `# ${b.label}\n${b.text}`).join("\n\n");
  const blob = new Blob([text], { type: "text/plain;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = ov.kind === "tool" ? "tool-output.txt" : "command-output.txt";
  document.body.append(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 0);
}
$("codeOverlay").addEventListener("click", (e) => { if (e.target === $("codeOverlay")) closeCodeOverlay(); });

/* ---------- composer + controls ---------- */
function sendInput() {
  const ta = $("input");
  const text = ta.value.trim();
  const attachments = state.pendingAttachments.slice();
  if (pendingAttachmentOperationCount() > 0) {
    notice("Wait for attached files to finish loading.", "warning");
    return;
  }
  // Nothing to send. The Send button is disabled in this state, so this only catches Enter, where
  // doing nothing is the right answer — an empty composer is not an error to report.
  if (!text && attachments.length === 0) return;
  // No thread surface at all. The composer is hidden until a thread or draft is open, so this is
  // unreachable in practice; it stays as a guard rather than a case worth explaining to the user.
  // The Send control carries the same condition, so reaching it by clicking is not possible either
  // — no title is set for it, since a hidden button's tooltip is not something a user can read.
  if (!state.threadId && !isDraftThread()) return;
  if (state.activeTurn) {
    notice("Wait for the current turn to finish, or stop it first.", "warning");
    return;
  }
  if (isDraftThread()) {
    startDraftThread(text, attachments);
    return;
  }
  if (!wsCanSend()) {
    notice(`Message not sent: WebSocket is ${state.wsStatus}.`, "warning");
    reconnectIfNeeded("send requested while disconnected");
    return;
  }
  const draftKey = composerDraftKey();
  const body = bubble("user pending","you");
  body.textContent = pendingUserDisplayText(text, attachments);
  const msgEl = body.parentElement;
  markAttachmentUserInput(msgEl, attachments);
  if (!send({ type:"send_input", thread_id: state.threadId, text, attachments })) {
    msgEl.classList.remove("pending");
    msgEl.classList.add("failed");
    notice(`Message not sent: WebSocket is ${state.wsStatus}.`, "error");
    return;
  }
  setTurnActive(true);
  state.pendingUserEl = msgEl;
  state.pendingUserText = text;
  clearComposerDraft(draftKey);
  clearPendingAttachments();
}
$("sendBtn").onclick = sendInput;
$("input").addEventListener("keydown", (e) => {
  if (e.key !== "Enter") return;
  // On a touch/mobile keyboard the newline key fires a plain Enter (no Shift reachable), so plain
  // Enter must insert a newline rather than send — otherwise the user can never type a newline.
  // Send with the Send button, or with Ctrl/Cmd+Enter (works for an external keyboard on a tablet).
  // On desktop, plain Enter sends and Shift+Enter inserts a newline.
  const isModifierSend = e.ctrlKey || e.metaKey;
  if (COMPOSER_IS_TOUCH) {
    if (isModifierSend) { e.preventDefault(); sendInput(); }
    return;
  }
  if (!e.shiftKey || isModifierSend) { e.preventDefault(); sendInput(); }
});
$("input").addEventListener("input", () => {
  saveComposerDraft();
  updateComposerControls();   // the Send button tracks whether there is anything to send
});
$("attachBtn").onclick = () => $("attachmentInput").click();
$("attachmentInput").addEventListener("change", attachSelectedFiles);
initComposerFileDrop();

// Whether the draft this send was issued for is no longer the one on screen: the user has since
// opened another draft (same project or not), switched to a persisted thread, or changed projects.
// The continuation must then touch nothing, or it acts on someone else's composer.
function staleDraftContinuation(draft, draftKey, pid) {
  return state.draftThread !== draft ||
    composerDraftKey() !== draftKey ||
    !isDraftThread() ||
    state.projectId !== pid;
}

async function startDraftThread(text, attachments) {
  const pid = state.projectId;
  if (!pid || !isDraftThread()) return;

  // The Send button is disabled while this is true, but Enter reaches here directly, so the refusal
  // lives here too. Nothing is drawn yet, so there is no optimistic row to unwind.
  if (draftModelUnresolved()) {
    notice(draftModelUnavailableReason(), "error");
    return;
  }

  const draftKey = composerDraftKey();
  // The draft object itself, not just its key: two successive drafts in the same project share
  // `draft:<pid>`, so the key cannot tell them apart. Without this a slow `threads/start` for the
  // first draft would come back after the user opened a second one and clear its composer, open
  // over it, or mark its rows failed — losing whatever they had typed in the meantime.
  const draft = state.draftThread;
  $("transcript").innerHTML = "";   // drop the draft placeholder before the first real row
  const body = bubble("user pending","you");
  body.textContent = pendingUserDisplayText(text, attachments);
  const msgEl = body.parentElement;
  markAttachmentUserInput(msgEl, attachments);
  state.pendingUserEl = msgEl;
  state.pendingUserText = text;
  setTurnActive(true);

  try {
    const res = await api("POST", `/api/projects/${pid}/threads/start`, {
      text,
      attachments,
      model_ref: state.currentModel,
      mode: state.mode || "build",
      permission_preset: state.permissionPreset || "ask_first"
    });
    const tid = res && res.thread_id;
    if (!tid) throw new Error("new thread response did not include thread_id");
    if (staleDraftContinuation(draft, draftKey, pid)) {
      await loadThreads(pid);
      return;
    }
    state.firstTurnStartingThreadId = String(tid);
    clearComposerDraft(draftKey);
    clearPendingAttachments();
    state.draftThread = null;
    await loadThreads(pid);
    await openThread(pid, tid, res.title || "New thread", { firstTurnStarting:true });
    state.firstTurnStartingThreadId = String(tid);
    setTurnActive(true);
    if (res.warning) notice(res.warning.message || "warning", res.warning.severity || "warning");
  } catch (e) {
    if (staleDraftContinuation(draft, draftKey, pid)) return;
    msgEl.classList.remove("pending");
    msgEl.classList.add("failed");
    state.pendingUserEl = null;
    state.pendingUserText = null;
    setTurnActive(false);
    notice("Message not sent: " + e.message, "error");
  }
}

async function attachSelectedFiles() {
  const input = $("attachmentInput");
  const files = Array.from(input.files || []);
  input.value = "";
  await attachFiles(files);
}

function attachFiles(files) {
  const batch = Array.from(files || []);
  if (!batch.length) return Promise.resolve();
  if (!composerCanAcceptAttachments()) return Promise.resolve();
  const generation = state.attachmentGeneration;
  const draftKey = composerDraftKey();
  state.pendingAttachmentOperations.set(
    generation,
    (state.pendingAttachmentOperations.get(generation) || 0) + 1
  );
  updateComposerControls();
  renderPendingAttachments();
  const operation = attachmentIngestQueue.then(() =>
    ingestAttachmentBatch(batch, generation, draftKey));
  attachmentIngestQueue = operation.catch(() => {});
  return operation.finally(() => {
    const remaining = Math.max(0,
      (state.pendingAttachmentOperations.get(generation) || 0) - 1);
    if (remaining > 0) state.pendingAttachmentOperations.set(generation, remaining);
    else state.pendingAttachmentOperations.delete(generation);
    updateComposerControls();
    renderPendingAttachments();
  });
}

function pendingAttachmentOperationCount() {
  return state.pendingAttachmentOperations.get(state.attachmentGeneration) || 0;
}

async function ingestAttachmentBatch(files, generation, draftKey) {
  if (!attachmentOperationIsCurrent(generation, draftKey)) return;
  const eligible = files.filter((file) => {
    const name = file.name || "attachment";
    if (new TextEncoder().encode(name).length > MAX_ATTACHMENT_NAME_BYTES || /[\u0000-\u001f\u007f-\u009f]/.test(name)) {
      notice(`${name} has an invalid or overlong file name.`, "error");
      return false;
    }
    if (file.size <= 0) {
      notice(`${name} is empty.`, "error");
      return false;
    }
    if (file.size > MAX_ATTACHMENT_BYTES) {
      notice(`${name} exceeds the 25 MB limit.`, "error");
      return false;
    }
    return true;
  });
  if (state.pendingAttachments.length + eligible.length > MAX_ATTACHMENTS_PER_MESSAGE) {
    notice(`Attach at most ${MAX_ATTACHMENTS_PER_MESSAGE} files per message.`, "error");
    return;
  }
  const pendingBytes = state.pendingAttachments.reduce(
    (total, attachment) => total + attachment.size, 0);
  const addedBytes = eligible.reduce((total, file) => total + file.size, 0);
  if (pendingBytes + addedBytes > MAX_TOTAL_ATTACHMENT_BYTES) {
    notice("Attachments exceed the 25 MB total limit.", "error");
    return;
  }
  for (const file of eligible) {
    try {
      const [data_base64, header] = await Promise.all([
        fileToBase64(file),
        file.slice(0, 16).arrayBuffer()
      ]);
      if (!attachmentOperationIsCurrent(generation, draftKey)) return;
      const declaredMime = normalizedAttachmentMime(file.type);
      const detectedMime = detectSupportedImageMime(new Uint8Array(header));
      const imageMime = detectedMime;
      const fileMime = declaredMime.startsWith("image/")
        ? "application/octet-stream" : declaredMime;
      state.pendingAttachments.push({
        name: file.name || "attachment",
        mime_type: imageMime || fileMime,
        size: file.size,
        kind: imageMime ? "image" : "file",
        data_base64
      });
    } catch (e) {
      if (!attachmentOperationIsCurrent(generation, draftKey)) return;
      notice(`Could not attach ${file.name || "file"}: ${e.message}`, "error");
    }
  }
  renderPendingAttachments();
}

function attachmentOperationIsCurrent(generation, draftKey) {
  return generation === state.attachmentGeneration && draftKey === composerDraftKey();
}

function composerCanAcceptAttachments() {
  const draft = isDraftThread();
  const hasThreadSurface = !!state.threadId || draft;
  return hasThreadSurface && !(state.threadReadOnly && !draft) && !(draft && state.activeTurn);
}

function initComposerFileDrop() {
  const composer = $("composer");
  let dragDepth = 0;
  const clearDrag = () => {
    dragDepth = 0;
    composer.classList.remove("drag-over");
  };

  composer.addEventListener("dragenter", (e) => {
    if (!dragEventHasFiles(e) || !composerCanAcceptAttachments()) return;
    e.preventDefault();
    dragDepth += 1;
    composer.classList.add("drag-over");
  });
  composer.addEventListener("dragover", (e) => {
    if (!dragEventHasFiles(e) || !composerCanAcceptAttachments()) return;
    e.preventDefault();
    e.dataTransfer.dropEffect = "copy";
    composer.classList.add("drag-over");
  });
  composer.addEventListener("dragleave", (e) => {
    if (!dragEventHasFiles(e)) return;
    dragDepth = Math.max(0, dragDepth - 1);
    if (dragDepth === 0) composer.classList.remove("drag-over");
  });
  composer.addEventListener("drop", async (e) => {
    if (!dragEventHasFiles(e) || !composerCanAcceptAttachments()) return;
    e.preventDefault();
    clearDrag();
    await attachFiles(Array.from(e.dataTransfer.files || []));
  });
  composer.addEventListener("dragend", clearDrag);
}

function dragEventHasFiles(e) {
  const types = e.dataTransfer && e.dataTransfer.types;
  return !!types && Array.from(types).includes("Files");
}

function fileToBase64(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    activeAttachmentReaders.add(reader);
    const finish = (callback, value) => {
      activeAttachmentReaders.delete(reader);
      callback(value);
    };
    reader.onerror = () => finish(reject, reader.error || new Error("file read failed"));
    reader.onabort = () => finish(reject, new Error("file read canceled"));
    reader.onload = () => {
      const result = typeof reader.result === "string" ? reader.result : "";
      const separator = result.indexOf(",");
      if (separator < 0) finish(reject, new Error("file encoding failed"));
      else finish(resolve, result.slice(separator + 1));
    };
    reader.readAsDataURL(file);
  });
}

function detectSupportedImageMime(bytes) {
  const prefix = (...values) => values.every((byte, index) => bytes[index] === byte);
  if (bytes.length >= 8 && prefix(0x89,0x50,0x4e,0x47,0x0d,0x0a,0x1a,0x0a)) {
    return "image/png";
  }
  if (bytes.length >= 3 && prefix(0xff,0xd8,0xff)) return "image/jpeg";
  if (bytes.length >= 6 &&
      (prefix(0x47,0x49,0x46,0x38,0x37,0x61) ||
       prefix(0x47,0x49,0x46,0x38,0x39,0x61))) {
    return "image/gif";
  }
  if (bytes.length >= 12 && prefix(0x52,0x49,0x46,0x46) &&
      bytes[8] === 0x57 && bytes[9] === 0x45 && bytes[10] === 0x42 && bytes[11] === 0x50) {
    return "image/webp";
  }
  return null;
}

function normalizedAttachmentMime(value) {
  const mime = (value || "").trim().toLowerCase();
  const token = "[a-z0-9!#$&^_.+-]+";
  return mime.length <= MAX_ATTACHMENT_MIME_BYTES && new RegExp(`^${token}/${token}$`).test(mime)
    ? mime : "application/octet-stream";
}

function renderPendingAttachments() {
  // Every attachment mutation routes through here, and attachments are half of "is there anything
  // to send", so this is where the Send button learns about them.
  updateComposerControls();
  const tray = $("attachmentTray");
  tray.innerHTML = "";
  const loading = pendingAttachmentOperationCount() > 0;
  tray.hidden = state.pendingAttachments.length === 0 && !loading;
  state.pendingAttachments.forEach((attachment, index) => {
    const chip = document.createElement("div");
    chip.className = "attachment-chip";
    const name = document.createElement("span");
    name.className = "attachment-chip-name";
    name.textContent = attachment.name;
    const size = document.createElement("span");
    size.className = "attachment-chip-size";
    size.textContent = formatAttachmentSize(attachment.size);
    const remove = document.createElement("button");
    remove.type = "button";
    remove.title = `Remove ${attachment.name}`;
    remove.setAttribute("aria-label", `Remove ${attachment.name}`);
    remove.textContent = "×";
    remove.onclick = () => {
      state.pendingAttachments.splice(index, 1);
      renderPendingAttachments();
    };
    chip.append(name, size, remove);
    tray.append(chip);
  });
  if (loading) {
    const status = document.createElement("span");
    status.className = "attachment-loading";
    status.textContent = "Loading attachments…";
    tray.append(status);
  }
}

function clearPendingAttachments() {
  state.attachmentGeneration += 1;
  for (const reader of activeAttachmentReaders) reader.abort();
  activeAttachmentReaders.clear();
  attachmentIngestQueue = Promise.resolve();
  state.pendingAttachments = [];
  renderPendingAttachments();
}

function formatAttachmentSize(size) {
  if (size >= 1024 * 1024) return `${(size / (1024 * 1024)).toFixed(1)} MB`;
  if (size >= 1024) return `${Math.ceil(size / 1024)} KB`;
  return `${size} B`;
}

function pendingUserDisplayText(text, attachments) {
  const names = (attachments || []).map(a => a.name).filter(Boolean);
  if (!names.length) return text;
  const suffix = names.length === 1 ? `[Attached: ${names[0]}]` : `[Attached: ${names.length} files]`;
  return text ? `${text}\n\n${suffix}` : suffix;
}

function markAttachmentUserInput(msg, attachments) {
  if (msg && attachments && attachments.length) msg.dataset.preserveUserInputDisplay = "true";
}

function preservesUserInputDisplay(element) {
  const msg = element && element.classList && element.classList.contains("msg")
    ? element : element && element.parentElement;
  return !!(msg && msg.dataset.preserveUserInputDisplay === "true");
}

function persistedUserInputDisplayText(userInput) {
  if (!userInput) return "";
  return pendingUserDisplayText(userInput.text || "", userInput.attachments || []);
}

function userMessageItemWithText(item, text) {
  if (item && item.payload) {
    return { ...item, payload:{ ...item.payload, text } };
  }
  return { ...item, text };
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

$("permissionPresetSel").onchange = () => {
  const previous = state.permissionPreset || "ask_first";
  const preset = $("permissionPresetSel").value || "ask_first";
  if (isDraftThread()) {
    setPermissionPreset(preset);
    return;
  }
  if (!state.threadId) {
    setPermissionPreset(previous);
    return;
  }
  if (send({ type:"set_permission_preset", thread_id: state.threadId, preset })) {
    setPermissionPreset(preset);
  } else {
    setPermissionPreset(previous);
    notice(`Permission preset not changed: WebSocket is ${state.wsStatus}.`, "error");
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
  if (!state.models.length) {
    const o=document.createElement("option");
    if (state.currentModel && state.currentModel.provider && state.currentModel.model) {
      o.value = modelKey(state.currentModel);
      o.textContent = modelOptionLabel(state.currentModel);
      o.dataset.provider = state.currentModel.provider;
      o.dataset.model = state.currentModel.model;
    } else {
      o.textContent = state.projectId ? "(loading models...)" : "(no models configured)";
    }
    sel.append(o);
  }
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
  if (!m || !m.model) {
    const draft = isDraftThread() ? state.draftThread : null;
    label.textContent = draft
      ? (draft.modelError ? "Model unavailable" : "Loading model…")
      : "Model";
    return;
  }
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
  pinDraftModel();
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
  pinDraftModel();
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
  if ($("removeThreadModal").classList.contains("open")) closeRemoveThreadModal();
  else if ($("codeOverlay").classList.contains("open")) closeCodeOverlay();
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
    $("subagentsMenu").hidden = true;
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
  el.setAttribute("role", cls ? "alert" : "status");
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
