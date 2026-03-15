/**
 * nav_dashboard/web/static/app.js
 * Nav Dashboard 前端主脚本
 *
 * 主要模块：
 *  · RAG Q&A 面板     — 问答输入、SSE/JSON 回复渲染、会话列表管理
 *  · Agent 面板       — Agent 多工具问答、会话切换/删除、Debug 开关
 *  · Dashboard 面板   — 统计卡片（RAG/Library/API用量/时延）、告警弹窗、用量调整弹窗
 *  · 快捷卡片         — 8 槽位自定义卡片，长按编辑、图片上传与裁剪（canvas 裁剪器）
 *  · Benchmark 面板   — 性能基准测试，SSE 进度流、计时器、日志Box、中止测试
 *
 * 数据流：
 *  · GET /api/dashboard/overview → refreshDashboard() → 渲染卡片 + 时延表 + 启动日志
 *  · POST /api/agent/chat → ask() → 流式/JSON 解析 → appendChatRow()
 *  · POST /api/benchmark/run (SSE) → runBenchmark() → progress/result 事件处理
 *  · PATCH /api/dashboard/usage → saveUsage() → 写入月度用量后自动刷新
 */
const qaMessages = document.getElementById("qa-messages");
const qaInput = document.getElementById("qa-input");
const qaAsk = document.getElementById("qa-ask");
const qaAskLocal = document.getElementById("qa-ask-local");
const qaAbort = document.getElementById("qa-abort");
const qaModel = document.getElementById("qa-model");
const qaDebugToggle = document.getElementById("qa-debug-toggle");
const dashboardGrid = document.getElementById("dashboard-grid");
const dashboardGeneratedAt = document.getElementById("dashboard-generated-at");
const dashboardDeployTime = document.getElementById("dashboard-deploy-time");
const dashboardRefreshBtn = document.getElementById("dashboard-refresh");
const dashboardLatencyTable = document.getElementById("dashboard-latency-table");
const dashboardObservabilityTable = document.getElementById("dashboard-observability-table");
const dashboardStartupLogs = document.getElementById("dashboard-startup-logs");
const dashboardJobsList = document.getElementById("dashboard-jobs-list");
const dashboardJobsRefreshBtn = document.getElementById("dashboard-jobs-refresh");
const dashboardJobsFilter = document.getElementById("dashboard-jobs-filter");
const dashboardTicketSummaryMeta = document.getElementById("dashboard-ticket-summary-meta");
const dashboardTicketSummaryBody = document.getElementById("dashboard-ticket-summary-body");
const dashboardTicketTrendMeta = document.getElementById("dashboard-ticket-trend-meta");
const dashboardTicketTrendChart = document.getElementById("dashboard-ticket-trend-chart");
const warningsModal = document.getElementById("warnings-modal");
const warningsModalList = document.getElementById("warnings-modal-list");
const warningsModalTimestamp = document.getElementById("warnings-modal-timestamp");
const warningsClearBtn = document.getElementById("warnings-clear-btn");
const warningsCloseBtn = document.getElementById("warnings-close-btn");
const missingQueriesModal = document.getElementById("missing-queries-modal");
const missingQueriesModalList = document.getElementById("missing-queries-modal-list");
const missingQueriesModalMeta = document.getElementById("missing-queries-modal-meta");
const missingQueriesSourceSelect = document.getElementById("missing-queries-source");
const missingQueriesExportBtn = document.getElementById("missing-queries-export-btn");
const missingQueriesClearBtn = document.getElementById("missing-queries-clear-btn");
const missingQueriesCloseBtn = document.getElementById("missing-queries-close-btn");
const feedbackModal = document.getElementById("feedback-modal");
const feedbackModalList = document.getElementById("feedback-modal-list");
const feedbackModalMeta = document.getElementById("feedback-modal-meta");
const feedbackSourceSelect = document.getElementById("feedback-source");
const feedbackExportBtn = document.getElementById("feedback-export-btn");
const feedbackClearBtn = document.getElementById("feedback-clear-btn");
const feedbackCloseBtn = document.getElementById("feedback-close-btn");
const sessionRenameModal = document.getElementById("session-rename-modal");
const sessionRenameMeta = document.getElementById("session-rename-meta");
const sessionRenameInput = document.getElementById("session-rename-input");
const sessionRenameSaveBtn = document.getElementById("session-rename-save-btn");
const sessionRenameCancelBtn = document.getElementById("session-rename-cancel-btn");
const runtimeDataModal = document.getElementById("runtime-data-modal");
const runtimeDataModalList = document.getElementById("runtime-data-modal-list");
const runtimeDataModalMeta = document.getElementById("runtime-data-modal-meta");
const runtimeDataRefreshBtn = document.getElementById("runtime-data-refresh-btn");
const runtimeDataClearBtn = document.getElementById("runtime-data-clear-btn");
const runtimeDataCloseBtn = document.getElementById("runtime-data-close-btn");
const dashboardTraceInput = document.getElementById("dashboard-trace-input");
const dashboardTraceQueryBtn = document.getElementById("dashboard-trace-query-btn");
const dashboardTraceOpenBtn = document.getElementById("dashboard-trace-open-btn");
const dashboardTraceTicketBtn = document.getElementById("dashboard-trace-ticket-btn");
const dashboardTraceMeta = document.getElementById("dashboard-trace-meta");
const dashboardTraceResult = document.getElementById("dashboard-trace-result");
const traceModal = document.getElementById("trace-modal");
const traceModalMeta = document.getElementById("trace-modal-meta");
const traceModalContent = document.getElementById("trace-modal-content");
const traceModalExport = document.getElementById("trace-modal-export");
const traceModalExportBtn = document.getElementById("trace-modal-export-btn");
const traceModalCloseBtn = document.getElementById("trace-modal-close-btn");
const appErrorModal = document.getElementById("app-error-modal");
const appErrorTitle = document.getElementById("app-error-title");
const appErrorMeta = document.getElementById("app-error-meta");
const appErrorCopybox = document.getElementById("app-error-copybox");
const appErrorCopyBtn = document.getElementById("app-error-copy-btn");
const appErrorCloseBtn = document.getElementById("app-error-close-btn");
const benchmarkCaseTraceRefreshBtn = document.getElementById("bm-case-trace-refresh");
const customCardModal = document.getElementById("custom-card-modal");
const customCardModalTitle = document.getElementById("custom-card-modal-title");
const customCardNameInput = document.getElementById("custom-card-name");
const customCardUrlInput = document.getElementById("custom-card-url");
const customCardImageInput = document.getElementById("custom-card-image");
const customCardPreview = document.getElementById("custom-card-preview");
const customCardUploadBtn = document.getElementById("custom-card-upload");
const customCardUploadInput = document.getElementById("custom-card-upload-input");
const customCardSaveBtn = document.getElementById("custom-card-save");
const customCardCancelBtn = document.getElementById("custom-card-cancel");
const ticketsMeta = document.getElementById("tickets-meta");
const ticketsDetailMeta = document.getElementById("tickets-detail-meta");
const ticketsRefreshBtn = document.getElementById("tickets-refresh-btn");
const ticketsNewBtn = document.getElementById("tickets-new-btn");
const ticketsAIDraftBtn = document.getElementById("tickets-ai-draft-btn");
const ticketsSaveBtn = document.getElementById("tickets-save-btn");
const ticketsDeleteBtn = document.getElementById("tickets-delete-btn");
const ticketsSortToggleBtn = document.getElementById("tickets-sort-toggle-btn");
const ticketsListCollapseBtn = document.getElementById("tickets-list-collapse-btn");
const ticketsSearchInput = document.getElementById("tickets-search-input");
const ticketsStatusFilter = document.getElementById("tickets-status-filter");
const ticketsPriorityFilter = document.getElementById("tickets-priority-filter");
const ticketsDomainFilter = document.getElementById("tickets-domain-filter");
const ticketsCategoryFilter = document.getElementById("tickets-category-filter");
const ticketsCreatedFrom = document.getElementById("tickets-created-from");
const ticketsCreatedTo = document.getElementById("tickets-created-to");
const ticketsMainGrid = document.getElementById("tickets-main-grid");
const ticketsListShell = document.getElementById("tickets-list-shell");
const ticketsList = document.getElementById("tickets-list");
const ticketIdInput = document.getElementById("ticket-id");
const ticketTraceIdInput = document.getElementById("ticket-trace-id");
const ticketTitleInput = document.getElementById("ticket-title");
const ticketStatusInput = document.getElementById("ticket-status");
const ticketPriorityInput = document.getElementById("ticket-priority");
const ticketDomainInput = document.getElementById("ticket-domain");
const ticketCategoryInput = document.getElementById("ticket-category");
const ticketCreatedAtInput = document.getElementById("ticket-created-at");
const ticketUpdatedAtInput = document.getElementById("ticket-updated-at");
const ticketRelatedTracesInput = document.getElementById("ticket-related-traces");
const ticketRelatedTracesLinks = document.getElementById("ticket-related-traces-links");
const ticketReproQueryInput = document.getElementById("ticket-repro-query");
const ticketSummaryInput = document.getElementById("ticket-summary");
const ticketExpectedBehaviorInput = document.getElementById("ticket-expected-behavior");
const ticketActualBehaviorInput = document.getElementById("ticket-actual-behavior");
const ticketRootCauseInput = document.getElementById("ticket-root-cause");
const ticketFixNotesInput = document.getElementById("ticket-fix-notes");
const ticketAdditionalNotesInput = document.getElementById("ticket-additional-notes");
const ticketDeleteModal = document.getElementById("ticket-delete-modal");
const ticketDeleteMeta = document.getElementById("ticket-delete-meta");
const ticketDeleteConfirmSelect = document.getElementById("ticket-delete-confirm-select");
const ticketDeleteConfirmBtn = document.getElementById("ticket-delete-confirm-btn");
const ticketDeleteCancelBtn = document.getElementById("ticket-delete-cancel-btn");
const pageLocalModel = (document.body?.dataset?.localModel || "").trim() || "qwen2.5-7b-instruct";
const pageAiSummaryUrl = (document.body?.dataset?.aiSummaryUrl || "").trim() || "http://127.0.0.1:8000/";
const pageLibraryUrl = (document.body?.dataset?.libraryUrl || "").trim() || "http://127.0.0.1:8091/";
const MAX_REFERENCE_ITEMS = 6;

let activeController = null;
let askInFlight = false;
let dashboardBootstrapped = false;
let ticketsBootstrapped = false;
let benchmarkBootstrapped = false;
let sessionsCache = [];
let currentSessionId = "";
let activeAgentStreamState = null;
let customCards = [];
let editingCardIndex = -1;
const CARD_LONG_PRESS_MS = 520;

// Warnings state
let currentWarnings = [];
let currentWarningsTimestamp = "";
let dismissedWarnings = new Set();
let currentMissingQueries = [];
let currentMissingQueriesSource = "all";
let currentFeedbackItems = [];
let currentFeedbackSource = "all";
let currentRuntimeDataItems = [];
let currentRuntimeDataSummary = {};
let currentTraceRecord = null;
let currentTraceExportText = "";
let currentRenameSessionId = "";
let suppressSessionClickUntil = 0;
let currentTickets = [];
let currentTicketId = "";
let pendingDeleteTicketId = "";
let currentTicketSort = "updated_desc";
let currentTicketStatusFilter = "non_closed";
let ticketsListCollapsed = false;
const TICKETS_LIST_COLLAPSED_STORAGE_KEY = "navDashboardTicketsListCollapsed";

// Startup polling
let lastStartupStatus = "";
let startupPollInterval = null;

// Crop state
let cropState = null;
const CROP_VSIZE = 200;

// Usage modal: last known values for prefill
let lastApiUsage = {};
let currentTaskJobs = [];
let selectedTaskJobId = "";
let taskCenterPollInterval = null;
let dashboardJobsView = "active";
let taskLogFollowFrame = 0;
let lastDashboardForceRefreshAt = 0;

if (dashboardJobsFilter?.value) {
  dashboardJobsView = String(dashboardJobsFilter.value || "active");
}

const SOURCE_LABELS = {
  all: "全部",
  agent: "LLM Agent",
  rag_qa: "RAG 问答",
  rag_qa_stream: "RAG 问答（流式）",
  agent_chat: "LLM Agent",
  rag_chat: "RAG 问答",
  unknown: "未知来源",
};

function escapeHtml(text) {
  return String(text || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

function basenameFromPath(path) {
  const raw = String(path || "").trim().replace(/\\/g, "/");
  if (!raw) return "";
  const parts = raw.split("/").filter(Boolean);
  return parts.length ? parts[parts.length - 1] : raw;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function extractApiErrorMessage(value) {
  const raw = String(value || "").trim();
  if (!raw) return "未知错误";
  try {
    const payload = JSON.parse(raw);
    if (typeof payload?.detail === "string" && payload.detail.trim()) return payload.detail.trim();
    if (typeof payload?.message === "string" && payload.message.trim()) return payload.message.trim();
  } catch (_err) {
    // keep raw
  }
  return raw;
}

function closeAppErrorModal() {
  if (!appErrorModal) return;
  appErrorModal.classList.add("hidden");
  appErrorModal.setAttribute("aria-hidden", "true");
}

async function copyAppErrorText() {
  const text = String(appErrorCopybox?.value || "").trim();
  if (!text) return;
  await navigator.clipboard.writeText(text);
}

function showAppErrorModal(title, detail, meta = "") {
  if (!appErrorModal || !appErrorCopybox) {
    window.alert(`${title}: ${detail}`);
    return;
  }
  if (appErrorTitle) appErrorTitle.textContent = String(title || "操作提示");
  if (appErrorMeta) appErrorMeta.textContent = String(meta || "");
  appErrorCopybox.value = extractApiErrorMessage(detail);
  autoSizeReadonlyTextarea(appErrorCopybox);
  appErrorModal.classList.remove("hidden");
  appErrorModal.setAttribute("aria-hidden", "false");
  appErrorCopybox.focus();
  appErrorCopybox.select();
}

function autoSizeReadonlyTextarea(node) {
  if (!node) return;
  const viewportHeight = Math.max(window.innerHeight || 0, document.documentElement?.clientHeight || 0, 720);
  const maxHeight = Math.max(180, Math.round(viewportHeight * 0.42));
  node.style.height = "0px";
  const nextHeight = Math.min(Math.max(node.scrollHeight + 2, 96), maxHeight);
  node.style.height = `${nextHeight}px`;
  node.style.overflowY = node.scrollHeight > nextHeight ? "auto" : "hidden";
}

function formatAssertionBadge(passed, label) {
  const cls = passed ? "status-completed" : "status-failed";
  const prefix = passed ? "通过" : "失败";
  return `<span class="dashboard-job-badge ${cls}">${prefix} ${escapeHtml(String(label || ""))}</span>`;
}

function summarizeAssertionScope(scope) {
  const checks = Array.isArray(scope?.checks) ? scope.checks : [];
  if (!checks.length) return `<span style='color:#5a5f50'>—</span>`;
  const passed = checks.filter((item) => item?.passed).length;
  const failed = checks.length - passed;
  return `${formatAssertionBadge(failed === 0, `${passed}/${checks.length}`)}${failed > 0 ? ` <span style="color:#d9a6a6">失败 ${failed}</span>` : ""}`;
}

function summarizeAssertionFailures(scope) {
  const checks = Array.isArray(scope?.checks) ? scope.checks : [];
  const failed = checks.filter((item) => !item?.passed);
  if (!failed.length) return `<span style="color:#8fb08d">全部通过</span>`;
  return failed.map((item) => {
    const name = escapeHtml(String(item?.name || "unknown"));
    const traceIds = Array.isArray(item?.trace_ids)
      ? item.trace_ids.map((value) => String(value || "").trim()).filter(Boolean)
      : [];
    if (!traceIds.length) return `<span class="bm-assert-fail-item">${name}</span>`;
    const preview = traceIds
      .slice(0, 2)
      .map((value) => `<span class="bm-assert-fail-trace"><code>${escapeHtml(value)}</code></span>`)
      .join("");
    const more = traceIds.length > 2 ? `<span class="bm-assert-fail-more">+${traceIds.length - 2} more</span>` : "";
    return `<span class="bm-assert-fail-item"><span>${name}</span>${preview}${more}</span>`;
  }).join(" ");
}

function syncBenchmarkLogBox(logBox, logs, { reset = false } = {}) {
  if (!logBox) return;
  const normalized = Array.isArray(logs) ? logs.map((line) => String(line || "")) : [];
  if (reset || !lastBenchmarkLogMarker) {
    logBox.textContent = normalized.length ? `${normalized.join("\n")}\n` : "";
    lastBenchmarkLogMarker = normalized.length ? normalized[normalized.length - 1] : "";
    lastBenchmarkLogCount = normalized.length;
    logBox.scrollTop = logBox.scrollHeight;
    return;
  }

  const markerIndex = normalized.lastIndexOf(lastBenchmarkLogMarker);
  if (markerIndex === -1) {
    logBox.textContent = normalized.length ? `${normalized.join("\n")}\n` : "";
  } else if (markerIndex < normalized.length - 1) {
    logBox.textContent += `${normalized.slice(markerIndex + 1).join("\n")}\n`;
  }

  lastBenchmarkLogMarker = normalized.length ? normalized[normalized.length - 1] : "";
  lastBenchmarkLogCount = normalized.length;
  logBox.scrollTop = logBox.scrollHeight;
}

function jobTypeLabel(type) {
  const labels = {
    benchmark: "Benchmark",
    rag_sync: "RAG 同步",
    library_graph_rebuild: "Library Graph",
    runtime_cleanup: "运行时清理",
  };
  return labels[String(type || "")] || String(type || "未知任务");
}

function jobStatusLabel(status) {
  const labels = {
    queued: "排队中",
    running: "运行中",
    completed: "已完成",
    failed: "失败",
    cancelled: "已取消",
  };
  return labels[String(status || "")] || String(status || "未知");
}

function renderTaskCenter() {
  if (!dashboardJobsList) return;
  if (!currentTaskJobs.length) {
    dashboardJobsList.innerHTML = '<div class="dashboard-job-empty">当前暂无后台任务</div>';
    return;
  }
  const selected = currentTaskJobs.find((job) => String(job.id) === String(selectedTaskJobId)) || currentTaskJobs[0];
  selectedTaskJobId = String(selected?.id || "");
  dashboardJobsList.innerHTML = currentTaskJobs.map((job) => {
    const selectedCls = String(job.id) === selectedTaskJobId ? " is-selected" : "";
    const status = String(job.status || "queued");
    const runningCls = status === "running" ? " is-running" : status === "failed" ? " is-failed" : status === "cancelled" ? " is-cancelled" : "";
    const summary = String(job.message || "等待开始");
    const moduleMeta = Array.isArray(job?.metadata?.modules) ? job.metadata.modules.join("+") : "";
    const createdAt = String(job.created_at || "");
    const logs = Array.isArray(job.logs) && job.logs.length
      ? job.logs.join("\n")
      : (job.error ? `ERROR: ${job.error}` : "暂无日志");
    const canCancel = ["queued", "running"].includes(status);
    const expanded = String(job.id) === selectedTaskJobId
      ? `<div class="dashboard-job-expanded">
          <div class="dashboard-meta">${escapeHtml(jobTypeLabel(job.type))} | ${escapeHtml(jobStatusLabel(status))} | 创建 ${escapeHtml(createdAt || "-")}</div>
          <pre class="dashboard-job-log-window">${escapeHtml(logs)}</pre>
          <div class="card-modal-actions dashboard-job-actions">
            <button class="ghost" data-job-cancel-id="${escapeHtml(String(job.id || ""))}"${canCancel ? "" : " disabled"}>取消任务</button>
          </div>
        </div>`
      : "";
    return `<div class="dashboard-job-card${selectedCls}${runningCls}" data-job-id="${escapeHtml(String(job.id || ""))}">
      <div class="dashboard-job-title">
        <strong>${escapeHtml(job.label || jobTypeLabel(job.type))}</strong>
        <span class="dashboard-job-badge status-${escapeHtml(status)}">${escapeHtml(jobStatusLabel(status))}</span>
      </div>
      <div class="dashboard-job-meta-line">${escapeHtml(jobTypeLabel(job.type))}${moduleMeta ? ` | ${escapeHtml(moduleMeta)}` : ""}</div>
      <div class="dashboard-job-meta-line">${escapeHtml(summary)}</div>
      <div class="dashboard-job-meta-line">${escapeHtml(createdAt)}</div>
      ${expanded}
    </div>`;
  }).join("\n");

  const selectedStatus = String(selected?.status || "");
  if (["queued", "running"].includes(selectedStatus)) scheduleTaskLogFollow();
}

function scheduleTaskLogFollow() {
  if (taskLogFollowFrame) cancelAnimationFrame(taskLogFollowFrame);
  taskLogFollowFrame = requestAnimationFrame(() => {
    taskLogFollowFrame = 0;
    const logWindow = dashboardJobsList?.querySelector(".dashboard-job-expanded .dashboard-job-log-window");
    if (!(logWindow instanceof HTMLElement)) return;
    logWindow.scrollTop = logWindow.scrollHeight;
    requestAnimationFrame(() => {
      if (logWindow.isConnected) logWindow.scrollTop = logWindow.scrollHeight;
    });
  });
}

function hasRunningLibraryGraphJob() {
  return currentTaskJobs.some((job) => {
    const type = String(job?.type || "");
    const status = String(job?.status || "");
    return type === "library_graph_rebuild" && ["queued", "running"].includes(status);
  });
}

async function refreshTaskCenter() {
  const onlyActive = dashboardJobsView !== "all";
  const payload = await apiGet(`/api/dashboard/jobs?only_active=${onlyActive ? "true" : "false"}`);
  currentTaskJobs = Array.isArray(payload?.jobs) ? payload.jobs : [];
  renderTaskCenter();

  const dashboardPanelActive = document.getElementById("panel-dashboard")?.classList.contains("active");
  const now = Date.now();
  if (dashboardPanelActive && hasRunningLibraryGraphJob() && (now - lastDashboardForceRefreshAt) >= 8000) {
    lastDashboardForceRefreshAt = now;
    refreshDashboard({ force: true, skipTaskCenter: true }).catch(() => {});
  }
}

function startTaskCenterPolling() {
  if (taskCenterPollInterval) clearInterval(taskCenterPollInterval);
  taskCenterPollInterval = setInterval(() => {
    refreshTaskCenter().catch(() => {});
  }, 4000);
}

// ─── Long-press helpers ───────────────────────────────────────────────────────

const LONG_PRESS_MS = 600;

function setLongPressSelectionLock(locked) {
  document.body?.classList.toggle("long-press-selection-lock", !!locked);
}

/**
 * Bind a long-press handler to a container element using event delegation.
 * Long-press fires `callback(target)` after LONG_PRESS_MS ms without moving/releasing.
 * @param {Element} el  - Container to listen on
 * @param {function} callback - Called with the original target Element
 */
function bindLongPress(el, callback) {
  let timer = null;
  let startX = 0, startY = 0;
  const cancel = () => {
    if (timer) {
      clearTimeout(timer);
      timer = null;
    }
    setLongPressSelectionLock(false);
  };
  el.addEventListener("pointerdown", (e) => {
    if (!(e.target instanceof Element)) return;
    startX = e.clientX; startY = e.clientY;
    const target = e.target;
    cancel();
    if (e.pointerType !== "mouse") setLongPressSelectionLock(true);
    timer = setTimeout(() => { timer = null; callback(target); }, LONG_PRESS_MS);
  });
  el.addEventListener("pointermove", (e) => {
    if (Math.abs(e.clientX - startX) > 8 || Math.abs(e.clientY - startY) > 8) cancel();
  });
  el.addEventListener("pointerup", cancel);
  el.addEventListener("pointercancel", cancel);
}

/**
 * Bind long-press to a single element. Short tap calls tapFn, long-press calls longFn.
 * @param {Element} el
 * @param {function} longFn
 * @param {function} [tapFn]
 */
function bindLongPressElement(el, longFn, tapFn) {
  let timer = null;
  let didLongPress = false;
  let startX = 0, startY = 0;
  const cancel = () => {
    if (timer) {
      clearTimeout(timer);
      timer = null;
    }
    setLongPressSelectionLock(false);
  };
  el.addEventListener("pointerdown", (e) => {
    startX = e.clientX; startY = e.clientY;
    didLongPress = false;
    cancel();
    if (e.pointerType !== "mouse") setLongPressSelectionLock(true);
    timer = setTimeout(() => { timer = null; didLongPress = true; longFn(); }, LONG_PRESS_MS);
  });
  el.addEventListener("pointermove", (e) => {
    if (Math.abs(e.clientX - startX) > 8 || Math.abs(e.clientY - startY) > 8) cancel();
  });
  el.addEventListener("pointerup", () => { cancel(); if (!didLongPress && tapFn) tapFn(); });
  el.addEventListener("pointercancel", cancel);
  // Prevent the default onclick from double-firing
  el.addEventListener("click", (e) => { if (didLongPress) e.stopImmediatePropagation(); });
}

function stripZeroWidth(text) {
  return String(text || "").replace(/[\u200B-\u200D\uFEFF]/g, "");
}

function normalizeListWhitespace(text) {
  return String(text || "")
    .replace(/[\u00A0\u1680\u2000-\u200A\u202F\u205F\u3000]/g, " ")
    .replace(/\r/g, "");
}

function normalizeMarkdown(source) {
  if (!source) return "";
  return stripZeroWidth(String(source))
    .replace(/([：:])\s+(?=>\s)/g, "$1\n")
    .replace(/([。！？!?；;：:])\s+(?=#{1,6}\s)/g, "$1\n")
    .replace(/([。！？!?；;：:])\s+(?=(?:[-*+]\s|\d+[\.)\u3001\uFF0E]\s))/g, "$1\n")
    .replace(/(^|[^\n])\s+>\s+(?=(?:["“”'‘’]|[-*+]|\d+[\.)\u3001\uFF0E]\s|#{1,6}\s))/gm, (_m, prefix) => `${prefix}\n> `);
}

function markdownToHtml(text) {
  const source = normalizeMarkdown(text || "");
  const codeBlocks = [];
  const hrBlocks = [];
  const mathBlocks = [];

  let protectedText = source.replace(/^-{3,}\s*$/gm, () => {
    const idx = hrBlocks.length;
    hrBlocks.push("<hr />");
    return `__HR_BLOCK_${idx}__`;
  });

  protectedText = protectedText.replace(/\\\[([\s\S]*?)\\\]/g, (_m, formula) => {
    const idx = mathBlocks.length;
    mathBlocks.push(`<div class=\"math-block\">\\[${escapeHtml(String(formula || "").trim())}\\]</div>`);
    return `__MATH_BLOCK_${idx}__`;
  });

  protectedText = protectedText.replace(/\\\((.*?)\\\)/g, (_m, formula) => {
    const idx = mathBlocks.length;
    mathBlocks.push(`<span class=\"math-inline\">\\(${escapeHtml(String(formula || "").trim())}\\)</span>`);
    return `__MATH_BLOCK_${idx}__`;
  });

  const withCodeTokens = protectedText.replace(/```([\s\S]*?)```/g, (_m, code) => {
    const idx = codeBlocks.length;
    codeBlocks.push(`<pre><code>${escapeHtml(String(code || "").trim())}</code></pre>`);
    return `__CODE_BLOCK_${idx}__`;
  });

  let html = escapeHtml(withCodeTokens);
  html = html.replace(/^(#{1,6})\s+(.+)$/gm, (_m, hashes, title) => `<h${hashes.length}>${title}</h${hashes.length}>`);
  html = html.replace(/^&gt;\s?(.+)$/gm, "<blockquote>$1</blockquote>");
  html = html.replace(/\*\*([^*]+)\*\*/g, "<strong>$1</strong>");
  html = html.replace(/`([^`]+)`/g, "<code>$1</code>");
  html = html.replace(/\[((?:[^\[\]]|\[[^\[\]]*\])+)]\(([^)]+)\)/g, (_m, label, url) => {
    const href = String(url || "").trim().replace(/"/g, "%22");
    const isExternal = /^https?:\/\//i.test(href);
    if (isExternal) {
      return `<a href=\"${href}\" class=\"external-link\" target=\"_blank\" rel=\"noopener noreferrer\">${label}<span class=\"ext-link-icon\" aria-hidden=\"true\">&#x2197;</span></a>`;
    }
    return `<a href=\"${href}\">${label}</a>`;
  });

  const lines = html.split("\n");
  function isTreeLikeText(value) {
    const text = String(value || "").replace(/<[^>]+>/g, " ").trim();
    if (!text) return false;
    return /[├└│┌┐┘┤┬┴─═]/.test(text) || /(?:📁|📄|folder|file)/i.test(text);
  }
  function getListMeta(line) {
    const normalizedLine = normalizeListWhitespace(stripZeroWidth(line));
    if (/^__HR_BLOCK_\d+__|^__CODE_BLOCK_\d+__|^__MATH_BLOCK_\d+__/.test(normalizedLine.trim())) return null;
    const m = normalizedLine.match(/^(\s*)([-*+•·]|\d+[\.\)\u3001\uFF0E])\s+(.+)$/);
    if (!m) return null;
    const indent = m[1].replace(/\t/g, "    ").length;
    const type = /^\d+/.test(m[2]) ? "ol" : "ul";
    return { indent, type, text: m[3] };
  }

  function collectListItems(startIdx) {
    const items = [];
    let idx = startIdx;
    while (idx < lines.length) {
      const raw = lines[idx];
      if (!raw.trim()) {
        // Allow blank lines inside list blocks when next non-empty line is still a list item.
        let lookahead = idx + 1;
        while (lookahead < lines.length && !String(lines[lookahead] || "").trim()) {
          lookahead += 1;
        }
        const nextMeta = lookahead < lines.length ? getListMeta(lines[lookahead]) : null;
        if (nextMeta) {
          idx += 1;
          continue;
        }
        break;
      }
      const meta = getListMeta(raw);
      if (!meta) break;
      items.push({ ...meta, lineIdx: idx });
      idx += 1;
    }
    return { items, nextIdx: idx };
  }

  function renderListTree(allItems, startIdx = 0, targetIndent = 0) {
    if (!allItems.length || startIdx >= allItems.length) return { html: "", nextIdx: startIdx };
    let out = "";
    let idx = startIdx;
    let currentType = null;
    let levelIsTree = false;

    for (let probe = startIdx; probe < allItems.length; probe += 1) {
      const candidate = allItems[probe];
      if (candidate.indent < targetIndent) break;
      if (candidate.indent !== targetIndent) continue;
      levelIsTree = true;
      if (!isTreeLikeText(candidate.text)) {
        levelIsTree = false;
        break;
      }
    }

    while (idx < allItems.length) {
      let item = allItems[idx];
      if (item.indent < targetIndent) break;
      if (item.indent > targetIndent) item = { ...item, indent: targetIndent };

      if (currentType !== item.type) {
        if (currentType) out += `</${currentType}>\n`;
        out += `<${item.type}${levelIsTree ? ' class="markdown-tree-list"' : ""}>\n`;
        currentType = item.type;
      }

      let content = item.text;
      idx += 1;
      if (idx < allItems.length && allItems[idx].indent > targetIndent) {
        const child = renderListTree(allItems, idx, allItems[idx].indent);
        if (child.html) content += `\n${child.html}`;
        idx = child.nextIdx;
      }
      const cls = isTreeLikeText(content) ? ' class="markdown-tree-item"' : "";
      out += `<li${cls}>${content}</li>\n`;
    }

    if (currentType) out += `</${currentType}>`;
    return { html: out, nextIdx: idx };
  }

  const out = [];
  let i = 0;
  while (i < lines.length) {
    const raw = lines[i];
    const trimmed = raw.trim();
    if (!trimmed) {
      out.push("");
      i += 1;
      continue;
    }

    const meta = getListMeta(raw);
    if (meta) {
      const block = collectListItems(i);
      const baseIndent = block.items.length ? block.items[0].indent : 0;
      out.push(renderListTree(block.items, 0, baseIndent).html);
      i = block.nextIdx;
      continue;
    }

    if (/^<h\d|^<pre|^<blockquote|^__CODE_BLOCK_\d+__|^__HR_BLOCK_\d+__|^__MATH_BLOCK_\d+__/.test(trimmed)) {
      out.push(trimmed);
    } else {
      const cls = isTreeLikeText(raw) ? ' class="markdown-tree-line"' : "";
      out.push(`<p${cls}>${raw}</p>`);
    }
    i += 1;
  }

  html = out.join("\n");
  html = html.replace(/__CODE_BLOCK_(\d+)__/g, (_m, idx) => codeBlocks[Number(idx)] || "");
  html = html.replace(/__HR_BLOCK_(\d+)__/g, (_m, idx) => hrBlocks[Number(idx)] || "");
  html = html.replace(/__MATH_BLOCK_(\d+)__/g, (_m, idx) => mathBlocks[Number(idx)] || "");
  return html;
}

function normalizeThinkText(text) {
  let value = stripZeroWidth(String(text || "")).replace(/\r\n/g, "\n");
  value = value.replace(/\[\s*Empty\s+Line\s*]/gi, "\n");
  value = value.replace(/\n{3,}/g, "\n\n");
  return value.trim();
}

function splitThinkBlocks(text) {
  const raw = String(text || "");
  const thoughts = [];
  const answer = raw.replace(/<think>([\s\S]*?)<\/think>/gi, (_m, inner) => {
    const cleaned = normalizeThinkText(inner);
    if (cleaned) thoughts.push(cleaned);
    return "\n";
  });
  return {
    answer: normalizeThinkText(answer),
    thoughts,
  };
}

function setMainTab(name) {
  for (const tab of document.querySelectorAll(".tab")) {
    const active = tab.dataset.tab === name;
    tab.classList.toggle("active", active);
    tab.setAttribute("aria-selected", String(active));
  }
  const panelHome = document.getElementById("panel-home");
  const panelAgent = document.getElementById("panel-agent");
  const panelDashboard = document.getElementById("panel-dashboard");
  const panelTickets = document.getElementById("panel-tickets");
  const panelBenchmark = document.getElementById("panel-benchmark");
  if (panelHome) panelHome.classList.toggle("active", name === "home");
  if (panelAgent) panelAgent.classList.toggle("active", name === "agent");
  if (panelDashboard) panelDashboard.classList.toggle("active", name === "dashboard");
  if (panelTickets) panelTickets.classList.toggle("active", name === "tickets");
  if (panelBenchmark) panelBenchmark.classList.toggle("active", name === "benchmark");
}

window.addEventListener("resize", () => {
  if (appErrorModal && !appErrorModal.classList.contains("hidden")) {
    autoSizeReadonlyTextarea(appErrorCopybox);
  }
});

function formatNum(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "0";
  return new Intl.NumberFormat("zh-CN").format(Math.round(n));
}

function formatSizeValue(bytes) {
  const n = Number(bytes);
  if (!Number.isFinite(n) || n <= 0) return "0 MB";
  if (n >= 1024 * 1024 * 1024) return (n / (1024 * 1024 * 1024)).toFixed(2) + " GB";
  if (n >= 1024 * 1024) return (n / (1024 * 1024)).toFixed(2) + " MB";
  if (n >= 1024) return (n / 1024).toFixed(1) + " KB";
  return `${Math.round(n)} B`;
}

function displaySourceLabel(source) {
  const key = String(source || "unknown").trim().toLowerCase();
  return SOURCE_LABELS[key] || String(source || "未知来源");
}

function buildStatCard(title, value, sub = "", role = "", state = "") {
  const extra = role ? ` data-role="${role}"` : "";
  const classes = ["stat-card", state].filter(Boolean).join(" ");
  return `
    <article class="${classes}"${extra}>
      <div class="stat-title">${escapeHtml(title)}</div>
      <div class="stat-value">${escapeHtml(value)}</div>
      <div class="stat-sub">${escapeHtml(sub)}</div>
    </article>
  `;
}

function formatDuration(seconds) {
  const n = Number(seconds);
  if (!Number.isFinite(n) || n < 0) return "—";
  if (n >= 1) return n.toFixed(2) + "s";
  if (n >= 0.001) return (n * 1000).toFixed(1) + "ms";
  if (n > 0) return Math.round(n * 1_000_000) + "µs";
  return "0s";
}

function formatRate(value) {
  const n = Number(value);
  if (!Number.isFinite(n) || n < 0) return "—";
  return (n * 100).toFixed(1) + "%";
}

function formatSigned(value, digits = 4) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "—";
  const text = n.toFixed(digits);
  return n > 0 ? `+${text}` : text;
}

function formatShortText(value, max = 120) {
  const text = String(value || "").trim().replace(/\s+/g, " ");
  if (!text) return "—";
  return text.length > max ? `${text.slice(0, max)}...` : text;
}

function computeRerankOptimization(trace) {
  const retrieval = trace?.retrieval && typeof trace.retrieval === "object" ? trace.retrieval : {};
  const ranking = trace?.ranking && typeof trace.ranking === "object" ? trace.ranking : {};
  const rankingMethod = String(ranking.method || "").trim().toLowerCase();
  const before = Number(retrieval.top1_score_before_rerank);
  const after = Number(retrieval.top1_score_after_rerank);
  const threshold = Number(retrieval.similarity_threshold);
  const identityChanged = Number(ranking.top1_identity_changed ?? retrieval.top1_identity_changed);
  const rankShift = Number(ranking.top1_rank_shift ?? retrieval.top1_rank_shift);
  const guardTriggered = Number(ranking.guard_triggered);
  const scoreComparable = rankingMethod !== "vector+keyword_fusion";
  const delta = scoreComparable && Number.isFinite(before) && Number.isFinite(after) ? after - before : null;
  const margin = Number.isFinite(after) && Number.isFinite(threshold) ? after - threshold : null;
  let direction = "无法判断";
  if (delta != null) {
    if (delta > 0.0001) direction = "正优化";
    else if (delta < -0.0001) direction = "负优化";
    else direction = "基本持平";
  } else if (!scoreComparable) {
    if (Number.isFinite(identityChanged)) {
      if (identityChanged > 0 && Number.isFinite(rankShift) && rankShift > 0) direction = "换榜上移";
      else if (identityChanged > 0 && Number.isFinite(rankShift) && rankShift < 0) direction = "换榜下移";
      else if (identityChanged > 0) direction = "换榜";
      else direction = "Top1 未变";
    } else {
      direction = "分数不可直比";
    }
  }
  return {
    direction,
    delta,
    margin,
    scoreComparable,
    identityChanged: Number.isFinite(identityChanged) ? identityChanged > 0 : null,
    rankShift: Number.isFinite(rankShift) ? rankShift : null,
    candidateCount: Number(ranking.rerank_candidate_count),
    candidateProfile: String(ranking.rerank_candidate_profile || "").trim(),
    fusionAlpha: Number(ranking.fusion_alpha),
    fusionAlphaBase: Number(ranking.fusion_alpha_base),
    dynamicAlphaEnabled: Number(ranking.dynamic_alpha_enabled),
    rerankSoftDiff: Number(ranking.rerank_soft_diff),
    rerankConfidenceFactor: Number(ranking.rerank_confidence_factor),
    baselineGap: Number(ranking.baseline_gap),
    swapBlockedByGap: Number(ranking.swap_blocked_by_gap),
    alphaReason: String(ranking.fusion_alpha_reason || "").trim(),
    guardTriggered: Number.isFinite(guardTriggered) ? guardTriggered > 0 : null,
    guardReason: String(ranking.guard_reason || "").trim(),
  };
}

function renderRerankOptimization(trace) {
  const optimization = computeRerankOptimization(trace);
  const deltaLabel = optimization.scoreComparable ? "同口径提升" : "分数直比";
  const deltaValue = optimization.scoreComparable ? formatSigned(optimization.delta, 4) : "不同口径";
  return `
    <div class="trace-kv"><span>重排序优化</span><strong>${escapeHtml(optimization.direction)}</strong></div>
    <div class="trace-kv"><span>${deltaLabel}</span><strong>${escapeHtml(deltaValue)}</strong></div>
    <div class="trace-kv"><span>是否换榜</span><strong>${optimization.identityChanged == null ? "—" : optimization.identityChanged ? "是" : "否"}</strong></div>
    <div class="trace-kv"><span>Top1 排名提升</span><strong>${escapeHtml(formatSigned(optimization.rankShift, 2))}</strong></div>
    <div class="trace-kv"><span>重排候选</span><strong>${Number.isFinite(optimization.candidateCount) ? optimization.candidateCount : "—"}${optimization.candidateProfile ? ` / ${escapeHtml(optimization.candidateProfile)}` : ""}</strong></div>
    <div class="trace-kv"><span>保护状态</span><strong>${optimization.guardTriggered == null ? "—" : optimization.guardTriggered ? "已触发" : (Number.isFinite(optimization.swapBlockedByGap) && optimization.swapBlockedByGap > 0 ? "强 Top1 拦截" : "未触发")}</strong></div>
  `;
}

function ensureMessageActions(row) {
  if (!(row instanceof HTMLElement)) return null;
  let host = row.querySelector(":scope > .msg-actions");
  if (host instanceof HTMLElement) return host;
  host = document.createElement("div");
  host.className = "msg-actions";
  row.appendChild(host);
  return host;
}

function isAssistantAnswerEligible(answer, traceId) {
  return !!String(answer || "").trim() && !!String(traceId || "").trim();
}

function addFeedbackButton(row, payload) {
  if (!(row instanceof HTMLElement) || !payload) return;
  const host = ensureMessageActions(row);
  if (!host) return;
  let button = host.querySelector("[data-action='feedback']");
  if (!(button instanceof HTMLButtonElement)) {
    button = document.createElement("button");
    button.type = "button";
    button.className = "msg-action-btn";
    button.dataset.action = "feedback";
    button.textContent = "反馈";
    host.appendChild(button);
  }
  if (button.dataset.bound === "1") return;
  button.dataset.bound = "1";
  button.addEventListener("click", async () => {
    if (button.disabled) return;
    button.disabled = true;
    try {
      await apiPost("/api/dashboard/feedback", payload);
      button.textContent = "已反馈";
      button.classList.add("is-done");
    } catch (err) {
      button.disabled = false;
      window.alert(`反馈保存失败: ${String(err)}`);
    }
  });
}

function toFiniteNumber(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function isAboveThreshold(value, threshold) {
  const n = toFiniteNumber(value);
  return n != null && n > threshold;
}

function isBelowThreshold(value, threshold) {
  const n = toFiniteNumber(value);
  return n != null && n < threshold;
}

function isOutsideRange(value, min, max) {
  const n = toFiniteNumber(value);
  if (n == null) return false;
  return n < min || n > max;
}

function isAbsAboveThreshold(value, threshold) {
  const n = toFiniteNumber(value);
  return n != null && Math.abs(n) > threshold;
}

function safeRatio(numerator, denominator) {
  const top = toFiniteNumber(numerator);
  const bottom = toFiniteNumber(denominator);
  if (top == null || bottom == null || bottom <= 0) return null;
  return top / bottom;
}

function unhealthyState(flag) {
  return flag ? "is-unhealthy" : "";
}

function buildDashboardHealthFlags({
  rag,
  library,
  apiUsage,
  latency,
  cacheStats,
  ragRerank,
  agentRerank,
  agentWallClock,
  warnings,
}) {
  const ragTop1Spread = safeRatio(
    ragRerank.avg_top1_local_doc_score_p99,
    ragRerank.avg_top1_local_doc_score,
  );
  const libraryGraphCoverage = library?.graph_quality?.item_coverage_rate ?? safeRatio(library.graph_nodes, library.total_items);
  const isolatedRate = library?.graph_quality?.isolated_node_rate;
  const libraryEdgesPerNode = library?.graph_quality?.edges_per_node ?? safeRatio(library.graph_edges, library.graph_nodes);
  const webUsageRatio = safeRatio(apiUsage.today_web_search, apiUsage.daily_web_limit);
  const deepseekUsageRatio = safeRatio(apiUsage.today_deepseek, apiUsage.daily_deepseek_limit);

  return {
    ragGraphDensity:
      isOutsideRange(rag.nodes_per_doc, 8, 20) ||
      isOutsideRange(rag.edges_per_node, 2, 5),
    libraryGraphScale:
      isBelowThreshold(libraryGraphCoverage, 0.18) ||
      isOutsideRange(libraryEdgesPerNode, 1.2, 8) ||
      isAboveThreshold(isolatedRate, 0.2),
    ragPending: isAboveThreshold(rag.changed_pending, 0),
    webUsage: isAboveThreshold(webUsageRatio, 0.9),
    deepseekUsage: isAboveThreshold(deepseekUsageRatio, 0.9),
    vectorRecall:
      isAboveThreshold(latency.stages?.total?.avg, 0.35),
    rerankLatency:
      isAboveThreshold(latency.stages?.rerank_seconds?.avg, 5),
    retrievalPercentiles:
      isAboveThreshold(latency.stages?.total?.p50, 0.25) ||
      isAboveThreshold(latency.stages?.total?.p95, 0.45) ||
      isAboveThreshold(latency.stages?.total?.p99, 0.8),
    endToEnd:
      isAboveThreshold(latency.stages?.elapsed_seconds?.p50, 25) ||
      isAboveThreshold(latency.stages?.elapsed_seconds?.p95, 45) ||
      isAboveThreshold(agentWallClock.p50, 25),
    rerankChangeRate:
      isAboveThreshold(ragRerank.top1_identity_change_rate, 0.35) ||
      isAboveThreshold(agentRerank.top1_identity_change_rate, 0.35),
    rankShift:
      isAbsAboveThreshold(ragRerank.avg_rank_shift, 2) ||
      isAbsAboveThreshold(agentRerank.avg_rank_shift, 2),
    embedCache: isBelowThreshold(cacheStats.rag_embed_cache_hit_rate, 0.7),
    noContext:
      isAboveThreshold(cacheStats.rag_no_context_rate, 0.20) ||
      isAboveThreshold(cacheStats.agent_no_context_rate, 0.30),
    top1Score:
      isBelowThreshold(ragRerank.avg_top1_local_doc_score_p99, 0.6) ||
      isAboveThreshold(ragTop1Spread, 6),
    warnings: Array.isArray(warnings) && warnings.length > 0,
  };
}

function renderDashboardLatencyTable(data) {
  if (!dashboardLatencyTable) return;
  const latency = data?.retrieval_latency || {};
  const stages = latency?.stages || {};
  if (!Object.keys(stages).length) {
    dashboardLatencyTable.innerHTML = `
      <tbody>
        <tr>
          <td colspan="5">暂无最近20次记录</td>
        </tr>
      </tbody>
    `;
    return;
  }

  // Display order and Chinese labels; elapsed_seconds pinned to end as end-to-end
  const STAGE_ORDER = [
    { key: "total",                    label: "向量召回" },
    { key: "rerank_seconds",           label: "模型重排" },
    { key: "context_assembly_seconds", label: "上下文组装" },
    { key: "web_search_seconds",       label: "网络检索" },
    { key: "elapsed_seconds",          label: "端到端总时长" },
  ];

  // Build ordered list; append any unknown keys at end (before elapsed)
  const knownKeys = new Set(STAGE_ORDER.map((s) => s.key));
  const extraKeys = Object.keys(stages).filter((k) => !knownKeys.has(k) && k !== "reranker_load_seconds");
  const orderedStages = [
    ...STAGE_ORDER.filter((s) => s.key !== "elapsed_seconds"),
    ...extraKeys.map((k) => ({ key: k, label: k })),
    { key: "elapsed_seconds", label: "端到端总时长" },
  ];

  const rows = orderedStages
    .filter(({ key }) => !!stages[key])
    .map(({ key, label }) => {
      const stat = stages[key] || {};
      const isEndToEnd = key === "elapsed_seconds";
      const trStyle = isEndToEnd ? ` class="latency-row-total"` : "";
      return `<tr${trStyle}>
        <td>${escapeHtml(label)}</td>
        <td>${formatDuration(stat.avg)}</td>
        <td>${formatDuration(stat.p50)}</td>
        <td>${formatDuration(stat.p95)}</td>
        <td>${formatDuration(stat.p99)}</td>
      </tr>`;
    })
    .join("\n");

  dashboardLatencyTable.innerHTML = `
    <thead>
      <tr>
        <th>阶段</th>
        <th>均值</th>
        <th>p50</th>
        <th>p95</th>
        <th>p99</th>
      </tr>
    </thead>
    <tbody>
      ${rows || '<tr><td colspan="5">暂无数据</td></tr>'}
    </tbody>
  `;
}

function renderDashboardObservabilityTable(data) {
  if (!dashboardObservabilityTable) return;
  const ragProfiles = data?.retrieval_by_profile || {};
  const ragModes = data?.retrieval_by_search_mode || {};
  const agentProfiles = data?.agent_by_profile || {};
  const agentModes = data?.agent_by_search_mode || {};
  const agentTypes = data?.agent_by_query_type || {};
  const rows = [];

  const pushSection = (title) => {
    rows.push(`<tr class="dashboard-observability-section"><td colspan="6"><span class="dashboard-observability-title">${escapeHtml(title)}</span></td></tr>`);
  };
  const pushRow = (name, c1, c2, c3, c4, c5) => {
    rows.push(`<tr>
      <td>${escapeHtml(name)}</td>
      <td>${c1}</td>
      <td>${c2}</td>
      <td>${c3}</td>
      <td>${c4}</td>
      <td>${c5}</td>
    </tr>`);
  };
  const orderedEntries = (bucket, orderedKeys) => {
    const seen = new Set();
    const entries = [];
    orderedKeys.forEach((key) => {
      seen.add(key);
      entries.push([key, bucket?.[key] || {}]);
    });
    Object.entries(bucket || {}).forEach(([key, value]) => {
      if (seen.has(key)) return;
      entries.push([key, value]);
    });
    return entries;
  };

  const sharedProfileNames = { short: "短查询", medium: "中查询", long: "长查询" };
  const orderedProfileKeys = ["short", "medium", "long"];
  const orderedModeKeys = ["local_only", "hybrid"];
  if (Object.keys(ragProfiles).length) {
    pushSection("RAG 分层观测");
    orderedEntries(ragProfiles, orderedProfileKeys).forEach(([key, value]) => {
      pushRow(
        `${sharedProfileNames[key] || key} (${formatNum(value?.count)})`,
        formatRate(value?.no_context_rate),
        formatRate(value?.embed_cache_hit_rate),
        formatRate(value?.query_rewrite_rate),
        formatDuration(value?.stages?.elapsed_seconds?.p50),
        formatDuration(value?.stages?.total?.p50),
      );
    });
  }

  if (Object.keys(ragModes).length) {
    pushSection("RAG 检索模式");
    orderedEntries(ragModes, orderedModeKeys).forEach(([key, value]) => {
      pushRow(
        `${key} (${formatNum(value?.count)})`,
        formatRate(value?.no_context_rate),
        formatRate(value?.embed_cache_hit_rate),
        formatRate(value?.query_rewrite_rate),
        formatDuration(value?.elapsed?.p50),
        formatDuration(value?.total?.p50),
      );
    });
  }

  if (Object.keys(agentProfiles).length) {
    pushSection("Agent 分层观测");
    orderedEntries(agentProfiles, orderedProfileKeys).forEach(([key, value]) => {
      pushRow(
        `${sharedProfileNames[key] || key} (${formatNum(value?.count)})`,
        formatRate(value?.no_context_rate),
        formatRate(value?.embed_cache_hit_rate),
        formatRate(value?.query_rewrite_rate),
        formatDuration(value?.wall_clock?.p50),
        formatDuration(value?.vector_recall?.p50),
      );
    });
  }

  if (Object.keys(agentModes).length) {
    pushSection("Agent 检索模式");
    orderedEntries(agentModes, orderedModeKeys).forEach(([key, value]) => {
      pushRow(
        `${key} (${formatNum(value?.count)})`,
        formatRate(value?.no_context_rate),
        formatRate(value?.embed_cache_hit_rate),
        formatRate(value?.query_rewrite_rate),
        formatDuration(value?.wall_clock?.p50),
        formatDuration(value?.vector_recall?.p50),
      );
    });
  }

  if (!rows.length) {
    dashboardObservabilityTable.innerHTML = `
      <tbody>
        <tr>
          <td colspan="6">暂无分层观测数据</td>
        </tr>
      </tbody>
    `;
    return;
  }

  dashboardObservabilityTable.innerHTML = `
    <thead>
      <tr>
        <th>维度</th>
        <th>检索未命中</th>
        <th>Embed 缓存命中率</th>
        <th>问题重写率</th>
        <th>端到端用时</th>
        <th>召回用时</th>
      </tr>
    </thead>
    <tbody>
      ${rows.join("\n")}
    </tbody>
  `;
}

function renderDashboardStartupLogs(data) {
  if (!dashboardStartupLogs) return;
  const startup = data?.startup || {};
  const logs = Array.isArray(startup?.logs) ? startup.logs : [];
  const status = String(startup?.status || "unknown");
  const checkedAt = String(startup?.last_checked_at || "");
  if (!logs.length) {
    dashboardStartupLogs.textContent = `status=${status}${checkedAt ? ` | checked_at=${checkedAt}` : ""}\n暂无日志`; 
    return;
  }
  const head = `status=${status}${checkedAt ? ` | checked_at=${checkedAt}` : ""}`;
  dashboardStartupLogs.textContent = `${head}\n${logs.join("\n")}`;
}

function renderDashboardError(err) {
  const text = `加载失败: ${String(err || "unknown error")}`;
  if (dashboardGrid) {
    dashboardGrid.innerHTML = `<div class="dashboard-error">${escapeHtml(text)}</div>`;
  }
  if (dashboardLatencyTable) {
    dashboardLatencyTable.innerHTML = `
      <tbody>
        <tr>
          <td>检索阶段时延</td>
          <td class="dashboard-error-cell">${escapeHtml(text)}</td>
        </tr>
      </tbody>
    `;
  }
  if (dashboardStartupLogs) {
    dashboardStartupLogs.textContent = text;
  }
  if (dashboardTicketSummaryMeta) {
    dashboardTicketSummaryMeta.textContent = text;
  }
  if (dashboardTicketSummaryBody) {
    dashboardTicketSummaryBody.innerHTML = `<div class="ticket-trend-empty">${escapeHtml(text)}</div>`;
  }
  if (dashboardTicketTrendMeta) {
    dashboardTicketTrendMeta.textContent = text;
  }
  if (dashboardTicketTrendChart) {
    dashboardTicketTrendChart.innerHTML = `<div class="ticket-trend-empty">${escapeHtml(text)}</div>`;
  }
  if (dashboardObservabilityTable) {
    dashboardObservabilityTable.innerHTML = `
      <tbody>
        <tr>
          <td>分层观测</td>
          <td class="dashboard-error-cell" colspan="5">${escapeHtml(text)}</td>
        </tr>
      </tbody>
    `;
  }
}

function loadDashboardPrefill() {
  try {
    const raw = document.getElementById("dashboard-prefill-data")?.textContent || "{}";
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object") return null;
    return parsed;
  } catch (_err) {
    return null;
  }
}

async function refreshDashboard({ force = false, skipTaskCenter = false } = {}) {
  if (dashboardGeneratedAt) dashboardGeneratedAt.textContent = "正在拉取最新状态...";
  if (dashboardRefreshBtn) dashboardRefreshBtn.disabled = true;
  try {
    const url = force ? "/api/dashboard/overview?force=true" : "/api/dashboard/overview";
    const data = await apiGet(url);
    const rag = data?.rag || {};
    const library = data?.library || {};
    const libraryGraphQuality = library?.graph_quality || {};
    const apiUsage = data?.api_usage || {};
    const agent = data?.agent || {};
    const ragQa = data?.rag_qa || {};
    const startup = data?.startup || {};
    const latency = data?.retrieval_latency || {};
    const cacheStats = data?.cache_stats || {};
    const rerankQuality = data?.rerank_quality || {};
    const ragRerank = rerankQuality.rag || {};
    const agentRerank = rerankQuality.agent || {};
    const missingQueries = data?.missing_queries_last_30d || {};
    const agentWallClock = data?.agent_wall_clock || {};
    const runtimeData = data?.runtime_data || {};
    lastStartupStatus = String(startup.status || "unknown");
    lastApiUsage = apiUsage;
    currentRuntimeDataSummary = runtimeData;
    const warnings = Array.isArray(data?.warnings) ? data.warnings : [];
    const health = buildDashboardHealthFlags({
      rag,
      library,
      apiUsage,
      latency,
      cacheStats,
      ragRerank,
      agentRerank,
      agentWallClock,
      warnings,
    });
    const cards = [
      buildStatCard("RAG 已索引文档", formatNum(rag.indexed_documents), `总文档数 ${formatNum(rag.source_markdown_files)}`),
      buildStatCard("书影音游戏总条目", formatNum(library.total_items), `今年条目 ${formatNum(library.this_year_items)}`),

      buildStatCard("RAG Graph 节点数", formatNum(rag.graph_nodes), `边数 ${formatNum(rag.graph_edges)}`),
      buildStatCard("Library Graph 节点数", formatNum(library.graph_nodes), `边数 ${formatNum(library.graph_edges)} | 覆盖 ${formatRate(libraryGraphQuality.item_coverage_rate)} | 孤点 ${formatRate(libraryGraphQuality.isolated_node_rate)}`, "library-graph-summary", unhealthyState(health.libraryGraphScale)),

      buildStatCard("RAG 平均节点数", `${rag.nodes_per_doc != null ? Number(rag.nodes_per_doc).toFixed(2) : "—"}`, `每节点平均边数 ${rag.edges_per_node != null ? Number(rag.edges_per_node).toFixed(2) : "—"}`, "", unhealthyState(health.ragGraphDensity)),
      buildStatCard("RAG 待重建文档", formatNum(rag.changed_pending), rag.changed_pending > 0 ? "等待后台同步" : "已全部同步", "rag-changed-pending", unhealthyState(health.ragPending)),

      buildStatCard("本月 Tavily API 调用", formatNum(apiUsage.month_web_search_calls), `今日 ${formatNum(apiUsage.today_web_search)} / 限额 ${formatNum(apiUsage.daily_web_limit)}`, "web-search-usage", unhealthyState(health.webUsage)),
      buildStatCard("本月 DeepSeek API 调用", formatNum(apiUsage.month_deepseek_calls), `今日 ${formatNum(apiUsage.today_deepseek)} / 限额 ${formatNum(apiUsage.daily_deepseek_limit)}`, "deepseek-usage", unhealthyState(health.deepseekUsage)),

      buildStatCard("Agent 消息总数", formatNum(agent.message_count), `会话数 ${formatNum(agent.session_count)}`),
      buildStatCard("RAG Q&A 消息总数", formatNum(ragQa.message_count), `会话数 ${formatNum(ragQa.session_count)}`),

      buildStatCard("向量召回均值", formatDuration(latency.stages?.total?.avg), `近 ${formatNum(latency.stages?.total?.count)} 次 | p50 ${formatDuration(latency.stages?.total?.p50)}`, "", unhealthyState(health.vectorRecall)),
      buildStatCard("RAG 模型重排均值", formatDuration(latency.stages?.rerank_seconds?.avg), `近 ${formatNum(latency.stages?.rerank_seconds?.count)} 次`, "", unhealthyState(health.rerankLatency)),

      buildStatCard("检索分位 p50", `${formatDuration(latency.stages?.total?.p50)}`, `p95 ${formatDuration(latency.stages?.total?.p95)} | p99 ${formatDuration(latency.stages?.total?.p99)}`, "", unhealthyState(health.retrievalPercentiles)),
      buildStatCard("RAG 全流程 p50",`${formatDuration(latency.stages?.elapsed_seconds?.p50)}`,`p95 ${formatDuration(latency.stages?.elapsed_seconds?.p95)} | Agent p50 ${formatDuration(agentWallClock.p50)}`, "", unhealthyState(health.endToEnd)),

      buildStatCard("RAG 重排序换榜率", `${formatRate(ragRerank.top1_identity_change_rate)}`, `Agent 换榜率 ${formatRate(agentRerank.top1_identity_change_rate)}`, "", unhealthyState(health.rerankChangeRate)),
      buildStatCard("RAG 平均换榜", `${formatSigned(ragRerank.avg_rank_shift, 2)}`, `Agent 平均换榜 ${formatSigned(agentRerank.avg_rank_shift, 2)}`, "", unhealthyState(health.rankShift)),
      
      buildStatCard("Embedding 缓存命中率", formatRate(cacheStats.rag_embed_cache_hit_rate), `近 ${formatNum(latency.record_count)} 次`, "", unhealthyState(health.embedCache)),
      buildStatCard("Agent 文档调用率", `${formatRate(cacheStats.agent_rag_trigger_rate)}`, `Media ${formatRate(cacheStats.agent_media_trigger_rate)} | Web ${formatRate(cacheStats.agent_web_trigger_rate)}`),

      buildStatCard("RAG Top1 均值", ragRerank.avg_top1_local_doc_score != null ? Number(ragRerank.avg_top1_local_doc_score).toFixed(4) : "—", `Agent Top1 均值 ${agentRerank.avg_top1_local_doc_score != null ? Number(agentRerank.avg_top1_local_doc_score).toFixed(4) : "—"}`),
      buildStatCard("RAG 未命中率", formatRate(cacheStats.rag_no_context_rate), `Agent 未命中率 ${formatRate(cacheStats.agent_no_context_rate)}`, "", unhealthyState(health.noContext)),
      
      buildStatCard("月检索缺失问题数", formatNum(missingQueries.count), "长按查看导出", "missing-queries-summary"),
      buildStatCard("聊天反馈数", formatNum(data?.chat_feedback?.count), "长按查看导出", "feedback-summary"),
      
      buildStatCard("运行时数据", formatSizeValue(runtimeData.total_size_bytes), `非空 ${formatNum(runtimeData.nonzero_items)} 项 | 长按查看`, "runtime-data-summary"),
    ];

    // Store warnings for modal
    currentWarnings = warnings.filter(w => !dismissedWarnings.has(w));
    currentWarningsTimestamp = String(data?.generated_at || "").trim();

    cards.push(buildStatCard("系统告警", formatNum(currentWarnings.length), currentWarnings.length > 0 ? currentWarnings.slice(0, 2).join(" | ") : "无告警", "warnings-summary", unhealthyState(health.warnings)));

    if (dashboardGrid) {
      dashboardGrid.innerHTML = cards.join("\n");
    }

    renderDashboardLatencyTable(data);
    renderDashboardObservabilityTable(data);
    renderDashboardStartupLogs(data);
    renderDashboardTicketTrend(data?.ticket_weekly_stats || {});
    if (!skipTaskCenter) refreshTaskCenter().catch(() => {});

    const generated = String(data?.generated_at || "").trim();
    const month = String(data?.month || "").trim();
    const deployed = String(data?.deployed_at || "").trim();
    if (dashboardGeneratedAt) {
      dashboardGeneratedAt.textContent = `统计月份: ${month || "-"} | 更新时间: ${generated || "-"}`;
    }
    if (dashboardDeployTime && deployed) {
      dashboardDeployTime.textContent = `部署时间: ${deployed}`;
    }
  } catch (err) {
    renderDashboardError(err);
    if (dashboardGeneratedAt) dashboardGeneratedAt.textContent = "统计加载失败";
  } finally {
    if (dashboardRefreshBtn) dashboardRefreshBtn.disabled = false;
  }
}

async function bootstrapDashboardTab() {
  const prefill = loadDashboardPrefill();
  if (prefill) {
    renderDashboardLatencyTable(prefill);
    renderDashboardObservabilityTable(prefill);
    renderDashboardStartupLogs(prefill);
  }
  await refreshDashboard({ force: true });
  await refreshTaskCenter().catch(() => {});
  startTaskCenterPolling();
  dashboardBootstrapped = true;
  scheduleStartupPollingIfNeeded();
}

function stopStartupPolling() {
  if (startupPollInterval) {
    clearInterval(startupPollInterval);
    startupPollInterval = null;
  }
}

function scheduleStartupPollingIfNeeded() {
  stopStartupPolling();
  if (lastStartupStatus === "ready" || !lastStartupStatus) return;
  startupPollInterval = setInterval(async () => {
    try {
      const data = await apiGet("/api/startup/status");
      const status = String(data?.status || "");
      if (status === "ready") {
        stopStartupPolling();
        lastStartupStatus = status;
        await refreshDashboard();
      } else if (status !== lastStartupStatus) {
        lastStartupStatus = status;
      }
    } catch (_e) {
      // ignore poll errors silently
    }
  }, 5000);
}

// ─── RAG sync trigger ─────────────────────────────────────────────────────────

async function triggerRagSync() {
  try {
    const job = await startDashboardJob("/api/dashboard/jobs/rag-sync", {}, {
      confirmText: "触发 RAG 向量重建同步？（将在后台处理所有 changed=true 的文档）",
      queuedText: "RAG 同步任务已加入后台队列...",
      onUpdate: (runningJob) => {
        if (dashboardGeneratedAt && runningJob?.message) dashboardGeneratedAt.textContent = runningJob.message;
      },
      onComplete: async () => {
        await refreshDashboard({ force: true });
      },
    });
    if (dashboardGeneratedAt && job?.message) dashboardGeneratedAt.textContent = job.message;
  } catch (err) {
    window.alert(`RAG 同步失败: ${err.message || String(err)}`);
    if (dashboardGeneratedAt) dashboardGeneratedAt.textContent = "同步触发失败";
  }
}

// ─── Warnings modal ───────────────────────────────────────────────────────────

function openWarningsModal() {
  if (!warningsModal) return;
  warningsModal.classList.remove("hidden");
  warningsModal.setAttribute("aria-hidden", "false");
  if (warningsModalTimestamp) {
    warningsModalTimestamp.textContent = currentWarningsTimestamp ? `更新时间: ${currentWarningsTimestamp}` : "";
  }
  if (warningsModalList) {
    warningsModalList.innerHTML = currentWarnings.length
      ? currentWarnings.map((w) => `<li>${escapeHtml(w)}</li>`).join("")
      : "<li>无告警</li>";
  }
}

function closeWarningsModal() {
  if (!warningsModal) return;
  warningsModal.classList.add("hidden");
  warningsModal.setAttribute("aria-hidden", "true");
}

function clearWarnings() {
  for (const w of currentWarnings) dismissedWarnings.add(w);
  currentWarnings = [];
  closeWarningsModal();
  if (dashboardGrid) {
    const card = dashboardGrid.querySelector("[data-role='warnings-summary']");
    if (card) {
      const valEl = card.querySelector(".stat-value");
      const subEl = card.querySelector(".stat-sub");
      if (valEl) valEl.textContent = "0";
      if (subEl) subEl.textContent = "已清除";
    }
  }
}

async function loadMissingQueries(source = "all") {
  currentMissingQueriesSource = String(source || "all");
  const data = await apiGet(`/api/dashboard/missing-queries?days=30&limit=200&source=${encodeURIComponent(currentMissingQueriesSource)}`);
  currentMissingQueries = Array.isArray(data?.items) ? data.items : [];
}

async function openMissingQueriesModal() {
  if (!missingQueriesModal) return;
  const source = missingQueriesSourceSelect?.value || "all";
  await loadMissingQueries(source);
  missingQueriesModal.classList.remove("hidden");
  missingQueriesModal.setAttribute("aria-hidden", "false");
  if (missingQueriesModalMeta) {
    missingQueriesModalMeta.textContent = `最近30天: ${formatNum(currentMissingQueries.length)} 条 | 来源: ${displaySourceLabel(currentMissingQueriesSource)}`;
  }
  if (missingQueriesModalList) {
    missingQueriesModalList.innerHTML = currentMissingQueries.length
      ? currentMissingQueries.map((row) => {
          const ts = escapeHtml(String(row?.ts || ""));
          const source = escapeHtml(String(row?.source_label || displaySourceLabel(row?.source || "unknown")));
          const query = escapeHtml(String(row?.query || ""));
          const top1 = row?.top1_score != null ? Number(row.top1_score).toFixed(4) : "—";
          const th = row?.threshold != null ? Number(row.threshold).toFixed(4) : "—";
          return `<li><strong>${ts}</strong> [${source}]<br/>${query}<br/><span class="dashboard-meta">top1=${top1}, threshold=${th}</span></li>`;
        }).join("")
      : "<li>最近30天暂无未命中 query</li>";
  }
}

async function loadFeedback(source = "all") {
  currentFeedbackSource = String(source || "all");
  const data = await apiGet(`/api/dashboard/feedback?limit=200&source=${encodeURIComponent(currentFeedbackSource)}`);
  currentFeedbackItems = Array.isArray(data?.items) ? data.items : [];
}

function showFeedbackDetail(item) {
  const metadata = item?.metadata && typeof item.metadata === "object" ? item.metadata : {};
  const text = [
    `时间: ${String(item?.created_at || "")}`,
    `来源: ${displaySourceLabel(item?.source || "unknown")}`,
    `trace_id: ${String(item?.trace_id || "—")}`,
    `session_id: ${String(item?.session_id || "—")}`,
    `模型: ${String(item?.model || "—")}`,
    `搜索模式: ${String(item?.search_mode || "—")}`,
    `问题类型: ${String(item?.query_type || metadata.query_type || "—")}`,
    "",
    "原始问题:",
    String(item?.question || "—"),
    "",
    "回答:",
    String(item?.answer || "—"),
  ].join("\n");
  const meta = [String(item?.created_at || ""), displaySourceLabel(item?.source || "unknown")].filter(Boolean).join(" | ");
  showAppErrorModal("聊天反馈详情", text, meta);
}

function renderDashboardTicketTrend(stats) {
  if (!dashboardTicketTrendChart) return;
  const weeks = Array.isArray(stats?.weeks) ? stats.weeks : [];
  const summary = stats?.summary && typeof stats.summary === "object" ? stats.summary : {};
  const statusCounts = stats?.status_counts && typeof stats.status_counts === "object" ? stats.status_counts : {};
  const priorityCounts = stats?.priority_counts && typeof stats.priority_counts === "object" ? stats.priority_counts : {};
  if (!weeks.length) {
    if (dashboardTicketSummaryMeta) dashboardTicketSummaryMeta.textContent = "暂无 ticket 统计";
    if (dashboardTicketSummaryBody) dashboardTicketSummaryBody.innerHTML = '<div class="ticket-trend-empty">暂无统计数据</div>';
    if (dashboardTicketTrendMeta) dashboardTicketTrendMeta.textContent = "暂无 ticket 周统计";
    dashboardTicketTrendChart.innerHTML = '<div class="ticket-trend-empty">暂无图表数据</div>';
    return;
  }

  const submitted = weeks.map((item) => Number(item?.submitted || 0));
  const closed = weeks.map((item) => Number(item?.closed || 0));
  const maxValue = Math.max(1, ...submitted, ...closed);
  const width = 620;
  const height = 180;
  const padLeft = 40;
  const padRight = 18;
  const padTop = 12;
  const padBottom = 34;
  const plotWidth = width - padLeft - padRight;
  const plotHeight = height - padTop - padBottom;
  const xFor = (index) => padLeft + (weeks.length === 1 ? plotWidth / 2 : (plotWidth * index) / (weeks.length - 1));
  const yFor = (value) => padTop + plotHeight - (plotHeight * value) / maxValue;
  const submittedPoints = weeks.map((item, index) => `${xFor(index)},${yFor(Number(item?.submitted || 0))}`).join(" ");
  const closedPoints = weeks.map((item, index) => `${xFor(index)},${yFor(Number(item?.closed || 0))}`).join(" ");
  const yTicks = Array.from(new Set([0, Math.ceil(maxValue / 2), maxValue])).sort((left, right) => left - right);
  const xLabelStep = weeks.length > 8 ? 2 : 1;

  const renderBreakdown = (counts) => Object.entries(counts)
    .sort((left, right) => Number(right[1] || 0) - Number(left[1] || 0))
    .map(([label, count]) => `<span class="dashboard-ticket-chip"><span class="dashboard-ticket-chip-label">${escapeHtml(String(label || "unknown"))}</span><strong>${formatNum(count)}</strong></span>`)
    .join("") || '<span class="ticket-empty-state">暂无数据</span>';

  if (dashboardTicketSummaryMeta) {
    dashboardTicketSummaryMeta.textContent = `当前遗留 ${formatNum(summary.current_open_total || 0)} | 近 1 周提交 ${formatNum(summary.submitted_last_week || 0)} | 近 1 周关闭 ${formatNum(summary.closed_last_week || 0)}`;
  }
  if (dashboardTicketSummaryBody) {
    dashboardTicketSummaryBody.innerHTML = `
      <div class="dashboard-ticket-summary-grid">
        <div class="dashboard-ticket-summary-card"><span>当前遗留</span><strong>${formatNum(summary.current_open_total || 0)}</strong></div>
        <div class="dashboard-ticket-summary-card"><span>Ticket 总数</span><strong>${formatNum(summary.ticket_total || 0)}</strong></div>
        <div class="dashboard-ticket-summary-card"><span>近 1 周提交</span><strong>${formatNum(summary.submitted_last_week || 0)}</strong></div>
        <div class="dashboard-ticket-summary-card"><span>近 1 月提交</span><strong>${formatNum(summary.submitted_last_month || 0)}</strong></div>
        <div class="dashboard-ticket-summary-card"><span>近 1 周关闭</span><strong>${formatNum(summary.closed_last_week || 0)}</strong></div>
        <div class="dashboard-ticket-summary-card"><span>当前最长遗留天数</span><strong>${formatNum(summary.current_longest_open_days || 0)}</strong></div>
      </div>
      <div class="dashboard-ticket-breakdown">
        <section class="dashboard-ticket-breakdown-group">
          <h4>状态分布（未关闭）</h4>
          <div class="dashboard-ticket-chip-row">${renderBreakdown(statusCounts)}</div>
        </section>
        <section class="dashboard-ticket-breakdown-group">
          <h4>优先级分布（未关闭）</h4>
          <div class="dashboard-ticket-chip-row">${renderBreakdown(priorityCounts)}</div>
        </section>
      </div>
    `;
  }

  if (dashboardTicketTrendMeta) {
    dashboardTicketTrendMeta.textContent = `近 ${weeks.length} 周 | 每周提交趋势 | 每周关闭趋势（仅 closed）`;
  }

  dashboardTicketTrendChart.innerHTML = `
    <div class="ticket-trend-legend">
      <span class="ticket-trend-legend-item"><i class="ticket-trend-legend-swatch submitted"></i>每周提交</span>
      <span class="ticket-trend-legend-item"><i class="ticket-trend-legend-swatch closed"></i>每周关闭</span>
    </div>
    <svg class="ticket-trend-svg" viewBox="0 0 ${width} ${height}" role="img" aria-label="每周 Ticket 提交和关闭趋势图">
      ${yTicks.map((tick) => `
        <line class="ticket-trend-grid" x1="${padLeft}" y1="${yFor(tick)}" x2="${width - padRight}" y2="${yFor(tick)}"></line>
        <text class="ticket-trend-label" x="${padLeft - 8}" y="${yFor(tick) + 4}" text-anchor="end">${tick}</text>
      `).join("")}
      <line class="ticket-trend-axis" x1="${padLeft}" y1="${padTop}" x2="${padLeft}" y2="${padTop + plotHeight}"></line>
      <line class="ticket-trend-axis" x1="${padLeft}" y1="${padTop + plotHeight}" x2="${width - padRight}" y2="${padTop + plotHeight}"></line>
      <polyline class="ticket-trend-line submitted" points="${submittedPoints}"></polyline>
      <polyline class="ticket-trend-line closed" points="${closedPoints}"></polyline>
      ${weeks.map((item, index) => `
        <circle class="ticket-trend-dot submitted" cx="${xFor(index)}" cy="${yFor(Number(item?.submitted || 0))}" r="3.5"></circle>
        <circle class="ticket-trend-dot closed" cx="${xFor(index)}" cy="${yFor(Number(item?.closed || 0))}" r="3.5"></circle>
        ${index % xLabelStep === 0 || index === weeks.length - 1 ? `<text class="ticket-trend-label" x="${xFor(index)}" y="${height - 10}" text-anchor="middle">${escapeHtml(String(item?.label || ""))}</text>` : ""}
      `).join("")}
    </svg>
  `;
}

async function openFeedbackModal() {
  if (!feedbackModal) return;
  const source = feedbackSourceSelect?.value || "all";
  await loadFeedback(source);
  feedbackModal.classList.remove("hidden");
  feedbackModal.setAttribute("aria-hidden", "false");
  if (feedbackModalMeta) {
    feedbackModalMeta.textContent = `共 ${formatNum(currentFeedbackItems.length)} 条 | 来源: ${displaySourceLabel(currentFeedbackSource)}`;
  }
  if (feedbackModalList) {
    feedbackModalList.innerHTML = currentFeedbackItems.length
      ? currentFeedbackItems.map((item) => `
          <li class="feedback-item" data-feedback-id="${escapeHtml(String(item.id || ""))}">
            <strong>${escapeHtml(String(item.created_at || ""))}</strong> [${escapeHtml(displaySourceLabel(item.source || "unknown"))}]<br/>
            <span class="dashboard-meta">${escapeHtml(formatShortText(item.question || "", 72))}</span>
            <div class="feedback-item-answer">${escapeHtml(formatShortText(item.answer || "", 180))}</div>
            <div class="feedback-item-hint">长按查看 trace_id 和原始问题</div>
          </li>`).join("")
      : "<li>暂无聊天反馈</li>";
  }
}

function closeFeedbackModal() {
  if (!feedbackModal) return;
  feedbackModal.classList.add("hidden");
  feedbackModal.setAttribute("aria-hidden", "true");
}

async function clearFeedback() {
  await apiPost(`/api/dashboard/feedback?source=${encodeURIComponent(currentFeedbackSource)}`, {}, "DELETE");
  currentFeedbackItems = [];
  closeFeedbackModal();
  await refreshDashboard({ force: true });
}

async function exportFeedbackJson() {
  const source = feedbackSourceSelect?.value || currentFeedbackSource || "all";
  const resp = await fetch(`/api/dashboard/feedback/export?limit=5000&source=${encodeURIComponent(source)}`);
  if (!resp.ok) throw new Error(`导出失败: HTTP ${resp.status}`);
  const text = await resp.text();
  downloadTextFile(text, `chat_feedback_${source}_${new Date().toISOString().slice(0, 19).replace(/[:T]/g, "-")}.json`);
}

function closeMissingQueriesModal() {
  if (!missingQueriesModal) return;
  missingQueriesModal.classList.add("hidden");
  missingQueriesModal.setAttribute("aria-hidden", "true");
}

async function clearMissingQueries() {
  await apiPost(`/api/dashboard/missing-queries?source=${encodeURIComponent(currentMissingQueriesSource)}`, {}, "DELETE");
  currentMissingQueries = [];
  closeMissingQueriesModal();
  await refreshDashboard({ force: true });
}

async function exportMissingQueriesCsv() {
  const source = missingQueriesSourceSelect?.value || currentMissingQueriesSource || "all";
  const resp = await fetch(`/api/dashboard/missing-queries/export?days=30&limit=5000&source=${encodeURIComponent(source)}`);
  if (!resp.ok) {
    throw new Error(`导出失败: HTTP ${resp.status}`);
  }
  const text = await resp.text();
  const normalized = String(text || "").replace(/\r?\n/g, "\r\n");
  const blob = new Blob(["\uFEFF", normalized], { type: "text/csv;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const ts = new Date().toISOString().slice(0, 19).replace(/[:T]/g, "-");
  const a = document.createElement("a");
  a.href = url;
  a.download = `missing_queries_${source}_${ts}.csv`;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

async function loadRuntimeData() {
  const data = await apiGet("/api/dashboard/runtime-data");
  currentRuntimeDataItems = Array.isArray(data?.items) ? data.items : [];
  currentRuntimeDataSummary = data || {};
}

function renderRuntimeDataModal() {
  if (runtimeDataModalMeta) {
    runtimeDataModalMeta.textContent = `总计 ${formatSizeValue(currentRuntimeDataSummary.total_size_bytes)} | 非空 ${formatNum(currentRuntimeDataSummary.nonzero_items)} 项`;
  }
  if (runtimeDataModalList) {
    runtimeDataModalList.innerHTML = currentRuntimeDataItems.length
      ? currentRuntimeDataItems.map((item) => {
          const key = escapeHtml(String(item?.key || ""));
          const label = escapeHtml(String(item?.label || "未命名项"));
          const desc = escapeHtml(String(item?.description || ""));
          const size = formatSizeValue(item?.size_bytes || 0);
          const files = formatNum(item?.file_count || 0);
          const disabled = Number(item?.size_bytes || 0) <= 0 ? " disabled" : "";
          const paths = Array.isArray(item?.paths)
            ? item.paths.map((path) => {
                const raw = String(path || "");
                const base = basenameFromPath(raw) || raw;
                return `<span class="runtime-data-path"><span class="runtime-data-path-name">${escapeHtml(base)}</span><br/><span class="runtime-data-path-full">${escapeHtml(raw)}</span></span>`;
              }).join("")
            : "";
          return `<li class="runtime-data-item">
            <label style="cursor:pointer;">
              <input type="checkbox" value="${key}"${disabled} />
              <span class="runtime-data-item-body">
                <strong>${label}</strong> <span class="dashboard-meta">${size} | 文件 ${files}</span><br/>
                <span class="dashboard-meta">${desc}</span>${paths ? `<span class="runtime-data-paths">${paths}</span>` : ""}
              </span>
            </label>
          </li>`;
        }).join("")
      : "<li>暂无可统计的运行时数据</li>";
  }
}

async function openRuntimeDataModal() {
  if (!runtimeDataModal) return;
  await loadRuntimeData();
  renderRuntimeDataModal();
  runtimeDataModal.classList.remove("hidden");
  runtimeDataModal.setAttribute("aria-hidden", "false");
}

function closeRuntimeDataModal() {
  if (!runtimeDataModal) return;
  runtimeDataModal.classList.add("hidden");
  runtimeDataModal.setAttribute("aria-hidden", "true");
}

async function pollJsonJob(url, { interval = 1200, onUpdate } = {}) {
  while (true) {
    const payload = await apiGet(url);
    const job = payload?.job || payload;
    if (onUpdate) onUpdate(job);
    const status = String(job?.status || "");
    if (["completed", "failed", "cancelled"].includes(status)) return job;
    await sleep(interval);
  }
}

async function startDashboardJob(endpoint, payload, { confirmText, queuedText, onUpdate, onComplete } = {}) {
  if (confirmText && !window.confirm(confirmText)) return null;
  const response = await apiPost(endpoint, payload || {});
  const jobId = String(response?.job?.id || "").trim();
  if (!jobId) throw new Error("未返回任务 ID");
  if (dashboardGeneratedAt && queuedText) dashboardGeneratedAt.textContent = queuedText;
  const job = await pollJsonJob(`/api/dashboard/jobs/${encodeURIComponent(jobId)}`, { onUpdate });
  if (job?.status === "completed") {
    if (onComplete) await onComplete(job);
    return job;
  }
  throw new Error(job?.error || job?.message || "后台任务失败");
}

async function clearRuntimeDataSelection() {
  const checked = Array.from(runtimeDataModalList?.querySelectorAll("input[type='checkbox']:checked") || [])
    .map((node) => String(node.value || "").trim())
    .filter(Boolean);
  if (!checked.length) {
    window.alert("请先勾选要清除的项目");
    return;
  }
  if (!window.confirm(`确定清除已勾选的 ${checked.length} 项运行时数据？`)) return;
  if (runtimeDataClearBtn) runtimeDataClearBtn.disabled = true;
  try {
    const job = await startDashboardJob("/api/dashboard/jobs/runtime-data-cleanup", { keys: checked }, {
      queuedText: `已提交运行时数据清理任务（${checked.length} 项）`,
      onComplete: async (doneJob) => {
        const result = doneJob?.result || {};
        if (Array.isArray(result?.failed) && result.failed.length) {
          const details = result.failed.map((item) => `${item.key}: ${item.error}`).join("\n");
          window.alert(`部分清除失败:\n${details}`);
        }
        await loadRuntimeData();
        renderRuntimeDataModal();
        await refreshDashboard({ force: true });
      },
    });
    if (dashboardGeneratedAt && job?.message) dashboardGeneratedAt.textContent = job.message;
  } finally {
    if (runtimeDataClearBtn) runtimeDataClearBtn.disabled = false;
  }
}

async function triggerLibraryGraphRebuild() {
  try {
    const job = await startDashboardJob("/api/dashboard/jobs/library-graph-rebuild", {}, {
      confirmText: "触发 Library Graph 缺失项补足？这会为当前缺失条目补建图节点与边。",
      queuedText: "Library Graph 补缺任务已加入后台队列...",
      onUpdate: (runningJob) => {
        if (dashboardGeneratedAt && runningJob?.message) dashboardGeneratedAt.textContent = runningJob.message;
      },
      onComplete: async () => {
        await refreshDashboard({ force: true });
      },
    });
    if (dashboardGeneratedAt && job?.message) dashboardGeneratedAt.textContent = job.message;
  } catch (err) {
    window.alert(`Library Graph 补缺失败: ${err.message || String(err)}`);
    if (dashboardGeneratedAt) dashboardGeneratedAt.textContent = "Library Graph 补缺触发失败";
  }
}

// ─── Usage edit modal ─────────────────────────────────────────────────────────

function openUsageModal() {
  const modal = document.getElementById("usage-edit-modal");
  if (!modal) return;
  const webInput = document.getElementById("usage-web-input");
  const dsInput = document.getElementById("usage-deepseek-input");
  if (webInput) webInput.value = String(lastApiUsage.month_web_search_calls ?? "");
  if (dsInput) dsInput.value = String(lastApiUsage.month_deepseek_calls ?? "");
  modal.classList.remove("hidden");
  modal.setAttribute("aria-hidden", "false");
  webInput?.focus();
}

function closeUsageModal() {
  const modal = document.getElementById("usage-edit-modal");
  if (!modal) return;
  modal.classList.add("hidden");
  modal.setAttribute("aria-hidden", "true");
}

async function saveUsage() {
  const webVal = parseInt(document.getElementById("usage-web-input")?.value ?? "", 10);
  const dsVal = parseInt(document.getElementById("usage-deepseek-input")?.value ?? "", 10);
  if (isNaN(webVal) || isNaN(dsVal) || webVal < 0 || dsVal < 0) {
    window.alert("请输入有效的非负整数");
    return;
  }
  await apiPost("/api/dashboard/usage", { month_web_search_calls: webVal, month_deepseek_calls: dsVal }, "PATCH");
  closeUsageModal();
  refreshDashboard().catch((err) => renderDashboardError(err));
}

// ─── Image crop UI ────────────────────────────────────────────────────────────

function drawCropCanvas() {
  if (!cropState) return;
  const canvas = document.getElementById("crop-canvas");
  if (!canvas) return;
  canvas.width = CROP_VSIZE;
  canvas.height = CROP_VSIZE;
  const ctx = canvas.getContext("2d");
  ctx.clearRect(0, 0, CROP_VSIZE, CROP_VSIZE);
  ctx.drawImage(
    cropState.img,
    cropState.ox,
    cropState.oy,
    cropState.img.naturalWidth * cropState.scale,
    cropState.img.naturalHeight * cropState.scale
  );
}

function showCropWrap(show) {
  const wrap = document.getElementById("crop-wrap");
  if (wrap) wrap.classList.toggle("hidden", !show);
  // Hide the flat rectangular preview when the crop canvas is active
  if (customCardPreview) customCardPreview.classList.toggle("hidden", !!show);
}

function initCropper(imageUrl) {
  const clean = String(imageUrl || "").trim();
  if (!clean) { showCropWrap(false); return; }
  const img = new Image();
  img.crossOrigin = "anonymous";
  img.onload = () => {
    const s = Math.max(CROP_VSIZE / img.naturalWidth, CROP_VSIZE / img.naturalHeight);
    cropState = {
      img,
      scale: s,
      ox: (CROP_VSIZE - img.naturalWidth * s) / 2,
      oy: (CROP_VSIZE - img.naturalHeight * s) / 2,
      dragging: false,
      lastX: 0,
      lastY: 0,
    };
    drawCropCanvas();
    showCropWrap(true);
  };
  img.onerror = () => showCropWrap(false);
  img.src = clean;
}

function cropZoom(factor) {
  if (!cropState) return;
  const cx = CROP_VSIZE / 2;
  const cy = CROP_VSIZE / 2;
  const newScale = Math.max(0.05, Math.min(20, cropState.scale * factor));
  const ratio = newScale / cropState.scale;
  cropState.ox = cx - (cx - cropState.ox) * ratio;
  cropState.oy = cy - (cy - cropState.oy) * ratio;
  cropState.scale = newScale;
  drawCropCanvas();
}

async function applyCrop() {
  if (!cropState) return;
  const srcCanvas = document.getElementById("crop-canvas");
  if (!srcCanvas) return;
  const blob = await new Promise((res) => srcCanvas.toBlob(res, "image/jpeg", 0.92));
  const file = new File([blob], "crop.jpg", { type: "image/jpeg" });
  const payload = await apiUploadImage("/api/custom_cards/upload", file);
  const image = String(payload?.image || "").trim();
  if (!image) throw new Error("裁剪图片上传失败");
  if (customCardImageInput) customCardImageInput.value = image;
  setCardPreview(image);
  showCropWrap(false);
}

function bindCropCanvasEvents() {
  const canvas = document.getElementById("crop-canvas");
  if (!canvas) return;
  canvas.addEventListener("pointerdown", (e) => {
    if (!cropState) return;
    canvas.setPointerCapture(e.pointerId);
    cropState.dragging = true;
    cropState.lastX = e.clientX;
    cropState.lastY = e.clientY;
  });
  canvas.addEventListener("pointermove", (e) => {
    if (!cropState || !cropState.dragging) return;
    cropState.ox += e.clientX - cropState.lastX;
    cropState.oy += e.clientY - cropState.lastY;
    cropState.lastX = e.clientX;
    cropState.lastY = e.clientY;
    drawCropCanvas();
  });
  canvas.addEventListener("pointerup", () => {
    if (cropState) cropState.dragging = false;
  });
  canvas.addEventListener("pointercancel", () => {
    if (cropState) cropState.dragging = false;
  });
  canvas.addEventListener("wheel", (e) => {
    e.preventDefault();
    cropZoom(e.deltaY < 0 ? 1.12 : 1 / 1.12);
  }, { passive: false });
  document.getElementById("crop-zoom-in")?.addEventListener("click", () => cropZoom(1.25));
  document.getElementById("crop-zoom-out")?.addEventListener("click", () => cropZoom(1 / 1.25));
  document.getElementById("crop-apply-btn")?.addEventListener("click", () => {
    applyCrop().catch((e) => window.alert(`裁剪失败: ${String(e)}`));
  });
}

// ─── Benchmark tab ────────────────────────────────────────────────────────────

let benchmarkEventSource = null;
let benchmarkHistory = [];
let benchmarkAbortController = null;
let benchmarkTimerInterval = null;
let activeBenchmarkJobId = "";
let lastBenchmarkLogCount = 0;
let lastBenchmarkLogMarker = "";
const BENCHMARK_HISTORY_COLUMNS = 5;
let benchmarkCaseSets = [];
const TRACE_STAGE_COLORS = ["#7fc97f", "#beaed4", "#fdc086", "#ffff99", "#386cb0", "#f0027f", "#bf5b17", "#666666"];
const TRACE_STAGE_ORDER = [
  "planning_seconds",
  "vector_recall_seconds",
  "rerank_seconds",
  "context_assembly_seconds",
  "web_search_seconds",
  "tool_execution_seconds",
  "llm_seconds",
];
const TRACE_STAGE_LABELS = {
  planning_seconds: "planning",
  vector_recall_seconds: "vector_recall",
  rerank_seconds: "rerank",
  context_assembly_seconds: "context_assembly",
  web_search_seconds: "web_search",
  tool_execution_seconds: "tool_execution",
  llm_seconds: "llm",
};
let benchmarkLiveLatestCase = null;
let benchmarkLiveLatestCaseKey = "";
let benchmarkLatestTraceRenderSeq = 0;
let benchmarkLatestTraceCacheKey = "";

// Current test-set selection: "<module>/<length>"
let currentBmTestSet = "rag/short";

function formatTraceNumber(value, digits = 4) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "—";
  return n.toFixed(digits);
}

function formatTracePercent(value) {
  const n = Number(value);
  if (!Number.isFinite(n) || n < 0) return "—";
  return (n * 100).toFixed(1) + "%";
}

function downloadTextFile(text, filename) {
  const blob = new Blob([String(text || "")], { type: "text/plain;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

function formatTraceStageLabel(key) {
  const normalized = String(key || "").trim();
  if (!normalized) return "";
  const mapped = TRACE_STAGE_LABELS[normalized] || normalized;
  return mapped.replace(/_seconds$/i, "").replace(/_/g, " ");
}

function buildTraceStageEntries(trace) {
  const stages = trace?.stages && typeof trace.stages === "object" ? trace.stages : {};
  const orderedKeys = [
    ...TRACE_STAGE_ORDER.filter((key) => Object.prototype.hasOwnProperty.call(stages, key)),
    ...Object.keys(stages).filter((key) => key !== "wall_clock_seconds" && !TRACE_STAGE_ORDER.includes(key)),
  ];
  const entries = orderedKeys
    .filter((key) => key !== "wall_clock_seconds")
    .map((key, index) => ({
      key,
      label: formatTraceStageLabel(key),
      value: Number(stages[key]),
      color: TRACE_STAGE_COLORS[index % TRACE_STAGE_COLORS.length],
    }))
    .filter((entry) => Number.isFinite(entry.value) && entry.value > 0);
  const stageTotal = entries.reduce((sum, entry) => sum + entry.value, 0);
  return entries.map((entry) => ({
    ...entry,
    ratio: stageTotal > 0 ? Math.max(0, Math.min(1, entry.value / stageTotal)) : 0,
  }));
}

function renderTraceStageBars(trace) {
  const entries = buildTraceStageEntries(trace);
  if (!entries.length) return '<div class="trace-result-empty">暂无阶段时延</div>';
  const totalSeconds = Number(trace?.total_elapsed_seconds || trace?.stages?.wall_clock_seconds || entries.reduce((sum, entry) => sum + entry.value, 0));
  return `
    <div class="trace-stage-composite-wrap">
      <div class="trace-stage-composite" aria-label="阶段时长占比总览">${entries.map((entry) => {
        const widthPct = Math.max(entry.ratio * 100, entry.ratio > 0 ? 1.5 : 0);
        const label = `${entry.label}: ${formatDuration(entry.value)} / ${formatTracePercent(entry.ratio)}`;
        return `<button type="button" class="trace-stage-segment" data-trace-stage-key="${escapeHtml(String(entry.key))}" title="${escapeHtml(label)}" style="width:${widthPct.toFixed(2)}%;background:${entry.color}"></button>`;
      }).join("")}</div>
      <div class="trace-stage-table-wrap">
        <table class="trace-stage-table">
          <thead><tr><th>阶段</th><th>用时</th><th>占比</th></tr></thead>
          <tbody>${entries.map((entry) => `<tr data-trace-stage-row="${escapeHtml(String(entry.key))}"><td><span class="trace-stage-label"><i style="background:${entry.color}"></i>${escapeHtml(String(entry.label))}</span></td><td>${escapeHtml(formatDuration(entry.value))}</td><td>${escapeHtml(formatTracePercent(entry.ratio))}</td></tr>`).join("")}</tbody>
          <tfoot><tr><td>total</td><td>${escapeHtml(formatDuration(totalSeconds))}</td><td>100.0%</td></tr></tfoot>
        </table>
      </div>
    </div>`;
}

function renderTraceSummary(trace) {
  if (!trace || typeof trace !== "object") return '<div class="trace-result-empty">未找到 trace 数据</div>';
  const profile = trace.query_profile && typeof trace.query_profile === "object" ? trace.query_profile : {};
  const router = trace.router && typeof trace.router === "object" ? trace.router : {};
  const retrieval = trace.retrieval && typeof trace.retrieval === "object" ? trace.retrieval : {};
  const ranking = trace.ranking && typeof trace.ranking === "object" ? trace.ranking : {};
  const llm = trace.llm && typeof trace.llm === "object" ? trace.llm : {};
  const result = trace.result && typeof trace.result === "object" ? trace.result : {};
  const tools = Array.isArray(trace.tools) ? trace.tools : [];
  const plannedTools = Array.isArray(router.planned_tools) ? router.planned_tools : [];
  const rerankOptimization = computeRerankOptimization(trace);
  const sessionId = String(trace.session_id || "").trim();
  const totalElapsed = trace.total_elapsed_seconds || trace?.stages?.wall_clock_seconds || 0;
  const usedContextDocs = Number(result.used_context_docs ?? 0);
  return `
    <div class="trace-summary-grid">
      <section class="trace-summary-card">
        <div class="bm-card-section-label">基本信息</div>
        <div class="trace-kv"><span>trace_id</span><strong>${escapeHtml(String(trace.trace_id || ""))}</strong></div>
        <div class="trace-kv"><span>时间</span><strong>${escapeHtml(String(trace.timestamp || ""))}</strong></div>
        <div class="trace-kv"><span>入口</span><strong>${escapeHtml(String(trace.entrypoint || ""))}</strong></div>
        <div class="trace-kv"><span>调用类型</span><strong>${escapeHtml(String(trace.call_type || ""))}</strong></div>
        <div class="trace-kv"><span>检索模式</span><strong>${escapeHtml(String(trace.search_mode || ""))}</strong></div>
        <div class="trace-kv"><span>问题长度判定</span><strong>${escapeHtml(String(profile.profile || ""))} / ${escapeHtml(String(profile.token_count || 0))} tokens</strong></div>
        <div class="trace-kv"><span>问题类型判定</span><strong>${escapeHtml(String(trace.query_type || "—"))}</strong></div>
        <div class="trace-kv"><span>Session</span><strong>${escapeHtml(sessionId || "—")}</strong></div>
        <div class="trace-kv"><span>总耗时</span><strong>${escapeHtml(formatDuration(totalElapsed))}</strong></div>
      </section>
      <section class="trace-summary-card">
        <div class="bm-card-section-label">路由与工具</div>
        <div class="trace-kv"><span>调用工具</span><strong>${escapeHtml(String(router.selected_tool || "—"))}</strong></div>
        <div class="trace-kv"><span>分类器结果</span><strong>${escapeHtml(String(router.classifier_label || "—"))}</strong></div>
        <div class="trace-kv"><span>文档相似度</span><strong>${escapeHtml(formatTraceNumber(router.doc_similarity))}</strong></div>
        <div class="trace-kv"><span>媒体工具调用</span><strong>${router.media_entity_confident ? "true" : "false"}</strong></div>
        <div class="trace-kv"><span>计划工具数</span><strong>${escapeHtml(String(plannedTools.length || 0))}</strong></div>
        <div class="trace-kv"><span>实际工具数</span><strong>${escapeHtml(String(tools.length || 0))}</strong></div>
        <div class="trace-chip-list">${plannedTools.length ? plannedTools.map((tool) => `<span class="trace-chip">${escapeHtml(String(tool))}</span>`).join("") : '<span class="trace-chip">none</span>'}</div>
        <div class="trace-tool-list">${tools.length ? tools.map((tool) => `<div class="trace-tool-row"><strong>${escapeHtml(String(tool?.name || ""))}</strong><span>${escapeHtml(String(tool?.status || ""))}</span><span>${escapeHtml(formatDuration((Number(tool?.latency_ms || 0) || 0) / 1000))}</span></div>`).join("") : '<div class="trace-result-empty">暂无工具记录</div>'}</div>
      </section>
      <section class="trace-summary-card">
        <div class="bm-card-section-label">检索与排序</div>
        <div class="trace-kv"><span>向量命中</span><strong>${escapeHtml(String(retrieval.vector_hits ?? "—"))}</strong></div>
        <div class="trace-kv"><span>阈值</span><strong>${escapeHtml(formatTraceNumber(retrieval.similarity_threshold))}</strong></div>
        <div class="trace-kv"><span>问题重写</span><strong>${escapeHtml(String(retrieval.query_rewrite_status || "—"))} / ${escapeHtml(String(retrieval.query_rewrite_count ?? 0))}</strong></div>
        <div class="trace-kv"><span>重排序</span><strong>${escapeHtml(String(ranking.method || "—"))} / k=${escapeHtml(String(ranking.rerank_k ?? "—"))}</strong></div>
        ${renderRerankOptimization(trace)}
        <div class="trace-kv"><span>阈值余量</span><strong>${escapeHtml(formatSigned(rerankOptimization.margin, 4))}</strong></div>
      </section>
      <section class="trace-summary-card">
        <div class="bm-card-section-label">LLM 与结果</div>
        <div class="trace-kv"><span>调用类型</span><strong>${escapeHtml(String(llm.backend || "—"))}</strong></div>
        <div class="trace-kv"><span>使用模型</span><strong>${escapeHtml(String(llm.model || "—"))}</strong></div>
        <div class="trace-kv"><span>LLM 用时</span><strong>${escapeHtml(formatDuration(llm.latency_seconds))}</strong></div>
        <div class="trace-kv"><span>输入 Tokens</span><strong>${escapeHtml(String(llm.input_tokens_est ?? 0))}</strong></div>
        <div class="trace-kv"><span>Prompt Tokens</span><strong>${escapeHtml(String(llm.prompt_tokens_est ?? 0))}</strong></div>
        <div class="trace-kv"><span>Context Tokens</span><strong>${escapeHtml(String(llm.context_tokens_est ?? 0))}</strong></div>
        <div class="trace-kv"><span>输出 Tokens</span><strong>${escapeHtml(String(llm.output_tokens_est ?? 0))}</strong></div>
        <div class="trace-kv"><span>LLM 调用次数</span><strong>${escapeHtml(String(llm.calls ?? 0))}</strong></div>
        <div class="trace-kv"><span>上下文文档数</span><strong>${escapeHtml(String(usedContextDocs || 0))}</strong></div>
        <div class="trace-kv"><span>无效回答</span><strong>${result.no_context ? `true / ${escapeHtml(String(result.no_context_reason || ""))}` : "false"}</strong></div>
      </section>
    </div>
    <section class="trace-summary-card trace-stage-panel">
      <div class="bm-card-section-label">阶段用时</div>
      ${renderTraceStageBars(trace)}
    </section>
  `;
}

function renderDashboardTrace(trace, exportText) {
  currentTraceRecord = trace;
  currentTraceExportText = String(exportText || "");
  if (dashboardTraceMeta) {
    dashboardTraceMeta.textContent = trace
      ? `${trace.entrypoint || "trace"} | ${trace.call_type || ""} | 总耗时 ${formatDuration(trace.total_elapsed_seconds || trace?.stages?.wall_clock_seconds || 0)}`
      : "输入 trace_id 查看阶段用时、路由和工具调用摘要";
  }
  if (dashboardTraceOpenBtn) dashboardTraceOpenBtn.disabled = !trace;
  if (dashboardTraceTicketBtn) dashboardTraceTicketBtn.disabled = !trace;
  if (dashboardTraceResult) dashboardTraceResult.innerHTML = trace ? renderTraceSummary(trace) : '<div class="trace-result-empty">暂无 trace 数据</div>';
}

async function fetchTrace(traceId) {
  const payload = await apiGet(`/api/dashboard/trace?trace_id=${encodeURIComponent(traceId)}`);
  return {
    trace: payload?.trace || null,
    exportText: String(payload?.export_text || ""),
  };
}

async function lookupDashboardTrace(traceId) {
  const value = String(traceId || dashboardTraceInput?.value || "").trim();
  if (!value) {
    showAppErrorModal("Trace 查询失败", "请输入 trace_id", "可以直接粘贴 trace_id 后回车或点击查询");
    return;
  }
  if (dashboardTraceMeta) dashboardTraceMeta.textContent = `正在查询 ${value} ...`;
  const { trace, exportText } = await fetchTrace(value);
  if (dashboardTraceInput) dashboardTraceInput.value = value;
  renderDashboardTrace(trace, exportText);
}

function closeTraceModal() {
  traceModal?.classList.add("hidden");
  traceModal?.setAttribute("aria-hidden", "true");
}

async function openTraceModal(traceId) {
  const value = String(traceId || currentTraceRecord?.trace_id || "").trim();
  if (!value) {
    showAppErrorModal("Trace 打开失败", "缺少 trace_id");
    return;
  }
  const { trace, exportText } = await fetchTrace(value);
  currentTraceRecord = trace;
  currentTraceExportText = exportText;
  if (traceModalMeta) {
    traceModalMeta.textContent = `${trace?.trace_id || value} | ${trace?.entrypoint || ""} | ${trace?.call_type || ""}`;
  }
  if (traceModalContent) traceModalContent.innerHTML = "";
  if (traceModalExport) traceModalExport.textContent = exportText;
  traceModal?.classList.remove("hidden");
  traceModal?.setAttribute("aria-hidden", "false");
}

async function exportCurrentTrace(traceId) {
  const value = String(traceId || currentTraceRecord?.trace_id || dashboardTraceInput?.value || "").trim();
  if (!value) {
    showAppErrorModal("Trace 导出失败", "缺少 trace_id");
    return;
  }
  if (currentTraceRecord && currentTraceRecord.trace_id === value && currentTraceExportText) {
    downloadTextFile(currentTraceExportText, `${value}.txt`);
    return;
  }
  const response = await fetch(`/api/dashboard/trace/export?trace_id=${encodeURIComponent(value)}`);
  if (!response.ok) throw new Error(await response.text());
  const text = await response.text();
  downloadTextFile(text, `${value}.txt`);
}

function parseTicketTraceList(value) {
  if (Array.isArray(value)) {
    return value.map((item) => String(item || "").trim()).filter(Boolean);
  }
  return String(value || "")
    .replace(/\r/g, "\n")
    .replace(/,/g, "\n")
    .split("\n")
    .map((item) => item.trim())
    .filter(Boolean)
    .filter((item, index, arr) => arr.indexOf(item) === index);
}

function ensureSelectValue(select, value, { allowAll = false } = {}) {
  if (!select) return;
  const targetValue = String(value || (allowAll ? "all" : "")).trim();
  if (!targetValue) return;
  const hasOption = Array.from(select.options || []).some((option) => String(option.value || "") === targetValue);
  if (!hasOption) {
    const option = document.createElement("option");
    option.value = targetValue;
    option.textContent = targetValue;
    select.appendChild(option);
  }
  select.value = targetValue;
}

function populateTicketFilterSelect(select, values, currentValue) {
  if (!select) return;
  const fallbackValue = select === ticketsStatusFilter ? (currentTicketStatusFilter || "non_closed") : "all";
  const selectedValue = String(currentValue || select.value || fallbackValue).trim() || fallbackValue;
  const dynamic = Array.isArray(values) ? values.map((item) => String(item || "").trim()).filter(Boolean) : [];
  const staticOptions = select === ticketsStatusFilter
    ? ['<option value="non_closed">非关闭</option>', '<option value="all">全部</option>']
    : ['<option value="all">全部</option>'];
  select.innerHTML = [...staticOptions, ...dynamic.map((item) => `<option value="${escapeHtml(item)}">${escapeHtml(item)}</option>`)].join("");
  ensureSelectValue(select, selectedValue, { allowAll: true });
}

function ticketBadgeClass(prefix, value) {
  const suffix = String(value || "").trim().toLowerCase().replace(/[^a-z0-9_-]+/g, "-");
  return suffix ? `${prefix}-${suffix}` : "";
}

function renderTicketTraceLinks(traces) {
  if (!ticketRelatedTracesLinks) return;
  const items = parseTicketTraceList(traces);
  ticketRelatedTracesLinks.innerHTML = items.length
    ? items.map((traceId) => `<button class="ticket-trace-link-btn" type="button" data-ticket-trace-open="${escapeHtml(traceId)}">${escapeHtml(traceId)}</button>`).join("")
    : '<span class="ticket-empty-state">暂无关联 trace</span>';
}

function buildEmptyTicket() {
  return {
    ticket_id: "",
    title: "",
    status: "open",
    priority: "medium",
    domain: "",
    category: "",
    summary: "",
    related_traces: [],
    repro_query: "",
    expected_behavior: "",
    actual_behavior: "",
    root_cause: "",
    fix_notes: "",
    additional_notes: "",
    created_at: "",
    updated_at: "",
  };
}

function applyTicketToForm(ticket) {
  const value = ticket && typeof ticket === "object" ? ticket : buildEmptyTicket();
  currentTicketId = String(value.ticket_id || "").trim();
  if (ticketIdInput) ticketIdInput.value = currentTicketId;
  if (ticketTraceIdInput) ticketTraceIdInput.value = parseTicketTraceList(value.related_traces)[0] || "";
  if (ticketTitleInput) ticketTitleInput.value = String(value.title || "");
  if (ticketStatusInput) ensureSelectValue(ticketStatusInput, value.status || "open");
  if (ticketPriorityInput) ensureSelectValue(ticketPriorityInput, value.priority || "medium");
  if (ticketDomainInput) ticketDomainInput.value = String(value.domain || "");
  if (ticketCategoryInput) ticketCategoryInput.value = String(value.category || "");
  if (ticketCreatedAtInput) ticketCreatedAtInput.value = String(value.created_at || "");
  if (ticketUpdatedAtInput) ticketUpdatedAtInput.value = String(value.updated_at || "");
  if (ticketRelatedTracesInput) ticketRelatedTracesInput.value = parseTicketTraceList(value.related_traces).join("\n");
  if (ticketReproQueryInput) ticketReproQueryInput.value = String(value.repro_query || "");
  if (ticketSummaryInput) ticketSummaryInput.value = String(value.summary || "");
  if (ticketExpectedBehaviorInput) ticketExpectedBehaviorInput.value = String(value.expected_behavior || "");
  if (ticketActualBehaviorInput) ticketActualBehaviorInput.value = String(value.actual_behavior || "");
  if (ticketRootCauseInput) ticketRootCauseInput.value = String(value.root_cause || "");
  if (ticketFixNotesInput) ticketFixNotesInput.value = String(value.fix_notes || "");
  if (ticketAdditionalNotesInput) ticketAdditionalNotesInput.value = String(value.additional_notes || "");
  renderTicketTraceLinks(value.related_traces || []);
  if (ticketsDetailMeta) {
    ticketsDetailMeta.textContent = currentTicketId
      ? `${currentTicketId} | ${String(value.status || "open")} | ${String(value.priority || "medium")}`
      : "可先用 trace_id 生成草稿，再人工编辑更新字段";
  }
  if (ticketsDeleteBtn) ticketsDeleteBtn.disabled = !currentTicketId;
}

function closeTicketDeleteModal() {
  if (!ticketDeleteModal) return;
  ticketDeleteModal.classList.add("hidden");
  ticketDeleteModal.setAttribute("aria-hidden", "true");
  if (ticketDeleteConfirmSelect) ticketDeleteConfirmSelect.value = "";
  if (ticketDeleteConfirmBtn) ticketDeleteConfirmBtn.disabled = true;
  pendingDeleteTicketId = "";
}

function openTicketDeleteModal() {
  if (!currentTicketId) {
    showAppErrorModal("删除 Ticket 失败", "当前没有可删除的 Ticket");
    return;
  }
  pendingDeleteTicketId = currentTicketId;
  const current = currentTickets.find((item) => String(item.ticket_id || "") === currentTicketId);
  if (ticketDeleteMeta) {
    ticketDeleteMeta.textContent = current
      ? `${currentTicketId} | ${String(current.title || "未命名 Ticket")}`
      : currentTicketId;
  }
  if (ticketDeleteConfirmSelect) ticketDeleteConfirmSelect.value = "";
  if (ticketDeleteConfirmBtn) ticketDeleteConfirmBtn.disabled = true;
  ticketDeleteModal?.classList.remove("hidden");
  ticketDeleteModal?.setAttribute("aria-hidden", "false");
}

async function confirmDeleteCurrentTicket() {
  const ticketId = String(pendingDeleteTicketId || currentTicketId || "").trim();
  if (!ticketId) {
    showAppErrorModal("删除 Ticket 失败", "未找到可删除的 Ticket ID");
    return;
  }
  if (String(ticketDeleteConfirmSelect?.value || "") !== "delete") {
    showAppErrorModal("删除 Ticket 失败", "请先在下拉框中选择“确认删除”");
    return;
  }
  await apiPost(`/api/dashboard/tickets/${encodeURIComponent(ticketId)}?deleted_by=human`, {}, "DELETE");
  closeTicketDeleteModal();
  applyTicketToForm(buildEmptyTicket());
  await refreshTickets({ keepSelection: false });
  await refreshDashboard({ force: true, skipTaskCenter: true });
}

function collectTicketFormPayload() {
  return {
    title: String(ticketTitleInput?.value || "").trim(),
    status: String(ticketStatusInput?.value || "open").trim() || "open",
    priority: String(ticketPriorityInput?.value || "medium").trim() || "medium",
    domain: String(ticketDomainInput?.value || "").trim(),
    category: String(ticketCategoryInput?.value || "").trim(),
    summary: String(ticketSummaryInput?.value || "").trim(),
    related_traces: parseTicketTraceList(ticketRelatedTracesInput?.value || ""),
    repro_query: String(ticketReproQueryInput?.value || "").trim(),
    expected_behavior: String(ticketExpectedBehaviorInput?.value || "").trim(),
    actual_behavior: String(ticketActualBehaviorInput?.value || "").trim(),
    root_cause: String(ticketRootCauseInput?.value || "").trim(),
    fix_notes: String(ticketFixNotesInput?.value || "").trim(),
    additional_notes: String(ticketAdditionalNotesInput?.value || "").trim(),
  };
}

function collectTicketFilterParams() {
  return {
    status: String(ticketsStatusFilter?.value || currentTicketStatusFilter || "non_closed").trim() || "non_closed",
    priority: String(ticketsPriorityFilter?.value || "all").trim() || "all",
    domain: String(ticketsDomainFilter?.value || "all").trim() || "all",
    category: String(ticketsCategoryFilter?.value || "all").trim() || "all",
    search: String(ticketsSearchInput?.value || "").trim(),
    created_from: String(ticketsCreatedFrom?.value || "").trim(),
    created_to: String(ticketsCreatedTo?.value || "").trim(),
    sort: currentTicketSort,
    limit: 200,
  };
}

function renderTicketSortButton() {
  if (!ticketsSortToggleBtn) return;
  ticketsSortToggleBtn.textContent = currentTicketSort === "updated_asc" ? "最早优先" : "最新优先";
}

function isCompactTicketsLayout() {
  return typeof window !== "undefined" && window.matchMedia("(max-width: 980px)").matches;
}

function renderTicketsListCollapseButton() {
  if (!ticketsListCollapseBtn) return;
  ticketsListCollapseBtn.textContent = ticketsListCollapsed
    ? (isCompactTicketsLayout() ? "向下展开" : ">")
    : (isCompactTicketsLayout() ? "向上折叠" : "<");
  ticketsListCollapseBtn.setAttribute("aria-expanded", String(!ticketsListCollapsed));
  ticketsListCollapseBtn.setAttribute("aria-label", ticketsListCollapseBtn.textContent || "折叠 Ticket 列表");
}

function applyTicketsListLayoutState() {
  ticketsMainGrid?.classList.toggle("is-list-collapsed", ticketsListCollapsed);
  ticketsListShell?.classList.toggle("is-collapsed", ticketsListCollapsed);
  renderTicketsListCollapseButton();
}

function setTicketsListCollapsed(nextValue) {
  ticketsListCollapsed = Boolean(nextValue);
  try {
    window.localStorage.setItem(TICKETS_LIST_COLLAPSED_STORAGE_KEY, ticketsListCollapsed ? "1" : "0");
  } catch (_) {
    // Ignore localStorage failures and keep the in-memory state.
  }
  applyTicketsListLayoutState();
}

try {
  ticketsListCollapsed = window.localStorage.getItem(TICKETS_LIST_COLLAPSED_STORAGE_KEY) === "1";
} catch (_) {
  ticketsListCollapsed = false;
}

applyTicketsListLayoutState();

function renderTicketsList() {
  if (!ticketsList) return;
  ticketsList.innerHTML = currentTickets.length
    ? currentTickets.map((ticket) => {
        const ticketId = String(ticket.ticket_id || "");
        const title = escapeHtml(String(ticket.title || "未命名 Ticket"));
        const status = String(ticket.status || "open");
        const priority = String(ticket.priority || "medium");
        const domain = String(ticket.domain || "—");
        const category = String(ticket.category || "—");
        const activeClass = ticketId && ticketId === currentTicketId ? " is-active" : "";
        return `
          <article class="ticket-list-item${activeClass}" data-ticket-id="${escapeHtml(ticketId)}">
            <div class="ticket-list-top">
              <div>
                <div class="ticket-list-title">${title}</div>
                <div class="ticket-list-id">${escapeHtml(ticketId || "未保存")}</div>
              </div>
              <div class="dashboard-meta">${escapeHtml(String(ticket.updated_at || ticket.created_at || ""))}</div>
            </div>
            <div class="ticket-badge-row">
              <span class="ticket-badge ${ticketBadgeClass("status", status)}">${escapeHtml(status)}</span>
              <span class="ticket-badge ${ticketBadgeClass("priority", priority)}">${escapeHtml(priority)}</span>
              <span class="ticket-badge">${escapeHtml(domain)}</span>
              <span class="ticket-badge">${escapeHtml(category)}</span>
            </div>
          </article>
        `;
      }).join("")
    : '<div class="ticket-empty-state">当前筛选下暂无 ticket</div>';
}

async function refreshTickets({ keepSelection = true, selectTicketId = "" } = {}) {
  const params = collectTicketFilterParams();
  currentTicketStatusFilter = String(params.status || "non_closed").trim() || "non_closed";
  renderTicketSortButton();
  if (ticketsMeta) ticketsMeta.textContent = "正在加载 tickets...";
  const query = new URLSearchParams(params).toString();
  const data = await apiGet(`/api/dashboard/tickets?${query}`);
  currentTickets = Array.isArray(data?.items) ? data.items : [];
  populateTicketFilterSelect(ticketsStatusFilter, data?.filters?.statuses, params.status);
  populateTicketFilterSelect(ticketsPriorityFilter, data?.filters?.priorities, params.priority);
  populateTicketFilterSelect(ticketsDomainFilter, data?.filters?.domains, params.domain);
  populateTicketFilterSelect(ticketsCategoryFilter, data?.filters?.categories, params.category);
  if (ticketsMeta) ticketsMeta.textContent = `当前 ${formatNum(data?.count || currentTickets.length)} 条 ticket`;

  const preferredId = String(selectTicketId || (keepSelection ? currentTicketId : "")).trim();
  const nextTicket = preferredId ? currentTickets.find((ticket) => String(ticket.ticket_id || "") === preferredId) : null;
  if (nextTicket) {
    applyTicketToForm(nextTicket);
  } else if (!keepSelection || !currentTicketId) {
    applyTicketToForm(buildEmptyTicket());
  }
  renderTicketsList();
}

async function bootstrapTicketsTab() {
  await refreshTickets({ keepSelection: false });
  ticketsBootstrapped = true;
}

async function createTicketAIDraft({ traceId = "", switchToTab = false } = {}) {
  const traceValue = String(traceId || ticketTraceIdInput?.value || currentTraceRecord?.trace_id || "").trim();
  const draftPayload = {
    trace_id: traceValue,
    title: String(ticketTitleInput?.value || "").trim(),
    priority: String(ticketPriorityInput?.value || "medium").trim() || "medium",
    domain: String(ticketDomainInput?.value || "").trim(),
    category: String(ticketCategoryInput?.value || "").trim(),
    summary: String(ticketSummaryInput?.value || "").trim(),
    related_traces: parseTicketTraceList(ticketRelatedTracesInput?.value || ""),
    repro_query: String(ticketReproQueryInput?.value || "").trim(),
    expected_behavior: String(ticketExpectedBehaviorInput?.value || "").trim(),
    actual_behavior: String(ticketActualBehaviorInput?.value || "").trim(),
    root_cause: String(ticketRootCauseInput?.value || "").trim(),
    fix_notes: String(ticketFixNotesInput?.value || "").trim(),
    additional_notes: String(ticketAdditionalNotesInput?.value || "").trim(),
  };
  const data = await apiPost("/api/dashboard/tickets/ai-draft", draftPayload);
  const ticket = data?.ticket || buildEmptyTicket();
  applyTicketToForm(ticket);
  renderTicketsList();
  if (switchToTab) setMainTab("tickets");
}

async function saveCurrentTicket() {
  const payload = collectTicketFormPayload();
  if (!payload.title && !payload.summary && !payload.actual_behavior && !payload.repro_query) {
    window.alert("请至少填写标题、摘要、实际行为或复现 query 之一");
    return;
  }
  const ticketId = String(ticketIdInput?.value || currentTicketId || "").trim();
  const response = ticketId
    ? await apiPost(`/api/dashboard/tickets/${encodeURIComponent(ticketId)}`, payload, "PATCH")
    : await apiPost("/api/dashboard/tickets", payload);
  const savedTicket = response?.ticket || null;
  if (!savedTicket) {
    throw new Error("保存 ticket 失败：接口未返回 ticket 对象");
  }
  applyTicketToForm(savedTicket);
  await refreshTickets({ keepSelection: true, selectTicketId: String(savedTicket.ticket_id || "") });
}

function resetTicketEditor({ traceId = "", reproQuery = "", relatedTraces = [] } = {}) {
  applyTicketToForm(buildEmptyTicket());
  if (ticketTraceIdInput) ticketTraceIdInput.value = String(traceId || "").trim();
  if (ticketReproQueryInput) ticketReproQueryInput.value = String(reproQuery || "").trim();
  if (ticketRelatedTracesInput) ticketRelatedTracesInput.value = parseTicketTraceList(relatedTraces).join("\n");
  renderTicketTraceLinks(relatedTraces);
  renderTicketsList();
}

async function openTicketFromCurrentTrace() {
  const traceId = String(currentTraceRecord?.trace_id || dashboardTraceInput?.value || "").trim();
  if (!traceId) {
    showAppErrorModal("Ticket 生成失败", "当前没有可用的 trace_id");
    return;
  }
  if (!ticketsBootstrapped) {
    await bootstrapTicketsTab();
  }
  resetTicketEditor({
    traceId,
    reproQuery: String(currentTraceRecord?.query_understanding?.resolved_question || currentTraceRecord?.query_understanding?.original_question || "").trim(),
    relatedTraces: [traceId],
  });
  setMainTab("tickets");
  await createTicketAIDraft({ traceId, switchToTab: true });
}

function getBenchmarkModuleLabel(module) {
  return ({ rag: "RAG", agent: "LLM Agent", hybrid: "Hybrid" }[String(module || "").trim()] || String(module || "").toUpperCase());
}

function getLatestBenchmarkCaseFromRun(run) {
  if (!run || typeof run !== "object") return null;
  const modules = Array.isArray(run?.config?.modules) ? run.config.modules.map((item) => String(item || "").trim()).filter(Boolean) : [];
  const orderedModules = [];
  if (modules.includes("rag")) orderedModules.push("rag");
  modules.forEach((module) => {
    if ((module === "agent" || module === "hybrid") && !orderedModules.includes(module)) orderedModules.push(module);
  });
  let latest = null;
  ["short", "medium", "long"].forEach((length) => {
    orderedModules.forEach((module) => {
      const records = Array.isArray(run?.[module]?.records_by_length?.[length]) ? run[module].records_by_length[length] : [];
      if (!records.length) return;
      latest = {
        module,
        length,
        case_index: records.length,
        case_total: records.length,
        timestamp: String(run.timestamp || "").trim(),
        record: records[records.length - 1],
      };
    });
  });
  return latest;
}

function getLatestBenchmarkCase(results) {
  if (benchmarkLiveLatestCase?.record) return benchmarkLiveLatestCase;
  if (!Array.isArray(results) || !results.length) return null;
  return getLatestBenchmarkCaseFromRun(results[results.length - 1]);
}

async function loadBenchmarkHistory() {
  const data = await apiGet("/api/benchmark/history");
  benchmarkHistory = Array.isArray(data?.results) ? data.results : [];
  renderBenchmarkTable(benchmarkHistory);
  const last = benchmarkHistory.length ? benchmarkHistory[benchmarkHistory.length - 1] : null;
  const lastRun = document.getElementById("benchmark-last-run");
  if (lastRun) {
    if (!last) {
      lastRun.textContent = "从未运行";
    } else {
      const summary = last?.assertion_summary;
      const assertionText = summary ? ` | 断言 ${summary.passed}/${summary.passed + summary.failed}` : "";
      lastRun.textContent = `上次运行: ${last.timestamp || ""}${assertionText}`;
    }
  }
  return benchmarkHistory;
}

function getBenchmarkLatestCaseKey(latestCase, mode = benchmarkLiveLatestCase?.record ? "live" : "history") {
  if (!latestCase?.record || typeof latestCase.record !== "object") return "";
  return [
    mode,
    String(latestCase.module || "").trim(),
    String(latestCase.length || "").trim(),
    Number(latestCase.case_index || 0),
    Number(latestCase.case_total || 0),
    String(latestCase.timestamp || "").trim(),
    String(latestCase.record?.trace_id || "").trim(),
  ].join("|");
}

function renderBenchmarkLatestCaseFallback(latestCase, { missingTrace = false } = {}) {
  const record = latestCase?.record && typeof latestCase.record === "object" ? latestCase.record : {};
  const traceId = String(record.trace_id || "");
  const plannedTools = Array.isArray(record.planned_tools) ? record.planned_tools : [];
  const noContext = Number(record.no_context || 0) > 0;
  const notice = missingTrace
    ? '<div class="dashboard-meta">未找到对应的共享 trace 记录，当前展示的是 benchmark 结果里的精简摘要。点击“刷新当前最新”会重新从服务端拉取最新历史。</div>'
    : "";
  return `<article class="bm-case-card">
    <div class="trace-kv"><span>Trace ID</span><strong>${escapeHtml(traceId || "—")}</strong></div>
    <div class="trace-kv"><span>Query</span><strong>${escapeHtml(String(record.query || "—"))}</strong></div>
    <div class="trace-kv"><span>Query Type</span><strong>${escapeHtml(String(record.query_type || "—"))}</strong></div>
    <div class="trace-kv"><span>No Context</span><strong>${noContext ? `true / ${escapeHtml(String(record.no_context_reason || ""))}` : "false"}</strong></div>
    ${notice}
    <div class="trace-chip-list">${plannedTools.length ? plannedTools.map((tool) => `<span class="trace-chip">${escapeHtml(String(tool))}</span>`).join("") : '<span class="trace-chip">none</span>'}</div>
    <div class="card-modal-actions">
      <button class="ghost" data-trace-open="${escapeHtml(traceId)}"${traceId ? "" : " disabled"}>查看 Trace</button>
    </div>
  </article>`;
}

async function renderBenchmarkCaseTraceList(results, latestCase = benchmarkLiveLatestCase, { force = false } = {}) {
  const container = document.getElementById("bm-case-trace-list");
  const meta = document.getElementById("bm-case-trace-meta");
  if (!container) return;
  const resolved = latestCase?.record ? latestCase : getLatestBenchmarkCase(results);
  if (!resolved?.record) {
    benchmarkLatestTraceCacheKey = "";
    if (meta) meta.textContent = "显示正在运行或最近一次 Benchmark 的最新 trace 详情";
    container.innerHTML = '<div class="trace-result-empty">暂无 case trace 数据</div>';
    return;
  }
  const traceId = String(resolved.record?.trace_id || "").trim();
  if (meta) {
    meta.textContent = `${benchmarkLiveLatestCase?.record ? "运行中最新" : "最近完成"}: ${String(resolved.timestamp || "")} | ${getBenchmarkModuleLabel(resolved.module)} / ${resolved.length || ""} | case ${resolved.case_index || "-"}/${resolved.case_total || "-"}`;
  }
  if (!traceId) {
    benchmarkLatestTraceCacheKey = "";
    container.innerHTML = renderBenchmarkLatestCaseFallback(resolved);
    return;
  }
  const cacheKey = `${traceId}|${benchmarkLiveLatestCase?.record ? "live" : "history"}`;
  if (!force && benchmarkLatestTraceCacheKey === cacheKey) return;
  benchmarkLatestTraceCacheKey = cacheKey;
  const renderSeq = ++benchmarkLatestTraceRenderSeq;
  if (!container.children.length) {
    container.innerHTML = '<div class="trace-result-empty">正在加载最新 case trace...</div>';
  }
  try {
    const { trace } = await fetchTrace(traceId);
    if (renderSeq !== benchmarkLatestTraceRenderSeq) return;
    if (!trace) {
      container.innerHTML = renderBenchmarkLatestCaseFallback(resolved, { missingTrace: true });
      return;
    }
    container.innerHTML = `${renderTraceSummary(trace)}<div class="card-modal-actions"><button class="ghost" data-trace-open="${escapeHtml(traceId)}">弹窗查看</button></div>`;
  } catch (_err) {
    if (renderSeq !== benchmarkLatestTraceRenderSeq) return;
    container.innerHTML = renderBenchmarkLatestCaseFallback(resolved, { missingTrace: true });
  }
}

function updateBenchmarkLiveStateFromJob(job, { render = true } = {}) {
  const meta = job?.metadata && typeof job.metadata === "object" ? job.metadata : {};
  const latestCase = meta.latest_case && typeof meta.latest_case === "object" ? meta.latest_case : null;
  let changed = false;
  if (latestCase && latestCase.record && typeof latestCase.record === "object") {
    const nextLatestCase = {
      module: String(latestCase.module || "").trim(),
      length: String(latestCase.length || "").trim(),
      case_index: Number(latestCase.case_index || 0),
      case_total: Number(latestCase.case_total || 0),
      timestamp: String(latestCase.timestamp || "").trim(),
      record: latestCase.record,
    };
    const nextKey = getBenchmarkLatestCaseKey(nextLatestCase, "live");
    changed = nextKey !== benchmarkLiveLatestCaseKey;
    benchmarkLiveLatestCase = nextLatestCase;
    benchmarkLiveLatestCaseKey = nextKey;
  } else if (benchmarkLiveLatestCase || benchmarkLiveLatestCaseKey) {
    benchmarkLiveLatestCase = null;
    benchmarkLiveLatestCaseKey = "";
    changed = true;
  }
  if (render && changed) renderBenchmarkCaseTraceList(benchmarkHistory, benchmarkLiveLatestCase, { force: true }).catch(() => {});
  return changed;
}

async function refreshBenchmarkLatestCase() {
  if (!activeBenchmarkJobId) {
    benchmarkLiveLatestCase = null;
    benchmarkLiveLatestCaseKey = "";
    await loadBenchmarkHistory();
    return;
  }
  const job = await apiGet(`/api/benchmark/jobs/${encodeURIComponent(activeBenchmarkJobId)}`);
  updateBenchmarkLiveStateFromJob(job, { render: false });
  await renderBenchmarkCaseTraceList(benchmarkHistory, benchmarkLiveLatestCase, { force: true });
}

function renderBenchmarkTable(results) {
  const table = document.getElementById("bm-history-table");
  if (!table) return;

  const testSet = currentBmTestSet;
  const [module, length] = testSet.split("/");
  const emptyColspan = 1 + BENCHMARK_HISTORY_COLUMNS;

  if (!results.length) {
    table.innerHTML = `<tbody><tr><td colspan="${emptyColspan}" style="text-align:center;color:#7a7f6f">运行测试后查看历史对比数据</td></tr></tbody>`;
    renderBenchmarkCaseTraceList([], benchmarkLiveLatestCase, { force: true }).catch(() => {});
    return;
  }

  // Only keep runs that contain data for the selected module+length
  const relevant = results.filter((r) => r?.[module]?.by_length?.[length]);
  if (!relevant.length) {
    const mod = ({ rag: "RAG", agent: "AGENT", hybrid: "HYBRID" }[module] || module.toUpperCase());
    const lenLabel = { short: "短", medium: "中", long: "长" }[length] || length;
    table.innerHTML = `<tbody><tr><td colspan="${emptyColspan}" style="text-align:center;color:#7a7f6f">当前测试集（${mod} / ${lenLabel}查询）暂无数据，请先运行包含该模块的测试</td></tr></tbody>`;
    renderBenchmarkCaseTraceList(results, benchmarkLiveLatestCase, { force: true }).catch(() => {});
    return;
  }

  // Up to 4 most-recent matching runs, newest first (→ leftmost column)
  const recent = relevant.slice(-BENCHMARK_HISTORY_COLUMNS).reverse();

  const thead = `<thead><tr>
    <th style="min-width:120px">指标</th>
    ${recent.map((r, i) => {
      const ts = String(r.timestamp || "").replace("T", " ").slice(5, 16);
      const mods = (r.config?.modules || []).map((m) => ({ rag: "RAG", agent: "AGENT", hybrid: "HYBRID" }[m] || String(m).toUpperCase())).join("+");
      return `<th${i === 0 ? ' class="bm-latest"' : ""}>${escapeHtml(ts)}<br><small style="font-weight:normal;color:#9a9f8f">${escapeHtml(mods)}</small></th>`;
    }).join("")}
  </tr></thead>`;

  const fmt = (v) => {
    const n = Number(v);
    if (v == null || !Number.isFinite(n) || n <= 0) return `<span style='color:#5a5f50'>—</span>`;
    let s;
    if (n >= 1) s = n.toFixed(2) + "s";
    else if (n >= 0.001) s = (n * 1000).toFixed(1) + "ms";
    else s = Math.round(n * 1_000_000) + "µs";
    return `<span style="font-variant-numeric:tabular-nums">${s}</span>`;
  };
  const fmtZeroable = (v) => {
    if (v == null) return `<span style='color:#5a5f50'>—</span>`;
    const n = Number(v);
    if (!Number.isFinite(n)) return `<span style='color:#5a5f50'>—</span>`;
    if (n === 0) return `<span style="font-variant-numeric:tabular-nums">0ms</span>`;
    return fmt(n);
  };
  const fmtPct = (v) =>
    v != null && Number.isFinite(Number(v))
      ? `${(Number(v) * 100).toFixed(1)}%`
      : `<span style='color:#5a5f50'>—</span>`;
  const fmtN = (d) =>
    d?.count != null ? `${d.count - (d.errors || 0)} / ${d.count}` : "—";

  // Row definitions differ slightly per module
  const ragRows = [
    ["总时长均值",     (d) => fmt(d?.avg_wall_clock_s)],
    ["总时长 p95",      (d) => fmt(d?.p95_wall_clock_s)],
    ["端到端均值",   (d) => fmt(d?.avg_elapsed_s)],
    ["向量召回均值",   (d) => fmt(d?.avg_total_s)],
    ["向量召回 p95",    (d) => fmt(d?.p95_total_s)],
    ["模型重排均值",   (d) => fmt(d?.avg_rerank_seconds_s)],
    ["模型重排 p95",    (d) => fmt(d?.p95_rerank_seconds_s)],
  ];
  const agentRows = [
    ["总时长均值",   (d) => fmt(d?.avg_wall_clock_s)],
    ["总时长 p95",    (d) => fmt(d?.p95_wall_clock_s)],
    ["端到端均值",   (d) => fmt(d?.avg_elapsed_s ?? d?.avg_wall_clock_s)],
    ["端到端 p95",    (d) => fmt(d?.p95_elapsed_s ?? d?.p95_wall_clock_s)],
    ["向量召回均值", (d) => fmt(d?.avg_vector_recall_seconds_s)],
    ["向量召回 p95",  (d) => fmt(d?.p95_vector_recall_seconds_s)],
    ["融合排序均值", (d) => fmtZeroable(d?.avg_rerank_seconds_s)],
    ["融合排序 p95",  (d) => fmtZeroable(d?.p95_rerank_seconds_s)],
  ];
  const assertionRows = [
    ["当前集断言", (_d, run) => summarizeAssertionScope(run?.assertions?.[module]?.by_length?.[length])],
    ["全局断言", (_d, run) => summarizeAssertionScope(run?.assertions?.[module]?.global)],
    ["当前集失败项", (_d, run) => summarizeAssertionFailures(run?.assertions?.[module]?.by_length?.[length])],
  ];
  const rowDefs = (module === "rag" ? ragRows : agentRows).concat(assertionRows);

  const rows = rowDefs.map(([label, getter]) => {
    const cellClass = /断言|失败项/.test(String(label)) ? "benchmark-assert-cell" : "benchmark-metric-cell";
    return `<tr><td style="white-space:nowrap;color:#b0b89a;font-size:0.82em">${escapeHtml(label)}</td>${recent
      .map((r) => `<td class="${cellClass}">${getter(r?.[module]?.by_length?.[length], r)}</td>`)
      .join("")}</tr>`;
  });
  table.innerHTML = `${thead}<tbody>${rows.join("")}</tbody>`;
  renderBenchmarkCaseTraceList(results, benchmarkLiveLatestCase, { force: true }).catch(() => {});
}

async function bootstrapBenchmarkTab() {
  try {
    const data = await apiGet("/api/benchmark/history");
    benchmarkHistory = Array.isArray(data.results) ? data.results : [];
    renderBenchmarkTable(benchmarkHistory);
    if (benchmarkHistory.length) {
      const last = benchmarkHistory[benchmarkHistory.length - 1];
      const lastRun = document.getElementById("benchmark-last-run");
      if (lastRun) lastRun.textContent = `上次运行: ${last.timestamp || ""}`;
    }
  } catch (_e) {
    // History not critical
  }

  try {
    const casesPayload = await apiGet("/api/benchmark/cases");
    const caseSel = document.getElementById("bm-case-set-select");
    benchmarkCaseSets = Array.isArray(casesPayload?.case_sets) ? casesPayload.case_sets : [];
    if (caseSel) {
      if (benchmarkCaseSets.length) {
        caseSel.innerHTML = benchmarkCaseSets.map((item) => {
          const id = String(item?.id || "");
          const label = String(item?.label || id);
          const maxCount = Number(item?.max_query_count_per_type || 0);
          const desc = `${id} / ${label}`;
          return `<option value="${escapeHtml(id)}"${id === "regression_v1" ? " selected" : ""}>${escapeHtml(desc)}</option>`;
        }).join("");
      }
    }
    syncBenchmarkCountOptions();
  } catch (_e) {
    // Case-set list is optional; keep template fallback.
  }

  // Wire up the test-set dropdown
  const sel = document.getElementById("bm-testset-select");
  if (sel) {
    sel.value = currentBmTestSet;
    sel.addEventListener("change", () => {
      currentBmTestSet = sel.value;
      renderBenchmarkTable(benchmarkHistory);
    });
  }

  document.getElementById("bm-case-set-select")?.addEventListener("change", () => {
    syncBenchmarkCountOptions();
  });

  benchmarkBootstrapped = true;
}

function syncBenchmarkCountOptions() {
  const caseSetSelect = document.getElementById("bm-case-set-select");
  const selectedCaseSetId = String(caseSetSelect?.value || "regression_v1");
  const selectedCaseSet = benchmarkCaseSets.find((item) => String(item?.id || "") === selectedCaseSetId) || null;
  const maxCount = Number(selectedCaseSet?.max_query_count_per_type || 0);
  const radios = Array.from(document.querySelectorAll("input[name='bm-count']"));
  let fallbackRadio = null;
  let checkedRadio = null;
  radios.forEach((radio) => {
    const value = parseInt(String(radio.value || "0"), 10);
    const enabled = !maxCount || value <= maxCount;
    radio.disabled = !enabled;
    if (enabled && !fallbackRadio) fallbackRadio = radio;
    if (radio.checked) checkedRadio = radio;
    const label = radio.closest("label");
    if (label) label.style.opacity = enabled ? "1" : "0.45";
  });
  if (checkedRadio?.disabled && fallbackRadio) {
    fallbackRadio.checked = true;
  }
}

function runBenchmark() {
  const ragCheck = document.getElementById("bm-rag");
  const agentCheck = document.getElementById("bm-agent");
  const hybridCheck = document.getElementById("bm-hybrid");
  const caseSetSelect = document.getElementById("bm-case-set-select");
  const countRadio = document.querySelector("input[name='bm-count']:checked");
  const modules = [];
  if (ragCheck?.checked) modules.push("rag");
  if (agentCheck?.checked) modules.push("agent");
  if (hybridCheck?.checked) modules.push("hybrid");
  if (!modules.length) { window.alert("请至少选择一个测试模块"); return; }
  const queryCount = parseInt(countRadio?.value || "3", 10);
  const caseSetId = String(caseSetSelect?.value || "regression_v1");

  const runBtn = document.getElementById("bm-run-btn");
  const abortBtn = document.getElementById("bm-abort-btn");
  const progressWrap = document.getElementById("bm-progress-wrap");
  const progressFill = document.getElementById("bm-progress-fill");
  const progressText = document.getElementById("bm-progress-text");
  const logBox = document.getElementById("bm-log-box");
  const timerEl = document.getElementById("bm-timer");
  const lastRun = document.getElementById("benchmark-last-run");

  if (runBtn) runBtn.disabled = true;
  if (abortBtn) abortBtn.disabled = false;
  if (progressWrap) progressWrap.classList.remove("hidden");
  if (logBox) { logBox.textContent = ""; logBox.classList.remove("hidden"); }
  benchmarkLiveLatestCase = null;
  benchmarkLiveLatestCaseKey = "";
  benchmarkLatestTraceCacheKey = "";
  renderBenchmarkCaseTraceList(benchmarkHistory, null, { force: true }).catch(() => {});
  if (progressFill) progressFill.style.width = "0%";
  if (progressText) progressText.textContent = "连接中...";

  // Start elapsed timer
  const startTime = Date.now();
  if (benchmarkTimerInterval) clearInterval(benchmarkTimerInterval);
  if (timerEl) timerEl.textContent = "00:00";
  benchmarkTimerInterval = setInterval(() => {
    const elapsed = Math.floor((Date.now() - startTime) / 1000);
    const mm = String(Math.floor(elapsed / 60)).padStart(2, "0");
    const ss = String(elapsed % 60).padStart(2, "0");
    if (timerEl) timerEl.textContent = `${mm}:${ss}`;
  }, 1000);

  apiPost("/api/benchmark/jobs", { modules, query_count_per_type: queryCount, case_set_id: caseSetId })
    .then(async (resp) => {
      activeBenchmarkJobId = String(resp?.job?.id || "");
      lastBenchmarkLogCount = 0;
      lastBenchmarkLogMarker = "";
      const job = await pollJsonJob(`/api/benchmark/jobs/${encodeURIComponent(activeBenchmarkJobId)}`, {
        onUpdate: (runningJob) => {
          updateBenchmarkLiveStateFromJob(runningJob, { render: true });
          const msg = String(runningJob?.message || "等待中...");
          if (progressText) progressText.textContent = msg;
          const total = Number(runningJob?.total || 0);
          const current = Number(runningJob?.current || 0);
          if (progressFill && total > 0) {
            progressFill.style.width = `${Math.round((current / total) * 100)}%`;
          }
          const logs = Array.isArray(runningJob?.logs) ? runningJob.logs : [];
          syncBenchmarkLogBox(logBox, logs);
        },
      });
      if (job?.status === "completed") {
        const result = job?.result;
        if (result) {
          benchmarkLiveLatestCase = null;
          benchmarkLiveLatestCaseKey = "";
          benchmarkLatestTraceCacheKey = "";
          benchmarkHistory.push(result);
          renderBenchmarkTable(benchmarkHistory);
          if (lastRun) {
            const summary = result?.assertion_summary;
            const assertionText = summary ? ` | 断言 ${summary.passed}/${summary.passed + summary.failed}` : "";
            lastRun.textContent = `上次运行: ${result.timestamp || ""}${assertionText}`;
          }
        }
        if (progressFill) progressFill.style.width = "100%";
        if (progressText) progressText.textContent = "测试完成";
        if (logBox) {
          const summary = job?.result?.assertion_summary;
          if (summary) logBox.textContent += `✓ 断言通过 ${summary.passed}，失败 ${summary.failed}\n`;
          logBox.textContent += "✓ 测试完成\n";
          logBox.scrollTop = logBox.scrollHeight;
        }
      } else if (job?.status === "cancelled") {
        benchmarkLiveLatestCase = null;
        benchmarkLiveLatestCaseKey = "";
        benchmarkLatestTraceCacheKey = "";
        renderBenchmarkCaseTraceList(benchmarkHistory, null, { force: true }).catch(() => {});
        if (progressText) progressText.textContent = "已中止";
        if (logBox) {
          logBox.textContent += "⏸ 用户中止测试\n";
          logBox.scrollTop = logBox.scrollHeight;
        }
      } else {
        throw new Error(job?.error || job?.message || "Benchmark 失败");
      }
    })
    .catch((err) => {
      if (progressText) progressText.textContent = `连接失败: ${String(err)}`;
    })
    .finally(() => {
    if (runBtn) runBtn.disabled = false;
    if (abortBtn) abortBtn.disabled = true;
    if (benchmarkTimerInterval) { clearInterval(benchmarkTimerInterval); benchmarkTimerInterval = null; }
    benchmarkAbortController = null;
    activeBenchmarkJobId = "";
  });
}

async function clearBenchmarkHistory() {
  if (!window.confirm("确定清除所有 Benchmark 历史记录？")) return;
  await apiDelete("/api/benchmark/history");
  benchmarkHistory = [];
  benchmarkLiveLatestCase = null;
  benchmarkLiveLatestCaseKey = "";
  benchmarkLatestTraceCacheKey = "";
  renderBenchmarkTable([]);
  const lastRun = document.getElementById("benchmark-last-run");
  if (lastRun) lastRun.textContent = "从未运行";
}



function toggleSidebar(id) {
  const el = document.getElementById(id);
  if (!el) return;
  el.classList.toggle("collapsed");
  const workspace = el.closest(".workspace");
  if (workspace) {
    workspace.classList.toggle("sidebar-collapsed", el.classList.contains("collapsed"));
  }
  updateSidebarToggleButton(el);
}

function updateSidebarToggleButton(sidebar) {
  const el = sidebar instanceof Element ? sidebar : document.getElementById(String(sidebar || ""));
  if (!el) return;
  const btn = el.querySelector(".sidebar-toggle");
  if (!btn) return;
  const isMobile = window.matchMedia("(max-width: 820px), (hover: none) and (pointer: coarse)").matches;
  const isCollapsed = el.classList.contains("collapsed");
  btn.textContent = isMobile ? (isCollapsed ? "v" : "^") : (isCollapsed ? ">" : "<");
  btn.title = isMobile ? (isCollapsed ? "向下展开" : "向上折叠") : (isCollapsed ? "向右展开" : "向左折叠");
}

function setModel() {
  qaModel.textContent = `当前模型：${pageLocalModel}`;
}

async function apiGet(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

async function apiPost(url, payload, method = "POST") {
  const r = await fetch(url, {
    method,
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

async function apiDelete(url) {
  const r = await fetch(url, { method: "DELETE" });
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

async function apiPostForm(url, formData) {
  const r = await fetch(url, {
    method: "POST",
    body: formData,
  });
  if (!r.ok) {
    let detail = "上传失败";
    const raw = await r.text();
    if (raw) {
      try {
        const payload = JSON.parse(raw);
        detail = String(payload?.detail || payload?.message || raw || detail);
      } catch (_err) {
        detail = raw;
      }
    }
    throw new Error(detail);
  }
  return r.json();
}

async function apiUploadImage(url, file) {
  const name = encodeURIComponent(String(file?.name || "card.png"));
  const target = `${url}?filename=${name}`;
  const r = await fetch(target, {
    method: "POST",
    headers: {
      "Content-Type": String(file?.type || "application/octet-stream"),
    },
    body: file,
  });
  if (!r.ok) {
    let detail = "上传失败";
    const raw = await r.text();
    if (raw) {
      try {
        const payload = JSON.parse(raw);
        detail = String(payload?.detail || payload?.message || raw || detail);
      } catch (_err) {
        detail = raw;
      }
    }
    throw new Error(detail);
  }
  return r.json();
}

function normalizeCard(item) {
  return {
    title: String(item?.title || "").trim(),
    url: String(item?.url || "").trim(),
    image: String(item?.image || "").trim(),
  };
}

function loadInitialCards() {
  try {
    const raw = document.getElementById("custom-cards-data")?.textContent || "[]";
    const parsed = JSON.parse(raw);
    if (Array.isArray(parsed)) {
      customCards = parsed.map(normalizeCard);
      return;
    }
  } catch (_err) {
    // Keep fallback below.
  }
  customCards = [];
}

function getCardAt(index) {
  return normalizeCard(customCards[index] || {});
}

function setCardPreview(imageUrl) {
  const clean = String(imageUrl || "").trim();
  if (!customCardPreview) return;
  if (!clean) {
    customCardPreview.textContent = "无预览";
    showCropWrap(false);
    return;
  }
  customCardPreview.innerHTML = `<img src="${escapeHtml(clean)}" alt="card-preview" />`;
  initCropper(clean);
}

function closeCustomCardModal() {
  if (!customCardModal) return;
  customCardModal.classList.add("hidden");
  customCardModal.setAttribute("aria-hidden", "true");
  editingCardIndex = -1;
  cropState = null;
  showCropWrap(false);
}

function openCustomCardModal(index) {
  if (!customCardModal || !Number.isInteger(index) || index < 0 || index >= 8) return;
  editingCardIndex = index;
  const card = getCardAt(index);
  if (customCardModalTitle) customCardModalTitle.textContent = `配置快捷卡片 ${index + 1}`;
  if (customCardNameInput) customCardNameInput.value = card.title;
  if (customCardUrlInput) customCardUrlInput.value = card.url;
  if (customCardImageInput) customCardImageInput.value = card.image;
  setCardPreview(card.image);
  customCardModal.classList.remove("hidden");
  customCardModal.setAttribute("aria-hidden", "false");
  customCardNameInput?.focus();
}

async function uploadCustomCardImage() {
  const file = customCardUploadInput?.files?.[0];
  if (!file) return;
  try {
    const payload = await apiUploadImage("/api/custom_cards/upload", file);
    const image = String(payload?.image || "").trim();
    if (!image) throw new Error("图片上传失败，未返回路径");
    if (customCardImageInput) customCardImageInput.value = image;
    initCropper(image);
  } finally {
    if (customCardUploadInput) customCardUploadInput.value = "";
  }
}

async function saveCustomCardFromModal() {
  if (editingCardIndex < 0 || editingCardIndex >= 8) return;
  const title = String(customCardNameInput?.value || "").trim();
  const url = String(customCardUrlInput?.value || "").trim();
  const image = String(customCardImageInput?.value || "").trim();

  if (!title || !url) {
    window.alert("请至少填写名称和 URL。");
    return;
  }

  const payload = await apiPost(`/api/custom_cards/slot/${editingCardIndex}`, { title, url, image });
  const cards = Array.isArray(payload?.cards) ? payload.cards.map(normalizeCard) : [];
  if (cards.length) {
    customCards = cards;
  } else {
    customCards[editingCardIndex] = normalizeCard({ title, url, image });
  }
  renderCustomCards();
  closeCustomCardModal();
}

function bindCardLongPressEdit(target, index) {
  if (!(target instanceof HTMLElement)) return;
  let timer = null;
  let longPressed = false;

  const clearPress = () => {
    if (timer) {
      window.clearTimeout(timer);
      timer = null;
    }
  };

  const startPress = (event) => {
    if (event instanceof PointerEvent && event.pointerType === "mouse" && event.button !== 0) return;
    longPressed = false;
    clearPress();
    timer = window.setTimeout(() => {
      longPressed = true;
      openCustomCardModal(index);
    }, CARD_LONG_PRESS_MS);
  };

  const cancelPress = () => {
    clearPress();
  };

  target.addEventListener("pointerdown", startPress);
  target.addEventListener("pointerup", cancelPress);
  target.addEventListener("pointerleave", cancelPress);
  target.addEventListener("pointercancel", cancelPress);

  target.addEventListener("click", (event) => {
    if (longPressed) {
      event.preventDefault();
      event.stopPropagation();
      longPressed = false;
    }
  });
}

function getCurrentSession() {
  return sessionsCache.find((s) => s.id === currentSessionId);
}

function getSessionById(sessionId) {
  return sessionsCache.find((session) => String(session.id || "") === String(sessionId || "").trim()) || null;
}

function ensureAgentSessionCache(sessionId) {
  const sid = String(sessionId || "").trim();
  if (!sid) return null;
  let session = getSessionById(sid);
  if (session) {
    if (!Array.isArray(session.messages)) session.messages = [];
    return session;
  }
  session = {
    id: sid,
    title: "新会话",
    updated_at: new Date().toISOString().slice(0, 19),
    messages: [],
  };
  sessionsCache.unshift(session);
  return session;
}

function appendMessageToAgentSessionCache(sessionId, role, text, traceId = "") {
  const session = ensureAgentSessionCache(sessionId);
  if (!session) return;
  const message = { role, text };
  const normalizedTrace = String(traceId || "").trim();
  if (normalizedTrace) message.trace_id = normalizedTrace;
  session.messages.push(message);
  session.updated_at = new Date().toISOString().slice(0, 19);
}

function renderActiveAgentStreamPreview() {
  if (!activeAgentStreamState || String(activeAgentStreamState.sessionId || "") !== String(currentSessionId || "")) return;
  const progressLines = Array.isArray(activeAgentStreamState.progressLines) ? activeAgentStreamState.progressLines : [];
  const toolDoneLines = Array.isArray(activeAgentStreamState.toolDoneLines) ? activeAgentStreamState.toolDoneLines : [];
  const progress = progressLines.length ? progressLines[progressLines.length - 1] : "正在规划工具并调用...";
  const toolsText = toolDoneLines.length ? `\n${toolDoneLines.join("\n")}` : "";
  appendChatRow("system", `${progress}${toolsText}`, false, "processing");
  const answerText = String(activeAgentStreamState.answerText || "").trim();
  if (answerText) {
    const parsed = splitThinkBlocks(answerText);
    const assistantRow = appendChatRow("assistant", parsed.answer || answerText, false);
    insertSystemRowsBefore(assistantRow, parsed.thoughts);
    upsertTraceMetaRowBefore(assistantRow, formatTraceMeta(activeAgentStreamState.traceId));
  }
}

function renderCurrentAgentSessionView() {
  renderChat(getCurrentSession()?.messages || []);
  renderActiveAgentStreamPreview();
}

function renderSessions() {
  const ul = document.getElementById("agent-session-list");
  ul.innerHTML = "";
  for (const session of sessionsCache) {
    const li = document.createElement("li");
    li.dataset.sessionId = String(session.id || "");
    li.title = String(session.title || "新会话");
    if (session.id === currentSessionId) li.classList.add("active");
    li.innerHTML = `<div class=\"title\">${escapeHtml(session.title || "新会话")}</div><div class=\"meta\">${escapeHtml(session.updated_at || "")}</div>`;
    li.onclick = () => {
      if (Date.now() < suppressSessionClickUntil) return;
      currentSessionId = session.id;
      renderSessions();
      renderCurrentAgentSessionView();
    };
    ul.appendChild(li);
  }
}

function getSessionSummary(sessionId) {
  return sessionsCache.find((session) => String(session.id || "") === String(sessionId || "").trim()) || null;
}

function closeSessionRenameModal() {
  currentRenameSessionId = "";
  if (!sessionRenameModal) return;
  sessionRenameModal.classList.add("hidden");
  sessionRenameModal.setAttribute("aria-hidden", "true");
}

function openSessionRenameModal(sessionId) {
  const session = getSessionSummary(sessionId);
  if (!session || !sessionRenameModal || !sessionRenameInput) return;
  currentRenameSessionId = String(session.id || "").trim();
  sessionRenameInput.value = String(session.title || "新会话");
  if (sessionRenameMeta) sessionRenameMeta.textContent = `会话 ID: ${currentRenameSessionId}`;
  sessionRenameModal.classList.remove("hidden");
  sessionRenameModal.setAttribute("aria-hidden", "false");
  const shouldAutoFocus = !!(window.matchMedia && window.matchMedia("(pointer: fine)").matches);
  if (shouldAutoFocus) {
    window.setTimeout(() => {
      sessionRenameInput.focus();
      sessionRenameInput.select();
    }, 20);
  }
}

async function saveSessionRename() {
  const sessionId = String(currentRenameSessionId || "").trim();
  const title = String(sessionRenameInput?.value || "").trim();
  if (!sessionId || !title) return;
  await apiPost(`/api/agent/sessions/${encodeURIComponent(sessionId)}`, { title, lock: true }, "PATCH");
  closeSessionRenameModal();
  await refreshSessions(sessionId);
}

function openSessionRenameModalFromTarget(target) {
  const item = target instanceof Element ? target.closest("[data-session-id]") : null;
  const sessionId = String(item?.getAttribute("data-session-id") || "").trim();
  if (!sessionId) return;
  suppressSessionClickUntil = Date.now() + 650;
  openSessionRenameModal(sessionId);
}

function formatTraceMeta(traceId) {
  const value = String(traceId || "").trim();
  return value ? `\`Trace ID:\` ${value}` : "";
}

function appendChatRow(role, text, keepBottom = true, extraClass = "", metaText = "") {
  const row = document.createElement("div");
  row.className = `msg ${role}${extraClass ? ` ${extraClass}` : ""}`;
  const roleLabel = role === "user" ? "用户" : role === "assistant" ? "助手" : "系统";
  const metaHtml = metaText ? `<div class="msg-meta">${escapeHtml(metaText)}</div>` : "";
  row.innerHTML = `<div class="role">${roleLabel}</div>${metaHtml}<div class="content markdown-body">${markdownToHtml(text)}</div>`;
  qaMessages.appendChild(row);
  if (keepBottom) qaMessages.scrollTop = qaMessages.scrollHeight;
  return row;
}

function upsertTraceMetaRowBefore(targetRow, metaText) {
  if (!targetRow || !targetRow.parentElement) return null;
  const chat = targetRow.parentElement;
  let anchor = targetRow;
  let sibling = targetRow.previousSibling;
  while (sibling && sibling.classList && sibling.classList.contains("think")) {
    anchor = sibling;
    sibling = sibling.previousSibling;
  }

  const existingMetaRow = sibling && sibling.classList && sibling.classList.contains("trace-meta") ? sibling : null;
  const normalizedMeta = String(metaText || "").trim();
  if (!normalizedMeta) {
    if (existingMetaRow) existingMetaRow.remove();
    return null;
  }

  const row = existingMetaRow || document.createElement("div");
  row.className = "msg system trace-meta";
  row.innerHTML = `<div class="role">系统</div><div class="content trace-meta-content markdown-body">${markdownToHtml(normalizedMeta)}</div>`;
  if (!existingMetaRow) chat.insertBefore(row, anchor);
  return row;
}

function insertSystemRowsBefore(targetRow, blocks) {
  if (!targetRow || !targetRow.parentElement || !Array.isArray(blocks)) return;
  const chat = targetRow.parentElement;
  for (const textRaw of blocks) {
    const text = normalizeThinkText(textRaw);
    if (!text) continue;
    const row = document.createElement("div");
    row.className = "msg system think collapsed";
    row.innerHTML = `<div class="role">系统</div><div class="content markdown-body">${markdownToHtml(text)}</div>`;
    row.addEventListener("click", () => row.classList.toggle("collapsed"));
    chat.insertBefore(row, targetRow);
  }
}

function renderChat(messages) {
  qaMessages.innerHTML = "";
  for (const message of messages || []) {
    const roleRaw = String(message.role || "").trim().toLowerCase();
    const role = roleRaw === "user" || roleRaw === "用户" ? "user" : roleRaw === "assistant" || roleRaw === "助手" ? "assistant" : "system";
    const text = String(message.text || "");
    const metaText = formatTraceMeta(message.trace_id);
    if (role === "assistant") {
      const parsed = splitThinkBlocks(text);
      const assistantRow = appendChatRow("assistant", parsed.answer || text, false);
      insertSystemRowsBefore(assistantRow, parsed.thoughts);
      upsertTraceMetaRowBefore(assistantRow, metaText);
      addAgentFeedbackForRow(assistantRow, { answer: parsed.answer || text, traceId: message.trace_id });
    } else {
      appendChatRow(role, text, false);
    }
  }
  qaMessages.scrollTop = qaMessages.scrollHeight;
}

async function refreshSessions(preferSessionId = "") {
  const data = await apiGet("/api/agent/sessions");
  sessionsCache = Array.isArray(data.sessions) ? data.sessions : [];
  if (!sessionsCache.length) {
    const created = await apiPost("/api/agent/sessions", { title: "新会话" });
    sessionsCache = [created];
  }
  const candidate = preferSessionId || currentSessionId;
  if (!candidate || !sessionsCache.some((x) => x.id === candidate)) {
    currentSessionId = sessionsCache[0].id;
  } else {
    currentSessionId = candidate;
  }
  renderSessions();
  renderCurrentAgentSessionView();
}

function extractErrorDetail(text) {
  try {
    const obj = JSON.parse(text);
    if (obj && typeof obj.detail === "string") return obj.detail;
  } catch (_err) {
    // Keep original text.
  }
  return text;
}

function buildReferencesMarkdown(toolResults) {
  if (!Array.isArray(toolResults)) return "";
  const refs = [];

  for (const item of toolResults) {
    if (!item || typeof item !== "object") continue;
    const tool = String(item.tool || "").trim();
    const rows = Array.isArray(item.data?.results) ? item.data.results : [];
    for (const row of rows) {
      const score = Number(row?.score);
      if (!Number.isFinite(score)) continue;
      if (tool === "query_document_rag") {
        const path = String(row?.path || "").trim();
        if (!path) continue;
        refs.push({ score, line: `- [文档: ${path} (${score.toFixed(4)})](doc://${encodeURIComponent(path)})` });
      } else if (tool === "search_web") {
        const title = String(row?.title || row?.url || "网页").trim() || "网页";
        const url = String(row?.url || "").trim();
        if (!url) continue;
        refs.push({ score, line: `- [网页: ${title} (${score.toFixed(4)})](${url})` });
      } else if (tool === "query_media_record") {
        const title = String(row?.title || "").trim();
        const mediaType = String(row?.media_type || "").trim();
        const itemId = String(row?.id || "").trim();
        if (!title) continue;
        const label = `媒体: ${title}${mediaType ? ` (${mediaType})` : ""} (${score.toFixed(4)})`;
        if (itemId) {
          const previewLink = `${pageLibraryUrl.replace(/\/$/, "")}/?item=${encodeURIComponent(itemId)}`;
          refs.push({ score, line: `- [${label}](${previewLink})` });
        } else {
          refs.push({ score, line: `- ${label}` });
        }
      }
    }
  }

  if (!refs.length) return "";
  const lines = refs.sort((a, b) => b.score - a.score).slice(0, MAX_REFERENCE_ITEMS).map((x) => x.line);
  return `\n\n### 参考资料\n${lines.join("\n")}`;
}

function hasReferenceSections(text) {
  const value = String(text || "");
  return /###\s*(?:本地知识库参考|外部参考|参考资料)/.test(value);
}

function setRowContent(row, markdownText) {
  if (!(row instanceof HTMLElement)) return;
  const content = row.querySelector(".content");
  if (!(content instanceof HTMLElement)) return;
  content.innerHTML = markdownToHtml(markdownText || "");
}

function formatElapsed(seconds) {
  const total = Math.max(0, Math.floor(Number(seconds) || 0));
  const mm = String(Math.floor(total / 60)).padStart(2, "0");
  const ss = String(total % 60).padStart(2, "0");
  return `${mm}:${ss}`;
}

function buildPlanDetailsMarkdown(result, elapsedSeconds) {
  const planned = Array.isArray(result?.planned_tools) ? result.planned_tools : [];
  const toolResults = Array.isArray(result?.tool_results) ? result.tool_results : [];
  const lines = [`正在规划与调用工具... (${formatElapsed(elapsedSeconds)})`, ""];
  const traceId = String(result?.trace_id || "").trim();

  if (traceId) {
    lines.push(`Trace ID: ${traceId}`, "");
  }

  if (planned.length) {
    lines.push("计划调用：");
    for (const p of planned) {
      const name = String(p?.name || "").trim() || "unknown_tool";
      const query = String(p?.query || "").trim();
      lines.push(`- ${name}${query ? ` | query: ${query}` : ""}`);
    }
  }

  if (toolResults.length) {
    lines.push("", "执行结果：");
    for (const t of toolResults) {
      const tool = String(t?.tool || "unknown_tool").trim();
      const status = String(t?.status || "unknown").trim();
      const summary = String(t?.summary || "").trim();
      lines.push(`- ${tool} [${status}]${summary ? `: ${summary}` : ""}`);
    }
  }

  return lines.join("\n");
}

async function callAgent(question, searchMode, opts = {}) {
  const payload = {
    question,
    session_id: currentSessionId,
    history: [],
    backend: "local",
    search_mode: searchMode,
    confirm_over_quota: !!opts.confirm,
    deny_over_quota: !!opts.deny,
    debug: !!(qaDebugToggle && qaDebugToggle.checked),
  };

  const resp = await fetch("/api/agent/chat", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
    signal: opts.signal,
  });
  if (!resp.ok) {
    const raw = await resp.text();
    throw new Error(extractErrorDetail(raw));
  }
  return resp.json();
}

function renderCustomCards() {
  const host = document.getElementById("custom-card-grid");
  if (!host) return;
  host.innerHTML = "";

  const maxCards = 8;
  for (let i = 0; i < maxCards; i += 1) {
    const item = getCardAt(i);
    const title = String(item.title || "").trim();
    const url = String(item.url || "").trim();
    const image = String(item.image || "").trim();

    if (!title || !url) {
      const placeholder = document.createElement("button");
      placeholder.type = "button";
      placeholder.className = "custom-square-card placeholder";
      placeholder.innerHTML = `<div class="card-text plus">+</div>`;
      placeholder.title = `配置快捷卡片 ${i + 1}`;
      placeholder.addEventListener("click", () => openCustomCardModal(i));
      host.appendChild(placeholder);
      continue;
    }

    const card = document.createElement("a");
    card.className = "custom-square-card";
    card.href = url;
    card.target = "_blank";
    card.rel = "noopener noreferrer";
    card.title = `点击打开，长按编辑（卡片 ${i + 1}）`;
    bindCardLongPressEdit(card, i);

    const bg = document.createElement("div");
    bg.className = "card-bg";
    if (image) {
      bg.style.backgroundImage = `url('${image.replace(/'/g, "\\'")}')`;
    } else {
      bg.style.background = "linear-gradient(135deg, #3a3f34, #262821)";
    }

    const overlay = document.createElement("div");
    overlay.className = "card-overlay";

    const text = document.createElement("div");
    text.className = "card-text";
    text.textContent = title;

    card.appendChild(bg);
    card.appendChild(overlay);
    card.appendChild(text);
    host.appendChild(card);
  }
}

function wireChatLinks() {
  qaMessages.addEventListener("click", (event) => {
    const target = event.target;
    const anchor = target instanceof Element ? target.closest("a") : null;
    if (!(anchor instanceof HTMLAnchorElement)) return;
    const href = anchor.getAttribute("href") || "";
    if (href.startsWith("doc://")) {
      event.preventDefault();
      const path = decodeURIComponent(href.slice("doc://".length));
      if (/^https?:\/\//i.test(path)) {
        window.open(path, "_blank", "noopener,noreferrer");
        return;
      }
      window.open(pageAiSummaryUrl, "_blank", "noopener,noreferrer");
      return;
    }
    if (/^https?:\/\//i.test(href)) {
      event.preventDefault();
      window.open(href, "_blank", "noopener,noreferrer");
    }
  });
}

function appendLocalMessage(sessionId, role, text, traceId = "") {
  appendMessageToAgentSessionCache(sessionId, role, text, traceId);
  renderSessions();
}

function getPreviousUserQuestion(row) {
  let node = row?.previousSibling;
  while (node) {
    if (node instanceof HTMLElement && node.classList.contains("msg") && node.classList.contains("user")) {
      const content = node.querySelector(".content");
      return String(content?.textContent || "").trim();
    }
    node = node.previousSibling;
  }
  return "";
}

function addAgentFeedbackForRow(row, payload = {}) {
  const question = String(payload.question || getPreviousUserQuestion(row) || "").trim();
  const answer = String(payload.answer || row?.querySelector(".content")?.textContent || "").trim();
  const traceId = String(payload.traceId || "").trim();
  if (!isAssistantAnswerEligible(answer, traceId)) return;
  addFeedbackButton(row, {
    source: "agent_chat",
    question,
    answer,
    trace_id: traceId,
    session_id: currentSessionId,
    model: pageLocalModel,
    search_mode: String(payload.searchMode || ""),
    query_type: String(payload.queryType || ""),
    metadata: {
      trace_id: traceId,
      planned_tools: Array.isArray(payload.plannedTools) ? payload.plannedTools : [],
    },
  });
}

async function ask(searchMode) {
  if (askInFlight) return;
  const question = (qaInput.value || "").trim();
  if (!question) return;
  const requestSessionId = String(currentSessionId || "").trim();

  qaInput.value = "";
  appendChatRow("user", question);
  appendLocalMessage(requestSessionId, "user", question);

  const pendingStart = Date.now();
  const pending = appendChatRow("system", "正在规划工具并调用... (00:00)", true, "processing");
  const pendingContent = pending.querySelector(".content");
  let progressLines = [];
  let toolDoneLines = [];
  let streamFinalized = false;
  let quotaExceededEvent = null;
  let activeTraceId = "";
  activeAgentStreamState = {
    sessionId: requestSessionId,
    question,
    searchMode,
    progressLines: [],
    toolDoneLines: [],
    answerText: "",
    traceId: "",
  };

  const pendingTimer = window.setInterval(() => {
    if (streamFinalized) return;
    const elapsed = formatElapsed((Date.now() - pendingStart) / 1000);
    const progress = progressLines.length ? progressLines[progressLines.length - 1] : "正在规划工具并调用...";
    const toolsText = toolDoneLines.length ? "\n" + toolDoneLines.join("\n") : "";
    if (pendingContent) pendingContent.innerHTML = markdownToHtml(`${progress}${toolsText}\n\n耗时: ${elapsed}`);
  }, 500);

  askInFlight = true;
  qaAsk.disabled = true;
  qaAskLocal.disabled = true;
  qaAbort.disabled = false;
  activeController = new AbortController();

  const buildPayload = (opts = {}) => ({
    question,
    session_id: currentSessionId,
    history: [],
    backend: "local",
    search_mode: searchMode,
    confirm_over_quota: !!opts.confirm,
    deny_over_quota: !!opts.deny,
    debug: !!(qaDebugToggle && qaDebugToggle.checked),
  });

  const tryStream = async (opts = {}) => {
    const resp = await fetch("/api/agent/chat_stream", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(buildPayload(opts)),
      signal: activeController.signal,
    });
    if (!resp.ok || !resp.body) {
      const raw = await resp.text();
      throw new Error(extractErrorDetail(raw));
    }

    const reader = resp.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";

    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const parts = buffer.split(/\r?\n\r?\n/);
      buffer = parts.pop() || "";

      for (const part of parts) {
        const lines = part.split(/\r?\n/);
        for (const raw of lines) {
          const line = String(raw || "").trimStart();
          if (!line.startsWith("data:")) continue;
          const jsonText = line.slice(5).trim();
          if (!jsonText) continue;
          let event;
          try { event = JSON.parse(jsonText); } catch (_e) { continue; }

          if (event.type === "progress") {
            if (event.trace_id) activeTraceId = String(event.trace_id || "").trim();
            progressLines.push(String(event.message || ""));
            if (activeAgentStreamState && activeAgentStreamState.sessionId === requestSessionId) {
              activeAgentStreamState.progressLines = [...progressLines];
              activeAgentStreamState.traceId = activeTraceId;
            }
          } else if (event.type === "tool_done") {
            if (event.trace_id) activeTraceId = String(event.trace_id || "").trim();
            const tool = String(event.tool || "");
            const status = String(event.status || "");
            const summary = String(event.summary || "");
            const statusIcon = status === "ok" ? "✓" : status === "error" ? "✗" : "•";
            toolDoneLines.push(`${statusIcon} **${tool}** [${status}]${summary ? `: ${summary}` : ""}`);
            if (activeAgentStreamState && activeAgentStreamState.sessionId === requestSessionId) {
              activeAgentStreamState.toolDoneLines = [...toolDoneLines];
              activeAgentStreamState.traceId = activeTraceId;
            }
          } else if (event.type === "quota_exceeded") {
            if (event.trace_id) activeTraceId = String(event.trace_id || "").trim();
            streamFinalized = true;
            quotaExceededEvent = event;
            if (activeAgentStreamState && activeAgentStreamState.sessionId === requestSessionId) {
              activeAgentStreamState.traceId = activeTraceId;
            }
            pending.classList.remove("processing");
            return { quotaExceeded: true, event };
          } else if (event.type === "error") {
            streamFinalized = true;
            throw new Error(String(event.message || "Agent 服务出错"));
          } else if (event.type === "done") {
            if (event.trace_id) activeTraceId = String(event.trace_id || "").trim();
            streamFinalized = true;
            return { done: true, payload: event.payload };
          }
        }
      }
      // also process trailing buffer
      if (buffer.trim()) {
        const lines = buffer.split(/\r?\n/);
        for (const raw of lines) {
          const line = String(raw || "").trimStart();
          if (!line.startsWith("data:")) continue;
          const jsonText = line.slice(5).trim();
          if (!jsonText) continue;
          let event;
          try { event = JSON.parse(jsonText); } catch (_e) { continue; }
          if (event.trace_id) activeTraceId = String(event.trace_id || "").trim();
          if (event.type === "done") { streamFinalized = true; return { done: true, payload: event.payload }; }
          if (event.type === "error") { streamFinalized = true; throw new Error(String(event.message || "Agent 服务出错")); }
          if (event.type === "quota_exceeded") { streamFinalized = true; quotaExceededEvent = event; pending.classList.remove("processing"); return { quotaExceeded: true, event }; }
        }
      }
    }
    if (!streamFinalized) {
      streamFinalized = true;
      throw new Error("服务器连接异常中断，请重试");
    }
  };

  try {
    let streamResult = await tryStream();

    if (streamResult?.quotaExceeded) {
      const qEvent = streamResult.event;
      const msg = qEvent.message || "已超过配额，是否继续？";
      const ok = window.confirm(msg);
      progressLines = [];
      toolDoneLines = [];
      streamFinalized = false;
      streamResult = ok
        ? await tryStream({ confirm: true })
        : await tryStream({ deny: true });
      if (streamResult?.quotaExceeded) {
        window.clearInterval(pendingTimer);
        pending.classList.remove("processing");
        if (pendingContent) pendingContent.innerHTML = markdownToHtml("已拒绝超配额操作。");
        return;
      }
    }

    const result = streamResult?.payload;
    if (!result) throw new Error("未收到回答");
    if (!result.trace_id && activeTraceId) result.trace_id = activeTraceId;
    const resolvedSessionId = String(result.session_id || requestSessionId || "").trim();
    if (currentSessionId === requestSessionId) currentSessionId = resolvedSessionId;

    // Show final tool summary in the pending row before removing it
    const finalDetail = buildPlanDetailsMarkdown(result, (Date.now() - pendingStart) / 1000);
    setRowContent(pending, finalDetail);
    pending.classList.remove("processing");
    window.clearInterval(pendingTimer);
    pending.remove();

    let finalText = String(result.answer || "").trim() || "未返回回答。";
    if (!hasReferenceSections(finalText)) {
      const refs = buildReferencesMarkdown(result.tool_results);
      if (refs) finalText += refs;
    }
    appendLocalMessage(resolvedSessionId, "assistant", finalText, result.trace_id);
    activeAgentStreamState = null;
    if (currentSessionId === resolvedSessionId) {
      renderCurrentAgentSessionView();
    }
  } catch (err) {
    streamFinalized = true;
    window.clearInterval(pendingTimer);
    pending.classList.remove("processing");
    pending.remove();
    if (err && err.name === "AbortError") {
      appendChatRow("assistant", "已中止当前请求。");
    } else {
      const msg = err instanceof Error ? err.message : String(err);
      appendChatRow("assistant", `**错误**: ${msg}`);
      appendLocalMessage(requestSessionId, "assistant", `**错误**: ${msg}`);
    }
  } finally {
    streamFinalized = true;
    window.clearInterval(pendingTimer);
    if (activeAgentStreamState && activeAgentStreamState.sessionId === requestSessionId) {
      activeAgentStreamState = null;
    }
    askInFlight = false;
    activeController = null;
    qaAsk.disabled = false;
    qaAskLocal.disabled = false;
    qaAbort.disabled = true;
  }
}

function abortAsk() {
  if (activeController) activeController.abort();
}

async function createSessionAction() {
  const created = await apiPost("/api/agent/sessions", { title: "新会话" });
  await refreshSessions(String(created.id || "").trim());
}

async function deleteCurrentSessionAction() {
  if (!currentSessionId) return;
  await apiDelete(`/api/agent/sessions/${encodeURIComponent(currentSessionId)}`);
  await refreshSessions("");
}

async function init() {
  await refreshSessions("");
  setModel();
  setMainTab("home");
  loadInitialCards();
  renderCustomCards();

  for (const tab of document.querySelectorAll(".tab")) {
    // Short tap → switch tab; long-press on usage-sensitive tabs → open usage modal
    const switchFn = () => {
      const target = tab.dataset.tab || "home";
      setMainTab(target);
      if (target === "dashboard" && !dashboardBootstrapped) {
        bootstrapDashboardTab().catch((err) => {
          renderDashboardError(err);
        });
      }
      if (target === "tickets" && !ticketsBootstrapped) {
        bootstrapTicketsTab().catch((err) => {
          showAppErrorModal("Tickets 初始化失败", String(err));
        });
      }
      if (target === "benchmark" && !benchmarkBootstrapped) {
        bootstrapBenchmarkTab().catch(() => {});
      }
    };
    const tabTarget = tab.dataset.tab || "home";
    if (tabTarget === "dashboard") {
      // Long-press on Dashboard tab → open usage modal
      bindLongPressElement(tab, openUsageModal, switchFn);
    } else {
      tab.onclick = switchFn;
    }
  }

  document.getElementById("agent-toggle-sidebar").onclick = () => toggleSidebar("agent-sidebar");
  updateSidebarToggleButton("agent-sidebar");
  document.getElementById("agent-new-session").onclick = () => {
    createSessionAction().catch((e) => appendChatRow("assistant", `**错误**: ${String(e)}`));
  };
  document.getElementById("agent-delete-session").onclick = () => {
    deleteCurrentSessionAction().catch((e) => appendChatRow("assistant", `**错误**: ${String(e)}`));
  };

  qaAsk.onclick = () => {
    ask("hybrid").catch((e) => appendChatRow("assistant", `**错误**: ${String(e)}`));
  };
  qaAskLocal.onclick = () => {
    ask("local_only").catch((e) => appendChatRow("assistant", `**错误**: ${String(e)}`));
  };
  qaAbort.onclick = abortAsk;
  qaInput.addEventListener("keydown", (event) => {
    if (event.key === "Enter" && !event.shiftKey) {
      event.preventDefault();
      ask("local_only").catch((e) => appendChatRow("assistant", `**错误**: ${String(e)}`));
    }
  });

  customCardUploadBtn?.addEventListener("click", () => customCardUploadInput?.click());
  customCardUploadInput?.addEventListener("change", () => {
    uploadCustomCardImage().catch((e) => window.alert(`上传失败: ${String(e)}`));
  });
  customCardImageInput?.addEventListener("input", () => setCardPreview(customCardImageInput.value));
  customCardSaveBtn?.addEventListener("click", () => {
    saveCustomCardFromModal().catch((e) => window.alert(`保存失败: ${String(e)}`));
  });
  customCardCancelBtn?.addEventListener("click", closeCustomCardModal);
  customCardModal?.addEventListener("click", (event) => {
    const target = event.target;
    if (target instanceof Element && target.getAttribute("data-role") === "custom-card-backdrop") {
      closeCustomCardModal();
    }
  });
  bindCropCanvasEvents();
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      if (warningsModal && !warningsModal.classList.contains("hidden")) closeWarningsModal();
      if (missingQueriesModal && !missingQueriesModal.classList.contains("hidden")) closeMissingQueriesModal();
      if (traceModal && !traceModal.classList.contains("hidden")) closeTraceModal();
      if (customCardModal && !customCardModal.classList.contains("hidden")) closeCustomCardModal();
      const usageModal = document.getElementById("usage-edit-modal");
      if (usageModal && !usageModal.classList.contains("hidden")) closeUsageModal();
      if (appErrorModal && !appErrorModal.classList.contains("hidden")) closeAppErrorModal();
    }
  });

  dashboardRefreshBtn?.addEventListener("click", () => {
    refreshDashboard({ force: true }).catch((err) => {
      renderDashboardError(err);
    });
  });

  document.getElementById("usage-save-btn")?.addEventListener("click", () => {
    saveUsage().catch((e) => window.alert(`保存失败: ${String(e)}`));
  });
  document.getElementById("usage-cancel-btn")?.addEventListener("click", closeUsageModal);
  document.getElementById("usage-edit-modal")?.addEventListener("click", (event) => {
    const target = event.target;
    if (target instanceof Element && target.getAttribute("data-role") === "usage-edit-backdrop") {
      closeUsageModal();
    }
  });

  // Warnings modal
  warningsClearBtn?.addEventListener("click", clearWarnings);
  warningsCloseBtn?.addEventListener("click", closeWarningsModal);
  missingQueriesClearBtn?.addEventListener("click", () => {
    clearMissingQueries().catch((e) => window.alert(`清除失败: ${String(e)}`));
  });
  missingQueriesExportBtn?.addEventListener("click", () => {
    exportMissingQueriesCsv().catch((e) => window.alert(`导出失败: ${String(e)}`));
  });
  missingQueriesSourceSelect?.addEventListener("change", () => {
    loadMissingQueries(missingQueriesSourceSelect.value)
      .then(() => {
        if (missingQueriesModalMeta) {
          missingQueriesModalMeta.textContent = `最近30天: ${formatNum(currentMissingQueries.length)} 条 | 来源: ${displaySourceLabel(currentMissingQueriesSource)}`;
        }
        if (missingQueriesModalList) {
          missingQueriesModalList.innerHTML = currentMissingQueries.length
            ? currentMissingQueries.map((row) => {
                const ts = escapeHtml(String(row?.ts || ""));
                const source = escapeHtml(String(row?.source_label || displaySourceLabel(row?.source || "unknown")));
                const query = escapeHtml(String(row?.query || ""));
                const top1 = row?.top1_score != null ? Number(row.top1_score).toFixed(4) : "—";
                const th = row?.threshold != null ? Number(row.threshold).toFixed(4) : "—";
                return `<li><strong>${ts}</strong> [${source}]<br/>${query}<br/><span class="dashboard-meta">top1=${top1}, threshold=${th}</span></li>`;
              }).join("")
            : "<li>最近30天暂无未命中 query</li>";
        }
      })
      .catch((e) => window.alert(`筛选失败: ${String(e)}`));
  });
  missingQueriesCloseBtn?.addEventListener("click", closeMissingQueriesModal);
  runtimeDataRefreshBtn?.addEventListener("click", () => {
    openRuntimeDataModal().catch((e) => window.alert(`刷新失败: ${String(e)}`));
  });
  runtimeDataClearBtn?.addEventListener("click", () => {
    clearRuntimeDataSelection().catch((e) => window.alert(`清除失败: ${String(e)}`));
  });
  runtimeDataCloseBtn?.addEventListener("click", closeRuntimeDataModal);
  dashboardTraceQueryBtn?.addEventListener("click", () => {
    lookupDashboardTrace().catch((e) => showAppErrorModal("Trace 查询失败", String(e)));
  });
  dashboardTraceOpenBtn?.addEventListener("click", () => {
    openTraceModal().catch((e) => showAppErrorModal("Trace 加载失败", String(e)));
  });
  dashboardTraceTicketBtn?.addEventListener("click", () => {
    openTicketFromCurrentTrace().catch((e) => showAppErrorModal("Ticket 生成失败", String(e)));
  });
  dashboardTraceInput?.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      event.preventDefault();
      lookupDashboardTrace().catch((e) => showAppErrorModal("Trace 查询失败", String(e)));
    }
  });
  traceModalCloseBtn?.addEventListener("click", closeTraceModal);
  traceModalExportBtn?.addEventListener("click", () => {
    exportCurrentTrace().catch((e) => showAppErrorModal("Trace 导出失败", String(e)));
  });
  const traceStageClickHandler = (event) => {
    const target = event.target;
    const segment = target instanceof Element ? target.closest("[data-trace-stage-key]") : null;
    const row = target instanceof Element ? target.closest("[data-trace-stage-row]") : null;
    const key = String(segment?.getAttribute("data-trace-stage-key") || row?.getAttribute("data-trace-stage-row") || "").trim();
    if (!key) return;
    const scope = target instanceof Element ? target.closest(".trace-stage-composite-wrap") : null;
    if (!scope) return;
    scope.querySelectorAll("[data-trace-stage-key], [data-trace-stage-row]").forEach((node) => {
      node.classList.toggle("is-active", String(node.getAttribute("data-trace-stage-key") || node.getAttribute("data-trace-stage-row") || "") === key);
    });
  };
  dashboardTraceResult?.addEventListener("click", traceStageClickHandler);
  traceModalContent?.addEventListener("click", traceStageClickHandler);
  warningsModal?.addEventListener("click", (event) => {
    const target = event.target;
    if (target instanceof Element && target.getAttribute("data-role") === "warnings-backdrop") {
      closeWarningsModal();
    }
  });
  missingQueriesModal?.addEventListener("click", (event) => {
    const target = event.target;
    if (target instanceof Element && target.getAttribute("data-role") === "missing-queries-backdrop") {
      closeMissingQueriesModal();
    }
  });
  runtimeDataModal?.addEventListener("click", (event) => {
    const target = event.target;
    if (target instanceof Element && target.getAttribute("data-role") === "runtime-data-backdrop") {
      closeRuntimeDataModal();
    }
  });
  traceModal?.addEventListener("click", (event) => {
    const target = event.target;
    if (target instanceof Element && target.getAttribute("data-role") === "trace-backdrop") {
      closeTraceModal();
    }
  });
  dashboardJobsRefreshBtn?.addEventListener("click", () => {
    refreshTaskCenter().catch((e) => window.alert(`任务刷新失败: ${String(e)}`));
  });
  dashboardJobsFilter?.addEventListener("change", () => {
    dashboardJobsView = String(dashboardJobsFilter.value || "active");
    refreshTaskCenter().catch((e) => window.alert(`任务筛选失败: ${String(e)}`));
  });
  ticketsRefreshBtn?.addEventListener("click", () => {
    refreshTickets({ keepSelection: true }).catch((e) => showAppErrorModal("Tickets 刷新失败", String(e)));
  });
  ticketsNewBtn?.addEventListener("click", () => {
    resetTicketEditor();
  });
  ticketsAIDraftBtn?.addEventListener("click", () => {
    createTicketAIDraft().catch((e) => showAppErrorModal("AI Ticket 草稿失败", String(e)));
  });
  ticketsSaveBtn?.addEventListener("click", () => {
    saveCurrentTicket().catch((e) => showAppErrorModal("Ticket 保存失败", String(e)));
  });
  ticketsDeleteBtn?.addEventListener("click", openTicketDeleteModal);
  ticketsList?.addEventListener("click", (event) => {
    const target = event.target;
    const traceBtn = target instanceof Element ? target.closest("[data-ticket-trace-open]") : null;
    if (traceBtn) {
      const traceId = String(traceBtn.getAttribute("data-ticket-trace-open") || "").trim();
      if (traceId) openTraceModal(traceId).catch((e) => showAppErrorModal("Trace 加载失败", String(e)));
      return;
    }
    const item = target instanceof Element ? target.closest("[data-ticket-id]") : null;
    if (!item) return;
    const ticketId = String(item.getAttribute("data-ticket-id") || "").trim();
    const ticket = currentTickets.find((entry) => String(entry.ticket_id || "") === ticketId);
    if (!ticket) return;
    applyTicketToForm(ticket);
    renderTicketsList();
  });
  ticketRelatedTracesLinks?.addEventListener("click", (event) => {
    const target = event.target;
    const button = target instanceof Element ? target.closest("[data-ticket-trace-open]") : null;
    if (!button) return;
    const traceId = String(button.getAttribute("data-ticket-trace-open") || "").trim();
    if (traceId) openTraceModal(traceId).catch((e) => showAppErrorModal("Trace 加载失败", String(e)));
  });
  [ticketsStatusFilter, ticketsPriorityFilter, ticketsDomainFilter, ticketsCategoryFilter, ticketsCreatedFrom, ticketsCreatedTo].forEach((node) => {
    node?.addEventListener("change", () => {
      refreshTickets({ keepSelection: false }).catch((e) => showAppErrorModal("Tickets 筛选失败", String(e)));
    });
  });
  ticketsSearchInput?.addEventListener("keydown", (event) => {
    if (event.key !== "Enter") return;
    event.preventDefault();
    refreshTickets({ keepSelection: false }).catch((e) => showAppErrorModal("Tickets 搜索失败", String(e)));
  });
  ticketRelatedTracesInput?.addEventListener("input", () => {
    renderTicketTraceLinks(ticketRelatedTracesInput.value || "");
  });
  ticketDeleteConfirmSelect?.addEventListener("change", () => {
    if (ticketDeleteConfirmBtn) ticketDeleteConfirmBtn.disabled = String(ticketDeleteConfirmSelect.value || "") !== "delete";
  });
  ticketsSortToggleBtn?.addEventListener("click", () => {
    currentTicketSort = currentTicketSort === "updated_asc" ? "updated_desc" : "updated_asc";
    renderTicketSortButton();
    refreshTickets({ keepSelection: false }).catch((e) => showAppErrorModal("Tickets 排序失败", String(e)));
  });
  ticketsListCollapseBtn?.addEventListener("click", () => {
    setTicketsListCollapsed(!ticketsListCollapsed);
  });
  window.addEventListener("resize", () => {
    renderTicketsListCollapseButton();
    updateSidebarToggleButton("agent-sidebar");
  });
  ticketDeleteConfirmBtn?.addEventListener("click", () => {
    confirmDeleteCurrentTicket().catch((e) => showAppErrorModal("Ticket 删除失败", String(e)));
  });
  ticketDeleteCancelBtn?.addEventListener("click", closeTicketDeleteModal);
  ticketDeleteModal?.addEventListener("click", (event) => {
    const target = event.target;
    if (target instanceof Element && target.getAttribute("data-role") === "ticket-delete-backdrop") {
      closeTicketDeleteModal();
    }
  });
  dashboardJobsList?.addEventListener("click", async (event) => {
    const target = event.target;
    const cancelBtn = target instanceof Element ? target.closest("[data-job-cancel-id]") : null;
    if (cancelBtn) {
      const jobId = String(cancelBtn.getAttribute("data-job-cancel-id") || "").trim();
      if (!jobId) return;
      try {
        await apiPost(`/api/dashboard/jobs/${encodeURIComponent(jobId)}/cancel`, {});
        await refreshTaskCenter();
      } catch (e) {
        window.alert(`取消任务失败: ${String(e)}`);
      }
      return;
    }
    const card = target instanceof Element ? target.closest("[data-job-id]") : null;
    if (!card) return;
    selectedTaskJobId = String(card.getAttribute("data-job-id") || "");
    renderTaskCenter();
  });
  // Long-press on warnings/usage cards to open their modals
  if (dashboardGrid) bindLongPress(dashboardGrid, (target) => {
    const warningsCard = target.closest("[data-role='warnings-summary']");
    if (warningsCard) { openWarningsModal(); return; }
    const missingQueriesCard = target.closest("[data-role='missing-queries-summary']");
    if (missingQueriesCard) {
      openMissingQueriesModal().catch((e) => window.alert(`加载失败: ${String(e)}`));
      return;
    }
    const feedbackCard = target.closest("[data-role='feedback-summary']");
    if (feedbackCard) {
      openFeedbackModal().catch((e) => window.alert(`加载失败: ${String(e)}`));
      return;
    }
    const runtimeDataCard = target.closest("[data-role='runtime-data-summary']");
    if (runtimeDataCard) {
      openRuntimeDataModal().catch((e) => window.alert(`加载失败: ${String(e)}`));
      return;
    }
    const libraryGraphCard = target.closest("[data-role='library-graph-summary']");
    if (libraryGraphCard) {
      triggerLibraryGraphRebuild().catch((e) => window.alert(`触发失败: ${String(e)}`));
      return;
    }
    const usageCard = target.closest("[data-role='web-search-usage'],[data-role='deepseek-usage']");
    if (usageCard) openUsageModal();
    const ragPendingCard = target.closest("[data-role='rag-changed-pending']");
    if (ragPendingCard) triggerRagSync();
  });

  // Benchmark
  document.getElementById("bm-run-btn")?.addEventListener("click", runBenchmark);
  document.getElementById("bm-abort-btn")?.addEventListener("click", async () => {
    if (!activeBenchmarkJobId) return;
    try {
      await apiPost(`/api/benchmark/jobs/${encodeURIComponent(activeBenchmarkJobId)}/cancel`, {});
    } catch (_err) {
      // Ignore cancel race with completed jobs.
    }
  });
  benchmarkCaseTraceRefreshBtn?.addEventListener("click", () => {
    refreshBenchmarkLatestCase().catch((e) => showAppErrorModal("刷新最新 case 失败", String(e), "可直接复制下面的报错内容"));
  });
  document.getElementById("bm-case-trace-list")?.addEventListener("click", (event) => {
    const target = event.target;
    const btn = target instanceof Element ? target.closest("[data-trace-open]") : null;
    if (!btn) return;
    const traceId = String(btn.getAttribute("data-trace-open") || "").trim();
    if (!traceId) return;
    openTraceModal(traceId).catch((e) => showAppErrorModal("Trace 加载失败", String(e)));
  });
  appErrorCopyBtn?.addEventListener("click", () => {
    copyAppErrorText().catch((e) => showAppErrorModal("复制失败", String(e)));
  });
  appErrorCloseBtn?.addEventListener("click", closeAppErrorModal);
  appErrorModal?.addEventListener("click", (event) => {
    const target = event.target;
    if (target instanceof Element && target.getAttribute("data-role") === "app-error-backdrop") closeAppErrorModal();
  });
  feedbackSourceSelect?.addEventListener("change", () => {
    openFeedbackModal().catch((e) => window.alert(`加载失败: ${String(e)}`));
  });
  feedbackExportBtn?.addEventListener("click", () => {
    exportFeedbackJson().catch((e) => window.alert(`导出失败: ${String(e)}`));
  });
  feedbackClearBtn?.addEventListener("click", () => {
    clearFeedback().catch((e) => window.alert(`清空失败: ${String(e)}`));
  });
  feedbackCloseBtn?.addEventListener("click", closeFeedbackModal);
  sessionRenameSaveBtn?.addEventListener("click", () => {
    saveSessionRename().catch((e) => window.alert(`重命名失败: ${String(e)}`));
  });
  sessionRenameCancelBtn?.addEventListener("click", closeSessionRenameModal);
  sessionRenameInput?.addEventListener("keydown", (event) => {
    if (event.key !== "Enter") return;
    event.preventDefault();
    saveSessionRename().catch((e) => window.alert(`重命名失败: ${String(e)}`));
  });
  feedbackModal?.addEventListener("click", (event) => {
    const target = event.target;
    if (!(target instanceof Element)) return;
    if (target.dataset.role === "feedback-backdrop") closeFeedbackModal();
  });
  sessionRenameModal?.addEventListener("click", (event) => {
    const target = event.target;
    if (!(target instanceof Element)) return;
    if (target.dataset.role === "session-rename-backdrop") closeSessionRenameModal();
  });
  feedbackModalList?.addEventListener("click", (event) => {
    const target = event.target;
    const item = target instanceof Element ? target.closest("[data-feedback-id]") : null;
    if (!item) return;
    const feedbackId = String(item.getAttribute("data-feedback-id") || "").trim();
    const row = currentFeedbackItems.find((entry) => String(entry.id || "") === feedbackId);
    if (row) showFeedbackDetail(row);
  });
  if (feedbackModalList) bindLongPress(feedbackModalList, (target) => {
    const item = target.closest("[data-feedback-id]");
    if (!item) return;
    const feedbackId = String(item.getAttribute("data-feedback-id") || "").trim();
    const row = currentFeedbackItems.find((entry) => String(entry.id || "") === feedbackId);
    if (row) showFeedbackDetail(row);
  });
  const sessionList = document.getElementById("agent-session-list");
  sessionList?.addEventListener("contextmenu", (event) => {
    const target = event.target;
    const item = target instanceof Element ? target.closest("[data-session-id]") : null;
    if (!item) return;
    event.preventDefault();
    openSessionRenameModalFromTarget(item);
  });
  if (sessionList) bindLongPress(sessionList, (target) => {
    openSessionRenameModalFromTarget(target);
  });

  wireChatLinks();
}

init().catch((err) => {
  appendChatRow("assistant", `**初始化失败**: ${String(err)}`);
});