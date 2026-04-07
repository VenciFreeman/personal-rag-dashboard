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
// Global error visibility: show a visible modal for uncaught errors and
// unhandled promise rejections so silent failures don't go unnoticed.
window.addEventListener("error", function (e) {
  console.error("[global] uncaught error:", e.error || e.message, (e.filename || "") + ":" + e.lineno);
});
window.addEventListener("unhandledrejection", function (e) {
  console.error("[global] unhandled rejection:", e.reason);
});
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
const dashboardBackupTime = document.getElementById("dashboard-backup-time");
const dashboardLoadStatus = document.getElementById("dashboard-load-status");
const dashboardInformationNotices = document.getElementById("dashboard-information-notices");
const dashboardRefreshBtn = document.getElementById("dashboard-refresh");
const dashboardLatencyTable = document.getElementById("dashboard-latency-table");
const dashboardObservabilityTable = document.getElementById("dashboard-observability-table");
const dashboardStartupLogs = document.getElementById("dashboard-startup-logs");
const dashboardJobsList = document.getElementById("dashboard-jobs-list");
const dashboardJobsClearBtn = document.getElementById("dashboard-jobs-clear-history");
const dashboardJobsRefreshBtn = document.getElementById("dashboard-jobs-refresh");
const dashboardTicketSummaryMeta = document.getElementById("dashboard-ticket-summary-meta");
const dashboardTicketSummaryBody = document.getElementById("dashboard-ticket-summary-body");
const dashboardTicketTrendMeta = document.getElementById("dashboard-ticket-trend-meta");
const dashboardTicketTrendModeSelect = document.getElementById("dashboard-ticket-trend-mode");
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
const feedbackDetailModal = document.getElementById("feedback-detail-modal");
const feedbackDetailMeta = document.getElementById("feedback-detail-meta");
const feedbackDetailContent = document.getElementById("feedback-detail-content");
const feedbackDetailOpenTraceBtn = document.getElementById("feedback-detail-open-trace-btn");
const feedbackDetailCloseBtn = document.getElementById("feedback-detail-close-btn");
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
const dataBackupMeta = document.getElementById("data-backup-meta");
const dataBackupSummary = document.getElementById("data-backup-summary");
const dataBackupExportBtn = document.getElementById("data-backup-export-btn");
const dataBackupCreateBtn = document.getElementById("data-backup-create-btn");
const dataBackupRestoreBtn = document.getElementById("data-backup-restore-btn");
const dataBackupRestoreInput = document.getElementById("data-backup-restore-input");
const usageModalMeta = document.getElementById("usage-modal-meta");
const usageProviderSelect = document.getElementById("usage-provider-select");
const usageTraceList = document.getElementById("usage-trace-list");
const usageExportBtn = document.getElementById("usage-export-btn");
const usageClearBtn = document.getElementById("usage-clear-btn");
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
const libraryAliasModal = document.getElementById("library-alias-modal");
const libraryAliasModalMeta = document.getElementById("library-alias-modal-meta");
const libraryAliasReviewList = document.getElementById("library-alias-review-list");
const libraryAliasPageInfo = document.getElementById("library-alias-page-info");
const libraryAliasPagePrevBtn = document.getElementById("library-alias-page-prev");
const libraryAliasPageNextBtn = document.getElementById("library-alias-page-next");
const libraryAliasCloseBtn = document.getElementById("library-alias-close-btn");
const appErrorModal = document.getElementById("app-error-modal");
const appErrorTitle = document.getElementById("app-error-title");
const appErrorMeta = document.getElementById("app-error-meta");
const appErrorCopybox = document.getElementById("app-error-copybox");
const appErrorCopyBtn = document.getElementById("app-error-copy-btn");
const appErrorCloseBtn = document.getElementById("app-error-close-btn");
const benchmarkCaseTraceRefreshBtn = document.getElementById("bm-case-trace-refresh");
const benchmarkRouterClsAddBtn = document.getElementById("bm-router-cls-add");
const benchmarkRouterClsModal = document.getElementById("bm-router-cls-modal");
const benchmarkRouterClsModalTitle = document.getElementById("bm-router-cls-modal-title");
const benchmarkRouterClsModalMeta = document.getElementById("bm-router-cls-modal-meta");
const benchmarkRouterClsTraceIdInput = document.getElementById("bm-router-cls-trace-id");
const benchmarkRouterClsImportBtn = document.getElementById("bm-router-cls-import");
const benchmarkRouterClsQueryInput = document.getElementById("bm-router-cls-query");
const benchmarkRouterClsExpectedDomainInput = document.getElementById("bm-router-cls-expected-domain");
const benchmarkRouterClsExpectedArbitrationInput = document.getElementById("bm-router-cls-expected-arbitration");
const benchmarkRouterClsNoteInput = document.getElementById("bm-router-cls-note");
const benchmarkRouterClsMockLabelInput = document.getElementById("bm-router-cls-mock-label");
const benchmarkRouterClsMockDomainInput = document.getElementById("bm-router-cls-mock-domain");
const benchmarkRouterClsMockLookupModeInput = document.getElementById("bm-router-cls-mock-lookup-mode");
const benchmarkRouterClsMockEntitiesInput = document.getElementById("bm-router-cls-mock-entities");
const benchmarkRouterClsLastRunBox = document.getElementById("bm-router-cls-last-run-box");
const benchmarkRouterClsSaveBtn = document.getElementById("bm-router-cls-save");
const benchmarkRouterClsDeleteBtn = document.getElementById("bm-router-cls-delete");
const benchmarkRouterClsCancelBtn = document.getElementById("bm-router-cls-cancel");
const authorizationMeta = document.getElementById("authorization-meta");
const authorizationRefreshBtn = document.getElementById("authorization-refresh-btn");
const authorizationReauthShell = document.getElementById("authorization-reauth-shell");
const authorizationPasswordInput = document.getElementById("authorization-password-input");
const authorizationUnlockBtn = document.getElementById("authorization-unlock-btn");
const authorizationReauthError = document.getElementById("authorization-reauth-error");
const authorizationBootstrapShell = document.getElementById("authorization-bootstrap-shell");
const authorizationBootstrapUsername = document.getElementById("authorization-bootstrap-username");
const authorizationBootstrapPassword = document.getElementById("authorization-bootstrap-password");
const authorizationBootstrapBtn = document.getElementById("authorization-bootstrap-btn");
const authorizationBootstrapError = document.getElementById("authorization-bootstrap-error");
const authorizationAdminShell = document.getElementById("authorization-admin-shell");
const authorizationSessionMeta = document.getElementById("authorization-session-meta");
const authorizationCreateUsername = document.getElementById("authorization-create-username");
const authorizationCreatePassword = document.getElementById("authorization-create-password");
const authorizationCreateRole = document.getElementById("authorization-create-role");
const authorizationCreateActive = document.getElementById("authorization-create-active");
const authorizationCreateApps = document.getElementById("authorization-create-apps");
const authorizationCreateBtn = document.getElementById("authorization-create-btn");
const authorizationUsersList = document.getElementById("authorization-users-list");
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
const ticketsDetailShell = document.getElementById("tickets-detail-shell");
const ticketsList = document.getElementById("tickets-list");
const ticketIdInput = document.getElementById("ticket-id");
const ticketTraceIdInput = document.getElementById("ticket-trace-id");
const ticketTitleInput = document.getElementById("ticket-title");
const ticketPasteInput = document.getElementById("ticket-paste-input");
const ticketPasteFillBtn = document.getElementById("ticket-paste-fill-btn");
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
const dashboardJobContextMenu = document.getElementById("dashboard-job-context-menu");
const dashboardJobContextDeleteBtn = document.getElementById("dashboard-job-context-delete");
const taskDeleteModal = document.getElementById("task-delete-modal");
const taskDeleteMeta = document.getElementById("task-delete-meta");
const taskDeleteHint = document.getElementById("task-delete-hint");
const taskDeleteConfirmBtn = document.getElementById("task-delete-confirm-btn");
const taskDeleteCancelBtn = document.getElementById("task-delete-cancel-btn");
const pageLocalModel = (document.body?.dataset?.localModel || "").trim();
const SOURCE_LABELS = Object.freeze({
  agent: "LLM Agent",
  rag_qa: "RAG 问答",
  rag_qa_stream: "RAG 问答（流式）",
  benchmark_rag: "Benchmark / RAG",
  benchmark_agent: "Benchmark / Agent",
  agent_chat: "LLM Agent",
  rag_chat: "RAG 问答",
});

function isLoopbackHostname(hostname) {
  const value = String(hostname || "").trim().toLowerCase();
  return value === "localhost" || value === "127.0.0.1" || value === "::1";
}

function rewriteLoopbackServiceUrl(rawUrl, fallbackPort) {
  const explicit = String(rawUrl || "").trim();
  if (!explicit) return "";
  try {
    const parsed = new URL(explicit, window.location.origin);
    if (!/^https?:$/.test(parsed.protocol) || !isLoopbackHostname(parsed.hostname)) return explicit;
    const currentHostname = String(window.location?.hostname || "").trim();
    if (!currentHostname) return explicit;
    parsed.protocol = String(window.location?.protocol || parsed.protocol || "http:").trim() || parsed.protocol || "http:";
    parsed.hostname = currentHostname;
    const targetPort = Number(parsed.port || fallbackPort || 0);
    if (targetPort > 0) parsed.port = String(targetPort);
    return parsed.toString();
  } catch (_) {
    return explicit;
  }
}

function deriveServiceUrl(explicitUrl, fallbackPort) {
  const explicit = String(explicitUrl || "").trim();
  if (explicit) return rewriteLoopbackServiceUrl(explicit, fallbackPort) || explicit;
  const scheme = String(window.location?.protocol || "http:").trim() || "http:";
  const host = String(window.location?.host || "").trim();
  if (host) {
    const hostname = String(window.location?.hostname || "").trim();
    const currentPort = Number(window.location?.port || 0);
    const targetPort = Number(fallbackPort);
    const netloc = hostname && currentPort === targetPort ? host : `${hostname || host}:${targetPort}`;
    return `${scheme}//${netloc}/`;
  }
  return `${scheme}//localhost:${Number(fallbackPort)}/`;
}

function rewriteLoopbackLinksInContainer(container) {
  if (!(container instanceof Element)) return;
  const currentHostname = String(window.location?.hostname || "").trim();
  if (!currentHostname) return;
  container.querySelectorAll('a[href]').forEach((anchor) => {
    const rawHref = String(anchor.getAttribute("href") || "").trim();
    if (!rawHref) return;
    try {
      const parsed = new URL(rawHref, window.location.origin);
      if (!/^https?:$/.test(parsed.protocol) || !isLoopbackHostname(parsed.hostname)) return;
      parsed.protocol = String(window.location?.protocol || parsed.protocol || "http:").trim() || parsed.protocol || "http:";
      parsed.hostname = currentHostname;
      anchor.href = parsed.toString();
    } catch (_) {}
  });
}

const pageAiSummaryUrl = deriveServiceUrl(document.body?.dataset?.aiSummaryUrl, 8000);
const pageLibraryUrl = deriveServiceUrl(document.body?.dataset?.libraryUrl, 8091);
const MAX_REFERENCE_ITEMS = 10;
const uiDebugStatus = document.getElementById("ui-debug-status");
const UI_DEBUG_ENABLED = (() => {
  try {
    const p = new URLSearchParams(window.location.search || "");
    if (p.get("debug_ui") === "1") return true;
    if (p.get("debug_ui") === "0") return false;
  } catch (_) {}
  const host = String(window.location?.hostname || "").trim().toLowerCase();
  return host === "localhost" || host === "127.0.0.1";
})();
let uiDebugProbeInstalled = false;

function debugUiEvent(label, detail = "") {
  if (!UI_DEBUG_ENABLED || !(uiDebugStatus instanceof HTMLElement)) return;
  const ts = new Date().toLocaleTimeString();
  const info = String(detail || "").trim();
  uiDebugStatus.textContent = info ? `[${ts}] ${label} | ${info}` : `[${ts}] ${label}`;
}

function initUiDebugStatus() {
  if (!(uiDebugStatus instanceof HTMLElement)) return;
  if (!UI_DEBUG_ENABLED) {
    uiDebugStatus.classList.add("hidden");
    return;
  }
  uiDebugStatus.classList.remove("hidden");
  debugUiEvent("ui debug ready", "append ?debug_ui=0 to disable");
}

function installUiClickProbe() {
  if (!UI_DEBUG_ENABLED || uiDebugProbeInstalled) return;
  uiDebugProbeInstalled = true;
  document.addEventListener("click", (event) => {
    const t = event.target instanceof Element ? event.target : null;
    if (!t) return;
    const top = document.elementFromPoint(event.clientX, event.clientY);
    const targetText = `${t.tagName.toLowerCase()}#${t.id || "-"}.${(t.className || "").toString().trim().replace(/\s+/g, ".") || "-"}`;
    const hitText = top instanceof Element
      ? `${top.tagName.toLowerCase()}#${top.id || "-"}.${(top.className || "").toString().trim().replace(/\s+/g, ".") || "-"}`
      : "none";
    debugUiEvent("click probe", `target=${targetText} | hit=${hitText}`);
  }, true);
}

let activeController = null;
let askInFlight = false;
let dashboardRefreshInFlight = false;
let dashboardHasFullOverview = false;
let dashboardLastFullRefreshAt = 0;
// Per-tab state objects: handlersBound is managed by _register* guards,
// loading/loaded/error drive the data-load state machine.
const dashboardState = { loading: false, loaded: false, error: null };
const ticketsState   = { loading: false, loaded: false, error: null };
const benchmarkState = { loading: false, loaded: false, error: null };
const authorizationState = { loading: false, loaded: false, error: null, token: "", expiresAt: "", data: null };
let sessionsCache = [];
let currentSessionId = "";
let activeAgentStreamState = null;
const DASHBOARD_CACHE_STORAGE_KEY = "navDashboardOverviewCache";
const DASHBOARD_FULL_OVERVIEW_TIMEOUT_MS = 120000;
const DASHBOARD_FULL_REFRESH_TTL_MS = 120000;

// Warnings state
let currentWarnings = [];

function basenameFromPath(rawPath) {
  const text = String(rawPath || "").trim();
  if (!text) return "";
  const parts = text.split(/[\\/]+/).filter(Boolean);
  return parts.length ? parts[parts.length - 1] : text;
}
let currentWarningsTimestamp = "";
let dismissedWarnings = new Set();
let currentMissingQueries = [];
let currentMissingQueriesSource = "all";
let currentFeedbackItems = [];
let currentFeedbackSource = "all";
let currentFeedbackDetailItem = null;
let currentRuntimeDataItems = [];
let currentRuntimeDataSummary = {};
let currentDataBackupSummary = {};
let currentTraceRecord = null;
let currentTraceExportText = "";
let currentUsageTraceProvider = "all";
let currentUsageTraceItems = [];
let currentLibraryAliasItems = [];
let currentLibraryAliasPage = 1;
let currentLibraryAliasTotalPages = 1;
let currentLibraryAliasSummary = {};
let activeLibraryAliasEditProposalId = "";
let activeLibraryAliasSaveProposalId = "";
let currentLibraryAliasEditDraft = { aliasesText: "" };
let currentDashboardData = null;
let suppressSessionClickUntil = 0;
let currentTaskJobs = [];
let currentTaskJobsAll = [];
let currentTaskContextJobId = "";
let selectedTaskJobId = "";
let pendingTaskDeleteMode = "";
let pendingDeleteTaskJobId = "";
let taskLogFollowFrame = 0;
let dashboardTicketTrendChartInstance = null;
let lastDashboardForceRefreshAt = 0;
let lastReportJobRefreshMarker = "";
let hadRunningReportJob = false;
let currentTickets = [];
let currentTicketId = "";
let pendingDeleteTicketId = "";
let currentTicketSort = "updated_desc";
let currentTicketStatusFilter = "non_closed";
let ticketsListCollapsed = false;
let ticketsDetailResizeObserver = null;
const TICKETS_LIST_COLLAPSED_STORAGE_KEY = "navDashboardTicketsListCollapsed";

// Startup polling
let lastStartupStatus = "";
let startupPollInterval = null;
let taskCenterPollInterval = null;

// First-paint bootstrap state shared with the SSR template. Keep all early
// handoff fields under one object so the template and JS bundle do not drift.
const NavBootstrap = window.__navDashboardBootstrap || null;

function consumePendingCardEditIndex() {
  const value = NavBootstrap?.consumeCardEdit?.();
  return Number.isInteger(value) ? value : null;
}

const AppShell = window.CoreUI?.get?.("appShell") || window.SharedAppShell || null;
const DashboardTraceModule = window.NavDashboardTrace || null;
const DashboardTraceBootstrapModule = window.NavDashboardTraceBootstrap || null;
const DashboardLibraryAliasModule = window.NavDashboardLibraryAlias || null;
const DashboardTicketsModule = window.NavDashboardTickets || null;
const DashboardTicketsBootstrapModule = window.NavDashboardTicketsBootstrap || null;
const DashboardBenchmarkModule = window.NavDashboardBenchmark || null;
const DashboardBenchmarkBootstrapModule = window.NavDashboardBenchmarkBootstrap || null;
const DashboardDataAdminModule = window.NavDashboardDataAdmin || null;
const DashboardDataAdminBootstrapModule = window.NavDashboardDataAdminBootstrap || null;
const DashboardSharedDom = window.NavDashboardSharedDom || null;
const DashboardSharedModal = window.NavDashboardSharedModal || null;
const DashboardSharedApi = window.NavDashboardSharedApi || null;
const DashboardSharedMarkdown = window.NavDashboardSharedMarkdown || null;
const DashboardSharedTrace = window.NavDashboardSharedTrace || null;
const DashboardOverviewBootstrapModule = window.NavDashboardOverviewBootstrap || null;
const DashboardCustomCardsBootstrapModule = window.NavDashboardCustomCardsBootstrap || null;
const DashboardUsageBootstrapModule = window.NavDashboardUsageBootstrap || null;
const DashboardModalBootstrapModule = window.NavDashboardModalBootstrap || null;
const DashboardTaskCenterBootstrapModule = window.NavDashboardTaskCenterBootstrap || null;
const DashboardHandlersBootstrapModule = window.NavDashboardHandlersBootstrap || null;
const DashboardAgentSessionsBootstrapModule = window.NavDashboardAgentSessionsBootstrap || null;
const DashboardAgentSessionUiBootstrapModule = window.NavDashboardAgentSessionUiBootstrap || null;
const escapeHtml = DashboardSharedDom?.escapeHtml || ((text) => String(text || "")
  .replaceAll("&", "&amp;")
  .replaceAll("<", "&lt;")
  .replaceAll(">", "&gt;"));
const traceModalController = AppShell?.createModalController?.({ root: traceModal, hiddenClass: "hidden" }) || null;
const libraryAliasModalController = AppShell?.createModalController?.({ root: libraryAliasModal, hiddenClass: "hidden" }) || null;
const ticketDeleteModalController = AppShell?.createModalController?.({ root: ticketDeleteModal, hiddenClass: "hidden" }) || null;
const taskDeleteModalController = AppShell?.createModalController?.({ root: taskDeleteModal, hiddenClass: "hidden" }) || null;
const benchmarkRouterClsModalController = AppShell?.createModalController?.({ root: benchmarkRouterClsModal, hiddenClass: "hidden" }) || null;
let ticketsShellController = null;
ticketsShellController = AppShell?.createListDetailShellController?.({
  container: ticketsMainGrid,
  listPane: ticketsListShell,
  toggleButton: ticketsListCollapseBtn,
  compactMediaQuery: "(max-width: 980px)",
  collapsedClass: "is-list-collapsed",
  listCollapsedClass: "is-collapsed",
  storageKey: TICKETS_LIST_COLLAPSED_STORAGE_KEY,
  onLayoutChange: ({ collapsed }) => {
    ticketsListCollapsed = !!collapsed;
    syncTicketsPaneHeights();
  },
}) || null;

function reportMissingFrontendModule(moduleName) {
  const label = String(moduleName || "frontend module").trim() || "frontend module";
  console.error(`[nav_dashboard] ${label} is unavailable during app bootstrap`);
}

function createMissingController(moduleName, syncMethods = [], asyncMethods = [], overrides = {}) {
  let warned = false;
  const warn = () => {
    if (warned) return;
    warned = true;
    reportMissingFrontendModule(moduleName);
  };
  const controller = { ...(overrides || {}) };
  syncMethods.forEach((methodName) => {
    if (typeof controller[methodName] === "function") return;
    controller[methodName] = () => {
      warn();
      return undefined;
    };
  });
  asyncMethods.forEach((methodName) => {
    if (typeof controller[methodName] === "function") return;
    controller[methodName] = async () => {
      warn();
      throw new Error(`${String(moduleName || "frontend module")} unavailable`);
    };
  });
  return controller;
}

function showToast(message, options = undefined) {
  if (AppShell?.showToast) {
    AppShell.showToast(message, {
      id: "nav-dashboard-toast",
      className: "nav-dashboard-toast hidden",
      hiddenClass: "hidden",
      duration: 2200,
      ...(options || {}),
    });
    return;
  }
  window.alert(String(message || ""));
}

function autoSizeReadonlyTextarea(textarea) {
  if (!(textarea instanceof HTMLTextAreaElement)) return;
  textarea.style.height = "auto";
  textarea.style.height = `${Math.max(textarea.scrollHeight, 120)}px`;
}

function closeAppErrorModal() {
  if (DashboardSharedModal?.closeModal) {
    DashboardSharedModal.closeModal(appErrorModal, null);
  } else if (appErrorModal) {
    appErrorModal.classList.add("hidden");
    appErrorModal.setAttribute("aria-hidden", "true");
  }
}

function showAppErrorModal(title, message, detail = "") {
  const heading = String(title || "操作提示").trim() || "操作提示";
  const summary = String(message || "").trim();
  const extra = String(detail || "").trim();
  if (appErrorTitle) appErrorTitle.textContent = heading;
  if (appErrorMeta) appErrorMeta.textContent = summary;
  if (appErrorCopybox instanceof HTMLTextAreaElement) {
    appErrorCopybox.value = [heading, summary, extra].filter(Boolean).join("\n\n");
    autoSizeReadonlyTextarea(appErrorCopybox);
  }
  if (DashboardSharedModal?.openModal) {
    DashboardSharedModal.openModal(appErrorModal, null);
    return;
  }
  if (appErrorModal) {
    appErrorModal.classList.remove("hidden");
    appErrorModal.setAttribute("aria-hidden", "false");
  }
}

async function copyAppErrorText() {
  const text = String(appErrorCopybox?.value || "");
  if (!text) return;
  if (navigator.clipboard?.writeText) {
    await navigator.clipboard.writeText(text);
    showToast("错误信息已复制");
    return;
  }
  if (appErrorCopybox instanceof HTMLTextAreaElement) {
    appErrorCopybox.focus();
    appErrorCopybox.select();
    document.execCommand("copy");
    showToast("错误信息已复制");
  }
}

function createContextMenuController(menu, options = {}) {
  const fallbackWidth = Number(options.width || 160);
  const fallbackHeight = Number(options.height || 60);
  let autoCloseBound = false;
  return {
    open(x, y) {
      if (!menu) return;
      menu.classList.remove("hidden");
      menu.setAttribute("aria-hidden", "false");
      const rect = menu.getBoundingClientRect();
      const width = Math.max(fallbackWidth, Number(rect.width || 0));
      const height = Math.max(fallbackHeight, Number(rect.height || 0));
      menu.style.left = `${Math.min(Number(x || 0), window.innerWidth - width - 8)}px`;
      menu.style.top = `${Math.min(Number(y || 0), window.innerHeight - height - 8)}px`;
    },
    close() {
      if (!menu) return;
      menu.classList.add("hidden");
      menu.setAttribute("aria-hidden", "true");
    },
    isOpen() {
      return !!(menu && !menu.classList.contains("hidden"));
    },
    bindAutoClose() {
      if (autoCloseBound) return;
      autoCloseBound = true;
      document.addEventListener("click", (event) => {
        if (!(event.target instanceof Element) || !menu?.contains(event.target)) {
          this.close();
        }
      });
      document.addEventListener("scroll", () => this.close(), true);
      window.addEventListener("keydown", (event) => {
        if (event.key === "Escape") this.close();
      });
      window.addEventListener("resize", () => this.close());
    },
  };
}

const dashboardJobContextMenuController = createContextMenuController(dashboardJobContextMenu, {
  width: 176,
  height: 64,
});
dashboardJobContextMenuController.bindAutoClose();

function summarizeAssertionScope(scope) {
  const checks = Array.isArray(scope?.checks) ? scope.checks : [];
  if (!checks.length) return '<span style="color:#7a7f6f">—</span>';
  const passedCount = checks.filter((item) => item?.passed).length;
  const failedCount = checks.length - passedCount;
  const tone = failedCount > 0 ? "#d08770" : "#9ecb8b";
  return `<span style="color:${tone}">${passedCount}/${checks.length}${failedCount > 0 ? `，失败 ${failedCount}` : "，全部通过"}</span>`;
}

function summarizeAssertionFailures(scope, title = "失败项") {
  const failed = (Array.isArray(scope?.checks) ? scope.checks : []).filter((item) => item && item.passed === false);
  if (!failed.length) return '<span style="color:#7a7f6f">无</span>';
  const preview = failed.slice(0, 3).map((item) => {
    const name = escapeHtml(String(item?.name || "check"));
    const actual = escapeHtml(String(item?.actual ?? "—"));
    const expected = escapeHtml(String(item?.expected ?? "—"));
    return `<span class="bm-assert-fail-item"><span>${name}</span><span class="bm-assert-fail-more">${actual} / ${expected}</span></span>`;
  }).join(" ");
  const more = failed.length > 3 ? ` <span class="bm-assert-fail-more">+${failed.length - 3} more</span>` : "";
  return `<div class="dashboard-meta">${escapeHtml(title)}: ${preview}${more}</div>`;
}

function summarizeBenchmarkContractFailures(records) {
  const failures = (Array.isArray(records) ? records : [])
    .flatMap((record) => Array.isArray(record?.contract_checks) ? record.contract_checks.map((check) => ({ record, check })) : [])
    .filter((entry) => entry.check && entry.check.passed === false);
  if (!failures.length) return '<span style="color:#7a7f6f">无</span>';
  const preview = failures.slice(0, 3).map(({ record, check }) => {
    const name = escapeHtml(String(check?.name || "contract"));
    const traceId = escapeHtml(String(record?.trace_id || "—"));
    return `<span class="bm-assert-fail-item"><span>${name}</span><span class="bm-assert-fail-more">trace ${traceId}</span></span>`;
  }).join(" ");
  const more = failures.length > 3 ? ` <span class="bm-assert-fail-more">+${failures.length - 3} more</span>` : "";
  return `<div class="dashboard-meta">Contract: ${preview}${more}</div>`;
}

function renderBenchmarkContractFailureSummary(record, options = {}) {
  const title = String(options?.title || "Contract 诊断");
  const checks = Array.isArray(record?.contract_checks) ? record.contract_checks : [];
  if (!checks.length) {
    return `<span style="color:#7a7f6f">${escapeHtml(title)}: 暂无 contract 检查</span>`;
  }
  const failed = checks.filter((item) => item && item.passed === false);
  if (!failed.length) {
    return `<span style="color:#9ecb8b">${escapeHtml(title)}: 全部通过</span>`;
  }
  const items = failed.map((item) => {
    const name = escapeHtml(String(item?.name || "contract"));
    const actual = escapeHtml(String(item?.actual ?? "—"));
    const expected = escapeHtml(String(item?.expected ?? "—"));
    return `<span class="bm-assert-fail-item"><span>${name}</span><span class="bm-assert-fail-more">${actual} / ${expected}</span></span>`;
  }).join(" ");
  return `<div class="dashboard-meta">${escapeHtml(title)}: ${items}</div>`;
}

function syncBenchmarkLogBox(logBox, logs, markerState = {}, { reset = false } = {}) {
  if (!logBox) return;
  const normalized = Array.isArray(logs) ? logs.map((line) => String(line || "")) : [];
  const lastMarker = String(markerState?.marker || "");
  if (reset || !lastMarker) {
    logBox.textContent = normalized.length ? `${normalized.join("\n")}\n` : "";
    logBox.scrollTop = logBox.scrollHeight;
    return { marker: normalized.length ? normalized[normalized.length - 1] : "", count: normalized.length };
  }

  const markerIndex = normalized.lastIndexOf(lastMarker);
  if (markerIndex === -1) {
    logBox.textContent = normalized.length ? `${normalized.join("\n")}\n` : "";
  } else if (markerIndex < normalized.length - 1) {
    logBox.textContent += `${normalized.slice(markerIndex + 1).join("\n")}\n`;
  }

  logBox.scrollTop = logBox.scrollHeight;
  return { marker: normalized.length ? normalized[normalized.length - 1] : "", count: normalized.length };
}

function jobTypeLabel(type) {
  const labels = {
    benchmark: "Benchmark",
    rag_sync: "RAG 同步",
    library_graph_rebuild: "Library Graph",
    report_generation: "报告生成",
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

function renderTaskCard(job, { selected = false, preview = false } = {}) {
  const status = String(job?.status || "queued");
  const selectedCls = selected ? " is-selected" : "";
  const previewCls = preview ? " is-preview" : "";
  const runningCls = status === "running" ? " is-running" : status === "failed" ? " is-failed" : status === "cancelled" ? " is-cancelled" : "";
  const summary = String(job?.message || "等待开始");
  const moduleMeta = Array.isArray(job?.metadata?.modules)
    ? job.metadata.modules.join("+")
    : String(job?.metadata?.module || "");
  const createdAt = String(job?.created_at || "");
  return `<div class="dashboard-job-card${selectedCls}${previewCls}${runningCls}" data-job-id="${escapeHtml(String(job?.id || ""))}">
    <div class="dashboard-job-title">
      <strong class="dashboard-job-title-text">${escapeHtml(job?.label || jobTypeLabel(job?.type))}</strong>
      <span class="dashboard-job-badge status-${escapeHtml(status)}">${escapeHtml(jobStatusLabel(status))}</span>
    </div>
    <div class="dashboard-job-meta-line">${escapeHtml(jobTypeLabel(job?.type))}${moduleMeta ? ` | ${escapeHtml(moduleMeta)}` : ""}</div>
    <div class="dashboard-job-meta-line dashboard-job-summary-line">${escapeHtml(summary)}</div>
    <div class="dashboard-job-meta-line">${escapeHtml(createdAt || "-")}</div>
  </div>`;
}

function renderTaskDetailPanel(job, { preview = false, previewMessage = "" } = {}) {
  if (!job) {
    return `<section class="dashboard-job-detail-panel dashboard-job-detail-panel-empty">
      <div class="dashboard-job-detail-head">
        <div class="dashboard-job-detail-title">
          <strong class="dashboard-job-detail-heading">当前暂无后台任务</strong>
          <div class="dashboard-job-meta-line">任务启动后会在这里显示详细进度。</div>
        </div>
        <span class="dashboard-job-badge">空闲</span>
      </div>
      <div class="dashboard-job-detail-summary">当前没有可显示的任务详情。</div>
      <div class="dashboard-job-expanded">
        <div class="dashboard-meta">暂无任务元信息</div>
        <pre class="dashboard-job-log-window">暂无日志</pre>
        <div class="card-modal-actions dashboard-job-actions">
          <button class="ghost" type="button" disabled>暂无可取消任务</button>
        </div>
      </div>
    </section>`;
  }

  const status = String(job?.status || "queued");
  const moduleMeta = Array.isArray(job?.metadata?.modules)
    ? job.metadata.modules.join("+")
    : String(job?.metadata?.module || "");
  const createdAt = String(job?.created_at || "");
  const updatedAt = String(job?.updated_at || "");
  const logs = Array.isArray(job?.logs) && job.logs.length
    ? job.logs.join("\n")
    : (job?.error ? `ERROR: ${job.error}` : "暂无日志");
  const canCancel = !preview && ["queued", "running"].includes(status);
  const metaText = preview
    ? (previewMessage || "当前没有运行中的任务，正在展示最近任务预览。")
    : `创建 ${createdAt || "-"} | 更新 ${updatedAt || createdAt || "-"}`;

  return `<section class="dashboard-job-detail-panel${preview ? " is-preview" : ""}">
    <div class="dashboard-job-detail-head">
      <div class="dashboard-job-detail-title">
        <strong class="dashboard-job-detail-heading">${escapeHtml(job?.label || jobTypeLabel(job?.type))}</strong>
        <div class="dashboard-job-meta-line">${escapeHtml(jobTypeLabel(job?.type))}${moduleMeta ? ` | ${escapeHtml(moduleMeta)}` : ""}</div>
      </div>
      <span class="dashboard-job-badge status-${escapeHtml(status)}">${escapeHtml(jobStatusLabel(status))}</span>
    </div>
    <div class="dashboard-job-detail-summary">${escapeHtml(String(job?.message || "等待开始"))}</div>
    <div class="dashboard-job-expanded">
      <div class="dashboard-meta">${escapeHtml(metaText)}</div>
      <pre class="dashboard-job-log-window">${escapeHtml(logs)}</pre>
      <div class="card-modal-actions dashboard-job-actions">
        <button class="ghost" data-job-cancel-id="${escapeHtml(String(job?.id || ""))}"${canCancel ? "" : " disabled"}>取消任务</button>
      </div>
    </div>
  </section>`;
}

function renderTaskCenter() {
  if (!dashboardJobsList) return;
  updateTaskHistoryClearButton();
  if (!currentTaskJobs.length && !currentTaskJobsAll.length) {
    dashboardJobsList.innerHTML = '<div class="dashboard-job-empty">当前暂无后台任务</div>';
    syncTaskCenterLayout();
    return;
  }
  const previewMode = !currentTaskJobs.length && currentTaskJobsAll.length > 0;
  const listJobs = previewMode ? currentTaskJobsAll.slice(0, 8) : currentTaskJobsAll;
  const selected = listJobs.find((job) => String(job?.id || "") === String(selectedTaskJobId)) || listJobs[0] || null;
  selectedTaskJobId = String(selected?.id || "");
  const previewNote = previewMode
    ? '<div class="dashboard-job-list-note">当前没有运行中的任务，下面展示最近任务预览。</div>'
    : "";
  const cardsHtml = listJobs.map((job) => renderTaskCard(job, {
    selected: String(job?.id || "") === selectedTaskJobId,
    preview: previewMode,
  })).join("\n");
  const detailHtml = renderTaskDetailPanel(selected, {
    preview: previewMode,
    previewMessage: "当前没有运行中的任务，下面展示最近一条任务预览。",
  });
  dashboardJobsList.innerHTML = `<div class="dashboard-job-list">${previewNote}${cardsHtml}</div>${detailHtml}`;
  syncTaskCenterLayout();

  if (!previewMode && ["queued", "running"].includes(String(selected?.status || ""))) scheduleTaskLogFollow();
}

function syncTaskCenterLayout() {
  if (!(dashboardJobsList instanceof HTMLElement)) return;
  const list = dashboardJobsList.querySelector(".dashboard-job-list");
  const detail = dashboardJobsList.querySelector(".dashboard-job-detail-panel");
  const compactLayout = window.matchMedia("(max-width: 720px), (hover: none) and (pointer: coarse)").matches;
  dashboardJobsList.style.removeProperty("--dashboard-job-list-height");
  if (!(list instanceof HTMLElement) || !(detail instanceof HTMLElement) || compactLayout) {
    list?.classList.remove("is-height-synced");
    return;
  }
  const detailHeight = Math.ceil(detail.getBoundingClientRect().height);
  if (!detailHeight) {
    list.classList.remove("is-height-synced");
    return;
  }
  dashboardJobsList.style.setProperty("--dashboard-job-list-height", `${detailHeight}px`);
  list.classList.add("is-height-synced");
}

function getTaskJobIdFromTarget(target) {
  if (!(target instanceof Element)) return "";
  const card = target.closest(".dashboard-job-card[data-job-id]");
  return String(card?.getAttribute("data-job-id") || "").trim();
}

function findTaskJob(jobId) {
  const value = String(jobId || "").trim();
  if (!value) return null;
  return currentTaskJobsAll.find((item) => String(item?.id || "") === value)
    || currentTaskJobs.find((item) => String(item?.id || "") === value)
    || null;
}

function closeTaskJobContextMenu() {
  dashboardJobContextMenuController.close();
  currentTaskContextJobId = "";
}

function openTaskJobContextMenu(x, y, jobId) {
  const value = String(jobId || "").trim();
  if (!value) return;
  const job = findTaskJob(value);
  const status = String(job?.status || "");
  currentTaskContextJobId = value;
  if (dashboardJobContextDeleteBtn) {
    dashboardJobContextDeleteBtn.disabled = ["queued", "running"].includes(status);
  }
  dashboardJobContextMenuController.open(x, y);
}

function closeTaskDeleteModal() {
  if (DashboardSharedModal && typeof DashboardSharedModal.closeModal === "function") {
    DashboardSharedModal.closeModal(taskDeleteModal, taskDeleteModalController);
  } else if (taskDeleteModalController?.close) {
    taskDeleteModalController.close();
  } else if (taskDeleteModal) {
    taskDeleteModal.classList.add("hidden");
    taskDeleteModal.setAttribute("aria-hidden", "true");
  }
  pendingTaskDeleteMode = "";
  pendingDeleteTaskJobId = "";
}

function taskHistoryJobs() {
  return currentTaskJobsAll.filter((job) => !["queued", "running"].includes(String(job?.status || "")));
}

function updateTaskHistoryClearButton() {
  if (!(dashboardJobsClearBtn instanceof HTMLButtonElement)) return;
  const count = taskHistoryJobs().length;
  dashboardJobsClearBtn.disabled = count < 1;
  dashboardJobsClearBtn.title = count > 0 ? `清空 ${count} 条历史任务` : "暂无可清空的历史任务";
}

function openTaskDeleteModal(jobId) {
  const value = String(jobId || currentTaskContextJobId || "").trim();
  closeTaskJobContextMenu();
  if (!value) return;
  const job = findTaskJob(value);
  const label = String(job?.label || jobTypeLabel(job?.type) || value);
  const status = String(job?.status || "");
  if (["queued", "running"].includes(status)) {
    showAppErrorModal("删除任务失败", "运行中或排队中的任务不能直接删除，请先取消任务。");
    return;
  }
  pendingTaskDeleteMode = "single";
  pendingDeleteTaskJobId = value;
  if (taskDeleteMeta) {
    taskDeleteMeta.textContent = `${label} | ${jobStatusLabel(status || "completed")}`;
  }
  if (taskDeleteHint) {
    taskDeleteHint.textContent = `确定删除任务“${label}”吗？删除后会从任务历史中移除该条目。`;
  }
  if (DashboardSharedModal && typeof DashboardSharedModal.openModal === "function") {
    DashboardSharedModal.openModal(taskDeleteModal, taskDeleteModalController);
  } else if (taskDeleteModalController?.open) {
    taskDeleteModalController.open();
  } else {
    taskDeleteModal?.classList.remove("hidden");
    taskDeleteModal?.setAttribute("aria-hidden", "false");
  }
}

function openTaskHistoryClearModal() {
  closeTaskJobContextMenu();
  const removableJobs = taskHistoryJobs();
  if (!removableJobs.length) {
    showToast("当前没有可清空的任务历史");
    return;
  }
  pendingTaskDeleteMode = "history";
  pendingDeleteTaskJobId = "";
  if (taskDeleteMeta) {
    taskDeleteMeta.textContent = `历史任务 ${formatNum(removableJobs.length)} 条`;
  }
  if (taskDeleteHint) {
    taskDeleteHint.textContent = `确定清空 ${removableJobs.length} 条历史任务吗？运行中和排队中的任务会保留。`;
  }
  if (DashboardSharedModal && typeof DashboardSharedModal.openModal === "function") {
    DashboardSharedModal.openModal(taskDeleteModal, taskDeleteModalController);
  } else if (taskDeleteModalController?.open) {
    taskDeleteModalController.open();
  } else {
    taskDeleteModal?.classList.remove("hidden");
    taskDeleteModal?.setAttribute("aria-hidden", "false");
  }
}

async function confirmTaskDeleteAction() {
  if (pendingTaskDeleteMode === "history") {
    const payload = await apiDelete("/api/dashboard/jobs/clear-history");
    closeTaskDeleteModal();
    const removedJobs = Array.isArray(payload?.jobs) ? payload.jobs : [];
    if (removedJobs.some((job) => String(job?.id || "") === String(selectedTaskJobId || ""))) {
      selectedTaskJobId = "";
    }
    await refreshTaskCenter();
    showToast(`已清空 ${formatNum(payload?.removed_count || removedJobs.length || 0)} 条历史任务`);
    return;
  }
  const value = String(pendingDeleteTaskJobId || "").trim();
  if (!value) {
    showAppErrorModal("删除任务失败", "未找到要删除的任务。");
    return;
  }
  await apiDelete(`/api/dashboard/jobs/${encodeURIComponent(value)}`);
  closeTaskDeleteModal();
  if (selectedTaskJobId === value) selectedTaskJobId = "";
  await refreshTaskCenter();
}

function scheduleTaskLogFollow() {
  if (taskLogFollowFrame) cancelAnimationFrame(taskLogFollowFrame);
  taskLogFollowFrame = requestAnimationFrame(() => {
    taskLogFollowFrame = 0;
    const logWindow = dashboardJobsList?.querySelector(".dashboard-job-detail-panel .dashboard-job-log-window");
    if (!(logWindow instanceof HTMLElement)) return;
    logWindow.scrollTop = logWindow.scrollHeight;
    requestAnimationFrame(() => {
      if (logWindow.isConnected) logWindow.scrollTop = logWindow.scrollHeight;
    });
  });
}

window.addEventListener("resize", () => {
  syncTaskCenterLayout();
  dashboardTicketTrendChartInstance?.resize?.();
});

function hasRunningLibraryGraphJob() {
  return currentTaskJobsAll.some((job) => {
    const type = String(job?.type || "");
    const status = String(job?.status || "");
    return type === "library_graph_rebuild" && ["queued", "running"].includes(status);
  });
}

function hasRunningReportGenerationJob() {
  return currentTaskJobsAll.some((job) => {
    const type = String(job?.type || "");
    const status = String(job?.status || "");
    return type === "report_generation" && ["queued", "running"].includes(status);
  });
}

function latestReportJobMarker() {
  const jobs = currentTaskJobsAll
    .filter((job) => String(job?.type || "") === "report_generation")
    .sort((left, right) => String(right?.updated_at || right?.created_at || "").localeCompare(String(left?.updated_at || left?.created_at || "")));
  const latest = jobs[0] || null;
  if (!latest) return "";
  return [String(latest.id || ""), String(latest.status || ""), String(latest.updated_at || latest.created_at || "")].join("|");
}

async function refreshTaskCenter() {
  const payload = await apiGet("/api/dashboard/jobs?only_active=false");
  currentTaskJobsAll = Array.isArray(payload?.jobs) ? payload.jobs : [];
  currentTaskJobs = currentTaskJobsAll.filter((job) => ["queued", "running"].includes(String(job?.status || "")));
  renderTaskCenter();

  const dashboardPanelActive = document.getElementById("panel-dashboard")?.classList.contains("active");
  const now = Date.now();
  if (dashboardPanelActive && hasRunningLibraryGraphJob() && (now - lastDashboardForceRefreshAt) >= 8000) {
    lastDashboardForceRefreshAt = now;
    refreshDashboard({ force: true, skipTaskCenter: true }).catch(() => {});
  }
  const reportMarker = latestReportJobMarker();
  const runningReportJob = hasRunningReportGenerationJob();
  const reportJobChanged = !!reportMarker && reportMarker !== lastReportJobRefreshMarker;
  const reportJobJustFinished = hadRunningReportJob && !runningReportJob;
  if (dashboardPanelActive && (reportJobChanged || reportJobJustFinished) && (now - lastDashboardForceRefreshAt) >= 1500) {
    lastDashboardForceRefreshAt = now;
    refreshDashboard({ force: true, skipTaskCenter: true }).catch(() => {});
  }
  lastReportJobRefreshMarker = reportMarker;
  hadRunningReportJob = runningReportJob;
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
  const sharedMarkdown = window.CoreMarkdown;
  if (sharedMarkdown && typeof sharedMarkdown.render === "function") {
    return sharedMarkdown.render(text || "");
  }

  const source = normalizeMarkdown(text || "");
  const codeBlocks = [];
  const hrBlocks = [];
  const mathBlocks = [];
  const rawAnchorBlocks = [];

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

  const withRawAnchorTokens = withCodeTokens.replace(/<a\s+href="([^"]+)"[^>]*>([\s\S]*?)<\/a>/gi, (_m, hrefRaw, labelRaw) => {
    const href = String(hrefRaw || "").trim().replace(/"/g, "%22");
    if (!/^(?:https?:\/\/|doc:\/\/)/i.test(href)) return escapeHtml(_m);
    const label = escapeHtml(String(labelRaw || "").trim());
    const isExternal = /^https?:\/\//i.test(href);
    const idx = rawAnchorBlocks.length;
    rawAnchorBlocks.push(isExternal
      ? `<a href="${href}" class="external-link" target="_blank" rel="noopener noreferrer">${label}</a>`
      : `<a href="${href}">${label}</a>`);
    return `__RAW_ANCHOR_BLOCK_${idx}__`;
  });

  let html = escapeHtml(withRawAnchorTokens);
  html = html.replace(/^(#{1,6})\s+(.+)$/gm, (_m, hashes, title) => `<h${hashes.length}>${title}</h${hashes.length}>`);
  html = html.replace(/^&gt;\s?(.+)$/gm, "<blockquote>$1</blockquote>");
  html = html.replace(/\*\*([^*]+)\*\*/g, "<strong>$1</strong>");
  html = html.replace(/`([^`]+)`/g, "<code>$1</code>");
  html = html.replace(/\[((?:[^\[\]]|\[[^\[\]]*\])+)]\(([^)]+)\)/g, (_m, label, url) => {
    const href = String(url || "").trim().replace(/"/g, "%22");
    const normalizedLabel = String(label || "").trim();
    const displayLabel = /^\d+$/.test(normalizedLabel) ? `[${normalizedLabel}]` : label;
    const isExternal = /^https?:\/\//i.test(href);
    if (isExternal) {
      return `<a href=\"${href}\" class=\"external-link\" target=\"_blank\" rel=\"noopener noreferrer\">${displayLabel}</a>`;
    }
    return `<a href=\"${href}\">${displayLabel}</a>`;
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

  function splitMarkdownTableRow(line) {
    const normalizedLine = normalizeListWhitespace(stripZeroWidth(String(line || ""))).trim();
    if (!normalizedLine || /^__HR_BLOCK_\d+__|^__CODE_BLOCK_\d+__|^__MATH_BLOCK_\d+__/.test(normalizedLine)) return null;
    if (!normalizedLine.includes("|")) return null;
    let value = normalizedLine;
    if (value.startsWith("|")) value = value.slice(1);
    if (value.endsWith("|")) value = value.slice(0, -1);
    const cells = value.split("|").map((cell) => cell.trim());
    return cells.length ? cells : null;
  }

  function getMarkdownTableAlignments(line) {
    const cells = splitMarkdownTableRow(line);
    if (!cells || !cells.length) return null;
    const alignments = [];
    for (const cell of cells) {
      const compact = String(cell || "").replace(/\s+/g, "");
      if (!/^:?-{3,}:?$/.test(compact)) return null;
      if (compact.startsWith(":") && compact.endsWith(":")) alignments.push("center");
      else if (compact.endsWith(":")) alignments.push("right");
      else if (compact.startsWith(":")) alignments.push("left");
      else alignments.push("");
    }
    return alignments;
  }

  function collectMarkdownTableBlock(startIdx) {
    const headerCells = splitMarkdownTableRow(lines[startIdx]);
    const alignments = startIdx + 1 < lines.length ? getMarkdownTableAlignments(lines[startIdx + 1]) : null;
    if (!headerCells || !alignments || headerCells.length !== alignments.length) return null;
    const bodyRows = [];
    let idx = startIdx + 2;
    while (idx < lines.length) {
      const raw = String(lines[idx] || "");
      if (!raw.trim()) break;
      const cells = splitMarkdownTableRow(raw);
      if (!cells || cells.length !== headerCells.length) break;
      bodyRows.push(cells);
      idx += 1;
    }
    return { headerCells, alignments, bodyRows, nextIdx: idx };
  }

  function renderMarkdownTable(block) {
    const headerHtml = block.headerCells.map((cell, idx) => {
      const align = block.alignments[idx] ? ` style="text-align:${block.alignments[idx]}"` : "";
      return `<th${align}>${cell}</th>`;
    }).join("");
    const bodyHtml = block.bodyRows.map((row) => {
      const cols = row.map((cell, idx) => {
        const align = block.alignments[idx] ? ` style="text-align:${block.alignments[idx]}"` : "";
        return `<td${align}>${cell}</td>`;
      }).join("");
      return `<tr>${cols}</tr>`;
    }).join("\n");
    return `<div class="markdown-table-wrap"><table class="markdown-table"><thead><tr>${headerHtml}</tr></thead><tbody>${bodyHtml}</tbody></table></div>`;
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

    const tableBlock = collectMarkdownTableBlock(i);
    if (tableBlock) {
      out.push(renderMarkdownTable(tableBlock));
      i = tableBlock.nextIdx;
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
  html = html.replace(/__RAW_ANCHOR_BLOCK_(\d+)__/g, (_m, idx) => rawAnchorBlocks[Number(idx)] || "");
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
  const panelAuthorization = document.getElementById("panel-authorization");
  if (panelHome) panelHome.classList.toggle("active", name === "home");
  if (panelAgent) panelAgent.classList.toggle("active", name === "agent");
  if (panelDashboard) panelDashboard.classList.toggle("active", name === "dashboard");
  if (panelTickets) panelTickets.classList.toggle("active", name === "tickets");
  if (panelBenchmark) panelBenchmark.classList.toggle("active", name === "benchmark");
  if (panelAuthorization) panelAuthorization.classList.toggle("active", name === "authorization");
  if (name === "dashboard") {
    scheduleDashboardChartResize();
  }
}

function scheduleDashboardChartResize() {
  const resize = () => dashboardTicketTrendChartInstance?.resize?.();
  resize();
  requestAnimationFrame(() => {
    resize();
    setTimeout(resize, 80);
    setTimeout(resize, 220);
  });
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

function cloneJsonValue(value) {
  try {
    return JSON.parse(JSON.stringify(value));
  } catch (_err) {
    return null;
  }
}

function loadDashboardCache() {
  try {
    const raw = window.localStorage.getItem(DASHBOARD_CACHE_STORAGE_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === "object" ? parsed : null;
  } catch (_err) {
    return null;
  }
}

function saveDashboardCache(data) {
  try {
    if (!data || typeof data !== "object") return;
    const existing = loadDashboardCache();
    const merged = mergeDashboardData(existing, data) || cloneJsonValue(data);
    if (!merged || typeof merged !== "object") return;
    if (!hasDashboardDetailSnapshot(merged)) return;
    window.localStorage.setItem(DASHBOARD_CACHE_STORAGE_KEY, JSON.stringify(merged));
  } catch (_err) {
    // Ignore localStorage failures and keep the in-memory snapshot.
  }
}

function hasMeaningfulDashboardValue(value) {
  if (value == null) return false;
  if (Array.isArray(value)) return value.length > 0;
  if (typeof value === "object") return Object.keys(value).length > 0;
  return true;
}

function mergeDashboardData(previous, incoming) {
  const next = cloneJsonValue(incoming);
  if (!next || typeof next !== "object") return null;
  if (!next.is_core || !previous || typeof previous !== "object") return next;

  const previousSnapshot = cloneJsonValue(previous) || {};
  const merged = { ...previousSnapshot, ...next };
  [
    "retrieval_latency",
    "cache_stats",
    "retrieval_by_profile",
    "retrieval_by_search_mode",
    "agent_by_profile",
    "agent_by_search_mode",
    "agent_by_query_type",
    "rerank_quality",
    "missing_queries_last_30d",
    "chat_feedback",
    "ticket_weekly_stats",
    "agent_wall_clock",
    "runtime_data",
  ].forEach((key) => {
    if (!hasMeaningfulDashboardValue(next[key]) && hasMeaningfulDashboardValue(previous[key])) {
      merged[key] = cloneJsonValue(previous[key]);
    }
  });
  return merged;
}

function hasDashboardDetailSnapshot(data) {
  if (!data || typeof data !== "object") return false;
  const latencyStages = data?.retrieval_latency?.stages;
  if (latencyStages && typeof latencyStages === "object" && Object.keys(latencyStages).length) return true;
  if (hasMeaningfulDashboardValue(data?.cache_stats)) return true;
  if (hasMeaningfulDashboardValue(data?.ticket_weekly_stats)) return true;
  if (hasMeaningfulDashboardValue(data?.agent_wall_clock)) return true;
  if (hasMeaningfulDashboardValue(data?.runtime_data)) return true;
  if (hasMeaningfulDashboardValue(data?.rerank_quality)) return true;
  return false;
}

function formatDateTimeLabel(value) {
  const text = String(value || "").trim();
  if (!text) return "";
  return text.replace("T", " ").slice(0, 16);
}

function selectedDataBackupApps() {
  if (window.NavDashboardOverviewMeta?.selectedDataBackupApps) {
    return window.NavDashboardOverviewMeta.selectedDataBackupApps(document);
  }
  const selected = Array.from(document.querySelectorAll("input[name='data-backup-app']:checked"))
    .map((input) => String(input.value || "").trim())
    .filter(Boolean);
  return selected.length ? selected : ["library_tracker", "property", "journey"];
}

function renderDataBackupPanel(data) {
  const summary = data?.data_backups || {};
  currentDataBackupSummary = summary;
  if (window.NavDashboardOverviewMeta?.renderDataBackupPanel) {
    window.NavDashboardOverviewMeta.renderDataBackupPanel(summary, {
      metaElement: dataBackupMeta,
      summaryElement: dataBackupSummary,
      formatDateTimeLabel,
      escapeHtml,
      formatSizeValue,
    });
    return;
  }
  if (dataBackupMeta) {
    const latestAt = formatDateTimeLabel(summary.latest_backup_at);
    dataBackupMeta.textContent = latestAt ? `最近备份: ${latestAt}` : "最近备份: 暂无";
  }
  if (dataBackupSummary) {
    dataBackupSummary.textContent = `备份目录: ${String(summary.backup_dir_relative || summary.backup_dir || "-")}`;
  }
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

function clampRate(value) {
  const n = toFiniteNumber(value);
  if (n == null) return null;
  return Math.min(1, Math.max(0, n));
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
  const libraryGraphCoverage = clampRate(
    library?.graph_quality?.item_coverage_rate ??
    safeRatio(library?.graph_quality?.item_node_count, library.total_items) ??
    safeRatio(library.graph_nodes, library.total_items)
  );
  const isolatedRate = clampRate(library?.graph_quality?.isolated_node_rate);
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
    { key: "total",                    label: "RAG 检索总时长" },
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
        <th>检索用时</th>
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
  renderDashboardLoadStatus(currentDashboardData, text);
  if (currentDashboardData) {
    if (dashboardGeneratedAt) dashboardGeneratedAt.textContent = `${text}，当前显示缓存数据`;
    return;
  }
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

function loadTicketsPrefill() {
  try {
    const raw = document.getElementById("tickets-prefill-data")?.textContent || "";
    if (!raw.trim()) return null;
    const parsed = JSON.parse(raw);
    return parsed?.ok ? parsed : null;
  } catch (_) {
    return null;
  }
}

function formatDateInputValue(date) {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function getDefaultTicketDateRange() {
  const createdToDate = new Date();
  const createdFromDate = new Date(createdToDate);
  createdFromDate.setDate(createdToDate.getDate() - 6);
  return {
    created_from: formatDateInputValue(createdFromDate),
    created_to: formatDateInputValue(createdToDate),
  };
}

function loadBenchmarkPrefill() {
  try {
    const raw = document.getElementById("benchmark-prefill-data")?.textContent || "";
    if (!raw.trim()) return null;
    return JSON.parse(raw) || null;
  } catch (_) {
    return null;
  }
}

function markDashboardFreshness(data) {
  if (!data || data.is_core) return;
  const mode = String(data?.overview_status?.mode || "").trim();
  if (mode && mode !== "full") return;
  dashboardHasFullOverview = true;
  dashboardLastFullRefreshAt = Date.now();
}

function isDashboardFullRefreshStale() {
  if (!dashboardHasFullOverview || !dashboardLastFullRefreshAt) return true;
  return (Date.now() - dashboardLastFullRefreshAt) > DASHBOARD_FULL_REFRESH_TTL_MS;
}

function ensureDashboardBackgroundTasks() {
  startTaskCenterPolling();
  scheduleStartupPollingIfNeeded();
}

function summarizeOverviewErrors(status) {
  const errors = Array.isArray(status?.errors) ? status.errors : [];
  if (!errors.length) return "";
  return errors
    .map((item) => {
      const section = String(item?.section || "unknown").trim();
      const message = String(item?.message || item?.type || "unknown error").trim();
      return `${section}: ${message}`;
    })
    .join(" | ");
}

function renderDashboardLoadStatus(data, explicitError = "") {
  if (!dashboardLoadStatus) return;
  const directError = String(explicitError || "").trim();
  const status = data?.overview_status && typeof data.overview_status === "object" ? data.overview_status : {};
  const mode = String(status.mode || "").trim();
  const baseMessage = String(status.message || "").trim();
  const errorSummary = summarizeOverviewErrors(status);
  let level = "info";
  let text = "";

  if (directError) {
    level = "error";
    text = `详细统计刷新失败：${directError}`;
  } else if (mode === "core") {
    text = baseMessage || "当前仅显示核心指标，详细统计补全中。";
  } else if (mode === "degraded_full") {
    level = "warning";
    text = `${baseMessage || "部分详细统计加载失败。"}${errorSummary ? ` ${errorSummary}` : ""}`.trim();
  } else if (mode === "core_fallback" || mode === "prefill_error") {
    level = "error";
    text = `${baseMessage || "详细统计加载失败。"}${errorSummary ? ` ${errorSummary}` : ""}`.trim();
  }

  dashboardLoadStatus.textContent = text;
  dashboardLoadStatus.dataset.level = level;
  dashboardLoadStatus.classList.toggle("hidden", !text);
}

// ── paintDashboardFromData ─────────────────────────────────────────────────
// Single source of truth for turning a data dict (prefill or network fetch)
// into visible DOM content. Both bootstrapDashboardTab() and refreshDashboard()
// call this so prefill + fresh data produce an identical layout.
function paintDashboardFromData(data, { skipTaskCenter = false, skipTicketInsights = false, markFreshness = true } = {}) {
  const mergedData = mergeDashboardData(currentDashboardData, data) || {};
  currentDashboardData = mergedData;
  saveDashboardCache(mergedData);

  const rag = mergedData?.rag || {};
  const library = mergedData?.library || {};
  const aliasProposal = library?.alias_proposal || {};
  const libraryGraphQuality = library?.graph_quality || {};
  const libraryCoverageRate = clampRate(
    libraryGraphQuality.item_coverage_rate ??
    safeRatio(libraryGraphQuality.item_node_count, library.total_items) ??
    safeRatio(library.graph_nodes, library.total_items)
  );
  const isolatedNodeRate = clampRate(libraryGraphQuality.isolated_node_rate);
  const apiUsage = mergedData?.api_usage || {};
  const agent = mergedData?.agent || {};
  const ragQa = mergedData?.rag_qa || {};
  const startup = mergedData?.startup || {};
  const latency = mergedData?.retrieval_latency || {};
  const cacheStats = mergedData?.cache_stats || {};
  const rerankQuality = mergedData?.rerank_quality || {};
  const ragRerank = rerankQuality.rag || {};
  const agentRerank = rerankQuality.agent || {};
  const missingQueries = mergedData?.missing_queries_last_30d || {};
  const agentWallClock = mergedData?.agent_wall_clock || {};
  const runtimeData = mergedData?.runtime_data || {};
  const dataBackups = mergedData?.data_backups || {};
  lastStartupStatus = String(startup.status || "unknown");
  lastApiUsage = apiUsage;
  currentRuntimeDataSummary = runtimeData;
  currentDataBackupSummary = dataBackups;
  const warnings = Array.isArray(mergedData?.warnings) ? mergedData.warnings : [];
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
    buildStatCard("Library Graph 节点数", formatNum(library.graph_nodes), `边数 ${formatNum(library.graph_edges)} | 覆盖 ${formatRate(libraryCoverageRate)}`, "library-graph-summary", unhealthyState(health.libraryGraphScale)),

    //buildStatCard("RAG 平均节点数", `${rag.nodes_per_doc != null ? Number(rag.nodes_per_doc).toFixed(2) : "—"}`, `每节点平均边数 ${rag.edges_per_node != null ? Number(rag.edges_per_node).toFixed(2) : "—"}`, "", unhealthyState(health.ragGraphDensity)),
    buildStatCard("本月 Tavily API 调用", formatNum(apiUsage.month_web_search_calls), `今日 ${formatNum(apiUsage.today_web_search)} / 限额 ${formatNum(apiUsage.daily_web_limit)}`, "web-search-usage", unhealthyState(health.webUsage)),
    buildStatCard("本月 DeepSeek API 调用", formatNum(apiUsage.month_deepseek_calls), `今日 ${formatNum(apiUsage.today_deepseek)} / 限额 ${formatNum(apiUsage.daily_deepseek_limit)}`, "deepseek-usage", unhealthyState(health.deepseekUsage)),

    buildStatCard("Agent 消息总数", formatNum(agent.message_count), `会话数 ${formatNum(agent.session_count)}`),
    buildStatCard("RAG Q&A 消息总数", formatNum(ragQa.message_count), `会话数 ${formatNum(ragQa.session_count)}`),

    buildStatCard("RAG 检索总时长均值", formatDuration(latency.stages?.total?.avg), `近 ${formatNum(latency.stages?.total?.count)} 次 | p50 ${formatDuration(latency.stages?.total?.p50)}`, "", unhealthyState(health.vectorRecall)),
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
    buildStatCard("聊天反馈数", formatNum(mergedData?.chat_feedback?.count), "长按查看导出", "feedback-summary"),

    buildStatCard("RAG 待重建文档", formatNum(rag.changed_pending), rag.changed_pending > 0 ? "等待后台同步" : "已全部同步", "rag-changed-pending", unhealthyState(health.ragPending)),
    buildStatCard("待审核媒体同义词", formatNum(aliasProposal.pending_count), "长按查看待审核列表", "library-alias-proposal-summary"),
    
    buildStatCard("运行时数据", formatSizeValue(runtimeData.total_size_bytes), `非空 ${formatNum(runtimeData.nonzero_items)} 项 | 长按查看`, "runtime-data-summary"),
  ];

  // Store warnings for modal
  currentWarnings = warnings.filter(w => !dismissedWarnings.has(w));
  currentWarningsTimestamp = String(mergedData?.generated_at || "").trim();

  cards.push(buildStatCard("系统告警", formatNum(currentWarnings.length), currentWarnings.length > 0 ? currentWarnings.slice(0, 2).join(" | ") : "无告警", "warnings-summary", unhealthyState(health.warnings)));

  renderDashboardNotifications(mergedData);

  if (dashboardGrid) {
    dashboardGrid.innerHTML = cards.join("\n");
  }

  renderDashboardLatencyTable(mergedData);
  renderDashboardObservabilityTable(mergedData);
  renderDashboardStartupLogs(mergedData);
  renderDataBackupPanel(mergedData);
  if (!skipTicketInsights) {
    renderDashboardTicketTrend(mergedData?.ticket_weekly_stats || {});
  }
  if (!skipTaskCenter) refreshTaskCenter().catch(() => {});

  const generated = String(mergedData?.generated_at || "").trim();
  const month = String(mergedData?.month || "").trim();
  const deployed = formatDateTimeLabel(mergedData?.deployed_at);
  if (dashboardGeneratedAt) {
    dashboardGeneratedAt.textContent = `统计月份: ${month || "-"} | 更新时间: ${generated || "-"}`;
  }
  if (dashboardDeployTime) {
    dashboardDeployTime.textContent = `部署时间: ${deployed || "-"}`;
  }
  if (dashboardBackupTime) {
    dashboardBackupTime.textContent = ` | ${window.NavDashboardOverviewMeta?.buildLatestBackupText
      ? window.NavDashboardOverviewMeta.buildLatestBackupText(dataBackups, formatDateTimeLabel, { includeName: false, emptyText: "最近备份: 暂无" })
      : (formatDateTimeLabel(dataBackups?.latest_backup_at) ? `最近备份: ${formatDateTimeLabel(dataBackups.latest_backup_at)}` : "最近备份: 暂无")}`;
  }
  renderDashboardLoadStatus(mergedData);
  if (markFreshness) markDashboardFreshness(mergedData);
}

const dashboardOverviewBootstrap = DashboardOverviewBootstrapModule?.createOverviewBootstrap?.({
  apiGet,
  apiPost,
  escapeHtml,
  dashboardInformationNotices,
  dashboardGeneratedAt,
  dashboardRefreshBtn,
  fullOverviewTimeoutMs: DASHBOARD_FULL_OVERVIEW_TIMEOUT_MS,
  paintDashboardFromData,
  renderDashboardError,
  loadDashboardCache,
  loadDashboardPrefill,
  hasDashboardDetailSnapshot,
  ensureDashboardBackgroundTasks,
  getDashboardState: () => dashboardState,
  patchDashboardState: (patch) => {
    if (!patch || typeof patch !== "object") return;
    Object.assign(dashboardState, patch);
  },
  getDashboardRefreshInFlight: () => dashboardRefreshInFlight,
  setDashboardRefreshInFlight: (value) => {
    dashboardRefreshInFlight = Boolean(value);
  },
  getCurrentDashboardData: () => currentDashboardData,
  hasFullOverview: () => dashboardHasFullOverview,
  isFullRefreshStale: () => isDashboardFullRefreshStale(),
}) || {
  hydrateInitialNotifications() {},
  renderDashboardNotifications() {},
  dismissDashboardNotification: async () => {},
  refreshDashboard: async () => null,
  hydrateDashboardShellFromStoredData: () => false,
  prewarmDashboardOverview() {},
  bootstrapDashboardTab: async () => {},
};

function renderDashboardNotifications(data) {
  return dashboardOverviewBootstrap.renderDashboardNotifications(data);
}

async function dismissDashboardNotification(key) {
  return dashboardOverviewBootstrap.dismissDashboardNotification(key);
}

async function refreshDashboard(options = {}) {
  return dashboardOverviewBootstrap.refreshDashboard(options);
}

function hydrateDashboardShellFromStoredData() {
  return dashboardOverviewBootstrap.hydrateDashboardShellFromStoredData();
}

function prewarmDashboardOverview() {
  return dashboardOverviewBootstrap.prewarmDashboardOverview();
}

async function bootstrapDashboardTab() {
  return dashboardOverviewBootstrap.bootstrapDashboardTab();
}

dashboardOverviewBootstrap.hydrateInitialNotifications();

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
      const data = await apiGet("/api/startup/status?fresh=1");
      const status = String(data?.status || "");
      if (status === "ready") {
        stopStartupPolling();
        lastStartupStatus = status;
        await refreshDashboard({ force: true });
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
  const traceId = String(item?.trace_id || "").trim();
  currentFeedbackDetailItem = item || null;
  if (feedbackDetailMeta) {
    feedbackDetailMeta.textContent = [String(item?.created_at || ""), displaySourceLabel(item?.source || "unknown")].filter(Boolean).join(" | ");
  }
  if (feedbackDetailContent) {
    feedbackDetailContent.innerHTML = `
      <div class="trace-summary-grid">
        <section class="trace-summary-card">
          <div class="bm-card-section-label">基本信息</div>
          <div class="trace-kv"><span>trace_id</span><strong>${escapeHtml(traceId || "—")}</strong></div>
          <div class="trace-kv"><span>session_id</span><strong>${escapeHtml(String(item?.session_id || "—"))}</strong></div>
          <div class="trace-kv"><span>模型</span><strong>${escapeHtml(String(item?.model || "—"))}</strong></div>
          <div class="trace-kv"><span>搜索模式</span><strong>${escapeHtml(String(item?.search_mode || "—"))}</strong></div>
          <div class="trace-kv"><span>问题类型</span><strong>${escapeHtml(String(item?.query_type || metadata.query_type || "—"))}</strong></div>
          <div class="trace-kv"><span>用户反馈</span><strong>${escapeHtml(String(item?.feedback || metadata.feedback || "—"))}</strong></div>
        </section>
        <section class="trace-summary-card">
          <div class="bm-card-section-label">原始问题</div>
          <div class="feedback-detail-text">${escapeHtml(String(item?.question || "—"))}</div>
        </section>
      </div>
      <section class="trace-summary-card">
        <div class="bm-card-section-label">回答</div>
        <div class="feedback-detail-text is-answer">${escapeHtml(String(item?.answer || "—"))}</div>
      </section>
    `;
  }
  if (feedbackDetailOpenTraceBtn) feedbackDetailOpenTraceBtn.disabled = !traceId;
  feedbackDetailModal?.classList.remove("hidden");
  feedbackDetailModal?.setAttribute("aria-hidden", "false");
}

function closeFeedbackDetailModal() {
  feedbackDetailModal?.classList.add("hidden");
  feedbackDetailModal?.setAttribute("aria-hidden", "true");
  currentFeedbackDetailItem = null;
}

function normalizeDashboardTicketTrendMode(mode) {
  const value = String(mode || "").trim().toLowerCase();
  if (value === "day" || value === "month") return value;
  return "week";
}

function dashboardTicketTrendModeLabel(mode) {
  if (mode === "day") return "日";
  if (mode === "month") return "月";
  return "周";
}

function dashboardPrioritySeriesLabel(priority) {
  const key = String(priority || "").trim().toLowerCase();
  return `优先级 ${key || "unknown"}`;
}

function dashboardPrioritySeriesColor(priority) {
  const key = String(priority || "").trim().toLowerCase();
  const colors = {
    critical: "#ff6b6b",
    high: "#f59f00",
    medium: "#66d9ef",
  };
  return colors[key] || "#a5ab9e";
}

function resolveDashboardTicketTrendSeries(stats, mode) {
  const trends = stats?.trends && typeof stats.trends === "object" ? stats.trends : {};
  const normalized = normalizeDashboardTicketTrendMode(mode);
  const fromTrends = Array.isArray(trends?.[normalized]) ? trends[normalized] : [];
  if (fromTrends.length) return fromTrends;
  if (normalized === "week" && Array.isArray(stats?.weeks)) return stats.weeks;
  return [];
}

function ensureDashboardTicketTrendChartInstance() {
  if (!dashboardTicketTrendChart) return;
  if (typeof window.echarts === "undefined" || !window.echarts?.init) return null;
  const canvas = dashboardTicketTrendChart.querySelector(".ticket-trend-echart-canvas");
  if (!(canvas instanceof HTMLElement)) return null;
  if (dashboardTicketTrendChartInstance) {
    const currentDom = dashboardTicketTrendChartInstance.getDom?.();
    if (currentDom === canvas) return dashboardTicketTrendChartInstance;
    dashboardTicketTrendChartInstance.dispose?.();
    dashboardTicketTrendChartInstance = null;
  }
  dashboardTicketTrendChartInstance = window.echarts.init(canvas, "dark");
  return dashboardTicketTrendChartInstance;
}

function renderDashboardTicketTrend(stats, options = {}) {
  if (!dashboardTicketTrendChart) return;
  if (stats && typeof stats === "object") {
    currentDashboardTicketTrendStats = stats;
  }
  const sourceStats = currentDashboardTicketTrendStats && typeof currentDashboardTicketTrendStats === "object"
    ? currentDashboardTicketTrendStats
    : {};
  const requestedMode = normalizeDashboardTicketTrendMode(options?.mode || dashboardTicketTrendModeSelect?.value || dashboardTicketTrendMode);
  const fallbackModes = [requestedMode, "week", "day", "month"];
  let mode = requestedMode;
  let series = [];
  for (const candidate of fallbackModes) {
    const rows = resolveDashboardTicketTrendSeries(sourceStats, candidate);
    if (rows.length) {
      mode = candidate;
      series = rows;
      break;
    }
  }
  dashboardTicketTrendMode = mode;
  if (dashboardTicketTrendModeSelect) {
    dashboardTicketTrendModeSelect.value = mode;
  }

  const weeks = series;
  const summary = sourceStats?.summary && typeof sourceStats.summary === "object" ? sourceStats.summary : {};
  const statusCounts = sourceStats?.status_counts && typeof sourceStats.status_counts === "object" ? sourceStats.status_counts : {};
  const priorityCounts = sourceStats?.priority_counts && typeof sourceStats.priority_counts === "object" ? sourceStats.priority_counts : {};
  if (!weeks.length) {
    if (dashboardTicketSummaryMeta) dashboardTicketSummaryMeta.textContent = "暂无 ticket 统计";
    if (dashboardTicketSummaryBody) dashboardTicketSummaryBody.innerHTML = '<div class="ticket-trend-empty">暂无统计数据</div>';
    if (dashboardTicketTrendMeta) dashboardTicketTrendMeta.textContent = "暂无 ticket 趋势统计";
    dashboardTicketTrendChart.innerHTML = '<div class="ticket-trend-empty">暂无图表数据</div>';
    return;
  }

  const priorityLegendKeys = ["critical", "high", "medium"];
  const modeLabel = dashboardTicketTrendModeLabel(mode);
  const firstLabel = String(weeks[0]?.label || weeks[0]?.bucket_start || "");
  const lastLabel = String(weeks[weeks.length - 1]?.label || weeks[weeks.length - 1]?.bucket_start || "");

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
    dashboardTicketTrendMeta.textContent = `${modeLabel}趋势 | ${firstLabel || "-"} 到 ${lastLabel || "-"} | 优先级堆叠面积图`;
  }

  dashboardTicketTrendChart.innerHTML = `
    <div class="ticket-trend-echart-canvas" role="img" aria-label="Ticket ${modeLabel}趋势图（优先级堆叠面积）"></div>
  `;

  const chart = ensureDashboardTicketTrendChartInstance();
  if (!chart) {
    dashboardTicketTrendChart.innerHTML = '<div class="ticket-trend-empty">图表组件未加载，无法渲染趋势图。</div>';
    return;
  }

  const labels = weeks.map((item) => String(item?.label || item?.bucket_start || "-"));
  chart.setOption({
    backgroundColor: "transparent",
    tooltip: {
      trigger: "axis",
      axisPointer: { type: "line" },
      formatter: (params = []) => {
        const list = Array.isArray(params) ? params : [];
        if (!list.length) return "";
        const title = String(list[0]?.axisValue || "");
        const lines = list.map((item) => `${item.marker}${item.seriesName}: ${formatNum(item.value)}`);
        return `<strong>${escapeHtml(title)}</strong><br/>${lines.join("<br/>")}`;
      },
    },
    legend: {
      top: 4,
      textStyle: { color: "#b7b7a4", fontSize: 12 },
      data: [...priorityLegendKeys.map((priority) => dashboardPrioritySeriesLabel(priority))],
    },
    grid: { left: 12, right: 12, top: 58, bottom: 26, containLabel: true },
    xAxis: {
      type: "category",
      data: labels,
      boundaryGap: false,
      axisLabel: {
        color: "#98a08d",
        fontSize: 11,
        interval: Math.max(0, Math.ceil(labels.length / 10) - 1),
      },
      axisLine: { lineStyle: { color: "#41453a" } },
      axisTick: { show: false },
    },
    yAxis: {
      type: "value",
      minInterval: 1,
      axisLabel: { color: "#98a08d", fontSize: 11 },
      splitLine: { lineStyle: { color: "#41453a", type: "dashed" } },
    },
    series: priorityLegendKeys.map((priority) => ({
      name: dashboardPrioritySeriesLabel(priority),
      type: "line",
      stack: "priority-submitted-area",
      smooth: true,
      symbol: "circle",
      symbolSize: 6,
      data: weeks.map((item) => Number((item?.priority_submitted || {})[priority] || 0)),
      lineStyle: { color: dashboardPrioritySeriesColor(priority), width: 2.5 },
      itemStyle: { color: dashboardPrioritySeriesColor(priority) },
      areaStyle: { color: `${dashboardPrioritySeriesColor(priority)}33` },
      emphasis: { focus: "series" },
    })),
  }, true);
  scheduleDashboardChartResize();
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
  await refreshDashboard({ force: false });
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
  await refreshDashboard({ force: false });
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

function sleep(ms) {
  return new Promise((resolve) => {
    window.setTimeout(resolve, Math.max(0, Number(ms || 0)));
  });
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

const UI_SUPPORTED_API_SCHEMA_VERSION = 1;

function readApiSchemaVersion(payload) {
  const data = payload && typeof payload === "object" ? payload : {};
  const raw = Number(data.api_schema_version || 0);
  return Number.isFinite(raw) && raw > 0 ? raw : 0;
}

function assertSupportedApiSchemaVersion(payload, contractName) {
  const version = readApiSchemaVersion(payload);
  if (version > UI_SUPPORTED_API_SCHEMA_VERSION) {
    throw new Error(`${contractName} 返回了更高版本的 API 契约（api_schema_version=${version}，当前 UI 仅支持 ${UI_SUPPORTED_API_SCHEMA_VERSION}）。请刷新到匹配版本后再试。`);
  }
  return version;
}

function normalizeAgentResponse(payload) {
  const data = payload && typeof payload === "object" ? payload : {};
  const apiSchemaVersion = assertSupportedApiSchemaVersion(data, "Agent");
  return {
    api_schema_version: apiSchemaVersion,
    trace_id: String(data.trace_id || "").trim(),
    session_id: String(data.session_id || "").trim(),
    answer: String(data.answer || ""),
    query_classification: data.query_classification && typeof data.query_classification === "object" ? data.query_classification : {},
    query_understanding: data.query_understanding && typeof data.query_understanding === "object" ? data.query_understanding : {},
    planned_tools: Array.isArray(data.planned_tools) ? data.planned_tools : [],
    tool_results: Array.isArray(data.tool_results) ? data.tool_results : [],
  };
}

function normalizeTraceResponse(payload) {
  const data = payload && typeof payload === "object" ? payload : {};
  const apiSchemaVersion = assertSupportedApiSchemaVersion(data, "Dashboard trace");
  return {
    api_schema_version: apiSchemaVersion,
    ok: data.ok !== false,
    trace: data.trace && typeof data.trace === "object" ? data.trace : null,
    exportText: String(data.export_text || data.exportText || ""),
  };
}

function normalizeBenchmarkRunResult(run) {
  const value = run && typeof run === "object" ? run : {};
  const extensions = value.extensions && typeof value.extensions === "object" ? value.extensions : {};
  const merged = { ...extensions, ...value };
  return {
    ...merged,
    cases: merged.cases && typeof merged.cases === "object" ? merged.cases : {},
    case_details: merged.case_details && typeof merged.case_details === "object" ? merged.case_details : {},
    case_details_by_taxonomy: merged.case_details_by_taxonomy && typeof merged.case_details_by_taxonomy === "object" ? merged.case_details_by_taxonomy : {},
    taxonomy_counts: merged.taxonomy_counts && typeof merged.taxonomy_counts === "object" ? merged.taxonomy_counts : {},
    assertions: merged.assertions && typeof merged.assertions === "object" ? merged.assertions : {},
    assertion_summary: merged.assertion_summary && typeof merged.assertion_summary === "object" ? merged.assertion_summary : null,
  };
}

function normalizeBenchmarkHistoryResponse(payload) {
  const data = payload && typeof payload === "object" ? payload : {};
  const apiSchemaVersion = assertSupportedApiSchemaVersion(data, "Benchmark history");
  return {
    api_schema_version: apiSchemaVersion,
    results: Array.isArray(data.results) ? data.results.map((item) => normalizeBenchmarkRunResult(item)) : [],
  };
}

function normalizeBenchmarkCaseSetsResponse(payload) {
  const data = payload && typeof payload === "object" ? payload : {};
  const apiSchemaVersion = assertSupportedApiSchemaVersion(data, "Benchmark case sets");
  return {
    api_schema_version: apiSchemaVersion,
    case_sets: Array.isArray(data.case_sets) ? data.case_sets.map((item) => normalizeBenchmarkCaseSet(item)) : [],
    chains: Array.isArray(data.chains) ? data.chains : [],
  };
}

function normalizeBenchmarkCaseSet(caseSet) {
  const value = caseSet && typeof caseSet === "object" ? caseSet : {};
  const extensions = value.extensions && typeof value.extensions === "object" ? value.extensions : {};
  const merged = { ...extensions, ...value };
  return {
    ...merged,
    lengths: merged.lengths && typeof merged.lengths === "object" ? merged.lengths : {},
    taxonomy_counts: merged.taxonomy_counts && typeof merged.taxonomy_counts === "object" ? merged.taxonomy_counts : {},
    source_counts: merged.source_counts && typeof merged.source_counts === "object" ? merged.source_counts : {},
    supported_modules: Array.isArray(merged.supported_modules)
      ? merged.supported_modules.map((item) => String(item || "").trim()).filter(Boolean)
      : [],
    module_case_counts: merged.module_case_counts && typeof merged.module_case_counts === "object" ? merged.module_case_counts : {},
  };
}

function getSelectedBenchmarkModules() {
  const modules = [];
  if (document.getElementById("bm-rag")?.checked) modules.push("rag");
  if (document.getElementById("bm-agent")?.checked) modules.push("agent");
  if (document.getElementById("bm-hybrid")?.checked) modules.push("hybrid");
  return modules;
}

function formatBenchmarkModuleName(moduleId) {
  if (moduleId === "rag") return "RAG";
  if (moduleId === "agent") return "Agent";
  if (moduleId === "hybrid") return "Hybrid";
  return String(moduleId || "").trim();
}

function renderBenchmarkCaseSetOptions() {
  return dashboardBenchmarkController.renderBenchmarkCaseSetOptions();
}

function normalizeBenchmarkJobResponse(payload) {
  const data = payload && typeof payload === "object" ? payload : {};
  const apiSchemaVersion = assertSupportedApiSchemaVersion(data, "Benchmark job");
  const job = data.job && typeof data.job === "object" ? data.job : data;
  return {
    api_schema_version: apiSchemaVersion,
    ok: data.ok !== false,
    job: {
      ...job,
      metadata: job.metadata && typeof job.metadata === "object" ? job.metadata : {},
      logs: Array.isArray(job.logs) ? job.logs : [],
      result: job.result && typeof job.result === "object" ? normalizeBenchmarkRunResult(job.result) : job.result,
    },
  };
}

function normalizeRouterClassificationCase(item) {
  const value = item && typeof item === "object" ? item : {};
  return {
    ...value,
    id: String(value.id || "").trim(),
    query: String(value.query || "").trim(),
    expected_domain: String(value.expected_domain || "").trim(),
    expected_arbitration: value.expected_arbitration,
    expected_query_class: String(value.expected_query_class || "").trim(),
    subject_scope: String(value.subject_scope || "").trim(),
    time_scope_type: String(value.time_scope_type || "").trim(),
    answer_shape: String(value.answer_shape || "").trim(),
    media_family: String(value.media_family || "").trim(),
    followup_mode: String(value.followup_mode || "").trim(),
    mock_entities: Array.isArray(value.mock_entities) ? value.mock_entities : [],
  };
}

function normalizeRouterClassificationCasesResponse(payload) {
  const data = payload && typeof payload === "object" ? payload : {};
  const apiSchemaVersion = assertSupportedApiSchemaVersion(data, "Benchmark router classification cases");
  return {
    api_schema_version: apiSchemaVersion,
    ok: data.ok !== false,
    case: data.case && typeof data.case === "object" ? normalizeRouterClassificationCase(data.case) : null,
    cases: Array.isArray(data.cases) ? data.cases.map((item) => normalizeRouterClassificationCase(item)) : [],
  };
}

function normalizeRouterClassificationRunResponse(payload) {
  const data = payload && typeof payload === "object" ? payload : {};
  const apiSchemaVersion = assertSupportedApiSchemaVersion(data, "Benchmark router classification run");
  return {
    api_schema_version: apiSchemaVersion,
    timestamp: String(data.timestamp || "").trim(),
    total: Number(data.total || 0),
    passed: Number(data.passed || 0),
    failed: Number(data.failed || 0),
    pass_rate: Number(data.pass_rate || 0),
    cases: Array.isArray(data.cases) ? data.cases.map((item) => normalizeRouterClassificationCase(item)) : [],
    violations: Array.isArray(data.violations) ? data.violations : [],
  };
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

function usageProviderLabel(provider) {
  const normalized = String(provider || "").trim().toLowerCase();
  if (normalized === "deepseek") return "DeepSeek";
  if (normalized === "web_search") return "Tavily";
  return "全部";
}

async function loadUsageTraces(provider = "all") {
  currentUsageTraceProvider = String(provider || "all").trim() || "all";
  const data = await apiGet(`/api/dashboard/usage/traces?days=7&limit=200&provider=${encodeURIComponent(currentUsageTraceProvider)}`);
  currentUsageTraceItems = Array.isArray(data?.items) ? data.items : [];
}

function renderUsageModal() {
  const webInput = document.getElementById("usage-web-input");
  const dsInput = document.getElementById("usage-deepseek-input");
  const totalCalls = currentUsageTraceItems.reduce((sum, item) => {
    const count = Number(item?.count || 1);
    return sum + (Number.isFinite(count) && count > 0 ? count : 1);
  }, 0);
  if (webInput) webInput.value = String(lastApiUsage.month_web_search_calls ?? "");
  if (dsInput) dsInput.value = String(lastApiUsage.month_deepseek_calls ?? "");
  if (usageProviderSelect) usageProviderSelect.value = currentUsageTraceProvider;
  if (usageModalMeta) {
    usageModalMeta.textContent = `最近7天: ${formatNum(currentUsageTraceItems.length)} 条记录 / ${formatNum(totalCalls)} 次调用 | 筛选: ${usageProviderLabel(currentUsageTraceProvider)}`;
  }
  if (usageTraceList) {
    usageTraceList.innerHTML = currentUsageTraceItems.length
      ? currentUsageTraceItems.map((item) => {
          const ts = escapeHtml(String(item?.timestamp || ""));
          const count = Math.max(1, Number(item?.count || 1) || 1);
          const provider = escapeHtml(String(item?.provider_label || usageProviderLabel(item?.provider || "")));
          const feature = escapeHtml(String(item?.feature || "未知功能"));
          const page = escapeHtml(String(item?.page || "未知页面"));
          const source = escapeHtml(String(item?.source || ""));
          const preview = escapeHtml(String(item?.message_preview || "")) || "—";
          const traceId = escapeHtml(String(item?.trace_id || ""));
          const meta = [provider, feature, page, source].filter(Boolean).join(" | ");
          const traceLine = traceId ? `<span class="dashboard-meta">trace_id: ${traceId}</span>` : "";
          const countBadge = count > 1 ? `<span class="usage-trace-count">x${formatNum(count)}</span>` : "";
          return `<li class="usage-trace-item" data-provider="${escapeHtml(String(item?.provider || ""))}"><strong>${ts}</strong>${countBadge}<br/><span class="dashboard-meta">${meta}</span><br/><span class="usage-trace-preview">${preview}</span>${traceLine ? `<br/>${traceLine}` : ""}</li>`;
        }).join("")
      : "<li>最近7天暂无 API 调用明细</li>";
  }
}

async function openUsageModal(provider = "all") {
  const modal = document.getElementById("usage-edit-modal");
  if (!modal) return;
  await loadUsageTraces(provider);
  renderUsageModal();
  modal.classList.remove("hidden");
  modal.setAttribute("aria-hidden", "false");
  document.getElementById("usage-web-input")?.focus();
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

async function clearUsageTraces() {
  await apiPost(`/api/dashboard/usage/traces?provider=${encodeURIComponent(currentUsageTraceProvider)}`, {}, "DELETE");
  currentUsageTraceItems = [];
  renderUsageModal();
  refreshDashboard({ force: false }).catch((err) => renderDashboardError(err));
}

async function exportUsageTracesCsv() {
  const resp = await fetch(`/api/dashboard/usage/traces/export?days=7&limit=5000&provider=${encodeURIComponent(currentUsageTraceProvider)}`);
  if (!resp.ok) throw new Error(`导出失败: HTTP ${resp.status}`);
  const text = await resp.text();
  const normalized = String(text || "").replace(/\r?\n/g, "\r\n");
  const blob = new Blob(["\uFEFF", normalized], { type: "text/csv;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const ts = new Date().toISOString().slice(0, 19).replace(/[:T]/g, "-");
  const a = document.createElement("a");
  a.href = url;
  a.download = `api_usage_traces_${currentUsageTraceProvider}_${ts}.csv`;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

// ─── Benchmark tab ────────────────────────────────────────────────────────────

let benchmarkEventSource = null;
let benchmarkHistory = [];
let benchmarkTimerInterval = null;
let activeBenchmarkJobId = "";
let benchmarkWatchingJobId = "";
let lastBenchmarkLogCount = 0;
let lastBenchmarkLogMarker = "";
const BENCHMARK_HISTORY_COLUMNS = 5;
let benchmarkCaseSets = [];
let routerClassificationCases = [];
let routerClassificationLastResults = [];
let editingRouterClassificationCaseId = "";
const TRACE_STAGE_COLORS = ["#7fc97f", "#beaed4", "#fdc086", "#ffff99", "#386cb0", "#f0027f", "#bf5b17", "#666666"];
const TRACE_STAGE_ORDER = [
  "session_prepare_seconds",
  "query_profile_seconds",
  "router_llm_classification_seconds",
  "router_entity_resolution_seconds",
  "router_alias_resolution_seconds",
  "router_followup_resolution_seconds",
  "router_semantic_repairs_seconds",
  "router_llm_rewrite_seconds",
  "router_non_llm_seconds",
  "router_normalization_seconds",
  "execution_plan_shape_seconds",
  "query_classification_finalize_seconds",
  "tool_planning_llm_seconds",
  "tool_planning_non_llm_seconds",
  "tool_planning_seconds",
  "planning_seconds",
  "vector_recall_seconds",
  "rerank_seconds",
  "context_assembly_seconds",
  "reference_limit_seconds",
  "per_item_expansion_seconds",
  "post_retrieval_evaluate_seconds",
  "post_retrieval_repairs_seconds",
  "answer_strategy_seconds",
  "guardrail_mode_seconds",
  "web_search_seconds",
  "tool_execution_seconds",
  "llm_seconds",
];
const TRACE_STAGE_LABELS = {
  session_prepare_seconds: "session_prepare",
  query_profile_seconds: "query_profile",
  router_llm_classification_seconds: "router_llm_classification",
  router_entity_resolution_seconds: "router_entity_resolution",
  router_alias_resolution_seconds: "router_alias_resolution",
  router_followup_resolution_seconds: "router_followup_resolution",
  router_semantic_repairs_seconds: "router_semantic_repairs",
  router_llm_rewrite_seconds: "router_llm_rewrite",
  router_non_llm_seconds: "router_non_llm",
  router_normalization_seconds: "router_normalization",
  execution_plan_shape_seconds: "execution_plan_shape",
  query_classification_finalize_seconds: "query_classification_finalize",
  tool_planning_llm_seconds: "tool_planning_llm",
  tool_planning_non_llm_seconds: "tool_planning_non_llm",
  tool_planning_seconds: "tool_planning",
  planning_seconds: "planning",
  vector_recall_seconds: "vector_recall",
  rerank_seconds: "rerank",
  context_assembly_seconds: "context_assembly",
  reference_limit_seconds: "reference_limit",
  per_item_expansion_seconds: "per_item_expansion",
  post_retrieval_evaluate_seconds: "post_retrieval_evaluate",
  post_retrieval_repairs_seconds: "post_retrieval_repairs",
  answer_strategy_seconds: "answer_strategy",
  guardrail_mode_seconds: "guardrail_mode",
  web_search_seconds: "web_search",
  tool_execution_seconds: "tool_execution",
  llm_seconds: "llm",
};
const TRACE_STAGE_DESCRIPTIONS = {
  planner: "planner 汇总本轮 query 理解、router、tool planning 与最终分类封装的总耗时。",
  execution: "execution 汇总实际工具执行墙钟时间，下面的明细用于解释这段时间内部发生了什么。",
  answer: "answer 汇总上下文拼装、回答策略、LLM 生成和最终整理等回答阶段耗时。",
  observability: "observability 汇总会话持久化和指标记录等收尾开销。",
  system: "system 仅负责内部口径核对；展示层统一只看 unaccounted。",
  tool_planning: "tool planning 是 planner 内部的子阶段，包含 router、执行计划成形和分类结果封装。",
  router: "router 是 planner 内部最核心的决策节点，负责 query 理解、followup 解析、别名解析和 LLM 辅助分类/改写。",
  session_prepare_seconds: "整理当前会话上下文、历史状态和入口参数，决定这一轮推理的初始工作面。",
  query_profile_seconds: "分析 query 的长度、结构和信号强弱，给后续路由和检索策略提供基础画像。",
  router_llm_classification_seconds: "planning 阶段里用于理解问题意图、domain 和 lookup_mode 的 LLM 分类耗时。",
  router_entity_resolution_seconds: "根据标题、作者、作品名等线索做实体解析和主实体选择的耗时。",
  router_alias_resolution_seconds: "用本地别名和标题词典做作品别名扩展、命中校正和媒体类型提示的耗时。",
  router_followup_resolution_seconds: "判断这一问是否承接上一轮，以及应该继承实体、筛选条件还是时间范围的耗时。",
  router_semantic_repairs_seconds: "对初始路由结果做语义修正，例如 creator collection、music compare、lookup_mode 修复的耗时。",
  router_llm_rewrite_seconds: "planning 阶段里生成 tool-grade retrieval query 的 LLM rewrite 耗时。",
  router_non_llm_seconds: "路由阶段中除 LLM 之外的本地判定、规则修复和状态整合耗时。",
  router_normalization_seconds: "把 router 决策规范化成执行前统一结构的耗时。",
  execution_plan_shape_seconds: "根据路由结果生成最终工具计划、上下文解析和执行选项的耗时。",
  query_classification_finalize_seconds: "把路由和执行计划序列化成 trace / UI 使用的 query classification 载荷的耗时。",
  tool_planning_llm_seconds: "tool planning 内部所有 LLM 调用的合计耗时。",
  tool_planning_non_llm_seconds: "tool planning 内部所有非 LLM 逻辑的合计耗时。",
  tool_planning_seconds: "在细化 planning 阶段时，确定要不要查库、检索文档、调用工具以及它们的顺序。",
  planning_seconds: "较粗粒度的整体路由规划时间；如果已经拆出更细阶段，这一项会被隐藏避免重复统计。",
  vector_recall_seconds: "向量检索召回候选文档或条目，决定后续可供排序和生成的原始素材范围。",
  rerank_seconds: "对召回结果进行重排，把最可能有用的内容提到前面。",
  context_assembly_seconds: "把最终入选的文档、片段和结构化结果拼成可供回答使用的上下文。",
  reference_limit_seconds: "控制引用数量和长度，避免上下文过长或引用噪声过多。",
  per_item_expansion_seconds: "对单条候选结果做额外展开、补字段或附加细节。",
  post_retrieval_evaluate_seconds: "在检索之后做质量评估，判断当前结果是否足够支撑回答。",
  post_retrieval_repairs_seconds: "对检索结果做补救，比如重试、修复排序、放宽阈值或补查缺漏。",
  answer_strategy_seconds: "决定最后回答采用什么策略，例如直接回答、解释型回答或保守降级。",
  guardrail_mode_seconds: "执行保护逻辑，判断是否需要走 no-context、保守回答或特殊降级分支。",
  web_search_seconds: "调用外部 Web 搜索的耗时。",
  tool_execution_seconds: "执行实际工具调用的总耗时，例如查库、检索、聚合外部结果。",
  execution_seconds: "execution 聚合时间，对应工具执行墙钟时间。",
  answer_seconds: "answer 聚合时间，对应回答阶段所有叶子耗时之和。",
  observability_seconds: "observability 聚合时间，对应持久化与指标记录耗时。",
  unaccounted_seconds: "未归类剩余时间，按 section totals 与 wall-clock 的差值计算，不再和叶子节点混算。",
  llm_seconds: "最终进入语言模型推理和生成回答的耗时。",
};
let benchmarkLiveLatestCase = null;
let benchmarkLiveLatestCaseKey = "";
let benchmarkLatestTraceRenderSeq = 0;
let benchmarkLatestTraceCacheKey = "";

// Current test-set selection: "<module>/<length>"
let currentBmTestSet = "rag/short";

const traceSummaryRenderer = DashboardSharedTrace?.createTraceSummaryRenderer?.({
  escapeHtml,
  formatDuration,
  formatSigned,
  computeRerankOptimization,
  renderRerankOptimization,
  traceStageColors: TRACE_STAGE_COLORS,
  traceStageOrder: TRACE_STAGE_ORDER,
  traceStageLabels: TRACE_STAGE_LABELS,
  traceStageDescriptions: TRACE_STAGE_DESCRIPTIONS,
}) || null;

function formatTraceNumber(value, digits = 4) {
  return traceSummaryRenderer?.formatTraceNumber?.(value, digits) || "—";
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

function downloadJsonFile(payload, filename) {
  const blob = new Blob([JSON.stringify(payload ?? {}, null, 2)], { type: "application/json;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

function formatTracePercent(value) {
  return traceSummaryRenderer?.formatTracePercent?.(value) || "—";
}

function formatTraceStageLabel(key) {
  return traceSummaryRenderer?.formatTraceStageLabel?.(key) || "";
}

function getTraceWallClockSeconds(trace) {
  return traceSummaryRenderer?.getTraceWallClockSeconds?.(trace) || 0;
}

function getTraceStageDescription(key) {
  return traceSummaryRenderer?.getTraceStageDescription?.(key) || "";
}

function renderTraceStageBars(trace) {
  return traceSummaryRenderer?.renderTraceStageBars?.(trace) || '<div class="trace-result-empty">暂无阶段时延</div>';
}

function renderTraceSummary(trace) {
  return traceSummaryRenderer?.renderTraceSummary?.(trace) || '<div class="trace-result-empty">未找到 trace 数据</div>';
}

function renderTraceModalSummary(trace) {
  return dashboardTraceBootstrap.renderTraceModalSummary(trace);
}

const dashboardDataAdminBootstrap = DashboardDataAdminBootstrapModule?.createDataAdminBootstrap?.({
  module: DashboardDataAdminModule,
  createMissingController,
  controllerDeps: {
    fetchImpl: fetch,
    apiPost,
    apiPostForm,
    authorizationHeaders,
    extractErrorDetail,
    selectedApps: selectedDataBackupApps,
    renderPanel: renderDataBackupPanel,
    refreshDashboard,
    showToast,
    exportButton: dataBackupExportBtn,
    createButton: dataBackupCreateBtn,
    restoreButton: dataBackupRestoreBtn,
    restoreInput: dataBackupRestoreInput,
  },
}) || (() => {
  const controller = createMissingController("NavDashboardDataAdmin", ["bindEvents"]);
  return { controller, bindEvents() {} };
})();
const dashboardDataAdminController = dashboardDataAdminBootstrap.controller;

const dashboardTraceBootstrap = DashboardTraceBootstrapModule?.createTraceBootstrap?.({
  module: DashboardTraceModule,
  createMissingController,
  controllerDeps: {
    apiGet,
    normalizeTraceResponse,
    showAppErrorModal,
    formatDuration,
    getTraceWallClockSeconds,
    renderTraceSummary,
    escapeHtml,
    downloadTextFile,
    getCurrentTraceRecord: () => currentTraceRecord,
    setCurrentTraceRecord: (value) => {
      currentTraceRecord = value;
    },
    getCurrentTraceExportText: () => currentTraceExportText,
    setCurrentTraceExportText: (value) => {
      currentTraceExportText = String(value || "");
    },
    getElements: () => ({
      dashboardTraceMeta,
      dashboardTraceOpenBtn,
      dashboardTraceTicketBtn,
      dashboardTraceResult,
      dashboardTraceInput,
      traceModalController,
      traceModal,
      traceModalMeta,
      traceModalContent,
      traceModalExport,
    }),
  },
  bindDataAdminEvents: () => dashboardDataAdminBootstrap.bindEvents(),
  dashboardTraceQueryBtn,
  dashboardTraceOpenBtn,
  dashboardTraceTicketBtn,
  dashboardTraceInput,
  dashboardTraceResult,
  onTraceStageClick: _traceStageClickHandler,
  onTraceStageHover: _traceStageHoverHandler,
  clearTraceStageTooltip,
  closeAllTraceStageTooltips,
  showAppErrorModal,
  openTicketFromCurrentTrace: () => openTicketFromCurrentTrace(),
}) || (() => {
  const controller = createMissingController(
    "NavDashboardTrace",
    ["renderTraceModalSummary", "renderDashboardTrace", "closeTraceModal"],
    ["fetchTrace", "lookupDashboardTrace", "openTraceModal", "exportCurrentTrace"],
    {
      renderTraceModalSummary: () => '<div class="trace-result-empty">Trace 模块未加载</div>',
    },
  );
  return {
    controller,
    bindEvents() {},
    renderTraceModalSummary: (trace) => controller.renderTraceModalSummary(trace),
    renderDashboardTrace: (...args) => controller.renderDashboardTrace(...args),
    fetchTrace: (...args) => controller.fetchTrace(...args),
    lookupDashboardTrace: (...args) => controller.lookupDashboardTrace(...args),
    closeTraceModal: (...args) => controller.closeTraceModal(...args),
    openTraceModal: (...args) => controller.openTraceModal(...args),
    exportCurrentTrace: (...args) => controller.exportCurrentTrace(...args),
  };
})();
const dashboardTraceController = dashboardTraceBootstrap.controller;
const renderDashboardTrace = (...args) => dashboardTraceBootstrap.renderDashboardTrace(...args);
const fetchTrace = (...args) => dashboardTraceBootstrap.fetchTrace(...args);
const lookupDashboardTrace = (...args) => dashboardTraceBootstrap.lookupDashboardTrace(...args);
const closeTraceModal = (...args) => dashboardTraceBootstrap.closeTraceModal(...args);
const openTraceModal = (...args) => dashboardTraceBootstrap.openTraceModal(...args);
const exportCurrentTrace = (...args) => dashboardTraceBootstrap.exportCurrentTrace(...args);

const dashboardTicketsBootstrap = DashboardTicketsBootstrapModule?.createTicketsBootstrap?.({
  module: DashboardTicketsModule,
  createMissingController,
  controllerDeps: {
    modalApi: DashboardSharedModal,
    ensureSelectValue,
    syncTicketsPaneHeights,
    showAppErrorModal,
    escapeHtml,
    getState: () => ({
      currentTicketId,
      pendingDeleteTicketId,
      currentTickets,
    }),
    setState: (patch) => {
      if (!patch || typeof patch !== "object") return;
      if (Object.prototype.hasOwnProperty.call(patch, "currentTicketId")) currentTicketId = String(patch.currentTicketId || "");
      if (Object.prototype.hasOwnProperty.call(patch, "pendingDeleteTicketId")) pendingDeleteTicketId = String(patch.pendingDeleteTicketId || "");
    },
    getElements: () => ({
      ticketIdInput,
      ticketTraceIdInput,
      ticketTitleInput,
      ticketStatusInput,
      ticketPriorityInput,
      ticketDomainInput,
      ticketCategoryInput,
      ticketCreatedAtInput,
      ticketUpdatedAtInput,
      ticketRelatedTracesInput,
      ticketRelatedTracesLinks,
      ticketReproQueryInput,
      ticketSummaryInput,
      ticketExpectedBehaviorInput,
      ticketActualBehaviorInput,
      ticketRootCauseInput,
      ticketFixNotesInput,
      ticketAdditionalNotesInput,
      ticketsDetailMeta,
      ticketsDeleteBtn,
      ticketDeleteModal,
      ticketDeleteModalController,
      ticketDeleteMeta,
      ticketDeleteConfirmSelect,
      ticketDeleteConfirmBtn,
    }),
  },
  ticketsRefreshBtn,
  ticketsNewBtn,
  ticketPasteFillBtn,
  ticketsAIDraftBtn,
  ticketsSaveBtn,
  ticketsDeleteBtn,
  ticketsList,
  ticketRelatedTracesLinks,
  ticketFilterNodes: [ticketsStatusFilter, ticketsPriorityFilter, ticketsDomainFilter, ticketsCategoryFilter, ticketsCreatedFrom, ticketsCreatedTo],
  ticketsSearchInput,
  ticketRelatedTracesInput,
  ticketPasteInput,
  ticketDeleteConfirmSelect,
  ticketsSortToggleBtn,
  ticketsListCollapseBtn,
  ticketDeleteConfirmBtn,
  ticketDeleteCancelBtn,
  refreshTickets,
  resetTicketEditor,
  fillTicketFromPaste,
  createTicketAIDraft,
  saveCurrentTicket,
  openTraceModal,
  showAppErrorModal,
  debugUiEvent,
  renderTicketsList,
  renderTicketTraceLinks,
  syncTicketsPaneHeights,
  findTicketById: (ticketId) => currentTickets.find((entry) => String(entry.ticket_id || "") === ticketId),
  toggleTicketSort: () => {
    currentTicketSort = currentTicketSort === "updated_asc" ? "updated_desc" : "updated_asc";
  },
  renderTicketSortButton,
  isTicketsListCollapsed: () => ticketsShellController?.isCollapsed?.() ?? ticketsListCollapsed,
  setTicketsListCollapsed,
  confirmDeleteCurrentTicket,
}) || (() => {
  const controller = createMissingController("NavDashboardTickets", ["applyTicketToForm", "closeTicketDeleteModal", "openTicketDeleteModal"]);
  return {
    controller,
    bindEvents() {},
    applyTicketToForm: (ticket) => controller.applyTicketToForm(ticket),
    closeTicketDeleteModal: (...args) => controller.closeTicketDeleteModal(...args),
    openTicketDeleteModal: (...args) => controller.openTicketDeleteModal(...args),
  };
})();
const dashboardTicketsController = dashboardTicketsBootstrap.controller;

const dashboardLibraryAliasController = DashboardLibraryAliasModule?.createLibraryAliasController?.({
  modalApi: DashboardSharedModal,
  apiGet,
  apiPost,
  refreshDashboard,
  showAppErrorModal,
  showToast,
  escapeHtml,
  formatNum,
  formatDateTimeLabel,
  getState: () => ({
    currentLibraryAliasItems,
    currentLibraryAliasPage,
    currentLibraryAliasTotalPages,
    currentLibraryAliasSummary,
    activeLibraryAliasEditProposalId,
    activeLibraryAliasSaveProposalId,
    currentLibraryAliasEditDraft,
  }),
  setState: (patch) => {
    if (!patch || typeof patch !== "object") return;
    if (Object.prototype.hasOwnProperty.call(patch, "currentLibraryAliasItems")) currentLibraryAliasItems = Array.isArray(patch.currentLibraryAliasItems) ? patch.currentLibraryAliasItems : [];
    if (Object.prototype.hasOwnProperty.call(patch, "currentLibraryAliasPage")) currentLibraryAliasPage = Math.max(1, Number(patch.currentLibraryAliasPage || 1));
    if (Object.prototype.hasOwnProperty.call(patch, "currentLibraryAliasTotalPages")) currentLibraryAliasTotalPages = Math.max(1, Number(patch.currentLibraryAliasTotalPages || 1));
    if (Object.prototype.hasOwnProperty.call(patch, "currentLibraryAliasSummary")) currentLibraryAliasSummary = patch.currentLibraryAliasSummary && typeof patch.currentLibraryAliasSummary === "object" ? patch.currentLibraryAliasSummary : {};
    if (Object.prototype.hasOwnProperty.call(patch, "activeLibraryAliasEditProposalId")) activeLibraryAliasEditProposalId = String(patch.activeLibraryAliasEditProposalId || "");
    if (Object.prototype.hasOwnProperty.call(patch, "activeLibraryAliasSaveProposalId")) activeLibraryAliasSaveProposalId = String(patch.activeLibraryAliasSaveProposalId || "");
    if (Object.prototype.hasOwnProperty.call(patch, "currentLibraryAliasEditDraft")) currentLibraryAliasEditDraft = patch.currentLibraryAliasEditDraft && typeof patch.currentLibraryAliasEditDraft === "object" ? patch.currentLibraryAliasEditDraft : { aliasesText: "" };
  },
  getElements: () => ({
    libraryAliasModal,
    libraryAliasModalController,
    libraryAliasModalMeta,
    libraryAliasReviewList,
    libraryAliasPageInfo,
    libraryAliasPagePrevBtn,
    libraryAliasPageNextBtn,
    libraryAliasCloseBtn,
  }),
}) || createMissingController(
  "NavDashboardLibraryAlias",
  ["closeLibraryAliasModal", "renderLibraryAliasProposalModal", "renderCurrentLibraryAliasModal", "bindEvents"],
  ["loadLibraryAliasProposalPage", "openLibraryAliasModal", "submitLibraryAliasReview"],
  { isOpen: () => false }
);

function parseTicketTraceList(value) {
  return DashboardTicketsModule?.parseTicketTraceList?.(value) || [];
}

function closeLibraryAliasModal() {
  return dashboardLibraryAliasController.closeLibraryAliasModal();
}

function renderLibraryAliasProposalModal(payload) {
  return dashboardLibraryAliasController.renderLibraryAliasProposalModal(payload);
}

function renderCurrentLibraryAliasModal() {
  return dashboardLibraryAliasController.renderCurrentLibraryAliasModal();
}

async function loadLibraryAliasProposalPage(page = 1) {
  return dashboardLibraryAliasController.loadLibraryAliasProposalPage(page);
}

async function openLibraryAliasModal() {
  return dashboardLibraryAliasController.openLibraryAliasModal();
}

async function submitLibraryAliasReview(proposalId, aliases) {
  return dashboardLibraryAliasController.submitLibraryAliasReview(proposalId, aliases);
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
  const normalizePriority = (value) => DashboardTicketsModule?.normalizeTicketPriority?.(value) || String(value || "").trim();
  const rawSelectedValue = String(currentValue || select.value || fallbackValue).trim() || fallbackValue;
  const selectedValue = select === ticketsPriorityFilter && rawSelectedValue !== "all"
    ? normalizePriority(rawSelectedValue)
    : rawSelectedValue;
  const dynamic = Array.isArray(values)
    ? values
      .map((item) => select === ticketsPriorityFilter ? normalizePriority(item) : String(item || "").trim())
      .filter(Boolean)
      .filter((item, index, list) => list.indexOf(item) === index)
    : [];
  const staticOptions = select === ticketsStatusFilter
    ? ['<option value="non_closed">非关闭</option>', '<option value="all">全部</option>']
    : ['<option value="all">全部</option>'];
  select.innerHTML = [...staticOptions, ...dynamic.map((item) => `<option value="${escapeHtml(item)}">${escapeHtml(item)}</option>`)].join("");
  ensureSelectValue(select, selectedValue, { allowAll: true });
}

function ticketBadgeClass(prefix, value) {
  return DashboardTicketsModule?.ticketBadgeClass?.(prefix, value) || "";
}

function renderTicketTraceLinks(traces) {
  DashboardTicketsModule?.renderTicketTraceLinks?.(ticketRelatedTracesLinks, traces, escapeHtml);
}

function buildEmptyTicket() {
  return DashboardTicketsModule?.buildEmptyTicket?.() || {};
}

function mergeTicketDraft(currentTicket, draftTicket) {
  return DashboardTicketsModule?.mergeTicketDraft?.(currentTicket, draftTicket) || { ...(currentTicket || {}), ...(draftTicket || {}) };
}

function syncTicketsPaneHeights() {
  if (!(ticketsListShell instanceof HTMLElement)) return;
  if (!(ticketsDetailShell instanceof HTMLElement) || ticketsListCollapsed || isCompactTicketsLayout()) {
    ticketsListShell.style.height = "";
    ticketsListShell.style.maxHeight = "";
    if (ticketsDetailShell instanceof HTMLElement) {
      ticketsDetailShell.style.maxHeight = "";
      ticketsDetailShell.style.overflow = "";
    }
    return;
  }
  const detailHeight = Math.ceil(ticketsDetailShell.getBoundingClientRect().height || 0);
  if (detailHeight <= 0) return;
  const shellTop = ticketsListShell.getBoundingClientRect().top;
  const panelBottom = document.getElementById("panel-tickets")?.getBoundingClientRect().bottom || window.innerHeight;
  const viewportCap = Math.max(320, Math.floor(panelBottom - shellTop - 12));
  const targetHeight = Math.max(320, Math.min(detailHeight, viewportCap));
  ticketsListShell.style.height = `${targetHeight}px`;
  ticketsListShell.style.maxHeight = `${targetHeight}px`;
  ticketsDetailShell.style.maxHeight = `${targetHeight}px`;
  ticketsDetailShell.style.overflow = "auto";
}

function setupTicketsPaneHeightSync() {
  if (!(ticketsDetailShell instanceof HTMLElement)) return;
  if (ticketsDetailResizeObserver) ticketsDetailResizeObserver.disconnect();
  if (typeof ResizeObserver === "function") {
    ticketsDetailResizeObserver = new ResizeObserver(() => {
      syncTicketsPaneHeights();
    });
    ticketsDetailResizeObserver.observe(ticketsDetailShell);
  }
  syncTicketsPaneHeights();
}

function applyTicketToForm(ticket) {
  dashboardTicketsBootstrap.applyTicketToForm(ticket);
}

function closeTicketDeleteModal() {
  dashboardTicketsBootstrap.closeTicketDeleteModal();
}

function openTicketDeleteModal() {
  dashboardTicketsBootstrap.openTicketDeleteModal();
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

function applyTicketDateFilters(appliedFilters, fallbackFilters = null) {
  const source = appliedFilters && typeof appliedFilters === "object"
    ? appliedFilters
    : (fallbackFilters && typeof fallbackFilters === "object" ? fallbackFilters : {});
  if (ticketsCreatedFrom) ticketsCreatedFrom.value = String(source.created_from || "");
  if (ticketsCreatedTo) ticketsCreatedTo.value = String(source.created_to || "");
}

function renderTicketSortButton() {
  if (!ticketsSortToggleBtn) return;
  ticketsSortToggleBtn.textContent = currentTicketSort === "updated_asc" ? "最早优先" : "最新优先";
}

function isCompactTicketsLayout() {
  return ticketsShellController?.isCompactLayout?.() || (typeof window !== "undefined" && window.matchMedia("(max-width: 980px)").matches);
}

function renderTicketsListCollapseButton() {
  if (ticketsShellController?.syncToggle) {
    ticketsShellController.syncToggle();
    return;
  }
  if (!ticketsListCollapseBtn) return;
  ticketsListCollapseBtn.textContent = ticketsListCollapsed
    ? (isCompactTicketsLayout() ? "向下展开" : ">")
    : (isCompactTicketsLayout() ? "向上折叠" : "<");
  ticketsListCollapseBtn.setAttribute("aria-expanded", String(!ticketsListCollapsed));
  ticketsListCollapseBtn.setAttribute("aria-label", ticketsListCollapseBtn.textContent || "折叠 Ticket 列表");
}

function applyTicketsListLayoutState() {
  if (ticketsShellController?.syncLayout) {
    ticketsShellController.syncLayout();
    return;
  }
  ticketsMainGrid?.classList.toggle("is-list-collapsed", ticketsListCollapsed);
  ticketsListShell?.classList.toggle("is-collapsed", ticketsListCollapsed);
  renderTicketsListCollapseButton();
  syncTicketsPaneHeights();
}

function setTicketsListCollapsed(nextValue) {
  if (ticketsShellController?.setCollapsed) {
    ticketsShellController.setCollapsed(nextValue);
    ticketsListCollapsed = ticketsShellController.isCollapsed();
    return;
  }
  ticketsListCollapsed = Boolean(nextValue);
  try {
    window.localStorage.setItem(TICKETS_LIST_COLLAPSED_STORAGE_KEY, ticketsListCollapsed ? "1" : "0");
  } catch (_) {
    // Ignore localStorage failures and keep the in-memory state.
  }
  applyTicketsListLayoutState();
}

try {
  ticketsListCollapsed = ticketsShellController?.isCollapsed?.() || window.localStorage.getItem(TICKETS_LIST_COLLAPSED_STORAGE_KEY) === "1";
} catch (_) {
  ticketsListCollapsed = false;
}

applyTicketsListLayoutState();

function renderTicketsList() {
  if (!ticketsList) return;
  ticketsList.innerHTML = DashboardTicketsModule?.renderTicketsListMarkup?.(currentTickets, currentTicketId, escapeHtml) || "";
  syncTicketsPaneHeights();
}

async function fillTicketFromPaste() {
  const raw = String(ticketPasteInput?.value || "").trim();
  if (!raw) {
    showAppErrorModal("BUG-TICKET 填充失败", "请先粘贴 BUG-TICKET 文本");
    return;
  }
  const data = await apiPost("/api/dashboard/tickets/parse", { text: raw });
  const merged = mergeTicketDraft(
    {
      ...collectTicketFormPayload(),
      ticket_id: String(ticketIdInput?.value || currentTicketId || ""),
      created_at: String(ticketCreatedAtInput?.value || ""),
      updated_at: String(ticketUpdatedAtInput?.value || ""),
    },
    data?.ticket || {},
  );
  applyTicketToForm(merged);
  if (ticketPasteInput) ticketPasteInput.value = "";
}

async function refreshTickets({ keepSelection = true, selectTicketId = "" } = {}) {
  const params = collectTicketFilterParams();
  currentTicketStatusFilter = String(params.status || "non_closed").trim() || "non_closed";
  renderTicketSortButton();
  if (ticketsMeta) ticketsMeta.textContent = "正在加载 tickets...";
  const query = new URLSearchParams(params).toString();
  const data = await apiGet(`/api/dashboard/tickets?${query}`);
  currentTickets = Array.isArray(data?.items) ? data.items : [];
  applyTicketDateFilters(data?.applied_filters, params);
  populateTicketFilterSelect(ticketsStatusFilter, data?.filters?.statuses, params.status);
  populateTicketFilterSelect(ticketsPriorityFilter, data?.filters?.priorities, params.priority);
  populateTicketFilterSelect(ticketsDomainFilter, data?.filters?.domains, params.domain);
  populateTicketFilterSelect(ticketsCategoryFilter, data?.filters?.categories, params.category);
  if (ticketsMeta) ticketsMeta.textContent = `当前 ${formatNum(data?.count || currentTickets.length)} 条 ticket`;

  const preferredId = String(selectTicketId || (keepSelection ? currentTicketId : "")).trim();
  const nextTicket = preferredId ? currentTickets.find((ticket) => String(ticket.ticket_id || "") === preferredId) : null;
  if (nextTicket) {
    applyTicketToForm(nextTicket);
  } else if (!keepSelection || !preferredId || (preferredId && !nextTicket)) {
    applyTicketToForm(buildEmptyTicket());
  }
  renderTicketsList();
}

async function bootstrapTicketsTab() {
  if (ticketsState.loaded || ticketsState.loading) return;
  ticketsState.loading = true;
  const defaultDateRange = getDefaultTicketDateRange();

  // Paint from SSR prefill immediately — no network request needed.
  const prefill = loadTicketsPrefill();
  if (prefill) {
    currentTickets = Array.isArray(prefill.items) ? prefill.items : [];
    currentTicketStatusFilter = String(prefill.applied_filters?.status || currentTicketStatusFilter || "non_closed").trim() || "non_closed";
    populateTicketFilterSelect(ticketsStatusFilter, prefill.filters?.statuses, currentTicketStatusFilter);
    populateTicketFilterSelect(ticketsPriorityFilter, prefill.filters?.priorities, "all");
    populateTicketFilterSelect(ticketsDomainFilter, prefill.filters?.domains, "all");
    populateTicketFilterSelect(ticketsCategoryFilter, prefill.filters?.categories, "all");
    applyTicketDateFilters(prefill.applied_filters, defaultDateRange);
    if (ticketsMeta) ticketsMeta.textContent = `当前 ${formatNum(prefill.count || currentTickets.length)} 条 ticket`;
    renderTicketsList();
    setupTicketsPaneHeightSync();
    ticketsState.loaded = true;
    ticketsState.error = null;
    ticketsState.loading = false;
    // Background refresh to pick up any updates since page load.
    refreshTickets({ keepSelection: true }).catch((err) => {
      console.warn("[tickets] background refresh failed:", err);
    });
    return;
  }

  // No SSR prefill: blocking fetch (fallback for cold/missing data).
  try {
    if (ticketsCreatedFrom && !ticketsCreatedFrom.value) ticketsCreatedFrom.value = defaultDateRange.created_from;
    if (ticketsCreatedTo && !ticketsCreatedTo.value) ticketsCreatedTo.value = defaultDateRange.created_to;
    await refreshTickets({ keepSelection: false });
    setupTicketsPaneHeightSync();
    ticketsState.loaded = true;
    ticketsState.error = null;
  } catch (err) {
    ticketsState.error = err;
    ticketsState.loaded = false;
    throw err;
  } finally {
    ticketsState.loading = false;
  }
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
  if (!ticketsState.loaded) {
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
  return dashboardBenchmarkBootstrap.getBenchmarkModuleLabel(module);
}

const dashboardBenchmarkBootstrap = DashboardBenchmarkBootstrapModule?.createBenchmarkBootstrap?.({
  module: DashboardBenchmarkModule,
  createMissingController,
  controllerDeps: {
  apiPost,
  apiGet,
  apiDelete,
  pollJsonJob,
  normalizeBenchmarkHistoryResponse,
  normalizeBenchmarkCaseSetsResponse,
  normalizeBenchmarkCaseSet,
  normalizeBenchmarkJobResponse,
  normalizeRouterClassificationCasesResponse,
  normalizeRouterClassificationRunResponse,
  loadBenchmarkPrefill,
  historyColumns: BENCHMARK_HISTORY_COLUMNS,
  renderBenchmarkCaseSetOptions,
  renderBenchmarkQualityPanels,
  syncBenchmarkCountOptions,
  fetchTrace,
  renderTraceSummary,
  syncBenchmarkLogBox,
  escapeHtml,
  showAppErrorModal,
  getCurrentBenchmarkTestSet: () => currentBmTestSet,
  getBenchmarkLatestCase: (results, liveLatestCase) => DashboardBenchmarkModule.getLatestBenchmarkCase(results, liveLatestCase),
  summarizeAssertionScope,
  summarizeAssertionFailures,
  summarizeBenchmarkContractFailures,
  renderBenchmarkContractFailureSummary,
  modalApi: DashboardSharedModal,
  debugUiEvent,
  setCurrentBenchmarkTestSet: (value) => {
    currentBmTestSet = String(value || currentBmTestSet || "rag/short");
  },
  getState: () => ({
    history: benchmarkHistory,
    liveLatestCase: benchmarkLiveLatestCase,
    liveLatestCaseKey: benchmarkLiveLatestCaseKey,
    latestTraceRenderSeq: benchmarkLatestTraceRenderSeq,
    latestTraceCacheKey: benchmarkLatestTraceCacheKey,
    activeBenchmarkJobId,
    lastBenchmarkLogCount,
    lastBenchmarkLogMarker,
    caseSets: benchmarkCaseSets,
    routerClassificationCases,
    routerClassificationLastResults,
    editingRouterClassificationCaseId,
    benchmarkState,
  }),
  setState: (patch) => {
    if (!patch || typeof patch !== "object") return;
    if (Object.prototype.hasOwnProperty.call(patch, "history")) benchmarkHistory = Array.isArray(patch.history) ? patch.history : [];
    if (Object.prototype.hasOwnProperty.call(patch, "liveLatestCase")) benchmarkLiveLatestCase = patch.liveLatestCase || null;
    if (Object.prototype.hasOwnProperty.call(patch, "liveLatestCaseKey")) benchmarkLiveLatestCaseKey = String(patch.liveLatestCaseKey || "");
    if (Object.prototype.hasOwnProperty.call(patch, "latestTraceRenderSeq")) benchmarkLatestTraceRenderSeq = Number(patch.latestTraceRenderSeq || 0);
    if (Object.prototype.hasOwnProperty.call(patch, "latestTraceCacheKey")) benchmarkLatestTraceCacheKey = String(patch.latestTraceCacheKey || "");
    if (Object.prototype.hasOwnProperty.call(patch, "activeBenchmarkJobId")) activeBenchmarkJobId = String(patch.activeBenchmarkJobId || "");
    if (Object.prototype.hasOwnProperty.call(patch, "lastBenchmarkLogCount")) lastBenchmarkLogCount = Number(patch.lastBenchmarkLogCount || 0);
    if (Object.prototype.hasOwnProperty.call(patch, "lastBenchmarkLogMarker")) lastBenchmarkLogMarker = String(patch.lastBenchmarkLogMarker || "");
    if (Object.prototype.hasOwnProperty.call(patch, "caseSets")) benchmarkCaseSets = Array.isArray(patch.caseSets) ? patch.caseSets : [];
    if (Object.prototype.hasOwnProperty.call(patch, "routerClassificationCases")) routerClassificationCases = Array.isArray(patch.routerClassificationCases) ? patch.routerClassificationCases : [];
    if (Object.prototype.hasOwnProperty.call(patch, "routerClassificationLastResults")) routerClassificationLastResults = Array.isArray(patch.routerClassificationLastResults) ? patch.routerClassificationLastResults : [];
    if (Object.prototype.hasOwnProperty.call(patch, "editingRouterClassificationCaseId")) editingRouterClassificationCaseId = String(patch.editingRouterClassificationCaseId || "");
  },
  getElements: () => ({
    testSetSelect: document.getElementById("bm-testset-select"),
    lastRun: document.getElementById("benchmark-last-run"),
    caseTraceContainer: document.getElementById("bm-case-trace-list"),
    caseTraceMeta: document.getElementById("bm-case-trace-meta"),
    historyTable: document.getElementById("bm-history-table"),
    caseSetSelect: document.getElementById("bm-case-set-select"),
    benchmarkRagInput: document.getElementById("bm-rag"),
    benchmarkAgentInput: document.getElementById("bm-agent"),
    benchmarkHybridInput: document.getElementById("bm-hybrid"),
    benchmarkCountInputs: Array.from(document.querySelectorAll("input[name='bm-count']")),
    qualitySummary: document.getElementById("bm-quality-summary"),
    qualityTable: document.getElementById("bm-taxonomy-table"),
    qualityMeta: document.getElementById("bm-quality-meta"),
    qualityContract: document.getElementById("bm-quality-contract"),
    routerClsRunBtn: document.getElementById("bm-router-cls-run"),
    routerClsMeta: document.getElementById("bm-router-cls-meta"),
    routerClsSummary: document.getElementById("bm-router-cls-summary"),
    routerClsTableWrap: document.getElementById("bm-router-cls-table-wrap"),
    routerClsTbody: document.getElementById("bm-router-cls-tbody"),
    routerClsAddBtn: benchmarkRouterClsAddBtn,
    routerClsModal: benchmarkRouterClsModal,
    routerClsModalController: benchmarkRouterClsModalController,
    routerClsModalTitle: benchmarkRouterClsModalTitle,
    routerClsModalMeta: benchmarkRouterClsModalMeta,
    routerClsTraceIdInput: benchmarkRouterClsTraceIdInput,
    routerClsImportBtn: benchmarkRouterClsImportBtn,
    routerClsQueryInput: benchmarkRouterClsQueryInput,
    routerClsExpectedDomainInput: benchmarkRouterClsExpectedDomainInput,
    routerClsExpectedArbitrationInput: benchmarkRouterClsExpectedArbitrationInput,
    routerClsNoteInput: benchmarkRouterClsNoteInput,
    routerClsMockLabelInput: benchmarkRouterClsMockLabelInput,
    routerClsMockDomainInput: benchmarkRouterClsMockDomainInput,
    routerClsMockLookupModeInput: benchmarkRouterClsMockLookupModeInput,
    routerClsMockEntitiesInput: benchmarkRouterClsMockEntitiesInput,
    routerClsLastRunBox: benchmarkRouterClsLastRunBox,
    routerClsSaveBtn: benchmarkRouterClsSaveBtn,
    routerClsDeleteBtn: benchmarkRouterClsDeleteBtn,
    routerClsCancelBtn: benchmarkRouterClsCancelBtn,
  }),
  },
  runButton: document.getElementById("bm-run-btn"),
  abortButton: document.getElementById("bm-abort-btn"),
  unitTestsButton: document.getElementById("bm-unit-tests-run"),
  caseTraceRefreshButton: benchmarkCaseTraceRefreshBtn,
  caseTraceList: document.getElementById("bm-case-trace-list"),
  onTraceStageClick: _traceStageClickHandler,
  onTraceStageHover: _traceStageHoverHandler,
  closeAllTraceStageTooltips,
  openTraceModal,
  showAppErrorModal,
  debugUiEvent,
  runBenchmark,
  runUnitTests,
  getActiveBenchmarkJobId: () => activeBenchmarkJobId,
  resumeSharedBenchmarkJobUi,
}) || (() => {
  const controller = createMissingController(
    "NavDashboardBenchmark",
    [
      "renderBenchmarkCaseSetOptions",
      "renderBenchmarkQualityPanels",
      "getBenchmarkLatestCaseKey",
      "renderBenchmarkLatestCaseFallback",
      "renderBenchmarkTable",
      "syncBenchmarkCountOptions",
      "clearHistoryState",
      "bindBenchmarkConfigEvents",
      "bindRouterClassificationEvents",
    ],
    [
      "loadBenchmarkHistory",
      "renderBenchmarkCaseTraceList",
      "updateBenchmarkLiveStateFromJob",
      "refreshBenchmarkLatestCase",
      "bootstrapBenchmarkTab",
      "resumeSharedBenchmarkJob",
      "loadRouterClassificationCases",
      "importRouterClassificationTrace",
      "openRouterClassificationEditor",
      "closeRouterClassificationEditor",
      "saveRouterClassificationCase",
      "deleteRouterClassificationCase",
      "runRouterClassification",
      "resetLiveCaseState",
      "runBenchmarkAction",
      "cancelBenchmarkRun",
    ],
    {
      getBenchmarkLatestCaseKey: () => "",
      renderBenchmarkLatestCaseFallback: () => '<div class="trace-result-empty">Benchmark 模块未加载</div>',
    },
  );
  return {
    controller,
    bindEvents() {},
    getBenchmarkModuleLabel: (module) => DashboardBenchmarkModule?.getBenchmarkModuleLabel?.(module) || String(module || "").toUpperCase(),
    renderBenchmarkQualityPanels: (...args) => controller.renderBenchmarkQualityPanels(...args),
    loadBenchmarkHistory: (...args) => controller.loadBenchmarkHistory(...args),
    getBenchmarkLatestCaseKey: (...args) => controller.getBenchmarkLatestCaseKey(...args),
    renderBenchmarkLatestCaseFallback: (...args) => controller.renderBenchmarkLatestCaseFallback(...args),
    renderBenchmarkCaseTraceList: (...args) => controller.renderBenchmarkCaseTraceList(...args),
    updateBenchmarkLiveStateFromJob: (...args) => controller.updateBenchmarkLiveStateFromJob(...args),
    refreshBenchmarkLatestCase: (...args) => controller.refreshBenchmarkLatestCase(...args),
    renderBenchmarkTable: (...args) => controller.renderBenchmarkTable(...args),
    bootstrapBenchmarkTab: (...args) => controller.bootstrapBenchmarkTab(...args),
    resumeSharedBenchmarkJob: (...args) => controller.resumeSharedBenchmarkJob(...args),
    syncBenchmarkCountOptions: (...args) => controller.syncBenchmarkCountOptions(...args),
    loadRouterClassificationCases: (...args) => controller.loadRouterClassificationCases(...args),
    importRouterClassificationTrace: (...args) => controller.importRouterClassificationTrace(...args),
    openRouterClassificationEditor: (...args) => controller.openRouterClassificationEditor(...args),
    closeRouterClassificationEditor: (...args) => controller.closeRouterClassificationEditor(...args),
    saveRouterClassificationCase: (...args) => controller.saveRouterClassificationCase(...args),
    deleteRouterClassificationCase: (...args) => controller.deleteRouterClassificationCase(...args),
    runRouterClassification: (...args) => controller.runRouterClassification(...args),
    resetLiveCaseState: (...args) => controller.resetLiveCaseState(...args),
    runBenchmarkAction: (...args) => controller.runBenchmarkAction(...args),
    clearHistoryState: (...args) => controller.clearHistoryState(...args),
  };
})();
const dashboardBenchmarkController = dashboardBenchmarkBootstrap.controller;

function getLatestBenchmarkCaseFromRun(run) {
  return DashboardBenchmarkModule?.getLatestBenchmarkCaseFromRun?.(run) || null;
}

function getLatestBenchmarkCase(results) {
  return DashboardBenchmarkModule?.getLatestBenchmarkCase?.(results, benchmarkLiveLatestCase) || null;
}

function getLatestBenchmarkRunForModule(results, module) {
  return DashboardBenchmarkModule?.getLatestBenchmarkRunForModule?.(results, module) || null;
}

function renderBenchmarkRateBadge(rate, count, label) {
  return DashboardBenchmarkModule?.renderBenchmarkRateBadge?.(rate, count, label, escapeHtml)
    || `<article class="bm-quality-card is-muted"><strong>${escapeHtml(String(label || ""))}</strong><span>—</span><small>模块未加载</small></article>`;
}

function renderBenchmarkQualityPanels(results) {
  return dashboardBenchmarkBootstrap.renderBenchmarkQualityPanels(results);
}

async function loadBenchmarkHistory() {
  return dashboardBenchmarkBootstrap.loadBenchmarkHistory();
}

function getBenchmarkLatestCaseKey(latestCase, mode = benchmarkLiveLatestCase?.record ? "live" : "history") {
  return dashboardBenchmarkBootstrap.getBenchmarkLatestCaseKey(latestCase, mode);
}

function renderBenchmarkLatestCaseFallback(latestCase, { missingTrace = false } = {}) {
  return dashboardBenchmarkBootstrap.renderBenchmarkLatestCaseFallback(latestCase, { missingTrace });
}

async function renderBenchmarkCaseTraceList(results, latestCase = benchmarkLiveLatestCase, { force = false } = {}) {
  return dashboardBenchmarkBootstrap.renderBenchmarkCaseTraceList(results, latestCase, { force });
}

function updateBenchmarkLiveStateFromJob(job, { render = true } = {}) {
  return dashboardBenchmarkBootstrap.updateBenchmarkLiveStateFromJob(job, { render });
}

async function refreshBenchmarkLatestCase() {
  return dashboardBenchmarkBootstrap.refreshBenchmarkLatestCase();
}

function renderBenchmarkTable(results) {
  return dashboardBenchmarkBootstrap.renderBenchmarkTable(results);
}

async function bootstrapBenchmarkTab() {
  return dashboardBenchmarkBootstrap.bootstrapBenchmarkTab();
}

function _setBenchmarkRunUiState(running, startedAt = "") {
  const runBtn = document.getElementById("bm-run-btn");
  const abortBtn = document.getElementById("bm-abort-btn");
  const progressWrap = document.getElementById("bm-progress-wrap");
  const timerEl = document.getElementById("bm-timer");
  if (runBtn) runBtn.disabled = !!running;
  if (abortBtn) abortBtn.disabled = !running;
  if (progressWrap) progressWrap.classList.toggle("hidden", !running);
  if (!running) {
    if (benchmarkTimerInterval) {
      clearInterval(benchmarkTimerInterval);
      benchmarkTimerInterval = null;
    }
    return;
  }
  const parsedStartedAt = Date.parse(String(startedAt || ""));
  const baseTime = Number.isFinite(parsedStartedAt) ? parsedStartedAt : Date.now();
  if (benchmarkTimerInterval) clearInterval(benchmarkTimerInterval);
  if (timerEl) timerEl.textContent = "00:00";
  benchmarkTimerInterval = setInterval(() => {
    const elapsed = Math.max(0, Math.floor((Date.now() - baseTime) / 1000));
    const mm = String(Math.floor(elapsed / 60)).padStart(2, "0");
    const ss = String(elapsed % 60).padStart(2, "0");
    if (timerEl) timerEl.textContent = `${mm}:${ss}`;
  }, 1000);
}

async function resumeSharedBenchmarkJobUi() {
  const progressFill = document.getElementById("bm-progress-fill");
  const progressText = document.getElementById("bm-progress-text");
  const logBox = document.getElementById("bm-log-box");
  const payload = await apiGet("/api/dashboard/jobs?job_type=benchmark&only_active=true");
  const jobs = Array.isArray(payload?.jobs) ? payload.jobs : [];
  const job = jobs.find((item) => item && (item.status === "queued" || item.status === "running")) || null;
  if (!job?.id) {
    benchmarkWatchingJobId = "";
    if (!activeBenchmarkJobId) _setBenchmarkRunUiState(false);
    return null;
  }
  const jobId = String(job.id || "");
  if (benchmarkWatchingJobId === jobId) return job;
  benchmarkWatchingJobId = jobId;
  _setBenchmarkRunUiState(true, String(job.started_at || ""));
  if (logBox) logBox.classList.remove("hidden");
  return dashboardBenchmarkController.resumeSharedBenchmarkJob({
    logBox,
    setProgressText: (message) => {
      if (progressText) progressText.textContent = message;
    },
    setProgressPercent: (percent) => {
      if (progressFill) progressFill.style.width = `${percent}%`;
    },
  }).finally(() => {
    if (benchmarkWatchingJobId === jobId) benchmarkWatchingJobId = "";
    _setBenchmarkRunUiState(false);
  });
}

function syncBenchmarkCountOptions() {
  return dashboardBenchmarkBootstrap.syncBenchmarkCountOptions();
}

function normalizeRouterClassificationArbitration(value) {
  if (Array.isArray(value)) {
    return value.map((item) => String(item || "").trim()).filter(Boolean);
  }
  return String(value || "")
    .replace(/\r/g, "\n")
    .split(/[|,\n]/)
    .map((item) => item.trim())
    .filter(Boolean);
}

function formatRouterClassificationArbitration(value) {
  const items = normalizeRouterClassificationArbitration(value);
  return items.join(" | ");
}

function setRouterClassificationMeta(text) {
  const meta = document.getElementById("bm-router-cls-meta");
  if (meta) meta.textContent = text;
}

function getRouterClassificationResultForCase(caseItem) {
  const caseId = String(caseItem?.id || "").trim();
  if (caseId) {
    const exact = routerClassificationLastResults.find((item) => String(item?.id || "").trim() === caseId);
    if (exact) return exact;
  }
  const query = String(caseItem?.query || "").trim();
  return routerClassificationLastResults.find((item) => String(item?.query || "").trim() === query) || null;
}

function renderRouterClassificationTable() {
  const tbody = document.getElementById("bm-router-cls-tbody");
  const tableWrap = document.getElementById("bm-router-cls-table-wrap");
  const summary = document.getElementById("bm-router-cls-summary");
  if (!tbody) return;

  if (!routerClassificationCases.length) {
    tbody.innerHTML = '<tr><td colspan="8"><div class="bm-router-cls-empty">当前没有 Router 分类回归用例，点击右上角“增加用例”开始维护。</div></td></tr>';
    if (tableWrap) tableWrap.classList.remove("hidden");
    if (summary) summary.classList.add("hidden");
    setRouterClassificationMeta("暂无用例，支持手动新增/编辑/删除");
    return;
  }

  tbody.innerHTML = routerClassificationCases.map((caseItem) => {
    const result = getRouterClassificationResultForCase(caseItem);
    const expectedArbitration = formatRouterClassificationArbitration(caseItem.expected_arbitration);
    const actualArbitration = result ? formatRouterClassificationArbitration(result.actual_arbitration) : "";
    const pass = result?.pass;
    const domainOk = !result || !result.actual_domain || result.actual_domain === caseItem.expected_domain;
    const arbitrationOk = !result || !actualArbitration || normalizeRouterClassificationArbitration(caseItem.expected_arbitration).includes(actualArbitration);
    const resultText = pass == null ? "—" : pass ? "✓" : "✗";
    const resultClass = pass == null ? "" : pass ? "" : " bm-cls-row-fail";
    return `<tr class="${resultClass}">
      <td class="bm-cls-query" title="${escapeHtml(caseItem.query || "")}">${escapeHtml((caseItem.query || "").slice(0, 42))}</td>
      <td>${escapeHtml(caseItem.expected_domain || "")}</td>
      <td>${escapeHtml(expectedArbitration)}</td>
      <td class="${domainOk ? "" : "bm-cls-cell-fail"}">${escapeHtml(result?.actual_domain || "")}</td>
      <td class="${arbitrationOk ? "" : "bm-cls-cell-fail"}">${escapeHtml(actualArbitration)}</td>
      <td>${resultText}</td>
      <td class="bm-cls-note">${escapeHtml(result?.note || result?.error || caseItem.note || "")}</td>
      <td class="bm-cls-actions">
        <button type="button" class="ghost bm-cls-action-btn" data-bm-router-action="edit" data-bm-router-id="${escapeHtml(caseItem.id || "")}">编辑</button>
        <button type="button" class="ghost bm-cls-action-btn" data-bm-router-action="delete" data-bm-router-id="${escapeHtml(caseItem.id || "")}">删除</button>
      </td>
    </tr>`;
  }).join("");

  if (tableWrap) tableWrap.classList.remove("hidden");
  if (summary && !routerClassificationLastResults.length) summary.classList.add("hidden");
  if (!routerClassificationLastResults.length) {
    setRouterClassificationMeta(`已配置 ${routerClassificationCases.length} 条用例，可直接手动维护后再运行回归`);
  }
}

async function loadRouterClassificationCases() {
  return dashboardBenchmarkBootstrap.loadRouterClassificationCases();
}

function resetRouterClassificationEditor() {
  editingRouterClassificationCaseId = "";
  if (benchmarkRouterClsTraceIdInput) benchmarkRouterClsTraceIdInput.value = "";
  if (benchmarkRouterClsQueryInput) benchmarkRouterClsQueryInput.value = "";
  if (benchmarkRouterClsExpectedDomainInput) benchmarkRouterClsExpectedDomainInput.value = "tech";
  if (benchmarkRouterClsExpectedArbitrationInput) benchmarkRouterClsExpectedArbitrationInput.value = "";
  if (benchmarkRouterClsNoteInput) benchmarkRouterClsNoteInput.value = "";
  if (benchmarkRouterClsMockLabelInput) benchmarkRouterClsMockLabelInput.value = "";
  if (benchmarkRouterClsMockDomainInput) benchmarkRouterClsMockDomainInput.value = "";
  if (benchmarkRouterClsMockLookupModeInput) benchmarkRouterClsMockLookupModeInput.value = "";
  if (benchmarkRouterClsMockEntitiesInput) benchmarkRouterClsMockEntitiesInput.value = "";
  benchmarkRouterClsDeleteBtn?.classList.add("hidden");
  if (benchmarkRouterClsLastRunBox) {
    benchmarkRouterClsLastRunBox.innerHTML = "";
    benchmarkRouterClsLastRunBox.classList.add("hidden");
  }
}

function normalizeRouterClassificationDomainValue(value, fallback = "tech") {
  const normalized = String(value || "").trim().toLowerCase();
  return ["tech", "media", "general"].includes(normalized) ? normalized : fallback;
}

function deriveRouterClassificationMockLabel(domain, fallback = "") {
  const normalized = normalizeRouterClassificationDomainValue(domain, "");
  if (normalized === "media") return "MEDIA";
  if (normalized === "tech") return "TECH";
  if (normalized === "general") return "OTHER";
  return fallback;
}

function deriveRouterClassificationPayloadFromTrace(trace) {
  const record = trace && typeof trace === "object" ? trace : {};
  const conversationStateAfter = record.conversation_state_after && typeof record.conversation_state_after === "object" ? record.conversation_state_after : {};
  const queryUnderstanding = record.query_understanding && typeof record.query_understanding === "object" ? record.query_understanding : {};
  const plannerSnapshot = record.planner_snapshot && typeof record.planner_snapshot === "object" ? record.planner_snapshot : {};
  const workingSet = record.working_set && typeof record.working_set === "object" ? record.working_set : {};
  const router = record.router && typeof record.router === "object" ? record.router : {};
  const result = record.result && typeof record.result === "object" ? record.result : {};
  const query = String(
    record.query
      || record.user_query
      || record.original_query
      || record.prompt
      || plannerSnapshot.query_text
      || workingSet.query
      || queryUnderstanding.original_question
      || queryUnderstanding.resolved_question
      || conversationStateAfter.question
      || result.query
      || "",
  ).trim();
  if (!query) throw new Error("这个 trace 里没有可导入的 query");
  const mockEntities = Array.isArray(queryUnderstanding.entities)
    ? queryUnderstanding.entities
    : Array.isArray(conversationStateAfter.entities)
      ? conversationStateAfter.entities
      : Array.isArray(router.entities)
        ? router.entities
        : Array.isArray(router.entity_hits)
          ? router.entity_hits
          : [conversationStateAfter.entity, queryUnderstanding.followup_target].filter(Boolean);
  const expectedDomain = normalizeRouterClassificationDomainValue(
    queryUnderstanding.domain || router.domain || router.selected_domain || "tech",
  );
  const expectedArbitration = String(
    queryUnderstanding.arbitration
      || router.arbitration
      || router.decision_reason
      || router.decision_intent
      || "",
  ).trim();
  const mockDomain = normalizeRouterClassificationDomainValue(
    router.domain || queryUnderstanding.domain || router.selected_domain || "",
    "",
  );
  const mockLabel = String(router.classifier_label || "").trim().toUpperCase() || deriveRouterClassificationMockLabel(mockDomain || expectedDomain, "");
  return {
    query,
    expectedDomain,
    expectedArbitration,
    mockLabel,
    mockDomain,
    mockLookupMode: String(queryUnderstanding.lookup_mode || conversationStateAfter.lookup_mode || router.lookup_mode || router.lookup_strategy || "").trim().toLowerCase(),
    mockEntities: mockEntities
      .map((item) => {
        if (typeof item === "string") return item.trim();
        if (item && typeof item === "object") return String(item.name || item.entity || item.title || "").trim();
        return "";
      })
      .filter(Boolean),
  };
}

async function importRouterClassificationTrace() {
  return dashboardBenchmarkBootstrap.importRouterClassificationTrace();
}

function openRouterClassificationEditor(caseItem = null) {
  return dashboardBenchmarkBootstrap.openRouterClassificationEditor(caseItem);
}

function closeRouterClassificationEditor() {
  return dashboardBenchmarkBootstrap.closeRouterClassificationEditor();
}

function buildRouterClassificationPayloadFromEditor() {
  const query = String(benchmarkRouterClsQueryInput?.value || "").trim();
  const expectedDomain = String(benchmarkRouterClsExpectedDomainInput?.value || "").trim();
  const expectedArbitration = normalizeRouterClassificationArbitration(benchmarkRouterClsExpectedArbitrationInput?.value || "");
  if (!query) throw new Error("请填写 query");
  if (!expectedArbitration.length) throw new Error("请至少填写一个期望 arbitration");
  return {
    query,
    expected_domain: expectedDomain,
    expected_arbitration: expectedArbitration.length === 1 ? expectedArbitration[0] : expectedArbitration,
    note: String(benchmarkRouterClsNoteInput?.value || "").trim(),
    mock_label: String(benchmarkRouterClsMockLabelInput?.value || "").trim() || null,
    mock_domain: String(benchmarkRouterClsMockDomainInput?.value || "").trim() || null,
    mock_lookup_mode: String(benchmarkRouterClsMockLookupModeInput?.value || "").trim() || null,
    mock_entities: normalizeRouterClassificationArbitration(benchmarkRouterClsMockEntitiesInput?.value || ""),
  };
}

async function saveRouterClassificationCase() {
  return dashboardBenchmarkBootstrap.saveRouterClassificationCase();
}

async function deleteRouterClassificationCase(caseId = editingRouterClassificationCaseId) {
  return dashboardBenchmarkBootstrap.deleteRouterClassificationCase(caseId);
}

// ─── Router classification benchmark ─────────────────────────────────────────
async function runRouterClassification() {
  return dashboardBenchmarkBootstrap.runRouterClassification();
}

async function runUnitTests() {
  const btn = document.getElementById("bm-unit-tests-run");
  const meta = document.getElementById("bm-unit-tests-meta");
  const summary = document.getElementById("bm-unit-tests-summary");
  const resultsEl = document.getElementById("bm-unit-tests-results");

  const suites = Array.from(document.querySelectorAll(".bm-ut-suite:checked")).map((cb) => cb.value);
  if (!suites.length) { window.alert("请至少选择一个测试套件"); return; }

  if (btn) btn.disabled = true;
  if (meta) meta.textContent = "运行中...";
  if (summary) summary.classList.add("hidden");
  if (resultsEl) { resultsEl.classList.add("hidden"); resultsEl.innerHTML = ""; }

  try {
    const result = await apiPost("/api/benchmark/unit-tests", { suites }, "POST", undefined, 120000);
    const { timestamp = "", total_passed = 0, total_failed = 0, total_errors = 0, suites: suiteResults = [] } = result;
    const allOk = total_failed === 0 && total_errors === 0;
    const total = total_passed + total_failed + total_errors;

    if (summary) {
      summary.className = `bm-router-cls-summary ${allOk ? "bm-cls-pass" : "bm-cls-fail"}`;
      summary.textContent = `${total_passed} / ${total} 通过${!allOk ? ` — ${total_failed + total_errors} 项失败` : " ✓ 全部通过"}`;
      summary.classList.remove("hidden");
    }
    if (meta) meta.textContent = `${timestamp} 运行完成`;

    if (resultsEl) {
      resultsEl.innerHTML = suiteResults.map((suite) => {
        const suiteOk = suite.failed === 0 && suite.errors === 0;
        const tests = Array.isArray(suite.tests) ? suite.tests : [];
        const suiteTotal = suite.passed + suite.failed + (suite.errors || 0);
        const rows = tests.map((t) => {
          const icon = t.status === "pass" ? "✓" : t.status === "skip" ? "–" : "✗";
          const rowCls = t.status === "pass" ? "" : t.status === "skip" ? "bm-cls-row-skip" : "bm-cls-row-fail";
          const shortId = (t.id || "").split(".").slice(-2).join(".");
          return `<tr class="${rowCls}">
            <td class="bm-cls-query" title="${escapeHtml(t.id || "")}">${escapeHtml(shortId)}</td>
            <td>${icon}</td>
            <td class="bm-cls-note">${escapeHtml((t.message || "").slice(0, 160))}</td>
          </tr>`;
        }).join("");
        return `<div class="bm-ut-suite-block ${suiteOk ? "bm-ut-suite-pass" : "bm-ut-suite-fail"}">
          <div class="bm-ut-suite-header">${escapeHtml(suite.label || suite.id)} — ${suite.passed}/${suiteTotal} ✓ (${suite.elapsed_seconds}s)</div>
          <div class="stats-table-wrap bm-ut-suite-table-wrap">
            <table class="stats-table bm-router-cls-table">
              <thead><tr><th>测试</th><th>结果</th><th>信息</th></tr></thead>
              <tbody>${rows}</tbody>
            </table>
          </div>
        </div>`;
      }).join("");
      resultsEl.classList.remove("hidden");
    }
  } catch (err) {
    const message = `运行失败: ${String(err)}`;
    if (meta) meta.textContent = message;
    if (summary) {
      summary.className = "bm-router-cls-summary bm-cls-fail";
      summary.textContent = message;
      summary.classList.remove("hidden");
    }
    if (resultsEl) {
      resultsEl.innerHTML = `<div class="ticket-empty-state">${escapeHtml(message)}</div>`;
      resultsEl.classList.remove("hidden");
    }
    showAppErrorModal("单元 / 回归测试失败", String(err), "请检查请求参数、后端日志或测试套件配置");
  } finally {
    if (btn) btn.disabled = false;
  }
}

async function runBenchmark() {
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
  const progressFill = document.getElementById("bm-progress-fill");
  const progressText = document.getElementById("bm-progress-text");
  const logBox = document.getElementById("bm-log-box");

  _setBenchmarkRunUiState(true);
  if (logBox) { logBox.textContent = ""; logBox.classList.remove("hidden"); }
  dashboardBenchmarkBootstrap.resetLiveCaseState({ render: true, latestCase: null }).catch(() => {});
  if (progressFill) progressFill.style.width = "0%";
  if (progressText) progressText.textContent = "连接中...";

  try {
    const sharedJobsPayload = await apiGet("/api/dashboard/jobs?job_type=benchmark&only_active=true");
    const sharedJobs = Array.isArray(sharedJobsPayload?.jobs) ? sharedJobsPayload.jobs : [];
    const activeJob = sharedJobs.find((item) => item && (item.status === "queued" || item.status === "running")) || null;
    if (activeJob?.id) {
      benchmarkWatchingJobId = String(activeJob.id || "");
      _setBenchmarkRunUiState(true, String(activeJob.started_at || ""));
    }
    await dashboardBenchmarkBootstrap.runBenchmarkAction({
      modules,
      queryCount,
      caseSetId,
      existingJob: activeJob,
      logBox,
      setProgressText: (message) => {
        if (progressText) progressText.textContent = message;
      },
      setProgressPercent: (percent) => {
        if (progressFill) progressFill.style.width = `${percent}%`;
      },
    });
  } finally {
    benchmarkWatchingJobId = "";
    _setBenchmarkRunUiState(false);
  }
}

async function clearBenchmarkHistory() {
  if (!window.confirm("确定清除所有 Benchmark 历史记录？")) return;
  await apiDelete("/api/benchmark/history");
  dashboardBenchmarkBootstrap.clearHistoryState();
}

function authorizationHeaders() {
  return authorizationState.token ? { "x-admin-reauth": authorizationState.token } : undefined;
}

function setAuthorizationMessage(message) {
  if (authorizationMeta) authorizationMeta.textContent = message;
}

function showAuthorizationError(node, message) {
  if (!(node instanceof HTMLElement)) return;
  const text = String(message || "").trim();
  node.textContent = text;
  node.classList.toggle("hidden", !text);
}

function renderAuthorizationAppChecks(container, apps, selectedApps, { disabled = false } = {}) {
  if (!(container instanceof HTMLElement)) return;
  const chosen = new Set(Array.isArray(selectedApps) ? selectedApps.map((item) => String(item || "")) : []);
  container.innerHTML = (apps || []).map((app) => {
    const appId = String(app?.app_id || "").trim();
    const label = String(app?.label || appId).trim();
    return `
      <label class="authorization-check">
        <input type="checkbox" value="${escapeHtml(appId)}" ${chosen.has(appId) ? "checked" : ""} ${disabled ? "disabled" : ""} />
        <span>${escapeHtml(label)}</span>
      </label>
    `;
  }).join("");
}

function collectAuthorizationApps(container) {
  if (!(container instanceof HTMLElement)) return [];
  return Array.from(container.querySelectorAll("input[type='checkbox']:checked"))
    .map((input) => String(input.value || "").trim())
    .filter(Boolean);
}

function renderAuthorizationUsers() {
  if (!(authorizationUsersList instanceof HTMLElement)) return;
  const users = Array.isArray(authorizationState.data?.users) ? authorizationState.data.users : [];
  const apps = Array.isArray(authorizationState.data?.available_apps) ? authorizationState.data.available_apps : [];
  if (!users.length) {
    authorizationUsersList.innerHTML = '<div class="authorization-empty">当前没有可管理账号。</div>';
    return;
  }
  authorizationUsersList.innerHTML = users.map((user) => {
    const userId = String(user.user_id || "").trim();
    const username = String(user.username || "").trim();
    const role = String(user.role || "user").trim();
    const activeSessions = Number(user.active_session_count || 0);
    const authVersion = Number(user.auth_version || 1);
    const allowedApps = Array.isArray(user.allowed_apps) ? user.allowed_apps : [];
    const appChecks = apps.map((app) => {
      const appId = String(app?.app_id || "").trim();
      const label = String(app?.label || appId).trim();
      return `
        <label class="authorization-check">
          <input type="checkbox" data-role="allowed-app" value="${escapeHtml(appId)}" ${allowedApps.includes(appId) || role === "admin" ? "checked" : ""} ${role === "admin" ? "disabled" : ""} />
          <span>${escapeHtml(label)}</span>
        </label>
      `;
    }).join("");
    return `
      <section class="authorization-user-card" data-user-id="${escapeHtml(userId)}">
        <div class="authorization-user-head">
          <div>
            <div class="authorization-user-name">${escapeHtml(username)}</div>
            <div class="authorization-user-meta">role=${escapeHtml(role)} · auth_version=${authVersion} · active_sessions=${activeSessions}</div>
          </div>
        </div>
        <div class="authorization-form-grid">
          <label class="authorization-field">
            <span>Role</span>
            <select data-role="role-select">
              <option value="user" ${role === "user" ? "selected" : ""}>user</option>
              <option value="admin" ${role === "admin" ? "selected" : ""}>admin</option>
            </select>
          </label>
          <label class="authorization-field">
            <span>New Password</span>
            <input data-role="password-input" type="password" autocomplete="new-password" placeholder="留空则不改" />
          </label>
          <label class="authorization-check inline">
            <input data-role="active-input" type="checkbox" ${user.is_active ? "checked" : ""} />
            <span>启用账号</span>
          </label>
        </div>
        <div class="authorization-apps-block">
          <div class="authorization-section-label">应用权限</div>
          <div class="authorization-apps-grid" data-role="apps-grid">${appChecks}</div>
        </div>
        <div class="authorization-user-actions">
          <button type="button" data-role="save-user">保存</button>
        </div>
      </section>
    `;
  }).join("");

  authorizationUsersList.querySelectorAll("[data-role='role-select']").forEach((select) => {
    select.addEventListener("change", () => {
      const card = select.closest("[data-user-id]");
      const appsGrid = card?.querySelector("[data-role='apps-grid']");
      const isAdmin = select.value === "admin";
      appsGrid?.querySelectorAll("input[type='checkbox']").forEach((input) => {
        input.checked = isAdmin || input.checked;
        input.disabled = isAdmin;
      });
    });
  });

  authorizationUsersList.querySelectorAll("[data-role='save-user']").forEach((button) => {
    button.addEventListener("click", async () => {
      const card = button.closest("[data-user-id]");
      const userId = String(card?.getAttribute("data-user-id") || "").trim();
      if (!userId) return;
      const role = String(card?.querySelector("[data-role='role-select']")?.value || "user").trim();
      const isActive = Boolean(card?.querySelector("[data-role='active-input']")?.checked);
      const newPassword = String(card?.querySelector("[data-role='password-input']")?.value || "");
      const appsGrid = card?.querySelector("[data-role='apps-grid']");
      const allowedApps = role === "admin" ? (authorizationState.data?.available_apps || []).map((item) => String(item.app_id || "")).filter(Boolean) : collectAuthorizationApps(appsGrid);
      try {
        button.disabled = true;
        const payload = await apiPost(`/_auth/api/admin/users/${encodeURIComponent(userId)}`, {
          role,
          is_active: isActive,
          new_password: newPassword,
          allowed_apps: allowedApps,
        }, "PATCH", authorizationHeaders());
        authorizationState.data = { ...(authorizationState.data || {}), users: payload.users || [] };
        renderAuthorizationUsers();
        showToast("账号权限已更新");
      } catch (error) {
        showAppErrorModal("Authorization 更新失败", String(error));
      } finally {
        button.disabled = false;
      }
    });
  });
}

function renderAuthorizationAdmin() {
  const allAppIds = (authorizationState.data?.available_apps || []).map((item) => String(item.app_id || "")).filter(Boolean);
  const createRole = String(authorizationCreateRole?.value || "user");
  renderAuthorizationAppChecks(
    authorizationCreateApps,
    authorizationState.data?.available_apps || [],
    createRole === "admin" ? allAppIds : [],
    { disabled: createRole === "admin" },
  );
  renderAuthorizationUsers();
  if (authorizationSessionMeta) {
    const until = String(authorizationState.expiresAt || "").trim();
    authorizationSessionMeta.textContent = until ? `已解锁，二次确认有效期至 ${until.replace("T", " ").slice(0, 16)}` : "已解锁";
  }
}

function showAuthorizationSection(name) {
  authorizationReauthShell?.classList.toggle("hidden", name !== "reauth");
  authorizationBootstrapShell?.classList.toggle("hidden", name !== "bootstrap");
  authorizationAdminShell?.classList.toggle("hidden", name !== "admin");
}

async function loadAuthorizationState() {
  const payload = await apiGet("/_auth/api/admin/state", authorizationHeaders());
  authorizationState.data = payload;
  authorizationState.loaded = true;
  authorizationState.error = null;
  if (!payload?.users_exist) {
    setAuthorizationMessage("当前还没有管理员账号。请先初始化。" );
    showAuthorizationSection("bootstrap");
    return payload;
  }
  setAuthorizationMessage("Authorization 页面已解锁，可直接管理远端访问账号和应用权限。");
  showAuthorizationSection("admin");
  renderAuthorizationAdmin();
  return payload;
}

function resetAuthorizationReauth() {
  authorizationState.token = "";
  authorizationState.expiresAt = "";
  authorizationState.loaded = false;
  authorizationState.error = null;
  authorizationState.data = null;
  showAuthorizationError(authorizationReauthError, "");
  showAuthorizationError(authorizationBootstrapError, "");
}

async function bootstrapAuthorizationTab({ forcePrompt = false } = {}) {
  if (authorizationState.loading) return;
  if (forcePrompt) {
    resetAuthorizationReauth();
  }
  authorizationState.loading = true;
  try {
    const payload = await apiGet("/_auth/api/admin/state", authorizationHeaders());
    authorizationState.data = payload;
    if (!payload?.users_exist) {
      authorizationState.loaded = true;
      authorizationState.error = null;
      showAuthorizationSection("bootstrap");
      setAuthorizationMessage("当前还没有管理员账号。请先初始化。" );
      return;
    }
    if (!authorizationState.token) {
      showAuthorizationSection("reauth");
      setAuthorizationMessage("进入此页需要重新输入管理员密码。" );
      authorizationPasswordInput?.focus();
      return;
    }
    await loadAuthorizationState();
  } catch (error) {
    const raw = String(error || "");
    if (raw.includes("admin_reauth_required")) {
      showAuthorizationSection("reauth");
      setAuthorizationMessage("进入此页需要重新输入管理员密码。" );
      authorizationPasswordInput?.focus();
    } else {
      authorizationState.error = error;
      showAuthorizationSection("reauth");
      showAuthorizationError(authorizationReauthError, raw.includes("forbidden") ? "当前访问方没有管理员权限。" : raw);
      setAuthorizationMessage("Authorization 页面加载失败。" );
    }
  } finally {
    authorizationState.loading = false;
  }
}

async function unlockAuthorizationPanel() {
  const password = String(authorizationPasswordInput?.value || "").trim();
  if (!password) {
    showAuthorizationError(authorizationReauthError, "请输入管理员密码。");
    authorizationPasswordInput?.focus();
    return;
  }
  showAuthorizationError(authorizationReauthError, "");
  authorizationUnlockBtn && (authorizationUnlockBtn.disabled = true);
  try {
    const payload = await apiPost("/_auth/api/admin/reauth", { password });
    authorizationState.token = String(payload?.token || "").trim();
    authorizationState.expiresAt = String(payload?.expires_at || "").trim();
    if (authorizationPasswordInput) authorizationPasswordInput.value = "";
    await loadAuthorizationState();
  } catch (error) {
    const raw = String(error || "");
    showAuthorizationError(authorizationReauthError, raw.includes("invalid_admin_password") ? "管理员密码错误。" : raw);
  } finally {
    authorizationUnlockBtn && (authorizationUnlockBtn.disabled = false);
  }
}

async function createAuthorizationBootstrapAdmin() {
  const username = String(authorizationBootstrapUsername?.value || "").trim();
  const password = String(authorizationBootstrapPassword?.value || "").trim();
  if (!username || !password) {
    showAuthorizationError(authorizationBootstrapError, "请填写管理员用户名和密码。");
    return;
  }
  showAuthorizationError(authorizationBootstrapError, "");
  authorizationBootstrapBtn && (authorizationBootstrapBtn.disabled = true);
  try {
    await apiPost("/_auth/api/admin/bootstrap", { username, password });
    if (authorizationBootstrapPassword) authorizationBootstrapPassword.value = "";
    setAuthorizationMessage("管理员已创建，请重新输入管理员密码解锁此页。" );
    resetAuthorizationReauth();
    await bootstrapAuthorizationTab({ forcePrompt: false });
  } catch (error) {
    showAuthorizationError(authorizationBootstrapError, String(error));
  } finally {
    authorizationBootstrapBtn && (authorizationBootstrapBtn.disabled = false);
  }
}

async function createAuthorizationUser() {
  const username = String(authorizationCreateUsername?.value || "").trim();
  const password = String(authorizationCreatePassword?.value || "").trim();
  const role = String(authorizationCreateRole?.value || "user").trim();
  if (!username || !password) {
    showAppErrorModal("Authorization 创建失败", "请填写用户名和密码");
    return;
  }
  const allowedApps = role === "admin"
    ? (authorizationState.data?.available_apps || []).map((item) => String(item.app_id || "")).filter(Boolean)
    : collectAuthorizationApps(authorizationCreateApps);
  authorizationCreateBtn && (authorizationCreateBtn.disabled = true);
  try {
    const payload = await apiPost("/_auth/api/admin/users", {
      username,
      password,
      role,
      is_active: Boolean(authorizationCreateActive?.checked),
      allowed_apps: allowedApps,
    }, "POST", authorizationHeaders());
    authorizationState.data = { ...(authorizationState.data || {}), users: payload.users || [] };
    if (authorizationCreateUsername) authorizationCreateUsername.value = "";
    if (authorizationCreatePassword) authorizationCreatePassword.value = "";
    if (authorizationCreateRole) authorizationCreateRole.value = "user";
    if (authorizationCreateActive) authorizationCreateActive.checked = true;
    renderAuthorizationAdmin();
    showToast("账号已创建");
  } catch (error) {
    showAppErrorModal("Authorization 创建失败", String(error));
  } finally {
    authorizationCreateBtn && (authorizationCreateBtn.disabled = false);
  }
}



function mountAgentSidebarController() {
  if (!AppShell?.mountSidebarController) return null;
  const registry = window.__navDashboardSidebarControllers || (window.__navDashboardSidebarControllers = new Map());
  if (registry.has("agent-sidebar")) return registry.get("agent-sidebar");
  const sidebar = document.getElementById("agent-sidebar");
  const button = document.getElementById("agent-toggle-sidebar");
  const workspace = document.getElementById("agent-workspace") || sidebar?.closest(".workspace") || null;
  if (!sidebar || !button || !workspace) return null;
  const controller = AppShell.mountSidebarController({
    appId: "nav_dashboard",
    scope: "agent",
    shell: workspace,
    sidebar,
    toggleButton: button,
    defaultMobileExpanded: true,
    storageKeyAliases: ["sidebar:nav_dashboard:agent:v3"],
  });
  registry.set("agent-sidebar", controller);
  return controller;
}

window.__navDashboardMountAgentSidebarController = mountAgentSidebarController;

function setModel() {
  if (qaModel) qaModel.textContent = `当前模型：${pageLocalModel}`;
}

async function apiGet(url, headers = undefined, timeoutMs = 20000) {
  if (DashboardSharedApi && typeof DashboardSharedApi.apiGet === "function") {
    return DashboardSharedApi.apiGet(url, headers, timeoutMs);
  }
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), Math.max(1, Number(timeoutMs || 20000)));
  try {
    const r = await fetch(url, { signal: ctrl.signal, headers });
    if (!r.ok) throw new Error(await r.text());
    return r.json();
  } finally {
    clearTimeout(t);
  }
}

async function apiPost(url, payload, method = "POST", extraHeaders = undefined, timeoutMs = 20000) {
  if (DashboardSharedApi && typeof DashboardSharedApi.apiPost === "function") {
    return DashboardSharedApi.apiPost(url, payload, method, extraHeaders, timeoutMs);
  }
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), Math.max(1, Number(timeoutMs || 20000)));
  try {
    const r = await fetch(url, {
      method,
      headers: { "Content-Type": "application/json", ...(extraHeaders || {}) },
      body: JSON.stringify(payload),
      signal: ctrl.signal,
    });
    if (!r.ok) throw new Error(await r.text());
    return r.json();
  } finally {
    clearTimeout(t);
  }
}

async function apiDelete(url) {
  if (DashboardSharedApi && typeof DashboardSharedApi.apiDelete === "function") {
    return DashboardSharedApi.apiDelete(url);
  }
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), 20000);
  try {
    const r = await fetch(url, { method: "DELETE", signal: ctrl.signal });
    if (!r.ok) throw new Error(await r.text());
    return r.json();
  } finally {
    clearTimeout(t);
  }
}

async function apiPostForm(url, formData, extraHeaders = undefined) {
  if (DashboardSharedApi && typeof DashboardSharedApi.apiPostForm === "function") {
    return DashboardSharedApi.apiPostForm(url, formData, extraHeaders);
  }
  const r = await fetch(url, {
    method: "POST",
    headers: extraHeaders,
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

function loadInitialAgentSessions() {
  return dashboardAgentSessionsBootstrap.loadInitialAgentSessions();
}

const dashboardCustomCardsBootstrap = DashboardCustomCardsBootstrapModule?.createCustomCardsBootstrap?.({
  escapeHtml,
  apiPost,
  consumePendingCardEditIndex,
  getElements: () => ({
    customCardModal,
    customCardModalTitle,
    customCardNameInput,
    customCardUrlInput,
    customCardImageInput,
    customCardPreview,
    customCardUploadBtn,
    customCardUploadInput,
    customCardSaveBtn,
    customCardCancelBtn,
  }),
}) || {
  bindEvents() {},
  loadInitialCards() {},
  hydratePendingCardEdit() {},
  openCustomCardModal() {},
  closeCustomCardModal() {},
};

const dashboardAgentSessionsBootstrap = DashboardAgentSessionsBootstrapModule?.createAgentSessionsBootstrap?.({
  agentSessionsDataElement: document.getElementById("agent-sessions-data"),
  sessionListElement: document.getElementById("agent-session-list"),
  document,
  escapeHtml,
  apiGet,
  apiPost,
  apiDelete,
  getSessionsCache: () => sessionsCache,
  setSessionsCache: (value) => {
    sessionsCache = Array.isArray(value) ? value : [];
  },
  getCurrentSessionId: () => currentSessionId,
  setCurrentSessionId: (value) => {
    currentSessionId = String(value || "").trim();
  },
  getPreferredSessionId: () => String(NavBootstrap?.getPreferredSessionId?.() || "").trim(),
  setPreferredSessionId: (value) => {
    NavBootstrap?.setPreferredSessionId?.(String(value || "").trim());
  },
  getSuppressSessionClickUntil: () => suppressSessionClickUntil,
  setSuppressSessionClickUntil: (value) => {
    suppressSessionClickUntil = Number(value || 0);
  },
  sessionRenameModal,
  sessionRenameMeta,
  sessionRenameInput,
  renderChat,
  renderActiveAgentStreamPreview,
  debugUiEvent,
}) || createMissingController(
  "NavDashboardAgentSessionsBootstrap",
  [],
  [],
  {
    loadInitialAgentSessions: () => false,
    getCurrentSession: () => null,
    getSessionById: () => null,
    ensureAgentSessionCache: () => null,
    appendMessageToAgentSessionCache() {},
    renderCurrentAgentSessionView() {},
    renderSessions() {},
    getSessionSummary: () => null,
    closeSessionRenameModal() {},
    openSessionRenameModal() {},
    saveSessionRename: async () => {},
    openSessionRenameModalFromTarget() {},
    refreshSessions: async () => {},
    createSessionAction: async () => {},
    deleteCurrentSessionAction: async () => {},
  },
);

const dashboardAgentSessionUiBootstrap = DashboardAgentSessionUiBootstrapModule?.createAgentSessionUiBootstrap?.({
  sessionRenameModal,
  sessionRenameInput,
  sessionRenameSaveBtn,
  sessionRenameCancelBtn,
  sessionList: document.getElementById("agent-session-list"),
  newSessionBtn: document.getElementById("agent-new-session"),
  deleteSessionBtn: document.getElementById("agent-delete-session"),
  qaAsk,
  qaAskLocal,
  qaAbort,
  qaInput,
  bindLongPress,
  debugUiEvent,
  appendChatRow,
  ask,
  abortAsk,
  getCurrentSessionId: () => currentSessionId,
  createSessionAction: () => createSessionAction(),
  deleteCurrentSessionAction: () => deleteCurrentSessionAction(),
  saveSessionRename: () => saveSessionRename(),
  closeSessionRenameModal,
  openSessionRenameModalFromTarget,
}) || createMissingController(
  "NavDashboardAgentSessionUiBootstrap",
  [],
  [],
  {
    bindEvents() {},
  },
);

function getCurrentSession() {
  return dashboardAgentSessionsBootstrap.getCurrentSession();
}

function getSessionById(sessionId) {
  return dashboardAgentSessionsBootstrap.getSessionById(sessionId);
}

function ensureAgentSessionCache(sessionId) {
  return dashboardAgentSessionsBootstrap.ensureAgentSessionCache(sessionId);
}

function appendMessageToAgentSessionCache(sessionId, role, text, traceId = "") {
  dashboardAgentSessionsBootstrap.appendMessageToAgentSessionCache(sessionId, role, text, traceId);
}

function renderActiveAgentStreamPreview({ keepBottom = true, replaceExisting = false } = {}) {
  if (!activeAgentStreamState || String(activeAgentStreamState.sessionId || "") !== String(currentSessionId || "")) return;
  if (replaceExisting) clearActiveStreamPreviewRows();
  const progressLines = Array.isArray(activeAgentStreamState.progressLines) ? activeAgentStreamState.progressLines : [];
  const toolDoneLines = Array.isArray(activeAgentStreamState.toolDoneLines) ? activeAgentStreamState.toolDoneLines : [];
  const progress = progressLines.length ? progressLines[progressLines.length - 1] : "正在规划工具并调用...";
  const toolsText = toolDoneLines.length ? `\n${toolDoneLines.join("\n")}` : "";
  const startedAt = Number(activeAgentStreamState.startedAt || 0);
  const elapsed = startedAt > 0 ? `\n\n耗时: ${formatElapsed((Date.now() - startedAt) / 1000)}` : "";
  const progressRow = appendChatRow("system", `${progress}${toolsText}${elapsed}`, false, "processing");
  progressRow.dataset.streamPreview = "1";
  const answerText = String(activeAgentStreamState.answerText || "").trim();
  if (answerText) {
    const parsed = splitThinkBlocks(answerText);
    const assistantRow = appendChatRow("assistant", parsed.answer || answerText, false);
    assistantRow.dataset.streamPreview = "1";
    insertSystemRowsBefore(assistantRow, parsed.thoughts, { streamPreview: true });
    upsertTraceMetaRowBefore(assistantRow, formatTraceMeta(activeAgentStreamState.traceId), { streamPreview: true });
  }
  if (keepBottom) qaMessages.scrollTop = qaMessages.scrollHeight;
}

function renderCurrentAgentSessionView() {
  dashboardAgentSessionsBootstrap.renderCurrentAgentSessionView();
}

function renderSessions() {
  dashboardAgentSessionsBootstrap.renderSessions();
}

function getSessionSummary(sessionId) {
  return dashboardAgentSessionsBootstrap.getSessionSummary(sessionId);
}

function closeSessionRenameModal() {
  dashboardAgentSessionsBootstrap.closeSessionRenameModal();
}

function openSessionRenameModal(sessionId) {
  dashboardAgentSessionsBootstrap.openSessionRenameModal(sessionId);
}

async function saveSessionRename() {
  await dashboardAgentSessionsBootstrap.saveSessionRename();
}

function openSessionRenameModalFromTarget(target) {
  dashboardAgentSessionsBootstrap.openSessionRenameModalFromTarget(target);
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
  rewriteLoopbackLinksInContainer(row);
  qaMessages.appendChild(row);
  if (keepBottom) qaMessages.scrollTop = qaMessages.scrollHeight;
  return row;
}

function isQaMessagesNearBottom(threshold = 64) {
  if (!(qaMessages instanceof HTMLElement)) return true;
  const distance = qaMessages.scrollHeight - qaMessages.clientHeight - qaMessages.scrollTop;
  return distance <= threshold;
}

function clearActiveStreamPreviewRows() {
  if (!(qaMessages instanceof HTMLElement)) return;
  qaMessages.querySelectorAll('[data-stream-preview="1"]').forEach((node) => node.remove());
}

function upsertTraceMetaRowBefore(targetRow, metaText, options = {}) {
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
  if (options.streamPreview) row.dataset.streamPreview = "1";
  row.innerHTML = `<div class="role">系统</div><div class="content trace-meta-content markdown-body">${markdownToHtml(normalizedMeta)}</div>`;
  rewriteLoopbackLinksInContainer(row);
  if (!existingMetaRow) chat.insertBefore(row, anchor);
  return row;
}

function insertSystemRowsBefore(targetRow, blocks, options = {}) {
  if (!targetRow || !targetRow.parentElement || !Array.isArray(blocks)) return;
  const chat = targetRow.parentElement;
  for (const textRaw of blocks) {
    const text = normalizeThinkText(textRaw);
    if (!text) continue;
    const row = document.createElement("div");
    row.className = "msg system think collapsed";
    if (options.streamPreview) row.dataset.streamPreview = "1";
    row.innerHTML = `<div class="role">系统</div><div class="content markdown-body">${markdownToHtml(text)}</div>`;
    rewriteLoopbackLinksInContainer(row);
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
  await dashboardAgentSessionsBootstrap.refreshSessions(preferSessionId);
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

/**
 * Collect source_counts across all fan-out tool results and return a compact
 * human-readable summary line, e.g. "外部补充来源：Bangumi 3，TMDB 5".
 * Returns "" when there is nothing to show.
 */
function buildExternalSourceSummary(toolResults) {
  if (!Array.isArray(toolResults)) return "";
  const totals = {};
  for (const item of toolResults) {
    if (!item || typeof item !== "object") continue;
    const counts = item.data?.source_counts;
    if (counts && typeof counts === "object") {
      for (const [src, cnt] of Object.entries(counts)) {
        const n = Number(cnt) || 0;
        if (n > 0) totals[src] = (totals[src] || 0) + n;
      }
    }
  }
  const entries = Object.entries(totals);
  if (!entries.length) return "";
  const LABEL_MAP = { bangumi: "Bangumi", tmdb: "TMDB", wiki: "Wiki", web: "Web" };
  const parts = entries
    .sort((a, b) => b[1] - a[1])
    .map(([src, cnt]) => `${LABEL_MAP[src] || src} ${cnt}`);
  return `\n\n---\n*外部补充来源：${parts.join("，")}*`;
}

function formatToolResultLabel(tool) {
  const labels = {
    query_media_record: "查询媒体记录",
    search_by_creator: "按创作者检索",
    search_tmdb_media: "查询 TMDB",
    search_bangumi_subject: "查询 Bangumi",
    search_mediawiki_action: "查询 Wiki",
    parse_mediawiki_page: "解析 Wiki 页面",
    expand_mediawiki_concept: "扩展 Wiki 概念",
    search_web: "网页检索",
    query_document_rag: "查询 RAG 文档",
  };
  return labels[String(tool || "").trim()] || String(tool || "").trim() || "工具";
}

function buildReferencesMarkdown(toolResults) {
  if (!Array.isArray(toolResults)) return "";
  const docRefs = [];
  const mediaRefs = [];
  const externalRefs = [];
  const hasLocalMediaResults = toolResults.some((item) => {
    if (!item || typeof item !== "object" || String(item.tool || "").trim() !== "query_media_record") return false;
    if (Array.isArray(item.data?.main_results) && item.data.main_results.length) return true;
    return Array.isArray(item.data?.results) && item.data.results.length;
  });
  const hasPerItemExternalRefs = toolResults.some((item) => {
    if (!item || typeof item !== "object") return false;
    if (!item.data?.per_item_fanout) return false;
    return Array.isArray(item.data?.results) && item.data.results.length > 0;
  });

  function pushRef(bucket, score, line) {
    bucket.push({ score: Number.isFinite(score) ? score : 0, ...line });
  }

  function formatReferenceLink(label, url) {
    return url ? `[${label}](${url})` : label;
  }

  function renderReferenceSection(title, refs, startIndex) {
    const lines = [];
    const inlineLinks = [];
    let nextIndex = startIndex;
    refs.forEach((ref) => {
      lines.push(`[${nextIndex}] ${formatReferenceLink(ref.label, ref.url)}`);
      inlineLinks.push(ref.url ? `[${nextIndex}](${ref.url})` : `[${nextIndex}]`);
      nextIndex += 1;
    });
    return {
      nextIndex,
      section: `### ${title}\n${lines.join("\n")}`,
      inlineLinks,
    };
  }

  function scoreOf(row) {
    const score = Number(row?.score);
    if (Number.isFinite(score)) return score;
    const confidence = Number(row?.match_confidence);
    return Number.isFinite(confidence) ? confidence : null;
  }

  function externalSourceLabel(tool, row) {
    const source = String(row?.external_source || "").trim().toLowerCase();
    if (source === "bangumi") return "外部 Bangumi";
    if (source === "tmdb") return "外部 TMDB";
    if (source === "wiki") return "外部 Wiki";
    if (tool === "search_bangumi_subject") return "外部 Bangumi";
    if (tool === "search_tmdb_media") return "外部 TMDB";
    if (["search_mediawiki_action", "parse_mediawiki_page", "expand_mediawiki_concept"].includes(tool)) return "外部 Wiki";
    if (tool === "search_web") return "外部网页";
    return "外部参考";
  }

  for (const item of toolResults) {
    if (!item || typeof item !== "object") continue;
    const tool = String(item.tool || "").trim();
    const rows = Array.isArray(item.data?.results) ? item.data.results : [];
    for (const row of rows) {
      const score = scoreOf(row);
      if (tool === "query_document_rag") {
        const path = String(row?.path || "").trim();
        if (!path || !Number.isFinite(score)) continue;
        pushRef(docRefs, score, {
          label: `本地文档: ${path} (${score.toFixed(4)})`,
          url: `doc://${encodeURIComponent(path)}`,
        });
      } else if (tool === "search_web") {
        const title = String(row?.title || row?.url || "网页").trim() || "网页";
        const url = String(row?.url || "").trim();
        if (!url) continue;
        pushRef(externalRefs, score, {
          label: Number.isFinite(score) ? `外部网页: ${title} (${score.toFixed(4)})` : `外部网页: ${title}`,
          url,
        });
      } else if (tool === "query_media_record" || tool === "search_by_creator") {
        const title = String(row?.title || "").trim();
        const mediaType = String(row?.media_type || "").trim();
        const itemId = String(row?.id || "").trim();
        if (!title || !Number.isFinite(score)) continue;
        const label = `本地媒体库: ${title}${mediaType ? ` (${mediaType})` : ""} (${score.toFixed(4)})`;
        if (itemId) {
          const previewLink = `${pageLibraryUrl.replace(/\/$/, "")}/?item=${encodeURIComponent(itemId)}`;
          pushRef(mediaRefs, score, { label, url: previewLink });
        } else {
          pushRef(mediaRefs, score, { label, url: "" });
        }
      } else if (["search_tmdb_media", "search_bangumi_subject", "search_mediawiki_action", "parse_mediawiki_page", "expand_mediawiki_concept"].includes(tool)) {
        if (hasLocalMediaResults && hasPerItemExternalRefs && !item.data?.per_item_fanout) continue;
        const title = String(row?.external_title || row?.display_title || row?.title || row?.name_cn || row?.name || "").trim();
        const mediaType = String(row?.media_type || "").trim();
        const url = String(row?.url || "").trim();
        if (!title || !url) continue;
        const sourceLabel = externalSourceLabel(tool, row);
        const suffix = mediaType ? ` (${mediaType})` : "";
        pushRef(externalRefs, score, {
          label: Number.isFinite(score)
            ? `${sourceLabel}: ${title}${suffix} (${score.toFixed(4)})`
            : `${sourceLabel}: ${title}${suffix}`,
          url,
        });
      } else if (String(row?.external_source || "").trim()) {
        const title = String(row?.external_title || row?.title || "").trim();
        const url = String(row?.url || "").trim();
        if (!title || !url) continue;
        const sourceLabel = externalSourceLabel(tool, row);
        pushRef(externalRefs, score, {
          label: Number.isFinite(score)
            ? `${sourceLabel}: ${title} (${score.toFixed(4)})`
            : `${sourceLabel}: ${title}`,
          url,
        });
      }
    }
  }

  if (!docRefs.length && !mediaRefs.length && !externalRefs.length) return "";

  const sections = [];
  const inlineCitations = [];
  let nextIndex = 1;
  if (mediaRefs.length) {
    const rendered = renderReferenceSection("本地媒体库参考", mediaRefs.sort((a, b) => b.score - a.score).slice(0, MAX_REFERENCE_ITEMS), nextIndex);
    sections.push(rendered.section);
    inlineCitations.push(...rendered.inlineLinks);
    nextIndex = rendered.nextIndex;
  }
  if (docRefs.length) {
    const rendered = renderReferenceSection("本地文档参考", docRefs.sort((a, b) => b.score - a.score).slice(0, MAX_REFERENCE_ITEMS), nextIndex);
    sections.push(rendered.section);
    inlineCitations.push(...rendered.inlineLinks);
    nextIndex = rendered.nextIndex;
  }
  if (externalRefs.length) {
    const rendered = renderReferenceSection("外部参考", externalRefs.sort((a, b) => b.score - a.score).slice(0, Math.max(1, Math.min(3, MAX_REFERENCE_ITEMS))), nextIndex);
    sections.push(rendered.section);
    inlineCitations.push(...rendered.inlineLinks);
  }
  const citationLine = "";
  return `\n\n${citationLine ? `${citationLine}\n\n` : ""}${sections.join("\n\n")}`;
}

function hasReferenceSections(text) {
  const value = String(text || "");
  return /###\s*(?:本地知识库参考|本地文档参考|本地媒体库参考|外部参考|参考资料)/.test(value);
}

function setRowContent(row, markdownText) {
  if (!(row instanceof HTMLElement)) return;
  const content = row.querySelector(".content");
  if (!(content instanceof HTMLElement)) return;
  content.innerHTML = markdownToHtml(markdownText || "");
  rewriteLoopbackLinksInContainer(content);
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

function refreshActiveStreamView(sessionId = "") {
  if (!activeAgentStreamState) return;
  const targetSessionId = String(sessionId || activeAgentStreamState.sessionId || "").trim();
  if (!targetSessionId || String(currentSessionId || "") !== targetSessionId) return;
  const keepBottom = isQaMessagesNearBottom() || !qaMessages.querySelector('[data-stream-preview="1"]');
  renderActiveAgentStreamPreview({ keepBottom, replaceExisting: true });
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
    startedAt: pendingStart,
  };
  refreshActiveStreamView(requestSessionId);

  const pendingTimer = window.setInterval(() => {
    if (streamFinalized) return;
    refreshActiveStreamView(requestSessionId);
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
            refreshActiveStreamView(requestSessionId);
          } else if (event.type === "tool_done") {
            if (event.trace_id) activeTraceId = String(event.trace_id || "").trim();
            const tool = String(event.tool || "");
            const status = String(event.status || "").trim().toLowerCase();
            if (status === "skipped") continue;
            const summary = String(event.summary || "");
            const toolLabel = formatToolResultLabel(tool);
            toolDoneLines.push(summary ? `- ${toolLabel}：${summary}` : `- ${toolLabel}`);
            if (activeAgentStreamState && activeAgentStreamState.sessionId === requestSessionId) {
              activeAgentStreamState.toolDoneLines = [...toolDoneLines];
              activeAgentStreamState.traceId = activeTraceId;
            }
            refreshActiveStreamView(requestSessionId);
          } else if (event.type === "answer_delta") {
            if (event.trace_id) activeTraceId = String(event.trace_id || "").trim();
            const delta = String(event.delta || "");
            if (delta) {
              activeAgentStreamState.answerText = String(activeAgentStreamState.answerText || "") + delta;
              activeAgentStreamState.traceId = activeTraceId;
            }
            refreshActiveStreamView(requestSessionId);
          } else if (event.type === "quota_exceeded") {
            if (event.trace_id) activeTraceId = String(event.trace_id || "").trim();
            streamFinalized = true;
            quotaExceededEvent = event;
            if (activeAgentStreamState && activeAgentStreamState.sessionId === requestSessionId) {
              activeAgentStreamState.traceId = activeTraceId;
            }
            return { quotaExceeded: true, event };
          } else if (event.type === "error") {
            streamFinalized = true;
            throw new Error(String(event.message || "Agent 服务出错"));
          } else if (event.type === "done") {
            if (event.trace_id) activeTraceId = String(event.trace_id || "").trim();
            streamFinalized = true;
            return { done: true, payload: normalizeAgentResponse(event.payload) };
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
          if (event.type === "done") { streamFinalized = true; return { done: true, payload: normalizeAgentResponse(event.payload) }; }
          if (event.type === "error") { streamFinalized = true; throw new Error(String(event.message || "Agent 服务出错")); }
          if (event.type === "quota_exceeded") { streamFinalized = true; quotaExceededEvent = event; return { quotaExceeded: true, event }; }
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
        if (activeAgentStreamState && activeAgentStreamState.sessionId === requestSessionId) {
          activeAgentStreamState.progressLines = ["已拒绝超配额操作。"]; 
          activeAgentStreamState.toolDoneLines = [];
          activeAgentStreamState.answerText = "";
        }
        refreshActiveStreamView(requestSessionId);
        return;
      }
    }

    const result = streamResult?.payload;
    if (!result) throw new Error("未收到回答");
    if (!result.trace_id && activeTraceId) result.trace_id = activeTraceId;
    const resolvedSessionId = String(result.session_id || requestSessionId || "").trim();
    if (currentSessionId === requestSessionId) currentSessionId = resolvedSessionId;
    window.clearInterval(pendingTimer);

    const finalText = String(result.answer || "").trim() || "未返回回答。";
    appendLocalMessage(resolvedSessionId, "assistant", finalText, result.trace_id);
    activeAgentStreamState = null;
    if (currentSessionId === resolvedSessionId) {
      renderCurrentAgentSessionView();
    }
  } catch (err) {
    streamFinalized = true;
    window.clearInterval(pendingTimer);
    if (activeAgentStreamState && activeAgentStreamState.sessionId === requestSessionId) {
      activeAgentStreamState = null;
    }
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
    if (String(currentSessionId || "") === String(requestSessionId || "")) {
      renderCurrentAgentSessionView();
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
  await dashboardAgentSessionsBootstrap.createSessionAction();
}

async function deleteCurrentSessionAction() {
  await dashboardAgentSessionsBootstrap.deleteCurrentSessionAction();
}

function findTraceStageNode(scope, attributeName, key) {
  return Array.from(scope.querySelectorAll(`[${attributeName}]`)).find((node) => String(node.getAttribute(attributeName) || "") === key) || null;
}

function buildTraceStageTooltipData(scope, key) {
  if (!(scope instanceof Element) || !key) return null;
  const row = findTraceStageNode(scope, "data-trace-stage-row", key);
  const segment = findTraceStageNode(scope, "data-trace-stage-key", key);
  const label = row?.querySelector(".trace-stage-label")?.textContent?.trim() || formatTraceStageLabel(key);
  const duration = row?.children?.[1]?.textContent?.trim() || "";
  const ratio = row?.children?.[2]?.textContent?.trim() || "";
  const color = row?.querySelector(".trace-stage-label i")?.style.background || segment?.style.background || TRACE_STAGE_COLORS[0];
  return { key, label, duration, ratio, color };
}

let traceStageTooltipPortal = null;
let traceStageTooltipScopeCounter = 0;

function ensureTraceStageTooltipPortal() {
  if (traceStageTooltipPortal instanceof HTMLElement && document.body.contains(traceStageTooltipPortal)) {
    return traceStageTooltipPortal;
  }
  const tooltip = document.createElement("div");
  tooltip.className = "trace-stage-tooltip trace-stage-tooltip-floating hidden";
  tooltip.setAttribute("aria-hidden", "true");
  document.body.appendChild(tooltip);
  traceStageTooltipPortal = tooltip;
  return tooltip;
}

function getTraceStageScopeId(scope) {
  if (!(scope instanceof HTMLElement)) return "";
  if (!scope.dataset.traceStageScopeId) {
    traceStageTooltipScopeCounter += 1;
    scope.dataset.traceStageScopeId = `trace-stage-scope-${traceStageTooltipScopeCounter}`;
  }
  return scope.dataset.traceStageScopeId;
}

function hideTraceStageTooltipPortal(scopeId = "") {
  const tooltip = ensureTraceStageTooltipPortal();
  if (scopeId && tooltip.dataset.scopeId && tooltip.dataset.scopeId !== scopeId) return;
  tooltip.innerHTML = "";
  tooltip.classList.add("hidden");
  tooltip.setAttribute("aria-hidden", "true");
  tooltip.style.left = "0px";
  tooltip.style.top = "0px";
  delete tooltip.dataset.scopeId;
  delete tooltip.dataset.activeKey;
  delete tooltip.dataset.pinned;
}

function positionTraceStageTooltip(scope, tooltip, key) {
  if (!(scope instanceof HTMLElement) || !(tooltip instanceof HTMLElement) || !key) return;
  const anchor = findTraceStageNode(scope, "data-trace-stage-key", key) || findTraceStageNode(scope, "data-trace-stage-row", key);
  if (!(anchor instanceof HTMLElement)) return;
  const anchorRect = anchor.getBoundingClientRect();
  const spacing = 10;
  const tooltipRect = tooltip.getBoundingClientRect();
  const viewportWidth = window.innerWidth || document.documentElement.clientWidth || 0;
  const viewportHeight = window.innerHeight || document.documentElement.clientHeight || 0;
  let left = anchorRect.left + (anchorRect.width - tooltipRect.width) / 2;
  left = Math.min(Math.max(12, left), Math.max(12, viewportWidth - tooltipRect.width - 12));
  let top = anchorRect.top - tooltipRect.height - spacing;
  if (top < 12) {
    top = anchorRect.bottom + spacing;
  }
  top = Math.min(Math.max(12, top), Math.max(12, viewportHeight - tooltipRect.height - 12));
  tooltip.style.left = `${left}px`;
  tooltip.style.top = `${top}px`;
}

function setTraceStageTooltip(scope, key, { pinned = false } = {}) {
  if (!(scope instanceof Element)) return;
  const tooltip = ensureTraceStageTooltipPortal();
  const scopeId = getTraceStageScopeId(scope);
  const data = buildTraceStageTooltipData(scope, key);
  if (!data) {
    hideTraceStageTooltipPortal(scopeId);
    scope.querySelectorAll("[data-trace-stage-key], [data-trace-stage-row]").forEach((node) => {
      node.classList.remove("is-active");
    });
    return;
  }
  scope.querySelectorAll("[data-trace-stage-key], [data-trace-stage-row]").forEach((node) => {
    node.classList.toggle("is-active", String(node.getAttribute("data-trace-stage-key") || node.getAttribute("data-trace-stage-row") || "") === key);
  });
  const meta = [data.duration, data.ratio].filter(Boolean).join(" / ");
  tooltip.innerHTML = `<div class="trace-stage-tooltip-title"><i style="background:${data.color}"></i>${escapeHtml(data.label)}</div>
    <div class="trace-stage-tooltip-body">${escapeHtml(getTraceStageDescription(key))}</div>
    <div class="trace-stage-tooltip-meta">${escapeHtml(meta)}</div>`;
  tooltip.classList.remove("hidden");
  tooltip.setAttribute("aria-hidden", "false");
  tooltip.dataset.scopeId = scopeId;
  tooltip.dataset.activeKey = key;
  tooltip.dataset.pinned = pinned ? "1" : "0";
  positionTraceStageTooltip(scope, tooltip, key);
}

function clearTraceStageTooltip(scope, { force = false } = {}) {
  if (!(scope instanceof Element)) return;
  const tooltip = ensureTraceStageTooltipPortal();
  const scopeId = getTraceStageScopeId(scope);
  if (tooltip.dataset.scopeId !== scopeId) return;
  if (!force && tooltip.dataset.pinned === "1") return;
  scope.querySelectorAll("[data-trace-stage-key], [data-trace-stage-row]").forEach((node) => {
    node.classList.remove("is-active");
  });
  setTraceStageTooltip(scope, "");
}

function closeAllTraceStageTooltips() {
  document.querySelectorAll(".trace-stage-composite-wrap").forEach((scope) => {
    scope.querySelectorAll("[data-trace-stage-key], [data-trace-stage-row]").forEach((node) => {
      node.classList.remove("is-active");
    });
  });
  hideTraceStageTooltipPortal();
}

function _traceStageHoverHandler(event) {
  const target = event.target;
  const node = target instanceof Element ? target.closest("[data-trace-stage-key], [data-trace-stage-row]") : null;
  if (!node) return;
  const scope = node.closest(".trace-stage-composite-wrap");
  const key = String(node.getAttribute("data-trace-stage-key") || node.getAttribute("data-trace-stage-row") || "").trim();
  if (!scope || !key) return;
  const tooltip = scope.querySelector(".trace-stage-tooltip");
  if (tooltip instanceof HTMLElement && tooltip.dataset.pinned === "1") return;
  setTraceStageTooltip(scope, key);
}

// ── Module-level trace stage click handler (shared across tabs) ──
function _traceStageClickHandler(event) {
  const target = event.target;
  const toggle = target instanceof Element ? target.closest("[data-trace-stage-toggle]") : null;
  if (toggle) {
    const parentKey = String(toggle.getAttribute("data-trace-stage-toggle") || "").trim();
    const scope = toggle.closest(".trace-stage-composite-wrap");
    if (!scope || !parentKey) return;
    const expanded = toggle.getAttribute("aria-expanded") === "true";
    toggle.setAttribute("aria-expanded", expanded ? "false" : "true");
    scope.querySelectorAll(`[data-trace-stage-parent="${CSS.escape(parentKey)}"]`).forEach((row) => {
      row.classList.toggle("hidden", expanded);
    });
    const parentRow = toggle.closest("tr");
    parentRow?.classList.toggle("is-expanded", !expanded);
    return;
  }
  const segment = target instanceof Element ? target.closest("[data-trace-stage-key]") : null;
  const row = target instanceof Element ? target.closest("[data-trace-stage-row]") : null;
  const key = String(segment?.getAttribute("data-trace-stage-key") || row?.getAttribute("data-trace-stage-row") || "").trim();
  if (!key) return;
  const scope = target instanceof Element ? target.closest(".trace-stage-composite-wrap") : null;
  if (!scope) return;
  const tooltip = scope.querySelector(".trace-stage-tooltip");
  const alreadyPinned = tooltip instanceof HTMLElement && tooltip.dataset.pinned === "1" && tooltip.dataset.activeKey === key;
  if (alreadyPinned) {
    clearTraceStageTooltip(scope, { force: true });
    return;
  }
  closeAllTraceStageTooltips();
  setTraceStageTooltip(scope, key, { pinned: true });
}

// ── Event-handler registration — called once at init, independent of data loading ──
let _dashboardHandlersRegistered = false;
const dashboardHandlersBootstrap = DashboardHandlersBootstrapModule?.createHandlersBootstrap?.({
  dashboardRefreshBtn,
  refreshDashboard,
  renderDashboardError,
  usageSaveBtn: document.getElementById("usage-save-btn"),
  saveUsage,
  usageExportBtn,
  exportUsageTracesCsv,
  usageClearBtn,
  clearUsageTraces,
  usageProviderSelect,
  loadUsageTraces,
  renderUsageModal,
  usageCancelBtn: document.getElementById("usage-cancel-btn"),
  closeUsageModal,
  usageEditModal: document.getElementById("usage-edit-modal"),
  warningsClearBtn,
  clearWarnings,
  warningsCloseBtn,
  closeWarningsModal,
  missingQueriesClearBtn,
  clearMissingQueries,
  missingQueriesExportBtn,
  exportMissingQueriesCsv,
  missingQueriesSourceSelect,
  openMissingQueriesModal,
  missingQueriesCloseBtn,
  closeMissingQueriesModal,
  runtimeDataRefreshBtn,
  openRuntimeDataModal,
  runtimeDataClearBtn,
  clearRuntimeDataSelection,
  runtimeDataCloseBtn,
  closeRuntimeDataModal,
  warningsModal,
  missingQueriesModal,
  runtimeDataModal,
  dashboardJobsClearBtn,
  openTaskHistoryClearModal,
  dashboardJobsRefreshBtn,
  refreshTaskCenter,
  dashboardTicketTrendModeSelect,
  setDashboardTicketTrendMode: (value) => {
    dashboardTicketTrendMode = normalizeDashboardTicketTrendMode(value);
  },
  getDashboardTicketTrendMode: () => dashboardTicketTrendMode,
  getCurrentDashboardTicketTrendStats: () => currentDashboardTicketTrendStats,
  renderDashboardTicketTrend,
  dashboardJobsList,
  apiPost,
  setSelectedTaskJobId: (value) => {
    selectedTaskJobId = String(value || "").trim();
  },
  renderTaskCenter,
  getTaskJobIdFromTarget,
  openTaskJobContextMenu,
  bindLongPress,
  dashboardJobContextDeleteBtn,
  getCurrentTaskContextJobId: () => currentTaskContextJobId,
  openTaskDeleteModal,
  taskDeleteConfirmBtn,
  confirmTaskDeleteAction,
  taskDeleteCancelBtn,
  closeTaskDeleteModal,
  dashboardJobContextMenuController,
  dashboardGrid,
  debugUiEvent,
  openWarningsModal,
  openFeedbackModal,
  triggerLibraryGraphRebuild,
  openLibraryAliasModal,
  showAppErrorModal,
  openUsageModal,
  triggerRagSync,
  feedbackSourceSelect,
  feedbackExportBtn,
  exportFeedbackJson,
  feedbackClearBtn,
  clearFeedback,
  feedbackCloseBtn,
  closeFeedbackModal,
  feedbackDetailCloseBtn,
  closeFeedbackDetailModal,
  feedbackDetailOpenTraceBtn,
  getCurrentFeedbackDetailTraceId: () => String(currentFeedbackDetailItem?.trace_id || "").trim(),
  openTraceModal,
  feedbackModal,
  feedbackDetailModal,
  feedbackModalList,
  findFeedbackItemById: (feedbackId) => currentFeedbackItems.find((entry) => String(entry.id || "") === String(feedbackId || "").trim()) || null,
  showFeedbackDetail,
  usageBootstrapModule: DashboardUsageBootstrapModule,
  modalBootstrapModule: DashboardModalBootstrapModule,
  taskCenterBootstrapModule: DashboardTaskCenterBootstrapModule,
  dashboardTraceBootstrap,
  dashboardLibraryAliasController,
}) || { bindEvents() {} };

function _registerDashboardHandlers() {
  if (_dashboardHandlersRegistered) return;
  _dashboardHandlersRegistered = true;
  dashboardHandlersBootstrap.bindEvents();
}

let _authorizationHandlersRegistered = false;
function _registerAuthorizationHandlers() {
  if (_authorizationHandlersRegistered) return;
  _authorizationHandlersRegistered = true;

  authorizationUnlockBtn?.addEventListener("click", () => {
    unlockAuthorizationPanel().catch((error) => {
      showAuthorizationError(authorizationReauthError, String(error));
    });
  });
  authorizationPasswordInput?.addEventListener("keydown", (event) => {
    if (event.key !== "Enter") return;
    event.preventDefault();
    unlockAuthorizationPanel().catch((error) => {
      showAuthorizationError(authorizationReauthError, String(error));
    });
  });
  authorizationRefreshBtn?.addEventListener("click", () => {
    bootstrapAuthorizationTab({ forcePrompt: false }).catch((error) => {
      showAppErrorModal("Authorization 刷新失败", String(error));
    });
  });
  authorizationBootstrapBtn?.addEventListener("click", () => {
    createAuthorizationBootstrapAdmin().catch((error) => {
      showAuthorizationError(authorizationBootstrapError, String(error));
    });
  });
  authorizationCreateRole?.addEventListener("change", () => {
    const allAppIds = (authorizationState.data?.available_apps || []).map((item) => String(item.app_id || "")).filter(Boolean);
    const nextRole = String(authorizationCreateRole?.value || "user");
    renderAuthorizationAppChecks(
      authorizationCreateApps,
      authorizationState.data?.available_apps || [],
      nextRole === "admin" ? allAppIds : collectAuthorizationApps(authorizationCreateApps),
      { disabled: nextRole === "admin" },
    );
  });
  authorizationCreateBtn?.addEventListener("click", () => {
    createAuthorizationUser().catch((error) => {
      showAppErrorModal("Authorization 创建失败", String(error));
    });
  });
}

async function init() {
  try { if (window.__navBoot) window.__navBoot.mark("init-started"); } catch (_) {}
  initUiDebugStatus();
  installUiClickProbe();
  // ── Tab switching: must be the very first binding so tabs work even if
  // later setup steps throw. All tabs use addEventListener("click") for
  // consistent behaviour on mobile. Dashboard additionally gets a long-press
  // overlay for the usage modal.
  for (const tab of document.querySelectorAll(".tab")) {
    const switchFn = () => {
      const target = tab.dataset.tab || "home";
      debugUiEvent("tab click received", target);
      setMainTab(target);
      if (target === "dashboard") {
        bootstrapDashboardTab().catch((err) => { renderDashboardError(err); });
      }
      if (target === "tickets") {
        bootstrapTicketsTab().catch((err) => { showAppErrorModal("Tickets 加载失败", String(err)); });
      }
      if (target === "benchmark") {
        bootstrapBenchmarkTab().catch(() => {});
      }
      if (target === "authorization") {
        bootstrapAuthorizationTab({ forcePrompt: true }).catch((err) => { showAppErrorModal("Authorization 加载失败", String(err)); });
      } else {
        resetAuthorizationReauth();
      }
    };
    tab.addEventListener("click", switchFn);
    if ((tab.dataset.tab || "home") === "dashboard") {
      // Dashboard: long-press opens usage modal (no conflict with click above)
      bindLongPressElement(tab, openUsageModal);
    }
  }
  try { if (window.__navBoot) window.__navBoot.mark("tabs-bound"); } catch (_) {}

  // ── Register all tab handler bindings up-front so every control is
  // immediately clickable — independent of when the data actually loads.
  _registerDashboardHandlers();
  dashboardTicketsBootstrap.bindEvents();
  dashboardBenchmarkBootstrap.bindEvents();
  _registerAuthorizationHandlers();
  traceModalController?.bindBackdropClose?.("trace-backdrop");
  libraryAliasModalController?.bindBackdropClose?.("library-alias-backdrop");
  ticketDeleteModalController?.bindBackdropClose?.("ticket-delete-backdrop");
  taskDeleteModalController?.bindBackdropClose?.("task-delete-backdrop");
  benchmarkRouterClsModalController?.bindBackdropClose?.("bm-router-cls-backdrop");

  // Bind error modal controls early so a later init failure never leaves
  // a blocking modal that cannot be dismissed.
  appErrorCopyBtn?.addEventListener("click", () => {
    copyAppErrorText().catch((e) => showAppErrorModal("复制失败", String(e)));
  });
  appErrorCloseBtn?.addEventListener("click", closeAppErrorModal);
  appErrorModal?.addEventListener("click", (event) => {
    const target = event.target;
    if (target instanceof Element && target.getAttribute("data-role") === "app-error-backdrop") closeAppErrorModal();
  });

  setModel();
  const currentTab = String(document.querySelector(".tab.active")?.getAttribute("data-tab") || "home");
  setMainTab(currentTab);
  dashboardCustomCardsBootstrap.bindEvents();
  dashboardCustomCardsBootstrap.loadInitialCards();
  dashboardCustomCardsBootstrap.hydratePendingCardEdit();
  loadInitialAgentSessions();
  hydrateDashboardShellFromStoredData();
  if (currentTab !== "dashboard") prewarmDashboardOverview();
  if (currentTab === "dashboard") {
    bootstrapDashboardTab().catch((err) => { renderDashboardError(err); });
  } else if (currentTab === "tickets") {
    bootstrapTicketsTab().catch((err) => { showAppErrorModal("Tickets 加载失败", String(err)); });
  } else if (currentTab === "benchmark") {
    bootstrapBenchmarkTab().catch(() => {});
  } else if (currentTab === "authorization") {
    bootstrapAuthorizationTab({ forcePrompt: true }).catch((err) => { showAppErrorModal("Authorization 加载失败", String(err)); });
  }

  mountAgentSidebarController();
  dashboardAgentSessionUiBootstrap.bindEvents();

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      if (warningsModal && !warningsModal.classList.contains("hidden")) closeWarningsModal();
      if (missingQueriesModal && !missingQueriesModal.classList.contains("hidden")) closeMissingQueriesModal();
      if (dashboardLibraryAliasController.isOpen()) closeLibraryAliasModal();
      if (traceModalController?.isOpen?.()) closeTraceModal();
      if (ticketDeleteModalController?.isOpen?.()) closeTicketDeleteModal();
      if (taskDeleteModalController?.isOpen?.()) closeTaskDeleteModal();
      if (benchmarkRouterClsModalController?.isOpen?.()) closeRouterClassificationEditor();
      if (customCardModal && !customCardModal.classList.contains("hidden")) dashboardCustomCardsBootstrap.closeCustomCardModal();
      const usageModal = document.getElementById("usage-edit-modal");
      if (usageModal && !usageModal.classList.contains("hidden")) closeUsageModal();
      if (appErrorModal && !appErrorModal.classList.contains("hidden")) closeAppErrorModal();
      closeAllTraceStageTooltips();
    }
  });

  traceModalCloseBtn?.addEventListener("click", closeTraceModal);
  traceModalExportBtn?.addEventListener("click", () => {
    exportCurrentTrace().catch((e) => showAppErrorModal("Trace 导出失败", String(e)));
  });
  traceModalContent?.addEventListener("click", _traceStageClickHandler);
  traceModalContent?.addEventListener("mouseover", _traceStageHoverHandler);
  traceModalContent?.addEventListener("focusin", _traceStageHoverHandler);
  traceModalContent?.addEventListener("mouseleave", () => clearTraceStageTooltip(traceModalContent, { force: false }));
  document.addEventListener("click", (event) => {
    const target = event.target;
    if (target instanceof Element && target.closest(".trace-stage-composite-wrap")) return;
    closeAllTraceStageTooltips();
  });
  window.addEventListener("resize", () => {
    renderTicketsListCollapseButton();
    syncTicketsPaneHeights();
    closeAllTraceStageTooltips();
  });
  // ── Background async work: fires after all bindings are set up ──
  // Sessions are loaded in the background so interactions are never blocked.
  refreshSessions("").catch((err) => {
    console.warn("[init] session load failed:", err);
    const ul = document.getElementById("agent-session-list");
    if (ul) ul.innerHTML = '<li class="list-loading" style="color:#c05050">会话加载失败，请刷新页面</li>';
  });

  wireChatLinks();
  try { if (window.__navBoot) window.__navBoot.mark("init-done"); } catch (_) {}
}

init().catch((err) => {
  console.error("[init] 初始化失败:", err);
  // Try to show a visible modal; fall back to alert if the modal isn't ready yet.
  try {
    showAppErrorModal("初始化失败", String(err), "请刷新页面重试");
  } catch (_e) {
    window.alert(`初始化失败：${String(err)}`);
  }
});