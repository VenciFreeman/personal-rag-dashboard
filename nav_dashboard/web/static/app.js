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
const dashboardStartupLogs = document.getElementById("dashboard-startup-logs");
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
const pageLocalModel = (document.body?.dataset?.localModel || "").trim() || "qwen2.5-7b-instruct";
const pageAiSummaryUrl = (document.body?.dataset?.aiSummaryUrl || "").trim() || "http://127.0.0.1:8000/";
const pageLibraryUrl = (document.body?.dataset?.libraryUrl || "").trim() || "http://127.0.0.1:8091/";

let activeController = null;
let askInFlight = false;
let dashboardBootstrapped = false;
let benchmarkBootstrapped = false;
let sessionsCache = [];
let currentSessionId = "";
let customCards = [];
let editingCardIndex = -1;
const CARD_LONG_PRESS_MS = 520;

// Warnings state
let currentWarnings = [];
let currentWarningsTimestamp = "";
let dismissedWarnings = new Set();
let currentMissingQueries = [];
let currentMissingQueriesSource = "all";

// Startup polling
let lastStartupStatus = "";
let startupPollInterval = null;

// Crop state
let cropState = null;
const CROP_VSIZE = 200;

// Usage modal: last known values for prefill
let lastApiUsage = {};

function escapeHtml(text) {
  return String(text || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

// ─── Long-press helpers ───────────────────────────────────────────────────────

const LONG_PRESS_MS = 600;

/**
 * Bind a long-press handler to a container element using event delegation.
 * Long-press fires `callback(target)` after LONG_PRESS_MS ms without moving/releasing.
 * @param {Element} el  - Container to listen on
 * @param {function} callback - Called with the original target Element
 */
function bindLongPress(el, callback) {
  let timer = null;
  let startX = 0, startY = 0;
  const cancel = () => { if (timer) { clearTimeout(timer); timer = null; } };
  el.addEventListener("pointerdown", (e) => {
    if (!(e.target instanceof Element)) return;
    startX = e.clientX; startY = e.clientY;
    const target = e.target;
    cancel();
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
  const cancel = () => { if (timer) { clearTimeout(timer); timer = null; } };
  el.addEventListener("pointerdown", (e) => {
    startX = e.clientX; startY = e.clientY;
    didLongPress = false;
    cancel();
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

    while (idx < allItems.length) {
      let item = allItems[idx];
      if (item.indent < targetIndent) break;
      if (item.indent > targetIndent) item = { ...item, indent: targetIndent };

      if (currentType !== item.type) {
        if (currentType) out += `</${currentType}>\n`;
        out += `<${item.type}>\n`;
        currentType = item.type;
      }

      let content = item.text;
      idx += 1;
      if (idx < allItems.length && allItems[idx].indent > targetIndent) {
        const child = renderListTree(allItems, idx, allItems[idx].indent);
        if (child.html) content += `\n${child.html}`;
        idx = child.nextIdx;
      }
      out += `<li>${content}</li>\n`;
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
      out.push(`<p>${raw}</p>`);
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
  const panelBenchmark = document.getElementById("panel-benchmark");
  if (panelHome) panelHome.classList.toggle("active", name === "home");
  if (panelAgent) panelAgent.classList.toggle("active", name === "agent");
  if (panelDashboard) panelDashboard.classList.toggle("active", name === "dashboard");
  if (panelBenchmark) panelBenchmark.classList.toggle("active", name === "benchmark");
}

function formatNum(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "0";
  return new Intl.NumberFormat("zh-CN").format(Math.round(n));
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
  const webUsageRatio = safeRatio(apiUsage.today_web_search, apiUsage.daily_web_limit);
  const deepseekUsageRatio = safeRatio(apiUsage.today_deepseek, apiUsage.daily_deepseek_limit);

  return {
    ragGraphDensity:
      isOutsideRange(rag.nodes_per_doc, 8, 20) ||
      isOutsideRange(rag.edges_per_node, 2, 5),
    ragPending: isAboveThreshold(rag.changed_pending, 0),
    webUsage: isAboveThreshold(webUsageRatio, 0.9),
    deepseekUsage: isAboveThreshold(deepseekUsageRatio, 0.9),
    vectorRecall:
      isAboveThreshold(latency.stages?.total?.avg, 0.35),
    rerankLatency:
      isAboveThreshold(latency.stages?.rerank_seconds?.avg, 3.5),
    retrievalPercentiles:
      isAboveThreshold(latency.stages?.total?.p50, 0.25) ||
      isAboveThreshold(latency.stages?.total?.p95, 0.45) ||
      isAboveThreshold(latency.stages?.total?.p99, 0.8),
    endToEnd:
      isAboveThreshold(latency.stages?.elapsed_seconds?.p50, 9) ||
      isAboveThreshold(latency.stages?.elapsed_seconds?.p95, 18) ||
      isAboveThreshold(agentWallClock.p50, 15),
    rerankChangeRate:
      isAboveThreshold(ragRerank.top1_identity_change_rate, 0.35) ||
      isAboveThreshold(agentRerank.top1_identity_change_rate, 0.35),
    rankShift:
      isAbsAboveThreshold(ragRerank.avg_rank_shift, 2) ||
      isAbsAboveThreshold(agentRerank.avg_rank_shift, 2),
    embedCache: isBelowThreshold(cacheStats.rag_embed_cache_hit_rate, 0.7),
    noContext:
      isAboveThreshold(cacheStats.rag_no_context_rate, 0.15) ||
      isAboveThreshold(cacheStats.agent_no_context_rate, 0.15),
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
    { key: "rerank_seconds",           label: "重排序" },
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

async function refreshDashboard({ force = false } = {}) {
  if (dashboardGeneratedAt) dashboardGeneratedAt.textContent = "正在拉取最新状态...";
  if (dashboardRefreshBtn) dashboardRefreshBtn.disabled = true;
  try {
    const url = force ? "/api/dashboard/overview?force=true" : "/api/dashboard/overview";
    const data = await apiGet(url);
    const rag = data?.rag || {};
    const library = data?.library || {};
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
    lastStartupStatus = String(startup.status || "unknown");
    lastApiUsage = apiUsage;
    const warnings = Array.isArray(data?.warnings) ? data.warnings : [];
    const health = buildDashboardHealthFlags({
      rag,
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
      buildStatCard("Library Graph 节点数", formatNum(library.graph_nodes), `边数 ${formatNum(library.graph_edges)}`),

      buildStatCard("RAG 平均节点数", `${rag.nodes_per_doc != null ? Number(rag.nodes_per_doc).toFixed(2) : "—"}`, `每节点平均边数 ${rag.edges_per_node != null ? Number(rag.edges_per_node).toFixed(2) : "—"}`, "", unhealthyState(health.ragGraphDensity)),
      buildStatCard("RAG 待重建文档", formatNum(rag.changed_pending), rag.changed_pending > 0 ? "等待后台同步" : "已全部同步", "rag-changed-pending", unhealthyState(health.ragPending)),

      buildStatCard("本月 Tavily API 调用", formatNum(apiUsage.month_web_search_calls), `今日 ${formatNum(apiUsage.today_web_search)} / 限额 ${formatNum(apiUsage.daily_web_limit)}`, "web-search-usage", unhealthyState(health.webUsage)),
      buildStatCard("本月 DeepSeek API 调用", formatNum(apiUsage.month_deepseek_calls), `今日 ${formatNum(apiUsage.today_deepseek)} / 限额 ${formatNum(apiUsage.daily_deepseek_limit)}`, "deepseek-usage", unhealthyState(health.deepseekUsage)),

      buildStatCard("Agent 消息总数", formatNum(agent.message_count), `会话数 ${formatNum(agent.session_count)}`),
      buildStatCard("RAG Q&A 消息总数", formatNum(ragQa.message_count), `会话数 ${formatNum(ragQa.session_count)}`),

      buildStatCard("向量召回均值", formatDuration(latency.stages?.total?.avg), `近 ${formatNum(latency.stages?.total?.count)} 次 | p50 ${formatDuration(latency.stages?.total?.p50)}`, "", unhealthyState(health.vectorRecall)),
      buildStatCard("重排序均值", formatDuration(latency.stages?.rerank_seconds?.avg), `近 ${formatNum(latency.stages?.rerank_seconds?.count)} 次 | 含排序回退用时`, "", unhealthyState(health.rerankLatency)),

      buildStatCard("检索分位 p50", `${formatDuration(latency.stages?.total?.p50)}`, `p95 ${formatDuration(latency.stages?.total?.p95)} | p99 ${formatDuration(latency.stages?.total?.p99)}`, "", unhealthyState(health.retrievalPercentiles)),
      buildStatCard("RAG 全流程 p50",`${formatDuration(latency.stages?.elapsed_seconds?.p50)}`,`p95 ${formatDuration(latency.stages?.elapsed_seconds?.p95)} | Agent p50 ${formatDuration(agentWallClock.p50)}`, "", unhealthyState(health.endToEnd)),

      buildStatCard("RAG 重排序换榜率", `${formatRate(ragRerank.top1_identity_change_rate)}`, `Agent 换榜率 ${formatRate(agentRerank.top1_identity_change_rate)}`, "", unhealthyState(health.rerankChangeRate)),
      buildStatCard("RAG 平均换榜", `${formatSigned(ragRerank.avg_rank_shift, 2)}`, `Agent 平均换榜 ${formatSigned(agentRerank.avg_rank_shift, 2)}`, "", unhealthyState(health.rankShift)),
      
      buildStatCard("Embedding 缓存命中率", formatRate(cacheStats.rag_embed_cache_hit_rate), `近 ${formatNum(latency.record_count)} 次`, "", unhealthyState(health.embedCache)),
      buildStatCard("Agent 文档调用率", `${formatRate(cacheStats.agent_rag_trigger_rate)}`, `Media ${formatRate(cacheStats.agent_media_trigger_rate)} | Web ${formatRate(cacheStats.agent_web_trigger_rate)}`),

      buildStatCard("RAG 未命中率", formatRate(cacheStats.rag_no_context_rate), `Agent 未命中率 ${formatRate(cacheStats.agent_no_context_rate)}`, "", unhealthyState(health.noContext)),
      buildStatCard("月检索缺失问题数", formatNum(missingQueries.count), "长按查看导出", "missing-queries-summary"),
      
      buildStatCard("RAG 最相关 Top1", ragRerank.avg_top1_local_doc_score_p99 != null ? Number(ragRerank.avg_top1_local_doc_score_p99).toFixed(4) : "—", `同口径均值 ${ragRerank.avg_top1_local_doc_score != null ? Number(ragRerank.avg_top1_local_doc_score).toFixed(4) : "—"}`, "", unhealthyState(health.top1Score)),
      buildStatCard("Agent 综合 Top1", agentRerank.avg_top1_local_doc_score_p99 != null ? Number(agentRerank.avg_top1_local_doc_score_p99).toFixed(4) : "—", `同口径均值 ${agentRerank.avg_top1_local_doc_score != null ? Number(agentRerank.avg_top1_local_doc_score).toFixed(4) : "—"}`),
      
      buildStatCard("模型预热状态", String(startup.status || "unknown"), String(startup.last_checked_at || "")),
    ];

    // Store warnings for modal
    currentWarnings = warnings.filter(w => !dismissedWarnings.has(w));
    currentWarningsTimestamp = String(data?.generated_at || "").trim();

    cards.push(buildStatCard("系统告警", formatNum(currentWarnings.length), currentWarnings.length > 0 ? currentWarnings.slice(0, 2).join(" | ") : "无告警", "warnings-summary", unhealthyState(health.warnings)));

    if (dashboardGrid) {
      dashboardGrid.innerHTML = cards.join("\n");
    }

    renderDashboardLatencyTable(data);
    renderDashboardStartupLogs(data);

    const generated = String(data?.generated_at || "").trim();
    const month = String(data?.month || "").trim();
    const deployed = String(data?.deployed_at || "").trim();
    if (dashboardGeneratedAt) {
      dashboardGeneratedAt.textContent = `统计月份: ${month || "-"} | 更新时间: ${generated || "-"}`;
    }
    if (dashboardDeployTime && deployed && !dashboardDeployTime.dataset.set) {
      dashboardDeployTime.textContent = `部署时间: ${deployed}`;
      dashboardDeployTime.dataset.set = "1";
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
    renderDashboardStartupLogs(prefill);
  }
  await refreshDashboard();
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
  if (!window.confirm("触发 RAG 向量重建同步？（将在后台处理所有 changed=true 的文档）")) return;
  if (dashboardGeneratedAt) dashboardGeneratedAt.textContent = "正在触发 RAG 同步...";
  try {
    await apiPost("/api/dashboard/trigger-rag-sync", {});
    if (dashboardGeneratedAt) dashboardGeneratedAt.textContent = "RAG 同步任务已触发，后台重建中...";
    setTimeout(() => refreshDashboard().catch(() => {}), 3000);
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
    missingQueriesModalMeta.textContent = `最近30天: ${formatNum(currentMissingQueries.length)} 条 | 来源: ${currentMissingQueriesSource}`;
  }
  if (missingQueriesModalList) {
    missingQueriesModalList.innerHTML = currentMissingQueries.length
      ? currentMissingQueries.map((row) => {
          const ts = escapeHtml(String(row?.ts || ""));
          const source = escapeHtml(String(row?.source || "unknown"));
          const query = escapeHtml(String(row?.query || ""));
          const top1 = row?.top1_score != null ? Number(row.top1_score).toFixed(4) : "—";
          const th = row?.threshold != null ? Number(row.threshold).toFixed(4) : "—";
          return `<li><strong>${ts}</strong> [${source}]<br/>${query}<br/><span class="dashboard-meta">top1=${top1}, threshold=${th}</span></li>`;
        }).join("")
      : "<li>最近30天暂无未命中 query</li>";
  }
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

// Current test-set selection: "<module>/<length>"
let currentBmTestSet = "rag/short";

function renderBenchmarkTable(results) {
  const table = document.getElementById("bm-history-table");
  if (!table) return;

  const testSet = currentBmTestSet;
  const [module, length] = testSet.split("/");

  if (!results.length) {
    table.innerHTML = `<tbody><tr><td colspan="6" style="text-align:center;color:#7a7f6f">运行测试后查看历史对比数据</td></tr></tbody>`;
    return;
  }

  // Only keep runs that contain data for the selected module+length
  const relevant = results.filter((r) => r?.[module]?.by_length?.[length]);
  if (!relevant.length) {
    const mod = module.toUpperCase();
    const lenLabel = { short: "短", medium: "中", long: "长" }[length] || length;
    table.innerHTML = `<tbody><tr><td colspan="6" style="text-align:center;color:#7a7f6f">当前测试集（${mod} / ${lenLabel}查询）暂无数据，请先运行包含该模块的测试</td></tr></tbody>`;
    return;
  }

  // Up to 5 most-recent matching runs, newest first (→ leftmost column)
  const recent = relevant.slice(-5).reverse();

  const thead = `<thead><tr>
    <th style="min-width:120px">指标</th>
    ${recent.map((r, i) => {
      const ts = String(r.timestamp || "").replace("T", " ").slice(5, 16);
      const mods = (r.config?.modules || []).map((m) => m.toUpperCase()).join("+");
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
    //["总时长 p99",      (d) => fmt(d?.p99_wall_clock_s)],
    ["端到端均值",   (d) => fmt(d?.avg_elapsed_s)],
    ["向量召回均值",   (d) => fmt(d?.avg_total_s)],
    ["向量召回 p95",    (d) => fmt(d?.p95_total_s)],
    ["重排序均值",     (d) => fmt(d?.avg_rerank_seconds_s)],
    //["上下文组装 均值", (d) => fmt(d?.avg_context_assembly_seconds_s)],
    ["文档未命中率",   (d) => fmtPct(d?.no_context_rate)],
    ["有效 / 总查询数", (d) => fmtN(d)],
  ];
  const agentRows = [
    ["总时长均值",   (d) => fmt(d?.avg_wall_clock_s)],
    ["总时长 p95",    (d) => fmt(d?.p95_wall_clock_s)],
    ["总时长 p99",    (d) => fmt(d?.p99_wall_clock_s)],
    ["向量召回均值", (d) => fmt(d?.avg_vector_recall_seconds_s)],
    ["重排序均值",   (d) => fmt(d?.avg_rerank_seconds_s)],
    ["文档未命中率",  (d) => fmtPct(d?.no_context_rate)],
    ["有效 / 总查询数", (d) => fmtN(d)],
  ];
  const rowDefs = module === "rag" ? ragRows : agentRows;

  const rows = rowDefs.map(([label, getter]) =>
    `<tr><td style="white-space:nowrap;color:#b0b89a;font-size:0.82em">${escapeHtml(label)}</td>${recent
      .map((r) => `<td>${getter(r?.[module]?.by_length?.[length])}</td>`)
      .join("")}</tr>`
  );
  table.innerHTML = `${thead}<tbody>${rows.join("")}</tbody>`;
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

  // Wire up the test-set dropdown
  const sel = document.getElementById("bm-testset-select");
  if (sel) {
    sel.value = currentBmTestSet;
    sel.addEventListener("change", () => {
      currentBmTestSet = sel.value;
      renderBenchmarkTable(benchmarkHistory);
    });
  }

  benchmarkBootstrapped = true;
}

function runBenchmark() {
  const ragCheck = document.getElementById("bm-rag");
  const agentCheck = document.getElementById("bm-agent");
  const countRadio = document.querySelector("input[name='bm-count']:checked");
  const modules = [];
  if (ragCheck?.checked) modules.push("rag");
  if (agentCheck?.checked) modules.push("agent");
  if (!modules.length) { window.alert("请至少选择一个测试模块"); return; }
  const queryCount = parseInt(countRadio?.value || "3", 10);

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

  benchmarkAbortController = new AbortController();
  const body = JSON.stringify({ modules, query_count_per_type: queryCount });
  fetch("/api/benchmark/run", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body,
    signal: benchmarkAbortController.signal,
  }).then(async (resp) => {
    if (!resp.ok) {
      const msg = await resp.text().catch(() => "未知错误");
      throw new Error(msg);
    }
    const reader = resp.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split("\n");
      buffer = lines.pop() || "";
      for (const line of lines) {
        if (!line.startsWith("data: ")) continue;
        let event;
        try { event = JSON.parse(line.slice(6)); } catch (_e) { continue; }
        if (event.type === "progress") {
          const msg = String(event.message || "");
          if (progressText) progressText.textContent = msg;
          if (progressFill && event.total > 0) {
            progressFill.style.width = `${Math.round((event.current / event.total) * 100)}%`;
          }
          if (logBox) { logBox.textContent += `${msg}\n`; logBox.scrollTop = logBox.scrollHeight; }
        } else if (event.type === "result") {
          benchmarkHistory.push(event.data);
          renderBenchmarkTable(benchmarkHistory);
          if (lastRun) lastRun.textContent = `上次运行: ${event.data.timestamp || ""}`;
          if (progressFill) progressFill.style.width = "100%";
          if (progressText) progressText.textContent = "测试完成";
          if (logBox) { logBox.textContent += "✓ 测试完成\n"; logBox.scrollTop = logBox.scrollHeight; }
        } else if (event.type === "error") {
          if (progressText) progressText.textContent = `错误: ${event.message}`;
          if (logBox) { logBox.textContent += `✗ 错误: ${event.message}\n`; logBox.scrollTop = logBox.scrollHeight; }
        }
      }
    }
  }).catch((err) => {
    if (err?.name === "AbortError") {
      if (progressText) progressText.textContent = "已中止";
      if (logBox) { logBox.textContent += "⏸ 用户中止测试\n"; logBox.scrollTop = logBox.scrollHeight; }
    } else {
      if (progressText) progressText.textContent = `连接失败: ${String(err)}`;
    }
  }).finally(() => {
    if (runBtn) runBtn.disabled = false;
    if (abortBtn) abortBtn.disabled = true;
    if (benchmarkTimerInterval) { clearInterval(benchmarkTimerInterval); benchmarkTimerInterval = null; }
    benchmarkAbortController = null;
  });
}

async function clearBenchmarkHistory() {
  if (!window.confirm("确定清除所有 Benchmark 历史记录？")) return;
  await apiDelete("/api/benchmark/history");
  benchmarkHistory = [];
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

function renderSessions() {
  const ul = document.getElementById("agent-session-list");
  ul.innerHTML = "";
  for (const session of sessionsCache) {
    const li = document.createElement("li");
    if (session.id === currentSessionId) li.classList.add("active");
    li.innerHTML = `<div class=\"title\">${escapeHtml(session.title || "新会话")}</div><div class=\"meta\">${escapeHtml(session.updated_at || "")}</div>`;
    li.onclick = () => {
      currentSessionId = session.id;
      renderSessions();
      renderChat(session.messages || []);
    };
    ul.appendChild(li);
  }
}

function appendChatRow(role, text, keepBottom = true, extraClass = "") {
  const row = document.createElement("div");
  row.className = `msg ${role}${extraClass ? ` ${extraClass}` : ""}`;
  const roleLabel = role === "user" ? "用户" : role === "assistant" ? "助手" : "系统";
  row.innerHTML = `<div class="role">${roleLabel}</div><div class="content markdown-body">${markdownToHtml(text)}</div>`;
  qaMessages.appendChild(row);
  if (keepBottom) qaMessages.scrollTop = qaMessages.scrollHeight;
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
    if (role === "assistant") {
      const parsed = splitThinkBlocks(text);
      const assistantRow = appendChatRow("assistant", parsed.answer || text, false);
      insertSystemRowsBefore(assistantRow, parsed.thoughts);
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
  renderChat(getCurrentSession()?.messages || []);
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
  const lines = refs.sort((a, b) => b.score - a.score).map((x) => x.line);
  return `\n\n### 参考资料\n${lines.join("\n")}`;
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

function appendLocalMessage(role, text) {
  const session = getCurrentSession();
  if (!session) return;
  if (!Array.isArray(session.messages)) session.messages = [];
  session.messages.push({ role, text });
  session.updated_at = new Date().toISOString().slice(0, 19);
  renderSessions();
}

async function ask(searchMode) {
  if (askInFlight) return;
  const question = (qaInput.value || "").trim();
  if (!question) return;

  qaInput.value = "";
  appendChatRow("user", question);
  appendLocalMessage("user", question);

  const pendingStart = Date.now();
  const pending = appendChatRow("system", "正在规划工具并调用... (00:00)", true, "processing");
  const pendingContent = pending.querySelector(".content");
  let progressLines = [];
  let toolDoneLines = [];
  let streamFinalized = false;
  let quotaExceededEvent = null;

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
            progressLines.push(String(event.message || ""));
          } else if (event.type === "tool_done") {
            const tool = String(event.tool || "");
            const status = String(event.status || "");
            const summary = String(event.summary || "");
            const statusIcon = status === "ok" ? "✓" : status === "error" ? "✗" : "•";
            toolDoneLines.push(`${statusIcon} **${tool}** [${status}]${summary ? `: ${summary}` : ""}`);
          } else if (event.type === "quota_exceeded") {
            streamFinalized = true;
            quotaExceededEvent = event;
            pending.classList.remove("processing");
            return { quotaExceeded: true, event };
          } else if (event.type === "error") {
            streamFinalized = true;
            throw new Error(String(event.message || "Agent 服务出错"));
          } else if (event.type === "done") {
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

    currentSessionId = String(result.session_id || currentSessionId || "").trim();

    // Show final tool summary in the pending row before removing it
    const finalDetail = buildPlanDetailsMarkdown(result, (Date.now() - pendingStart) / 1000);
    setRowContent(pending, finalDetail);
    pending.classList.remove("processing");
    window.clearInterval(pendingTimer);
    pending.remove();

    let finalText = String(result.answer || "").trim() || "未返回回答。";
    if (!/参考资料/.test(finalText)) {
      const refs = buildReferencesMarkdown(result.tool_results);
      if (refs) finalText += refs;
    }

    const parsed = splitThinkBlocks(finalText);
    const assistantRow = appendChatRow("assistant", parsed.answer || finalText);
    insertSystemRowsBefore(assistantRow, parsed.thoughts);
    appendLocalMessage("assistant", finalText);
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
      appendLocalMessage("assistant", `**错误**: ${msg}`);
    }
  } finally {
    streamFinalized = true;
    window.clearInterval(pendingTimer);
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
      if (customCardModal && !customCardModal.classList.contains("hidden")) closeCustomCardModal();
      const usageModal = document.getElementById("usage-edit-modal");
      if (usageModal && !usageModal.classList.contains("hidden")) closeUsageModal();
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
          missingQueriesModalMeta.textContent = `最近30天: ${formatNum(currentMissingQueries.length)} 条 | 来源: ${currentMissingQueriesSource}`;
        }
        if (missingQueriesModalList) {
          missingQueriesModalList.innerHTML = currentMissingQueries.length
            ? currentMissingQueries.map((row) => {
                const ts = escapeHtml(String(row?.ts || ""));
                const source = escapeHtml(String(row?.source || "unknown"));
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
  // Long-press on warnings/usage cards to open their modals
  if (dashboardGrid) bindLongPress(dashboardGrid, (target) => {
    const warningsCard = target.closest("[data-role='warnings-summary']");
    if (warningsCard) { openWarningsModal(); return; }
    const missingQueriesCard = target.closest("[data-role='missing-queries-summary']");
    if (missingQueriesCard) {
      openMissingQueriesModal().catch((e) => window.alert(`加载失败: ${String(e)}`));
      return;
    }
    const usageCard = target.closest("[data-role='web-search-usage'],[data-role='deepseek-usage']");
    if (usageCard) openUsageModal();
    const ragPendingCard = target.closest("[data-role='rag-changed-pending']");
    if (ragPendingCard) triggerRagSync();
  });

  // Benchmark
  document.getElementById("bm-run-btn")?.addEventListener("click", runBenchmark);
  document.getElementById("bm-abort-btn")?.addEventListener("click", () => {
    benchmarkAbortController?.abort();
  });

  wireChatLinks();
}

init().catch((err) => {
  appendChatRow("assistant", `**初始化失败**: ${String(err)}`);
});