let currentSessionId = "";
let currentMode = "local";
let askInFlight = false;
let sessionsCache = [];
let activeRagStreamState = null;
const collapsedDirs = new Set();
let localEmbeddingModelDisplay = "本地模型";
let previewTreeNodes = [];
let previewMode = "tree";
let overlayTimer = null;
let overlayStartedAt = 0;
let workflowJobId = "";
let workflowPollingTimer = null;
let workflowLogRenderedLines = 0;
let workflowApiKeyVisible = false;
let currentRenameSessionId = "";
let suppressSessionClickUntil = 0;
const AppShell = window.CoreUI?.get?.("appShell") || window.SharedAppShell || null;

const LONG_PRESS_MS = 600;
const sessionRenameModal = () => document.getElementById("rag-session-rename-modal");
const sessionRenameInput = () => document.getElementById("rag-session-rename-input");
const sessionRenameMeta = () => document.getElementById("rag-session-rename-meta");

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

function setLongPressSelectionLock(locked) {
  document.body?.classList.toggle("long-press-selection-lock", !!locked);
}

function bindLongPress(el, callback) {
  let timer = null;
  let startX = 0;
  let startY = 0;
  const cancel = () => {
    if (timer) {
      clearTimeout(timer);
      timer = null;
    }
    setLongPressSelectionLock(false);
  };
  el.addEventListener("pointerdown", (event) => {
    if (!(event.target instanceof Element)) return;
    startX = event.clientX;
    startY = event.clientY;
    cancel();
    if (event.pointerType !== "mouse") setLongPressSelectionLock(true);
    timer = setTimeout(() => {
      timer = null;
      callback(event.target);
    }, LONG_PRESS_MS);
  });
  el.addEventListener("pointermove", (event) => {
    if (Math.abs(event.clientX - startX) > 8 || Math.abs(event.clientY - startY) > 8) cancel();
  });
  el.addEventListener("pointerup", cancel);
  el.addEventListener("pointercancel", cancel);
}

function setTab(name) {
  const tabs = ["rag", "preview", "workflow"];
  for (const tab of document.querySelectorAll(".tab")) {
    const active = tab.dataset.tab === name;
    tab.classList.toggle("active", active);
    tab.setAttribute("aria-selected", String(active));
  }
  for (const tabName of tabs) {
    const panel = document.getElementById(`panel-${tabName}`);
    if (panel) {
      panel.classList.toggle("active", name === tabName);
    }
  }
}

function mountAppSidebarController(sidebarId, scope, options = {}) {
  const key = String(sidebarId || "").trim();
  const sidebarScope = String(scope || "").trim();
  if (!key || !sidebarScope || !AppShell?.mountSidebarController) return null;
  const registry = window.__aiConversationSidebarControllers || (window.__aiConversationSidebarControllers = new Map());
  if (registry.has(key)) return registry.get(key);
  const sidebar = document.getElementById(key);
  const button = sidebar?.querySelector(".sidebar-toggle") || null;
  const workspace = sidebar?.closest(".workspace") || null;
  if (!sidebar || !button || !workspace) return null;
  const controller = AppShell.mountSidebarController({
    appId: "ai_conversations_summary",
    scope: sidebarScope,
    shell: workspace,
    sidebar,
    toggleButton: button,
    storageKeyAliases: Array.isArray(options.storageKeyAliases) ? options.storageKeyAliases : [],
  });
  registry.set(key, controller);
  return controller;
}

function setMode(mode) {
  currentMode = mode;
  for (const btn of document.querySelectorAll(".mode-btn")) {
    btn.classList.toggle("active", btn.dataset.mode === mode);
  }
  const modelText = mode === "deepseek"
    ? "deepseek-chat"
    : mode === "reasoner"
      ? "deepseek-reasoner"
      : localEmbeddingModelDisplay;
  document.getElementById("rag-model").textContent = `当前模型：${modelText}`;
}

async function loadRagConfig() {
  const cfg = await apiGet("/api/rag/config");
  const localModel = String(cfg.embedding_model_display || cfg.embedding_model || "").trim();
  localEmbeddingModelDisplay = localModel || "本地模型";
  if (currentMode === "local") setMode("local");
}

function escapeHtml(text) {
  return text
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

function stripZeroWidth(text) {
  return String(text || "").replace(/[\u200B-\u200D\uFEFF]/g, "");
}

function normalizeListWhitespace(text) {
  // Normalize uncommon unicode spaces so list regex can match reliably.
  return String(text || "")
    .replace(/[\u00A0\u1680\u2000-\u200A\u202F\u205F\u3000]/g, " ")
    .replace(/\r/g, "");
}

function normalizeMarkdown(source) {
  if (!source) return "";
  let value = stripZeroWidth(String(source));
  value = value.replace(/([^\n])\s+(#{1,6}\s+)/g, "$1\n\n$2");
  value = value.replace(/([^\n])\s+(-{3,}\s*(?:\n|$))/g, "$1\n\n$2");
  return value;
}

function markdownToHtml(text) {
  if (window.CoreMarkdown?.render) {
    return window.CoreMarkdown.render(text);
  }
  return `<p>${escapeHtml(String(text || ""))}</p>`;
}

function streamTextToHtml(text) {
  // Lightweight renderer for in-flight chunks; final pass still uses markdownToHtml.
  return escapeHtml(String(text || "")).replace(/\n/g, "<br>");
}

function markdownPreviewForStreaming(text) {
  if (window.CoreMarkdown?.previewForStreaming) {
    return window.CoreMarkdown.previewForStreaming(text);
  }
  return markdownToHtml(text);
}

function normalizeThinkText(text) {
  let value = stripZeroWidth(String(text || "")).replace(/\r\n/g, "\n");
  value = value.replace(/\[\s*Empty\s+Line\s*\]/gi, "\n");
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

function stripThinkForPreview(text) {
  // For streaming preview: extract and track think blocks separately
  let value = String(text || "");
  const thinkBlocks = [];
  
  // Extract all completed think blocks
  value = value.replace(/<think>([\s\S]*?)<\/think>/gi, (_m, inner) => {
    const cleaned = normalizeThinkText(inner);
    if (cleaned) thinkBlocks.push(cleaned);
    return "\n";
  });
  
  // Handle unfinished think block in streaming state
  const openIdx = value.toLowerCase().lastIndexOf("<think>");
  const closeIdx = value.toLowerCase().lastIndexOf("</think>");
  if (openIdx !== -1 && openIdx > closeIdx) {
    const beforeThink = value.slice(0, openIdx);
    const inProgressThink = value.slice(openIdx + 7); // skip "<think>"
    if (inProgressThink.trim()) {
      thinkBlocks.push(normalizeThinkText(inProgressThink));
    }
    value = beforeThink;
  }
  
  return {
    answer: normalizeThinkText(value),
    thoughts: thinkBlocks,
  };
}

function insertSystemRowsBefore(targetRow, blocks) {
  if (!targetRow || !targetRow.parentElement || !Array.isArray(blocks)) return;
  const chat = targetRow.parentElement;
  
  // Track existing think blocks to avoid duplicates
  const existingThinks = [];
  let sibling = targetRow.previousSibling;
  while (sibling && sibling.classList && sibling.classList.contains("think")) {
    existingThinks.unshift(sibling);
    sibling = sibling.previousSibling;
  }
  
  // Update or create think blocks
  for (let i = 0; i < blocks.length; i++) {
    const text = normalizeThinkText(blocks[i]);
    if (!text) continue;
    
    if (existingThinks[i]) {
      // Update existing think block content
      const content = existingThinks[i].querySelector(".content");
      if (content) {
        content.innerHTML = markdownToHtml(text);
      }
    } else {
      // Create new think block
      const row = document.createElement("div");
      row.className = "msg system think";
      row.innerHTML = `<div class="role">系统</div><div class="content">${markdownToHtml(text)}</div>`;
      row.addEventListener("click", () => {
        row.classList.toggle("collapsed");
      });
      chat.insertBefore(row, targetRow);
    }
  }
  
  // Remove excess old think blocks if new count is less
  for (let i = blocks.length; i < existingThinks.length; i++) {
    existingThinks[i].remove();
  }
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

function formatElapsed(ms) {
  const total = Math.max(0, Math.floor(ms / 1000));
  const min = Math.floor(total / 60);
  const sec = total % 60;
  return `${String(min).padStart(2, "0")}:${String(sec).padStart(2, "0")}`;
}

function showOverlay(text) {
  const overlay = document.getElementById("loading-overlay");
  const textEl = document.getElementById("loading-text");
  const timerEl = document.getElementById("loading-timer");
  overlayStartedAt = Date.now();
  textEl.textContent = text || "正在处理...";
  timerEl.textContent = "00:00";
  overlay.classList.remove("hidden");

  if (overlayTimer) clearInterval(overlayTimer);
  overlayTimer = setInterval(() => {
    timerEl.textContent = formatElapsed(Date.now() - overlayStartedAt);
  }, 250);
}

function hideOverlay() {
  if (overlayTimer) {
    clearInterval(overlayTimer);
    overlayTimer = null;
  }
  const overlay = document.getElementById("loading-overlay");
  overlay.classList.add("hidden");
}

function formatTraceMeta(traceId) {
  const value = String(traceId || "").trim();
  return value ? `\`Trace ID\`: ${value}` : "";
}

function appendChatRow(role, text, keepBottom = true, extraClass = "", metaText = "") {
  const chat = document.getElementById("chat");
  const row = document.createElement("div");
  row.className = `msg ${role}${extraClass ? ` ${extraClass}` : ""}`;
  const roleLabel = role === "user" ? "用户" : role === "assistant" ? "助手" : "系统";
  const metaHtml = metaText ? `<div class="msg-meta">${escapeHtml(metaText)}</div>` : "";
  row.innerHTML = `<div class="role">${roleLabel}</div>${metaHtml}<div class="content">${markdownToHtml(text)}</div>`;
  chat.appendChild(row);
  if (keepBottom) chat.scrollTop = chat.scrollHeight;
  return row;
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
      await apiPost("/api/rag/feedback", payload);
      button.textContent = "已反馈";
      button.classList.add("is-done");
    } catch (err) {
      button.disabled = false;
      window.alert(`反馈保存失败: ${String(err)}`);
    }
  });
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

function addRagFeedbackForRow(row, payload = {}) {
  const question = String(payload.question || getPreviousUserQuestion(row) || "").trim();
  const answer = String(payload.answer || row?.querySelector(".content")?.textContent || "").trim();
  const traceId = String(payload.traceId || "").trim();
  if (!answer || !traceId) return;
  addFeedbackButton(row, {
    question,
    answer,
    trace_id: traceId,
    session_id: String(currentSessionId || "").trim(),
    model: String(payload.model || currentMode || "local"),
    search_mode: String(payload.searchMode || ""),
    metadata: {
      mode: String(payload.mode || currentMode || "local"),
      debug_enabled: !!payload.debugEnabled,
    },
  });
}

function appendChat(role, text, keepBottom = true) {
  appendChatRow(role, text, keepBottom);
}

function renderChat(messages) {
  const chat = document.getElementById("chat");
  chat.innerHTML = "";
  for (const message of messages || []) {
    const roleRaw = String(message.role || "").trim();
    const role = roleRaw === "用户" ? "user" : roleRaw === "助手" ? "assistant" : "system";
    const text = String(message.text || "");
    if (role === "assistant") {
      const metaText = formatTraceMeta(message.trace_id);
      const parsed = splitThinkBlocks(text);
      const assistantRow = appendChatRow("assistant", parsed.answer || text, false);
      insertSystemRowsBefore(assistantRow, parsed.thoughts);
      upsertTraceMetaRowBefore(assistantRow, metaText);
      addRagFeedbackForRow(assistantRow, { answer: parsed.answer || text, traceId: message.trace_id });
      continue;
    }
    appendChat(role, text, false);
  }
  chat.scrollTop = chat.scrollHeight;
}

function getSessionById(sessionId) {
  return sessionsCache.find((session) => String(session.id || "") === String(sessionId || "").trim()) || null;
}

function ensureRagSessionCache(sessionId) {
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

function appendMessageToRagSessionCache(sessionId, role, text, traceId = "") {
  const session = ensureRagSessionCache(sessionId);
  if (!session) return;
  const message = { role, text };
  const normalizedTrace = String(traceId || "").trim();
  if (normalizedTrace) message.trace_id = normalizedTrace;
  session.messages.push(message);
  session.updated_at = new Date().toISOString().slice(0, 19);
}

function renderActiveRagStreamPreview() {
  if (!activeRagStreamState || String(activeRagStreamState.sessionId || "") !== String(currentSessionId || "")) return;
  const progressText = String(activeRagStreamState.progressText || "正在检索相关文档...").trim();
  appendChatRow("system", progressText, false, "processing");
  const answerText = String(activeRagStreamState.streamedText || "");
  if (answerText.trim()) {
    const parsed = stripThinkForPreview(answerText);
    const assistantRow = appendChatRow("assistant", parsed.answer || answerText, false);
    insertSystemRowsBefore(assistantRow, parsed.thoughts);
    upsertTraceMetaRowBefore(assistantRow, formatTraceMeta(activeRagStreamState.traceId));
  }
}

function renderCurrentRagSessionView() {
  renderChat(getSessionById(currentSessionId)?.messages || []);
  renderActiveRagStreamPreview();
}

function renderRagSessionLoadingState(message = "正在加载历史会话...") {
  renderChat([]);
  appendChatRow("system", message, false, "processing");
}

function renderSessions() {
  const ul = document.getElementById("rag-session-list");
  ul.innerHTML = "";
  for (const session of sessionsCache) {
    const li = document.createElement("li");
    li.dataset.sessionId = String(session.id || "");
    li.title = String(session.title || "新会话");
    if (session.id === currentSessionId) li.classList.add("active");
    li.innerHTML = `<div class="title">${escapeHtml(session.title || "新会话")}</div><div class="meta">${escapeHtml(session.updated_at || "")}</div>`;
    li.onclick = async () => {
      if (Date.now() < suppressSessionClickUntil) return;
      const selectedSessionId = String(session.id || "").trim();
      currentSessionId = selectedSessionId;
      renderSessions();
      if (session.messages) {
        renderCurrentRagSessionView();
      } else {
        renderRagSessionLoadingState();
        try {
          const full = await apiGet(`/api/rag/sessions/${encodeURIComponent(selectedSessionId)}`);
          const idx = sessionsCache.findIndex((s) => s.id === selectedSessionId);
          if (idx >= 0) sessionsCache[idx] = { ...sessionsCache[idx], messages: full.messages || [] };
          if (currentSessionId === selectedSessionId) renderCurrentRagSessionView();
        } catch (_e) {
          if (currentSessionId === selectedSessionId) renderCurrentRagSessionView();
        }
      }
    };
    ul.appendChild(li);
  }
}

function getSessionSummary(sessionId) {
  return sessionsCache.find((session) => String(session.id || "") === String(sessionId || "").trim()) || null;
}

function closeSessionRenameModal() {
  currentRenameSessionId = "";
  const modal = sessionRenameModal();
  if (!modal) return;
  modal.classList.add("hidden");
  modal.setAttribute("aria-hidden", "true");
}

function openSessionRenameModal(sessionId) {
  const session = getSessionSummary(sessionId);
  const modal = sessionRenameModal();
  const input = sessionRenameInput();
  if (!session || !modal || !input) return;
  currentRenameSessionId = String(session.id || "").trim();
  input.value = String(session.title || "新会话");
  const meta = sessionRenameMeta();
  if (meta) meta.textContent = `会话 ID: ${currentRenameSessionId}`;
  modal.classList.remove("hidden");
  modal.setAttribute("aria-hidden", "false");
  const shouldAutoFocus = !!(window.matchMedia && window.matchMedia("(pointer: fine)").matches);
  if (shouldAutoFocus) {
    window.setTimeout(() => {
      input.focus();
      input.select();
    }, 20);
  }
}

function openSessionRenameModalFromTarget(target) {
  const item = target instanceof Element ? target.closest("[data-session-id]") : null;
  const sessionId = String(item?.getAttribute("data-session-id") || "").trim();
  if (!sessionId) return;
  suppressSessionClickUntil = Date.now() + 650;
  openSessionRenameModal(sessionId);
}

async function saveSessionRename() {
  const sessionId = String(currentRenameSessionId || "").trim();
  const input = sessionRenameInput();
  const title = String(input?.value || "").trim();
  if (!sessionId || !title) return;
  await apiPost(`/api/rag/sessions/${encodeURIComponent(sessionId)}`, { title, lock: true }, "PATCH");
  closeSessionRenameModal();
  await refreshSessions(true);
}

async function refreshSessions(selectNewest = false) {
  const data = await apiGet("/api/rag/sessions");
  const newSummaries = Array.isArray(data.sessions) ? data.sessions : [];

  if (!newSummaries.length) {
    const created = await apiPost("/api/rag/sessions", { title: "新会话" });
    sessionsCache = [created];
    currentSessionId = created.id;
    renderSessions();
    renderChat(created.messages || []);
    return;
  }

  if (selectNewest || !currentSessionId || !newSummaries.some((s) => s.id === currentSessionId)) {
    currentSessionId = newSummaries[0].id;
  }

  // Merge cached messages from previous sessionsCache into the new summaries
  sessionsCache = newSummaries.map((s) => {
    const old = sessionsCache.find((o) => o.id === s.id);
    return (old && old.messages) ? { ...s, messages: old.messages } : s;
  });

  renderSessions();

  const session = sessionsCache.find((s) => s.id === currentSessionId);
  if (session?.messages) {
    renderCurrentRagSessionView();
  } else {
    try {
      const full = await apiGet(`/api/rag/sessions/${encodeURIComponent(currentSessionId)}`);
      const idx = sessionsCache.findIndex((s) => s.id === currentSessionId);
      if (idx >= 0) sessionsCache[idx] = { ...sessionsCache[idx], messages: full.messages || [] };
      renderCurrentRagSessionView();
    } catch (_e) {
      renderCurrentRagSessionView();
    }
  }
}

// Lightweight sidebar-only refresh: update titles/timestamps without touching the chat panel.
// Use this after ask/stream completes so we don't clobber chat with a server re-fetch.
async function refreshSidebarTitles() {
  try {
    const data = await apiGet("/api/rag/sessions");
    const newSummaries = Array.isArray(data.sessions) ? data.sessions : [];
    if (!newSummaries.length) return;
    // If current session was deleted externally, fall back to newest.
    if (currentSessionId && !newSummaries.some((s) => s.id === currentSessionId)) {
      currentSessionId = newSummaries[0].id;
    }
    // Merge cached messages so clicking a session still has them available.
    sessionsCache = newSummaries.map((s) => {
      const old = sessionsCache.find((o) => o.id === s.id);
      return (old && old.messages) ? { ...s, messages: old.messages } : s;
    });
    renderSessions();
  } catch (_e) {
    // Sidebar refresh is best-effort; don't propagate into post-ask flow.
  }
}

async function createSession() {
  const created = await apiPost("/api/rag/sessions", { title: "新会话" });
  currentSessionId = created.id;
  await refreshSessions(true);
}

async function deleteCurrentSession() {
  if (!currentSessionId) return;
  try {
    await fetch(`/api/rag/sessions/${encodeURIComponent(currentSessionId)}`, { method: "DELETE" });
  } catch (_err) {
    // Ignore 404 or network race; refresh next.
  }
  currentSessionId = "";
  await refreshSessions(true);
}

function wireChatLinks() {
  const chat = document.getElementById("chat");
  chat.addEventListener("click", async (event) => {
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
      await openDoc(path);
      setTab("preview");
      return;
    }

    // Fallback enforcement for external links in case target attr is missing.
    if (/^https?:\/\//i.test(href)) {
      event.preventDefault();
      window.open(href, "_blank", "noopener,noreferrer");
    }
  });
}

function wireDocLinks() {
  const doc = document.getElementById("doc");
  doc.addEventListener("click", (event) => {
    const target = event.target;
    const anchor = target instanceof Element ? target.closest("a") : null;
    if (!(anchor instanceof HTMLAnchorElement)) return;
    const href = anchor.getAttribute("href") || "";
    if (href.startsWith("doc://")) {
      const path = decodeURIComponent(href.slice("doc://".length));
      if (/^https?:\/\//i.test(path)) {
        event.preventDefault();
        window.open(path, "_blank", "noopener,noreferrer");
        return;
      }
    }
    if (/^https?:\/\//i.test(href)) {
      event.preventDefault();
      window.open(href, "_blank", "noopener,noreferrer");
    }
  });
}

function renderTreeNodes(ul, nodes) {
  ul.innerHTML = "";
  for (const node of nodes) {
    const li = document.createElement("li");
    const isDir = node.type === "dir";
    const row = document.createElement("div");
    row.className = `tree-row ${isDir ? "dir" : "file"}`;
    const caret = document.createElement("span");
    caret.className = "tree-caret";

    // expandable = dir that either has loaded children or is known to have children
    const expandable = isDir && (node.has_children || Array.isArray(node.children));
    // expanded = children array is loaded AND dir is not manually collapsed
    const isExpanded = isDir && Array.isArray(node.children) && !collapsedDirs.has(node.path);

    caret.textContent = !expandable ? "·" : (isExpanded ? "▾" : "▸");
    const label = document.createElement("span");
    label.className = "tree-label";
    label.textContent = node.name;
    label.title = node.path || node.name;
    row.appendChild(caret);
    row.appendChild(label);
    li.appendChild(row);

    if (isDir && expandable) {
      row.onclick = async () => {
        if (Array.isArray(node.children)) {
          // Children already loaded — just toggle collapse
          if (collapsedDirs.has(node.path)) collapsedDirs.delete(node.path);
          else collapsedDirs.add(node.path);
          renderPreviewTree();
        } else {
          // Lazy-load children on first expand
          row.style.opacity = "0.5";
          try {
            const data = await apiGet(`/api/preview/tree?path=${encodeURIComponent(node.path)}`);
            node.children = data.children || [];
            node.has_children = node.children.length > 0;
          } catch (_e) {
            node.children = [];
          } finally {
            row.style.opacity = "";
          }
          collapsedDirs.delete(node.path); // expand after load
          renderPreviewTree();
        }
      };
    } else if (!isDir) {
      row.onclick = async () => openDoc(node.path);
    }

    ul.appendChild(li);
    if (isDir && isExpanded) {
      const child = document.createElement("ul");
      child.className = "tree nested";
      li.appendChild(child);
      renderTreeNodes(child, node.children);
    }
  }
}

function renderPreviewListTitle() {
  const title = document.getElementById("preview-list-title");
  title.textContent = previewMode === "tree" ? "目录" : "搜索结果";
}

function renderPreviewTree() {
  previewMode = "tree";
  renderPreviewListTitle();
  renderTreeNodes(document.getElementById("preview-list"), previewTreeNodes);
}

function renderSearchResults(rows, isVector = false) {
  previewMode = "results";
  renderPreviewListTitle();
  const ul = document.getElementById("preview-list");
  ul.innerHTML = "";
  for (const row of rows || []) {
    const li = document.createElement("li");
    const item = document.createElement("div");
    item.className = "tree-row file";
    item.title = row.path || "";
    const dot = document.createElement("span");
    dot.className = "tree-caret";
    dot.textContent = "•";
    const label = document.createElement("span");
    label.className = "tree-label";
    label.textContent = isVector
      ? `${Number(row.score || 0).toFixed(4)} | ${row.path}`
      : String(row.path || "");
    item.appendChild(dot);
    item.appendChild(label);
    item.onclick = async () => openDoc(String(row.path || ""));
    li.appendChild(item);
    ul.appendChild(li);
  }
}

async function openDoc(path) {
  const data = await apiGet(`/api/preview/file?path=${encodeURIComponent(path)}`);
  document.getElementById("doc").innerHTML = markdownToHtml(data.markdown || "");
}

async function loadTree() {
  const data = await apiGet("/api/preview/tree");
  previewTreeNodes = data.children || [];
  collapsedDirs.clear();
  // No pre-population of collapsedDirs: dirs without loaded children are
  // implicitly closed (has_children=true but no .children array yet).
  renderPreviewTree();
}

async function runKeyword() {
  const q = document.getElementById("kw").value.trim();
  if (!q) return;
  const data = await apiGet(`/api/preview/search/keyword?q=${encodeURIComponent(q)}`);
  renderSearchResults(data.results, false);
}

async function runVector() {
  const q = document.getElementById("kw").value.trim();
  if (!q) return;
  const btnKw = document.getElementById("btn-kw");
  const btnVs = document.getElementById("btn-vs");
  const btnRefresh = document.getElementById("btn-refresh");
  btnKw.disabled = true;
  btnVs.disabled = true;
  btnRefresh.disabled = true;
  showOverlay("正在向量检索（首次可能较慢）...");
  try {
    const data = await apiGet(`/api/preview/search/vector?q=${encodeURIComponent(q)}`);
    renderSearchResults(data.results, true);
  } finally {
    hideOverlay();
    btnKw.disabled = false;
    btnVs.disabled = false;
    btnRefresh.disabled = false;
  }
}

function resetPreviewList() {
  document.getElementById("kw").value = "";
  renderPreviewTree();
}

async function ask(searchMode = "hybrid", forcedMode = null, confirmOverQuota = false) {
  if (askInFlight) return;
  const questionInput = document.getElementById("question");
  const question = questionInput.value.trim();
  if (!question) return;
  const requestSessionId = String(currentSessionId || "").trim();
  const isViewingRequestSession = () => String(currentSessionId || "").trim() === requestSessionId;

  questionInput.value = "";
  appendChat("user", question);
  appendMessageToRagSessionCache(requestSessionId, "用户", question);
  const pending = appendChatRow("system", "正在检索相关文档... `00:00`", true, "processing");
  const pendingContent = pending.querySelector(".content");
  let progressText = "正在检索相关文档...";
  let assistantRow = null;
  let assistantContent = null;
  let streamedText = "";
  let streamFinalized = false;
  let lastRenderedText = "";
  let lastRenderAt = 0;
  let quotaExceededEvent = null;
  let activeTraceId = "";
  let finalPayload = null;
  activeRagStreamState = {
    sessionId: requestSessionId,
    progressText: "正在检索相关文档...",
    streamedText: "",
    traceId: "",
  };
  const removePending = () => {
    try {
      pending.remove();
    } catch (_err) {
      if (pendingContent) pendingContent.innerHTML = "";
      pending.classList.remove("processing");
    }
  };
  const pendingStartedAt = Date.now();
  const pendingTimer = setInterval(() => {
    if (!pendingContent) return;
    pendingContent.innerHTML = markdownToHtml(`${progressText} \`${formatElapsed(Date.now() - pendingStartedAt)}\``);
  }, 250);
  const streamRenderTimer = setInterval(() => {
    if (streamFinalized || !streamedText) return;
    if (!isViewingRequestSession()) return;
    if (!assistantRow) {
      assistantRow = appendChatRow("assistant", "", true);
      assistantContent = assistantRow.querySelector(".content");
    }
    if (!assistantContent) return;
    if (streamedText !== lastRenderedText) {
      const parsed = stripThinkForPreview(streamedText);
      insertSystemRowsBefore(assistantRow, parsed.thoughts);
      assistantContent.innerHTML = markdownPreviewForStreaming(parsed.answer);
      lastRenderedText = streamedText;
      lastRenderAt = Date.now();
    }
  }, 300);

  askInFlight = true;
  document.getElementById("ask").disabled = true;
  const askLocalBtn = document.getElementById("ask-local");
  if (askLocalBtn) askLocalBtn.disabled = true;
  document.getElementById("abort").disabled = false;

  try {
    const debugToggle = document.getElementById("rag-debug-toggle");
    const debugEnabled = !!(debugToggle && debugToggle.checked);
    const payload = { 
      question, 
      session_id: requestSessionId,
      mode: String(forcedMode || currentMode || "local"),
      search_mode: searchMode,
      top_k: 5,
      similarity_threshold: 0.4,  // Minimum similarity score to include documents
      debug: debugEnabled,
      confirm_over_quota: confirmOverQuota,
    };
    const response = await fetch("/api/rag/ask_stream", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    if (!response.ok || !response.body) {
      const answer = await apiPost("/api/rag/ask", payload);
      if (isViewingRequestSession()) {
        currentSessionId = answer.session_id || currentSessionId;
      }
      if (answer.aborted) {
        if (isViewingRequestSession()) {
          if (pendingContent) pendingContent.innerHTML = markdownToHtml("已中止");
          pending.classList.remove("processing");
        }
      } else {
        if (isViewingRequestSession()) pending.remove();
        const parsed = splitThinkBlocks(answer.answer || "");
        if (isViewingRequestSession()) {
          const assistantRow = appendChatRow("assistant", parsed.answer || answer.answer || "", true);
          insertSystemRowsBefore(assistantRow, parsed.thoughts);
          upsertTraceMetaRowBefore(assistantRow, formatTraceMeta(answer.trace_id));
          addRagFeedbackForRow(assistantRow, {
            question,
            answer: parsed.answer || answer.answer || "",
            traceId: answer.trace_id,
            searchMode,
            mode: answer.mode,
            debugEnabled,
          });
        }
      }
      await refreshSidebarTitles();
      return;
    }

    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";

    const processSseBlock = (part) => {
      const lines = part.split(/\r?\n/);
      for (const raw of lines) {
        const line = String(raw || "").trimStart();
        if (!line.startsWith("data:")) continue;
        const jsonText = line.slice(5).trim();
        if (!jsonText) continue;
        let event;
        try {
          event = JSON.parse(jsonText);
        } catch (_err) {
          continue;
        }

        if (event.type === "quota_exceeded") {
          if (event.trace_id) activeTraceId = String(event.trace_id || "").trim();
          streamFinalized = true;
          quotaExceededEvent = event;
          if (activeRagStreamState && activeRagStreamState.sessionId === requestSessionId) {
            activeRagStreamState.traceId = activeTraceId;
            activeRagStreamState.progressText = String(event.message || "API 配额已满，等待处理");
          }
          if (isViewingRequestSession()) pending.classList.remove("processing");
        } else if (event.type === "progress") {
          if (event.trace_id) activeTraceId = String(event.trace_id || "").trim();
          progressText = String(event.message || progressText);
          if (activeRagStreamState && activeRagStreamState.sessionId === requestSessionId) {
            activeRagStreamState.progressText = progressText;
            activeRagStreamState.traceId = activeTraceId;
          }
        } else if (event.type === "chunk") {
          if (event.trace_id) activeTraceId = String(event.trace_id || "").trim();
          const chunk = String(event.text || "");
          if (isViewingRequestSession() && !assistantRow) {
            assistantRow = appendChatRow("assistant", "", true);
            assistantContent = assistantRow.querySelector(".content");
          }
          if (chunk) {
            streamedText += chunk;
            if (activeRagStreamState && activeRagStreamState.sessionId === requestSessionId) {
              activeRagStreamState.streamedText = streamedText;
              activeRagStreamState.traceId = activeTraceId;
            }
            // Fast path: update immediately if enough time elapsed since last paint.
            const now = Date.now();
            if (isViewingRequestSession() && assistantContent && now - lastRenderAt >= 180) {
              const parsed = stripThinkForPreview(streamedText);
              insertSystemRowsBefore(assistantRow, parsed.thoughts);
              upsertTraceMetaRowBefore(assistantRow, formatTraceMeta(activeTraceId));
              assistantContent.innerHTML = markdownPreviewForStreaming(parsed.answer);
              lastRenderedText = streamedText;
              lastRenderAt = now;
            }
          }
        } else if (event.type === "aborted") {
          if (event.trace_id) activeTraceId = String(event.trace_id || "").trim();
          streamFinalized = true;
          if (activeRagStreamState && activeRagStreamState.sessionId === requestSessionId) {
            activeRagStreamState.traceId = activeTraceId;
          }
          if (isViewingRequestSession()) {
            if (pendingContent) pendingContent.innerHTML = markdownToHtml("已中止");
            pending.classList.remove("processing");
          }
          // Preserve any streamed content
          if (isViewingRequestSession() && streamedText && assistantContent) {
            const parsed = stripThinkForPreview(streamedText);
            insertSystemRowsBefore(assistantRow, parsed.thoughts);
            upsertTraceMetaRowBefore(assistantRow, formatTraceMeta(activeTraceId));
            assistantContent.innerHTML = markdownToHtml(parsed.answer + "\n\n_[已中止]_");
          }
        } else if (event.type === "error") {
          if (event.trace_id) activeTraceId = String(event.trace_id || "").trim();
          streamFinalized = true;
          if (activeRagStreamState && activeRagStreamState.sessionId === requestSessionId) {
            activeRagStreamState.traceId = activeTraceId;
          }
          const errorMsg = event.message || "请求失败";
          if (isViewingRequestSession()) {
            if (pendingContent) pendingContent.innerHTML = markdownToHtml(`[错误] ${errorMsg}`);
            pending.classList.remove("processing");
          }
          // Preserve any streamed content before error
          if (isViewingRequestSession() && streamedText && assistantContent) {
            const parsed = stripThinkForPreview(streamedText);
            insertSystemRowsBefore(assistantRow, parsed.thoughts);
            upsertTraceMetaRowBefore(assistantRow, formatTraceMeta(activeTraceId));
            assistantContent.innerHTML = markdownToHtml(parsed.answer + `\n\n---\n\n**错误**: ${errorMsg}`);
          } else if (isViewingRequestSession() && !assistantRow) {
            // No content yet, just show error
            assistantRow = appendChatRow("assistant", `**错误**: ${errorMsg}`, true);
            upsertTraceMetaRowBefore(assistantRow, formatTraceMeta(activeTraceId));
          }
        } else if (event.type === "done") {
          if (event.trace_id) activeTraceId = String(event.trace_id || "").trim();
          streamFinalized = true;
          if (isViewingRequestSession()) removePending();
          finalPayload = event.payload || null;
          const answer = String(event.payload?.answer || streamedText || "");
          const resolvedSessionId = String(event.payload?.session_id || requestSessionId || "").trim();
          if (isViewingRequestSession()) currentSessionId = resolvedSessionId;
          appendMessageToRagSessionCache(resolvedSessionId, "助手", answer, activeTraceId || event.payload?.trace_id || "");
          activeRagStreamState = null;
          if (isViewingRequestSession() && !assistantRow) {
            assistantRow = appendChatRow("assistant", "", true);
            assistantContent = assistantRow.querySelector(".content");
          }
          const parsed = splitThinkBlocks(answer);
          if (isViewingRequestSession()) insertSystemRowsBefore(assistantRow, parsed.thoughts);
          if (isViewingRequestSession()) upsertTraceMetaRowBefore(assistantRow, formatTraceMeta(activeTraceId || event.payload?.trace_id));
          if (isViewingRequestSession() && assistantContent) {
            const fallback = stripThinkForPreview(answer);
            assistantContent.innerHTML = markdownToHtml(parsed.answer || fallback.answer || "");
          }
          if (isViewingRequestSession()) {
            addRagFeedbackForRow(assistantRow, {
              question,
              answer: parsed.answer || answer,
              traceId: activeTraceId || event.payload?.trace_id,
              searchMode,
              mode: event.payload?.mode,
              debugEnabled,
            });
          }
          if (currentSessionId === resolvedSessionId) renderCurrentRagSessionView();
        }
      }
    };

    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const parts = buffer.split(/\r?\n\r?\n/);
      buffer = parts.pop() || "";

      for (const part of parts) processSseBlock(part);
    }

    // Process a trailing block if server closed without a final blank separator.
    if (buffer.trim()) processSseBlock(buffer);

    // Safeguard: if stream ended without a terminal event (done/error/aborted),
    // finalize the UI so the user isn't stuck with a spinning indicator.
    if (!streamFinalized && !quotaExceededEvent) {
      streamFinalized = true;
      if (isViewingRequestSession()) removePending();
      if (isViewingRequestSession() && streamedText && assistantContent) {
        const parsed = stripThinkForPreview(streamedText);
        insertSystemRowsBefore(assistantRow, parsed.thoughts);
        upsertTraceMetaRowBefore(assistantRow, formatTraceMeta(activeTraceId || finalPayload?.trace_id));
        assistantContent.innerHTML = markdownToHtml(parsed.answer);
      } else if (isViewingRequestSession() && streamedText && !assistantRow) {
        const parsed = stripThinkForPreview(streamedText);
        assistantRow = appendChatRow("assistant", parsed.answer, true);
        upsertTraceMetaRowBefore(assistantRow, formatTraceMeta(activeTraceId || finalPayload?.trace_id));
        assistantContent = assistantRow.querySelector(".content");
      } else if (isViewingRequestSession() && !assistantRow) {
        // No content was received at all
        appendChatRow("assistant", "**\u9519\u8BEF**: \u670D\u52A1\u5668\u8FDE\u63A5\u5F02\u5E38\u4E2D\u65AD\uFF0C\u8BF7\u91CD\u8BD5", true);
      }
    }

    if (quotaExceededEvent) {
      // Show inline quota warning with action buttons
      const msg = quotaExceededEvent.message || "API 配额已满，是否继续？";
      if (pendingContent) {
        pendingContent.innerHTML =
          `<span class="quota-warn-msg">${msg}</span>` +
          `<button class="quota-btn quota-btn-confirm" style="margin-left:8px;padding:2px 10px;cursor:pointer">继续调用</button>` +
          `<button class="quota-btn quota-btn-degrade" style="margin-left:6px;padding:2px 10px;cursor:pointer">切换本地</button>`;
        const confirmBtn = pendingContent.querySelector(".quota-btn-confirm");
        const degradeBtn = pendingContent.querySelector(".quota-btn-degrade");
        if (confirmBtn) {
          confirmBtn.addEventListener("click", () => {
            pending.remove();
            questionInput.value = question;
            ask(searchMode, forcedMode, true);
          });
        }
        if (degradeBtn) {
          degradeBtn.addEventListener("click", () => {
            pending.remove();
            questionInput.value = question;
            ask("local_only", "local", false);
          });
        }
      }
      await refreshSidebarTitles();
      return;
    }

    await refreshSidebarTitles();
  } catch (err) {
    streamFinalized = true;
    const errorMsg = err.message || "请求失败";
      if (isViewingRequestSession()) removePending();
    // Preserve any streamed content before network/parsing error
    if (isViewingRequestSession() && streamedText && assistantContent) {
      const parsed = stripThinkForPreview(streamedText);
      insertSystemRowsBefore(assistantRow, parsed.thoughts);
      assistantContent.innerHTML = markdownToHtml(parsed.answer + `\n\n---\n\n**错误**: ${errorMsg}`);
    } else if (isViewingRequestSession() && !assistantRow) {
      appendChatRow("assistant", `**错误**: ${errorMsg}`, true);
    }
    appendMessageToRagSessionCache(requestSessionId, "助手", `**错误**: ${errorMsg}`);
    await refreshSidebarTitles();
  } finally {
    clearInterval(pendingTimer);
    clearInterval(streamRenderTimer);
    if (activeRagStreamState && activeRagStreamState.sessionId === requestSessionId) {
      activeRagStreamState = null;
    }
    if (!quotaExceededEvent) {
      removePending();
      document.querySelectorAll("#chat .msg.processing").forEach((node) => {
        if (node instanceof HTMLElement) node.remove();
      });
    }
    askInFlight = false;
    document.getElementById("ask").disabled = false;
    const askLocalBtn = document.getElementById("ask-local");
    if (askLocalBtn) askLocalBtn.disabled = false;
    document.getElementById("abort").disabled = true;
  }
}

async function abortAsk() {
  if (!askInFlight || !currentSessionId) return;
  try {
    await apiPost("/api/rag/abort", { session_id: currentSessionId });
  } catch (_e) {
    // Keep UI responsive even if abort API fails.
  }
}

function setWorkflowStatus(text) {
  const statusEl = document.getElementById("wf-job-status");
  if (statusEl) statusEl.textContent = String(text || "");
}

function appendWorkflowLog(lines) {
  const logEl = document.getElementById("wf-log");
  if (!logEl || !Array.isArray(lines)) return;
  if (workflowLogRenderedLines === 0) {
    logEl.textContent = "";
  }
  const incoming = lines.slice(workflowLogRenderedLines);
  if (incoming.length) {
    const prefix = logEl.textContent ? "\n" : "";
    logEl.textContent += prefix + incoming.join("\n");
    workflowLogRenderedLines = lines.length;
    logEl.scrollTop = logEl.scrollHeight;
  }
}

function setWorkflowLastClassify(text) {
  const el = document.getElementById("wf-last-classify");
  if (!el) return;
  const value = String(text || "").trim();
  el.textContent = value ? `上次分类归档: ${value}` : "上次分类归档: 未记录";
}

async function refreshWorkflowStats() {
  const startDate = String(document.getElementById("wf-start")?.value || "").trim();
  const endDate = String(document.getElementById("wf-end")?.value || "").trim();
  const data = await apiPost("/api/workflow/stats", {
    start_date: startDate,
    end_date: endDate,
  });
  setWorkflowLastClassify(data.last_classify_archive_at || "");
  const countEl = document.getElementById("wf-count");
  if (!countEl) return;
  if (data.error) {
    countEl.textContent = `提取文件: 日期无效 / 总计 ${Number(data.total || 0)}`;
  } else {
    countEl.textContent = `提取文件: ${Number(data.matched || 0)} / 总计 ${Number(data.total || 0)}`;
  }
}

async function loadWorkflowConfig() {
  const cfg = await apiGet("/api/workflow/config");
  const baseUrl = document.getElementById("wf-base-url");
  const model = document.getElementById("wf-model");
  const apiKey = document.getElementById("wf-api-key");
  const source = document.getElementById("wf-source");
  if (baseUrl) baseUrl.value = String(cfg.base_url || "");
  if (model) model.value = String(cfg.model || "");
  if (apiKey) {
    apiKey.value = "";
    const keyConfigured = String(cfg.api_key_configured || "0") === "1";
    const keySource = String(cfg.api_key_source || "");
    apiKey.placeholder = keyConfigured
      ? (keySource === "env" ? "留空则使用环境变量 DEEPSEEK_API_KEY" : "已保存密钥，留空优先使用环境变量")
      : "留空则使用环境变量 DEEPSEEK_API_KEY";
  }
  if (source) source.value = String(cfg.source || "deepseek");

  const end = new Date();
  const start = new Date(end.getTime() - 365 * 24 * 60 * 60 * 1000);
  const toDateText = (d) => `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
  const startEl = document.getElementById("wf-start");
  const endEl = document.getElementById("wf-end");
  if (startEl && !startEl.value) startEl.value = toDateText(start);
  if (endEl && !endEl.value) endEl.value = toDateText(end);
  await refreshWorkflowStats();
}

function initWorkflowDatePickers() {
  const isCoarsePointer = window.matchMedia && window.matchMedia("(pointer: coarse)").matches;
  if (isCoarsePointer) {
    return;
  }

  const tryShowPicker = (input) => {
    if (!input || typeof input.showPicker !== "function") {
      return;
    }
    try {
      input.showPicker();
    } catch (_err) {
      // Ignore browsers that block showPicker outside allowed activation timing.
    }
  };

  for (const id of ["wf-start", "wf-end"]) {
    const input = document.getElementById(id);
    if (!input || input.dataset.pickerBound === "1") {
      continue;
    }
    input.dataset.pickerBound = "1";
    input.addEventListener("pointerdown", () => {
      tryShowPicker(input);
    });
    input.addEventListener("click", () => {
      tryShowPicker(input);
    });
    input.addEventListener("focus", () => {
      window.setTimeout(() => tryShowPicker(input), 0);
    });
    input.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " " || event.key === "ArrowDown") {
        tryShowPicker(input);
      }
    });
  }
}

async function saveWorkflowConfig() {
  const payload = {
    base_url: String(document.getElementById("wf-base-url")?.value || "").trim(),
    model: String(document.getElementById("wf-model")?.value || "").trim(),
    api_key: String(document.getElementById("wf-api-key")?.value || "").trim(),
  };
  await apiPost("/api/workflow/config/save", payload);
  setWorkflowStatus("配置已保存");
}

async function uploadWorkflowFiles(fileList) {
  if (!fileList || !fileList.length) return;
  const files = [];
  for (const file of fileList) {
    const buf = await file.arrayBuffer();
    let binary = "";
    const bytes = new Uint8Array(buf);
    for (let i = 0; i < bytes.length; i += 1) {
      binary += String.fromCharCode(bytes[i]);
    }
    files.push({
      name: file.name,
      content_base64: btoa(binary),
    });
  }
  const resp = await fetch("/api/workflow/upload", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ files }),
  });
  if (!resp.ok) throw new Error(await resp.text());
  await resp.json();
  setWorkflowStatus("源文件上传完成");
}

function openWorkflowCreateModal() {
  const modal = document.getElementById("wf-create-modal");
  if (!modal) return;
  modal.classList.remove("hidden");
  modal.setAttribute("aria-hidden", "false");
  const cancelBtn = document.getElementById("wf-create-cancel");
  if (cancelBtn) {
    cancelBtn.focus({ preventScroll: true });
  }
}

function closeWorkflowCreateModal() {
  const modal = document.getElementById("wf-create-modal");
  if (!modal) return;
  modal.classList.add("hidden");
  modal.setAttribute("aria-hidden", "true");
}

function summarizeWorkflowResult(data) {
  const result = (data && data.result && typeof data.result === "object") ? data.result : {};
  const statusSummary = String(result.status_summary || "").trim();
  if (statusSummary) return statusSummary;

  const staleRemoved = Number(result.stale_removed || 0);
  const vectorsAdded = Number(result.vectors_added || 0);
  const graphNodesAdded = Number(result.graph_nodes_added || 0);
  const graphEdgesAdded = Number(result.graph_edges_added || 0);

  if (staleRemoved || vectorsAdded || graphNodesAdded || graphEdgesAdded) {
    return `执行完成 | stale清理: ${staleRemoved} | 向量新增: ${vectorsAdded} | graph新增: 节点${graphNodesAdded}/边${graphEdgesAdded}`;
  }
  return "执行完成";
}

async function saveWorkflowCreatedFile() {
  const fileName = String(document.getElementById("wf-create-name")?.value || "").trim();
  const content = String(document.getElementById("wf-create-content")?.value || "");
  if (!content.trim()) {
    setWorkflowStatus("保存失败: 正文不能为空");
    return;
  }
  const result = await apiPost("/api/workflow/create-file", {
    file_name: fileName,
    content,
  });
  setWorkflowStatus(`创建成功: ${String(result.file_name || "")}`);
  const nameInput = document.getElementById("wf-create-name");
  const contentInput = document.getElementById("wf-create-content");
  if (nameInput) nameInput.value = "";
  if (contentInput) contentInput.value = "";
  closeWorkflowCreateModal();
  await refreshWorkflowStats();
}

function pollWorkflowJob(jobId) {
  if (workflowPollingTimer) {
    clearInterval(workflowPollingTimer);
    workflowPollingTimer = null;
  }
  workflowPollingTimer = setInterval(async () => {
    try {
      const data = await apiGet(`/api/workflow/jobs/${encodeURIComponent(jobId)}`);
      appendWorkflowLog(Array.isArray(data.logs) ? data.logs : []);
      if (data.status === "running") {
        setWorkflowStatus("运行中");
        return;
      }
      if (data.status === "succeeded") {
        setWorkflowStatus(summarizeWorkflowResult(data));
      } else {
        setWorkflowStatus("执行失败");
      }
      clearInterval(workflowPollingTimer);
      workflowPollingTimer = null;
      await refreshWorkflowStats();
    } catch (err) {
      clearInterval(workflowPollingTimer);
      workflowPollingTimer = null;
      setWorkflowStatus(`轮询失败: ${String(err)}`);
    }
  }, 1200);
}

async function startWorkflowAction(action, extraPayload = {}) {
  if (workflowPollingTimer) {
    clearInterval(workflowPollingTimer);
    workflowPollingTimer = null;
  }
  workflowLogRenderedLines = 0;
  const logEl = document.getElementById("wf-log");
  if (logEl) logEl.textContent = "";

  const payload = {
    action,
    source: String(document.getElementById("wf-source")?.value || "deepseek"),
    start_date: String(document.getElementById("wf-start")?.value || ""),
    end_date: String(document.getElementById("wf-end")?.value || ""),
    base_url: String(document.getElementById("wf-base-url")?.value || "").trim(),
    model: String(document.getElementById("wf-model")?.value || "").trim(),
    api_key: String(document.getElementById("wf-api-key")?.value || "").trim(),
    ...extraPayload,
  };

  const started = await apiPost("/api/workflow/run", payload);
  workflowJobId = String(started.job_id || "");
  if (!workflowJobId) throw new Error("任务启动失败");
  setWorkflowStatus("已启动");
  refreshWorkflowStats().catch(() => {});
  pollWorkflowJob(workflowJobId);
}

function wireWorkflowTab() {
  const uploadBtn = document.getElementById("wf-upload");
  const uploadInput = document.getElementById("wf-upload-input");
  const toggleKeyBtn = document.getElementById("wf-toggle-key");
  const apiKeyInput = document.getElementById("wf-api-key");
  const createModal = document.getElementById("wf-create-modal");

  if (uploadBtn && uploadInput) {
    uploadBtn.addEventListener("click", () => uploadInput.click());
    uploadInput.addEventListener("change", async () => {
      try {
        await uploadWorkflowFiles(uploadInput.files);
        await refreshWorkflowStats();
      } catch (err) {
        setWorkflowStatus(`上传失败: ${String(err)}`);
      } finally {
        uploadInput.value = "";
      }
    });
  }

  document.getElementById("wf-create-file")?.addEventListener("click", () => {
    openWorkflowCreateModal();
  });
  document.getElementById("wf-create-cancel")?.addEventListener("click", () => {
    closeWorkflowCreateModal();
  });
  document.getElementById("wf-create-save")?.addEventListener("click", async () => {
    try {
      await saveWorkflowCreatedFile();
    } catch (err) {
      setWorkflowStatus(`创建失败: ${String(err)}`);
    }
  });
  if (createModal) {
    createModal.addEventListener("click", (e) => {
      const target = e.target;
      if (target && target.dataset && target.dataset.role === "wf-backdrop") {
        closeWorkflowCreateModal();
      }
    });
  }

  if (toggleKeyBtn && apiKeyInput) {
    toggleKeyBtn.addEventListener("click", () => {
      workflowApiKeyVisible = !workflowApiKeyVisible;
      apiKeyInput.type = workflowApiKeyVisible ? "text" : "password";
      toggleKeyBtn.textContent = workflowApiKeyVisible ? "隐藏" : "显示";
    });
  }

  document.getElementById("wf-save-config")?.addEventListener("click", async () => {
    try {
      await saveWorkflowConfig();
    } catch (err) {
      setWorkflowStatus(`保存失败: ${String(err)}`);
    }
  });
  document.getElementById("wf-refresh-count")?.addEventListener("click", () => {
    refreshWorkflowStats().catch((err) => setWorkflowStatus(`刷新失败: ${String(err)}`));
  });
  document.getElementById("wf-batch")?.addEventListener("click", () => {
    startWorkflowAction("batch_process").catch((err) => setWorkflowStatus(`启动失败: ${String(err)}`));
  });
  document.getElementById("wf-summary")?.addEventListener("click", () => {
    startWorkflowAction("ai_summary").catch((err) => setWorkflowStatus(`启动失败: ${String(err)}`));
  });
  document.getElementById("wf-split")?.addEventListener("click", () => {
    startWorkflowAction("split_topics").catch((err) => setWorkflowStatus(`启动失败: ${String(err)}`));
  });
  document.getElementById("wf-classify")?.addEventListener("click", () => {
    startWorkflowAction("classify_archive").catch((err) => setWorkflowStatus(`启动失败: ${String(err)}`));
  });
  document.getElementById("wf-sync")?.addEventListener("click", () => {
    startWorkflowAction("sync_embeddings").catch((err) => setWorkflowStatus(`启动失败: ${String(err)}`));
  });
  document.getElementById("wf-estimate")?.addEventListener("click", () => {
    startWorkflowAction("estimate_tokens").catch((err) => setWorkflowStatus(`启动失败: ${String(err)}`));
  });
}

async function init() {
  for (const tab of document.querySelectorAll(".tab")) {
    tab.onclick = () => setTab(tab.dataset.tab);
  }
  setTab("rag");

  document.getElementById("mode-local").onclick = () => setMode("local");
  document.getElementById("mode-deepseek").onclick = () => setMode("deepseek");
  document.getElementById("mode-reasoner").onclick = () => setMode("reasoner");
  setMode("local");

  mountAppSidebarController("rag-sidebar", "rag", {
    storageKeyAliases: ["sidebar:rag_system:rag:v2"],
  });
  mountAppSidebarController("preview-sidebar", "preview", {
    storageKeyAliases: ["sidebar:rag_system:preview:v2"],
  });
  document.getElementById("rag-new-session").onclick = createSession;
  document.getElementById("rag-delete-session").onclick = deleteCurrentSession;
  document.getElementById("rag-session-rename-save")?.addEventListener("click", () => {
    saveSessionRename().catch((err) => window.alert(`重命名失败: ${String(err)}`));
  });
  document.getElementById("rag-session-rename-cancel")?.addEventListener("click", closeSessionRenameModal);
  sessionRenameModal()?.addEventListener("click", (event) => {
    const target = event.target;
    if (!(target instanceof Element)) return;
    if (target.getAttribute("data-role") === "rag-session-rename-backdrop") closeSessionRenameModal();
  });
  sessionRenameInput()?.addEventListener("keydown", (event) => {
    if (event.key !== "Enter") return;
    event.preventDefault();
    saveSessionRename().catch((err) => window.alert(`重命名失败: ${String(err)}`));
  });
  const ragSessionList = document.getElementById("rag-session-list");
  ragSessionList?.addEventListener("contextmenu", (event) => {
    const target = event.target;
    const item = target instanceof Element ? target.closest("[data-session-id]") : null;
    if (!item) return;
    event.preventDefault();
    openSessionRenameModalFromTarget(item);
  });
  if (ragSessionList) bindLongPress(ragSessionList, (target) => {
    openSessionRenameModalFromTarget(target);
  });

  document.getElementById("btn-kw").onclick = runKeyword;
  document.getElementById("btn-vs").onclick = runVector;
  document.getElementById("btn-refresh").onclick = resetPreviewList;
  document.getElementById("ask").onclick = () => ask("hybrid");
  const askLocalBtn = document.getElementById("ask-local");
  if (askLocalBtn) askLocalBtn.onclick = () => ask("local_only");
  document.getElementById("abort").onclick = abortAsk;
  initWorkflowDatePickers();
  document.getElementById("kw").addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      event.preventDefault();
      runKeyword();
    }
  });
  document.getElementById("question").addEventListener("keydown", (event) => {
    if (event.key === "Enter" && !event.shiftKey) {
      event.preventDefault();
      if (String(currentMode || "").toLowerCase() === "local") {
        ask("local_only", "local");
      } else {
        ask("hybrid");
      }
    }
  });
  wireChatLinks();
  wireDocLinks();
  wireWorkflowTab();

  // UI event bindings above are synchronous so the page is interactive immediately.
  // Load config + tree together; sessions are independent and don't block first paint.
  refreshSessions(true).catch((err) => console.error("sessions load failed:", err));
  await Promise.all([
    loadRagConfig(),
    loadWorkflowConfig(),
    loadTree(),
  ]);
}

init().catch((err) => {
  console.error(err);
  appendChat("assistant", `[错误] ${String(err)}`);
});
