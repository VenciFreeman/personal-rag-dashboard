let currentSessionId = "";
let currentMode = "local";
let askInFlight = false;
let sessionsCache = [];
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

async function apiGet(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

async function apiPost(url, payload) {
  const r = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!r.ok) throw new Error(await r.text());
  return r.json();
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

function toggleSidebar(id) {
  const el = document.getElementById(id);
  if (!el) return;
  el.classList.toggle("collapsed");
  const workspace = el.closest(".workspace");
  if (workspace) {
    workspace.classList.toggle("sidebar-collapsed", el.classList.contains("collapsed"));
  }
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
  // Minimal normalization: only ensure code blocks are protected
  // Trust that input markdown is already properly formatted with correct line breaks
  // Remove the aggressive regex replacements that were destroying indentation
  if (!source) return "";
  return stripZeroWidth(String(source));
}

function markdownToHtml(text) {
  const source = normalizeMarkdown(text || "");
  const codeBlocks = [];
  const hrBlocks = [];
  const mathBlocks = [];
  const themeBlocks = [];
  
  // Protect HR blocks and code blocks early to prevent disruption
  let protected = source.replace(/^-{3,}\s*$/gm, () => {
    const idx = hrBlocks.length;
    hrBlocks.push("<hr />");
    return `__HR_BLOCK_${idx}__`;
  });
  
  // Protect LaTeX block math: \[ ... \]
  protected = protected.replace(/\\\[([\s\S]*?)\\\]/g, (_m, formula) => {
    const idx = mathBlocks.length;
    mathBlocks.push(`<div class="math-block">\\[${escapeHtml(String(formula || "").trim())}\\]</div>`);
    return `__MATH_BLOCK_${idx}__`;
  });
  
  // Protect LaTeX inline math: \( ... \)
  protected = protected.replace(/\\\((.*?)\\\)/g, (_m, formula) => {
    const idx = mathBlocks.length;
    mathBlocks.push(`<span class="math-inline">\\(${escapeHtml(String(formula || "").trim())}\\)</span>`);
    return `__MATH_BLOCK_${idx}__`;
  });
  
  // Protect <theme> tags
  protected = protected.replace(/<theme>(.*?)<\/theme>/gi, (_m, content) => {
    const idx = themeBlocks.length;
    themeBlocks.push(`<span class="theme-tag">${escapeHtml(String(content || "").trim())}</span>`);
    return `__THEME_BLOCK_${idx}__`;
  });
  
  const withCodeTokens = protected.replace(/```([\s\S]*?)```/g, (_m, code) => {
    const idx = codeBlocks.length;
    codeBlocks.push(`<pre><code>${escapeHtml(String(code || "").trim())}</code></pre>`);
    return `__CODE_BLOCK_${idx}__`;
  });

  let html = escapeHtml(withCodeTokens);

  html = html.replace(/^(#{1,6})\s+(.+)$/gm, (_m, hashes, title) => {
    const level = hashes.length;
    return `<h${level}>${title}</h${level}>`;
  });
  html = html.replace(/^&gt;\s?(.+)$/gm, "<blockquote>$1</blockquote>");
  html = html.replace(/\*\*([^*]+)\*\*/g, "<strong>$1</strong>");
  html = html.replace(/`([^`]+)`/g, "<code>$1</code>");
  // Support one-level nested brackets in link label, e.g. [[PDF] 标题](https://...).
  html = html.replace(/\[((?:[^\[\]]|\[[^\[\]]*\])+)]\(([^)]+)\)/g, (_m, label, url) => {
    const href = String(url || "").trim().replace(/"/g, "%22");
    const isExternal = /^https?:\/\//i.test(href);
    if (isExternal) {
      return `<a href="${href}" class="external-link" target="_blank" rel="noopener noreferrer">${label}<span class="ext-link-icon" aria-hidden="true">&#x2197;</span></a>`;
    }
    return `<a href="${href}">${label}</a>`;
  });

  const lines = html.split("\n");

  function getListMeta(line) {
    const normalizedLine = normalizeListWhitespace(stripZeroWidth(line));
    // Exclude HR, code, math, and theme block placeholders from list detection
    if (/^__HR_BLOCK_\d+__|^__CODE_BLOCK_\d+__|^__MATH_BLOCK_\d+__|^__THEME_BLOCK_\d+__/.test(normalizedLine.trim())) {
      return null;
    }
    // Support more ordered-list markers: 1. / 1) / 1、 / 1．
    const m = normalizedLine.match(/^(\s*)([-*+•·]|\d+[\.\)\u3001\uFF0E])\s+(.+)$/);
    if (!m) return null;
    const indent = m[1].replace(/\t/g, "    ").length;
    const type = /^\d+/.test(m[2]) ? "ol" : "ul";
    return { indent, type, text: m[3] };
  }

  const out = [];
  let i = 0;
  
  // Collect consecutive list items
  function collectListItems(startIdx) {
    const items = [];
    while (startIdx < lines.length) {
      const raw = lines[startIdx];
      if (!raw.trim()) {
        // Allow blank lines inside lists if the next non-empty line is a list item.
        let lookahead = startIdx + 1;
        while (lookahead < lines.length && !String(lines[lookahead] || "").trim()) {
          lookahead += 1;
        }
        const nextMeta = lookahead < lines.length ? getListMeta(lines[lookahead]) : null;
        if (nextMeta) {
          startIdx += 1;
          continue;
        }
        break;
      }
      const meta = getListMeta(raw);
      if (!meta) break;
      items.push({ ...meta, lineIdx: startIdx });
      startIdx += 1;
    }
    return { items, nextIdx: startIdx };
  }
  
  // Render list items with nesting. Child indentation is based on actual next-line indent,
  // not a fixed +2 spaces, so 2/4-space markdown styles both work.
  function renderListTree(allItems, startIdx = 0, targetIndent = 0) {
    if (!allItems || !allItems.length || startIdx >= allItems.length) {
      return { html: "", nextIdx: startIdx };
    }

    let html = "";
    let i = startIdx;
    let currentListType = null;

    while (i < allItems.length) {
      let item = allItems[i];

      if (item.indent < targetIndent) {
        break;
      }

      // Do not drop malformed deeper items. Promote them to current level so content stays visible.
      if (item.indent > targetIndent) {
        item = { ...item, indent: targetIndent };
      }

      if (currentListType !== item.type) {
        if (currentListType) html += `</${currentListType}>\n`;
        html += `<${item.type}>\n`;
        currentListType = item.type;
      }

      let itemContent = item.text;
      i += 1;

      if (i < allItems.length && allItems[i].indent > targetIndent) {
        const childIndent = allItems[i].indent;
        const child = renderListTree(allItems, i, childIndent);
        if (child.html) {
          itemContent += `\n${child.html}`;
        }
        i = child.nextIdx;
      }

      html += `<li>${itemContent}</li>\n`;
    }

    if (currentListType) {
      html += `</${currentListType}>`;
    }

    return { html, nextIdx: i };
  }
  
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
      // Collect all consecutive list items
      const block = collectListItems(i);
      const baseIndent = block.items.length ? block.items[0].indent : 0;
      const result = renderListTree(block.items, 0, baseIndent);
      out.push(result.html);
      i = block.nextIdx;
      continue;
    }

    if (/^<h\d|^<pre|^<blockquote|^__CODE_BLOCK_\d+__|^__HR_BLOCK_\d+__|^__MATH_BLOCK_\d+__|^__THEME_BLOCK_\d+__/.test(trimmed)) {
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
  html = html.replace(/__THEME_BLOCK_(\d+)__/g, (_m, idx) => themeBlocks[Number(idx)] || "");
  return html;
}

function streamTextToHtml(text) {
  // Lightweight renderer for in-flight chunks; final pass still uses markdownToHtml.
  return escapeHtml(String(text || "")).replace(/\n/g, "<br>");
}

function markdownPreviewForStreaming(text) {
  // During streaming, incomplete fenced blocks can suppress heading/list rendering.
  // Add a temporary closing fence when needed, then let full markdown render on done.
  let value = String(text || "");
  const fenceCount = (value.match(/```/g) || []).length;
  if (fenceCount % 2 === 1) {
    value += "\n```";
  }
  return markdownToHtml(value);
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
  addFeedbackButton(row, {
    question,
    answer,
    trace_id: String(payload.traceId || "").trim(),
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

function renderSessions() {
  const ul = document.getElementById("rag-session-list");
  ul.innerHTML = "";
  for (const session of sessionsCache) {
    const li = document.createElement("li");
    if (session.id === currentSessionId) li.classList.add("active");
    li.innerHTML = `<div class="title">${escapeHtml(session.title || "新会话")}</div><div class="meta">${escapeHtml(session.updated_at || "")}</div>`;
    li.onclick = async () => {
      currentSessionId = session.id;
      renderSessions();
      if (session.messages) {
        renderChat(session.messages);
      } else {
        try {
          const full = await apiGet(`/api/rag/sessions/${encodeURIComponent(session.id)}`);
          const idx = sessionsCache.findIndex((s) => s.id === session.id);
          if (idx >= 0) sessionsCache[idx] = { ...sessionsCache[idx], messages: full.messages || [] };
          renderChat(full.messages || []);
        } catch (_e) {
          renderChat([]);
        }
      }
    };
    ul.appendChild(li);
  }
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
    renderChat(session.messages);
  } else {
    try {
      const full = await apiGet(`/api/rag/sessions/${encodeURIComponent(currentSessionId)}`);
      const idx = sessionsCache.findIndex((s) => s.id === currentSessionId);
      if (idx >= 0) sessionsCache[idx] = { ...sessionsCache[idx], messages: full.messages || [] };
      renderChat(full.messages || []);
    } catch (_e) {
      renderChat([]);
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
    const hasChildren = isDir && node.children && node.children.length;
    caret.textContent = hasChildren ? (collapsedDirs.has(node.path) ? "▸" : "▾") : "•";
    const label = document.createElement("span");
    label.className = "tree-label";
    label.textContent = node.name;
    label.title = node.path || node.name;
    row.appendChild(caret);
    row.appendChild(label);
    li.appendChild(row);

    if (isDir && hasChildren) {
      row.onclick = () => {
        if (collapsedDirs.has(node.path)) collapsedDirs.delete(node.path);
        else collapsedDirs.add(node.path);
        renderTreeNodes(ul, nodes);
      };
    } else if (!isDir) {
      row.onclick = async () => openDoc(node.path);
    }

    ul.appendChild(li);
    if (isDir && hasChildren && !collapsedDirs.has(node.path)) {
      const child = document.createElement("ul");
      child.className = "tree nested";
      li.appendChild(child);
      renderTreeNodes(child, node.children);
    }
  }
}

function collectDirPaths(nodes) {
  for (const node of nodes || []) {
    if (node.type !== "dir") continue;
    if (node.path) collapsedDirs.add(node.path);
    collectDirPaths(node.children || []);
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
  previewTreeNodes = (data.tree && data.tree.children) || [];
  collapsedDirs.clear();
  collectDirPaths(previewTreeNodes);
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

  questionInput.value = "";
  appendChat("user", question);
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
      session_id: currentSessionId, 
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
      currentSessionId = answer.session_id || currentSessionId;
      if (answer.aborted) {
        if (pendingContent) pendingContent.innerHTML = markdownToHtml("已中止");
        pending.classList.remove("processing");
      } else {
        pending.remove();
        const parsed = splitThinkBlocks(answer.answer || "");
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
          pending.classList.remove("processing");
        } else if (event.type === "progress") {
          if (event.trace_id) activeTraceId = String(event.trace_id || "").trim();
          progressText = String(event.message || progressText);
        } else if (event.type === "chunk") {
          if (event.trace_id) activeTraceId = String(event.trace_id || "").trim();
          const chunk = String(event.text || "");
          if (!assistantRow) {
            assistantRow = appendChatRow("assistant", "", true);
            assistantContent = assistantRow.querySelector(".content");
          }
          if (chunk) {
            streamedText += chunk;
            // Fast path: update immediately if enough time elapsed since last paint.
            const now = Date.now();
            if (assistantContent && now - lastRenderAt >= 180) {
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
          if (pendingContent) pendingContent.innerHTML = markdownToHtml("已中止");
          pending.classList.remove("processing");
          // Preserve any streamed content
          if (streamedText && assistantContent) {
            const parsed = stripThinkForPreview(streamedText);
            insertSystemRowsBefore(assistantRow, parsed.thoughts);
            upsertTraceMetaRowBefore(assistantRow, formatTraceMeta(activeTraceId));
            assistantContent.innerHTML = markdownToHtml(parsed.answer + "\n\n_[已中止]_");
          }
        } else if (event.type === "error") {
          if (event.trace_id) activeTraceId = String(event.trace_id || "").trim();
          streamFinalized = true;
          const errorMsg = event.message || "请求失败";
          if (pendingContent) pendingContent.innerHTML = markdownToHtml(`[错误] ${errorMsg}`);
          pending.classList.remove("processing");
          // Preserve any streamed content before error
          if (streamedText && assistantContent) {
            const parsed = stripThinkForPreview(streamedText);
            insertSystemRowsBefore(assistantRow, parsed.thoughts);
            upsertTraceMetaRowBefore(assistantRow, formatTraceMeta(activeTraceId));
            assistantContent.innerHTML = markdownToHtml(parsed.answer + `\n\n---\n\n**错误**: ${errorMsg}`);
          } else if (!assistantRow) {
            // No content yet, just show error
            assistantRow = appendChatRow("assistant", `**错误**: ${errorMsg}`, true);
            upsertTraceMetaRowBefore(assistantRow, formatTraceMeta(activeTraceId));
          }
        } else if (event.type === "done") {
          if (event.trace_id) activeTraceId = String(event.trace_id || "").trim();
          streamFinalized = true;
          removePending();
          finalPayload = event.payload || null;
          const answer = String(event.payload?.answer || streamedText || "");
          currentSessionId = event.payload?.session_id || currentSessionId;
          if (!assistantRow) {
            assistantRow = appendChatRow("assistant", "", true);
            assistantContent = assistantRow.querySelector(".content");
          }
          const parsed = splitThinkBlocks(answer);
          insertSystemRowsBefore(assistantRow, parsed.thoughts);
          upsertTraceMetaRowBefore(assistantRow, formatTraceMeta(activeTraceId || event.payload?.trace_id));
          if (assistantContent) {
            const fallback = stripThinkForPreview(answer);
            assistantContent.innerHTML = markdownToHtml(parsed.answer || fallback.answer || "");
          }
          addRagFeedbackForRow(assistantRow, {
            question,
            answer: parsed.answer || answer,
            traceId: activeTraceId || event.payload?.trace_id,
            searchMode,
            mode: event.payload?.mode,
            debugEnabled,
          });
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
      removePending();
      if (streamedText && assistantContent) {
        const parsed = stripThinkForPreview(streamedText);
        insertSystemRowsBefore(assistantRow, parsed.thoughts);
        upsertTraceMetaRowBefore(assistantRow, formatTraceMeta(activeTraceId || finalPayload?.trace_id));
        assistantContent.innerHTML = markdownToHtml(parsed.answer);
      } else if (streamedText && !assistantRow) {
        const parsed = stripThinkForPreview(streamedText);
        assistantRow = appendChatRow("assistant", parsed.answer, true);
        upsertTraceMetaRowBefore(assistantRow, formatTraceMeta(activeTraceId || finalPayload?.trace_id));
        assistantContent = assistantRow.querySelector(".content");
      } else if (!assistantRow) {
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
    removePending();
    // Preserve any streamed content before network/parsing error
    if (streamedText && assistantContent) {
      const parsed = stripThinkForPreview(streamedText);
      insertSystemRowsBefore(assistantRow, parsed.thoughts);
      assistantContent.innerHTML = markdownToHtml(parsed.answer + `\n\n---\n\n**错误**: ${errorMsg}`);
    } else if (!assistantRow) {
      appendChatRow("assistant", `**错误**: ${errorMsg}`, true);
    }
    await refreshSidebarTitles();
  } finally {
    clearInterval(pendingTimer);
    clearInterval(streamRenderTimer);
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
  if (apiKey) apiKey.value = String(cfg.api_key || "");
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

  document.getElementById("rag-toggle-sidebar").onclick = () => toggleSidebar("rag-sidebar");
  document.getElementById("preview-toggle-sidebar").onclick = () => toggleSidebar("preview-sidebar");
  document.getElementById("rag-new-session").onclick = createSession;
  document.getElementById("rag-delete-session").onclick = deleteCurrentSession;

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

  await loadRagConfig();
  await Promise.all([loadWorkflowConfig(), loadTree()]);
  await refreshSessions(true);
}

init().catch((err) => {
  console.error(err);
  appendChat("assistant", `[错误] ${String(err)}`);
});
