(function () {
  function escapeHtml(text) {
    return String(text || "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;");
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
    let value = stripZeroWidth(String(source));
    value = value.replace(/([^\n])\s+(#{1,6}\s+)/g, "$1\n\n$2");
    value = value.replace(/([^\n])\s+(-{3,}\s*(?:\n|$))/g, "$1\n\n$2");
    return value;
  }

  function renderMarkdown(text) {
    const source = normalizeMarkdown(text || "");
    const codeBlocks = [];
    const hrBlocks = [];
    const mathBlocks = [];
    const themeBlocks = [];
    const brBlocks = [];
    const rawAnchorBlocks = [];

    let protectedText = source.replace(/^-{3,}\s*$/gm, () => {
      const idx = hrBlocks.length;
      hrBlocks.push("<hr />");
      return `__HR_BLOCK_${idx}__`;
    });

    protectedText = protectedText.replace(/\\\[([\s\S]*?)\\\]/g, (_m, formula) => {
      const idx = mathBlocks.length;
      mathBlocks.push(`<div class="math-block">\\[${escapeHtml(String(formula || "").trim())}\\]</div>`);
      return `__MATH_BLOCK_${idx}__`;
    });

    protectedText = protectedText.replace(/\\\((.*?)\\\)/g, (_m, formula) => {
      const idx = mathBlocks.length;
      mathBlocks.push(`<span class="math-inline">\\(${escapeHtml(String(formula || "").trim())}\\)</span>`);
      return `__MATH_BLOCK_${idx}__`;
    });

    protectedText = protectedText.replace(/<br\s*\/?>/gi, () => {
      const idx = brBlocks.length;
      brBlocks.push("<br />");
      return `__BR_BLOCK_${idx}__`;
    });

    protectedText = protectedText.replace(/<theme>(.*?)<\/theme>/gi, (_m, content) => {
      const idx = themeBlocks.length;
      themeBlocks.push(`<span class="theme-tag">${escapeHtml(String(content || "").trim())}</span>`);
      return `__THEME_BLOCK_${idx}__`;
    });

    protectedText = protectedText.replace(/<think>([\s\S]*?)<\/think>/gi, (_m, content) => {
      const idx = themeBlocks.length;
      const body = escapeHtml(String(content || "").trim()).replace(/\n/g, "<br />");
      themeBlocks.push(`<details class="thinking-block"><summary>思考</summary><div class="thinking-block-body">${body}</div></details>`);
      return `__THEME_BLOCK_${idx}__`;
    });

    const withCodeTokens = protectedText.replace(/```([\s\S]*?)```/g, (_m, code) => {
      const idx = codeBlocks.length;
      codeBlocks.push(`<pre><code>${escapeHtml(String(code || "").trim())}</code></pre>`);
      return `__CODE_BLOCK_${idx}__`;
    });

    const withRawAnchorTokens = withCodeTokens.replace(/<a\s+href="([^"]+)"[^>]*>([\s\S]*?)<\/a>/gi, (_m, hrefRaw, labelRaw) => {
      const href = String(hrefRaw || "").trim().replace(/"/g, "%22");
      if (!/^(?:https?:\/\/|doc:\/\/)/i.test(href)) {
        return escapeHtml(_m);
      }
      const label = escapeHtml(String(labelRaw || "").trim());
      const idx = rawAnchorBlocks.length;
      if (/^https?:\/\//i.test(href)) {
        rawAnchorBlocks.push(`<a href="${href}" class="external-link" target="_blank" rel="noopener noreferrer">${label}</a>`);
      } else {
        rawAnchorBlocks.push(`<a href="${href}">${label}</a>`);
      }
      return `__RAW_ANCHOR_BLOCK_${idx}__`;
    });

    let html = escapeHtml(withRawAnchorTokens);
    html = html.replace(/^(#{1,6})\s+(.+)$/gm, (_m, hashes, title) => {
      const level = hashes.length;
      return `<h${level}>${title}</h${level}>`;
    });
    html = html.replace(/^&gt;\s?(.+)$/gm, "<blockquote>$1</blockquote>");
    html = html.replace(/\*\*([^*]+)\*\*/g, "<strong>$1</strong>");
    html = html.replace(/`([^`]+)`/g, "<code>$1</code>");
    html = html.replace(/\[((?:[^\[\]]|\[[^\[\]]*\])+)]\(([^)]+)\)/g, (_m, label, url) => {
      const href = String(url || "").trim().replace(/"/g, "%22");
      const normalizedLabel = String(label || "").trim();
      const displayLabel = /^\d+$/.test(normalizedLabel) || /^资料\s*\d+$/.test(normalizedLabel)
        ? `[${normalizedLabel}]`
        : label;
      const isExternal = /^https?:\/\//i.test(href);
      if (isExternal) {
        return `<a href="${href}" class="external-link" target="_blank" rel="noopener noreferrer">${displayLabel}<span class="ext-link-icon" aria-hidden="true">&#x2197;</span></a>`;
      }
      return `<a href="${href}">${displayLabel}</a>`;
    });

    const lines = html.split("\n");

    function getListMeta(line) {
      const normalizedLine = normalizeListWhitespace(stripZeroWidth(line));
      if (/^__HR_BLOCK_\d+__|^__CODE_BLOCK_\d+__|^__MATH_BLOCK_\d+__|^__THEME_BLOCK_\d+__|^__BR_BLOCK_\d+__|^__RAW_ANCHOR_BLOCK_\d+__/.test(normalizedLine.trim())) {
        return null;
      }
      const match = normalizedLine.match(/^(\s*)([-*+•·]|\d+[\.\)\u3001\uFF0E])\s+(.+)$/);
      if (!match) return null;
      const indent = match[1].replace(/\t/g, "    ").length;
      const type = /^\d+/.test(match[2]) ? "ol" : "ul";
      return { indent, type, text: match[3] };
    }

    function collectListItems(startIdx) {
      const items = [];
      while (startIdx < lines.length) {
        const raw = lines[startIdx];
        if (!raw.trim()) {
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

    function splitTableRow(line) {
      let value = String(line || "").trim();
      if (value.startsWith("|")) value = value.slice(1);
      if (value.endsWith("|")) value = value.slice(0, -1);
      return value.split("|").map(cell => cell.trim());
    }

    function isTableSeparator(line) {
      const cells = splitTableRow(line);
      return cells.length > 0 && cells.every(cell => /^:?-{3,}:?$/.test(cell));
    }

    function tableAlignment(cell) {
      const value = String(cell || "").trim();
      if (value.startsWith(":") && value.endsWith(":")) return "center";
      if (value.endsWith(":")) return "right";
      return "left";
    }

    function renderTableBlock(startIdx) {
      const headerLine = String(lines[startIdx] || "").trim();
      const separatorLine = String(lines[startIdx + 1] || "").trim();
      if (!headerLine.includes("|") || !isTableSeparator(separatorLine)) {
        return null;
      }
      const headers = splitTableRow(headerLine);
      const alignments = splitTableRow(separatorLine).map(tableAlignment);
      const rows = [];
      let nextIdx = startIdx + 2;
      while (nextIdx < lines.length) {
        const candidate = String(lines[nextIdx] || "").trim();
        if (!candidate || !candidate.includes("|")) break;
        rows.push(splitTableRow(candidate));
        nextIdx += 1;
      }
      const headerHtml = headers.map((cell, index) => `<th style="text-align:${alignments[index] || "left"}">${cell}</th>`).join("");
      const bodyHtml = rows.map(row => {
        const normalized = row.slice(0, headers.length);
        while (normalized.length < headers.length) normalized.push("");
        return `<tr>${normalized.map((cell, index) => `<td style="text-align:${alignments[index] || "left"}">${cell}</td>`).join("")}</tr>`;
      }).join("\n");
      return {
        html: `<table><thead><tr>${headerHtml}</tr></thead><tbody>${bodyHtml}</tbody></table>`,
        nextIdx,
      };
    }

    function renderListTree(allItems, startIdx, targetIndent) {
      if (!allItems || !allItems.length || startIdx >= allItems.length) {
        return { html: "", nextIdx: startIdx };
      }

      let html = "";
      let index = startIdx;
      let currentListType = null;

      while (index < allItems.length) {
        let item = allItems[index];
        if (item.indent < targetIndent) break;
        if (item.indent > targetIndent) {
          item = { ...item, indent: targetIndent };
        }

        if (currentListType !== item.type) {
          if (currentListType) html += `</${currentListType}>\n`;
          html += `<${item.type}>\n`;
          currentListType = item.type;
        }

        let itemContent = item.text;
        index += 1;

        if (index < allItems.length && allItems[index].indent > targetIndent) {
          const childIndent = allItems[index].indent;
          const child = renderListTree(allItems, index, childIndent);
          if (child.html) itemContent += `\n${child.html}`;
          index = child.nextIdx;
        }

        html += `<li>${itemContent}</li>\n`;
      }

      if (currentListType) html += `</${currentListType}>`;
      return { html, nextIdx: index };
    }

    const out = [];
    let lineIndex = 0;
    while (lineIndex < lines.length) {
      const raw = lines[lineIndex];
      const trimmed = raw.trim();

      if (!trimmed) {
        out.push("");
        lineIndex += 1;
        continue;
      }

      const table = renderTableBlock(lineIndex);
      if (table) {
        out.push(table.html);
        lineIndex = table.nextIdx;
        continue;
      }

      const meta = getListMeta(raw);
      if (meta) {
        const block = collectListItems(lineIndex);
        const baseIndent = block.items.length ? block.items[0].indent : 0;
        const result = renderListTree(block.items, 0, baseIndent);
        out.push(result.html);
        lineIndex = block.nextIdx;
        continue;
      }

      if (/^<h\d|^<pre|^<blockquote|^__CODE_BLOCK_\d+__|^__HR_BLOCK_\d+__|^__MATH_BLOCK_\d+__|^__THEME_BLOCK_\d+__|^__BR_BLOCK_\d+__|^__RAW_ANCHOR_BLOCK_\d+__/.test(trimmed)) {
        out.push(trimmed);
      } else {
        out.push(`<p>${raw}</p>`);
      }
      lineIndex += 1;
    }

    html = out.join("\n");
    html = html.replace(/__CODE_BLOCK_(\d+)__/g, (_m, idx) => codeBlocks[Number(idx)] || "");
    html = html.replace(/__HR_BLOCK_(\d+)__/g, (_m, idx) => hrBlocks[Number(idx)] || "");
    html = html.replace(/__MATH_BLOCK_(\d+)__/g, (_m, idx) => mathBlocks[Number(idx)] || "");
    html = html.replace(/__THEME_BLOCK_(\d+)__/g, (_m, idx) => themeBlocks[Number(idx)] || "");
    html = html.replace(/__BR_BLOCK_(\d+)__/g, (_m, idx) => brBlocks[Number(idx)] || "<br />");
    html = html.replace(/__RAW_ANCHOR_BLOCK_(\d+)__/g, (_m, idx) => rawAnchorBlocks[Number(idx)] || "");
    return html;
  }

  function previewForStreaming(text) {
    let value = String(text || "");
    const fenceCount = (value.match(/```/g) || []).length;
    if (fenceCount % 2 === 1) {
      value += "\n```";
    }
    return renderMarkdown(value);
  }

  window.CoreMarkdown = {
    render: renderMarkdown,
    previewForStreaming,
  };
})();