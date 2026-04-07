(function () {
  "use strict";

  const coreUI = window.CoreUI || null;
  const appShell = coreUI?.get?.("appShell") || window.SharedAppShell || null;

  function escapeHtml(value) {
    return String(value || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/\"/g, "&quot;");
  }

  function markdownToHtml(text) {
    if (window.CoreMarkdown?.render) {
      return window.CoreMarkdown.render(text);
    }
    return `<p>${escapeHtml(String(text || ""))}</p>`;
  }

  function formatTime(value) {
    if (!value) return "—";
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return String(value || "");
    return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}-${String(date.getDate()).padStart(2, "0")} ${String(date.getHours()).padStart(2, "0")}:${String(date.getMinutes()).padStart(2, "0")}`;
  }

  function renderMetaPills(metaEl, pills, emptyText = "暂无报告") {
    if (!metaEl) return;
    const rows = Array.isArray(pills) ? pills.filter(Boolean) : [];
    if (!rows.length) {
      metaEl.innerHTML = `<span class="analysis-meta-pill muted">${escapeHtml(emptyText)}</span>`;
      return;
    }
    metaEl.innerHTML = rows.map((item) => `<span class="analysis-meta-pill${item.muted ? " muted" : ""}">${escapeHtml(item.text || "")}</span>`).join("");
  }

  function renderReportBody(docEl, report, emptyState) {
    if (!docEl) return;
    if (!report?.markdown) {
      const title = String(emptyState?.title || "暂无报告");
      const body = String(emptyState?.body || "请先生成报告。");
      docEl.innerHTML = `<div class="analysis-empty-state"><h3>${escapeHtml(title)}</h3><p>${escapeHtml(body)}</p></div>`;
      return;
    }
    docEl.innerHTML = markdownToHtml(report.markdown);
  }

  function renderReportViewer(options) {
    const report = options?.report || null;
    const pills = typeof options?.buildMetaPills === "function" ? options.buildMetaPills(report) : [];
    renderMetaPills(options?.metaEl, pills, options?.emptyMetaText || "暂无报告");
    if (options?.docEl) {
      renderReportBody(options.docEl, report, options?.emptyState || {});
    }
  }

  function syncSidebarToggleButton(button, collapsed) {
    appShell?.syncSidebarToggleButton?.(button, collapsed, { mobileMediaQuery: appShell?.DEFAULT_SIDEBAR_MOBILE_MEDIA_QUERY || "(max-width: 820px), (hover: none) and (pointer: coarse)" });
  }

  function setSidebarCollapsed(shell, sidebar, button, collapsed) {
    return appShell?.setSidebarCollapsed?.(shell, sidebar, button, collapsed, { mobileMediaQuery: appShell?.DEFAULT_SIDEBAR_MOBILE_MEDIA_QUERY || "(max-width: 820px), (hover: none) and (pointer: coarse)" });
  }

  function toggleSidebar(shell, sidebar, button) {
    return appShell?.toggleSidebar?.(shell, sidebar, button, { mobileMediaQuery: appShell?.DEFAULT_SIDEBAR_MOBILE_MEDIA_QUERY || "(max-width: 820px), (hover: none) and (pointer: coarse)" });
  }

  function createContextMenuController(menu, options = {}) {
    const width = Number(options.width || 160);
    const height = Number(options.height || 92);
    return {
      open(x, y) {
        if (!menu) return;
        menu.classList.remove("hidden");
        menu.setAttribute("aria-hidden", "false");
        menu.style.left = `${Math.min(Number(x || 0), window.innerWidth - width - 8)}px`;
        menu.style.top = `${Math.min(Number(y || 0), window.innerHeight - height - 8)}px`;
      },
      close() {
        if (!menu) return;
        menu.classList.add("hidden");
        menu.setAttribute("aria-hidden", "true");
      },
      bindAutoClose() {
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

  function openHiddenModal(nodeOrId) {
    const node = typeof nodeOrId === "string" ? document.getElementById(nodeOrId) : nodeOrId;
    if (!node) return;
    node.classList.remove("hidden");
    node.setAttribute("aria-hidden", "false");
  }

  function closeHiddenModal(nodeOrId) {
    const node = typeof nodeOrId === "string" ? document.getElementById(nodeOrId) : nodeOrId;
    if (!node) return;
    node.classList.add("hidden");
    node.setAttribute("aria-hidden", "true");
  }

  function createHiddenModalController(nodeOrId) {
    return {
      open() {
        openHiddenModal(nodeOrId);
      },
      close() {
        closeHiddenModal(nodeOrId);
      },
    };
  }

  const reportShell = {
    escapeHtml,
    markdownToHtml,
    formatTime,
    renderMetaPills,
    renderReportBody,
    renderReportViewer,
    syncSidebarToggleButton,
    setSidebarCollapsed,
    toggleSidebar,
    createContextMenuController,
    createHiddenModalController,
    openHiddenModal,
    closeHiddenModal,
  };

  if (coreUI?.register) {
    coreUI.register("reportShell", reportShell);
  }
  window.SharedReportShell = reportShell;
})();
