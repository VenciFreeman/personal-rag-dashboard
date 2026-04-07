"use strict";

(() => {
  const AppShell = window.CoreUI?.get?.("appShell") || window.SharedAppShell || {};
  const SharedShell = window.CoreUI?.get?.("reportShell") || window.SharedReportShell || {};

  const state = {
    currentTab: "query",
    currentMode: "keyword",
    latestResults: [],
    currentSearchOffset: 0,
    currentSearchLimit: 50,
    currentSearchTotal: 0,
    searchSerial: 0,
    searchAbortController: null,
    currentPreviewIndex: -1,
    editingItemId: "",
    editingCoverPath: null,
    filterSearchTimer: null,
    toastTimer: null,
    filterMeta: { filters: {} },
    latestFacetCounts: null,
    formSuggestionMeta: { fields: {} },
    cardPointerState: new WeakMap(),
    currentRating: null,
    editDialogDraftBeforeDelete: null,
    pendingDeleteItemId: null,
    statsBootstrapped: false,
    statsBootstrapping: false,
    statsResizeBound: false,
    aliasProposalReviewPage: 1,
    aliasProposalReviewTotalPages: 1,
    aliasProposalReviewItems: [],
    activeAliasEditProposalId: "",
    statsCharts: {},
    filterExpandState: {
      media_type: false,
      nationality: false,
      category: false,
      channel: false,
    },
    filterState: {
      year: null,
      rating: null,
      media_type: null,
      nationality: null,
      category: null,
      channel: null,
    },
  };

  async function apiGet(url) {
    const response = await fetch(url);
    if (!response.ok) throw new Error(await response.text());
    return response.json();
  }

  async function apiPost(url, payload, options = {}) {
    const response = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
      ...(options.signal ? { signal: options.signal } : {}),
    });
    if (!response.ok) throw new Error(await response.text());
    return response.json();
  }

  async function apiPut(url, payload) {
    const response = await fetch(url, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!response.ok) throw new Error(await response.text());
    return response.json();
  }

  async function apiDelete(url) {
    const response = await fetch(url, { method: "DELETE" });
    if (!response.ok) throw new Error(await response.text());
    return response.json();
  }

  function normalizeDateInput(raw) {
    const text = String(raw || "").trim();
    if (!text) return null;
    const digits = text.replace(/[^0-9]/g, "");
    if (digits.length === 8) {
      const year = Number(digits.slice(0, 4));
      let month = Number(digits.slice(4, 6));
      let day = Number(digits.slice(6, 8));
      if (!Number.isFinite(year) || year <= 0) return null;
      if (!Number.isFinite(month) || month <= 0) month = 1;
      if (!Number.isFinite(day) || day <= 0) day = 1;
      month = Math.min(12, month);
      day = Math.min(31, day);
      return `${String(year).padStart(4, "0")}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
    }
    const match = text.match(/^(\d{4})-(\d{1,2})-(\d{1,2})$/);
    if (!match) return null;
    const year = Number(match[1]);
    let month = Number(match[2]);
    let day = Number(match[3]);
    if (!Number.isFinite(year) || year <= 0) return null;
    if (!Number.isFinite(month) || month <= 0) month = 1;
    if (!Number.isFinite(day) || day <= 0) day = 1;
    month = Math.min(12, month);
    day = Math.min(31, day);
    return `${String(year).padStart(4, "0")}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
  }

  async function uploadCoverFile(file, title, overwritePath = null) {
    const safeName = file?.name ? encodeURIComponent(file.name) : "";
    const safeTitle = title ? encodeURIComponent(title) : "";
    let url = `/api/library/cover?filename=${safeName}`;
    if (safeTitle) url += `&title=${safeTitle}`;
    const safeOverwritePath = overwritePath ? encodeURIComponent(String(overwritePath).trim()) : "";
    if (safeOverwritePath) url += `&overwrite_path=${safeOverwritePath}`;
    const response = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": file.type || "application/octet-stream" },
      body: file,
    });
    if (!response.ok) throw new Error(await response.text());
    return response.json();
  }

  function formatNumber(value) {
    const numeric = Number(value || 0);
    if (!Number.isFinite(numeric)) return "0";
    return numeric.toLocaleString("zh-CN");
  }

  function showToast(message) {
    if (AppShell.showToast) {
      AppShell.showToast(message, {
        id: "floating-toast",
        className: "floating-toast hidden",
        hiddenClass: "hidden",
        duration: 1000,
      });
      return;
    }
    let toast = document.getElementById("floating-toast");
    if (!toast) {
      toast = document.createElement("div");
      toast.id = "floating-toast";
      toast.className = "floating-toast hidden";
      document.body.appendChild(toast);
    }
    toast.textContent = message;
    toast.classList.remove("hidden");
    if (state.toastTimer) clearTimeout(state.toastTimer);
    state.toastTimer = setTimeout(() => {
      toast.classList.add("hidden");
    }, 1000);
  }

  function setDialogOpenState(isOpen) {
    document.documentElement.classList.toggle("dialog-open", Boolean(isOpen));
    document.body.classList.toggle("dialog-open", Boolean(isOpen));
  }

  const api = { apiDelete, apiGet, apiPost, apiPut, normalizeDateInput, uploadCoverFile };
  const helpers = { formatNumber, setDialogOpenState, showToast };
  const shell = { AppShell, SharedShell };
  const libraryModules = {};
  const getModules = () => libraryModules;

  function scheduleLibraryStatsChartsResize() {
    const resize = () => {
      Object.values(state.statsCharts || {}).forEach(chart => chart?.resize?.());
    };
    resize();
    requestAnimationFrame(() => {
      resize();
      setTimeout(resize, 80);
      setTimeout(resize, 220);
    });
  }

  function setTab(name) {
    state.currentTab = name;
    document.querySelectorAll(".tab").forEach(tab => {
      const active = tab.dataset.tab === name;
      tab.classList.toggle("active", active);
      tab.setAttribute("aria-selected", String(active));
    });
    document.querySelectorAll(".tab-panel").forEach(panel => {
      panel.classList.toggle("active", panel.id === `panel-${name}`);
    });
    if (name === "stats" && !state.statsBootstrapped && !state.statsBootstrapping) {
      state.statsBootstrapping = true;
      libraryModules.stats.bootstrapStatsTab()
        .then(() => {
          scheduleLibraryStatsChartsResize();
        })
        .catch(error => {
          state.statsBootstrapping = false;
          console.error(error);
          alert(`统计页初始化失败: ${error.message}`);
        });
      return;
    }
    if (name === "stats") {
      scheduleLibraryStatsChartsResize();
    }
  }

  function requestedLibraryTabFromLocation() {
    const search = String(window.location.search || "").trim();
    if (!search.startsWith("?")) return "";
    const tab = String(new URLSearchParams(search.slice(1)).get("tab") || "").trim();
    return ["query", "stats", "analysis"].includes(tab) ? tab : "";
  }

  function instantiateModules() {
    const registry = window.LibraryTrackerModules || {};
    const context = { state, api, helpers, shell, getModules };
    libraryModules.query = registry.createQueryWorkspaceModule?.(context);
    libraryModules.editor = registry.createLibraryEditorModule?.(context);
    libraryModules.stats = registry.createLibraryStatsModule?.(context);
    const missing = Object.entries(libraryModules).filter(([, value]) => !value).map(([key]) => key);
    if (missing.length) {
      throw new Error(`Library modules missing: ${missing.join(", ")}`);
    }
  }

  async function bootstrap() {
    instantiateModules();
    libraryModules.editor.initRatingControl();
    libraryModules.editor.bindEvents();
    libraryModules.query.bindEvents();
    libraryModules.stats.bindEvents();

    const requestedTab = requestedLibraryTabFromLocation();
    if (requestedTab) setTab(requestedTab);

    document.querySelectorAll(".tab").forEach(tabBtn => {
      tabBtn.addEventListener("click", () => {
        const name = String(tabBtn.dataset.tab || "query");
        setTab(name);
      });
    });

    let bootstrapData;
    try {
      bootstrapData = await apiGet("/api/library/bootstrap?limit=50");
    } catch (error) {
      console.error("bootstrap failed, falling back:", error);
      const [metaPayload, suggestionPayload] = await Promise.all([
        apiGet("/api/library/meta"),
        apiGet("/api/library/suggestions"),
      ]);
      state.filterMeta = metaPayload;
      state.formSuggestionMeta = libraryModules.editor.normalizeSuggestionPayload(suggestionPayload);
      try {
        await libraryModules.query.refreshFacetsAndFilters();
      } catch (refreshError) {
        console.error(refreshError);
        libraryModules.query.renderFilters(state.filterMeta, null);
      }
      await libraryModules.query.runSearch("keyword");
      await libraryModules.query.tryOpenPreviewItemFromLocation();
      window.addEventListener("hashchange", () => {
        libraryModules.query.tryOpenPreviewItemFromLocation().catch(() => {});
      });
      return;
    }

    state.filterMeta = { filters: bootstrapData.filter_options || {} };
    state.formSuggestionMeta = libraryModules.editor.normalizeSuggestionPayload({ fields: bootstrapData.suggestions || {} });
    libraryModules.query.renderFilters(state.filterMeta, bootstrapData.facets || {});
    const initialResults = bootstrapData.initial_results || bootstrapData.initial_search;
    if (initialResults && Array.isArray(initialResults.results)) {
      libraryModules.query.renderResults(initialResults);
    } else {
      await libraryModules.query.runSearch("keyword");
    }
    await libraryModules.query.tryOpenPreviewItemFromLocation();
    window.addEventListener("hashchange", () => {
      libraryModules.query.tryOpenPreviewItemFromLocation().catch(() => {});
    });
  }

  bootstrap().catch(error => {
    console.error(error);
    alert(`初始化失败: ${error.message}`);
  });
})();
