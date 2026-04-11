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
    openOverlayKeys: new Set(),
    overlayScrollSnapshot: null,
  };

  let viewportHeightSyncBound = false;
  let viewportHeightSyncFrame = 0;

  async function apiGet(url) {
    const response = await fetch(url);
    if (!response.ok) throw new Error(await response.text());
    return response.json();
  }

  function isTransientFetchError(error) {
    if (!error || error.name === "AbortError") return false;
    const message = String(error.message || "").trim().toLowerCase();
    return error instanceof TypeError
      || message === "load failed"
      || message.includes("failed to fetch")
      || message.includes("fetch failed")
      || message.includes("networkerror");
  }

  async function fetchWithRetry(url, requestOptions, options = {}) {
    const retries = Math.max(0, Number(options.retries) || 0);
    let attempt = 0;
    while (true) {
      try {
        return await fetch(url, requestOptions);
      } catch (error) {
        if (attempt >= retries || !isTransientFetchError(error)) {
          throw error;
        }
        attempt += 1;
      }
    }
  }

  async function apiPost(url, payload, options = {}) {
    const response = await fetchWithRetry(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
      ...(options.signal ? { signal: options.signal } : {}),
    }, { retries: options.retries });
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
    setOverlayOpenState("dialog", isOpen);
  }

  function applyViewportHeightVar() {
    viewportHeightSyncFrame = 0;
    const viewportHeight = window.visualViewport?.height || window.innerHeight || document.documentElement.clientHeight || 0;
    if (!viewportHeight) return;
    document.documentElement.style.setProperty("--app-viewport-height", `${Math.round(viewportHeight)}px`);
  }

  function scheduleViewportHeightSync() {
    if (viewportHeightSyncFrame) return;
    viewportHeightSyncFrame = window.requestAnimationFrame(applyViewportHeightVar);
  }

  function bindViewportHeightSync() {
    if (viewportHeightSyncBound) return;
    viewportHeightSyncBound = true;
    scheduleViewportHeightSync();
    window.addEventListener("resize", scheduleViewportHeightSync, { passive: true });
    window.addEventListener("orientationchange", scheduleViewportHeightSync, { passive: true });
    window.addEventListener("pageshow", scheduleViewportHeightSync, { passive: true });
    if (window.visualViewport) {
      window.visualViewport.addEventListener("resize", scheduleViewportHeightSync, { passive: true });
      window.visualViewport.addEventListener("scroll", scheduleViewportHeightSync, { passive: true });
    }
  }

  function setOverlayOpenState(key, isOpen) {
    const overlayKey = String(key || "dialog").trim() || "dialog";
    if (isOpen) state.openOverlayKeys.add(overlayKey);
    else state.openOverlayKeys.delete(overlayKey);
    const anyOpen = state.openOverlayKeys.size > 0;
    if (isOpen && !state.overlayScrollSnapshot) {
      const mainPanel = document.querySelector(".main-panel");
      state.overlayScrollSnapshot = {
        windowX: window.scrollX || 0,
        windowY: window.scrollY || window.pageYOffset || 0,
        mainPanelScrollTop: mainPanel ? mainPanel.scrollTop : 0,
        bodyPosition: document.body.style.position || "",
        bodyTop: document.body.style.top || "",
        bodyLeft: document.body.style.left || "",
        bodyRight: document.body.style.right || "",
        bodyWidth: document.body.style.width || "",
        bodyOverflow: document.body.style.overflow || "",
        htmlOverflow: document.documentElement.style.overflow || "",
      };
      document.body.style.position = "fixed";
      document.body.style.top = `-${state.overlayScrollSnapshot.windowY}px`;
      document.body.style.left = `-${state.overlayScrollSnapshot.windowX}px`;
      document.body.style.right = "0";
      document.body.style.width = "100%";
      document.body.style.overflow = "hidden";
      document.documentElement.style.overflow = "hidden";
    }
    document.documentElement.classList.toggle("dialog-open", anyOpen);
    document.body.classList.toggle("dialog-open", anyOpen);
    if (!anyOpen && state.overlayScrollSnapshot) {
      const snapshot = state.overlayScrollSnapshot;
      state.overlayScrollSnapshot = null;
      document.body.style.position = snapshot.bodyPosition;
      document.body.style.top = snapshot.bodyTop;
      document.body.style.left = snapshot.bodyLeft;
      document.body.style.right = snapshot.bodyRight;
      document.body.style.width = snapshot.bodyWidth;
      document.body.style.overflow = snapshot.bodyOverflow;
      document.documentElement.style.overflow = snapshot.htmlOverflow;
      window.requestAnimationFrame(() => {
        const mainPanel = document.querySelector(".main-panel");
        window.scrollTo(snapshot.windowX, snapshot.windowY);
        if (mainPanel) mainPanel.scrollTop = snapshot.mainPanelScrollTop;
      });
    }
  }

  const api = { apiDelete, apiGet, apiPost, apiPut, normalizeDateInput, uploadCoverFile };
  const helpers = { bindViewportHeightSync, formatNumber, setDialogOpenState, setOverlayOpenState, showToast };
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
    bindViewportHeightSync();
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
