(function () {
  "use strict";

  const coreUI = window.CoreUI || null;
  const DEFAULT_SIDEBAR_MOBILE_MEDIA_QUERY = "(max-width: 820px), (hover: none) and (pointer: coarse)";
  const DEFAULT_SIDEBAR_STORAGE_VERSION = "v1";

  const toastTimers = new Map();

  function byId(id) {
    return document.getElementById(id);
  }

  function resolveNode(nodeOrId) {
    if (!nodeOrId) return null;
    if (typeof nodeOrId === "string") return byId(nodeOrId);
    return nodeOrId instanceof Element ? nodeOrId : null;
  }

  function escapeHtml(value) {
    return String(value || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/\"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function jsonHeaders() {
    return { "Content-Type": "application/json" };
  }

  function normalizeSidebarStorageToken(value, fallback) {
    const normalized = String(value || "")
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9_-]+/g, "-")
      .replace(/-{2,}/g, "-")
      .replace(/^[-_]+|[-_]+$/g, "");
    return normalized || String(fallback || "sidebar").trim().toLowerCase();
  }

  function buildSidebarStorageKey(appId, scope, options) {
    const resolved = options || {};
    const appToken = normalizeSidebarStorageToken(appId, "app");
    const scopeToken = normalizeSidebarStorageToken(scope, "sidebar");
    const versionToken = normalizeSidebarStorageToken(resolved.version || DEFAULT_SIDEBAR_STORAGE_VERSION, DEFAULT_SIDEBAR_STORAGE_VERSION);
    return `sidebar:${appToken}:${scopeToken}:${versionToken}`;
  }

  function showToast(message, options) {
    const resolved = options || {};
    const id = String(resolved.id || "_toast");
    const duration = Number(resolved.duration || 2400);
    const hiddenClass = String(resolved.hiddenClass || "");
    let node = byId(id);
    if (!node) {
      node = document.createElement("div");
      node.id = id;
      if (resolved.className) {
        node.className = String(resolved.className);
      }
      if (resolved.style) {
        node.style.cssText = String(resolved.style);
      }
      document.body.appendChild(node);
    }
    node.textContent = String(message || "");
    if (hiddenClass) {
      node.classList.remove(hiddenClass);
    } else {
      node.style.opacity = "1";
    }

    const existingTimer = toastTimers.get(id);
    if (existingTimer) {
      clearTimeout(existingTimer);
    }
    const timer = window.setTimeout(() => {
      if (hiddenClass) {
        node.classList.add(hiddenClass);
      } else {
        node.style.opacity = "0";
      }
      toastTimers.delete(id);
    }, Math.max(0, duration));
    toastTimers.set(id, timer);
    return node;
  }

  function createModalController(options) {
    const resolved = options || {};
    const root = resolveNode(resolved.root);
    const hiddenClass = String(resolved.hiddenClass || "hidden");
    return {
      open() {
        if (!root) return;
        root.classList.remove(hiddenClass);
        root.setAttribute("aria-hidden", "false");
      },
      close() {
        if (!root) return;
        root.classList.add(hiddenClass);
        root.setAttribute("aria-hidden", "true");
      },
      isOpen() {
        return !!(root && !root.classList.contains(hiddenClass));
      },
      bindBackdropClose(backdropRole) {
        if (!root) return;
        const role = String(backdropRole || "").trim();
        root.addEventListener("click", (event) => {
          const target = event.target;
          if (!(target instanceof Element)) return;
          if (role && target.getAttribute("data-role") !== role) return;
          this.close();
        });
      },
    };
  }

  function createListDetailShellController(options) {
    const resolved = options || {};
    const container = resolveNode(resolved.container);
    const listPane = resolveNode(resolved.listPane);
    const toggleButton = resolveNode(resolved.toggleButton);
    const compactMediaQuery = String(resolved.compactMediaQuery || "(max-width: 980px)");
    const collapsedClass = String(resolved.collapsedClass || "is-list-collapsed");
    const listCollapsedClass = String(resolved.listCollapsedClass || "is-collapsed");
    const storageKey = String(resolved.storageKey || "").trim();
    const onLayoutChange = typeof resolved.onLayoutChange === "function" ? resolved.onLayoutChange : null;

    let collapsed = false;
    if (storageKey) {
      try {
        collapsed = window.localStorage.getItem(storageKey) === "1";
      } catch (_error) {
        collapsed = false;
      }
    }

    function isCompactLayout() {
      return typeof window !== "undefined" && window.matchMedia(compactMediaQuery).matches;
    }

    function syncToggle() {
      if (!toggleButton) return;
      toggleButton.textContent = collapsed
        ? (isCompactLayout() ? "向下展开" : ">")
        : (isCompactLayout() ? "向上折叠" : "<");
      toggleButton.setAttribute("aria-expanded", String(!collapsed));
      toggleButton.setAttribute("aria-label", toggleButton.textContent || "切换列表区域");
    }

    function applyLayout() {
      container?.classList.toggle(collapsedClass, collapsed);
      listPane?.classList.toggle(listCollapsedClass, collapsed);
      syncToggle();
      if (onLayoutChange) {
        onLayoutChange({ collapsed, compact: isCompactLayout() });
      }
      return collapsed;
    }

    function persist() {
      if (!storageKey) return;
      try {
        window.localStorage.setItem(storageKey, collapsed ? "1" : "0");
      } catch (_error) {
        // Ignore storage write failures.
      }
    }

    applyLayout();

    return {
      isCompactLayout,
      isCollapsed() {
        return collapsed;
      },
      setCollapsed(nextValue) {
        collapsed = Boolean(nextValue);
        persist();
        return applyLayout();
      },
      toggle() {
        return this.setCollapsed(!collapsed);
      },
      syncLayout() {
        return applyLayout();
      },
      syncToggle,
    };
  }

  function isMobileSidebarLayout(mediaQuery) {
    const query = String(mediaQuery || DEFAULT_SIDEBAR_MOBILE_MEDIA_QUERY);
    return typeof window !== "undefined" && window.matchMedia(query).matches;
  }

  function syncSidebarToggleButton(button, collapsed, options) {
    const resolved = options || {};
    const node = resolveNode(button);
    if (!node) return;
    const mobile = resolved.mobile == null ? isMobileSidebarLayout(resolved.mobileMediaQuery) : !!resolved.mobile;
    const nextCollapsed = !!collapsed;
    const desktopExpandedIcon = String(resolved.desktopExpandedIcon || "<");
    const desktopCollapsedIcon = String(resolved.desktopCollapsedIcon || ">");
    const mobileExpandedIcon = String(resolved.mobileExpandedIcon || "^");
    const mobileCollapsedIcon = String(resolved.mobileCollapsedIcon || "v");
    const label = mobile
      ? (nextCollapsed ? mobileCollapsedIcon : mobileExpandedIcon)
      : (nextCollapsed ? desktopCollapsedIcon : desktopExpandedIcon);
    const title = mobile
      ? (nextCollapsed ? "向下展开侧边栏" : "向上折叠侧边栏")
      : (nextCollapsed ? "向右展开侧边栏" : "向左折叠侧边栏");
    node.textContent = label;
    node.setAttribute("aria-expanded", String(!nextCollapsed));
    node.setAttribute("aria-label", title);
    node.setAttribute("title", title);
  }

  function setSidebarCollapsed(shell, sidebar, button, collapsed, options) {
    const resolved = options || {};
    const shellNode = resolveNode(shell);
    const sidebarNode = resolveNode(sidebar);
    const collapsedClass = String(resolved.collapsedClass || "collapsed");
    const shellCollapsedClass = String(resolved.shellCollapsedClass || "sidebar-collapsed");
    const nextCollapsed = !!collapsed;
    shellNode?.classList.toggle(shellCollapsedClass, nextCollapsed);
    sidebarNode?.classList.toggle(collapsedClass, nextCollapsed);
    syncSidebarToggleButton(button, nextCollapsed, resolved);
    return nextCollapsed;
  }

  function toggleSidebar(shell, sidebar, button, options) {
    const sidebarNode = resolveNode(sidebar);
    const collapsedClass = String((options || {}).collapsedClass || "collapsed");
    const nextCollapsed = !(sidebarNode?.classList.contains(collapsedClass));
    return setSidebarCollapsed(shell, sidebar, button, nextCollapsed, options);
  }

  function createSidebarController(options) {
    const resolved = options || {};
    const shell = resolveNode(resolved.shell);
    const sidebar = resolveNode(resolved.sidebar);
    const toggleButton = resolveNode(resolved.toggleButton || resolved.button);
    const storageKey = String(resolved.storageKey || "").trim();
    const storageKeyAliases = Array.isArray(resolved.storageKeyAliases)
      ? resolved.storageKeyAliases.map((item) => String(item || "").trim()).filter(Boolean)
      : [];
    const collapsedClass = String(resolved.collapsedClass || "collapsed");
    const shellCollapsedClass = String(resolved.shellCollapsedClass || "sidebar-collapsed");
    const mobileMediaQuery = String(resolved.mobileMediaQuery || DEFAULT_SIDEBAR_MOBILE_MEDIA_QUERY);
    const rememberMobileState = !!resolved.rememberMobileState;
    const defaultDesktopCollapsed = !!resolved.defaultDesktopCollapsed;
    const defaultMobileExpanded = resolved.defaultMobileExpanded == null ? true : !!resolved.defaultMobileExpanded;
    const onChange = typeof resolved.onChange === "function" ? resolved.onChange : null;
    let desktopCollapsed = defaultDesktopCollapsed;
    let mobileExpanded = defaultMobileExpanded;

    function readStoredValue(candidateKeys, suffix) {
      for (const key of candidateKeys) {
        if (!key) continue;
        try {
          const storedValue = window.localStorage.getItem(suffix ? `${key}${suffix}` : key);
          if (storedValue === "1" || storedValue === "0") {
            return storedValue;
          }
        } catch (_error) {
          return null;
        }
      }
      return null;
    }

    if (storageKey) {
      try {
        const storedDesktop = readStoredValue([storageKey, ...storageKeyAliases], "");
        if (storedDesktop === "1" || storedDesktop === "0") {
          desktopCollapsed = storedDesktop === "1";
        }
        if (rememberMobileState) {
          const storedMobile = readStoredValue([storageKey, ...storageKeyAliases], ":mobile");
          if (storedMobile === "1" || storedMobile === "0") {
            mobileExpanded = storedMobile === "1";
          }
        }
      } catch (_error) {
        desktopCollapsed = defaultDesktopCollapsed;
        mobileExpanded = defaultMobileExpanded;
      }
    }

    function isMobileLayout() {
      return isMobileSidebarLayout(mobileMediaQuery);
    }

    function currentCollapsed() {
      return isMobileLayout() ? !mobileExpanded : desktopCollapsed;
    }

    function persist() {
      if (!storageKey) return;
      try {
        window.localStorage.setItem(storageKey, desktopCollapsed ? "1" : "0");
        if (rememberMobileState) {
          window.localStorage.setItem(`${storageKey}:mobile`, mobileExpanded ? "1" : "0");
        }
      } catch (_error) {
        // Ignore storage failures.
      }
    }

    function apply() {
      const mobile = isMobileLayout();
      const collapsed = currentCollapsed();
      if (toggleButton) {
        toggleButton.dataset.sidebarStorageKey = storageKey;
        toggleButton.dataset.sidebarMobileKey = `${storageKey}:mobile`;
      }
      setSidebarCollapsed(shell, sidebar, toggleButton, collapsed, {
        ...resolved,
        collapsedClass,
        shellCollapsedClass,
        mobile,
        mobileMediaQuery,
      });
      if (onChange) {
        onChange({ collapsed, expanded: !collapsed, mobile });
      }
      return collapsed;
    }

    apply();

    return {
      isMobileLayout,
      isCollapsed() {
        return currentCollapsed();
      },
      setCollapsed(nextValue) {
        if (isMobileLayout()) {
          mobileExpanded = !Boolean(nextValue);
        } else {
          desktopCollapsed = Boolean(nextValue);
        }
        persist();
        return apply();
      },
      toggle() {
        if (isMobileLayout()) {
          mobileExpanded = !mobileExpanded;
        } else {
          desktopCollapsed = !desktopCollapsed;
        }
        persist();
        return apply();
      },
      syncLayout() {
        return apply();
      },
      syncToggle() {
        syncSidebarToggleButton(toggleButton, currentCollapsed(), {
          ...resolved,
          mobile: isMobileLayout(),
          mobileMediaQuery,
        });
      },
    };
  }

  function mountSidebarController(options) {
    const resolved = options || {};
    const sidebarNode = resolveNode(resolved.sidebar);
    const scope = String(resolved.scope || resolved.sidebarScope || sidebarNode?.id || "sidebar").trim();
    const storageKey = String(resolved.storageKey || "").trim() || buildSidebarStorageKey(resolved.appId || resolved.app || "app", scope, {
      version: resolved.storageVersion || DEFAULT_SIDEBAR_STORAGE_VERSION,
    });
    const controller = createSidebarController({
      ...resolved,
      storageKey,
      mobileMediaQuery: resolved.mobileMediaQuery || DEFAULT_SIDEBAR_MOBILE_MEDIA_QUERY,
    });
    const toggleButton = resolveNode(resolved.toggleButton || resolved.button);
    const layoutSync = () => controller?.syncLayout?.();
    const mediaQueryList = typeof window !== "undefined" && typeof window.matchMedia === "function"
      ? window.matchMedia(String(resolved.mobileMediaQuery || DEFAULT_SIDEBAR_MOBILE_MEDIA_QUERY))
      : null;

    if (toggleButton && toggleButton.dataset.sidebarControllerBound !== "1") {
      toggleButton.dataset.sidebarControllerBound = "1";
      toggleButton.addEventListener("click", () => {
        controller?.toggle?.();
      });
    }

    if (toggleButton) {
      toggleButton.dataset.sidebarStorageKey = storageKey;
      toggleButton.dataset.sidebarMobileKey = `${storageKey}:mobile`;
    }

    if (typeof window !== "undefined") {
      window.addEventListener("resize", layoutSync);
    }
    if (mediaQueryList) {
      if (typeof mediaQueryList.addEventListener === "function") {
        mediaQueryList.addEventListener("change", layoutSync);
      } else if (typeof mediaQueryList.addListener === "function") {
        mediaQueryList.addListener(layoutSync);
      }
    }
    controller?.syncLayout?.();
    return controller;
  }

  const appShell = {
    DEFAULT_SIDEBAR_MOBILE_MEDIA_QUERY,
    DEFAULT_SIDEBAR_STORAGE_VERSION,
    byId,
    buildSidebarStorageKey,
    createListDetailShellController,
    createModalController,
    createSidebarController,
    escapeHtml,
    jsonHeaders,
    mountSidebarController,
    resolveNode,
    setSidebarCollapsed,
    showToast,
    syncSidebarToggleButton,
    toggleSidebar,
  };

  if (coreUI?.register) {
    coreUI.register("appShell", appShell);
  }
  window.SharedAppShell = appShell;
})();