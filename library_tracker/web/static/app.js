let currentTab = "query";
let currentMode = "keyword";
let latestResults = [];
let currentSearchOffset = 0;
let currentSearchLimit = 50;
let currentSearchTotal = 0;
let _searchSerial = 0;
let _searchAbortController = null;
let currentPreviewIndex = -1;
let editingItemId = "";
let editingCoverPath = null;
let filterSearchTimer = null;
let toastTimer = null;
let filterMeta = { filters: {} };
let formSuggestionMeta = { fields: {} };
const cardPointerState = new WeakMap();
let currentRating = null;
let statsBootstrapped = false;
let _statsBootstrapping = false;
const statsCharts = {};
const STATS_MEDIA_TYPES = ["book", "video", "music", "game"];
const FORM_SUGGESTION_FIELDS = ["author", "nationality", "category", "channel", "publisher"];
const FORM_MULTI_VALUE_FIELDS = new Set(["author", "nationality", "category", "publisher"]);
function setDialogOpenState(_isOpen) {
  // Dialog state hook intentionally kept as no-op after rollback.
}

function ensurePreviewImageViewer() {
  let viewer = document.getElementById("preview-image-viewer");
  if (viewer) return viewer;

  viewer = document.createElement("div");
  viewer.id = "preview-image-viewer";
  viewer.className = "preview-image-viewer hidden";
  viewer.setAttribute("aria-hidden", "true");
  viewer.innerHTML = `
    <div class="preview-image-backdrop" data-role="image-backdrop"></div>
    <div class="preview-image-panel" role="dialog" aria-modal="true" aria-label="封面大图预览">
      <button type="button" class="preview-image-close" data-role="image-close" title="关闭">✕</button>
      <img class="preview-image-img" alt="封面大图" />
    </div>
  `;
  document.body.appendChild(viewer);

  viewer.addEventListener("click", (e) => {
    if (!e.target) return;
    const target = e.target;
    if (
      target.dataset?.role === "image-backdrop" ||
      target.dataset?.role === "image-close"
    ) {
      closePreviewImageViewer();
    }
  });

  return viewer;
}

function openPreviewImageViewer(src, altText) {
  if (!src) return;
  const viewer = ensurePreviewImageViewer();
  const img = viewer.querySelector(".preview-image-img");
  if (!img) return;
  img.src = src;
  img.alt = altText || "封面大图";
  viewer.classList.remove("hidden");
  viewer.setAttribute("aria-hidden", "false");
}

function closePreviewImageViewer() {
  const viewer = document.getElementById("preview-image-viewer");
  if (!viewer) return;
  viewer.classList.add("hidden");
  viewer.setAttribute("aria-hidden", "true");
}

function isPreviewImageViewerOpen() {
  const viewer = document.getElementById("preview-image-viewer");
  return !!(viewer && !viewer.classList.contains("hidden"));
}

const filterExpandState = {
  media_type: false,
  nationality: false,
  category: false,
  channel: false,
};

const CHIP_FILTER_FIELDS = ["media_type", "nationality", "category", "channel"];
const SELECT_FILTER_FIELDS = ["year", "rating"];

const filterState = {
  year: null,
  rating: null,
  media_type: null,
  nationality: null,
  category: null,
  channel: null,
};

function formatDisplayToken(value) {
  const text = String(value || "").trim();
  if (!text) return "";
  return text.replace(/\b([a-z])/g, (_m, c) => c.toUpperCase()).replace(/_/g, " ");
}

function formatFieldLabel(field) {
  return formatDisplayToken(field);
}

function normalizeSuggestionPayload(payload) {
  const fields = {};
  for (const field of FORM_SUGGESTION_FIELDS) {
    const source = Array.isArray(payload?.fields?.[field]) ? payload.fields[field] : [];
    const deduped = [];
    const seen = new Set();
    for (const raw of source) {
      const value = String(raw || "").trim();
      if (!value || seen.has(value)) continue;
      seen.add(value);
      deduped.push(value);
    }
    fields[field] = deduped;
  }
  return { fields };
}

function getFieldSuggestions(field) {
  return Array.isArray(formSuggestionMeta?.fields?.[field]) ? formSuggestionMeta.fields[field] : [];
}

function isSuggestionSeparator(char) {
  return /[,;，；、\n]/.test(char);
}

function getSuggestionSegment(input) {
  const field = String(input?.dataset?.suggestField || input?.name || "").trim();
  const value = String(input?.value || "");
  if (!FORM_MULTI_VALUE_FIELDS.has(field)) {
    return {
      field,
      query: value.trim(),
      replaceStart: 0,
      replaceEnd: value.length,
    };
  }

  const cursor = typeof input.selectionStart === "number" ? input.selectionStart : value.length;
  let segmentStart = 0;
  for (let index = cursor - 1; index >= 0; index -= 1) {
    if (isSuggestionSeparator(value.charAt(index))) {
      segmentStart = index + 1;
      break;
    }
  }

  let segmentEnd = value.length;
  for (let index = cursor; index < value.length; index += 1) {
    if (isSuggestionSeparator(value.charAt(index))) {
      segmentEnd = index;
      break;
    }
  }

  const segment = value.slice(segmentStart, segmentEnd);
  const leadingWhitespace = (segment.match(/^\s*/) || [""])[0].length;
  const trailingWhitespace = (segment.match(/\s*$/) || [""])[0].length;
  const replaceStart = segmentStart + leadingWhitespace;
  const replaceEnd = Math.max(replaceStart, segmentEnd - trailingWhitespace);

  return {
    field,
    query: value.slice(replaceStart, replaceEnd).trim(),
    replaceStart,
    replaceEnd,
  };
}

function filterSuggestions(field, query) {
  const options = getFieldSuggestions(field);
  if (!options.length) return [];
  if (!query) return options.slice(0, 8);

  const loweredQuery = query.toLocaleLowerCase();
  const startsWith = [];
  const includes = [];
  for (const option of options) {
    const loweredOption = option.toLocaleLowerCase();
    if (loweredOption === loweredQuery) continue;
    if (loweredOption.startsWith(loweredQuery)) {
      startsWith.push(option);
      continue;
    }
    if (loweredOption.includes(loweredQuery)) {
      includes.push(option);
    }
  }

  return startsWith.concat(includes).slice(0, 8);
}

function getSuggestionMenu(input) {
  return input?.closest("label")?.querySelector(".form-suggestion-menu") || null;
}

function ensureSuggestionMenu(input) {
  const host = input?.closest("label");
  if (!host) return null;
  host.classList.add("has-suggestion-menu");
  let menu = getSuggestionMenu(input);
  if (menu) return menu;
  menu = document.createElement("div");
  menu.className = "form-suggestion-menu hidden";
  menu.setAttribute("role", "listbox");
  menu.dataset.activeIndex = "-1";
  menu.addEventListener("mousedown", (event) => {
    event.preventDefault();
  });
  host.appendChild(menu);
  return menu;
}

function getSuggestionButtons(menu) {
  return menu ? Array.from(menu.querySelectorAll(".form-suggestion-option")) : [];
}

function hideSuggestionMenu(input) {
  const menu = getSuggestionMenu(input);
  if (!menu) return;
  menu.classList.add("hidden");
  menu.innerHTML = "";
  menu.dataset.activeIndex = "-1";
  input.setAttribute("aria-expanded", "false");
}

function setActiveSuggestion(menu, nextIndex) {
  const buttons = getSuggestionButtons(menu);
  if (!buttons.length) {
    menu.dataset.activeIndex = "-1";
    return;
  }

  const boundedIndex = Math.max(0, Math.min(nextIndex, buttons.length - 1));
  buttons.forEach((button, index) => {
    const active = index === boundedIndex;
    button.classList.toggle("active", active);
    button.setAttribute("aria-selected", String(active));
    if (active) {
      button.scrollIntoView({ block: "nearest" });
    }
  });
  menu.dataset.activeIndex = String(boundedIndex);
}

function applySuggestion(input, suggestion) {
  const value = String(input.value || "");
  const segment = getSuggestionSegment(input);
  input.value = `${value.slice(0, segment.replaceStart)}${suggestion}${value.slice(segment.replaceEnd)}`;
  const nextCursor = segment.replaceStart + suggestion.length;
  if (typeof input.setSelectionRange === "function") {
    input.setSelectionRange(nextCursor, nextCursor);
  }
  input.dispatchEvent(new Event("input", { bubbles: true }));
  hideSuggestionMenu(input);
}

function renderSuggestionMenu(input, suggestions) {
  const menu = ensureSuggestionMenu(input);
  if (!menu) return;

  if (!suggestions.length) {
    hideSuggestionMenu(input);
    return;
  }

  menu.innerHTML = "";
  suggestions.forEach((suggestion) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "form-suggestion-option";
    button.textContent = suggestion;
    button.setAttribute("role", "option");
    button.setAttribute("aria-selected", "false");
    button.addEventListener("click", () => {
      applySuggestion(input, suggestion);
    });
    menu.appendChild(button);
  });

  menu.classList.remove("hidden");
  menu.dataset.activeIndex = "-1";
  input.setAttribute("aria-expanded", "true");
}

function refreshSuggestionsForInput(input, showWhenEmpty = false) {
  const field = String(input?.dataset?.suggestField || input?.name || "").trim();
  if (!FORM_SUGGESTION_FIELDS.includes(field)) return;

  const segment = getSuggestionSegment(input);
  if (!segment.query && !showWhenEmpty) {
    hideSuggestionMenu(input);
    return;
  }

  const suggestions = filterSuggestions(field, segment.query);
  renderSuggestionMenu(input, suggestions);
}

function handleSuggestionKeydown(event) {
  const input = event.currentTarget;
  const menu = getSuggestionMenu(input);
  const buttons = getSuggestionButtons(menu);
  const isOpen = !!(menu && !menu.classList.contains("hidden") && buttons.length);

  if (event.key === "ArrowDown") {
    event.preventDefault();
    if (!isOpen) {
      refreshSuggestionsForInput(input, true);
      const nextMenu = getSuggestionMenu(input);
      if (nextMenu) {
        setActiveSuggestion(nextMenu, 0);
      }
      return;
    }
    const currentIndex = Number(menu.dataset.activeIndex || -1);
    setActiveSuggestion(menu, currentIndex + 1 >= buttons.length ? 0 : currentIndex + 1);
    return;
  }

  if (event.key === "ArrowUp" && isOpen) {
    event.preventDefault();
    const currentIndex = Number(menu.dataset.activeIndex || -1);
    setActiveSuggestion(menu, currentIndex <= 0 ? buttons.length - 1 : currentIndex - 1);
    return;
  }

  if (event.key === "Enter" && isOpen) {
    const activeIndex = Number(menu.dataset.activeIndex || -1);
    const targetButton = buttons[activeIndex >= 0 ? activeIndex : 0];
    if (targetButton) {
      event.preventDefault();
      targetButton.click();
    }
    return;
  }

  if (event.key === "Escape" && isOpen) {
    event.preventDefault();
    hideSuggestionMenu(input);
  }
}

function setupFormAutocomplete() {
  const form = document.getElementById("item-form");
  if (!form) return;

  for (const field of FORM_SUGGESTION_FIELDS) {
    const input = form.elements.namedItem(field);
    if (!input || typeof input.addEventListener !== "function") continue;
    input.dataset.suggestField = field;
    input.setAttribute("autocomplete", "off");
    input.setAttribute("spellcheck", "false");
    input.setAttribute("aria-autocomplete", "list");
    input.setAttribute("aria-expanded", "false");
    ensureSuggestionMenu(input);
    input.addEventListener("focus", () => {
      refreshSuggestionsForInput(input, true);
    });
    input.addEventListener("input", () => {
      refreshSuggestionsForInput(input, true);
    });
    input.addEventListener("keydown", handleSuggestionKeydown);
    input.addEventListener("blur", () => {
      window.setTimeout(() => {
        hideSuggestionMenu(input);
      }, 120);
    });
  }

  document.addEventListener("click", (event) => {
    if (event.target instanceof Element && event.target.closest(".has-suggestion-menu")) {
      return;
    }
    for (const field of FORM_SUGGESTION_FIELDS) {
      const input = form.elements.namedItem(field);
      if (input) {
        hideSuggestionMenu(input);
      }
    }
  });
}

async function apiGet(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

async function apiPost(url, payload, options = {}) {
  const { signal } = options;
  const r = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
    ...(signal ? { signal } : {}),
  });
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

async function apiPut(url, payload) {
  const r = await fetch(url, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

function normalizeDateInput(raw) {
  const text = String(raw || "").trim();
  if (!text) return null;
  const digits = text.replace(/[^0-9]/g, "");
  if (digits.length === 8) {
    const y = Number(digits.slice(0, 4));
    let m = Number(digits.slice(4, 6));
    let d = Number(digits.slice(6, 8));
    if (!Number.isFinite(y) || y <= 0) return null;
    if (!Number.isFinite(m) || m <= 0) m = 1;
    if (!Number.isFinite(d) || d <= 0) d = 1;
    m = Math.min(12, m);
    d = Math.min(31, d);
    return `${String(y).padStart(4, "0")}-${String(m).padStart(2, "0")}-${String(d).padStart(2, "0")}`;
  }
  const m = text.match(/^(\d{4})-(\d{1,2})-(\d{1,2})$/);
  if (!m) return null;
  const y = Number(m[1]);
  let mm = Number(m[2]);
  let dd = Number(m[3]);
  if (!Number.isFinite(y) || y <= 0) return null;
  if (!Number.isFinite(mm) || mm <= 0) mm = 1;
  if (!Number.isFinite(dd) || dd <= 0) dd = 1;
  mm = Math.min(12, mm);
  dd = Math.min(31, dd);
  return `${String(y).padStart(4, "0")}-${String(mm).padStart(2, "0")}-${String(dd).padStart(2, "0")}`;
}

async function uploadCoverFile(file, title, overwritePath = null) {
  const safeName = (file && file.name) ? encodeURIComponent(file.name) : "";
  const safeTitle = title ? encodeURIComponent(title) : "";
  let url = `/api/library/cover?filename=${safeName}`;
  if (safeTitle) {
    url += `&title=${safeTitle}`;
  }
  const safeOverwritePath = overwritePath ? encodeURIComponent(String(overwritePath).trim()) : "";
  if (safeOverwritePath) {
    url += `&overwrite_path=${safeOverwritePath}`;
  }
  const r = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": file.type || "application/octet-stream",
    },
    body: file,
  });
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

function setTab(name) {
  currentTab = name;
  for (const tab of document.querySelectorAll(".tab")) {
    const active = tab.dataset.tab === name;
    tab.classList.toggle("active", active);
    tab.setAttribute("aria-selected", String(active));
  }
  for (const panel of document.querySelectorAll(".tab-panel")) {
    panel.classList.toggle("active", panel.id === `panel-${name}`);
  }

  if (name === "stats" && !statsBootstrapped && !_statsBootstrapping) {
    _statsBootstrapping = true;
    bootstrapStatsTab().catch((err) => {
      _statsBootstrapping = false;
      console.error(err);
      alert(`统计页初始化失败: ${err.message}`);
    });
  }
}

function monokaiPalette() {
  return [
    "#f92672",
    "#a6e22e",
    "#66d9ef",
    "#fd971f",
    "#ae81ff",
    "#e6db74",
    "#38ccd1",
    "#f8f8f2",
    "#75715e",
    "#8f908a",
    "#cfcfc2",
    "#c2395e",
  ];
}

function renderStatsOverview(overview) {
  const table = document.getElementById("stats-overview-table");
  if (!table) return;
  table.innerHTML = "";

  const mediaTypes = Array.isArray(overview?.media_types) ? overview.media_types : STATS_MEDIA_TYPES;
  const currentYear = Number(overview?.current_year || new Date().getFullYear());

  const thead = document.createElement("thead");
  const headRow = document.createElement("tr");
  headRow.innerHTML = `<th>指标</th>${mediaTypes.map((m) => `<th>${formatDisplayToken(m)}</th>`).join("")}<th>合计</th>`;
  thead.appendChild(headRow);

  const tbody = document.createElement("tbody");
  const totalRow = document.createElement("tr");
  totalRow.innerHTML = `
    <td>总计</td>
    ${mediaTypes.map((m) => `<td>${Number((overview?.total_by_media || {})[m] || 0)}</td>`).join("")}
    <td><strong>${Number(overview?.total_all || 0)}</strong></td>
  `;
  tbody.appendChild(totalRow);

  const currentRow = document.createElement("tr");
  currentRow.innerHTML = `
    <td>${currentYear} 年</td>
    ${mediaTypes.map((m) => `<td>${Number((overview?.current_year_by_media || {})[m] || 0)}</td>`).join("")}
    <td><strong>${Number(overview?.current_year_all || 0)}</strong></td>
  `;
  tbody.appendChild(currentRow);

  table.appendChild(thead);
  table.appendChild(tbody);
}

function fillStatsFieldOptions(fields) {
  const select = document.getElementById("stats-field");
  if (!select) return;
  const preferred = ["category", "nationality", "channel", "author", "publisher"];
  const source = Array.isArray(fields) && fields.length ? fields : preferred;
  const ordered = preferred.filter((x) => source.includes(x)).concat(source.filter((x) => !preferred.includes(x)));
  select.innerHTML = "";
  for (const field of ordered) {
    const opt = document.createElement("option");
    opt.value = field;
    opt.textContent = formatDisplayToken(field);
    select.appendChild(opt);
  }
  if (ordered.includes("category")) {
    select.value = "category";
  }
}

function fillStatsYearOptions(overview) {
  const select = document.getElementById("stats-year");
  if (!select) return;
  const years = Array.isArray(overview?.available_years) ? overview.available_years : [];
  const currentYear = Number(overview?.current_year || new Date().getFullYear());

  select.innerHTML = "";

  const allOpt = document.createElement("option");
  allOpt.value = "";
  allOpt.textContent = "全部年份";
  select.appendChild(allOpt);

  const currentOpt = document.createElement("option");
  currentOpt.value = String(currentYear);
  currentOpt.textContent = `仅 ${currentYear}`;
  select.appendChild(currentOpt);

  for (const y of years) {
    const yearNum = Number(y);
    if (!Number.isFinite(yearNum)) continue;
    if (yearNum === currentYear) continue;
    const opt = document.createElement("option");
    opt.value = String(yearNum);
    opt.textContent = String(yearNum);
    select.appendChild(opt);
  }
}

function renderChartLegend(mediaType, rows, colors) {
  const wrap = document.getElementById(`stats-legend-${mediaType}`);
  if (!wrap) return;
  wrap.innerHTML = "";

  if (!rows.length) {
    wrap.textContent = "无可用数据";
    return;
  }

  const buildList = (items, offset) => {
    const ul = document.createElement("ul");
    ul.className = "chart-legend-list";
    items.forEach((row, idx) => {
      const li = document.createElement("li");
      li.className = "chart-legend-item";
      const dot = document.createElement("span");
      dot.className = "chart-legend-dot";
      dot.style.background = colors[(offset + idx) % colors.length];
      const text = document.createElement("span");
      text.textContent = `${row.label}: ${row.value}`;
      li.appendChild(dot);
      li.appendChild(text);
      ul.appendChild(li);
    });
    return ul;
  };

  const topRows = rows.slice(0, 5);
  wrap.appendChild(buildList(topRows, 0));

  if (rows.length > 5) {
    const detail = document.createElement("details");
    detail.className = "chart-legend-more";
    const summary = document.createElement("summary");
    summary.textContent = `显示更多 (${rows.length - 5})`;
    detail.appendChild(summary);
    detail.appendChild(buildList(rows.slice(5), 5));
    wrap.appendChild(detail);
  }
}

function upsertPieChart(mediaType, rows, fieldLabel, yearText) {
  const canvas = document.getElementById(`stats-chart-${mediaType}`);
  if (!canvas || typeof Chart === "undefined") return;

  const validRows = rows.filter((r) => Number(r?.value || 0) > 0);
  const labels = (validRows.length ? validRows : [{ label: "无数据", value: 0 }]).map((r) => String(r.label || "(未填写)"));
  const values = validRows.length ? validRows.map((r) => Number(r.value || 0)) : [1];
  const palette = monokaiPalette();
  const colors = labels.map((_, i) => palette[i % palette.length]);

  const existing = statsCharts[mediaType];
  if (existing) {
    existing.destroy();
  }

  statsCharts[mediaType] = new Chart(canvas, {
    type: "pie",
    data: {
      labels,
      datasets: [
        {
          data: values,
          backgroundColor: colors,
          borderColor: "#2f3129",
          borderWidth: 1,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        title: {
          display: true,
          text: `${formatDisplayToken(mediaType)} | ${fieldLabel}${yearText ? ` | ${yearText}` : ""}`,
          color: "#d7d7c9",
          font: { size: 12 },
        },
        legend: {
          display: false,
        },
      },
    },
  });

  renderChartLegend(mediaType, validRows, colors);
}

async function refreshStatsCharts() {
  const fieldEl = document.getElementById("stats-field");
  const yearEl = document.getElementById("stats-year");
  if (!fieldEl || !yearEl) return;

  const field = String(fieldEl.value || "category");
  const yearText = String(yearEl.value || "").trim();
  const query = yearText ? `?field=${encodeURIComponent(field)}&year=${encodeURIComponent(yearText)}` : `?field=${encodeURIComponent(field)}`;
  const payload = await apiGet(`/api/library/stats/pie${query}`);

  for (const mediaType of STATS_MEDIA_TYPES) {
    const rows = Array.isArray(payload?.charts?.[mediaType]) ? payload.charts[mediaType] : [];
    upsertPieChart(mediaType, rows, formatDisplayToken(field), yearText);
  }
}

let _chartJsLoaded = false;

async function _ensureChartJs() {
  if (typeof Chart !== "undefined" || _chartJsLoaded) return;
  await new Promise((resolve, reject) => {
    const s = document.createElement("script");
    s.src = "https://cdn.jsdelivr.net/npm/chart.js@4.4.3/dist/chart.umd.min.js";
    s.onload = () => { _chartJsLoaded = true; resolve(); };
    s.onerror = () => reject(new Error("Chart.js 加载失败"));
    document.head.appendChild(s);
  });
}

async function bootstrapStatsTab() {
  await _ensureChartJs();
  const overview = await apiGet("/api/library/stats/overview");
  renderStatsOverview(overview);
  fillStatsFieldOptions(overview?.dimension_fields || []);
  fillStatsYearOptions(overview);

  document.getElementById("stats-refresh")?.addEventListener("click", async () => {
    const refreshed = await apiGet("/api/library/stats/overview");
    renderStatsOverview(refreshed);
    fillStatsYearOptions(refreshed);
    await refreshStatsCharts();
  });

  document.getElementById("stats-field")?.addEventListener("change", () => {
    refreshStatsCharts().catch((err) => {
      console.error(err);
      alert(`刷新图表失败: ${err.message}`);
    });
  });

  document.getElementById("stats-year")?.addEventListener("change", () => {
    refreshStatsCharts().catch((err) => {
      console.error(err);
      alert(`刷新图表失败: ${err.message}`);
    });
  });

  await refreshStatsCharts();
  statsBootstrapped = true;
  _statsBootstrapping = false;
}

function selectedFilters() {
  return {
    year: filterState.year ? [filterState.year] : [],
    rating: filterState.rating ? [filterState.rating] : [],
    media_type: filterState.media_type ? [filterState.media_type] : [],
    nationality: filterState.nationality ? [filterState.nationality] : [],
    category: filterState.category ? [filterState.category] : [],
    channel: filterState.channel ? [filterState.channel] : [],
  };
}

function formatFilterSelectOption(field, value, count = null) {
  const suffix = count == null ? "" : ` (${count})`;
  if (field === "rating") return `${value} 分${suffix}`;
  return `${value}${suffix}`;
}

function buildSqlPreview(query, mode) {
  const filters = selectedFilters();
  const clauses = [];
  for (const [field, values] of Object.entries(filters)) {
    if (!values.length) continue;
    if (values.length === 1) {
      clauses.push(`${field} = '${values[0]}'`);
    } else {
      clauses.push(`${field} IN (${values.map((v) => `'${v}'`).join(", ")})`);
    }
  }
  if ((query || "").trim()) {
    if (mode === "vector") {
      clauses.push(`VECTOR_SIM(content, '${query.trim()}') > 0`);
    } else {
      clauses.push(`content LIKE '%${query.trim()}%'`);
    }
  }
  const where = clauses.length ? clauses.join(" AND ") : "1 = 1";
  return `SELECT * FROM library_items WHERE ${where} ORDER BY score DESC;`;
}

function triggerSearchFromFilter() {
  if (filterSearchTimer) {
    clearTimeout(filterSearchTimer);
  }
  filterSearchTimer = setTimeout(() => {
    runSearch(currentMode).catch((err) => {
      console.error(err);
      alert(`筛选查询失败: ${err.message}`);
    });
  }, 80);
}

async function fetchFacets(filters) {
  return apiPost("/api/library/facets", { filters });
}

async function refreshFacetsAndFilters() {
  let payload = await fetchFacets(selectedFilters());
  let changed = false;

  for (const key of Object.keys(filterState)) {
    const selected = filterState[key];
    if (!selected) continue;
    const count = ((payload.facets || {})[key] || {})[selected] || 0;
    if (count <= 0) {
      filterState[key] = null;
      changed = true;
    }
  }

  if (changed) {
    payload = await fetchFacets(selectedFilters());
  }

  renderFilters(filterMeta, payload.facets || {});
}

function renderFilters(meta, facets) {
  const map = (meta && meta.filters) || {};
  const hasFacets = !!(facets && Object.keys(facets).length);
  for (const key of SELECT_FILTER_FIELDS) {
    const select = document.getElementById(`filter-${key}-select`);
    if (!select) continue;
    const options = Array.isArray(map[key]) ? map[key] : [];
    const selected = filterState[key] || "";
    const facetCounts = (facets && facets[key]) || {};
    const previousValue = select.value;
    select.innerHTML = "";

    const allOpt = document.createElement("option");
    allOpt.value = "";
    allOpt.textContent = key === "year" ? "全部年份" : "全部评分";
    select.appendChild(allOpt);

    for (const value of options) {
      const count = hasFacets ? (facetCounts[value] || 0) : null;
      const isSelected = selected === value;
      if (hasFacets && !isSelected && Number(count || 0) <= 0) continue;
      const opt = document.createElement("option");
      opt.value = value;
      opt.textContent = formatFilterSelectOption(key, value, count);
      select.appendChild(opt);
    }

    select.value = selected;
    if (select.value !== selected) {
      filterState[key] = select.value || null;
    }
    if (!select.dataset.bound) {
      select.addEventListener("change", () => {
        filterState[key] = select.value || null;
        refreshFacetsAndFilters().catch((err) => {
          console.error(err);
        });
        triggerSearchFromFilter();
      });
      select.dataset.bound = "1";
    }
    if (!selected && previousValue && !select.value) {
      select.value = "";
    }
  }

  for (const key of CHIP_FILTER_FIELDS) {
    const box = document.getElementById(`filter-${key}`);
    if (!box) continue;
    box.innerHTML = "";
    const options = map[key] || [];
    const selected = filterState[key];
    const facetCounts = (facets && facets[key]) || {};

    const sortedOptions = [...options].sort((a, b) => {
      const ca = hasFacets ? (facetCounts[a] || 0) : 0;
      const cb = hasFacets ? (facetCounts[b] || 0) : 0;
      if (cb !== ca) return cb - ca;
      return String(a).localeCompare(String(b), "zh-CN");
    });

    const total = sortedOptions.length;
    const useCollapse = total > 15;
    const showLimit = filterExpandState[key] ? total : Math.min(total, 15);
    const visibleOptions = sortedOptions.slice(0, showLimit);

    for (const value of visibleOptions) {
      const count = hasFacets ? (facetCounts[value] || 0) : 1;
      const isSelected = selected === value;
      const shouldHide = hasFacets && !isSelected && count <= 0;
      if (shouldHide) {
        continue;
      }

      const chip = document.createElement("button");
      chip.className = "chip";
      chip.textContent = `${formatDisplayToken(value)} (${count})`;
      chip.type = "button";
      chip.classList.toggle("active", isSelected);

      // Single-select per group with direct switching: siblings are visually locked but still clickable.
      const lockSibling = !!selected && !isSelected;
      chip.classList.toggle("locked", lockSibling);

      chip.addEventListener("click", () => {
        if (filterState[key] === value) {
          filterState[key] = null;
        } else {
          filterState[key] = value;
        }
        // Apply single-select visual state immediately, then sync linked facets async.
        renderFilters(filterMeta, null);
        refreshFacetsAndFilters().catch((err) => {
          console.error(err);
        });
        triggerSearchFromFilter();
      });
      box.appendChild(chip);
    }

    const group = box.closest(".filter-group");
    if (group) {
      const oldToggle = group.querySelector(`.filter-toggle[data-key='${key}']`);
      if (oldToggle) oldToggle.remove();
      if (useCollapse) {
        const toggle = document.createElement("button");
        toggle.type = "button";
        toggle.className = "ghost filter-toggle";
        toggle.dataset.key = key;
        const hiddenCount = Math.max(0, total - 15);
        toggle.textContent = filterExpandState[key]
          ? "收起"
          : `展开更多 (${hiddenCount})`;
        toggle.addEventListener("click", () => {
          filterExpandState[key] = !filterExpandState[key];
          renderFilters(filterMeta, facets);
        });
        group.appendChild(toggle);
      }
    }
  }
}

function createDetailRow(label, value) {
  const row = document.createElement("div");
  row.className = "preview-row";

  const k = document.createElement("div");
  k.className = "preview-key";
  k.textContent = label;

  const v = document.createElement("div");
  v.className = "preview-val";
  v.textContent = value || "-";

  row.appendChild(k);
  row.appendChild(v);
  return row;
}

function renderPreviewCard(item) {
  const box = document.getElementById("preview-card");
  const legacyBox = document.getElementById("preview-json");
  if (!box && legacyBox) {
    // Backward-compatible fallback for older template container.
    legacyBox.textContent = item ? JSON.stringify(item, null, 2) : "请选择一条结果进行预览...";
    return;
  }
  if (!box) {
    return;
  }
  box.innerHTML = "";

  if (!item) {
    const empty = document.createElement("div");
    empty.className = "preview-empty";
    empty.textContent = "请选择一条结果进行预览...";
    box.appendChild(empty);
    return;
  }

  const title = document.createElement("h3");
  title.className = "preview-title";
  title.textContent = item.title || "(无标题)";

  const chips = document.createElement("div");
  chips.className = "preview-chips";
  for (const text of [item.media_type, item.category, item.channel].filter(Boolean)) {
    const chip = document.createElement("span");
    chip.className = "preview-chip";
    chip.textContent = formatDisplayToken(text);
    chips.appendChild(chip);
  }

  const ratingWrap = document.createElement("div");
  ratingWrap.className = "preview-rating";
  const rating = Number(item.rating);
  const hasRating = Number.isFinite(rating);
  ratingWrap.textContent = hasRating ? `★ ${rating.toFixed(1)}` : "★ -";

  const top = document.createElement("div");
  top.className = "preview-top";

  const cover = document.createElement("div");
  cover.className = "preview-cover";
  if (String(item.media_type || "").trim().toLowerCase() === "music") {
    cover.classList.add("preview-cover-square");
  }
  const coverImg = document.createElement("img");
  coverImg.className = "preview-cover-img";
  const coverPlaceholder = document.createElement("div");
  coverPlaceholder.className = "preview-cover-placeholder";
  coverPlaceholder.textContent = "No Cover";
  const coverPath = (item.cover_path || "").trim();
  if (coverPath) {
    coverImg.src = `/media/${coverPath}`;
    coverImg.alt = `${item.title || "条目"} 封面`;
    coverImg.style.cursor = "zoom-in";
    coverImg.addEventListener("click", (e) => {
      e.stopPropagation();
      openPreviewImageViewer(coverImg.src, coverImg.alt);
    });
    cover.appendChild(coverImg);
  } else {
    cover.appendChild(coverPlaceholder);
  }

  const left = document.createElement("div");
  left.className = "preview-top-left";
  left.appendChild(title);
  left.appendChild(chips);
  top.appendChild(cover);
  top.appendChild(left);
  top.appendChild(ratingWrap);

  const grid = document.createElement("div");
  grid.className = "preview-grid";
  grid.appendChild(createDetailRow("作者", item.author));
  grid.appendChild(createDetailRow("国家", item.nationality));
  grid.appendChild(createDetailRow("日期", item.date || [item.year, item.month, item.day].filter((v) => v != null && v !== "").join("-")));
  grid.appendChild(createDetailRow("出版", item.publisher));

  const reviewBlock = document.createElement("div");
  reviewBlock.className = "preview-review";
  const reviewTitle = document.createElement("h4");
  reviewTitle.textContent = "短评";
  const reviewBody = document.createElement("p");
  reviewBody.textContent = item.review || "暂无内容";
  reviewBlock.appendChild(reviewTitle);
  reviewBlock.appendChild(reviewBody);

  box.appendChild(top);
  box.appendChild(grid);
  box.appendChild(reviewBlock);

  if (item.url) {
    const link = document.createElement("a");
    link.className = "preview-link";
    link.href = item.url;
    link.target = "_blank";
    link.rel = "noopener noreferrer";
    link.textContent = "查看原始链接";
    box.appendChild(link);
  }
}

function pickItem(item) {
  renderPreviewCard(item);
  const modal = document.getElementById("preview-modal");
  if (modal) {
    modal.classList.remove("hidden");
    modal.setAttribute("aria-hidden", "false");
  }
}

function closePreviewModal() {
  const modal = document.getElementById("preview-modal");
  if (!modal) return;
  modal.classList.add("hidden");
  modal.setAttribute("aria-hidden", "true");
}

function parsePreviewItemIdFromLocation() {
  const search = String(window.location.search || "").trim();
  if (search.startsWith("?")) {
    const queryParams = new URLSearchParams(search.slice(1));
    const queryItemId = String(queryParams.get("item") || "").trim();
    if (queryItemId) return queryItemId;
  }

  const hash = String(window.location.hash || "").trim();
  if (!hash || !hash.startsWith("#")) return "";
  const payload = hash.slice(1);
  const params = new URLSearchParams(payload);
  const itemId = String(params.get("item") || "").trim();
  return itemId;
}

async function tryOpenPreviewItemFromLocation() {
  const itemId = parsePreviewItemIdFromLocation();
  if (!itemId) return;
  try {
    const data = await apiGet(`/api/library/item/${encodeURIComponent(itemId)}`);
    const item = data && data.item ? data.item : null;
    if (item) {
      pickItem(item);
    }
  } catch (_err) {
    // Ignore malformed/missing item id in URL hash.
  }
}

function showToast(message) {
  let toast = document.getElementById("floating-toast");
  if (!toast) {
    toast = document.createElement("div");
    toast.id = "floating-toast";
    toast.className = "floating-toast hidden";
    document.body.appendChild(toast);
  }
  toast.textContent = message;
  toast.classList.remove("hidden");
  if (toastTimer) {
    clearTimeout(toastTimer);
  }
  toastTimer = setTimeout(() => {
    toast.classList.add("hidden");
  }, 1000);
}

function previewByIndex(index) {
  if (!Array.isArray(latestResults) || !latestResults.length) return;
  if (index < 0) {
    showToast("已经到顶");
    return;
  }
  if (index >= latestResults.length) {
    showToast("已经到底");
    return;
  }
  currentPreviewIndex = index;
  pickItem(latestResults[index]);
}

function escapeHtml(text) {
  return String(text || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function highlightSnippet(snippet, term) {
  const src = String(snippet || "");
  const keyword = String(term || "");
  if (!keyword) return escapeHtml(src);
  const srcLower = src.toLowerCase();
  const kwLower = keyword.toLowerCase();
  const idx = srcLower.indexOf(kwLower);
  if (idx < 0) return escapeHtml(src);
  const before = escapeHtml(src.slice(0, idx));
  const match = escapeHtml(src.slice(idx, idx + keyword.length));
  const after = escapeHtml(src.slice(idx + keyword.length));
  return `${before}<mark>${match}</mark>${after}`;
}

function buildLocalKeywordHits(item, query, contextChars = 5) {
  const q = String(query || "").trim();
  if (!q) return [];
  const qLower = q.toLowerCase();
  const tokens = qLower.split(/\s+/).filter(Boolean);
  const fields = ["title", "author", "nationality", "category", "channel", "review", "publisher", "url"];
  const hits = [];

  for (const field of fields) {
    const raw = String((item && item[field]) || "");
    if (!raw) continue;
    const lower = raw.toLowerCase();

    let idx = lower.indexOf(qLower);
    let term = "";
    let termLen = q.length;
    if (idx >= 0) {
      term = raw.slice(idx, idx + termLen);
    } else {
      const token = tokens.sort((a, b) => b.length - a.length).find((t) => lower.includes(t));
      if (!token) continue;
      idx = lower.indexOf(token);
      termLen = token.length;
      term = raw.slice(idx, idx + termLen);
    }

    const start = Math.max(0, idx - contextChars);
    const end = Math.min(raw.length, idx + termLen + contextChars);
    let snippet = raw.slice(start, end);
    if (start > 0) snippet = `...${snippet}`;
    if (end < raw.length) snippet = `${snippet}...`;

    hits.push({ field, snippet, term });
  }

  return hits;
}

function renderResults(payload) {
  latestResults = payload.results || [];
  const list = document.getElementById("result-list");
  const meta = document.getElementById("result-meta");
  const pagination = document.getElementById("result-pagination");
  const firstBtn = document.getElementById("result-page-first");
  const pageInfo = document.getElementById("result-page-info");
  const prevBtn = document.getElementById("result-page-prev");
  const nextBtn = document.getElementById("result-page-next");
  const lastBtn = document.getElementById("result-page-last");
  const queryText = String((payload && payload.query) || "");
  const isKeywordMode = String((payload && payload.mode) || "") === "keyword";
  const totalCount = Math.max(0, Number(payload?.total_count ?? payload?.count ?? latestResults.length) || 0);
  const offset = Math.max(0, Number(payload?.offset ?? currentSearchOffset) || 0);
  const limit = Math.max(1, Number(payload?.limit ?? currentSearchLimit) || 1);
  currentSearchOffset = offset;
  currentSearchLimit = limit;
  currentSearchTotal = totalCount;
  list.innerHTML = "";
  const rangeStart = totalCount > 0 ? offset + 1 : 0;
  const rangeEnd = totalCount > 0 ? offset + latestResults.length : 0;
  const page = Math.floor(offset / limit) + 1;
  const pageCount = Math.max(1, Math.ceil(totalCount / limit));
  const lastOffset = Math.max(0, (pageCount - 1) * limit);
  meta.textContent = `模式: ${payload.mode} | 总命中: ${totalCount} | 当前: ${rangeStart}-${rangeEnd}`;
  if (pagination) pagination.classList.toggle("hidden", totalCount <= limit);
  if (pageInfo) {
    pageInfo.textContent = `${page} / ${pageCount}`;
  }
  if (firstBtn) firstBtn.disabled = offset <= 0;
  if (prevBtn) prevBtn.disabled = offset <= 0;
  if (nextBtn) nextBtn.disabled = offset + latestResults.length >= totalCount;
  if (lastBtn) lastBtn.disabled = offset >= lastOffset;

  if (!latestResults.length) {
    list.innerHTML = "<div class='hint'>没有匹配结果</div>";
    return;
  }

  for (let i = 0; i < latestResults.length; i += 1) {
    const item = latestResults[i];
    const card = document.createElement("div");
    card.className = "card";
    if (item.id) card.dataset.itemId = String(item.id);
    const cardCoverPath = String(item.cover_path || "").trim();
    const cardCoverUrl = cardCoverPath ? `/media/${encodeURI(cardCoverPath)}` : "";
    const thumbHtml = cardCoverUrl
      ? `<img class="card-thumb-img" src="${cardCoverUrl}" alt="${escapeHtml(item.title || "条目")} 封面" loading="lazy" />`
      : `<div class="card-thumb-placeholder">No Cover</div>`;
    card.innerHTML = `
      <div class="card-head">
        <div class="card-thumb">${thumbHtml}</div>
        <div class="card-main">
          <h4>${item.title || "(无标题)"}</h4>
          <div class="meta">${formatDisplayToken(item.media_type || "")} | ${item.author || ""} | 评分 ${item.rating ?? "-"}</div>
        </div>
        <div class="card-actions row">
          <button data-action="edit">修改</button>
        </div>
      </div>
    `;
    card.addEventListener("pointerdown", (e) => {
      cardPointerState.set(card, { x: e.clientX, y: e.clientY, moved: false });
    });
    card.addEventListener("pointermove", (e) => {
      const state = cardPointerState.get(card);
      if (!state) return;
      const dx = Math.abs(e.clientX - state.x);
      const dy = Math.abs(e.clientY - state.y);
      if (dx > 6 || dy > 6) state.moved = true;
    });
    card.addEventListener("click", (e) => {
      const target = e.target;
      if (target && typeof target.closest === "function" && target.closest("button,a,input,textarea,select,label")) {
        return;
      }
      const selection = typeof window.getSelection === "function" ? window.getSelection() : null;
      if (selection && String(selection.toString() || "").trim()) {
        return;
      }
      const state = cardPointerState.get(card);
      if (state && state.moved) {
        return;
      }
      previewByIndex(i);
    });
    const editBtn = card.querySelector("[data-action='edit']");
    editBtn.addEventListener("click", (e) => {
      e.stopPropagation();
      openEditDialog(item);
    });

    let hits = Array.isArray(item.keyword_hits) ? item.keyword_hits : [];
    if ((!hits || !hits.length) && isKeywordMode && queryText.trim()) {
      hits = buildLocalKeywordHits(item, queryText);
    }
    if (hits.length) {
      const hitWrap = document.createElement("div");
      hitWrap.className = "hit-tags";
      for (const hit of hits) {
        if (!hit || !hit.field || !hit.snippet) continue;
        const tag = document.createElement("span");
        tag.className = "hit-tag";
        tag.innerHTML = `<strong>${escapeHtml(formatFieldLabel(hit.field))}</strong>: ${highlightSnippet(hit.snippet, hit.term || "")}`;
        hitWrap.appendChild(tag);
      }
      if (hitWrap.childNodes.length) {
        card.appendChild(hitWrap);
      }
    }
    list.appendChild(card);
  }
}

function normalizeNumber(value) {
  const text = String(value || "").trim();
  if (!text) return null;
  const n = Number(text);
  return Number.isFinite(n) ? n : null;
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function renderRatingVisual(value) {
  const fill = document.getElementById("rating-stars-fill");
  const text = document.getElementById("rating-text");
  const stars = document.getElementById("rating-stars");
  const normalized = value == null ? null : clamp(Number(value), 0, 10);
  const visualValue = Number.isFinite(normalized) ? normalized : null;
  const percent = visualValue == null ? 0 : (visualValue / 10) * 100;
  if (fill) {
    fill.style.width = `${percent}%`;
  }
  if (stars) {
    stars.setAttribute("aria-valuenow", String(visualValue == null ? 0 : visualValue));
  }
  if (text) {
    text.textContent = visualValue == null ? "未评分" : `${visualValue.toFixed(1)} / 10`;
  }
}

function setRatingValue(value) {
  const input = document.getElementById("rating-input");
  const normalized = value == null ? null : clamp(Number(value), 0, 10);
  currentRating = Number.isFinite(normalized) ? normalized : null;
  if (input) {
    input.value = currentRating == null ? "" : String(currentRating);
  }
  renderRatingVisual(currentRating);
}

function ratingFromPointer(event, element) {
  const rect = element.getBoundingClientRect();
  const x = clamp(event.clientX - rect.left, 0, rect.width);
  const starValue = (x / Math.max(rect.width, 1)) * 5;
  const halfStar = Math.round(starValue * 2) / 2;
  return clamp(halfStar * 2, 0, 10);
}

function initRatingControl() {
  const stars = document.getElementById("rating-stars");
  if (!stars) return;

  let pointerDown = false;

  stars.addEventListener("pointerdown", (e) => {
    pointerDown = true;
    if (typeof stars.setPointerCapture === "function") {
      stars.setPointerCapture(e.pointerId);
    }
    setRatingValue(ratingFromPointer(e, stars));
  });

  stars.addEventListener("pointermove", (e) => {
    const preview = ratingFromPointer(e, stars);
    if (pointerDown) {
      setRatingValue(preview);
      return;
    }
    renderRatingVisual(preview);
  });

  const clearPointerState = (e) => {
    pointerDown = false;
    if (e && typeof stars.releasePointerCapture === "function") {
      try {
        stars.releasePointerCapture(e.pointerId);
      } catch (_err) {
        // Ignore release errors when capture is already gone.
      }
    }
  };

  stars.addEventListener("pointerup", clearPointerState);
  stars.addEventListener("pointercancel", clearPointerState);
  stars.addEventListener("pointerleave", (e) => {
    renderRatingVisual(currentRating);
    if (e.buttons === 0) {
      clearPointerState(e);
    }
  });

  stars.addEventListener("keydown", (e) => {
    if (e.key === "ArrowLeft" || e.key === "ArrowDown") {
      e.preventDefault();
      const base = currentRating == null ? 0 : currentRating;
      setRatingValue(clamp(base - 1, 0, 10));
      return;
    }
    if (e.key === "ArrowRight" || e.key === "ArrowUp") {
      e.preventDefault();
      const base = currentRating == null ? 0 : currentRating;
      setRatingValue(clamp(base + 1, 0, 10));
      return;
    }
    if (e.key === "Backspace" || e.key === "Delete" || e.key === "0") {
      e.preventDefault();
      setRatingValue(null);
    }
  });
}

function openEditDialog(item) {
  editingItemId = item.id || "";
  editingCoverPath = item && item.cover_path ? String(item.cover_path) : null;
  document.getElementById("dialog-title").textContent = editingItemId ? "修改条目" : "新增条目";
  const form = document.getElementById("item-form");
  const fields = ["media_type", "date", "title", "author", "nationality", "category", "channel", "publisher", "url", "review"];
  for (const key of fields) {
    const el = form.elements.namedItem(key);
    if (!el) continue;
    if (key === "date") {
      el.value = normalizeDateInput(item && item.date ? item.date : "") || "";
    } else {
      el.value = item && item[key] != null ? String(item[key]) : "";
    }
  }
  const coverFileEl = form.elements.namedItem("cover_file");
  if (coverFileEl) {
    coverFileEl.value = "";
  }
  setRatingValue(item && item.rating != null ? Number(item.rating) : null);
  const dialog = document.getElementById("item-dialog");
  if (dialog) {
    dialog.setAttribute("tabindex", "-1");
    dialog.showModal();
    setDialogOpenState(true);
    const cancelBtn = document.getElementById("btn-cancel");
    if (cancelBtn) {
      cancelBtn.focus({ preventScroll: true });
    } else {
      dialog.focus({ preventScroll: true });
    }

    // Some mobile browsers may auto-focus the first input after showModal().
    // Force focus back to a non-input target to avoid opening the soft keyboard.
    setTimeout(() => {
      const active = document.activeElement;
      const tag = active && active.tagName ? String(active.tagName).toLowerCase() : "";
      if (tag === "input" || tag === "textarea" || tag === "select") {
        active.blur();
      }
      if (cancelBtn) {
        cancelBtn.focus({ preventScroll: true });
      } else {
        dialog.focus({ preventScroll: true });
      }
    }, 0);
  }
}

async function saveDialog() {
  const form = document.getElementById("item-form");
  const normalizedDate = normalizeDateInput(form.elements.namedItem("date").value);
  const payload = {
    media_type: form.elements.namedItem("media_type").value,
    date: normalizedDate,
    title: form.elements.namedItem("title").value,
    author: form.elements.namedItem("author").value || null,
    nationality: form.elements.namedItem("nationality").value || null,
    category: form.elements.namedItem("category").value || null,
    channel: form.elements.namedItem("channel").value || null,
    publisher: form.elements.namedItem("publisher").value || null,
    rating: normalizeNumber(form.elements.namedItem("rating").value),
    review: form.elements.namedItem("review").value || null,
    url: form.elements.namedItem("url").value || null,
    cover_path: editingCoverPath,
    embedding: null,
  };

  const coverFileEl = form.elements.namedItem("cover_file");
  const selectedFile = coverFileEl && coverFileEl.files && coverFileEl.files[0] ? coverFileEl.files[0] : null;
  if (selectedFile) {
    const uploaded = await uploadCoverFile(selectedFile, payload.title, editingCoverPath);
    payload.cover_path = uploaded.path || payload.cover_path;
  }

  if (!payload.title || !payload.media_type) {
    alert("Title 和 Media Type 不能为空");
    return;
  }

  let savedItem = null;
  if (editingItemId) {
    const resp = await apiPut(`/api/library/item/${editingItemId}`, { item: payload });
    savedItem = resp && resp.item ? resp.item : null;
  } else {
    const resp = await apiPost("/api/library/item", { item: payload });
    savedItem = resp && resp.item ? resp.item : null;
  }

  const scrollTargetId = editingItemId || "";
  const savedOffset = currentSearchOffset;
  // Capture scroll position before re-render so we can restore it exactly —
  // the same reason cancel works perfectly: it never re-renders the list at all.
  const mainPanel = document.querySelector(".main-panel");
  const savedScrollTop = mainPanel ? mainPanel.scrollTop : 0;
  showToast("保存成功");
  document.getElementById("item-dialog").close();
  await runSearch(currentMode, { offset: savedOffset });
  // Restore scroll position first; only scroll to the card if it ended up
  // outside the visible area (e.g. the card grew/shrank and pushed it off-screen).
  if (mainPanel) mainPanel.scrollTop = savedScrollTop;
  if (scrollTargetId) {
    const targetCard = document.querySelector(`[data-item-id="${scrollTargetId}"]`);
    if (targetCard) {
      const panelRect = mainPanel ? mainPanel.getBoundingClientRect() : null;
      const cardRect = targetCard.getBoundingClientRect();
      const visible = panelRect
        ? cardRect.top >= panelRect.top && cardRect.bottom <= panelRect.bottom
        : cardRect.top >= 0 && cardRect.bottom <= window.innerHeight;
      if (!visible) targetCard.scrollIntoView({ block: "nearest", behavior: "instant" });
    }
  }
  const [metaPayload, suggestionPayload] = await Promise.all([
    apiGet("/api/library/meta"),
    apiGet("/api/library/suggestions"),
  ]);
  filterMeta = metaPayload;
  formSuggestionMeta = normalizeSuggestionPayload(suggestionPayload);
  await refreshFacetsAndFilters();

  if (savedItem && savedItem.id) {
    apiPost("/api/library/embedding/refresh-items", { item_ids: [String(savedItem.id)] })
      .then((refreshResult) => {
        const refreshed = Number(refreshResult?.refreshed || 0);
        const graphItems = Number(refreshResult?.graph?.items_added || 0);
        if (refreshed > 0) {
          showToast(`该条目向量已刷新，图谱新增节点关联 ${graphItems}`);
        }
      })
      .catch((err) => {
        console.error(err);
        showToast("向量刷新失败，可稍后点刷新");
      });
  }
}

async function runSearch(mode, options = {}) {
  currentMode = mode;
  const query = document.getElementById("query-input").value || "";
  const offset = Number.isFinite(Number(options?.offset)) ? Math.max(0, Number(options.offset)) : 0;

  // Cancel any in-flight request and bump the serial so stale results are dropped.
  if (_searchAbortController) {
    _searchAbortController.abort();
  }
  _searchAbortController = new AbortController();
  const { signal } = _searchAbortController;
  const serial = ++_searchSerial;

  // Disable pagination buttons for the duration of this request.
  const _pageIds = ["result-page-first", "result-page-prev", "result-page-next", "result-page-last"];
  _pageIds.forEach((id) => { const el = document.getElementById(id); if (el) el.disabled = true; });

  try {
    const payload = await apiPost("/api/library/search", {
      query,
      mode,
      limit: currentSearchLimit,
      offset,
      filters: selectedFilters(),
    }, { signal });
    if (serial !== _searchSerial) return; // stale — a newer request has already fired
    renderResults(payload);
  } catch (err) {
    if (err && err.name === "AbortError") return; // cancelled intentionally
    if (serial !== _searchSerial) return; // stale error, ignore
    // Re-enable so the user can retry.
    _pageIds.forEach((id) => { const el = document.getElementById(id); if (el) el.disabled = false; });
    throw err;
  }
}

async function refreshPendingEmbeddings() {
  const btn = document.getElementById("btn-refresh-embedding");
  if (btn) btn.disabled = true;
  try {
    const result = await apiPost("/api/library/embedding/refresh", {});
    const refreshed = Number(result?.refreshed || 0);
    const scanned = Number(result?.scanned || 0);
    const failed = Number(result?.failed || 0);
    const graphItems = Number(result?.graph?.items_added || 0);
    const graphNodes = Number(result?.graph?.nodes_added || 0);
    const graphEdges = Number(result?.graph?.edges_added || 0);
    showToast(`Embedding刷新完成：${refreshed}/${scanned}，失败 ${failed}；图谱补齐条目 ${graphItems}，节点 ${graphNodes}，边 ${graphEdges}`);
    if (currentMode === "vector") {
      await runSearch("vector");
    }
  } catch (err) {
    console.error(err);
    alert(`刷新 embedding 失败: ${err.message}`);
  } finally {
    if (btn) btn.disabled = false;
  }
}

async function bootstrap() {
  initRatingControl();
  setupFormAutocomplete();
  document.querySelectorAll(".tab").forEach((tabBtn) => {
    tabBtn.addEventListener("click", () => {
      const name = String(tabBtn.dataset.tab || "query");
      setTab(name);
    });
  });
  document.getElementById("btn-keyword").addEventListener("click", () => runSearch("keyword"));
  document.getElementById("btn-vector").addEventListener("click", () => runSearch("vector"));
  document.getElementById("result-page-first")?.addEventListener("click", () => {
    if (currentSearchOffset <= 0) return;
    runSearch(currentMode, { offset: 0 }).catch((err) => {
      console.error(err);
      alert(`翻页失败: ${err.message}`);
    });
  });
  document.getElementById("result-page-prev")?.addEventListener("click", () => {
    if (currentSearchOffset <= 0) return;
    runSearch(currentMode, { offset: Math.max(0, currentSearchOffset - currentSearchLimit) }).catch((err) => {
      console.error(err);
      alert(`翻页失败: ${err.message}`);
    });
  });
  document.getElementById("result-page-next")?.addEventListener("click", () => {
    const nextOffset = currentSearchOffset + currentSearchLimit;
    if (nextOffset >= currentSearchTotal) return;
    runSearch(currentMode, { offset: nextOffset }).catch((err) => {
      console.error(err);
      alert(`翻页失败: ${err.message}`);
    });
  });
  document.getElementById("result-page-last")?.addEventListener("click", () => {
    const pageCount = Math.max(1, Math.ceil(currentSearchTotal / currentSearchLimit));
    const lastOffset = Math.max(0, (pageCount - 1) * currentSearchLimit);
    if (currentSearchOffset >= lastOffset) return;
    runSearch(currentMode, { offset: lastOffset }).catch((err) => {
      console.error(err);
      alert(`翻页失败: ${err.message}`);
    });
  });
  document.getElementById("btn-refresh-embedding").addEventListener("click", () => {
    refreshPendingEmbeddings().catch((err) => {
      console.error(err);
      alert(`刷新 embedding 失败: ${err.message}`);
    });
  });
  document.getElementById("btn-add").addEventListener("click", () => {
    editingItemId = "";
    openEditDialog({ media_type: "book" });
  });

  document.getElementById("query-input").addEventListener("keydown", (e) => {
    if (e.key === "Enter") {
      e.preventDefault();
      runSearch("keyword").catch((err) => {
        console.error(err);
        alert(`查询失败: ${err.message}`);
      });
    }
  });

  const form = document.getElementById("item-form");
  const dateInput = form.elements.namedItem("date");
  if (dateInput) {
    dateInput.addEventListener("blur", () => {
      const normalized = normalizeDateInput(dateInput.value || "");
      if (normalized) {
        dateInput.value = normalized;
      }
    });
  }

  document.getElementById("btn-cancel").addEventListener("click", () => {
    document.getElementById("item-dialog").close();
  });

  const previewModal = document.getElementById("preview-modal");
  if (previewModal) {
    previewModal.addEventListener("click", (e) => {
      if (e.target && e.target.dataset && e.target.dataset.role === "backdrop") {
        closePreviewModal();
      }
    });
  }

  const previewCloseBtn = document.getElementById("preview-close-btn");
  if (previewCloseBtn) {
    previewCloseBtn.addEventListener("click", () => {
      closePreviewModal();
    });
  }

  const coverUploadZone = document.getElementById("cover-upload-zone");
  const coverFileInput = document.getElementById("cover-file-input");
  const coverSelectBtn = document.getElementById("cover-select-btn");
  if (coverSelectBtn && coverFileInput) {
    coverSelectBtn.addEventListener("click", () => {
      coverFileInput.click();
    });
  }
  if (coverUploadZone && coverFileInput) {
    coverUploadZone.addEventListener("click", (e) => {
      if (e.target === coverUploadZone || e.target.classList.contains("cover-upload-hint")) {
        coverUploadZone.focus();
      }
    });
    coverUploadZone.addEventListener("dragover", (e) => {
      e.preventDefault();
      coverUploadZone.classList.add("drag-over");
    });
    coverUploadZone.addEventListener("dragleave", (e) => {
      e.preventDefault();
      coverUploadZone.classList.remove("drag-over");
    });
    coverUploadZone.addEventListener("drop", async (e) => {
      e.preventDefault();
      coverUploadZone.classList.remove("drag-over");
      const files = e.dataTransfer?.files;
      if (files && files.length > 0) {
        const file = files[0];
        if (file.type.startsWith("image/")) {
          try {
            const form = document.getElementById("item-form");
            const titleInput = form?.elements.namedItem("title");
            const title = titleInput?.value || "";
            const uploaded = await uploadCoverFile(file, title, editingCoverPath);
            editingCoverPath = uploaded.path || editingCoverPath;
            showToast(`封面上传成功: ${file.name}`);
          } catch (err) {
            console.error(err);
            alert(`上传失败: ${err.message}`);
          }
        } else {
          alert("请上传图片文件");
        }
      }
    });
  }

  const itemDialog = document.getElementById("item-dialog");
  if (itemDialog) {
    itemDialog.addEventListener("close", () => {
      setDialogOpenState(false);
    });

    itemDialog.addEventListener("paste", async (e) => {
      const items = e.clipboardData?.items;
      if (!items) return;
      for (const item of items) {
        if (item.type.startsWith("image/")) {
          e.preventDefault();
          const file = item.getAsFile();
          if (file) {
            try {
              const form = document.getElementById("item-form");
              const titleInput = form?.elements.namedItem("title");
              const title = titleInput?.value || "";
              const uploaded = await uploadCoverFile(file, title, editingCoverPath);
              editingCoverPath = uploaded.path || editingCoverPath;
              showToast(`封面粘贴上传成功`);
            } catch (err) {
              console.error(err);
              alert(`上传失败: ${err.message}`);
            }
          }
          break;
        }
      }
    });
  }

  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape" && isPreviewImageViewerOpen()) {
      e.preventDefault();
      closePreviewImageViewer();
      return;
    }

    const modal = document.getElementById("preview-modal");
    const isOpen = modal && !modal.classList.contains("hidden");
    if (!isOpen) return;
    if (e.key === "ArrowLeft" || e.key === "ArrowUp") {
      e.preventDefault();
      previewByIndex(currentPreviewIndex - 1);
      return;
    }
    if (e.key === "ArrowRight" || e.key === "ArrowDown") {
      e.preventDefault();
      previewByIndex(currentPreviewIndex + 1);
      return;
    }
    if (e.key === "Escape") {
      e.preventDefault();
      closePreviewModal();
    }
  });

  document.getElementById("item-form").addEventListener("submit", async (e) => {
    e.preventDefault();
    await saveDialog();
  });

  // Single bootstrap call replaces the sequential /meta + /suggestions + /facets + /search
  // that used to fire 4 separate full-scan requests on every cold start.
  let bootstrapData;
  try {
    bootstrapData = await apiGet("/api/library/bootstrap?limit=50");
  } catch (err) {
    console.error("bootstrap failed, falling back:", err);
    // Fallback: individual requests so the page still works if the endpoint is missing
    const [metaPayload, suggestionPayload] = await Promise.all([
      apiGet("/api/library/meta"),
      apiGet("/api/library/suggestions"),
    ]);
    filterMeta = metaPayload;
    formSuggestionMeta = normalizeSuggestionPayload(suggestionPayload);
    try {
      await refreshFacetsAndFilters();
    } catch (e) {
      console.error(e);
      renderFilters(filterMeta, null);
    }
    await runSearch("keyword");
    await tryOpenPreviewItemFromLocation();
    window.addEventListener("hashchange", () => {
      tryOpenPreviewItemFromLocation().catch(() => {});
    });
    return;
  }

  // Unpack the combined bootstrap payload
  filterMeta = { filters: bootstrapData.filter_options || {} };
  formSuggestionMeta = normalizeSuggestionPayload({ fields: bootstrapData.suggestions || {} });
  // Render filters immediately using the returned facets (no extra round-trip needed)
  renderFilters(filterMeta, bootstrapData.facets || {});
  // Use the pre-fetched first-page results directly
  if (bootstrapData.initial_search) {
    renderResults(bootstrapData.initial_search);
  }
  await tryOpenPreviewItemFromLocation();
  window.addEventListener("hashchange", () => {
    tryOpenPreviewItemFromLocation().catch(() => {});
  });
}

bootstrap().catch((err) => {
  console.error(err);
  alert(`初始化失败: ${err.message}`);
});
