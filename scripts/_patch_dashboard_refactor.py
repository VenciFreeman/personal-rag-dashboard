"""Patch script: refactor Dashboard JS bootstrap state machine.

Changes applied to nav_dashboard/web/static/app.js:
  1. Replace dashboardBootstrapped with dashboardInitialPainted + dashboardRefreshInFlight
  2. Extract paintDashboardFromData() shared renderer
  3. Slim down refreshDashboard() to just fetch + paint
  4. Add dashboardRefreshInFlight flag tracking in refreshDashboard
  5. Rewrite bootstrapDashboardTab() to paint prefill immediately (non-blocking)
  6. Update tab-click guard from !dashboardBootstrapped → !dashboardInitialPainted
  7. Add console.warn to swallowed benchmark exceptions
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
APP_JS = ROOT / "nav_dashboard" / "web" / "static" / "app.js"


def apply(content: str) -> str:
    # ── 1. State flags ────────────────────────────────────────────────────────
    old = "let dashboardBootstrapped = false;\n"
    new = "let dashboardInitialPainted = false;\nlet dashboardRefreshInFlight = false;\n"
    assert old in content, "FAIL: state flag 'dashboardBootstrapped' not found"
    content = content.replace(old, new, 1)

    # ── 2. Slim refreshDashboard inner try-block → paintDashboardFromData call ─
    # Find the unique signature just before the rendering block
    FETCH_LINE = "      ? \"/api/dashboard/overview/core\"\n      : force ? \"/api/dashboard/overview?force=true\" : \"/api/dashboard/overview\";\n    const data = await apiGet(url);\n"
    CATCH_LINE = "  } catch (err) {\n    renderDashboardError(err);\n    if (dashboardGeneratedAt) dashboardGeneratedAt.textContent = coreOnly ? \"\u6838\u5fc3\u6570\u636e\u52a0\u8f7d\u5931\u8d25\" : \"\u7edf\u8ba1\u52a0\u8f7d\u5931\u8d25\";\n  }"

    fetch_pos = content.index(FETCH_LINE)
    inner_start = fetch_pos + len(FETCH_LINE)   # right after `const data = await apiGet(url);`
    catch_pos = content.index(CATCH_LINE, inner_start)

    # Extract the old rendering block (we'll use it to build paintDashboardFromData)
    inner_block = content[inner_start:catch_pos]

    # Build paintDashboardFromData with 2-space-indented body (dedent from 4→2)
    paint_lines = []
    for line in inner_block.split("\n"):
        if line.startswith("    "):
            paint_lines.append(line[2:])   # remove 2 leading spaces
        else:
            paint_lines.append(line)
    paint_body = "\n".join(paint_lines)

    paint_fn = (
        "// ── paintDashboardFromData ─────────────────────────────────────────────────\n"
        "// Single source of truth for turning a data dict (prefill or network fetch)\n"
        "// into visible DOM content. Both bootstrapDashboardTab() and refreshDashboard()\n"
        "// call this so prefill + fresh data produce an identical layout.\n"
        f"function paintDashboardFromData(data, {{ skipTaskCenter = false }} = {{}}) {{\n"
        f"{paint_body}"
        "}\n\n"
    )

    # Replace old rendering block with single function call
    new_inner = "    paintDashboardFromData(data, { skipTaskCenter });\n"
    content = content[:inner_start] + new_inner + content[catch_pos:]

    # Insert paintDashboardFromData just before refreshDashboard
    RD_SIG = "async function refreshDashboard("
    rd_pos = content.index(RD_SIG)
    content = content[:rd_pos] + paint_fn + content[rd_pos:]

    # ── 3. Add dashboardRefreshInFlight tracking to refreshDashboard ──────────
    old = "  if (!coreOnly && dashboardRefreshBtn) dashboardRefreshBtn.disabled = true;\n  try {"
    new = "  if (!coreOnly && dashboardRefreshBtn) dashboardRefreshBtn.disabled = true;\n  dashboardRefreshInFlight = true;\n  try {"
    assert old in content, "FAIL: refreshBtn.disabled pattern not found"
    content = content.replace(old, new, 1)

    old = "  } finally {\n    if (!coreOnly && dashboardRefreshBtn) dashboardRefreshBtn.disabled = false;\n  }\n}\n\nasync function bootstrapDashboardTab()"
    new = "  } finally {\n    dashboardRefreshInFlight = false;\n    if (!coreOnly && dashboardRefreshBtn) dashboardRefreshBtn.disabled = false;\n  }\n}\n\nasync function bootstrapDashboardTab()"
    assert old in content, "FAIL: finally block pattern not found"
    content = content.replace(old, new, 1)

    # ── 4. Rewrite bootstrapDashboardTab ─────────────────────────────────────
    old_bootstrap = (
        "async function bootstrapDashboardTab() {\n"
        "  _registerDashboardHandlers();\n"
        "  // Mark bootstrapped immediately so the tab is interactive regardless of\n"
        "  // whether the network requests below succeed or fail.\n"
        "  dashboardBootstrapped = true;\n"
        "  const prefill = loadDashboardPrefill();\n"
        "  if (prefill) {\n"
        "    renderDashboardLatencyTable(prefill);\n"
        "    renderDashboardObservabilityTable(prefill);\n"
        "    renderDashboardStartupLogs(prefill);\n"
        "  }\n"
        "  // Step 1: paint cards fast with core counters (15-second cache, skips heavy stats)\n"
        "  await refreshDashboard({ coreOnly: true });\n"
        "  // Step 2: fire the full detail load + task center in the background.\n"
        "  refreshDashboard({ force: false }).catch((err) => { console.warn(\"[dashboard] detail refresh failed:\", err); });\n"
        "  refreshTaskCenter().catch((err) => { console.warn(\"[dashboard] task center refresh failed:\", err); });\n"
        "  startTaskCenterPolling();\n"
        "  scheduleStartupPollingIfNeeded();\n"
        "}"
    )
    new_bootstrap = (
        "async function bootstrapDashboardTab() {\n"
        "  // Register event handlers exactly once (idempotent guard inside).\n"
        "  _registerDashboardHandlers();\n"
        "  // Paint from serialised prefill immediately for instant visible content;\n"
        "  // dashboardInitialPainted guards against re-entry while a refresh is in-flight.\n"
        "  if (!dashboardInitialPainted) {\n"
        "    try {\n"
        "      const prefill = loadDashboardPrefill();\n"
        "      if (prefill) {\n"
        "        paintDashboardFromData(prefill, { skipTaskCenter: true });\n"
        "      }\n"
        "    } catch (paintErr) {\n"
        "      console.warn(\"[dashboard] prefill paint failed:\", paintErr);\n"
        "    }\n"
        "    // Set unconditionally so the flag transitions even with empty prefill;\n"
        "    // the background refresh below will fill in real data.\n"
        "    dashboardInitialPainted = true;\n"
        "    try { if (window.__navBoot) window.__navBoot.mark(\"dashboard-painted\"); } catch (_) {}\n"
        "  }\n"
        "  // Fire network refresh in background; do NOT await — prefill is visible.\n"
        "  if (!dashboardRefreshInFlight) {\n"
        "    refreshDashboard({ coreOnly: true })\n"
        "      .then(() => {\n"
        "        try { if (window.__navBoot) window.__navBoot.mark(\"dashboard-refreshed\"); } catch (_) {}\n"
        "        refreshDashboard({ force: false }).catch((err) => { console.warn(\"[dashboard] detail refresh failed:\", err); });\n"
        "        refreshTaskCenter().catch((err) => { console.warn(\"[dashboard] task center refresh failed:\", err); });\n"
        "        startTaskCenterPolling();\n"
        "        scheduleStartupPollingIfNeeded();\n"
        "      })\n"
        "      .catch((err) => {\n"
        "        console.warn(\"[dashboard] core refresh failed:\", err);\n"
        "        renderDashboardError(err);\n"
        "      });\n"
        "  }\n"
        "}"
    )
    assert old_bootstrap in content, "FAIL: old bootstrapDashboardTab not found"
    content = content.replace(old_bootstrap, new_bootstrap, 1)

    # ── 5. Update tab-click guard ─────────────────────────────────────────────
    old = "      if (target === \"dashboard\" && !dashboardBootstrapped) {"
    new = "      if (target === \"dashboard\" && !dashboardInitialPainted) {"
    assert old in content, "FAIL: tab-click guard not found"
    content = content.replace(old, new, 1)

    # ── 6. Fix swallowed benchmark exceptions ─────────────────────────────────
    old = "  } catch (_e) {\n    // History not critical\n  }"
    new = "  } catch (e) {\n    console.warn(\"[benchmark] history load failed:\", e);\n  }"
    assert old in content, "FAIL: benchmark catch 1 not found"
    content = content.replace(old, new, 1)

    old = "  } catch (_e) {\n    // Case-set list is optional; keep template fallback.\n  }"
    new = "  } catch (e) {\n    console.warn(\"[benchmark] case-set load failed:\", e);\n  }"
    assert old in content, "FAIL: benchmark catch 2 not found"
    content = content.replace(old, new, 1)

    return content


def main():
    original = APP_JS.read_text(encoding="utf-8")
    patched = apply(original)
    if patched == original:
        print("WARNING: no changes made")
        sys.exit(1)
    APP_JS.write_text(patched, encoding="utf-8")
    print(f"OK: app.js patched ({len(original)} → {len(patched)} bytes)")

    # Quick sanity checks
    checks = [
        "dashboardInitialPainted",
        "dashboardRefreshInFlight",
        "paintDashboardFromData",
        "dashboard-painted",
        "dashboard-refreshed",
        "[benchmark] history load failed:",
    ]
    for check in checks:
        assert check in patched, f"FAIL post-check: '{check}' missing"
    assert "dashboardBootstrapped" not in patched, "FAIL: old flag still present"
    print("All post-write checks passed.")


if __name__ == "__main__":
    main()
