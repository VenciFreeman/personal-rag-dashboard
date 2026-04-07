from __future__ import annotations

import importlib.util
import json
import os
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from ..runtime_paths import DATA_DIR, QUOTA_FILE, QUOTA_HISTORY_FILE


_LOCK = threading.RLock()


def _load_shared_quota_defaults() -> tuple[int, int]:
    workspace_root = Path(__file__).resolve().parents[3]
    cfg_path = workspace_root / "api_config.py"
    if not cfg_path.is_file():
        return 50, 25
    try:
        spec = importlib.util.spec_from_file_location("_shared_api_config", cfg_path)
        if not spec or not spec.loader:
            return 50, 25
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        web_limit = int(getattr(module, "NAV_DASHBOARD_WEB_SEARCH_DAILY_LIMIT", 50) or 50)
        deepseek_limit = int(getattr(module, "NAV_DASHBOARD_DEEPSEEK_DAILY_LIMIT", 25) or 25)
        return web_limit, deepseek_limit
    except Exception:
        return 50, 25


_WEB_LIMIT_DEFAULT, _DEEPSEEK_LIMIT_DEFAULT = _load_shared_quota_defaults()
WEB_SEARCH_DAILY_LIMIT = int(os.getenv("NAV_DASHBOARD_WEB_SEARCH_DAILY_LIMIT", str(_WEB_LIMIT_DEFAULT)))
DEEPSEEK_DAILY_LIMIT = int(os.getenv("NAV_DASHBOARD_DEEPSEEK_DAILY_LIMIT", str(_DEEPSEEK_LIMIT_DEFAULT)))


def load_quota_state() -> dict[str, Any]:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not QUOTA_FILE.exists():
        return {"date": datetime.now().strftime("%Y-%m-%d"), "web_search": 0, "deepseek": 0}
    try:
        data = json.loads(QUOTA_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {"date": datetime.now().strftime("%Y-%m-%d"), "web_search": 0, "deepseek": 0}
    if not isinstance(data, dict):
        data = {}
    today = datetime.now().strftime("%Y-%m-%d")
    if data.get("date") != today:
        return {"date": today, "web_search": 0, "deepseek": 0}
    return {
        "date": today,
        "web_search": int(data.get("web_search", 0) or 0),
        "deepseek": int(data.get("deepseek", 0) or 0),
    }


def save_quota_state(state: dict[str, Any]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    QUOTA_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def _current_month() -> str:
    return datetime.now().strftime("%Y-%m")


def load_quota_history() -> dict[str, Any]:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not QUOTA_HISTORY_FILE.exists():
        return {"months": {}}
    try:
        payload = json.loads(QUOTA_HISTORY_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {"months": {}}
    if not isinstance(payload, dict):
        return {"months": {}}
    months = payload.get("months")
    if not isinstance(months, dict):
        months = {}
    cleaned: dict[str, dict[str, int]] = {}
    for month, row in months.items():
        if not isinstance(row, dict):
            continue
        key = str(month or "").strip()
        if not key:
            continue
        cleaned[key] = {
            "web_search": int(row.get("web_search", 0) or 0),
            "deepseek": int(row.get("deepseek", 0) or 0),
        }
    return {"months": cleaned}


def save_quota_history(history: dict[str, Any]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    QUOTA_HISTORY_FILE.write_text(json.dumps(history, ensure_ascii=False, indent=2), encoding="utf-8")


def set_monthly_quota_usage(*, web_search: int | None = None, deepseek: int | None = None) -> dict[str, int]:
    with _LOCK:
        history = load_quota_history()
        months = history.get("months") if isinstance(history.get("months"), dict) else {}
        month_key = _current_month()
        current = months.get(month_key) if isinstance(months.get(month_key), dict) else {}
        next_row = {
            "web_search": int(current.get("web_search", 0) or 0),
            "deepseek": int(current.get("deepseek", 0) or 0),
        }
        if web_search is not None:
            next_row["web_search"] = max(0, int(web_search))
        if deepseek is not None:
            next_row["deepseek"] = max(0, int(deepseek))
        months[month_key] = next_row
        history["months"] = months
        save_quota_history(history)
        return next_row


def record_quota_usage(*, web_search_delta: int = 0, deepseek_delta: int = 0) -> None:
    web_inc = int(web_search_delta or 0)
    deepseek_inc = int(deepseek_delta or 0)
    if web_inc <= 0 and deepseek_inc <= 0:
        return
    with _LOCK:
        history = load_quota_history()
        months = history.get("months") if isinstance(history.get("months"), dict) else {}
        month_key = _current_month()
        current = months.get(month_key) if isinstance(months.get(month_key), dict) else {}
        current_web = int(current.get("web_search", 0) or 0)
        current_deepseek = int(current.get("deepseek", 0) or 0)
        months[month_key] = {
            "web_search": current_web + max(0, web_inc),
            "deepseek": current_deepseek + max(0, deepseek_inc),
        }
        history["months"] = months
        save_quota_history(history)


def increment_quota_state(state: dict[str, Any], *, web_search_delta: int = 0, deepseek_delta: int = 0) -> None:
    web_inc = int(web_search_delta or 0)
    deepseek_inc = int(deepseek_delta or 0)
    with _LOCK:
        if web_inc > 0:
            state["web_search"] = int(state.get("web_search", 0) or 0) + web_inc
        if deepseek_inc > 0:
            state["deepseek"] = int(state.get("deepseek", 0) or 0) + deepseek_inc
        if web_inc > 0 or deepseek_inc > 0:
            save_quota_state(state)
            record_quota_usage(web_search_delta=web_inc, deepseek_delta=deepseek_inc)


def build_quota_exceeded(*, quota_state: dict[str, Any], web_search_needed: int = 0, deepseek_needed: int = 0) -> list[dict[str, Any]]:
    exceeded: list[dict[str, Any]] = []
    if int(web_search_needed or 0) > 0:
        now_count = int(quota_state.get("web_search", 0) or 0)
        if now_count + int(web_search_needed) > WEB_SEARCH_DAILY_LIMIT:
            exceeded.append(
                {
                    "kind": "web_search",
                    "current": now_count,
                    "add": int(web_search_needed),
                    "limit": WEB_SEARCH_DAILY_LIMIT,
                }
            )
    if int(deepseek_needed or 0) > 0:
        now_count = int(quota_state.get("deepseek", 0) or 0)
        if now_count + int(deepseek_needed) > DEEPSEEK_DAILY_LIMIT:
            exceeded.append(
                {
                    "kind": "deepseek",
                    "current": now_count,
                    "add": int(deepseek_needed),
                    "limit": DEEPSEEK_DAILY_LIMIT,
                }
            )
    return exceeded


def check_external_quota_exceeded(
    *,
    mode: str,
    search_mode: str,
    normalize_search_mode: Callable[[str], str],
) -> list[dict[str, Any]]:
    normalized_mode = str(mode or "local").strip().lower()
    normalized_search = normalize_search_mode(search_mode)
    return build_quota_exceeded(
        quota_state=load_quota_state(),
        web_search_needed=1 if normalized_search == "hybrid" else 0,
        deepseek_needed=1 if normalized_mode in {"deepseek", "reasoner"} else 0,
    )