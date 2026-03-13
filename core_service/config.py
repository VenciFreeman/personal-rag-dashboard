from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class CoreSettings:
    api_base_url: str
    api_key: str
    chat_model: str
    embedding_model: str
    timeout: int
    local_llm_url: str
    local_llm_model: str
    local_llm_api_key: str


def _workspace_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _config_candidates() -> list[Path]:
    root = _workspace_root()
    explicit = (os.getenv("CORE_SERVICE_CONFIG_FILE") or "").strip()
    paths: list[Path] = []
    if explicit:
        paths.append(Path(explicit))
    paths.append(root / "core_service" / "config.local.json")
    paths.append(root / "core_service" / "config.json")
    return paths


def _read_config_file() -> dict[str, Any]:
    for path in _config_candidates():
        if not path.exists() or not path.is_file():
            continue
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if isinstance(data, dict):
            return data
    return {}


def _first_non_empty(*values: str) -> str:
    for value in values:
        text = (value or "").strip()
        if text:
            return text
    return ""


def get_settings() -> CoreSettings:
    file_cfg = _read_config_file()

    api_cfg = file_cfg.get("api", {}) if isinstance(file_cfg.get("api"), dict) else {}
    rag_cfg = file_cfg.get("rag", {}) if isinstance(file_cfg.get("rag"), dict) else {}
    local_llm_cfg = file_cfg.get("local_llm", {}) if isinstance(file_cfg.get("local_llm"), dict) else {}

    api_base_url = _first_non_empty(
        os.getenv("DEEPSEEK_BASE_URL", ""),
        str(api_cfg.get("base_url", "")),
        "https://api.deepseek.com",
    )
    api_key = _first_non_empty(
        os.getenv("DEEPSEEK_API_KEY", ""),
        str(api_cfg.get("api_key", "")),
        "",
    )
    chat_model = _first_non_empty(
        os.getenv("DEEPSEEK_MODEL", ""),
        str(api_cfg.get("chat_model", "")),
        "deepseek-chat",
    )

    embedding_model = _first_non_empty(
        os.getenv("LOCAL_EMBEDDING_MODEL", ""),
        os.getenv("DEEPSEEK_EMBEDDING_MODEL", ""),
        str(rag_cfg.get("embedding_model", "")),
        "BAAI/bge-base-zh-v1.5",
    )

    timeout_raw = _first_non_empty(
        os.getenv("DEEPSEEK_TIMEOUT", ""),
        str(api_cfg.get("timeout", "")),
        "120",
    )
    try:
        timeout = int(timeout_raw)
    except ValueError:
        timeout = 120

    local_llm_url = _first_non_empty(
        os.getenv("AI_SUMMARY_LOCAL_LLM_URL", ""),
        str(local_llm_cfg.get("url", "")),
        "http://127.0.0.1:1234/v1",
    )
    local_llm_model = _first_non_empty(
        os.getenv("AI_SUMMARY_LOCAL_LLM_MODEL", ""),
        str(local_llm_cfg.get("model", "")),
        "qwen2.5-7b-instruct",
    )
    local_llm_api_key = _first_non_empty(
        os.getenv("AI_SUMMARY_LOCAL_LLM_API_KEY", ""),
        str(local_llm_cfg.get("api_key", "")),
        "local",
    )

    if local_llm_url and not local_llm_url.rstrip("/").endswith("/v1"):
        local_llm_url = local_llm_url.rstrip("/") + "/v1"

    return CoreSettings(
        api_base_url=api_base_url,
        api_key=api_key,
        chat_model=chat_model,
        embedding_model=embedding_model,
        timeout=timeout,
        local_llm_url=local_llm_url,
        local_llm_model=local_llm_model,
        local_llm_api_key=local_llm_api_key,
    )
