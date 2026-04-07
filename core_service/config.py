from __future__ import annotations

import json
import os
import re
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
    rag_response_max_tokens_with_context: int
    rag_response_max_tokens_no_context: int
    rag_local_only_read_web_cache: bool
    local_llm_url: str
    local_llm_model: str
    local_llm_api_key: str
    mediawiki_zh_api_url: str
    mediawiki_en_api_url: str
    mediawiki_user_agent: str
    mediawiki_api_user_agent: str
    mediawiki_contact: str
    mediawiki_timeout: int
    tmdb_api_base_url: str
    tmdb_api_key: str
    tmdb_read_access_token: str
    tmdb_timeout: int
    tmdb_language: str
    bangumi_access_token: str


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


def _load_env_local_ps1() -> None:
    root = _workspace_root()
    path = root / "env.local.ps1"
    if not path.exists() or not path.is_file():
        return
    try:
        raw = path.read_text(encoding="utf-8")
    except Exception:
        return
    for line in raw.splitlines():
        text = line.strip()
        if not text or text.startswith("#"):
            continue
        match = re.match(r'^\$env:([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(["\'])(.*?)\2\s*$', text)
        if not match:
            continue
        key = match.group(1)
        value = match.group(3)
        if os.getenv(key, "").strip():
            continue
        os.environ[key] = value


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
    _load_env_local_ps1()
    file_cfg = _read_config_file()

    api_cfg = file_cfg.get("api", {}) if isinstance(file_cfg.get("api"), dict) else {}
    rag_cfg = file_cfg.get("rag", {}) if isinstance(file_cfg.get("rag"), dict) else {}
    local_llm_cfg = file_cfg.get("local_llm", {}) if isinstance(file_cfg.get("local_llm"), dict) else {}
    external_cfg = file_cfg.get("external_apis", {}) if isinstance(file_cfg.get("external_apis"), dict) else {}
    mediawiki_cfg = external_cfg.get("mediawiki", {}) if isinstance(external_cfg.get("mediawiki"), dict) else {}
    tmdb_cfg = external_cfg.get("tmdb", {}) if isinstance(external_cfg.get("tmdb"), dict) else {}
    bangumi_cfg = external_cfg.get("bangumi", {}) if isinstance(external_cfg.get("bangumi"), dict) else {}

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

    with_context_raw = _first_non_empty(
        os.getenv("AI_SUMMARY_RESPONSE_MAX_TOKENS_WITH_CONTEXT", ""),
        str(rag_cfg.get("response_max_tokens_with_context", "")),
        "2500",
    )
    no_context_raw = _first_non_empty(
        os.getenv("AI_SUMMARY_RESPONSE_MAX_TOKENS_NO_CONTEXT", ""),
        str(rag_cfg.get("response_max_tokens_no_context", "")),
        "1200",
    )
    local_only_read_web_cache_raw = _first_non_empty(
        os.getenv("AI_SUMMARY_LOCAL_ONLY_READ_WEB_CACHE", ""),
        str(rag_cfg.get("local_only_read_web_cache", "")),
        "1",
    )

    try:
        rag_response_max_tokens_with_context = int(with_context_raw)
    except ValueError:
        rag_response_max_tokens_with_context = 2500

    try:
        rag_response_max_tokens_no_context = int(no_context_raw)
    except ValueError:
        rag_response_max_tokens_no_context = 1200

    rag_local_only_read_web_cache = str(local_only_read_web_cache_raw).strip().lower() in {"1", "true", "yes", "on"}

    local_llm_url = _first_non_empty(
        os.getenv("AI_SUMMARY_LOCAL_LLM_URL", ""),
        str(local_llm_cfg.get("url", "")),
        "http://127.0.0.1:1234/v1",
    )
    local_llm_model = _first_non_empty(
        os.getenv("AI_SUMMARY_LOCAL_LLM_MODEL", ""),
        str(local_llm_cfg.get("model", "")),
        # ── One-line switch: change the default below to switch models ──
        "qwen2.5-7b-instruct",
        # "unsloth/Qwen3.5-4B-GGUF-no-thinking",
    )
    local_llm_api_key = _first_non_empty(
        os.getenv("AI_SUMMARY_LOCAL_LLM_API_KEY", ""),
        str(local_llm_cfg.get("api_key", "")),
        "local",
    )

    mediawiki_zh_api_url = _first_non_empty(
        os.getenv("NAV_DASHBOARD_MEDIAWIKI_ZH_API_URL", ""),
        str(mediawiki_cfg.get("zh_api_url", "")),
        "https://zh.wikipedia.org/w/api.php",
    )
    mediawiki_en_api_url = _first_non_empty(
        os.getenv("NAV_DASHBOARD_MEDIAWIKI_EN_API_URL", ""),
        str(mediawiki_cfg.get("en_api_url", "")),
        "https://en.wikipedia.org/w/api.php",
    )
    mediawiki_user_agent = _first_non_empty(
        os.getenv("NAV_DASHBOARD_MEDIAWIKI_USER_AGENT", ""),
        str(mediawiki_cfg.get("user_agent", "")),
        "",
    )
    mediawiki_api_user_agent = _first_non_empty(
        os.getenv("NAV_DASHBOARD_MEDIAWIKI_API_USER_AGENT", ""),
        str(mediawiki_cfg.get("api_user_agent", "")),
        "",
    )
    mediawiki_contact = _first_non_empty(
        os.getenv("NAV_DASHBOARD_MEDIAWIKI_CONTACT", ""),
        str(mediawiki_cfg.get("contact", "")),
        "",
    )
    mediawiki_timeout_raw = _first_non_empty(
        os.getenv("NAV_DASHBOARD_MEDIAWIKI_TIMEOUT", ""),
        str(mediawiki_cfg.get("timeout", "")),
        "20",
    )
    try:
        mediawiki_timeout = int(mediawiki_timeout_raw)
    except ValueError:
        mediawiki_timeout = 20

    tmdb_api_base_url = _first_non_empty(
        os.getenv("NAV_DASHBOARD_TMDB_API_BASE_URL", ""),
        str(tmdb_cfg.get("api_base_url", "")),
        "https://api.themoviedb.org/3",
    )
    tmdb_api_key = _first_non_empty(
        os.getenv("NAV_DASHBOARD_TMDB_API_KEY", ""),
        str(tmdb_cfg.get("api_key", "")),
        "",
    )
    tmdb_read_access_token = _first_non_empty(
        os.getenv("NAV_DASHBOARD_TMDB_READ_ACCESS_TOKEN", ""),
        str(tmdb_cfg.get("read_access_token", "")),
        "",
    )
    tmdb_timeout_raw = _first_non_empty(
        os.getenv("NAV_DASHBOARD_TMDB_TIMEOUT", ""),
        str(tmdb_cfg.get("timeout", "")),
        "20",
    )
    try:
        tmdb_timeout = int(tmdb_timeout_raw)
    except ValueError:
        tmdb_timeout = 20
    tmdb_language = _first_non_empty(
        os.getenv("NAV_DASHBOARD_TMDB_LANGUAGE", ""),
        str(tmdb_cfg.get("language", "")),
        "zh-CN",
    )

    bangumi_access_token = _first_non_empty(
        os.getenv("BANGUMI_ACCESS_TOKEN", ""),
        os.getenv("NAV_DASHBOARD_BANGUMI_ACCESS_TOKEN", ""),
        str(bangumi_cfg.get("access_token", "")),
        "",
    )

    if local_llm_url and not local_llm_url.rstrip("/").endswith("/v1"):
        local_llm_url = local_llm_url.rstrip("/") + "/v1"

    return CoreSettings(
        api_base_url=api_base_url,
        api_key=api_key,
        chat_model=chat_model,
        embedding_model=embedding_model,
        timeout=timeout,
        rag_response_max_tokens_with_context=rag_response_max_tokens_with_context,
        rag_response_max_tokens_no_context=rag_response_max_tokens_no_context,
        rag_local_only_read_web_cache=rag_local_only_read_web_cache,
        local_llm_url=local_llm_url,
        local_llm_model=local_llm_model,
        local_llm_api_key=local_llm_api_key,
        mediawiki_zh_api_url=mediawiki_zh_api_url,
        mediawiki_en_api_url=mediawiki_en_api_url,
        mediawiki_user_agent=mediawiki_user_agent,
        mediawiki_api_user_agent=mediawiki_api_user_agent,
        mediawiki_contact=mediawiki_contact,
        mediawiki_timeout=mediawiki_timeout,
        tmdb_api_base_url=tmdb_api_base_url,
        tmdb_api_key=tmdb_api_key,
        tmdb_read_access_token=tmdb_read_access_token,
        tmdb_timeout=tmdb_timeout,
        tmdb_language=tmdb_language,
        bangumi_access_token=bangumi_access_token,
    )


_PACKAGING_SUFFIX_RE = re.compile(
    r"[-_](?:GGUF|GPTQ|AWQ|EXL2|EXL|MLX).*$",
    re.IGNORECASE,
)


def display_model_name(model: str) -> str:
    """Derive a short human-readable display name from a model path or ID.

    Examples:
        "unsloth/Qwen3.5-4B-GGUF-no-thinking"  -> "Qwen3.5-4B"
        "qwen2.5-7b-instruct"                   -> "qwen2.5-7b-instruct"
        "/path/to/models/Qwen2.5-7B-Instruct"   -> "Qwen2.5-7B-Instruct"
    """
    value = (model or "").strip()
    if not value:
        return value
    parts = [p for p in value.replace("\\", "/").split("/") if p]
    name = parts[-1] if parts else value
    clean = _PACKAGING_SUFFIX_RE.sub("", name).strip()
    return clean or name
