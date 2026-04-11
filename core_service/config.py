from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable


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
    tavily_api_key: str
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


CONFIG_LOCAL_PATH = _workspace_root() / "core_service" / "config.local.json"
CONFIG_JSON_PATH = _workspace_root() / "core_service" / "config.json"
ENV_LOCAL_PATH = _workspace_root() / "env.local.ps1"


def _config_candidates() -> list[Path]:
    explicit = (os.getenv("CORE_SERVICE_CONFIG_FILE") or "").strip()
    paths: list[Path] = []
    if explicit:
        paths.append(Path(explicit))
    paths.append(CONFIG_LOCAL_PATH)
    paths.append(CONFIG_JSON_PATH)
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
        data = _read_json_file(path)
        if data:
            return data
    return {}


def _read_json_file(path: Path) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _write_json_file(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _env_assignment_pattern(key: str) -> re.Pattern[str]:
    return re.compile(rf'^\s*\$env:{re.escape(key)}\s*=\s*(["\']).*?\1\s*$')


def _write_env_local_values(updates: dict[str, str]) -> None:
    if not updates:
        return
    existing_lines: list[str] = []
    if ENV_LOCAL_PATH.exists() and ENV_LOCAL_PATH.is_file():
        try:
            existing_lines = ENV_LOCAL_PATH.read_text(encoding="utf-8").splitlines()
        except Exception:
            existing_lines = []
    lines = list(existing_lines)
    for key, value in updates.items():
        escaped_value = str(value or "").replace("`", "``").replace('"', '`"')
        line = f'$env:{key}="{escaped_value}"'
        pattern = _env_assignment_pattern(key)
        replaced = False
        for index, existing in enumerate(lines):
            if pattern.match(existing):
                lines[index] = line
                replaced = True
                break
        if not replaced:
            if lines and lines[-1].strip():
                lines.append("")
            lines.append(line)
    ENV_LOCAL_PATH.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def _nested_delete(parent: dict[str, Any], path: tuple[str, ...]) -> None:
    cursor: Any = parent
    stack: list[tuple[dict[str, Any], str]] = []
    for key in path[:-1]:
        if not isinstance(cursor, dict):
            return
        next_value = cursor.get(key)
        if not isinstance(next_value, dict):
            return
        stack.append((cursor, key))
        cursor = next_value
    if not isinstance(cursor, dict):
        return
    cursor.pop(path[-1], None)
    while stack:
        parent_dict, key = stack.pop()
        child = parent_dict.get(key)
        if isinstance(child, dict) and not child:
            parent_dict.pop(key, None)
        else:
            break


def _first_non_empty(*values: str) -> str:
    for value in values:
        text = (value or "").strip()
        if text:
            return text
    return ""


def _normalize_local_llm_url(value: str) -> str:
    cleaned = str(value or "").strip()
    if cleaned and not cleaned.rstrip("/").endswith("/v1"):
        cleaned = cleaned.rstrip("/") + "/v1"
    return cleaned


def _nested_dict(parent: dict[str, Any], key: str) -> dict[str, Any]:
    value = parent.get(key)
    if isinstance(value, dict):
        return value
    value = {}
    parent[key] = value
    return value


def _nested_set(parent: dict[str, Any], path: tuple[str, ...], value: str) -> None:
    cursor = parent
    for key in path[:-1]:
        cursor = _nested_dict(cursor, key)
    cursor[path[-1]] = value


def _resolve_admin_field(
    *,
    env_names: tuple[str, ...],
    config_value: Any,
    default: str = "",
    normalize: Callable[[str], str] | None = None,
    secret: bool = False,
) -> dict[str, Any]:
    env_value = ""
    for env_name in env_names:
        candidate = str(os.getenv(env_name, "") or "").strip()
        if candidate:
            env_value = candidate
            break
    config_text = str(config_value or "").strip()
    source = "empty"
    value = ""
    if env_value:
        value = env_value
        source = "env"
    elif config_text:
        value = config_text
        source = "config"
    elif str(default or "").strip():
        value = str(default or "").strip()
        source = "default"
    if normalize is not None:
        value = normalize(value)
    return {
        "value": "" if secret else value,
        "configured": bool(value),
        "source": source,
        "secret": secret,
    }


def load_admin_config_state() -> dict[str, Any]:
    _load_env_local_ps1()
    file_cfg = _read_json_file(CONFIG_LOCAL_PATH)
    api_cfg = file_cfg.get("api", {}) if isinstance(file_cfg.get("api"), dict) else {}
    local_llm_cfg = file_cfg.get("local_llm", {}) if isinstance(file_cfg.get("local_llm"), dict) else {}
    external_cfg = file_cfg.get("external_apis", {}) if isinstance(file_cfg.get("external_apis"), dict) else {}
    tavily_cfg = external_cfg.get("tavily", {}) if isinstance(external_cfg.get("tavily"), dict) else {}
    tmdb_cfg = external_cfg.get("tmdb", {}) if isinstance(external_cfg.get("tmdb"), dict) else {}
    bangumi_cfg = external_cfg.get("bangumi", {}) if isinstance(external_cfg.get("bangumi"), dict) else {}
    mediawiki_cfg = external_cfg.get("mediawiki", {}) if isinstance(external_cfg.get("mediawiki"), dict) else {}

    return {
        "persistence_path": "core_service/config.local.json",
        "secret_persistence_path": "env.local.ps1",
        "config_file_exists": CONFIG_LOCAL_PATH.exists(),
        "env_file_exists": ENV_LOCAL_PATH.exists(),
        "external_api": {
            "base_url": _resolve_admin_field(
                env_names=("DEEPSEEK_BASE_URL",),
                config_value=api_cfg.get("base_url", ""),
                default="https://api.deepseek.com",
            ),
            "model": _resolve_admin_field(
                env_names=("DEEPSEEK_MODEL",),
                config_value=api_cfg.get("chat_model", ""),
                default="deepseek-chat",
            ),
            "api_key": _resolve_admin_field(
                env_names=("DEEPSEEK_API_KEY",),
                config_value=api_cfg.get("api_key", ""),
                secret=True,
            ),
        },
        "local_llm": {
            "base_url": _resolve_admin_field(
                env_names=("AI_SUMMARY_LOCAL_LLM_URL",),
                config_value=local_llm_cfg.get("url", ""),
                default="http://127.0.0.1:1234/v1",
                normalize=_normalize_local_llm_url,
            ),
            "model": _resolve_admin_field(
                env_names=("AI_SUMMARY_LOCAL_LLM_MODEL",),
                config_value=local_llm_cfg.get("model", ""),
                default="qwen2.5-7b-instruct",
            ),
            "api_key": _resolve_admin_field(
                env_names=("AI_SUMMARY_LOCAL_LLM_API_KEY",),
                config_value=local_llm_cfg.get("api_key", ""),
                default="local",
                secret=True,
            ),
        },
        "other_apis": {
            "tavily_api_key": _resolve_admin_field(
                env_names=("TAVILY_API_KEY", "NAV_DASHBOARD_TAVILY_API_KEY"),
                config_value=tavily_cfg.get("api_key", ""),
                secret=True,
            ),
            "tmdb_api_key": _resolve_admin_field(
                env_names=("NAV_DASHBOARD_TMDB_API_KEY",),
                config_value=tmdb_cfg.get("api_key", ""),
                secret=True,
            ),
            "bangumi_access_token": _resolve_admin_field(
                env_names=("BANGUMI_ACCESS_TOKEN", "NAV_DASHBOARD_BANGUMI_ACCESS_TOKEN"),
                config_value=bangumi_cfg.get("access_token", ""),
                secret=True,
            ),
        },
    }


def update_admin_config(payload: dict[str, Any]) -> dict[str, Any]:
    file_cfg = _read_json_file(CONFIG_LOCAL_PATH)
    changed_fields: list[str] = []
    env_updates: dict[str, str] = {}

    def _maybe_update(path: tuple[str, ...], value: Any, *, normalize: Callable[[str], str] | None = None) -> None:
        text = str(value or "").strip()
        if not text:
            return
        if normalize is not None:
            text = normalize(text)
        _nested_set(file_cfg, path, text)
        changed_fields.append(".".join(path))

    def _maybe_update_secret(path: tuple[str, ...], env_name: str, value: Any) -> None:
        text = str(value or "").strip()
        if not text:
            return
        env_updates[env_name] = text
        _nested_delete(file_cfg, path)
        changed_fields.append(env_name)

    external_api = payload.get("external_api") if isinstance(payload.get("external_api"), dict) else {}
    local_llm = payload.get("local_llm") if isinstance(payload.get("local_llm"), dict) else {}
    other_apis = payload.get("other_apis") if isinstance(payload.get("other_apis"), dict) else {}

    _maybe_update(("api", "base_url"), external_api.get("base_url"))
    _maybe_update(("api", "chat_model"), external_api.get("model"))
    _maybe_update_secret(("api", "api_key"), "DEEPSEEK_API_KEY", external_api.get("api_key"))
    _maybe_update(("local_llm", "url"), local_llm.get("base_url"), normalize=_normalize_local_llm_url)
    _maybe_update(("local_llm", "model"), local_llm.get("model"))
    _maybe_update_secret(("local_llm", "api_key"), "AI_SUMMARY_LOCAL_LLM_API_KEY", local_llm.get("api_key"))
    _maybe_update_secret(("external_apis", "tavily", "api_key"), "TAVILY_API_KEY", other_apis.get("tavily_api_key"))
    _maybe_update_secret(("external_apis", "tmdb", "api_key"), "NAV_DASHBOARD_TMDB_API_KEY", other_apis.get("tmdb_api_key"))
    _maybe_update_secret(("external_apis", "bangumi", "access_token"), "BANGUMI_ACCESS_TOKEN", other_apis.get("bangumi_access_token"))

    if env_updates:
        _write_env_local_values(env_updates)
    if changed_fields:
        _write_json_file(CONFIG_LOCAL_PATH, file_cfg)

    return {
        "ok": True,
        "updated_fields": changed_fields,
        "config": load_admin_config_state(),
    }


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
    tavily_cfg = external_cfg.get("tavily", {}) if isinstance(external_cfg.get("tavily"), dict) else {}
    tavily_api_key = _first_non_empty(
        os.getenv("TAVILY_API_KEY", ""),
        os.getenv("NAV_DASHBOARD_TAVILY_API_KEY", ""),
        str(tavily_cfg.get("api_key", "")),
        "",
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
        tavily_api_key=tavily_api_key,
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
