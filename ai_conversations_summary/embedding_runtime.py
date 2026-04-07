from __future__ import annotations

import json
import os
from pathlib import Path

from ai_conversations_summary.runtime_paths import DATA_DIR, VECTOR_DB_DIR
from core_service.runtime_data import iter_core_runtime_data_roots

try:
    from ai_conversations_summary.scripts.api_config import EMBEDDING_MODEL as DEFAULT_EMBEDDING_MODEL
except ImportError:
    from scripts.api_config import EMBEDDING_MODEL as DEFAULT_EMBEDDING_MODEL  # type: ignore[no-redef]


def _data_roots() -> list[Path]:
    roots: list[Path] = []
    seen: set[str] = set()
    for root in [DATA_DIR, *iter_core_runtime_data_roots()]:
        key = str(root)
        if key in seen:
            continue
        seen.add(key)
        roots.append(root)
    return roots


def _dedupe(values: list[str]) -> list[str]:
    resolved: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip()
        if not text:
            continue
        if text in seen:
            continue
        seen.add(text)
        resolved.append(text)
    return resolved


def _relocate_legacy_local_model_path(model_value: str) -> str:
    raw = str(model_value or "").strip()
    if not raw:
        return ""

    path_text = raw.replace("\\", "/")
    lowered = path_text.lower()
    marker = "/data/local_models/"
    marker_index = lowered.find(marker)
    if marker_index < 0:
        return raw

    suffix = path_text[marker_index + len(marker) :].strip("/")
    if not suffix:
        return raw

    suffix_candidates = [suffix]
    models_marker = "/models--"
    models_index = suffix.lower().find(models_marker)
    if models_index >= 0:
        trimmed = suffix[models_index + 1 :].strip("/")
        if trimmed and trimmed not in suffix_candidates:
            suffix_candidates.append(trimmed)

    for root in _data_roots():
        for suffix_item in suffix_candidates:
            remapped = (root / "local_models" / Path(suffix_item)).resolve()
            if remapped.exists():
                return str(remapped)
    return raw


def _normalize_embedding_model_value(model_value: str) -> str:
    raw = str(model_value or "").strip()
    if not raw:
        return ""

    path_candidate = Path(raw)
    if path_candidate.is_absolute() and not path_candidate.exists():
        remapped = _relocate_legacy_local_model_path(raw)
        if remapped != raw:
            return remapped
    return raw


def _find_local_snapshot_path(model_name: str) -> Path | None:
    raw = str(model_name or "").strip()
    if "/" not in raw:
        return None

    safe_id = raw.replace("/", "--")
    pattern = f"**/models--{safe_id}/snapshots/*"
    candidates: list[Path] = []
    for root in _data_roots():
        cache_root = root / "local_models"
        if not cache_root.exists():
            continue
        candidates.extend([path for path in cache_root.glob(pattern) if path.is_dir()])

    if not candidates:
        return None

    candidates.sort(key=lambda path: path.stat().st_mtime, reverse=True)
    return candidates[0]


def _find_local_model_dir_candidates(model_name: str) -> list[Path]:
    raw = _normalize_embedding_model_value(model_name)
    if not raw:
        return []

    candidates: list[Path] = []
    as_path = Path(raw)
    if as_path.exists() and as_path.is_dir():
        candidates.append(as_path.resolve())

    leaf = raw.split("/")[-1].strip()
    for root in _data_roots():
        local_models_root = root / "local_models"
        if not local_models_root.exists():
            continue
        candidates.append(local_models_root / raw.replace("\\", "/"))
        if "/" in raw:
            candidates.append(local_models_root / raw.replace("/", "--"))
        if leaf:
            candidates.append(local_models_root / leaf)

    resolved: list[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        if not candidate.is_dir():
            continue
        key = str(candidate.resolve())
        if key in seen:
            continue
        seen.add(key)
        resolved.append(candidate.resolve())
    return resolved


def _read_manifest_embedding_model(index_dir: Path) -> str:
    meta_path = Path(index_dir) / "metadata.json"
    if not meta_path.exists():
        return ""

    try:
        raw = json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception:
        return ""

    if isinstance(raw, dict):
        manifest = raw.get("manifest") if isinstance(raw.get("manifest"), dict) else {}
        return str(manifest.get("embedding_model") or "").strip()
    return ""


def _looks_like_sentence_transformer_dir(path: Path) -> bool:
    if not path.is_dir():
        return False
    required_markers = (
        "modules.json",
        "config_sentence_transformers.json",
        "sentence_bert_config.json",
        "model.safetensors",
        "pytorch_model.bin",
    )
    if any((path / marker).exists() for marker in required_markers):
        return True
    return False


def _discover_single_local_model_dir() -> str:
    candidates: list[Path] = []
    seen: set[str] = set()
    for root in _data_roots():
        local_models_root = root / "local_models"
        if not local_models_root.exists():
            continue
        for path in local_models_root.rglob("*"):
            if not _looks_like_sentence_transformer_dir(path):
                continue
            key = str(path.resolve())
            if key in seen:
                continue
            seen.add(key)
            candidates.append(path.resolve())
    if len(candidates) == 1:
        return str(candidates[0])
    return ""


def _resolve_existing_local_model_ref(model_name: str) -> str:
    raw = _normalize_embedding_model_value(model_name)
    if not raw:
        return ""

    direct = Path(raw)
    if direct.is_dir():
        return str(direct.resolve())

    for candidate in _find_local_model_dir_candidates(raw):
        return str(candidate)

    snapshot = _find_local_snapshot_path(raw)
    if snapshot is not None:
        return str(snapshot.resolve())

    return ""


def resolve_embedding_model(preferred: str = "", *, index_dir: Path | None = None) -> str:
    target_index_dir = Path(index_dir) if index_dir is not None else VECTOR_DB_DIR
    requested = _dedupe(
        [
            preferred,
            os.getenv("LOCAL_EMBEDDING_MODEL", ""),
            os.getenv("DEEPSEEK_EMBEDDING_MODEL", ""),
            str(DEFAULT_EMBEDDING_MODEL or ""),
            _read_manifest_embedding_model(target_index_dir),
        ]
    )

    for candidate in requested:
        resolved_local = _resolve_existing_local_model_ref(candidate)
        if resolved_local:
            return resolved_local

    single_local_model = _discover_single_local_model_dir()
    if single_local_model:
        return single_local_model

    for candidate in requested:
        normalized = _normalize_embedding_model_value(candidate)
        if normalized:
            return normalized
    return ""