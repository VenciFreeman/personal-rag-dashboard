from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4


ROUTER_CLS_HISTORY_MAX = 20
ROUTER_CLS_ALLOWED_DOMAINS = {"media", "tech", "general"}
ROUTER_CLS_ALLOWED_LABELS = {"MEDIA", "TECH", "OTHER"}
ROUTER_CLS_ALLOWED_LOOKUP_MODES = {"general_lookup", "entity_lookup", "concept_lookup", "filter_search"}


def coerce_router_cls_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        items = value
    elif isinstance(value, tuple):
        items = list(value)
    else:
        items = [value]
    normalized: list[str] = []
    for item in items:
        text = str(item or "").strip()
        if text:
            normalized.append(text)
    return normalized


def derive_router_cls_mock_label(expected_domain: str) -> str:
    if expected_domain == "media":
        return "MEDIA"
    if expected_domain == "tech":
        return "TECH"
    return "OTHER"


def derive_router_cls_mock_domain(expected_domain: str) -> str:
    return expected_domain if expected_domain in {"media", "tech"} else "general"


def derive_router_cls_lookup_mode(expected_domain: str, expected_arbitration: list[str], mock_entities: list[str]) -> str:
    if mock_entities:
        return "entity_lookup"
    if any("concept" in item for item in expected_arbitration):
        return "concept_lookup"
    if expected_domain == "media" and any("surface" in item for item in expected_arbitration):
        return "filter_search"
    return "general_lookup"


def normalize_router_cls_case_record(payload: dict[str, Any], *, case_id: str | None = None) -> dict[str, Any]:
    query = str(payload.get("query") or "").strip()
    if not query:
        raise ValueError("query 不能为空")

    expected_domain = str(payload.get("expected_domain") or payload.get("exp_domain") or "").strip().lower()
    if expected_domain not in ROUTER_CLS_ALLOWED_DOMAINS:
        raise ValueError("expected_domain 必须是 media / tech / general")

    expected_arbitration = coerce_router_cls_list(
        payload.get("expected_arbitration")
        if "expected_arbitration" in payload
        else payload.get("exp_arb_any") or payload.get("exp_arb")
    )
    if not expected_arbitration:
        raise ValueError("expected_arbitration 不能为空")

    mock_entities = coerce_router_cls_list(payload.get("mock_entities"))
    mock_label = str(payload.get("mock_label") or derive_router_cls_mock_label(expected_domain)).strip().upper()
    if mock_label not in ROUTER_CLS_ALLOWED_LABELS:
        raise ValueError("mock_label 必须是 MEDIA / TECH / OTHER")

    mock_domain = str(payload.get("mock_domain") or derive_router_cls_mock_domain(expected_domain)).strip().lower()
    if mock_domain not in ROUTER_CLS_ALLOWED_DOMAINS:
        raise ValueError("mock_domain 必须是 media / tech / general")

    mock_lookup_mode = str(
        payload.get("mock_lookup_mode")
        or payload.get("mock_lu")
        or derive_router_cls_lookup_mode(expected_domain, expected_arbitration, mock_entities)
    ).strip().lower()
    if mock_lookup_mode not in ROUTER_CLS_ALLOWED_LOOKUP_MODES:
        raise ValueError("mock_lookup_mode 必须是 general_lookup / entity_lookup / concept_lookup / filter_search")

    normalized_id = str(case_id or payload.get("id") or f"router-cls-{uuid4().hex[:10]}").strip()
    if not normalized_id:
        raise ValueError("case id 不能为空")

    note = str(payload.get("note") or "").strip()
    timestamp = datetime.now().isoformat(timespec="seconds")
    arbitration_value: str | list[str] = expected_arbitration[0] if len(expected_arbitration) == 1 else expected_arbitration

    return {
        "id": normalized_id,
        "query": query,
        "expected_domain": expected_domain,
        "expected_arbitration": arbitration_value,
        "expected_query_class": str(payload.get("expected_query_class") or "").strip(),
        "subject_scope": str(payload.get("subject_scope") or "").strip(),
        "time_scope_type": str(payload.get("time_scope_type") or "").strip(),
        "answer_shape": str(payload.get("answer_shape") or "").strip(),
        "media_family": str(payload.get("media_family") or "").strip(),
        "followup_mode": str(payload.get("followup_mode") or "").strip(),
        "note": note,
        "mock_label": mock_label,
        "mock_domain": mock_domain,
        "mock_entities": mock_entities,
        "mock_lookup_mode": mock_lookup_mode,
        "updated_at": timestamp,
    }


def default_router_cls_case_records(legacy_router_classification_cases: list[dict[str, Any]]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for index, case in enumerate(legacy_router_classification_cases, start=1):
        record = normalize_router_cls_case_record(case, case_id=f"preset-{index:03d}")
        records.append(record)
    return records


def load_history(
    benchmark_file: Path,
    benchmark_history_max: int,
    attach_assertions: Callable[[dict[str, Any], list[str]], dict[str, Any]],
) -> list[dict[str, Any]]:
    if not benchmark_file.exists():
        return []
    try:
        raw = json.loads(benchmark_file.read_text(encoding="utf-8"))
        results = list(raw.get("results", [])) if isinstance(raw, dict) else []
        normalized: list[dict[str, Any]] = []
        for item in results[-benchmark_history_max:]:
            if not isinstance(item, dict):
                continue
            config = item.get("config") if isinstance(item.get("config"), dict) else {}
            modules = [m for m in config.get("modules", []) if m in ("rag", "agent", "hybrid")]
            normalized.append(attach_assertions(item, modules) if modules else item)
        return normalized
    except Exception:
        return []


def save_result(
    benchmark_file: Path,
    benchmark_history_max: int,
    result: dict[str, Any],
    attach_assertions: Callable[[dict[str, Any], list[str]], dict[str, Any]],
) -> None:
    history = load_history(benchmark_file, benchmark_history_max, attach_assertions)
    history.append(result)
    history = history[-benchmark_history_max:]
    benchmark_file.parent.mkdir(parents=True, exist_ok=True)
    benchmark_file.write_text(
        json.dumps({"results": history}, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def load_router_cls_case_records(
    router_cls_cases_file: Path,
    legacy_router_classification_cases: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if router_cls_cases_file.exists():
        try:
            raw = json.loads(router_cls_cases_file.read_text(encoding="utf-8"))
            rows = raw.get("cases", []) if isinstance(raw, dict) else []
            if isinstance(rows, list):
                normalized: list[dict[str, Any]] = []
                for row in rows:
                    if isinstance(row, dict):
                        normalized.append(normalize_router_cls_case_record(row))
                if normalized:
                    return normalized
        except Exception:
            pass
    return default_router_cls_case_records(legacy_router_classification_cases)


def save_router_cls_case_records(router_cls_cases_file: Path, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    router_cls_cases_file.parent.mkdir(parents=True, exist_ok=True)
    payload = {"cases": records}
    router_cls_cases_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return records


def load_router_cls_history(router_cls_history_file: Path) -> list[dict[str, Any]]:
    if not router_cls_history_file.exists():
        return []
    try:
        raw = json.loads(router_cls_history_file.read_text(encoding="utf-8"))
        return list(raw.get("results", []))[-ROUTER_CLS_HISTORY_MAX:] if isinstance(raw, dict) else []
    except Exception:
        return []


def save_router_cls_result(router_cls_history_file: Path, result: dict[str, Any]) -> None:
    history = load_router_cls_history(router_cls_history_file)
    history.append(result)
    router_cls_history_file.parent.mkdir(parents=True, exist_ok=True)
    router_cls_history_file.write_text(
        json.dumps({"results": history[-ROUTER_CLS_HISTORY_MAX:]}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )