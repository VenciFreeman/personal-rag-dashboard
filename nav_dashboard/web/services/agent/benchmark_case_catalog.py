from __future__ import annotations

import json
import threading
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from nav_dashboard.web.services.runtime_paths import BENCHMARK_CASES_FILE, LEGACY_NAV_DASHBOARD_DATA_DIR

_BUNDLED_BENCHMARK_CASES_FILE = LEGACY_NAV_DASHBOARD_DATA_DIR / "benchmark" / "query_case_sets.json"

_ALLOWED_LENGTHS = {"short", "medium", "long"}
_ALLOWED_MODULES = {"rag", "agent", "hybrid"}
_MAX_CASE_SET_SELECTION_COUNT = 20
_ANSWER_SHAPE_ALIASES = {
    "summary": "summary",
}
_FOLLOWUP_MODE_ALIASES = {
    "followup": "inherit_filters",
    "entity_carry": "inherit_entity",
    "time_window_replace": "inherit_timerange",
}


def normalize_followup_mode(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    return _FOLLOWUP_MODE_ALIASES.get(raw, raw)


def _selection_cap(total_count: int) -> int:
    normalized_total = max(0, int(total_count or 0))
    if normalized_total <= 0:
        return 0
    return min(_MAX_CASE_SET_SELECTION_COUNT, normalized_total)

_RAG_ONLY_TAXONOMIES = {
    "general_knowledge",
    "no_context_below_threshold",
    "tech_primary",
    "tech_signal_only",
}

_AGENT_HYBRID_TAXONOMIES = {
    "compare_terminal_quality",
    "creator_collection",
    "entity_alias_recall",
    "entity_detail_noise",
    "followup_contamination",
    "followup_inheritance",
    "media_entity_detail",
    "personal_review_cross_contamination",
    "personal_review_layering",
    "strict_scope_alias_collection",
}


def _ensure_catalog_file() -> Path:
    if BENCHMARK_CASES_FILE.exists():
        return BENCHMARK_CASES_FILE
    if not _BUNDLED_BENCHMARK_CASES_FILE.exists():
        raise FileNotFoundError(f"benchmark case catalog not found: {BENCHMARK_CASES_FILE}")
    BENCHMARK_CASES_FILE.parent.mkdir(parents=True, exist_ok=True)
    BENCHMARK_CASES_FILE.write_text(_BUNDLED_BENCHMARK_CASES_FILE.read_text(encoding="utf-8"), encoding="utf-8")
    return BENCHMARK_CASES_FILE


def _coerce_str_list(value: Any) -> list[str]:
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


def _normalize_history(value: Any) -> list[dict[str, str]]:
    if not isinstance(value, list):
        return []
    normalized: list[dict[str, str]] = []
    for row in value:
        if not isinstance(row, dict):
            continue
        role = str(row.get("role") or "").strip()
        content = str(row.get("content") or "").strip()
        if not role or not content:
            continue
        normalized.append({"role": role, "content": content})
    return normalized


def _normalize_optional_bool(value: Any) -> bool | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y"}:
        return True
    if text in {"false", "0", "no", "n"}:
        return False
    return None


def _normalize_selection_priority(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _normalize_modules(value: Any) -> list[str]:
    normalized: list[str] = []
    for module in _coerce_str_list(value):
        lowered = module.strip().lower()
        if lowered in _ALLOWED_MODULES and lowered not in normalized:
            normalized.append(lowered)
    return normalized


def _infer_supported_modules(payload: dict[str, Any]) -> list[str]:
    explicit = _normalize_modules(payload.get("supported_modules"))
    if explicit:
        return explicit

    taxonomy = str(payload.get("taxonomy") or "").strip().lower()
    expected_query_type = str(payload.get("expected_query_type") or "").strip().upper()
    expected_tools = {item.strip().lower() for item in _coerce_str_list(payload.get("expected_tools"))}

    if taxonomy in _RAG_ONLY_TAXONOMIES:
        return ["rag", "agent", "hybrid"]
    if expected_query_type == "MIXED_QUERY":
        return ["hybrid"]
    if expected_tools & {"query_document_rag", "search_web"} and expected_tools & {"query_media_record", "search_by_creator"}:
        return ["hybrid"]
    if taxonomy in _AGENT_HYBRID_TAXONOMIES:
        return ["agent", "hybrid"]
    if expected_tools == {"query_document_rag"}:
        return ["rag"]
    if expected_query_type == "MEDIA_QUERY":
        return ["agent", "hybrid"]
    return ["rag", "agent", "hybrid"]


def _normalize_case_record(payload: dict[str, Any]) -> dict[str, Any]:
    case_id = str(payload.get("id") or "").strip()
    query = str(payload.get("query") or "").strip()
    length = str(payload.get("length") or "").strip().lower()
    if not case_id:
        raise ValueError("benchmark case id is required")
    if not query:
        raise ValueError(f"benchmark case {case_id} is missing query")
    if length not in _ALLOWED_LENGTHS:
        raise ValueError(f"benchmark case {case_id} has invalid length: {length}")
    raw_answer_shape = str(payload.get("answer_shape") or "").strip()
    raw_followup_mode = str(payload.get("followup_mode") or "").strip()
    return {
        "id": case_id,
        "query": query,
        "length": length,
        "source": str(payload.get("source") or "manual").strip() or "manual",
        "taxonomy": str(payload.get("taxonomy") or "uncategorized").strip() or "uncategorized",
        "expected_query_type": str(payload.get("expected_query_type") or "").strip(),
        "expected_domain": str(payload.get("expected_domain") or "").strip(),
        "expected_arbitration": str(payload.get("expected_arbitration") or "").strip(),
        "expected_query_class": str(payload.get("expected_query_class") or "").strip(),
        "subject_scope": str(payload.get("subject_scope") or "").strip(),
        "time_scope_type": str(payload.get("time_scope_type") or "").strip(),
        "answer_shape": _ANSWER_SHAPE_ALIASES.get(raw_answer_shape, raw_answer_shape),
        "media_family": str(payload.get("media_family") or "").strip(),
        "followup_mode": normalize_followup_mode(raw_followup_mode),
        "supported_modules": _infer_supported_modules(payload),
        "strict_scope_expected": _normalize_optional_bool(payload.get("strict_scope_expected")),
        "selection_priority": _normalize_selection_priority(payload.get("selection_priority")),
        "expected_tools": _coerce_str_list(payload.get("expected_tools")),
        "quality_assertions": dict(payload.get("quality_assertions") or {}) if isinstance(payload.get("quality_assertions"), dict) else {},
        "notes": str(payload.get("notes") or "").strip(),
        "history": _normalize_history(payload.get("history")),
    }


def _catalog_source_token() -> tuple[str, int, int]:
    path = _ensure_catalog_file()
    stat = path.stat()
    return (str(path), int(stat.st_mtime_ns), int(stat.st_size))


def _load_catalog() -> dict[str, Any]:
    raw = json.loads(_ensure_catalog_file().read_text(encoding="utf-8"))
    rows = raw.get("cases", []) if isinstance(raw, dict) else []
    if not isinstance(rows, list):
        raise ValueError("benchmark case catalog must define a cases list")
    case_library: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        record = _normalize_case_record(row)
        case_library[record["id"]] = record

    raw_case_sets = raw.get("case_sets", {}) if isinstance(raw, dict) else {}
    if not isinstance(raw_case_sets, dict):
        raise ValueError("benchmark case catalog must define case_sets")

    case_set_cases: dict[str, dict[str, list[dict[str, Any]]]] = {}
    query_case_sets: dict[str, dict[str, list[str]]] = {}
    labels: dict[str, str] = {}
    metadata: dict[str, dict[str, Any]] = {}
    for case_set_id, payload in raw_case_sets.items():
        if not isinstance(payload, dict):
            continue
        label = str(payload.get("label") or case_set_id).strip() or case_set_id
        case_ids = _coerce_str_list(payload.get("case_ids"))
        grouped_cases = {length: [] for length in sorted(_ALLOWED_LENGTHS)}
        taxonomy_counts: dict[str, int] = {}
        source_counts: dict[str, int] = {}
        module_case_counts: dict[str, int] = {}
        module_length_counts: dict[str, dict[str, int]] = {}
        supported_modules: list[str] = []
        for case_id in case_ids:
            case = case_library.get(case_id)
            if case is None:
                raise ValueError(f"unknown benchmark case id in {case_set_id}: {case_id}")
            grouped_cases[case["length"]].append(dict(case))
            taxonomy = str(case.get("taxonomy") or "uncategorized")
            source = str(case.get("source") or "manual")
            taxonomy_counts[taxonomy] = taxonomy_counts.get(taxonomy, 0) + 1
            source_counts[source] = source_counts.get(source, 0) + 1
            for module in list(case.get("supported_modules") or []):
                normalized_module = str(module or "").strip().lower()
                if normalized_module not in _ALLOWED_MODULES:
                    continue
                module_case_counts[normalized_module] = module_case_counts.get(normalized_module, 0) + 1
                module_length_counts.setdefault(normalized_module, {length: 0 for length in sorted(_ALLOWED_LENGTHS)})
                module_length_counts[normalized_module][case["length"]] = int(module_length_counts[normalized_module].get(case["length"], 0)) + 1
                if normalized_module not in supported_modules:
                    supported_modules.append(normalized_module)
        length_counts = {length: len(grouped_cases.get(length, [])) for length in sorted(_ALLOWED_LENGTHS)}
        case_set_cases[case_set_id] = grouped_cases
        query_case_sets[case_set_id] = {
            length: [str(case.get("query") or "") for case in grouped_cases.get(length, [])]
            for length in sorted(_ALLOWED_LENGTHS)
        }
        labels[case_set_id] = label
        metadata[case_set_id] = {
            "label": label,
            "length_counts": length_counts,
            "max_query_count_per_type": _selection_cap(sum(length_counts.values())),
            "taxonomy_counts": taxonomy_counts,
            "source_counts": source_counts,
            "module_case_counts": module_case_counts,
            "module_length_counts": module_length_counts,
            "module_max_query_count_per_type": {
                module: _selection_cap(module_case_counts.get(module, 0))
                for module in module_length_counts
            },
            "supported_modules": supported_modules,
        }

    return {
        "case_library": case_library,
        "case_set_cases": case_set_cases,
        "query_case_sets": query_case_sets,
        "labels": labels,
        "metadata": metadata,
    }


class BenchmarkCatalog:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._catalog: dict[str, Any] | None = None
        self._source_token: tuple[str, int, int] | None = None

    def load(self, *, force_reload: bool = False) -> dict[str, Any]:
        current_token = _catalog_source_token()
        if not force_reload and self._catalog is not None and self._source_token == current_token:
            return self._catalog
        with self._lock:
            current_token = _catalog_source_token()
            if force_reload or self._catalog is None or self._source_token != current_token:
                self._catalog = _load_catalog()
                self._source_token = current_token
            return self._catalog

    def reload(self) -> dict[str, Any]:
        return self.load(force_reload=True)

    def validate(self) -> dict[str, Any]:
        return self.reload()

    @property
    def case_library(self) -> dict[str, dict[str, Any]]:
        return self.load()["case_library"]

    @property
    def case_set_cases(self) -> dict[str, dict[str, list[dict[str, Any]]]]:
        return self.load()["case_set_cases"]

    @property
    def query_case_sets(self) -> dict[str, dict[str, list[str]]]:
        return self.load()["query_case_sets"]

    @property
    def labels(self) -> dict[str, str]:
        return self.load()["labels"]

    @property
    def metadata(self) -> dict[str, dict[str, Any]]:
        return self.load()["metadata"]


class _LazyCatalogMapping(Mapping[str, Any]):
    def __init__(self, getter: Any) -> None:
        self._getter = getter

    def _mapping(self) -> Mapping[str, Any]:
        return self._getter()

    def __getitem__(self, key: str) -> Any:
        return self._mapping()[key]

    def __iter__(self):
        return iter(self._mapping())

    def __len__(self) -> int:
        return len(self._mapping())

    def get(self, key: str, default: Any = None) -> Any:
        return self._mapping().get(key, default)

    def items(self):
        return self._mapping().items()

    def keys(self):
        return self._mapping().keys()

    def values(self):
        return self._mapping().values()


BENCHMARK_CATALOG = BenchmarkCatalog()


def load_benchmark_catalog(*, force_reload: bool = False) -> dict[str, Any]:
    return BENCHMARK_CATALOG.load(force_reload=force_reload)


def reload_benchmark_catalog() -> dict[str, Any]:
    return BENCHMARK_CATALOG.reload()


def validate_benchmark_catalog() -> dict[str, Any]:
    return BENCHMARK_CATALOG.validate()


BENCHMARK_CASE_LIBRARY: Mapping[str, dict[str, Any]] = _LazyCatalogMapping(lambda: BENCHMARK_CATALOG.case_library)
BENCHMARK_CASE_SET_CASES: Mapping[str, dict[str, list[dict[str, Any]]]] = _LazyCatalogMapping(lambda: BENCHMARK_CATALOG.case_set_cases)
QUERY_CASE_SETS: Mapping[str, dict[str, list[str]]] = _LazyCatalogMapping(lambda: BENCHMARK_CATALOG.query_case_sets)
CASE_SET_LABELS: Mapping[str, str] = _LazyCatalogMapping(lambda: BENCHMARK_CATALOG.labels)
CASE_SET_METADATA: Mapping[str, dict[str, Any]] = _LazyCatalogMapping(lambda: BENCHMARK_CATALOG.metadata)


def resolve_case_batch(case_set_id: str, length: str, query_count: int, *, module: str = "") -> list[dict[str, Any]]:
    normalized_case_set_id = str(case_set_id or "regression_v1").strip() or "regression_v1"
    normalized_length = str(length or "").strip().lower()
    normalized_module = str(module or "").strip().lower()
    case_set_cases = BENCHMARK_CATALOG.case_set_cases
    case_set = case_set_cases.get(normalized_case_set_id) or case_set_cases.get("regression_v1") or {}
    cases = list(case_set.get(normalized_length) or [])
    if normalized_module in _ALLOWED_MODULES:
        cases = [
            case for case in cases
            if normalized_module in {str(item or "").strip().lower() for item in list(case.get("supported_modules") or [])}
        ]
        prioritized = list(enumerate(cases))
        prioritized.sort(key=lambda item: (-int(item[1].get("selection_priority", 0) or 0), item[0]))
        cases = [case for _, case in prioritized]
    return cases[: max(0, min(int(query_count or 0), len(cases)))]