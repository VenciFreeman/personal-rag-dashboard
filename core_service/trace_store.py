from __future__ import annotations

from datetime import datetime
import json
import os
import threading
from pathlib import Path
from typing import Any


WORKSPACE_ROOT = Path(__file__).resolve().parent.parent
TRACE_RECORDS_DIR = WORKSPACE_ROOT / "nav_dashboard" / "data"
TRACE_RECORDS_LEGACY_FILE = TRACE_RECORDS_DIR / "trace_records.jsonl"
TRACE_RECORDS_JSON_FILE = TRACE_RECORDS_DIR / "trace_records.json"
TRACE_RECORDS_MAX = max(100, int(os.getenv("TRACE_RECORDS_MAX", "2000") or "2000"))
_LOCK = threading.Lock()


def _json_safe(value: Any) -> Any:
    try:
        return json.loads(json.dumps(value, ensure_ascii=False, default=str))
    except Exception:
        return str(value)


def _parse_trace_timestamp(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        normalized = text.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized)
    except Exception:
        return None


def _trace_month_key(record: dict[str, Any]) -> str:
    dt = _parse_trace_timestamp(record.get("timestamp")) or datetime.now()
    return dt.strftime("%Y_%m")


def _trace_records_file_for_month(month_key: str) -> Path:
    safe_key = str(month_key or "").strip() or datetime.now().strftime("%Y_%m")
    return TRACE_RECORDS_DIR / f"trace_records_{safe_key}.jsonl"


def list_trace_record_paths() -> list[Path]:
    monthly = sorted(
        [path for path in TRACE_RECORDS_DIR.glob("trace_records_*.jsonl") if path.is_file()],
        key=lambda path: path.name,
    )
    paths: list[Path] = []
    if TRACE_RECORDS_LEGACY_FILE.exists():
        paths.append(TRACE_RECORDS_LEGACY_FILE)
    paths.extend(monthly)
    if TRACE_RECORDS_JSON_FILE.exists() or paths:
        paths.append(TRACE_RECORDS_JSON_FILE)
    return paths


def _iter_trace_jsonl_files_locked() -> list[Path]:
    monthly = sorted(
        [path for path in TRACE_RECORDS_DIR.glob("trace_records_*.jsonl") if path.is_file()],
        key=lambda path: path.name,
    )
    files: list[Path] = []
    if TRACE_RECORDS_LEGACY_FILE.exists():
        files.append(TRACE_RECORDS_LEGACY_FILE)
    files.extend(monthly)
    return files


def write_trace_record(record: dict[str, Any]) -> None:
    if not isinstance(record, dict):
        return
    trace_id = str(record.get("trace_id", "") or "").strip()
    if not trace_id:
        return

    payload = _json_safe(record)
    payload["trace_id"] = trace_id

    with _LOCK:
        trace_file = _trace_records_file_for_month(_trace_month_key(payload))
        trace_file.parent.mkdir(parents=True, exist_ok=True)
        with trace_file.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=False) + "\n")
        records = _load_recent_trace_records_locked(limit=TRACE_RECORDS_MAX)
        records.append(payload)
        if len(records) > TRACE_RECORDS_MAX:
            records = records[-TRACE_RECORDS_MAX:]
        _write_trace_snapshot_locked(records)


def get_trace_record(trace_id: str) -> dict[str, Any] | None:
    value = str(trace_id or "").strip()
    if not value:
        return None

    with _LOCK:
        records = _load_trace_records_from_snapshot_locked()
        for row in reversed(records):
            if isinstance(row, dict) and str(row.get("trace_id", "") or "").strip() == value:
                return row
        for path in reversed(_iter_trace_jsonl_files_locked()):
            for row in reversed(_load_trace_records_from_jsonl_path_locked(path)):
                if isinstance(row, dict) and str(row.get("trace_id", "") or "").strip() == value:
                    return row
    return None


def render_trace_export(record: dict[str, Any]) -> str:
    trace_id = str(record.get("trace_id", "") or "")
    timestamp = str(record.get("timestamp", "") or "")
    entrypoint = str(record.get("entrypoint", "") or "")
    call_type = str(record.get("call_type", "") or "")
    session_id = str(record.get("session_id", "") or "")

    query_profile = record.get("query_profile") if isinstance(record.get("query_profile"), dict) else {}
    router = record.get("router") if isinstance(record.get("router"), dict) else {}
    retrieval = record.get("retrieval") if isinstance(record.get("retrieval"), dict) else {}
    ranking = record.get("ranking") if isinstance(record.get("ranking"), dict) else {}
    llm = record.get("llm") if isinstance(record.get("llm"), dict) else {}
    result = record.get("result") if isinstance(record.get("result"), dict) else {}
    tools = record.get("tools") if isinstance(record.get("tools"), list) else []
    stages = record.get("stages") if isinstance(record.get("stages"), dict) else {}

    total = _to_float(record.get("total_elapsed_seconds"))
    if total <= 0:
        total = _to_float(stages.get("wall_clock_seconds"))
    top1_before = _to_float(retrieval.get("top1_score_before_rerank"))
    top1_after = _to_float(retrieval.get("top1_score_after_rerank"))
    threshold = _to_float(retrieval.get("similarity_threshold"))
    rerank_delta = top1_after - top1_before if top1_before > 0 and top1_after > 0 else None
    threshold_margin = top1_after - threshold if top1_after > 0 and threshold > 0 else None

    lines = [
        f"Trace ID: {trace_id}",
        f"Timestamp: {timestamp}",
        f"Entrypoint: {entrypoint}",
        f"Call Type: {call_type}",
    ]
    if session_id:
        lines.append(f"Session ID: {session_id}")

    lines.extend([
        "",
        "[Query]",
        f"Profile: {query_profile.get('profile', '')}",
        f"Token Count: {query_profile.get('token_count', '')}",
        f"Search Mode: {record.get('search_mode', '')}",
        f"Query Type: {record.get('query_type', '')}",
        "",
        "[Router]",
        f"Selected Tool: {router.get('selected_tool', '')}",
        f"Planned Tools: {', '.join(str(item) for item in list(router.get('planned_tools') or []) if str(item).strip())}",
        f"Decision Category: {router.get('decision_category', '')}",
        f"Decision Path: {', '.join(str(item) for item in list(router.get('decision_path') or []) if str(item).strip())}",
        f"Planned Tool Depth: {router.get('planned_tool_depth', '')}",
        f"Executed Tool Depth: {router.get('executed_tool_depth', '')}",
        f"Classifier Label: {router.get('classifier_label', '')}",
        f"Doc Similarity: {_format_optional(router.get('doc_similarity'))}",
        f"Media Entity Confident: {router.get('media_entity_confident', '')}",
    ])

    lines.extend(["", "[Tools]"])
    if tools:
        for tool in tools:
            if not isinstance(tool, dict):
                continue
            name = str(tool.get("name", "") or "")
            status = str(tool.get("status", "") or "")
            latency_ms = _format_ms(tool.get("latency_ms"))
            result_count = tool.get("result_count")
            lines.append(f"- {name} | {status} | latency={latency_ms} | results={result_count if result_count is not None else ''}")
    else:
        lines.append("- none")

    lines.extend([
        "",
        "[Retrieval]",
        f"Vector Hits: {retrieval.get('vector_hits', '')}",
        f"Vector Candidates: {retrieval.get('vector_candidates', '')}",
        f"Similarity Threshold: {_format_optional(retrieval.get('similarity_threshold'))}",
        f"Query Rewrite Status: {retrieval.get('query_rewrite_status', '')}",
        f"Query Rewrite Count: {retrieval.get('query_rewrite_count', '')}",
        f"Graph Expansion Batches: {retrieval.get('graph_expansion_batches', '')}",
        "",
        "[Ranking]",
        f"Method: {ranking.get('method', '')}",
        f"Rerank K: {ranking.get('rerank_k', '')}",
        f"Rerank Candidate Count: {ranking.get('rerank_candidate_count', '')}",
        f"Rerank Candidate Profile: {ranking.get('rerank_candidate_profile', '')}",
        f"Fusion Alpha: {_format_optional(ranking.get('fusion_alpha'))}",
        f"Fusion Alpha Base: {_format_optional(ranking.get('fusion_alpha_base'))}",
        f"Dynamic Alpha Enabled: {ranking.get('dynamic_alpha_enabled', '')}",
        f"Rerank Soft Top1: {_format_optional(ranking.get('rerank_soft_top1'))}",
        f"Rerank Soft Top2: {_format_optional(ranking.get('rerank_soft_top2'))}",
        f"Rerank Soft Diff: {_format_optional(ranking.get('rerank_soft_diff'))}",
        f"Rerank Confidence Factor: {_format_optional(ranking.get('rerank_confidence_factor'))}",
        f"Fusion Alpha Reason: {ranking.get('fusion_alpha_reason', '')}",
        f"Rerank Optimization: {_format_optional(rerank_delta)}",
        f"Top1 Final Score: {_format_optional(ranking.get('top1_final_score'))}",
        f"Top1 Vector Delta: {_format_optional(ranking.get('top1_vector_delta'))}",
        f"Baseline Gap: {_format_optional(ranking.get('baseline_gap'))}",
        f"Threshold Margin: {_format_optional(threshold_margin)}",
        f"Identity Changed: {ranking.get('top1_identity_changed', '')}",
        f"Rank Shift: {_format_optional(ranking.get('top1_rank_shift'))}",
        f"Swap Blocked By Gap: {ranking.get('swap_blocked_by_gap', '')}",
        f"Guard Triggered: {ranking.get('guard_triggered', '')}",
        f"Guard Reason: {ranking.get('guard_reason', '')}",
        "",
        "[LLM]",
        f"Backend: {llm.get('backend', '')}",
        f"Model: {llm.get('model', '')}",
        f"Latency: {_format_seconds(llm.get('latency_seconds'))}",
        f"Input Tokens Est: {llm.get('input_tokens_est', '')}",
        f"Prompt Tokens Est: {llm.get('prompt_tokens_est', '')}",
        f"Context Tokens Est: {llm.get('context_tokens_est', '')}",
        f"Output Tokens Est: {llm.get('output_tokens_est', '')}",
        f"Calls: {llm.get('calls', '')}",
        "",
        "[Stages]",
    ])

    for key, value in stages.items():
        seconds = _to_float(value)
        ratio = ""
        if total > 0 and seconds >= 0:
            ratio = f" ({(seconds / total) * 100:.1f}%)"
        lines.append(f"- {key}: {_format_seconds(seconds)}{ratio}")

    lines.extend([
        "",
        "[Result]",
        f"Status: {result.get('status', '')}",
        f"No Context: {result.get('no_context', '')}",
        f"No Context Reason: {result.get('no_context_reason', '')}",
        f"Degraded To Retrieval: {result.get('degraded_to_retrieval', '')}",
    ])
    return "\n".join(lines).strip() + "\n"


def _load_recent_trace_records_locked(limit: int = TRACE_RECORDS_MAX) -> list[dict[str, Any]]:
    records = _load_trace_records_from_snapshot_locked()
    if records:
        return records[-max(1, int(limit)):]
    return _load_trace_records_from_all_jsonl_locked(limit=limit)


def _load_trace_records_from_snapshot_locked() -> list[dict[str, Any]]:
    if not TRACE_RECORDS_JSON_FILE.exists():
        return []
    try:
        payload = json.loads(TRACE_RECORDS_JSON_FILE.read_text(encoding="utf-8"))
    except Exception:
        return []
    if isinstance(payload, dict):
        rows = payload.get("records")
        if isinstance(rows, list):
            return [row for row in rows if isinstance(row, dict)]
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    return []


def _load_trace_records_from_jsonl_path_locked(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except Exception:
        return []
    records: list[dict[str, Any]] = []
    for line in lines:
        raw = str(line or "").strip()
        if not raw:
            continue
        try:
            row = json.loads(raw)
        except Exception:
            continue
        if isinstance(row, dict):
            records.append(row)
    return records


def _load_trace_records_from_all_jsonl_locked(limit: int | None = None) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for path in _iter_trace_jsonl_files_locked():
        records.extend(_load_trace_records_from_jsonl_path_locked(path))
    if limit is not None and len(records) > max(1, int(limit)):
        return records[-max(1, int(limit)):]
    return records


def _write_trace_snapshot_locked(records: list[dict[str, Any]]) -> None:
    payload = {
        "updated_at": datetime.now().isoformat(timespec="seconds"),
        "max_records": TRACE_RECORDS_MAX,
        "records": [_json_safe(item) for item in records if isinstance(item, dict)],
    }
    try:
        TRACE_RECORDS_JSON_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        return


def _to_float(value: Any) -> float:
    try:
        number = float(value)
        if number >= 0:
            return number
    except Exception:
        pass
    return 0.0


def _format_seconds(value: Any) -> str:
    number = _to_float(value)
    if number >= 1:
        return f"{number:.3f}s"
    if number > 0:
        return f"{number * 1000:.1f}ms"
    return "0s"


def _format_ms(value: Any) -> str:
    try:
        number = float(value)
    except Exception:
        return "0ms"
    if number >= 1000:
        return f"{number / 1000:.3f}s"
    if number > 0:
        return f"{number:.1f}ms"
    return "0ms"


def _format_optional(value: Any) -> str:
    if value is None or value == "":
        return ""
    try:
        return f"{float(value):.4f}"
    except Exception:
        return str(value)