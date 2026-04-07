from __future__ import annotations

import time
from datetime import datetime
from typing import Any, Callable
from uuid import uuid4

from nav_dashboard.web.services.agent.benchmark_case_catalog import resolve_case_batch


def derive_rag_no_context(resp: dict[str, Any], timings: dict[str, Any]) -> tuple[int, str]:
    local_after_threshold = timings.get("local_after_threshold")
    try:
        if local_after_threshold is not None and float(local_after_threshold) <= 0:
            return 1, "below_threshold"
    except Exception:
        pass
    top1 = timings.get("local_top1_vector_score_after_rerank")
    if top1 is None:
        top1 = timings.get("local_top1_score")
    threshold = resp.get("similarity_threshold")
    try:
        if top1 is not None and threshold is not None and float(top1) < float(threshold):
            return 1, "below_threshold"
    except Exception:
        pass
    return 0, ""


def run_rag_query(
    ai_base: str,
    case: dict[str, Any],
    *,
    post_json: Callable[[str, dict[str, Any], int], dict[str, Any]],
    benchmark_trace_id: Callable[[str], str],
) -> dict[str, Any]:
    question = str(case.get("query") or "").strip()
    url = f"{ai_base}/api/rag/ask"
    trace_id = benchmark_trace_id("rag")
    t0 = time.perf_counter()
    resp = post_json(
        url,
        {
            "question": question,
            "mode": "local",
            "search_mode": "local_only",
            "no_embed_cache": True,
            "benchmark_mode": True,
            "trace_id": trace_id,
        },
        180,
    )
    wall_clock = round(time.perf_counter() - t0, 3)
    error = resp.get("_error") or None
    timings = resp.get("timings") if isinstance(resp.get("timings"), dict) else {}
    elapsed = float(resp.get("elapsed_seconds") or 0.0)
    no_context, no_context_reason = derive_rag_no_context(resp, timings)
    return {
        "trace_id": str(resp.get("trace_id") or trace_id),
        "wall_clock_s": wall_clock,
        "elapsed_s": elapsed,
        "error": error,
        "timings": timings,
        "planned_tools": ["query_document_rag"],
        "no_context": no_context,
        "no_context_reason": no_context_reason,
        "doc_top1_score": timings.get("local_top1_vector_score_after_rerank", timings.get("local_top1_score")),
        "doc_score_threshold": resp.get("similarity_threshold"),
    }


def run_agent_query(
    self_base: str,
    case: dict[str, Any],
    *,
    search_mode: str,
    post_json: Callable[[str, dict[str, Any], int], dict[str, Any]],
    benchmark_trace_id: Callable[[str], str],
    extract_agent_benchmark_metrics: Callable[[dict[str, Any]], dict[str, Any]],
    evaluate_quality_assertions: Callable[[dict[str, Any], dict[str, Any]], tuple[list[dict[str, Any]], dict[str, bool], dict[str, bool]]],
) -> dict[str, Any]:
    question = str(case.get("query") or "").strip()
    history = case.get("history") if isinstance(case.get("history"), list) else []
    url = f"{self_base}/api/agent/chat"
    trace_id = benchmark_trace_id("agent")
    t0 = time.perf_counter()
    resp = post_json(
        url,
        {
            "question": question,
            "history": history,
            "backend": "local",
            "search_mode": search_mode,
            "deny_over_quota": True,
            "benchmark_mode": True,
            "trace_id": trace_id,
        },
        300,
    )
    wall_clock = round(time.perf_counter() - t0, 3)
    metrics = extract_agent_benchmark_metrics(resp)
    error = resp.get("_error") or metrics.get("error") or None
    timings = metrics.get("timings") if isinstance(metrics.get("timings"), dict) else {}
    doc_data = metrics.get("doc_data") if isinstance(metrics.get("doc_data"), dict) else {}
    quality_checks, quality_flags, quality_flag_applicability = evaluate_quality_assertions(case, metrics)
    return {
        "trace_id": str(metrics.get("trace_id") or resp.get("trace_id") or trace_id),
        "wall_clock_s": wall_clock,
        "elapsed_s": wall_clock,
        "error": error,
        "timings": timings,
        "no_context": int(metrics.get("no_context", 0) or 0),
        "no_context_reason": str(metrics.get("no_context_reason") or "").strip(),
        "query_type": str(metrics.get("query_type") or "").strip(),
        "query_class": str(metrics.get("query_class") or "").strip(),
        "subject_scope": str(metrics.get("subject_scope") or "").strip(),
        "time_scope_type": str(metrics.get("time_scope_type") or "").strip(),
        "answer_shape": str(metrics.get("answer_shape") or "").strip(),
        "media_family": str(metrics.get("media_family") or "").strip(),
        "followup_mode": str(metrics.get("followup_mode") or "").strip(),
        "strict_scope_active": bool(metrics.get("strict_scope_active")),
        "planned_tools": [str(item).strip() for item in list(metrics.get("planned_tools") or []) if str(item).strip()],
        "doc_top1_score": doc_data.get("doc_top1_score") if isinstance(doc_data, dict) else None,
        "doc_score_threshold": timings.get("doc_score_threshold"),
        "answer": str(metrics.get("answer") or ""),
        "answer_empty": bool(metrics.get("answer_empty")),
        "references_monotonic": bool(metrics.get("references_monotonic")),
        "reference_order_violation": bool(metrics.get("reference_order_violation")),
        "resolved_question": str(metrics.get("resolved_question") or ""),
        "rewritten_query_blob": str(metrics.get("rewritten_query_blob") or ""),
        "answer_guardrail_mode": str(metrics.get("answer_guardrail_mode") or ""),
        "returned_result_count": int(metrics.get("returned_result_count", 0) or 0),
        "strict_scope_false_negative": bool(metrics.get("strict_scope_false_negative")),
        "quality_checks": quality_checks,
        "quality_pass": all(bool(item.get("passed")) for item in quality_checks) if quality_checks else True,
        "quality_flags": quality_flags,
        "quality_flag_applicability": quality_flag_applicability,
        "quality_check_count": len(quality_checks),
    }


def resolve_case_batch_for_chain(case_set_id: str, length: str, query_count: int, chain: str) -> list[dict[str, Any]]:
    return resolve_case_batch(case_set_id, length, query_count, module=chain)


def select_total_case_batches(
    benchmark_lengths: list[str],
    cases_by_length: dict[str, list[dict[str, Any]]],
    query_count: int,
) -> dict[str, list[dict[str, Any]]]:
    target_total = max(0, min(20, int(query_count or 0)))
    selected: dict[str, list[dict[str, Any]]] = {length: [] for length in benchmark_lengths}
    if target_total <= 0:
        return selected

    remaining: dict[str, list[dict[str, Any]]] = {
        length: list(cases_by_length.get(length) or [])
        for length in benchmark_lengths
    }
    selected_total = 0
    while selected_total < target_total:
        progressed = False
        for length in benchmark_lengths:
            bucket = remaining.get(length) or []
            if not bucket:
                continue
            selected[length].append(bucket.pop(0))
            selected_total += 1
            progressed = True
            if selected_total >= target_total:
                break
        if not progressed:
            break
    return selected


def resolve_case_batches_for_chain(
    benchmark_lengths: list[str],
    case_set_id: str,
    query_count: int,
    chain: str,
) -> dict[str, list[dict[str, Any]]]:
    available = {
        length: list(resolve_case_batch_for_chain(case_set_id, length, 9999, chain))
        for length in benchmark_lengths
    }
    return select_total_case_batches(benchmark_lengths, available, query_count)


def benchmark_case_detail(case: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(case.get("id") or ""),
        "query": str(case.get("query") or ""),
        "source": str(case.get("source") or ""),
        "taxonomy": str(case.get("taxonomy") or ""),
        "expected_query_type": str(case.get("expected_query_type") or ""),
        "expected_domain": str(case.get("expected_domain") or ""),
        "expected_arbitration": str(case.get("expected_arbitration") or ""),
        "expected_query_class": str(case.get("expected_query_class") or ""),
        "subject_scope": str(case.get("subject_scope") or ""),
        "time_scope_type": str(case.get("time_scope_type") or ""),
        "answer_shape": str(case.get("answer_shape") or ""),
        "media_family": str(case.get("media_family") or ""),
        "followup_mode": str(case.get("followup_mode") or ""),
        "strict_scope_expected": case.get("strict_scope_expected"),
        "supported_modules": [str(item).strip() for item in list(case.get("supported_modules") or []) if str(item).strip()],
        "expected_tools": [str(item).strip() for item in list(case.get("expected_tools") or []) if str(item).strip()],
        "quality_assertions": dict(case.get("quality_assertions") or {}) if isinstance(case.get("quality_assertions"), dict) else {},
        "notes": str(case.get("notes") or ""),
    }


def build_case_details(benchmark_lengths: list[str], rows_by_length: dict[str, list[dict[str, Any]]]) -> dict[str, list[dict[str, Any]]]:
    return {
        length: [benchmark_case_detail(case) for case in list(rows_by_length.get(length) or [])]
        for length in benchmark_lengths
    }


def merge_case_batches_by_length(
    benchmark_lengths: list[str],
    cases_by_chain: dict[str, dict[str, list[dict[str, Any]]]],
) -> dict[str, list[dict[str, Any]]]:
    merged: dict[str, list[dict[str, Any]]] = {length: [] for length in benchmark_lengths}
    seen_ids: dict[str, set[str]] = {length: set() for length in benchmark_lengths}
    for rows_by_length in cases_by_chain.values():
        for length in benchmark_lengths:
            for case in list(rows_by_length.get(length) or []):
                case_id = str(case.get("id") or "").strip()
                if case_id and case_id in seen_ids[length]:
                    continue
                if case_id:
                    seen_ids[length].add(case_id)
                merged[length].append(case)
    return merged


def normalize_cases_by_chain_input(
    benchmark_lengths: list[str],
    chain_specs: dict[str, dict[str, str]],
    chains: list[str],
    cases_payload: dict[str, Any],
) -> dict[str, dict[str, list[dict[str, Any]]]]:
    if all(length in cases_payload for length in benchmark_lengths):
        default_chain = next((chain for chain in chains if chain in chain_specs), chains[0] if chains else "agent")
        return {
            default_chain: {
                length: list(cases_payload.get(length) or [])
                for length in benchmark_lengths
            }
        }
    normalized: dict[str, dict[str, list[dict[str, Any]]]] = {}
    for chain, rows_by_length in cases_payload.items():
        if not isinstance(rows_by_length, dict):
            continue
        normalized[str(chain)] = {
            length: list(rows_by_length.get(length) or [])
            for length in benchmark_lengths
        }
    return normalized


def build_result(
    benchmark_lengths: list[str],
    chain_specs: dict[str, dict[str, str]],
    chains: list[str],
    query_count: int,
    case_set_id: str,
    cases_by_chain: dict[str, dict[str, list[dict[str, Any]]]],
    rag_recs: dict[str, list[dict[str, Any]]],
    chain_records: dict[str, dict[str, list[dict[str, Any]]]],
    *,
    compact_benchmark_record: Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]],
    group_rows_by_taxonomy: Callable[[dict[str, list[dict[str, Any]]]], dict[str, list[dict[str, Any]]]],
    aggregate_records: Callable[[list[dict[str, Any]]], dict[str, Any]],
    attach_assertions: Callable[[dict[str, Any], list[str]], dict[str, Any]],
) -> dict[str, Any]:
    cases_by_chain = normalize_cases_by_chain_input(benchmark_lengths, chain_specs, chains, cases_by_chain)
    merged_cases_by_length = merge_case_batches_by_length(benchmark_lengths, cases_by_chain)
    case_details_by_length = build_case_details(benchmark_lengths, merged_cases_by_length)
    case_details_by_taxonomy = group_rows_by_taxonomy(case_details_by_length)
    case_details_by_chain = {
        chain: build_case_details(benchmark_lengths, cases_by_chain.get(chain, {}))
        for chain in chains
    }
    result: dict[str, Any] = {
        "id": str(uuid4())[:8],
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "config": {"modules": chains, "query_count_per_type": query_count, "case_set_id": case_set_id, "query_count_mode": "total"},
        "cases": {
            length: [str(case.get("query") or "") for case in list(merged_cases_by_length.get(length) or [])]
            for length in benchmark_lengths
        },
        "case_details": case_details_by_length,
        "case_details_by_taxonomy": case_details_by_taxonomy,
        "taxonomy_counts": {taxonomy: len(rows) for taxonomy, rows in case_details_by_taxonomy.items()},
        "case_details_by_chain": case_details_by_chain,
    }
    if "rag" in chains:
        rag_cases_by_length = cases_by_chain.get("rag", {})
        rag_records_by_length = {
            length: [compact_benchmark_record(case, rec) for case, rec in zip(rag_cases_by_length.get(length, []), rag_recs[length])]
            for length in benchmark_lengths
        }
        rag_metrics_by_taxonomy = group_rows_by_taxonomy(
            {
                length: [dict(rec, taxonomy=str(case.get("taxonomy") or "")) for case, rec in zip(rag_cases_by_length.get(length, []), rag_recs[length])]
                for length in benchmark_lengths
            }
        )
        rag_records_by_taxonomy = group_rows_by_taxonomy(rag_records_by_length)
        all_rag = [r for length in benchmark_lengths for r in rag_recs[length]]
        result["rag"] = {
            "by_length": {length: aggregate_records(rag_recs[length]) for length in benchmark_lengths},
            "global": aggregate_records(all_rag),
            "by_taxonomy": {taxonomy: aggregate_records(rows) for taxonomy, rows in rag_metrics_by_taxonomy.items()},
            "records_by_length": rag_records_by_length,
            "records_by_taxonomy": rag_records_by_taxonomy,
        }
    for chain in ("agent", "hybrid"):
        if chain not in chains:
            continue
        per_length = chain_records.get(chain, {})
        chain_cases_by_length = cases_by_chain.get(chain, {})
        records_by_length = {
            length: [
                compact_benchmark_record(case, rec)
                for case, rec in zip(chain_cases_by_length.get(length, []), per_length.get(length, []))
            ]
            for length in benchmark_lengths
        }
        metrics_by_taxonomy = group_rows_by_taxonomy(
            {
                length: [dict(rec, taxonomy=str(case.get("taxonomy") or "")) for case, rec in zip(chain_cases_by_length.get(length, []), per_length.get(length, []))]
                for length in benchmark_lengths
            }
        )
        records_by_taxonomy = group_rows_by_taxonomy(records_by_length)
        all_rows = [r for length in benchmark_lengths for r in per_length.get(length, [])]
        result[chain] = {
            "by_length": {length: aggregate_records(per_length.get(length, [])) for length in benchmark_lengths},
            "global": aggregate_records(all_rows),
            "by_taxonomy": {taxonomy: aggregate_records(rows) for taxonomy, rows in metrics_by_taxonomy.items()},
            "records_by_length": records_by_length,
            "records_by_taxonomy": records_by_taxonomy,
        }
    return attach_assertions(result, chains)


def run_benchmark(
    benchmark_lengths: list[str],
    chain_specs: dict[str, dict[str, str]],
    chains: list[str],
    query_count: int,
    ai_base: str,
    self_base: str,
    case_set_id: str,
    *,
    run_rag_query_fn: Callable[[str, dict[str, Any]], dict[str, Any]],
    run_agent_query_fn: Callable[[str, dict[str, Any], str], dict[str, Any]],
    build_result_fn: Callable[[list[str], int, str, dict[str, dict[str, list[dict[str, Any]]]], dict[str, list[dict[str, Any]]], dict[str, dict[str, list[dict[str, Any]]]]], dict[str, Any]],
    save_result_fn: Callable[[dict[str, Any]], None],
):
    cases_by_chain: dict[str, dict[str, list[dict[str, Any]]]] = {
        chain: resolve_case_batches_for_chain(benchmark_lengths, case_set_id, query_count, chain)
        for chain in chains
    }
    total = sum(len(cases_by_chain.get(chain, {}).get(length, [])) for chain in chains for length in benchmark_lengths)
    done = 0
    rag_recs: dict[str, list[dict[str, Any]]] = {length: [] for length in benchmark_lengths}
    chain_records: dict[str, dict[str, list[dict[str, Any]]]] = {
        chain: {length: [] for length in benchmark_lengths}
        for chain in chains
        if chain in {"agent", "hybrid"}
    }

    yield {"type": "progress", "message": f"准备运行 {total} 项测试...", "current": 0, "total": total}

    for length in benchmark_lengths:
        if "rag" in chains:
            cases = list(cases_by_chain.get("rag", {}).get(length) or [])
            for index, case in enumerate(cases, start=1):
                q = str(case.get("query") or "")
                label = q[:22] + ("..." if len(q) > 22 else "")
                yield {"type": "progress", "message": f"[RAG / {length}] {index}/{len(cases)}: {label}", "current": done, "total": total}
                rec = run_rag_query_fn(ai_base, case)
                rag_recs[length].append(rec)
                done += 1

        for chain in [c for c in chains if c in {"agent", "hybrid"}]:
            chain_label = chain_specs.get(chain, {}).get("label", chain)
            search_mode = chain_specs.get(chain, {}).get("search_mode", "local_only")
            cases = list(cases_by_chain.get(chain, {}).get(length) or [])
            for index, case in enumerate(cases, start=1):
                q = str(case.get("query") or "")
                label = q[:22] + ("..." if len(q) > 22 else "")
                yield {"type": "progress", "message": f"[{chain_label} / {length}] {index}/{len(cases)}: {label}", "current": done, "total": total}
                rec = run_agent_query_fn(self_base, case, search_mode)
                chain_records.setdefault(chain, {}).setdefault(length, []).append(rec)
                done += 1

    result = build_result_fn(chains, query_count, case_set_id, cases_by_chain, rag_recs, chain_records)
    save_result_fn(result)
    yield {"type": "result", "data": result, "current": total, "total": total}


def run_benchmark_job(
    benchmark_lengths: list[str],
    chain_specs: dict[str, dict[str, str]],
    chains: list[str],
    query_count: int,
    ai_base: str,
    self_base: str,
    case_set_id: str,
    *,
    report_progress: Callable[..., None],
    is_cancelled: Callable[[], bool],
    run_rag_query_fn: Callable[[str, dict[str, Any]], dict[str, Any]],
    run_agent_query_fn: Callable[[str, dict[str, Any], str], dict[str, Any]],
    compact_benchmark_record: Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]],
    build_result_fn: Callable[[list[str], int, str, dict[str, dict[str, list[dict[str, Any]]]], dict[str, list[dict[str, Any]]], dict[str, dict[str, list[dict[str, Any]]]]], dict[str, Any]],
    save_result_fn: Callable[[dict[str, Any]], None],
) -> dict[str, Any]:
    cases_by_chain: dict[str, dict[str, list[dict[str, Any]]]] = {
        chain: resolve_case_batches_for_chain(benchmark_lengths, case_set_id, query_count, chain)
        for chain in chains
    }
    total = sum(len(cases_by_chain.get(chain, {}).get(length, [])) for chain in chains for length in benchmark_lengths)
    done = 0
    rag_recs: dict[str, list[dict[str, Any]]] = {length: [] for length in benchmark_lengths}
    chain_records: dict[str, dict[str, list[dict[str, Any]]]] = {
        chain: {length: [] for length in benchmark_lengths}
        for chain in chains
        if chain in {"agent", "hybrid"}
    }
    trace_log_lines: list[str] = []
    report_progress(message=f"准备运行 {total} 项测试...", current=0, total=total, log=f"准备运行 {total} 项测试")

    for length in benchmark_lengths:
        if "rag" in chains:
            cases = list(cases_by_chain.get("rag", {}).get(length) or [])
            case_total = len(cases)
            for index, case in enumerate(cases, start=1):
                q = str(case.get("query") or "")
                if is_cancelled():
                    report_progress(message="Benchmark 已取消", log="Benchmark 已取消")
                    return {"cancelled": True}
                label = q[:22] + ("..." if len(q) > 22 else "")
                message = f"[RAG / {length}] {index}/{case_total}: {label}"
                report_progress(message=message, current=done, total=total, log=message)
                rec = run_rag_query_fn(ai_base, case)
                rag_recs[length].append(rec)
                done += 1
                compact = compact_benchmark_record(case, rec)
                trace_line = f"TRACE_ID [RAG / {length} / {index}] {compact.get('trace_id', '')}"
                trace_log_lines.append(trace_line)
                report_progress(
                    current=done,
                    total=total,
                    log=trace_line,
                    metadata={
                        "latest_case": {
                            "module": "rag",
                            "length": length,
                            "case_index": index,
                            "case_total": len(cases),
                            "timestamp": datetime.now().isoformat(timespec="seconds"),
                            "record": compact,
                        },
                        "trace_ids": list(trace_log_lines),
                    },
                )

        for chain in [c for c in chains if c in {"agent", "hybrid"}]:
            chain_label = chain_specs.get(chain, {}).get("label", chain)
            search_mode = chain_specs.get(chain, {}).get("search_mode", "local_only")
            cases = list(cases_by_chain.get(chain, {}).get(length) or [])
            case_total = len(cases)
            for index, case in enumerate(cases, start=1):
                q = str(case.get("query") or "")
                if is_cancelled():
                    report_progress(message="Benchmark 已取消", log="Benchmark 已取消")
                    return {"cancelled": True}
                label = q[:22] + ("..." if len(q) > 22 else "")
                message = f"[{chain_label} / {length}] {index}/{case_total}: {label}"
                report_progress(message=message, current=done, total=total, log=message)
                rec = run_agent_query_fn(self_base, case, search_mode)
                chain_records.setdefault(chain, {}).setdefault(length, []).append(rec)
                done += 1
                compact = compact_benchmark_record(case, rec)
                trace_line = f"TRACE_ID [{chain_label} / {length} / {index}] {compact.get('trace_id', '')}"
                trace_log_lines.append(trace_line)
                report_progress(
                    current=done,
                    total=total,
                    log=trace_line,
                    metadata={
                        "latest_case": {
                            "module": chain,
                            "length": length,
                            "case_index": index,
                            "case_total": len(cases),
                            "timestamp": datetime.now().isoformat(timespec="seconds"),
                            "record": compact,
                        },
                        "trace_ids": list(trace_log_lines),
                    },
                )

    result = build_result_fn(chains, query_count, case_set_id, cases_by_chain, rag_recs, chain_records)
    save_result_fn(result)
    report_progress(message="Benchmark 完成", current=total, total=total, result=result, log="Benchmark 完成")
    return result