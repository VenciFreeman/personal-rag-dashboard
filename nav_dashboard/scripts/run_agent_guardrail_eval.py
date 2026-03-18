from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

WORKSPACE_ROOT = Path(__file__).resolve().parents[2]
if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))

from nav_dashboard.web.services.agent_service import run_agent_round


DEFAULT_CASES_PATH = WORKSPACE_ROOT / "nav_dashboard" / "data" / "evals" / "agent_guardrail_cases.json"
DEFAULT_OUTPUT_PATH = WORKSPACE_ROOT / "nav_dashboard" / "data" / "evals" / "agent_guardrail_last_run.json"


HISTORY_TEMPLATES: dict[str, list[dict[str, str]]] = {
    "anime_window_2025": [
        {"role": "system", "content": "你好，我可以帮你并行查询文档与媒体记录。"},
        {"role": "user", "content": "2025年7月-10月的动画番剧我看过哪些，评价比较好的有哪些？"},
        {"role": "assistant", "content": "根据查询结果，您在2025年10月19日观看了一部名为《小城日常》的动画番剧。该作品获得了9.0分的高评价。"},
    ],
    "nolan_trilogy": [
        {"role": "system", "content": "你好，我可以帮你并行查询文档与媒体记录。"},
        {"role": "user", "content": "诺兰的蝙蝠侠三部曲"},
        {"role": "assistant", "content": "诺兰的蝙蝠侠三部曲包括《蝙蝠侠：侠影之谜》《黑暗骑士》《黑暗骑士崛起》。"},
    ],
    "interstellar_intro": [
        {"role": "system", "content": "你好，我可以帮你并行查询文档与媒体记录。"},
        {"role": "user", "content": "《星际穿越》剧情简介"},
        {"role": "assistant", "content": "《星际穿越》讲述了人类在地球生态崩坏背景下寻找新家园的故事。"},
    ],
}


def _load_cases(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        return [item for item in payload.get("cases", []) if isinstance(item, dict)]
    return [item for item in payload if isinstance(item, dict)]


def _history_for_case(case: dict[str, Any]) -> list[dict[str, str]]:
    template_name = str(case.get("history_template", "") or "").strip()
    if template_name:
        return [dict(item) for item in HISTORY_TEMPLATES.get(template_name, [])]
    history = case.get("history")
    if isinstance(history, list):
        return [dict(item) for item in history if isinstance(item, dict)]
    return []


def _subset_matches(expected: Any, actual: Any) -> bool:
    if isinstance(expected, dict):
        if not isinstance(actual, dict):
            return False
        return all(_subset_matches(value, actual.get(key)) for key, value in expected.items())
    if isinstance(expected, list):
        if not isinstance(actual, list):
            return False
        return all(any(_subset_matches(item, candidate) for candidate in actual) for item in expected)
    return expected == actual


def _contains_any(text: str, needles: list[str]) -> bool:
    return any(str(needle) in text for needle in needles)


def _contains_all(text: str, needles: list[str]) -> bool:
    return all(str(needle) in text for needle in needles)


def _tool_map(result: dict[str, Any]) -> dict[str, dict[str, Any]]:
    mapped: dict[str, dict[str, Any]] = {}
    for item in result.get("tool_results", []):
        if not isinstance(item, dict):
            continue
        name = str(item.get("tool", "") or "").strip()
        if name:
            mapped[name] = item
    return mapped


def _query_understanding(result: dict[str, Any]) -> dict[str, Any]:
    payload = result.get("query_understanding")
    return payload if isinstance(payload, dict) else {}


def _planner_snapshot(result: dict[str, Any]) -> dict[str, Any]:
    payload = result.get("planner_snapshot")
    return payload if isinstance(payload, dict) else {}


def _media_rows(result: dict[str, Any]) -> list[dict[str, Any]]:
    media_tool = _tool_map(result).get("query_media_record", {})
    data = media_tool.get("data") if isinstance(media_tool.get("data"), dict) else {}
    rows = data.get("results") if isinstance(data.get("results"), list) else []
    return [row for row in rows if isinstance(row, dict)]


def _media_validation(result: dict[str, Any]) -> dict[str, Any]:
    media_tool = _tool_map(result).get("query_media_record", {})
    data = media_tool.get("data") if isinstance(media_tool.get("data"), dict) else {}
    validation = data.get("validation") if isinstance(data.get("validation"), dict) else {}
    return validation


def _candidate_sources(result: dict[str, Any]) -> dict[str, Any]:
    media_tool = _tool_map(result).get("query_media_record", {})
    data = media_tool.get("data") if isinstance(media_tool.get("data"), dict) else {}
    payload = data.get("candidate_source_breakdown") if isinstance(data.get("candidate_source_breakdown"), dict) else {}
    return payload


def _record_check(messages: list[str], ok: bool, label: str, detail: str) -> bool:
    prefix = "PASS" if ok else "FAIL"
    messages.append(f"{prefix}: {label} - {detail}")
    return ok


def _evaluate_understanding(case: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
    expected = ((case.get("expect") or {}).get("understanding") or {})
    if not expected:
        return {"enabled": False, "passed": True, "checks": []}
    checks: list[str] = []
    passed = True
    understanding = _query_understanding(result)
    planner = _planner_snapshot(result)
    if "detected_followup" in expected:
        passed &= _record_check(checks, bool(result.get("detected_followup")) == bool(expected.get("detected_followup")), "detected_followup", f"expected={expected.get('detected_followup')} actual={result.get('detected_followup')}")
    if expected.get("resolved_question_contains"):
        actual = str(understanding.get("resolved_question") or (result.get("query_classification") or {}).get("resolved_question", "") or "")
        value = str(expected.get("resolved_question_contains"))
        passed &= _record_check(checks, value in actual, "resolved_question_contains", value)
    if expected.get("lookup_mode"):
        actual = str(understanding.get("lookup_mode") or planner.get("lookup_mode") or "")
        value = str(expected.get("lookup_mode"))
        passed &= _record_check(checks, actual == value, "lookup_mode", f"expected={value} actual={actual}")
    if expected.get("selection"):
        actual = understanding.get("selection") if isinstance(understanding.get("selection"), dict) else planner.get("selection", {})
        value = expected.get("selection")
        passed &= _record_check(checks, _subset_matches(value, actual), "selection", json.dumps(value, ensure_ascii=False))
    if expected.get("time_constraint"):
        actual = understanding.get("time_constraint") if isinstance(understanding.get("time_constraint"), dict) else planner.get("time_constraint", {})
        value = expected.get("time_constraint")
        passed &= _record_check(checks, _subset_matches(value, actual), "time_constraint", json.dumps(value, ensure_ascii=False))
    if expected.get("ranking"):
        actual = understanding.get("ranking") if isinstance(understanding.get("ranking"), dict) else ((planner.get("soft_constraints") or {}).get("ranking", {}) if isinstance(planner.get("soft_constraints"), dict) else {})
        value = expected.get("ranking")
        passed &= _record_check(checks, _subset_matches(value, actual), "ranking", json.dumps(value, ensure_ascii=False))
    if expected.get("planner_followup_mode"):
        actual = str(planner.get("followup_mode", "") or "")
        value = str(expected.get("planner_followup_mode"))
        passed &= _record_check(checks, actual == value, "planner_followup_mode", f"expected={value} actual={actual}")
    if expected.get("planner_hard_filters"):
        actual = planner.get("hard_filters", {})
        value = expected.get("planner_hard_filters")
        passed &= _record_check(checks, _subset_matches(value, actual), "planner_hard_filters", json.dumps(value, ensure_ascii=False))
    if expected.get("inheritance_applied"):
        actual = result.get("inheritance_applied", {})
        value = expected.get("inheritance_applied")
        passed &= _record_check(checks, _subset_matches(value, actual), "inheritance_applied", json.dumps(value, ensure_ascii=False))
    if expected.get("state_diff"):
        actual = result.get("state_diff", {})
        value = expected.get("state_diff")
        passed &= _record_check(checks, _subset_matches(value, actual), "state_diff", json.dumps(value, ensure_ascii=False))
    if expected.get("guardrail_flags"):
        actual = result.get("guardrail_flags", {})
        value = expected.get("guardrail_flags")
        passed &= _record_check(checks, _subset_matches(value, actual), "guardrail_flags", json.dumps(value, ensure_ascii=False))
    if expected.get("error_taxonomy"):
        actual = result.get("error_taxonomy", {})
        value = expected.get("error_taxonomy")
        passed &= _record_check(checks, _subset_matches(value, actual), "error_taxonomy", json.dumps(value, ensure_ascii=False))
    return {"enabled": True, "passed": bool(passed), "checks": checks}


def _evaluate_retrieval(case: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
    expected = ((case.get("expect") or {}).get("retrieval") or {})
    if not expected:
        return {"enabled": False, "passed": True, "checks": []}
    checks: list[str] = []
    passed = True
    planned_tools = [str(item.get("name", "") or "") for item in result.get("planned_tools", []) if isinstance(item, dict)]
    if expected.get("planned_tools_equals") is not None:
        value = list(expected.get("planned_tools_equals") or [])
        passed &= _record_check(checks, planned_tools == value, "planned_tools_equals", f"expected={value} actual={planned_tools}")
    if expected.get("planned_tools_contains"):
        value = [str(item) for item in expected.get("planned_tools_contains", [])]
        ok = all(item in planned_tools for item in value)
        passed &= _record_check(checks, ok, "planned_tools_contains", json.dumps(value, ensure_ascii=False))
    rows = _media_rows(result)
    if expected.get("min_results") is not None:
        value = int(expected.get("min_results"))
        passed &= _record_check(checks, len(rows) >= value, "min_results", f"expected>={value} actual={len(rows)}")
    if expected.get("max_results") is not None:
        value = int(expected.get("max_results"))
        passed &= _record_check(checks, len(rows) <= value, "max_results", f"expected<={value} actual={len(rows)}")
    if expected.get("result_media_types_all"):
        allowed = {str(item) for item in expected.get("result_media_types_all", [])}
        actual_types = {str(row.get("media_type", "") or "") for row in rows}
        ok = all(media_type in allowed for media_type in actual_types if media_type)
        passed &= _record_check(checks, ok, "result_media_types_all", f"allowed={sorted(allowed)} actual={sorted(actual_types)}")
    if expected.get("result_dates_within"):
        start, end = expected.get("result_dates_within")
        actual_dates = [str(row.get("date", "") or "") for row in rows if str(row.get("date", "") or "")]
        ok = all(start <= value <= end for value in actual_dates)
        passed &= _record_check(checks, ok, "result_dates_within", f"range={[start, end]} actual={actual_dates[:6]}")
    if expected.get("candidate_source_breakdown_min"):
        actual = _candidate_sources(result)
        minima = expected.get("candidate_source_breakdown_min") or {}
        ok = all(int(actual.get(key, 0) or 0) >= int(value) for key, value in minima.items())
        passed &= _record_check(checks, ok, "candidate_source_breakdown_min", json.dumps(minima, ensure_ascii=False))
    if expected.get("tool_result_count_max"):
        tools = _tool_map(result)
        maxima = expected.get("tool_result_count_max") or {}
        ok = True
        for tool_name, value in maxima.items():
            tool = tools.get(str(tool_name), {})
            data = tool.get("data") if isinstance(tool.get("data"), dict) else {}
            rows_for_tool = data.get("results") if isinstance(data.get("results"), list) else []
            if len(rows_for_tool) > int(value):
                ok = False
                break
        passed &= _record_check(checks, ok, "tool_result_count_max", json.dumps(maxima, ensure_ascii=False))
    return {"enabled": True, "passed": bool(passed), "checks": checks}


def _evaluate_validation(case: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
    expected = ((case.get("expect") or {}).get("validation") or {})
    if not expected:
        return {"enabled": False, "passed": True, "checks": []}
    checks: list[str] = []
    passed = True
    validation = _media_validation(result)
    if expected.get("dropped_by_validator_max") is not None:
        value = int(expected.get("dropped_by_validator_max"))
        actual = int(validation.get("dropped_by_validator", 0) or 0)
        passed &= _record_check(checks, actual <= value, "dropped_by_validator_max", f"expected<={value} actual={actual}")
    if expected.get("dropped_by_reference_limit_min") is not None:
        value = int(expected.get("dropped_by_reference_limit_min"))
        actual = int(validation.get("dropped_by_reference_limit", 0) or 0)
        passed &= _record_check(checks, actual >= value, "dropped_by_reference_limit_min", f"expected>={value} actual={actual}")
    if expected.get("returned_result_count_max") is not None:
        value = int(expected.get("returned_result_count_max"))
        actual = int(validation.get("returned_result_count", 0) or 0)
        passed &= _record_check(checks, actual <= value, "returned_result_count_max", f"expected<={value} actual={actual}")
    if expected.get("returned_result_count_min") is not None:
        value = int(expected.get("returned_result_count_min"))
        actual = int(validation.get("returned_result_count", 0) or 0)
        passed &= _record_check(checks, actual >= value, "returned_result_count_min", f"expected>={value} actual={actual}")
    if expected.get("validator_drop_reasons_min"):
        actual = validation.get("drop_reasons") if isinstance(validation.get("drop_reasons"), dict) else {}
        minima = expected.get("validator_drop_reasons_min") or {}
        ok = all(int(actual.get(key, 0) or 0) >= int(value) for key, value in minima.items())
        passed &= _record_check(checks, ok, "validator_drop_reasons_min", json.dumps(minima, ensure_ascii=False))
    if expected.get("reference_limit_drop_reasons_min"):
        actual = validation.get("reference_limit_drop_reasons") if isinstance(validation.get("reference_limit_drop_reasons"), dict) else {}
        minima = expected.get("reference_limit_drop_reasons_min") or {}
        ok = all(int(actual.get(key, 0) or 0) >= int(value) for key, value in minima.items())
        passed &= _record_check(checks, ok, "reference_limit_drop_reasons_min", json.dumps(minima, ensure_ascii=False))
    return {"enabled": True, "passed": bool(passed), "checks": checks}


def _evaluate_answer(case: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
    expected = ((case.get("expect") or {}).get("answer") or {})
    if not expected:
        return {"enabled": False, "passed": True, "checks": []}
    checks: list[str] = []
    passed = True
    answer = str(result.get("answer", "") or "")
    answer_mode = result.get("answer_guardrail_mode") if isinstance(result.get("answer_guardrail_mode"), dict) else {}
    if expected.get("mode"):
        value = str(expected.get("mode"))
        actual = str(answer_mode.get("mode", "") or "")
        passed &= _record_check(checks, actual == value, "answer_mode", f"expected={value} actual={actual}")
    if expected.get("mode_reasons_contains"):
        value = [str(item) for item in expected.get("mode_reasons_contains", [])]
        actual = [str(item) for item in (answer_mode.get("reasons") or [])]
        ok = all(item in actual for item in value)
        passed &= _record_check(checks, ok, "answer_mode_reasons_contains", json.dumps(value, ensure_ascii=False))
    if expected.get("contains_any"):
        value = [str(item) for item in expected.get("contains_any", [])]
        passed &= _record_check(checks, _contains_any(answer, value), "answer_contains_any", json.dumps(value, ensure_ascii=False))
    if expected.get("contains_all"):
        value = [str(item) for item in expected.get("contains_all", [])]
        passed &= _record_check(checks, _contains_all(answer, value), "answer_contains_all", json.dumps(value, ensure_ascii=False))
    if expected.get("not_contains"):
        value = [str(item) for item in expected.get("not_contains", [])]
        ok = all(item not in answer for item in value)
        passed &= _record_check(checks, ok, "answer_not_contains", json.dumps(value, ensure_ascii=False))
    return {"enabled": True, "passed": bool(passed), "checks": checks}


def _score_layers(layer_results: dict[str, dict[str, Any]]) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for layer_name, payload in layer_results.items():
        summary[layer_name] = {
            "enabled": bool(payload.get("enabled")),
            "passed": bool(payload.get("passed")),
            "check_count": len(payload.get("checks", [])),
        }
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Run layered regression evals for nav_dashboard agent guardrails.")
    parser.add_argument("--cases", default=str(DEFAULT_CASES_PATH), help="Path to eval case JSON file")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT_PATH), help="Path to write JSON summary")
    parser.add_argument("--case-ids", nargs="*", default=[], help="Optional subset of case ids to run")
    args = parser.parse_args()

    case_path = Path(args.cases)
    output_path = Path(args.output)
    requested_ids = {str(item).strip() for item in args.case_ids if str(item).strip()}
    cases = _load_cases(case_path)
    if requested_ids:
        cases = [case for case in cases if str(case.get("id", "") or "") in requested_ids]

    run_results: list[dict[str, Any]] = []
    layer_totals = {
        "understanding": {"enabled": 0, "passed": 0},
        "retrieval": {"enabled": 0, "passed": 0},
        "validation": {"enabled": 0, "passed": 0},
        "answer": {"enabled": 0, "passed": 0},
    }

    for case in cases:
        result = run_agent_round(
            question=str(case.get("question", "") or ""),
            session_id="",
            history=_history_for_case(case),
            backend=str(case.get("backend", "local") or "local"),
            search_mode=str(case.get("search_mode", "local_only") or "local_only"),
            benchmark_mode=True,
        )
        layer_results = {
            "understanding": _evaluate_understanding(case, result),
            "retrieval": _evaluate_retrieval(case, result),
            "validation": _evaluate_validation(case, result),
            "answer": _evaluate_answer(case, result),
        }
        for layer_name, payload in layer_results.items():
            if payload.get("enabled"):
                layer_totals[layer_name]["enabled"] += 1
                if payload.get("passed"):
                    layer_totals[layer_name]["passed"] += 1
        run_results.append(
            {
                "id": case.get("id"),
                "category": case.get("category"),
                "question": case.get("question"),
                "trace_id": result.get("trace_id"),
                "passed": all(payload.get("passed") for payload in layer_results.values() if payload.get("enabled")),
                "layers": _score_layers(layer_results),
                "checks": {name: payload.get("checks", []) for name, payload in layer_results.items() if payload.get("enabled")},
            }
        )

    overall_passed = sum(1 for item in run_results if item.get("passed"))
    payload = {
        "case_file": str(case_path),
        "case_count": len(run_results),
        "passed_case_count": overall_passed,
        "failed_case_count": max(0, len(run_results) - overall_passed),
        "layer_totals": layer_totals,
        "results": run_results,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if overall_passed == len(run_results) else 1


if __name__ == "__main__":
    raise SystemExit(main())