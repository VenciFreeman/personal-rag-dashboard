from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

WORKSPACE_ROOT = Path(__file__).resolve().parents[2]
if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))

from nav_dashboard.web.services.agent_service import RoutingPolicy, _build_router_decision, _resolve_query_profile


DEFAULT_CASES_PATH = WORKSPACE_ROOT / "nav_dashboard" / "data" / "evals" / "router_eval_cases.json"
DEFAULT_OUTPUT_PATH = WORKSPACE_ROOT / "nav_dashboard" / "data" / "evals" / "router_eval_last_run.json"


def _load_payload(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {"cases": []}


def _history_for_case(case: dict[str, Any], templates: dict[str, list[dict[str, str]]]) -> list[dict[str, str]]:
    template_name = str(case.get("history_template", "") or "").strip()
    if template_name:
        return [dict(item) for item in templates.get(template_name, []) if isinstance(item, dict)]
    history = case.get("history")
    if isinstance(history, list):
        return [dict(item) for item in history if isinstance(item, dict)]
    return []


def _score_match(expected: Any, actual: Any) -> bool:
    if isinstance(expected, list):
        return list(actual or []) == expected
    return actual == expected


def _record_check(checks: list[str], label: str, ok: bool, expected: Any, actual: Any) -> bool:
    checks.append(f"{'PASS' if ok else 'FAIL'}: {label} expected={expected!r} actual={actual!r}")
    return ok


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate nav_dashboard router understanding and policy without executing tools.")
    parser.add_argument("--cases", default=str(DEFAULT_CASES_PATH), help="Path to router eval case JSON")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT_PATH), help="Path to write router eval summary")
    parser.add_argument("--case-ids", nargs="*", default=[], help="Optional subset of case ids")
    args = parser.parse_args()

    payload = _load_payload(Path(args.cases))
    templates = payload.get("history_templates", {}) if isinstance(payload.get("history_templates"), dict) else {}
    cases = [item for item in payload.get("cases", []) if isinstance(item, dict)]
    requested_ids = {str(item).strip() for item in args.case_ids if str(item).strip()}
    if requested_ids:
        cases = [case for case in cases if str(case.get("id", "") or "") in requested_ids]

    metric_totals = {
        "domain": {"correct": 0, "total": 0},
        "intent": {"correct": 0, "total": 0},
        "followup_mode": {"correct": 0, "total": 0},
        "planned_tools": {"correct": 0, "total": 0},
        "query_type": {"correct": 0, "total": 0},
    }
    results: list[dict[str, Any]] = []

    for case in cases:
        question = str(case.get("question", "") or "")
        history = _history_for_case(case, templates)
        search_mode = str(case.get("search_mode", "local_only") or "local_only")
        query_profile = _resolve_query_profile(question)
        decision, _, _ = _build_router_decision(question, history, {}, query_profile)
        plan = RoutingPolicy().build_plan(decision, search_mode)
        expected = case.get("expect") if isinstance(case.get("expect"), dict) else {}
        planned_tool_names = [call.name for call in plan.planned_tools]
        checks: list[str] = []
        passed = True

        for field, actual in (
            ("domain", decision.domain),
            ("intent", decision.intent),
            ("followup_mode", decision.followup_mode),
            ("planned_tools", planned_tool_names),
            ("query_type", decision.query_type),
        ):
            if field not in expected:
                continue
            metric_totals[field]["total"] += 1
            ok = _score_match(expected.get(field), actual)
            if ok:
                metric_totals[field]["correct"] += 1
            passed &= _record_check(checks, field, ok, expected.get(field), actual)

        results.append(
            {
                "id": case.get("id"),
                "category": case.get("category"),
                "question": question,
                "passed": passed,
                "decision": {
                    "domain": decision.domain,
                    "intent": decision.intent,
                    "followup_mode": decision.followup_mode,
                    "query_type": decision.query_type,
                    "confidence": round(float(decision.confidence or 0.0), 4),
                    "reasons": list(decision.reasons),
                },
                "planned_tools": planned_tool_names,
                "checks": checks,
            }
        )

    metric_summary = {
        key: {
            "correct": value["correct"],
            "total": value["total"],
            "accuracy": round((value["correct"] / value["total"]), 4) if value["total"] else None,
        }
        for key, value in metric_totals.items()
    }
    passed_case_count = sum(1 for item in results if item.get("passed"))
    summary = {
        "case_file": str(args.cases),
        "case_count": len(results),
        "passed_case_count": passed_case_count,
        "failed_case_count": max(0, len(results) - passed_case_count),
        "metrics": metric_summary,
        "results": results,
    }
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if passed_case_count == len(results) else 1


if __name__ == "__main__":
    raise SystemExit(main())