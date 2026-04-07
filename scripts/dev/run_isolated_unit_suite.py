from __future__ import annotations

import importlib.util
import json
import sys
import time
import unittest
from pathlib import Path
from typing import Any


class CollectingResult(unittest.TestResult):
    def __init__(self) -> None:
        super().__init__()
        self.test_outcomes: list[dict[str, Any]] = []

    def addSuccess(self, test: unittest.TestCase) -> None:
        self.test_outcomes.append({"id": test.id(), "status": "pass", "message": ""})

    def addFailure(self, test: unittest.TestCase, err: Any) -> None:
        super().addFailure(test, err)
        message = str(err[1])[:300] if err and err[1] else ""
        self.test_outcomes.append({"id": test.id(), "status": "fail", "message": message})

    def addError(self, test: unittest.TestCase, err: Any) -> None:
        super().addError(test, err)
        message = str(err[1])[:300] if err and err[1] else ""
        self.test_outcomes.append({"id": test.id(), "status": "error", "message": message})

    def addSkip(self, test: unittest.TestCase, reason: str) -> None:
        super().addSkip(test, reason)
        self.test_outcomes.append({"id": test.id(), "status": "skip", "message": str(reason or "")[:300]})


def run_suite(suite_id: str, label: str, file_path: Path) -> dict[str, Any]:
    if not file_path.is_file():
        return {
            "id": suite_id,
            "label": label,
            "elapsed_seconds": 0.0,
            "passed": 0,
            "failed": 0,
            "errors": 1,
            "tests": [{"id": "<setup>", "status": "error", "message": f"File not found: {file_path}"}],
        }

    repo_root = str(file_path.resolve().parents[2])
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)

    module_name = f"_bm_isolated_suite_{suite_id}"
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot create spec for {file_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)  # type: ignore[union-attr]

    loader = unittest.TestLoader()
    suite = loader.loadTestsFromModule(module)
    result = CollectingResult()
    t0 = time.perf_counter()
    suite.run(result)
    elapsed = round(time.perf_counter() - t0, 3)
    passed = sum(1 for item in result.test_outcomes if item["status"] == "pass")
    failed = sum(1 for item in result.test_outcomes if item["status"] == "fail")
    errors = sum(1 for item in result.test_outcomes if item["status"] == "error")
    return {
        "id": suite_id,
        "label": label,
        "elapsed_seconds": elapsed,
        "passed": passed,
        "failed": failed,
        "errors": errors,
        "tests": result.test_outcomes,
    }


def main(argv: list[str]) -> int:
    if len(argv) != 4:
        payload = {
            "id": "<runner>",
            "label": "<runner>",
            "elapsed_seconds": 0.0,
            "passed": 0,
            "failed": 0,
            "errors": 1,
            "tests": [{"id": "<runner>", "status": "error", "message": "usage: run_isolated_unit_suite.py <suite_id> <label> <file_path>"}],
        }
        print(json.dumps(payload, ensure_ascii=False))
        return 2

    _, suite_id, label, raw_path = argv
    try:
        payload = run_suite(str(suite_id or ""), str(label or suite_id or ""), Path(raw_path))
        print(json.dumps(payload, ensure_ascii=False))
        return 0 if int(payload.get("failed", 0) or 0) <= 0 and int(payload.get("errors", 0) or 0) <= 0 else 1
    except Exception as exc:  # noqa: BLE001
        payload = {
            "id": str(suite_id or "<runner>"),
            "label": str(label or suite_id or "<runner>"),
            "elapsed_seconds": 0.0,
            "passed": 0,
            "failed": 0,
            "errors": 1,
            "tests": [{"id": "<import>", "status": "error", "message": str(exc)[:400]}],
        }
        print(json.dumps(payload, ensure_ascii=False))
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
