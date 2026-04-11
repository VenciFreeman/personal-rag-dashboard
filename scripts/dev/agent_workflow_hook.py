from __future__ import annotations

import importlib.util
import json
import re
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

WORKSPACE_ROOT = Path(__file__).resolve().parents[2]
STATE_FILE = WORKSPACE_ROOT / ".git" / "copilot_agent_workflow_state.json"
LOG_FILE = WORKSPACE_ROOT / ".git" / "copilot_agent_workflow_log.jsonl"

TASK_TYPES = {"debug", "feature", "refactor"}
WRITE_TOOLS = {
    "apply_patch",
    "create_file",
    "edit_notebook_file",
    "vscode_renameSymbol",
    "create_new_workspace",
}
READ_ONLY_TOOLS = {
    "read_file",
    "file_search",
    "grep_search",
    "search_subagent",
    "semantic_search",
    "list_dir",
    "get_changed_files",
    "get_errors",
    "fetch_webpage",
    "memory",
}
DEBUG_KEYWORDS = (
    "bug",
    "debug",
    "fix",
    "broken",
    "regression",
    "error",
    "issue",
    "报错",
    "异常",
    "修复",
    "坏了",
    "回归",
    "不对",
    "故障",
)
REFACTOR_KEYWORDS = (
    "refactor",
    "cleanup",
    "clean up",
    "simplify",
    "rename",
    "extract",
    "restructure",
    "重构",
    "整理",
    "抽离",
    "降复杂度",
)
TICKET_HINTS = (
    "ticket",
    "tickets",
    "tickets.jsonl",
    "bug_ticket",
    "历史 ticket",
    "historical ticket",
)
TOOLING_ONLY_PREFIXES = (
    ".github/",
    "scripts/dev/agent_workflow_hook.py",
)
FRONTEND_PREFIXES = (
    "nav_dashboard/web/static/",
    "nav_dashboard/web/templates/",
    "core_service/static/",
    "ai_conversations_summary/web/static/",
    "ai_conversations_summary/web/templates/",
)
HOOK_PATHS = (
    ".github/",
    "scripts/dev/agent_workflow_hook.py",
)
MEDIA_REGRESSION_PREFIXES = (
    "nav_dashboard/web/services/",
    "data/library_tracker/structured/",
    "tests/router/",
    "tests/retrieval/",
    "tests/answer/",
)
ARCHITECTURE_GUARD_HOTSPOTS = (
    "nav_dashboard/web/services/agent_service.py",
    "nav_dashboard/web/api/benchmark.py",
)
ARCHITECTURE_GUARD_TEST_PREFIXES = (
    "tests/router/",
    "tests/retrieval/",
    "tests/answer/",
    "tests/benchmark/",
)
QUESTION_LITERAL_PATTERNS = (
    re.compile(r"\b(?:raw_question|question|resolved_question)\b[^\n]{0,120}(?:==|!=)\s*(['\"])(?P<literal>[^'\"]{8,})\1"),
    re.compile(r"(['\"])(?P<literal>[^'\"]{8,})\1\s+(?:not\s+in|in)\s+\b(?:raw_question|question|resolved_question)\b"),
    re.compile(r"\b(?:raw_question|question|resolved_question)\b[^\n]{0,120}\.(?:startswith|endswith)\(\s*(['\"])(?P<literal>[^'\"]{8,})\1\s*\)"),
)


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_hook_input() -> dict[str, Any]:
    raw = sys.stdin.read().strip()
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_json(payload: dict[str, Any]) -> int:
    print(json.dumps(payload, ensure_ascii=False))
    return 0


def _append_log(event: str, payload: dict[str, Any]) -> None:
    try:
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        with LOG_FILE.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps({"ts": _now_iso(), "event": event, **payload}, ensure_ascii=False) + "\n")
    except Exception:
        return


def _load_state() -> dict[str, Any]:
    if not STATE_FILE.exists():
        return {}
    try:
        payload = json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _save_state(state: dict[str, Any]) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def _session_record(state: dict[str, Any], session_id: str) -> dict[str, Any]:
    sessions = state.setdefault("sessions", {})
    record = sessions.get(session_id)
    if isinstance(record, dict):
        return record
    record = {
        "task_type": "feature",
        "prompt": "",
        "ticket_search_done": False,
        "last_ticket_search_at": "",
        "last_changed_files": [],
        "last_checks": {},
        "updated_at": _now_iso(),
    }
    sessions[session_id] = record
    return record


def _classify_task(prompt: str) -> str:
    text = prompt.casefold()
    if any(keyword in text for keyword in DEBUG_KEYWORDS):
        return "debug"
    if any(keyword in text for keyword in REFACTOR_KEYWORDS):
        return "refactor"
    return "feature"


def _norm_rel_path(path: str) -> str:
    raw = _safe_text(path).replace("\\", "/")
    if not raw:
        return ""
    candidate = Path(raw)
    if not candidate.is_absolute():
        candidate = (WORKSPACE_ROOT / candidate).resolve()
    try:
        return candidate.relative_to(WORKSPACE_ROOT).as_posix()
    except Exception:
        return candidate.as_posix().replace("\\", "/")


def _parse_apply_patch_files(tool_input: dict[str, Any]) -> list[str]:
    patch_text = _safe_text(tool_input.get("input"))
    if not patch_text:
        return []
    files: list[str] = []
    for line in patch_text.splitlines():
        if not line.startswith("*** ") or " File: " not in line:
            continue
        _, raw_path = line.split(" File: ", 1)
        path_text = raw_path.split(" -> ", 1)[0].strip()
        rel_path = _norm_rel_path(path_text)
        if rel_path and rel_path not in files:
            files.append(rel_path)
    return files


def _extract_changed_files(tool_name: str, tool_input: dict[str, Any]) -> list[str]:
    if tool_name == "apply_patch":
        return _parse_apply_patch_files(tool_input)
    file_path = _safe_text(tool_input.get("filePath") or tool_input.get("path"))
    if file_path:
        rel_path = _norm_rel_path(file_path)
        return [rel_path] if rel_path else []
    files = tool_input.get("files")
    if isinstance(files, list):
        resolved = []
        for item in files:
            rel_path = _norm_rel_path(item)
            if rel_path and rel_path not in resolved:
                resolved.append(rel_path)
        return resolved
    return []


def _tool_is_mutating(tool_name: str) -> bool:
    return tool_name in WRITE_TOOLS


def _stringify_payload(value: Any) -> str:
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, ensure_ascii=False)
    except Exception:
        return str(value)


def _tool_call_counts_as_ticket_search(tool_name: str, tool_input: dict[str, Any]) -> bool:
    if tool_name not in READ_ONLY_TOOLS and tool_name != "run_in_terminal":
        return False
    haystack = _stringify_payload(tool_input).casefold()
    return any(hint in haystack for hint in TICKET_HINTS)


def _terminal_command_is_read_only(command: str) -> bool:
    text = command.strip().lower()
    if not text:
        return True
    read_only_prefixes = (
        "rg ",
        "rg\n",
        "grep ",
        "git status",
        "git diff",
        "git show",
        "get-childitem",
        "get-content",
        "select-string",
        "dir",
        "ls",
        "type ",
        "pwd",
        "get-location",
    )
    return text.startswith(read_only_prefixes)


def _requires_debug_ticket_gate(tool_name: str, tool_input: dict[str, Any]) -> bool:
    if _tool_is_mutating(tool_name):
        return True
    if tool_name != "run_in_terminal":
        return False
    command = _safe_text(tool_input.get("command"))
    return not _terminal_command_is_read_only(command) and not _tool_call_counts_as_ticket_search(tool_name, tool_input)


def _module_available(name: str) -> bool:
    return importlib.util.find_spec(name) is not None


def _git_diff_for_paths(paths: list[str], *, cached: bool) -> str:
    if not paths:
        return ""
    command = ["git", "diff", "--unified=0"]
    if cached:
        command.append("--cached")
    command.extend(["--", *paths])
    try:
        completed = subprocess.run(
            command,
            cwd=WORKSPACE_ROOT,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=20,
            check=False,
        )
    except Exception:
        return ""
    if completed.returncode not in {0, 1}:
        return ""
    return str(completed.stdout or "")


def _collect_added_diff_lines(paths: list[str]) -> list[str]:
    diff_text = "\n".join(
        part for part in (
            _git_diff_for_paths(paths, cached=False),
            _git_diff_for_paths(paths, cached=True),
        ) if part
    )
    added: list[str] = []
    for line in diff_text.splitlines():
        if line.startswith("+++") or not line.startswith("+"):
            continue
        added.append(line[1:])
    return added


def _line_contains_query_specific_literal(line: str) -> bool:
    text = line.strip()
    if not text:
        return False
    for pattern in QUESTION_LITERAL_PATTERNS:
        match = pattern.search(text)
        if not match:
            continue
        literal = _safe_text(match.group("literal"))
        if not literal:
            continue
        if re.search(r"[\u4e00-\u9fff]", literal) or " " in literal or len(literal) >= 16:
            return True
    return False


def _run_architecture_guard(changed_files: list[str]) -> dict[str, Any]:
    deduped = [path for path in changed_files if path]
    hotspot_files = [path for path in deduped if path in ARCHITECTURE_GUARD_HOTSPOTS]
    if not hotspot_files:
        return {"name": "architecture-guard", "status": "skipped", "details": "no hotspot files changed"}

    has_companion_tests = any(
        any(path.startswith(prefix) for prefix in ARCHITECTURE_GUARD_TEST_PREFIXES)
        for path in deduped
    )
    if not has_companion_tests:
        return {
            "name": "architecture-guard",
            "status": "failed",
            "details": "Hotspot logic changed without companion regression tests in tests/router, tests/retrieval, tests/answer, or tests/benchmark.",
        }

    added_lines = _collect_added_diff_lines(hotspot_files)
    offenders = [line.strip() for line in added_lines if _line_contains_query_specific_literal(line)]
    if offenders:
        return {
            "name": "architecture-guard",
            "status": "failed",
            "details": "Query-specific literal patch detected in hotspot diff: " + " | ".join(offenders[:3]),
        }
    return {"name": "architecture-guard", "status": "passed", "details": "hotspot diff uses shared logic and companion tests are present"}


def _run_command(command: list[str], *, timeout: int) -> dict[str, Any]:
    try:
        completed = subprocess.run(
            command,
            cwd=WORKSPACE_ROOT,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return {"status": "failed", "command": command, "details": f"timeout after {timeout}s"}
    except Exception as exc:
        return {"status": "failed", "command": command, "details": str(exc)}
    output = "\n".join(part.strip() for part in [completed.stdout, completed.stderr] if part.strip()).strip()
    return {
        "status": "passed" if completed.returncode == 0 else "failed",
        "command": command,
        "details": output[:4000],
        "returncode": completed.returncode,
    }


def _run_lint(changed_files: list[str]) -> dict[str, Any]:
    py_files = [path for path in changed_files if path.endswith(".py")]
    if not py_files:
        return {"name": "lint", "status": "skipped", "details": "no Python files changed"}
    python_exe = sys.executable
    if _module_available("ruff"):
        check = _run_command([python_exe, "-m", "ruff", "check", "--fix", *py_files], timeout=90)
        if check["status"] != "passed":
            return {"name": "lint", **check}
        fmt = _run_command([python_exe, "-m", "ruff", "format", *py_files], timeout=90)
        fmt["name"] = "lint"
        return fmt
    if _module_available("black"):
        result = _run_command([python_exe, "-m", "black", *py_files], timeout=90)
        result["name"] = "lint"
        return result
    return {"name": "lint", "status": "skipped", "details": "no formatter/linter module available"}


def _run_typecheck(changed_files: list[str]) -> dict[str, Any]:
    py_files = [path for path in changed_files if path.endswith(".py")]
    if not py_files:
        return {"name": "typecheck", "status": "skipped", "details": "no Python files changed"}
    python_exe = sys.executable
    if shutil.which("pyright"):
        result = _run_command(["pyright", *py_files], timeout=90)
        result["name"] = "typecheck"
        return result
    if _module_available("mypy"):
        result = _run_command([python_exe, "-m", "mypy", *py_files], timeout=90)
        result["name"] = "typecheck"
        return result
    result = _run_command([python_exe, "-m", "py_compile", *py_files], timeout=90)
    result["name"] = "typecheck"
    if result["status"] == "passed":
        result["details"] = "py_compile fallback passed"
    return result


def _run_smoke(changed_files: list[str]) -> dict[str, Any]:
    if not changed_files:
        return {"name": "smoke", "status": "skipped", "details": "no changed files"}
    python_exe = sys.executable
    smoke_runs: list[dict[str, Any]] = []
    if any(path.startswith(prefix) for path in changed_files for prefix in FRONTEND_PREFIXES):
        smoke_runs.append(
            _run_command([python_exe, "scripts/smoke/check_app_parse.py"], timeout=90)
        )
    if _module_available("pytest"):
        smoke_runs.append(_run_command([python_exe, "-m", "pytest", "-q"], timeout=120))
    if not smoke_runs:
        return {"name": "smoke", "status": "skipped", "details": "no smoke runner available"}

    failures = [item for item in smoke_runs if item.get("status") == "failed"]
    details = []
    for item in smoke_runs:
        command = " ".join(str(part) for part in item.get("command") or [])
        status = _safe_text(item.get("status")) or "unknown"
        line = f"[{status}] {command}".strip()
        item_details = _safe_text(item.get("details"))
        if item_details:
            line = f"{line}\n{item_details}".strip()
        details.append(line)
    return {
        "name": "smoke",
        "status": "failed" if failures else "passed",
        "details": "\n\n".join(part for part in details if part).strip(),
    }


def _debug_regression_commands(changed_files: list[str]) -> list[list[str]]:
    python_exe = sys.executable
    commands: list[list[str]] = []
    if any(path.startswith(prefix) for path in changed_files for prefix in HOOK_PATHS):
        commands.append([python_exe, "-m", "pytest", "-q", "tests/dev/test_agent_workflow_hook.py"])
    if any(path.startswith(prefix) for path in changed_files for prefix in MEDIA_REGRESSION_PREFIXES):
        commands.extend(
            [
                [python_exe, "-m", "pytest", "-q", "tests/router/test_classification.py"],
                [python_exe, "-m", "pytest", "-q", "tests/retrieval/test_media_boundaries.py"],
                [python_exe, "-m", "pytest", "-q", "tests/answer/test_policy.py"],
            ]
        )
    deduped: list[list[str]] = []
    seen: set[tuple[str, ...]] = set()
    for command in commands:
        marker = tuple(command)
        if marker in seen:
            continue
        seen.add(marker)
        deduped.append(command)
    return deduped


def _run_regression(changed_files: list[str], task_type: str) -> dict[str, Any]:
    if task_type != "debug":
        return {"name": "regression", "status": "skipped", "details": "not a debug task"}
    if not changed_files:
        return {"name": "regression", "status": "skipped", "details": "no changed files"}
    if not _module_available("pytest"):
        return {"name": "regression", "status": "failed", "details": "pytest is required for debug regression checks"}
    commands = _debug_regression_commands(changed_files)
    if not commands:
        commands = [[sys.executable, "-m", "pytest", "-q"]]
    runs = [_run_command(command, timeout=180) for command in commands]
    failures = [item for item in runs if item.get("status") == "failed"]
    details = []
    for item in runs:
        command = " ".join(str(part) for part in item.get("command") or [])
        status = _safe_text(item.get("status")) or "unknown"
        line = f"[{status}] {command}".strip()
        item_details = _safe_text(item.get("details"))
        if item_details:
            line = f"{line}\n{item_details}".strip()
        details.append(line)
    return {
        "name": "regression",
        "status": "failed" if failures else "passed",
        "details": "\n\n".join(part for part in details if part).strip(),
    }


def _summarize_checks(results: list[dict[str, Any]]) -> str:
    lines = []
    for item in results:
        name = _safe_text(item.get("name")) or "check"
        status = _safe_text(item.get("status")) or "unknown"
        details = _safe_text(item.get("details"))
        lines.append(f"- {name}: {status}")
        if details:
            lines.append(details)
    return "\n".join(lines).strip()


def _run_post_checks(changed_files: list[str], task_type: str) -> dict[str, Any]:
    deduped = []
    for path in changed_files:
        if path and path not in deduped:
            deduped.append(path)
    results = [
        _run_architecture_guard(deduped),
        _run_regression(deduped, task_type),
        _run_lint(deduped),
        _run_typecheck(deduped),
        _run_smoke(deduped),
    ]
    failures = [item for item in results if item.get("status") == "failed"]
    return {
        "changed_files": deduped,
        "results": results,
        "all_passed": not failures,
        "summary": _summarize_checks(results),
    }


def _session_message(task_type: str) -> str:
    task_label = task_type if task_type in TASK_TYPES else "feature"
    return (
        f"TASK_TYPE={task_label}. "
        f"Only follow GLOBAL RULES + {task_label.upper()} RULES from .github/AGENT_RULES.md."
    )


def _handle_session_start() -> dict[str, Any]:
    return {
        "hookSpecificOutput": {
            "hookEventName": "SessionStart",
            "additionalContext": (
                "Workflow entrypoint: .github/AGENT_WORKFLOW.md | "
                "Rule library: .github/AGENT_RULES.md | "
                "Hooks enforce TASK_TYPE declaration, debug ticket search before coding, and post-check gates after edits."
            ),
        }
    }


def _handle_user_prompt_submit(payload: dict[str, Any], state: dict[str, Any], session_id: str) -> dict[str, Any]:
    prompt = _safe_text(payload.get("prompt"))
    task_type = _classify_task(prompt)
    session = _session_record(state, session_id)
    session.update(
        {
            "task_type": task_type,
            "prompt": prompt,
            "ticket_search_done": False,
            "last_ticket_search_at": "",
            "last_changed_files": [],
            "last_checks": {},
            "updated_at": _now_iso(),
        }
    )
    _save_state(state)
    return {
        "continue": True,
        "systemMessage": _session_message(task_type),
    }


def _handle_pre_tool_use(payload: dict[str, Any], state: dict[str, Any], session_id: str) -> dict[str, Any]:
    session = _session_record(state, session_id)
    tool_name = _safe_text(payload.get("tool_name"))
    tool_input = payload.get("tool_input") if isinstance(payload.get("tool_input"), dict) else {}
    if session.get("task_type") == "debug" and not bool(session.get("ticket_search_done")):
        if _requires_debug_ticket_gate(tool_name, tool_input):
            return {
                "hookSpecificOutput": {
                    "hookEventName": "PreToolUse",
                    "permissionDecision": "deny",
                    "permissionDecisionReason": "Debug tasks must search similar tickets before editing or non-read-only terminal execution.",
                    "additionalContext": "Search data/nav_dashboard/tickets/tickets.jsonl or related ticket history first, then continue with the fix.",
                }
            }
    return {
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "allow",
        }
    }


def _handle_post_tool_use(payload: dict[str, Any], state: dict[str, Any], session_id: str) -> dict[str, Any]:
    session = _session_record(state, session_id)
    tool_name = _safe_text(payload.get("tool_name"))
    tool_input = payload.get("tool_input") if isinstance(payload.get("tool_input"), dict) else {}

    if _tool_call_counts_as_ticket_search(tool_name, tool_input):
        session["ticket_search_done"] = True
        session["last_ticket_search_at"] = _now_iso()

    if not _tool_is_mutating(tool_name):
        session["updated_at"] = _now_iso()
        _save_state(state)
        return {"continue": True}

    changed_files = _extract_changed_files(tool_name, tool_input)
    session["last_changed_files"] = changed_files
    checks = _run_post_checks(changed_files, _safe_text(session.get("task_type")) or "feature")
    session["last_checks"] = checks
    session["updated_at"] = _now_iso()
    _save_state(state)

    if not checks.get("all_passed"):
        return {
            "decision": "block",
            "reason": "Post-check failed",
            "hookSpecificOutput": {
                "hookEventName": "PostToolUse",
                "additionalContext": checks.get("summary") or "Post-check failed.",
            },
        }
    summary = _safe_text(checks.get("summary"))
    if summary:
        return {
            "hookSpecificOutput": {
                "hookEventName": "PostToolUse",
                "additionalContext": summary,
            }
        }
    return {"continue": True}


def _handle_stop(payload: dict[str, Any], state: dict[str, Any], session_id: str) -> dict[str, Any]:
    if bool(payload.get("stop_hook_active")):
        return {"continue": True}
    session = _session_record(state, session_id)
    task_type = _safe_text(session.get("task_type")) or "feature"
    checks = session.get("last_checks") if isinstance(session.get("last_checks"), dict) else {}
    changed_files = session.get("last_changed_files") if isinstance(session.get("last_changed_files"), list) else []

    if task_type == "debug" and changed_files and not bool(session.get("ticket_search_done")):
        return {
            "hookSpecificOutput": {
                "hookEventName": "Stop",
                "decision": "block",
                "reason": "Debug workflow incomplete: search similar tickets before finalizing the task.",
            }
        }
    if checks and not bool(checks.get("all_passed", True)):
        return {
            "hookSpecificOutput": {
                "hookEventName": "Stop",
                "decision": "block",
                "reason": checks.get("summary") or "Post-check failed. Fix the reported issues before stopping.",
            }
        }
    return {"continue": True}


def main() -> int:
    payload = _read_hook_input()
    event = _safe_text(payload.get("hookEventName") or ((payload.get("hookSpecificOutput") or {}).get("hookEventName") if isinstance(payload.get("hookSpecificOutput"), dict) else ""))
    session_id = _safe_text(payload.get("sessionId")) or "default-session"
    state = _load_state()

    _append_log(event or "unknown", {"session_id": session_id})

    if event == "SessionStart":
        return _write_json(_handle_session_start())
    if event == "UserPromptSubmit":
        return _write_json(_handle_user_prompt_submit(payload, state, session_id))
    if event == "PreToolUse":
        return _write_json(_handle_pre_tool_use(payload, state, session_id))
    if event == "PostToolUse":
        return _write_json(_handle_post_tool_use(payload, state, session_id))
    if event == "Stop":
        return _write_json(_handle_stop(payload, state, session_id))
    return _write_json({"continue": True})


if __name__ == "__main__":
    raise SystemExit(main())