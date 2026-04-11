from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from core_service.bug_ticket_payloads import parse_bug_ticket_payload


WORKSPACE_ROOT = Path(__file__).resolve().parents[2]


def _resolve_outbox_path(explicit_path: str) -> Path:
    if explicit_path:
        candidate = Path(explicit_path)
        if not candidate.is_absolute():
            candidate = WORKSPACE_ROOT / candidate
        return candidate.resolve()
    return (WORKSPACE_ROOT / "data" / "nav_dashboard" / "tickets" / "bug_ticket_outbox.jsonl").resolve()


def _load_payload(args: argparse.Namespace) -> dict[str, Any]:
    raw = args.json.strip()
    if not raw and not sys.stdin.isatty():
        raw = sys.stdin.read().strip()
    if not raw:
        raise ValueError("Missing bug ticket JSON payload")
    return parse_bug_ticket_payload(raw)


def main() -> int:
    parser = argparse.ArgumentParser(description="Append a bug ticket payload into the repo-local ingest outbox.")
    parser.add_argument("--json", default="", help="Bug ticket JSON payload")
    parser.add_argument("--outbox", default="", help="Optional outbox path")
    parser.add_argument("--session-id", default="", help="Optional session id for diagnostics")
    parser.add_argument("--file-path", action="append", default=[], help="Optional touched file path, can be repeated")
    args = parser.parse_args()

    payload = _load_payload(args)
    outbox_path = _resolve_outbox_path(args.outbox)
    outbox_path.parent.mkdir(parents=True, exist_ok=True)
    envelope = {
        "queued_at": datetime.now().isoformat(timespec="seconds"),
        "session_id": str(args.session_id or "").strip(),
        "file_paths": [str(item).strip() for item in (args.file_path or []) if str(item).strip()],
        "payload": payload,
    }
    with outbox_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(envelope, ensure_ascii=False) + "\n")
    print(json.dumps({"ok": True, "outbox": outbox_path.as_posix()}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())