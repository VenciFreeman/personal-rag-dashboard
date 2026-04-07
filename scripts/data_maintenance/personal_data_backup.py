from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


WORKSPACE_ROOT = Path(__file__).resolve().parents[2]
if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))

from nav_dashboard.web.services.operations import data_backup_service


def _parse_apps(raw: str) -> list[str]:
    return [part.strip() for part in str(raw or "").split(",") if part.strip()]


def main() -> int:
    parser = argparse.ArgumentParser(description="Export, back up, or restore personal-ai-stack main data contracts.")
    parser.add_argument("command", choices=("export", "backup", "restore", "summary", "validate", "rehearse-restore"))
    parser.add_argument("--apps", default="library_tracker,property,journey,ai_conversations_summary", help="Comma-separated app list")
    parser.add_argument("--file", default="", help="Input or output JSON file path")
    parser.add_argument("--replace-existing", action="store_true", help="Replace existing main data on restore")
    args = parser.parse_args()

    apps = data_backup_service.normalize_backup_apps(_parse_apps(args.apps))
    if args.command == "summary":
        print(json.dumps(data_backup_service.backup_summary(), ensure_ascii=False, indent=2))
        return 0
    if args.command == "export":
        payload = data_backup_service.export_main_data_contract(apps)
        if args.file:
            target = Path(args.file)
            if target.suffix.lower() == ".zip":
                target.write_bytes(data_backup_service.build_backup_archive_bytes(payload))
            else:
                target.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        else:
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0
    if args.command == "backup":
        result = data_backup_service.create_backup_snapshot(apps)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    if args.command in {"validate", "rehearse-restore", "restore"} and not args.file:
        raise SystemExit(f"{args.command} requires --file")
    raw_path = Path(args.file)
    if raw_path.suffix.lower() == ".zip":
        payload = data_backup_service.load_backup_payload(raw_path.name, raw_path.read_bytes())
    else:
        payload = json.loads(raw_path.read_text(encoding="utf-8"))
    if args.command == "validate":
        result = data_backup_service.validate_main_data_contract_payload(payload, apps=apps)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    if args.command == "rehearse-restore":
        result = data_backup_service.rehearse_restore_main_data_contract(payload, apps=apps, replace_existing=bool(args.replace_existing))
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    result = data_backup_service.restore_main_data_contract(payload, apps=apps, replace_existing=bool(args.replace_existing))
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())