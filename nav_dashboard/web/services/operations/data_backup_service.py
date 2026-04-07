from __future__ import annotations

import io
import json
import threading
import time
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from ai_conversations_summary import runtime_paths as ai_summary_runtime_paths
from core_service.runtime_data import backup_snapshots_root, core_runtime_path, ensure_backup_snapshots_dir, workspace_root
from core_service.ticket_store import TICKETS_FILE as NAV_TICKETS_FILE, list_tickets


BACKUP_CONTRACT = "personal_ai_stack_main_data_backup"
BACKUP_SCHEMA_VERSION = 1
SUPPORTED_BACKUP_APPS = ("library_tracker", "property", "journey", "ai_conversations_summary", "nav_dashboard")
BACKUP_ARCHIVE_PREFIX = "personal_data_backup"
PRE_RESTORE_ARCHIVE_PREFIX = "pre_restore_backup"
BACKUP_PAYLOAD_FILENAME = "main_data_contract.json"
BACKUP_MANIFEST_FILENAME = "backup_manifest.json"
BACKUP_STORAGE_CONTRACT_FILENAME = "storage_contract.json"
BACKUP_APP_PAYLOAD_DIR = "apps"
LIBRARY_MEDIA_TYPES = ("book", "video", "music", "game")
AI_SUMMARY_PAYLOAD_CONTRACT = "ai_conversations_summary_backup"
AI_SUMMARY_PAYLOAD_VERSION = 1
NAV_DASHBOARD_PAYLOAD_CONTRACT = "nav_dashboard_backup"
NAV_DASHBOARD_PAYLOAD_VERSION = 1
BACKUP_SCHEDULER_STATE_PATH = core_runtime_path("backup_scheduler_state.json")
BACKUP_SCHEDULER_INTERVAL_SECONDS = 6 * 60 * 60

_BACKUP_SCHEDULER_LOCK = threading.Lock()
_BACKUP_SCHEDULER_STARTED = False
_BACKUP_SCHEDULER_STOP = threading.Event()


def data_storage_contract() -> dict[str, Any]:
    return {
        "contract": "personal_ai_stack_data_storage_contract",
        "version": 1,
        "defined_at": datetime.now().isoformat(timespec="seconds"),
        "categories": {
            "primary_business_data": {
                "description": "需要长期保留、备份与迁移的业务主数据/元数据。",
                "paths": {
                    "library_tracker": ["data/library_tracker/structured/entities", "data/library_tracker/structured/aliases", "data/library_tracker/structured/concepts"],
                    "property": ["data/property"],
                    "journey": ["data/journey"],
                    "ai_conversations_summary": ["data/ai_conversations_summary/documents"],
                    "nav_dashboard": ["data/nav_dashboard/tickets/tickets.jsonl"],
                },
            },
            "shared_runtime_metadata": {
                "description": "共享运行态与运维元数据。",
                "paths": {"core_service": ["data/core_service", "data/core_service/backup_snapshots"]},
            },
            "cache_and_indexes": {
                "description": "可重建缓存、索引和向量数据库，默认不进主数据备份。",
                "paths": {
                    "library_tracker": ["data/library_tracker/vector_db", "data/library_tracker/media/covers"],
                    "ai_conversations_summary": ["data/ai_conversations_summary/cache", "data/ai_conversations_summary/hf_cache", "data/ai_conversations_summary/vector_db"],
                },
            },
            "app_runtime_state": {
                "description": "应用级运行态配置、状态和任务队列；不属于主数据，但应有稳定分层。",
                "paths": {
                    "nav_dashboard": ["data/nav_dashboard/config", "data/nav_dashboard/state"],
                    "ai_conversations_summary": ["data/ai_conversations_summary/state", "data/ai_conversations_summary/sessions"],
                },
            },
            "logs_and_observability": {
                "description": "追踪、Ticket、反馈与运行日志，按需单独保留。",
                "paths": {
                    "nav_dashboard": ["data/nav_dashboard/trace_records", "data/nav_dashboard/observability/chat_feedback.json", "data/nav_dashboard/observability/api_usage_traces.json"],
                    "ai_conversations_summary": ["data/ai_conversations_summary/observability", "data/ai_conversations_summary/sessions/rag/debug_data"],
                },
            },
            "analysis_outputs": {
                "description": "分析报告和衍生产物，可重建，默认不进主数据备份。",
                "paths": {
                    "library_tracker": ["data/library_tracker/analysis"],
                    "property": ["data/property/analysis"],
                    "journey": ["data/journey/analysis"],
                },
            },
            "temporary_files": {
                "description": "临时文件、任务中间产物和调试输出，应定期清理。",
                "paths": {
                    "nav_dashboard": ["data/nav_dashboard/agent_sessions/debug_data"],
                    "ai_conversations_summary": ["data/ai_conversations_summary/processing/extracted", "data/ai_conversations_summary/processing/split", "data/ai_conversations_summary/processing/summarize"],
                },
            },
            "cross_app_operations": {
                "description": "跨应用烟测与一次性运维产物，避免混入任何单 app 主数据 contract。",
                "paths": {"workspace": ["data/_smoke_runs"]},
            },
            "legacy_residuals": {
                "description": "历史残留路径，保留只为显式清理/核对，不应再被当前运行时写入。",
                "paths": {"workspace": ["data/lan_auth.sqlite3"]},
            },
        },
    }


def normalize_backup_apps(apps: list[str] | tuple[str, ...] | None = None) -> list[str]:
    normalized: list[str] = []
    for app in list(apps or SUPPORTED_BACKUP_APPS):
        value = str(app or "").strip().lower()
        if value in SUPPORTED_BACKUP_APPS and value not in normalized:
            normalized.append(value)
    if not normalized:
        return list(SUPPORTED_BACKUP_APPS)
    return normalized


def _relative_display_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(workspace_root().resolve())).replace("\\", "/")
    except Exception:
        return str(path).replace("\\", "/")


def _iter_ai_summary_document_paths(documents_dir: Path) -> list[Path]:
    if not documents_dir.exists():
        return []
    return sorted(
        [path for path in documents_dir.rglob("*.md") if path.is_file() and path.name != ".gitkeep"],
        key=lambda item: item.as_posix().lower(),
    )


def _clear_directory_contents(path: Path) -> None:
    if not path.exists():
        return
    for child in path.iterdir():
        if child.is_dir():
            import shutil

            shutil.rmtree(child)
        else:
            child.unlink()


def _export_ai_summary_payload() -> dict[str, Any]:
    documents_dir = ai_summary_runtime_paths.DOCUMENTS_DIR
    document_rows: list[dict[str, Any]] = []
    category_counts: dict[str, int] = {}
    for file_path in _iter_ai_summary_document_paths(documents_dir):
        rel_path = file_path.relative_to(documents_dir).as_posix()
        category = rel_path.split("/", 1)[0] if "/" in rel_path else "_root"
        category_counts[category] = int(category_counts.get(category, 0) or 0) + 1
        document_rows.append(
            {
                "path": rel_path,
                "content": file_path.read_text(encoding="utf-8"),
            }
        )

    return {
        "contract": AI_SUMMARY_PAYLOAD_CONTRACT,
        "version": AI_SUMMARY_PAYLOAD_VERSION,
        "documents": document_rows,
        "summary": {
            "document_count": len(document_rows),
            "category_counts": dict(sorted(category_counts.items())),
        },
    }


def _import_ai_summary_payload(payload: dict[str, Any], *, replace_existing: bool) -> dict[str, Any]:
    documents_dir = ai_summary_runtime_paths.DOCUMENTS_DIR
    document_rows = payload.get("documents") if isinstance(payload.get("documents"), list) else []
    clean_rows = [row for row in document_rows if isinstance(row, dict)]

    existing_docs = _iter_ai_summary_document_paths(documents_dir)
    if existing_docs and not replace_existing and clean_rows:
        raise ValueError("ai_conversations_summary documents already exist; set replace_existing=true to overwrite")

    documents_dir.mkdir(parents=True, exist_ok=True)
    if replace_existing:
        _clear_directory_contents(documents_dir)

    restored_paths: list[str] = []
    category_counts: dict[str, int] = {}
    for row in clean_rows:
        rel_path = str(row.get("path") or "").strip().replace("\\", "/").strip("/")
        if not rel_path:
            continue
        target = (documents_dir / rel_path).resolve()
        if not str(target).startswith(str(documents_dir.resolve())):
            raise ValueError(f"invalid ai_conversations_summary document path: {rel_path}")
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(str(row.get("content") or ""), encoding="utf-8")
        restored_paths.append(rel_path)
        category = rel_path.split("/", 1)[0] if "/" in rel_path else "_root"
        category_counts[category] = int(category_counts.get(category, 0) or 0) + 1

    gitkeep = documents_dir / ".gitkeep"
    if not gitkeep.exists():
        gitkeep.write_text("", encoding="utf-8")

    return {
        "ok": True,
        "document_count": len(restored_paths),
        "category_counts": dict(sorted(category_counts.items())),
        "documents_dir": _relative_display_path(documents_dir),
        "replace_existing": bool(replace_existing),
    }


def _export_app_payload(app_name: str) -> dict[str, Any]:
    if app_name == "library_tracker":
        from library_tracker.web.services import library_service

        return library_service.export_library_contract()
    if app_name == "property":
        from property.web.services import property_query_service

        return property_query_service.export_property_contract()
    if app_name == "journey":
        from journey.web.services import journey_query_service

        return journey_query_service.export_journey_contract()
    if app_name == "ai_conversations_summary":
        return _export_ai_summary_payload()
    if app_name == "nav_dashboard":
        ticket_text = NAV_TICKETS_FILE.read_text(encoding="utf-8") if NAV_TICKETS_FILE.exists() else ""
        return {
            "contract": NAV_DASHBOARD_PAYLOAD_CONTRACT,
            "version": NAV_DASHBOARD_PAYLOAD_VERSION,
            "ticket_events_text": ticket_text,
        }
    raise ValueError(f"unsupported backup app: {app_name}")


def _import_app_payload(app_name: str, payload: dict[str, Any], *, replace_existing: bool) -> dict[str, Any]:
    if app_name == "library_tracker":
        from library_tracker.web.services import library_service

        return library_service.import_library_contract(payload, replace_existing=replace_existing)
    if app_name == "property":
        from property.web.services import property_crud_service

        return property_crud_service.import_property_contract(payload, replace_existing=replace_existing)
    if app_name == "journey":
        from journey.web.services import journey_crud_service

        return journey_crud_service.import_journey_contract(payload, replace_existing=replace_existing)
    if app_name == "ai_conversations_summary":
        return _import_ai_summary_payload(payload, replace_existing=replace_existing)
    if app_name == "nav_dashboard":
        if NAV_TICKETS_FILE.exists() and not replace_existing:
            raise ValueError("nav_dashboard tickets already exist; set replace_existing=true to overwrite")
        NAV_TICKETS_FILE.parent.mkdir(parents=True, exist_ok=True)
        NAV_TICKETS_FILE.write_text(str(payload.get("ticket_events_text") or ""), encoding="utf-8")
        return {
            "ok": True,
            "ticket_file": _relative_display_path(NAV_TICKETS_FILE),
            "ticket_count": len(list_tickets(limit=5000, sort="updated_desc")),
            "replace_existing": bool(replace_existing),
        }
    raise ValueError(f"unsupported backup app: {app_name}")


def _derive_app_summary(app_name: str, payload: dict[str, Any]) -> dict[str, Any]:
    if app_name == "library_tracker":
        summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
        record_counts = summary.get("record_counts") if isinstance(summary.get("record_counts"), dict) else {}
        return {
            "record_counts": {key: int(value or 0) for key, value in record_counts.items()},
            "alias_file_counts": dict(summary.get("alias_file_counts") or {}),
        }
    if app_name == "property":
        return {
            "snapshot_count": len(payload.get("snapshots") or []),
            "income_record_count": len(payload.get("income_records") or []),
        }
    if app_name == "journey":
        return {"trip_count": len(payload.get("trip_schedules") or [])}
    if app_name == "ai_conversations_summary":
        summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
        documents = payload.get("documents") if isinstance(payload.get("documents"), list) else []
        category_counts = summary.get("category_counts") if isinstance(summary.get("category_counts"), dict) else {}
        return {
            "document_count": len([row for row in documents if isinstance(row, dict)]),
            "category_counts": dict(category_counts),
        }
    if app_name == "nav_dashboard":
        text = str(payload.get("ticket_events_text") or "")
        event_count = len([line for line in text.splitlines() if str(line).strip()])
        return {
            "ticket_event_count": event_count,
        }
    return {}


def _summary_has_business_data(summary: dict[str, Any]) -> bool:
    if not isinstance(summary, dict):
        return False
    for value in summary.values():
        if isinstance(value, dict) and _summary_has_business_data(value):
            return True
        if isinstance(value, (int, float)) and int(value or 0) > 0:
            return True
    return False


def _validate_library_payload(payload: dict[str, Any]) -> dict[str, Any]:
    missing_keys = [key for key in ("contract", "version", "media_payloads", "alias_buckets", "concept_ontology") if key not in payload]
    warnings: list[str] = []
    media_payloads = payload.get("media_payloads") if isinstance(payload.get("media_payloads"), dict) else {}
    missing_media = [media_type for media_type in LIBRARY_MEDIA_TYPES if media_type not in media_payloads]
    if missing_media:
        warnings.append(f"missing media payloads: {', '.join(missing_media)}")
    alias_buckets = payload.get("alias_buckets") if isinstance(payload.get("alias_buckets"), dict) else {}
    missing_buckets = [bucket for bucket in ("approved", "proposal", "keep_original") if bucket not in alias_buckets]
    if missing_buckets:
        warnings.append(f"missing alias buckets: {', '.join(missing_buckets)}")
    return {
        "ok": not missing_keys,
        "missing_keys": missing_keys,
        "warnings": warnings,
    }


def _validate_property_payload(payload: dict[str, Any]) -> dict[str, Any]:
    missing_keys = [key for key in ("snapshots", "income_records") if key not in payload]
    warnings: list[str] = []
    if not isinstance(payload.get("snapshots"), list):
        warnings.append("snapshots should be a list")
    if not isinstance(payload.get("income_records"), list):
        warnings.append("income_records should be a list")
    return {
        "ok": not missing_keys,
        "missing_keys": missing_keys,
        "warnings": warnings,
    }


def _validate_journey_payload(payload: dict[str, Any]) -> dict[str, Any]:
    missing_keys = [key for key in ("trip_schedules",) if key not in payload]
    warnings: list[str] = []
    if not isinstance(payload.get("trip_schedules"), list):
        warnings.append("trip_schedules should be a list")
    return {
        "ok": not missing_keys,
        "missing_keys": missing_keys,
        "warnings": warnings,
    }


def _validate_ai_summary_payload(payload: dict[str, Any]) -> dict[str, Any]:
    missing_keys = [key for key in ("contract", "version", "documents") if key not in payload]
    warnings: list[str] = []
    if str(payload.get("contract") or "").strip() != AI_SUMMARY_PAYLOAD_CONTRACT:
        warnings.append("unexpected ai_conversations_summary contract")
    if int(payload.get("version") or 0) != AI_SUMMARY_PAYLOAD_VERSION:
        warnings.append("unexpected ai_conversations_summary payload version")
    if not isinstance(payload.get("documents"), list):
        warnings.append("documents should be a list")
    return {
        "ok": not missing_keys,
        "missing_keys": missing_keys,
        "warnings": warnings,
    }


def _validate_app_payload(app_name: str, payload: dict[str, Any]) -> dict[str, Any]:
    if app_name == "library_tracker":
        return _validate_library_payload(payload)
    if app_name == "property":
        return _validate_property_payload(payload)
    if app_name == "journey":
        return _validate_journey_payload(payload)
    if app_name == "ai_conversations_summary":
        return _validate_ai_summary_payload(payload)
    if app_name == "nav_dashboard":
        missing_keys = [key for key in ("contract", "version", "ticket_events_text") if key not in payload]
        warnings: list[str] = []
        if str(payload.get("contract") or "").strip() != NAV_DASHBOARD_PAYLOAD_CONTRACT:
            warnings.append("unexpected nav_dashboard contract")
        if int(payload.get("version") or 0) != NAV_DASHBOARD_PAYLOAD_VERSION:
            warnings.append("unexpected nav_dashboard payload version")
        return {
            "ok": not missing_keys,
            "missing_keys": missing_keys,
            "warnings": warnings,
        }
    return {"ok": False, "missing_keys": [], "warnings": [f"unsupported backup app: {app_name}"]}


def validate_main_data_contract_payload(payload: dict[str, Any], *, apps: list[str] | None = None) -> dict[str, Any]:
    if str(payload.get("contract") or "").strip() != BACKUP_CONTRACT:
        raise ValueError("unsupported backup contract")
    if int(payload.get("version") or 0) != BACKUP_SCHEMA_VERSION:
        raise ValueError("unsupported backup schema version")

    all_payloads = payload.get("apps") if isinstance(payload.get("apps"), dict) else {}
    selected_apps = normalize_backup_apps(apps or list(all_payloads.keys()))
    results: dict[str, Any] = {}
    validated_apps: list[str] = []
    for app_name in selected_apps:
        app_payload = all_payloads.get(app_name)
        if not isinstance(app_payload, dict):
            results[app_name] = {
                "ok": False,
                "missing_payload": True,
                "missing_keys": [],
                "warnings": ["missing app payload"],
                "backup_summary": {},
            }
            continue
        validated_apps.append(app_name)
        validation = _validate_app_payload(app_name, app_payload)
        derived_summary = _derive_app_summary(app_name, app_payload)
        stored_summary = payload.get("summary", {}).get(app_name) if isinstance(payload.get("summary"), dict) else {}
        warnings = list(validation.get("warnings") or [])
        if stored_summary and stored_summary != derived_summary:
            warnings.append("stored summary differs from derived payload summary")
        results[app_name] = {
            "ok": bool(validation.get("ok")),
            "missing_payload": False,
            "missing_keys": list(validation.get("missing_keys") or []),
            "warnings": warnings,
            "backup_summary": derived_summary,
            "stored_summary": stored_summary if isinstance(stored_summary, dict) else {},
        }
    return {
        "ok": all(bool(item.get("ok")) for item in results.values()) if results else False,
        "validated_apps": validated_apps,
        "requested_apps": selected_apps,
        "payload_created_at": str(payload.get("created_at") or ""),
        "storage_contract_present": isinstance(payload.get("storage_contract"), dict),
        "results": results,
    }


def rehearse_restore_main_data_contract(payload: dict[str, Any], *, apps: list[str] | None = None, replace_existing: bool = False) -> dict[str, Any]:
    validation = validate_main_data_contract_payload(payload, apps=apps)
    selected_apps = list(validation.get("requested_apps") or [])
    current_export = export_main_data_contract(selected_apps)
    current_summary = current_export.get("summary") if isinstance(current_export.get("summary"), dict) else {}

    rehearsed_results: dict[str, Any] = {}
    for app_name in selected_apps:
        validated = validation.get("results", {}).get(app_name) if isinstance(validation.get("results"), dict) else {}
        current_app_summary = current_summary.get(app_name) if isinstance(current_summary.get(app_name), dict) else {}
        rehearsed_results[app_name] = {
            **(validated if isinstance(validated, dict) else {}),
            "current_summary": current_app_summary,
            "current_has_data": _summary_has_business_data(current_app_summary),
            "replace_existing": bool(replace_existing),
            "would_overwrite_existing": bool(replace_existing) and _summary_has_business_data(current_app_summary),
        }

    return {
        "ok": bool(validation.get("ok")),
        "mode": "rehearsal",
        "requested_apps": selected_apps,
        "replace_existing": bool(replace_existing),
        "payload_created_at": str(payload.get("created_at") or ""),
        "results": rehearsed_results,
        "summary": backup_summary(),
    }


def export_main_data_contract(apps: list[str] | None = None) -> dict[str, Any]:
    selected_apps = normalize_backup_apps(apps)
    app_payloads: dict[str, Any] = {}
    app_summaries: dict[str, Any] = {}
    for app_name in selected_apps:
        payload = _export_app_payload(app_name)
        app_payloads[app_name] = payload
        app_summaries[app_name] = _derive_app_summary(app_name, payload)
    return {
        "contract": BACKUP_CONTRACT,
        "version": BACKUP_SCHEMA_VERSION,
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "apps": app_payloads,
        "app_order": selected_apps,
        "summary": app_summaries,
        "storage_contract": data_storage_contract(),
    }


def _app_payload_archive_name(app_name: str) -> str:
    return f"{BACKUP_APP_PAYLOAD_DIR}/{app_name}/main_data.json"


def _build_backup_manifest(payload: dict[str, Any]) -> dict[str, Any]:
    app_order = [str(item).strip() for item in list(payload.get("app_order") or []) if str(item).strip()]
    return {
        "contract": BACKUP_CONTRACT,
        "version": BACKUP_SCHEMA_VERSION,
        "created_at": payload.get("created_at"),
        "app_order": app_order,
        "summary": dict(payload.get("summary") or {}) if isinstance(payload.get("summary"), dict) else {},
        "app_payload_files": {
            app_name: _app_payload_archive_name(app_name)
            for app_name in app_order
        },
    }


def build_backup_archive_bytes(payload: dict[str, Any]) -> bytes:
    buffer = io.BytesIO()
    manifest = _build_backup_manifest(payload)
    storage_contract = payload.get("storage_contract") if isinstance(payload.get("storage_contract"), dict) else data_storage_contract()
    app_payloads = payload.get("apps") if isinstance(payload.get("apps"), dict) else {}
    with zipfile.ZipFile(buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(BACKUP_MANIFEST_FILENAME, json.dumps(manifest, ensure_ascii=False, indent=2))
        archive.writestr(BACKUP_STORAGE_CONTRACT_FILENAME, json.dumps(storage_contract, ensure_ascii=False, indent=2))
        for app_name in manifest["app_order"]:
            app_payload = app_payloads.get(app_name)
            if not isinstance(app_payload, dict):
                continue
            archive.writestr(_app_payload_archive_name(app_name), json.dumps(app_payload, ensure_ascii=False, indent=2))
    return buffer.getvalue()


def _snapshot_metadata(path: Path) -> dict[str, Any]:
    stat = path.stat()
    return {
        "name": path.name,
        "path": str(path),
        "path_relative": _relative_display_path(path),
        "size_bytes": int(stat.st_size),
        "updated_at": datetime.fromtimestamp(stat.st_mtime).isoformat(timespec="seconds"),
    }


def _load_backup_scheduler_state() -> dict[str, str]:
    try:
        payload = json.loads(BACKUP_SCHEDULER_STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        payload = {}
    if not isinstance(payload, dict):
        payload = {}
    return {
        "last_weekly_backup_week": str(payload.get("last_weekly_backup_week") or payload.get("last_monthly_backup_month") or ""),
        "last_weekly_backup_at": str(payload.get("last_weekly_backup_at") or payload.get("last_monthly_backup_at") or ""),
        "last_weekly_backup_name": str(payload.get("last_weekly_backup_name") or payload.get("last_monthly_backup_name") or ""),
    }


def _save_backup_scheduler_state(payload: dict[str, str]) -> None:
    BACKUP_SCHEDULER_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    BACKUP_SCHEDULER_STATE_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def backup_summary(limit: int = 5) -> dict[str, Any]:
    root = ensure_backup_snapshots_dir()
    files = sorted(root.glob("*.zip"), key=lambda item: item.stat().st_mtime, reverse=True)
    latest = _snapshot_metadata(files[0]) if files else {}
    scheduler_state = _load_backup_scheduler_state()
    return {
        "backup_dir": str(root),
        "backup_dir_relative": _relative_display_path(root),
        "snapshot_count": len(files),
        "latest_backup_name": str(latest.get("name") or ""),
        "latest_backup_at": str(latest.get("updated_at") or ""),
        "latest_backup_size_bytes": int(latest.get("size_bytes") or 0),
        "last_weekly_backup_week": str(scheduler_state.get("last_weekly_backup_week") or ""),
        "last_weekly_backup_at": str(scheduler_state.get("last_weekly_backup_at") or ""),
        "last_weekly_backup_name": str(scheduler_state.get("last_weekly_backup_name") or ""),
        "recent_snapshots": [_snapshot_metadata(path) for path in files[: max(1, int(limit or 1))]],
    }


def _write_backup_archive(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(build_backup_archive_bytes(payload))


def create_backup_snapshot(apps: list[str] | None = None, *, prefix: str = BACKUP_ARCHIVE_PREFIX) -> dict[str, Any]:
    payload = export_main_data_contract(apps)
    root = ensure_backup_snapshots_dir()
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = root / f"{prefix}_{stamp}.zip"
    _write_backup_archive(path, payload)
    return {
        "ok": True,
        "snapshot": _snapshot_metadata(path),
        "apps": list(payload.get("app_order") or []),
        "summary": backup_summary(),
    }


def _weekly_backup_key(current: datetime) -> str:
    iso_year, iso_week, _ = current.isocalendar()
    return f"{iso_year}-W{iso_week:02d}"


def _next_monday_window(current: datetime) -> datetime:
    days_ahead = (7 - current.weekday()) % 7
    if days_ahead == 0:
        days_ahead = 7
    next_day = (current + timedelta(days=days_ahead)).replace(hour=0, minute=0, second=0, microsecond=0)
    return next_day


def ensure_weekly_backup(now: datetime | None = None) -> dict[str, Any]:
    current = now or datetime.now()
    week_key = _weekly_backup_key(current)
    state = _load_backup_scheduler_state()
    if current.weekday() != 0:
        return {
            "ok": True,
            "created": False,
            "week": week_key,
            "due": False,
            "next_run_at": _next_monday_window(current).isoformat(timespec="seconds"),
            "state": state,
            "summary": backup_summary(),
        }
    if state.get("last_weekly_backup_week") == week_key:
        return {
            "ok": True,
            "created": False,
            "week": week_key,
            "due": True,
            "state": state,
            "summary": backup_summary(),
        }

    result = create_backup_snapshot(list(SUPPORTED_BACKUP_APPS))
    snapshot = result.get("snapshot") if isinstance(result.get("snapshot"), dict) else {}
    next_state = {
        "last_weekly_backup_week": week_key,
        "last_weekly_backup_at": str(snapshot.get("updated_at") or current.isoformat(timespec="seconds")),
        "last_weekly_backup_name": str(snapshot.get("name") or ""),
    }
    _save_backup_scheduler_state(next_state)
    return {
        "ok": True,
        "created": True,
        "week": week_key,
        "due": True,
        "state": next_state,
        "snapshot": snapshot,
        "summary": backup_summary(),
    }


def _backup_scheduler_loop() -> None:
    try:
        ensure_weekly_backup()
    except Exception:
        pass
    while not _BACKUP_SCHEDULER_STOP.wait(BACKUP_SCHEDULER_INTERVAL_SECONDS):
        try:
            ensure_weekly_backup()
        except Exception:
            continue


def start_scheduler() -> None:
    global _BACKUP_SCHEDULER_STARTED  # noqa: PLW0603
    with _BACKUP_SCHEDULER_LOCK:
        if _BACKUP_SCHEDULER_STARTED:
            return
        _BACKUP_SCHEDULER_STARTED = True
    thread = threading.Thread(target=_backup_scheduler_loop, daemon=True, name="data-backup-scheduler")
    thread.start()


def load_backup_payload(file_name: str, raw_bytes: bytes) -> dict[str, Any]:
    suffix = Path(str(file_name or "backup.json")).suffix.lower()
    if suffix == ".zip":
        with zipfile.ZipFile(io.BytesIO(raw_bytes), mode="r") as archive:
            archive_names = set(archive.namelist())
            if BACKUP_MANIFEST_FILENAME in archive_names:
                try:
                    manifest = json.loads(archive.read(BACKUP_MANIFEST_FILENAME).decode("utf-8"))
                except Exception as exc:
                    raise ValueError("backup archive manifest is not valid JSON") from exc
                if not isinstance(manifest, dict):
                    raise ValueError("backup archive manifest must be a JSON object")
                app_files = manifest.get("app_payload_files") if isinstance(manifest.get("app_payload_files"), dict) else {}
                app_order = [str(item).strip() for item in list(manifest.get("app_order") or []) if str(item).strip()]
                app_payloads: dict[str, Any] = {}
                for app_name in app_order:
                    payload_name = str(app_files.get(app_name) or _app_payload_archive_name(app_name)).strip()
                    if not payload_name:
                        continue
                    try:
                        app_payloads[app_name] = json.loads(archive.read(payload_name).decode("utf-8"))
                    except KeyError as exc:
                        raise ValueError(f"backup archive missing {payload_name}") from exc
                    except Exception as exc:
                        raise ValueError(f"backup archive payload for {app_name} is not valid JSON") from exc
                storage_contract = {}
                if BACKUP_STORAGE_CONTRACT_FILENAME in archive_names:
                    try:
                        storage_contract = json.loads(archive.read(BACKUP_STORAGE_CONTRACT_FILENAME).decode("utf-8"))
                    except Exception as exc:
                        raise ValueError("backup archive storage contract is not valid JSON") from exc
                payload = {
                    "contract": manifest.get("contract"),
                    "version": manifest.get("version"),
                    "created_at": manifest.get("created_at"),
                    "app_order": app_order,
                    "apps": app_payloads,
                    "summary": manifest.get("summary") if isinstance(manifest.get("summary"), dict) else {},
                    "storage_contract": storage_contract if isinstance(storage_contract, dict) else {},
                }
            else:
                try:
                    raw_payload = archive.read(BACKUP_PAYLOAD_FILENAME)
                except KeyError as exc:
                    raise ValueError(f"backup archive missing {BACKUP_MANIFEST_FILENAME}") from exc
                try:
                    payload = json.loads(raw_payload.decode("utf-8"))
                except Exception as exc:
                    raise ValueError("backup archive payload is not valid JSON") from exc
    else:
        try:
            payload = json.loads(raw_bytes.decode("utf-8"))
        except Exception as exc:
            raise ValueError("backup file is not valid JSON") from exc
    if not isinstance(payload, dict):
        raise ValueError("backup payload must be a JSON object")
    return payload


def restore_main_data_contract(payload: dict[str, Any], *, apps: list[str] | None = None, replace_existing: bool = False, create_safety_snapshot: bool = True) -> dict[str, Any]:
    validate_main_data_contract_payload(payload, apps=apps)

    all_payloads = payload.get("apps") if isinstance(payload.get("apps"), dict) else {}
    selected_apps = normalize_backup_apps(apps or list(all_payloads.keys()))
    safety_snapshot: dict[str, Any] | None = None
    if create_safety_snapshot and selected_apps:
        safety_snapshot = create_backup_snapshot(selected_apps, prefix=PRE_RESTORE_ARCHIVE_PREFIX)
    restored: dict[str, Any] = {}
    for app_name in selected_apps:
        app_payload = all_payloads.get(app_name)
        if not isinstance(app_payload, dict):
            continue
        restored[app_name] = _import_app_payload(app_name, dict(app_payload), replace_existing=bool(replace_existing))
    return {
        "ok": True,
        "restored_apps": list(restored.keys()),
        "replace_existing": bool(replace_existing),
        "results": restored,
        "pre_restore_backup": safety_snapshot,
        "summary": backup_summary(),
    }