from __future__ import annotations

import os
import shutil
from pathlib import Path

from core_service.runtime_data import app_runtime_root


APP_ROOT = Path(__file__).resolve().parent
WORKSPACE_ROOT = APP_ROOT.parent
CORE_SERVICE_ROOT = WORKSPACE_ROOT / "core_service"
SCRIPTS_DIR = APP_ROOT / "scripts"
DATA_DIR = app_runtime_root("ai_conversations_summary")

_documents_dir_env = (os.getenv("AI_SUMMARY_DOCUMENTS_DIR", "") or "").strip()
DOCUMENTS_DIR = Path(_documents_dir_env) if _documents_dir_env else DATA_DIR / "documents"

_vector_db_dir_env = (os.getenv("AI_SUMMARY_VECTOR_DB_DIR", "") or "").strip()
VECTOR_DB_DIR = Path(_vector_db_dir_env) if _vector_db_dir_env else DATA_DIR / "vector_db"

PROCESSING_DIR = DATA_DIR / "processing"
SESSIONS_DIR = DATA_DIR / "sessions"
STATE_DIR = DATA_DIR / "state"
OBSERVABILITY_DIR = DATA_DIR / "observability"
RAG_SESSIONS_DIR = SESSIONS_DIR / "rag"
WEB_SESSIONS_FILE = RAG_SESSIONS_DIR / "web_sessions.json"
MEMORY_DIR = RAG_SESSIONS_DIR / "_memory"
DEBUG_DIR = RAG_SESSIONS_DIR / "debug_data"
RETRIEVAL_METRICS_FILE = RAG_SESSIONS_DIR / "retrieval_metrics.json"
CACHE_DIR = DATA_DIR / "cache"
RAW_DIR = PROCESSING_DIR / "raw"
EXTRACTED_DIR = PROCESSING_DIR / "extracted"
SUMMARIZE_DIR = PROCESSING_DIR / "summarize"
SPLIT_DIR = PROCESSING_DIR / "split"
WORKFLOW_STATE_PATH = STATE_DIR / "workflow_state.json"
RAG_REBUILD_PID_PATH = STATE_DIR / "rag_rebuild.pid"
DEEPSEEK_AUDIT_DIR = OBSERVABILITY_DIR / "deepseek_api_audit"
HF_CACHE_DIR = DATA_DIR / "hf_cache"
LOCAL_MODELS_DIR = DATA_DIR / "local_models"


def _runtime_layout_entries() -> list[tuple[Path, Path]]:
	return [
		(DATA_DIR / "rag_sessions", RAG_SESSIONS_DIR),
		(DATA_DIR / "raw_dir", RAW_DIR),
		(DATA_DIR / "extracted_dir", EXTRACTED_DIR),
		(DATA_DIR / "summarize_dir", SUMMARIZE_DIR),
		(DATA_DIR / "split_dir", SPLIT_DIR),
		(DATA_DIR / "workflow_state.json", WORKFLOW_STATE_PATH),
		(DATA_DIR / "rag_rebuild.pid", RAG_REBUILD_PID_PATH),
		(DATA_DIR / "deepseek_api_audit", DEEPSEEK_AUDIT_DIR),
	]


def _remove_empty_parents(path: Path, *, stop: Path) -> None:
	current = path.parent
	while current != stop and current.exists():
		try:
			current.rmdir()
		except OSError:
			break
		current = current.parent


def _merge_directory_contents(source: Path, target: Path) -> None:
	target.mkdir(parents=True, exist_ok=True)
	for child in source.iterdir():
		destination = target / child.name
		if child.is_dir():
			if destination.exists() and destination.is_dir():
				_merge_directory_contents(child, destination)
				try:
					child.rmdir()
				except OSError:
					pass
				continue
			shutil.move(str(child), str(destination))
			continue
		if destination.exists():
			continue
		shutil.move(str(child), str(destination))
	try:
		source.rmdir()
	except OSError:
		pass


def _normalize_layout_entry(source: Path, target: Path) -> None:
	if source == target or not source.exists():
		return
	target.parent.mkdir(parents=True, exist_ok=True)
	if source.is_dir():
		if target.exists() and target.is_dir():
			_merge_directory_contents(source, target)
		elif not target.exists():
			shutil.move(str(source), str(target))
		return
	if target.exists():
		return
	shutil.move(str(source), str(target))
	_remove_empty_parents(source, stop=DATA_DIR)


def ensure_ai_summary_runtime_layout() -> None:
	for directory in (DATA_DIR, PROCESSING_DIR, SESSIONS_DIR, STATE_DIR, OBSERVABILITY_DIR, CACHE_DIR, HF_CACHE_DIR, LOCAL_MODELS_DIR, RAG_SESSIONS_DIR):
		directory.mkdir(parents=True, exist_ok=True)
	for source, target in _runtime_layout_entries():
		_normalize_layout_entry(source, target)


ensure_ai_summary_runtime_layout()