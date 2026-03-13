from __future__ import annotations

import os
from pathlib import Path

WORKSPACE_ROOT = Path(__file__).resolve().parent.parent
CORE_SERVICE_ROOT = WORKSPACE_ROOT.parent / "core_service"
CORE_DATA_DIR = CORE_SERVICE_ROOT / "data"
SCRIPTS_DIR = WORKSPACE_ROOT / "scripts"
DATA_DIR = WORKSPACE_ROOT / "data"
DOCUMENTS_DIR = WORKSPACE_ROOT / "documents"
_vector_db_env = (os.getenv("AI_SUMMARY_VECTOR_DB_DIR", "") or "").strip()


def _has_vector_index(path: Path) -> bool:
	meta = path / "metadata.json"
	faiss = path / "faiss.index"
	backend = path / "backend.json"
	return meta.exists() and (faiss.exists() or backend.exists())


if _vector_db_env:
	VECTOR_DB_DIR = Path(_vector_db_env)
else:
	# Converged default: always use the shared core_service vector DB.
	VECTOR_DB_DIR = CORE_DATA_DIR / "vector_db"
RAG_SESSIONS_DIR = DATA_DIR / "rag_sessions"
WEB_SESSIONS_FILE = RAG_SESSIONS_DIR / "web_sessions.json"

HOST = os.getenv("AI_SUMMARY_WEB_HOST", "0.0.0.0")
PORT = int(os.getenv("AI_SUMMARY_WEB_PORT", "8000"))

# Future public exposure option: trust Cloudflare Access headers when enabled.
ENABLE_CF_ACCESS_HEADERS = os.getenv("AI_SUMMARY_ENABLE_CF_ACCESS_HEADERS", "0") == "1"
