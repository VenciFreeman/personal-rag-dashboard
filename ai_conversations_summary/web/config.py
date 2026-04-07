from __future__ import annotations

import os
from ai_conversations_summary.runtime_paths import CORE_SERVICE_ROOT, DATA_DIR, DEBUG_DIR, DOCUMENTS_DIR, MEMORY_DIR, RAG_SESSIONS_DIR, RETRIEVAL_METRICS_FILE, SCRIPTS_DIR, VECTOR_DB_DIR, WEB_SESSIONS_FILE, WORKSPACE_ROOT

HOST = os.getenv("AI_SUMMARY_WEB_HOST", "0.0.0.0")
PORT = int(os.getenv("AI_SUMMARY_WEB_PORT", "8000"))

# Future public exposure option: trust Cloudflare Access headers when enabled.
ENABLE_CF_ACCESS_HEADERS = os.getenv("AI_SUMMARY_ENABLE_CF_ACCESS_HEADERS", "0") == "1"
