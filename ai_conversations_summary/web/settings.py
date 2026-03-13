from __future__ import annotations

"""Compatibility aliases for legacy imports.

Use `web.config` directly for new code.
"""

from web.config import (  # noqa: F401
    DOCUMENTS_DIR,
    ENABLE_CF_ACCESS_HEADERS,
    HOST,
    PORT,
    RAG_SESSIONS_DIR,
    SCRIPTS_DIR,
    VECTOR_DB_DIR,
    WORKSPACE_ROOT as ROOT_DIR,
)

WEB_HOST = HOST
WEB_PORT = PORT
TRUST_CF_ACCESS = ENABLE_CF_ACCESS_HEADERS
WEB_CORS_ALLOW_ORIGINS = "*"
