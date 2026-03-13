from __future__ import annotations

import os
import sys

from web.config import SCRIPTS_DIR


if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from api_config import API_BASE_URL, API_KEY, EMBEDDING_MODEL, MODEL, TAVILY_API_KEY, TIMEOUT  # noqa: E402
from rag_vector_index import RAGIndexError, search_vector_index_with_diagnostics  # noqa: E402


DEFAULT_API_BASE_URL = os.getenv("DEEPSEEK_BASE_URL", API_BASE_URL).strip() or API_BASE_URL
DEFAULT_API_KEY = os.getenv("DEEPSEEK_API_KEY", API_KEY).strip() or API_KEY
DEFAULT_MODEL = MODEL
DEFAULT_EMBEDDING_MODEL = EMBEDDING_MODEL
DEFAULT_TAVILY_API_KEY = TAVILY_API_KEY
DEFAULT_TIMEOUT = TIMEOUT
