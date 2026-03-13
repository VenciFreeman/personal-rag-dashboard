from __future__ import annotations

import os

HOST = os.getenv("LIBRARY_WEB_HOST", "127.0.0.1")
PORT = int(os.getenv("LIBRARY_WEB_PORT", "8091"))
