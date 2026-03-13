from __future__ import annotations

import os

HOST = os.getenv("NAV_DASHBOARD_WEB_HOST", "0.0.0.0")
PORT = int(os.getenv("NAV_DASHBOARD_WEB_PORT", "8092"))

AI_SUMMARY_URL_OVERRIDE = (os.getenv("NAV_DASHBOARD_AI_SUMMARY_URL", "") or "").strip()
LIBRARY_TRACKER_URL_OVERRIDE = (os.getenv("NAV_DASHBOARD_LIBRARY_TRACKER_URL", "") or "").strip()
