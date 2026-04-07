from __future__ import annotations

import os
import signal
import threading

try:
    from library_tracker.web.services import analysis_service, library_service
except ImportError:
    from web.services import analysis_service, library_service

_STOP = threading.Event()


def _install_signal_handlers() -> None:
    def _handle_signal(_signum, _frame) -> None:
        _STOP.set()

    for signum in (getattr(signal, "SIGINT", None), getattr(signal, "SIGTERM", None)):
        if signum is None:
            continue
        try:
            signal.signal(signum, _handle_signal)
        except Exception:
            continue


def run() -> None:
    _install_signal_handlers()
    analysis_service.start_scheduler()

    delay_seconds = max(0.0, float(os.getenv("LIBRARY_TRACKER_STARTUP_EMBED_DELAY_SECONDS", "8")))
    if not _STOP.wait(delay_seconds):
        try:
            library_service.refresh_pending_embeddings()
        except Exception:
            pass

    try:
        while not _STOP.wait(1.0):
            continue
    except KeyboardInterrupt:
        _STOP.set()


if __name__ == "__main__":
    run()