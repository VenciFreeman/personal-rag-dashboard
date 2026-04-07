from __future__ import annotations

from typing import Any

from fastapi import FastAPI, Request

from .lan_auth import AuthContext
from .lan_auth import get_request_auth as get_request_auth_context
from .lan_auth import init_lan_auth_storage as ensure_auth_storage
from .lan_auth import install_lan_auth


def install_app_auth(app: FastAPI, *, app_id: str, app_title: str) -> None:
    install_lan_auth(app, app_id=app_id, app_title=app_title)


def get_request_auth(request: Request) -> AuthContext | None:
    return get_request_auth_context(request)


__all__ = [
    "AuthContext",
    "ensure_auth_storage",
    "get_request_auth",
    "get_request_auth_context",
    "install_app_auth",
]