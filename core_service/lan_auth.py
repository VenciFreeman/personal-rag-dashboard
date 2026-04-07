from __future__ import annotations

import html
import ipaddress
import json
import secrets
import sqlite3
import threading
import urllib.parse
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerifyMismatchError
from fastapi import APIRouter, FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from starlette.middleware.base import BaseHTTPMiddleware

from .runtime_data import lan_auth_db_path


DB_PATH = lan_auth_db_path()
DATA_DIR = DB_PATH.parent
COOKIE_NAME = "personal_ai_stack_session"
SESSION_TTL_DAYS = 180
SESSION_TOUCH_INTERVAL_SECONDS = 300
PASSWORD_MIN_LENGTH = 8
ADMIN_REAUTH_TTL_SECONDS = 10 * 60

AVAILABLE_APPS: dict[str, str] = {
    "nav_dashboard": "Navigation Dashboard",
    "rag_system": "RAG System",
    "library_tracker": "Library Tracker",
    "property": "Property Management",
    "journey": "Journey Archive",
}

PUBLIC_PATHS = {
    "/healthz",
    "/_auth/login",
    "/_auth/logout",
}

PASSWORD_HASHER = PasswordHasher()
_ADMIN_REAUTH_LOCK = threading.Lock()
_ADMIN_REAUTH_TOKENS: dict[str, dict[str, Any]] = {}


@dataclass(slots=True)
class AuthContext:
    user_id: str
    username: str
    role: str
    allowed_apps: set[str]
    auth_version: int
    is_local: bool = False

    def can_access(self, app_id: str) -> bool:
        return self.is_local or self.role == "admin" or app_id in self.allowed_apps


class LANAuthMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: Any, *, app_id: str, app_title: str) -> None:
        super().__init__(app)
        self.app_id = app_id
        self.app_title = app_title

    async def dispatch(self, request: Request, call_next: Any) -> Response:
        request.state.lan_auth = None
        request.state.lan_auth_app_id = self.app_id
        request.state.lan_auth_app_title = self.app_title

        if request.method.upper() == "OPTIONS" or _is_public_path(request.url.path):
            return await call_next(request)

        if _is_loopback_request(request):
            request.state.lan_auth = AuthContext(
                user_id="localhost",
                username="localhost",
                role="admin",
                allowed_apps=set(AVAILABLE_APPS),
                auth_version=0,
                is_local=True,
            )
            return await call_next(request)

        session_id = str(request.cookies.get(COOKIE_NAME) or "").strip()
        if not session_id:
            return _unauthorized_response(request, clear_cookie=False)

        auth = get_session_auth_context(session_id)
        if auth is None:
            return _unauthorized_response(request, clear_cookie=True)

        if not auth.can_access(self.app_id):
            return _forbidden_response(request, self.app_title)

        request.state.lan_auth = auth
        return await call_next(request)


def install_lan_auth(app: FastAPI, *, app_id: str, app_title: str) -> None:
    app.add_event_handler("startup", init_lan_auth_storage)
    app.add_middleware(LANAuthMiddleware, app_id=app_id, app_title=app_title)
    app.include_router(build_lan_auth_router(app_id=app_id, app_title=app_title))


def get_request_auth(request: Request) -> AuthContext | None:
    auth = getattr(request.state, "lan_auth", None)
    return auth if isinstance(auth, AuthContext) else None


def init_lan_auth_storage() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with _connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id TEXT PRIMARY KEY,
                username TEXT NOT NULL,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL DEFAULT 'user',
                allowed_apps_json TEXT NOT NULL DEFAULT '[]',
                auth_version INTEGER NOT NULL DEFAULT 1,
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sessions (
                session_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                created_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                auth_version INTEGER NOT NULL,
                user_agent TEXT NOT NULL DEFAULT '',
                ip_hint TEXT NOT NULL DEFAULT '',
                is_revoked INTEGER NOT NULL DEFAULT 0,
                FOREIGN KEY(user_id) REFERENCES users(user_id) ON DELETE CASCADE
            )
            """
        )
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_username_ci ON users(lower(username))")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON sessions(user_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sessions_expires_at ON sessions(expires_at)")


def build_lan_auth_router(*, app_id: str, app_title: str) -> APIRouter:
    router = APIRouter(prefix="/_auth", include_in_schema=False)

    @router.get("/login")
    async def login_page(request: Request) -> Response:
        next_target = _sanitize_next_target(request.query_params.get("next"))
        if _is_loopback_request(request):
            return RedirectResponse(next_target, status_code=303)
        if get_request_auth(request) is not None:
            return RedirectResponse(next_target, status_code=303)
        users_exist = user_count() > 0
        body = _render_login_page(
            app_title=app_title,
            next_target=next_target,
            error_message=str(request.query_params.get("error") or "").strip(),
            users_exist=users_exist,
        )
        return HTMLResponse(body)

    @router.post("/login")
    async def login_submit(request: Request) -> Response:
        next_target = _sanitize_next_target(request.query_params.get("next"))
        if _is_loopback_request(request):
            return RedirectResponse(next_target, status_code=303)

        form = await _parse_form_body(request)
        username = _form_value(form, "username").lower()
        password = _form_value(form, "password")
        posted_next = _sanitize_next_target(_form_value(form, "next") or next_target)

        if user_count() <= 0:
            return HTMLResponse(
                _render_login_page(
                    app_title=app_title,
                    next_target=posted_next,
                    error_message="尚未初始化管理员账号，请先在本机打开 /_auth/admin 完成初始化。",
                    users_exist=False,
                ),
                status_code=400,
            )

        auth = authenticate_user(username, password)
        if auth is None:
            return HTMLResponse(
                _render_login_page(
                    app_title=app_title,
                    next_target=posted_next,
                    error_message="用户名或密码错误。",
                    users_exist=True,
                ),
                status_code=401,
            )

        if not auth.can_access(app_id):
            return HTMLResponse(
                _render_login_page(
                    app_title=app_title,
                    next_target=posted_next,
                    error_message="该账号无权访问当前应用。",
                    users_exist=True,
                ),
                status_code=403,
            )

        session_id = create_session(
            user_id=auth.user_id,
            auth_version=auth.auth_version,
            user_agent=request.headers.get("user-agent", ""),
            ip_hint=request.client.host if request.client else "",
        )
        response = RedirectResponse(posted_next, status_code=303)
        _set_session_cookie(response, session_id, request)
        return response

    @router.post("/logout")
    async def logout_submit(request: Request) -> Response:
        next_target = _sanitize_next_target(request.query_params.get("next") or "/")
        session_id = str(request.cookies.get(COOKIE_NAME) or "").strip()
        if session_id:
            revoke_session(session_id)
        response = RedirectResponse(f"/_auth/login?next={urllib.parse.quote(next_target, safe='')}", status_code=303)
        response.delete_cookie(COOKIE_NAME, path="/")
        return response

    @router.get("/admin")
    async def admin_page(request: Request) -> Response:
        if not _is_admin_or_local(request):
            return _forbidden_response(request, app_title)
        users_exist = user_count() > 0
        if not users_exist and not _is_loopback_request(request):
            return _forbidden_response(request, app_title)
        flash = str(request.query_params.get("msg") or "").strip()
        error = str(request.query_params.get("error") or "").strip()
        return HTMLResponse(
            _render_admin_page(
                app_title=app_title,
                users=list_users_for_admin(),
                users_exist=users_exist,
                flash_message=flash,
                error_message=error,
                is_local=_is_loopback_request(request),
            )
        )

    @router.post("/admin/bootstrap")
    async def admin_bootstrap(request: Request) -> Response:
        if not _is_loopback_request(request):
            return _forbidden_response(request, app_title)
        if user_count() > 0:
            return _admin_redirect(error="管理员已初始化。")
        form = await _parse_form_body(request)
        username = _form_value(form, "username").lower()
        password = _form_value(form, "password")
        try:
            create_user(
                username=username,
                password=password,
                role="admin",
                allowed_apps=list(AVAILABLE_APPS),
                is_active=True,
            )
        except ValueError as exc:
            return _admin_redirect(error=str(exc))
        return _admin_redirect(message="管理员账号已创建。")

    @router.post("/admin/users/create")
    async def admin_create_user(request: Request) -> Response:
        if not _is_admin_or_local(request):
            return _forbidden_response(request, app_title)
        form = await _parse_form_body(request)
        username = _form_value(form, "username").lower()
        password = _form_value(form, "password")
        role = _normalize_role(_form_value(form, "role"))
        allowed_apps = _normalize_allowed_apps(_form_list(form, "allowed_apps"))
        is_active = _form_checkbox(form, "is_active")
        try:
            create_user(
                username=username,
                password=password,
                role=role,
                allowed_apps=allowed_apps,
                is_active=is_active,
            )
        except ValueError as exc:
            return _admin_redirect(error=str(exc))
        return _admin_redirect(message=f"用户 {username} 已创建。")

    @router.post("/admin/users/{user_id}")
    async def admin_update_user(user_id: str, request: Request) -> Response:
        if not _is_admin_or_local(request):
            return _forbidden_response(request, app_title)
        form = await _parse_form_body(request)
        role = _normalize_role(_form_value(form, "role"))
        allowed_apps = _normalize_allowed_apps(_form_list(form, "allowed_apps"))
        is_active = _form_checkbox(form, "is_active")
        new_password = _form_value(form, "new_password")
        try:
            update_user(
                user_id=user_id,
                role=role,
                allowed_apps=allowed_apps,
                is_active=is_active,
                new_password=new_password,
            )
        except ValueError as exc:
            return _admin_redirect(error=str(exc))
        return _admin_redirect(message="用户权限已更新。")

    @router.get("/api/admin/state")
    async def admin_state_api(request: Request) -> Response:
        users_exist = user_count() > 0
        if users_exist:
            auth_error = _require_recent_admin_reauth(request)
            if auth_error is not None:
                return auth_error
        elif not _is_loopback_request(request):
            return _forbidden_response(request, app_title)
        auth = get_request_auth(request)
        return JSONResponse(
            {
                "ok": True,
                "users_exist": users_exist,
                "is_local": _is_loopback_request(request),
                "current_username": auth.username if auth else "",
                "available_apps": [
                    {"app_id": app_key, "label": app_label}
                    for app_key, app_label in AVAILABLE_APPS.items()
                ],
                "users": list_users_for_admin() if users_exist else [],
            }
        )

    @router.post("/api/admin/reauth")
    async def admin_reauth_api(request: Request) -> Response:
        if not _is_admin_or_local(request):
            return _forbidden_response(request, app_title)
        payload = await _parse_json_body(request)
        password = str(payload.get("password") or "")
        username_hint = str(payload.get("username") or "")
        result = _verify_admin_password_for_request(request, password=password, username_hint=username_hint)
        if result is None:
            return JSONResponse({"detail": "invalid_admin_password"}, status_code=401)
        token, expires_at = create_admin_reauth_token(
            username=result["username"],
            request=request,
        )
        return JSONResponse(
            {
                "ok": True,
                "username": result["username"],
                "token": token,
                "expires_at": expires_at,
            }
        )

    @router.post("/api/admin/bootstrap")
    async def admin_bootstrap_api(request: Request) -> Response:
        if not _is_loopback_request(request):
            return _forbidden_response(request, app_title)
        if user_count() > 0:
            return JSONResponse({"detail": "管理员已初始化。"}, status_code=400)
        payload = await _parse_json_body(request)
        username = str(payload.get("username") or "").lower()
        password = str(payload.get("password") or "")
        try:
            create_user(
                username=username,
                password=password,
                role="admin",
                allowed_apps=list(AVAILABLE_APPS),
                is_active=True,
            )
        except ValueError as exc:
            return JSONResponse({"detail": str(exc)}, status_code=400)
        return JSONResponse({"ok": True, "users": list_users_for_admin()})

    @router.post("/api/admin/users")
    async def admin_create_user_api(request: Request) -> Response:
        auth_error = _require_recent_admin_reauth(request)
        if auth_error is not None:
            return auth_error
        payload = await _parse_json_body(request)
        username = str(payload.get("username") or "").lower()
        password = str(payload.get("password") or "")
        role = _normalize_role(str(payload.get("role") or "user"))
        allowed_apps = _normalize_allowed_apps([str(item or "") for item in (payload.get("allowed_apps") or [])])
        is_active = bool(payload.get("is_active", True))
        try:
            user_id = create_user(
                username=username,
                password=password,
                role=role,
                allowed_apps=allowed_apps,
                is_active=is_active,
            )
        except ValueError as exc:
            return JSONResponse({"detail": str(exc)}, status_code=400)
        return JSONResponse(
            {
                "ok": True,
                "user_id": user_id,
                "users": list_users_for_admin(),
            }
        )

    @router.patch("/api/admin/users/{user_id}")
    async def admin_update_user_api(user_id: str, request: Request) -> Response:
        auth_error = _require_recent_admin_reauth(request)
        if auth_error is not None:
            return auth_error
        payload = await _parse_json_body(request)
        role = _normalize_role(str(payload.get("role") or "user"))
        allowed_apps = _normalize_allowed_apps([str(item or "") for item in (payload.get("allowed_apps") or [])])
        is_active = bool(payload.get("is_active", True))
        new_password = str(payload.get("new_password") or "")
        try:
            update_user(
                user_id=user_id,
                role=role,
                allowed_apps=allowed_apps,
                is_active=is_active,
                new_password=new_password,
            )
        except ValueError as exc:
            return JSONResponse({"detail": str(exc)}, status_code=400)
        return JSONResponse({"ok": True, "users": list_users_for_admin()})

    return router


def create_user(*, username: str, password: str, role: str, allowed_apps: list[str], is_active: bool) -> str:
    normalized_username = _normalize_username(username)
    _validate_password(password)
    normalized_role = _normalize_role(role)
    normalized_apps = list(AVAILABLE_APPS) if normalized_role == "admin" else _normalize_allowed_apps(allowed_apps)
    now = _utcnow_text()
    user_id = uuid4().hex
    password_hash = PASSWORD_HASHER.hash(password)
    try:
        with _connect() as conn:
            conn.execute(
                """
                INSERT INTO users(user_id, username, password_hash, role, allowed_apps_json, auth_version, is_active, created_at, updated_at)
                VALUES(?,?,?,?,?,?,?,?,?)
                """,
                (
                    user_id,
                    normalized_username,
                    password_hash,
                    normalized_role,
                    json.dumps(normalized_apps, ensure_ascii=False),
                    1,
                    1 if is_active else 0,
                    now,
                    now,
                ),
            )
    except sqlite3.IntegrityError as exc:
        raise ValueError("用户名已存在。") from exc
    return user_id


def update_user(*, user_id: str, role: str, allowed_apps: list[str], is_active: bool, new_password: str = "") -> None:
    with _connect() as conn:
        row = conn.execute(
            "SELECT user_id, role, auth_version FROM users WHERE user_id=?",
            (user_id,),
        ).fetchone()
        if row is None:
            raise ValueError("用户不存在。")
        auth_version = int(row["auth_version"] or 1)
        normalized_role = _normalize_role(role)
        normalized_apps = list(AVAILABLE_APPS) if normalized_role == "admin" else _normalize_allowed_apps(allowed_apps)
        updates: list[str] = [
            "role=?",
            "allowed_apps_json=?",
            "is_active=?",
            "updated_at=?",
        ]
        params: list[Any] = [
            normalized_role,
            json.dumps(normalized_apps, ensure_ascii=False),
            1 if is_active else 0,
            _utcnow_text(),
        ]
        if str(new_password or "").strip():
            _validate_password(new_password)
            auth_version += 1
            updates.extend(["password_hash=?", "auth_version=?"])
            params.extend([PASSWORD_HASHER.hash(new_password), auth_version])
        params.append(user_id)
        conn.execute(f"UPDATE users SET {', '.join(updates)} WHERE user_id=?", tuple(params))


def authenticate_user(username: str, password: str) -> AuthContext | None:
    normalized_username = _normalize_username(username)
    row = _fetch_user_by_username(normalized_username)
    if row is None or not bool(row["is_active"]):
        return None
    try:
        ok = PASSWORD_HASHER.verify(str(row["password_hash"] or ""), password)
    except (VerifyMismatchError, InvalidHashError):
        return None
    if not ok:
        return None
    return _auth_context_from_user_row(row)


def create_session(*, user_id: str, auth_version: int, user_agent: str, ip_hint: str) -> str:
    now = _utcnow()
    expires_at = now + timedelta(days=SESSION_TTL_DAYS)
    session_id = secrets.token_urlsafe(32)
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO sessions(session_id, user_id, created_at, last_seen_at, expires_at, auth_version, user_agent, ip_hint, is_revoked)
            VALUES(?,?,?,?,?,?,?,?,0)
            """,
            (
                session_id,
                user_id,
                _dt_to_text(now),
                _dt_to_text(now),
                _dt_to_text(expires_at),
                int(auth_version),
                str(user_agent or "")[:300],
                str(ip_hint or "")[:80],
            ),
        )
    return session_id


def revoke_session(session_id: str) -> None:
    with _connect() as conn:
        conn.execute("UPDATE sessions SET is_revoked=1 WHERE session_id=?", (session_id,))


def get_session_auth_context(session_id: str) -> AuthContext | None:
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT
                s.session_id,
                s.user_id,
                s.last_seen_at,
                s.expires_at,
                s.auth_version AS session_auth_version,
                s.is_revoked,
                u.username,
                u.role,
                u.allowed_apps_json,
                u.auth_version AS user_auth_version,
                u.is_active
            FROM sessions s
            JOIN users u ON u.user_id = s.user_id
            WHERE s.session_id=?
            """,
            (session_id,),
        ).fetchone()
        if row is None:
            return None
        if bool(row["is_revoked"]) or not bool(row["is_active"]):
            return None
        if int(row["session_auth_version"] or 0) != int(row["user_auth_version"] or -1):
            conn.execute("UPDATE sessions SET is_revoked=1 WHERE session_id=?", (session_id,))
            return None
        expires_at = _parse_dt(row["expires_at"])
        now = _utcnow()
        if expires_at is None or expires_at <= now:
            conn.execute("UPDATE sessions SET is_revoked=1 WHERE session_id=?", (session_id,))
            return None

        last_seen_at = _parse_dt(row["last_seen_at"])
        if last_seen_at is None or (now - last_seen_at).total_seconds() >= SESSION_TOUCH_INTERVAL_SECONDS:
            conn.execute(
                "UPDATE sessions SET last_seen_at=?, expires_at=? WHERE session_id=?",
                (_dt_to_text(now), _dt_to_text(now + timedelta(days=SESSION_TTL_DAYS)), session_id),
            )

    return AuthContext(
        user_id=str(row["user_id"] or ""),
        username=str(row["username"] or ""),
        role=_normalize_role(str(row["role"] or "user")),
        allowed_apps=set(_normalize_allowed_apps(_parse_allowed_apps(row["allowed_apps_json"]))),
        auth_version=int(row["user_auth_version"] or 1),
        is_local=False,
    )


def list_users_for_admin() -> list[dict[str, Any]]:
    now = _dt_to_text(_utcnow())
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT
                u.user_id,
                u.username,
                u.role,
                u.allowed_apps_json,
                u.auth_version,
                u.is_active,
                u.created_at,
                u.updated_at,
                COUNT(CASE WHEN s.is_revoked=0 AND s.expires_at>? AND s.auth_version=u.auth_version THEN 1 END) AS active_session_count
            FROM users u
            LEFT JOIN sessions s ON s.user_id = u.user_id
            GROUP BY u.user_id
            ORDER BY lower(u.username) ASC
            """,
            (now,),
        ).fetchall()
    output: list[dict[str, Any]] = []
    for row in rows:
        output.append(
            {
                "user_id": str(row["user_id"] or ""),
                "username": str(row["username"] or ""),
                "role": _normalize_role(str(row["role"] or "user")),
                "allowed_apps": _normalize_allowed_apps(_parse_allowed_apps(row["allowed_apps_json"])),
                "auth_version": int(row["auth_version"] or 1),
                "is_active": bool(row["is_active"]),
                "created_at": str(row["created_at"] or ""),
                "updated_at": str(row["updated_at"] or ""),
                "active_session_count": int(row["active_session_count"] or 0),
            }
        )
    return output


def user_count() -> int:
    with _connect() as conn:
        row = conn.execute("SELECT COUNT(*) AS count FROM users").fetchone()
    return int(row["count"] or 0) if row else 0


def create_admin_reauth_token(*, username: str, request: Request) -> tuple[str, str]:
    expires_at = _utcnow() + timedelta(seconds=ADMIN_REAUTH_TTL_SECONDS)
    token = secrets.token_urlsafe(24)
    record = {
        "username": _normalize_username(username),
        "is_local": _is_loopback_request(request),
        "client_host": str(getattr(request.client, "host", "") or ""),
        "user_agent": str(request.headers.get("user-agent") or "")[:300],
        "expires_at": _dt_to_text(expires_at),
    }
    with _ADMIN_REAUTH_LOCK:
        _prune_admin_reauth_tokens_locked()
        _ADMIN_REAUTH_TOKENS[token] = record
    return token, record["expires_at"]


def validate_admin_reauth_token(request: Request, token: str) -> bool:
    cleaned = str(token or "").strip()
    if not cleaned:
        return False
    with _ADMIN_REAUTH_LOCK:
        _prune_admin_reauth_tokens_locked()
        record = _ADMIN_REAUTH_TOKENS.get(cleaned)
    if not record:
        return False
    if bool(record.get("is_local")) != _is_loopback_request(request):
        return False
    if str(record.get("client_host") or "") != str(getattr(request.client, "host", "") or ""):
        return False
    if str(record.get("user_agent") or "") != str(request.headers.get("user-agent") or "")[:300]:
        return False
    if _is_loopback_request(request):
        return True
    auth = get_request_auth(request)
    return auth is not None and auth.role == "admin" and auth.username == str(record.get("username") or "")


def _fetch_user_by_username(username: str) -> sqlite3.Row | None:
    with _connect() as conn:
        return conn.execute("SELECT * FROM users WHERE lower(username)=lower(?) LIMIT 1", (username,)).fetchone()


def _list_admin_users() -> list[sqlite3.Row]:
    with _connect() as conn:
        return conn.execute(
            "SELECT * FROM users WHERE role='admin' AND is_active=1 ORDER BY lower(username) ASC"
        ).fetchall()


def _verify_password_against_user_row(row: sqlite3.Row, password: str) -> bool:
    try:
        return bool(PASSWORD_HASHER.verify(str(row["password_hash"] or ""), password))
    except (VerifyMismatchError, InvalidHashError):
        return False


def _verify_admin_password_for_request(request: Request, *, password: str, username_hint: str = "") -> dict[str, str] | None:
    candidate_password = str(password or "")
    if not candidate_password:
        return None
    auth = get_request_auth(request)
    if auth is not None and auth.role == "admin" and not auth.is_local:
        row = _fetch_user_by_username(auth.username)
        if row is not None and bool(row["is_active"]) and str(row["role"] or "") == "admin" and _verify_password_against_user_row(row, candidate_password):
            return {"username": auth.username}
        return None

    admin_rows = _list_admin_users()
    normalized_hint = ""
    if str(username_hint or "").strip():
        try:
            normalized_hint = _normalize_username(username_hint)
        except ValueError:
            return None
        admin_rows = [row for row in admin_rows if str(row["username"] or "") == normalized_hint]
    for row in admin_rows:
        if _verify_password_against_user_row(row, candidate_password):
            return {"username": str(row["username"] or "")}
    return None


def _auth_context_from_user_row(row: sqlite3.Row) -> AuthContext:
    role = _normalize_role(str(row["role"] or "user"))
    allowed_apps = list(AVAILABLE_APPS) if role == "admin" else _normalize_allowed_apps(_parse_allowed_apps(row["allowed_apps_json"]))
    return AuthContext(
        user_id=str(row["user_id"] or ""),
        username=str(row["username"] or ""),
        role=role,
        allowed_apps=set(allowed_apps),
        auth_version=int(row["auth_version"] or 1),
        is_local=False,
    )


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


async def _parse_json_body(request: Request) -> dict[str, Any]:
    try:
        payload = await request.json()
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _prune_admin_reauth_tokens_locked() -> None:
    now = _utcnow()
    expired = []
    for token, record in _ADMIN_REAUTH_TOKENS.items():
        expires_at = _parse_dt(record.get("expires_at"))
        if expires_at is None or expires_at <= now:
            expired.append(token)
    for token in expired:
        _ADMIN_REAUTH_TOKENS.pop(token, None)


def _extract_admin_reauth_token(request: Request) -> str:
    header_token = str(request.headers.get("x-admin-reauth") or "").strip()
    if header_token:
        return header_token
    return str(request.query_params.get("reauth_token") or "").strip()


def _require_recent_admin_reauth(request: Request) -> Response | None:
    if not _is_admin_or_local(request):
        return _forbidden_response(request, str(getattr(request.state, "lan_auth_app_title", "当前应用") or "当前应用"))
    token = _extract_admin_reauth_token(request)
    if not validate_admin_reauth_token(request, token):
        return JSONResponse({"detail": "admin_reauth_required"}, status_code=401)
    return None


def _parse_allowed_apps(value: Any) -> list[str]:
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            parsed = []
    elif isinstance(value, list):
        parsed = value
    else:
        parsed = []
    return [str(item or "").strip() for item in parsed if str(item or "").strip()]


def _normalize_allowed_apps(values: list[str]) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for value in values:
        app_id = str(value or "").strip()
        if not app_id or app_id not in AVAILABLE_APPS or app_id in seen:
            continue
        seen.add(app_id)
        output.append(app_id)
    return output


def _normalize_role(value: str) -> str:
    return "admin" if str(value or "").strip().lower() == "admin" else "user"


def _normalize_username(value: str) -> str:
    username = str(value or "").strip().lower()
    if not username:
        raise ValueError("用户名不能为空。")
    if len(username) > 64:
        raise ValueError("用户名过长。")
    for char in username:
        if char.isalnum() or char in {"_", "-", "."}:
            continue
        raise ValueError("用户名只能包含字母、数字、点、下划线或短横线。")
    return username


def _validate_password(password: str) -> None:
    if len(str(password or "")) < PASSWORD_MIN_LENGTH:
        raise ValueError(f"密码至少需要 {PASSWORD_MIN_LENGTH} 位。")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _dt_to_text(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def _utcnow_text() -> str:
    return _dt_to_text(_utcnow())


def _parse_dt(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _is_public_path(path: str) -> bool:
    cleaned = str(path or "").strip() or "/"
    return cleaned in PUBLIC_PATHS


def _is_loopback_request(request: Request) -> bool:
    host = str(getattr(request.client, "host", "") or "").strip()
    if not host:
        return False
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return host in {"localhost"}


def _wants_html(request: Request) -> bool:
    if str(request.url.path or "").startswith("/api/"):
        return False
    accept = str(request.headers.get("accept") or "").lower()
    return "text/html" in accept or accept in {"", "*/*"}


def _sanitize_next_target(value: str | None) -> str:
    target = str(value or "").strip() or "/"
    if not target.startswith("/"):
        return "/"
    if target.startswith("//"):
        return "/"
    if target.startswith("/_auth/"):
        return "/"
    return target


def _current_request_target(request: Request) -> str:
    query = str(request.url.query or "").strip()
    return f"{request.url.path}?{query}" if query else request.url.path


def _unauthorized_response(request: Request, *, clear_cookie: bool) -> Response:
    login_url = f"/_auth/login?next={urllib.parse.quote(_current_request_target(request), safe='')}"
    if _wants_html(request):
        response: Response = RedirectResponse(login_url, status_code=303)
    else:
        response = JSONResponse({"detail": "authentication_required", "login_url": login_url}, status_code=401)
    if clear_cookie:
        response.delete_cookie(COOKIE_NAME, path="/")
    return response


def _forbidden_response(request: Request, app_title: str) -> Response:
    if _wants_html(request):
        return HTMLResponse(
            _render_status_page(
                title="访问被拒绝",
                heading="当前账号无权访问此应用",
                detail=f"你已经登录，但未被授予 {html.escape(app_title)} 的访问权限。",
                actions='<a class="primary-link" href="/_auth/logout?next=/">切换账号</a>',
            ),
            status_code=403,
        )
    return JSONResponse({"detail": "forbidden"}, status_code=403)


def _set_session_cookie(response: Response, session_id: str, request: Request) -> None:
    response.set_cookie(
        COOKIE_NAME,
        session_id,
        max_age=SESSION_TTL_DAYS * 24 * 60 * 60,
        httponly=True,
        samesite="lax",
        secure=str(request.url.scheme or "").lower() == "https",
        path="/",
    )


def _is_admin_or_local(request: Request) -> bool:
    if _is_loopback_request(request):
        return True
    auth = get_request_auth(request)
    return auth is not None and auth.role == "admin"


async def _parse_form_body(request: Request) -> dict[str, list[str]]:
    body = (await request.body()).decode("utf-8", errors="replace")
    return urllib.parse.parse_qs(body, keep_blank_values=True)


def _form_value(form: dict[str, list[str]], key: str) -> str:
    return str((form.get(key) or [""])[0] or "").strip()


def _form_list(form: dict[str, list[str]], key: str) -> list[str]:
    return [str(item or "").strip() for item in (form.get(key) or []) if str(item or "").strip()]


def _form_checkbox(form: dict[str, list[str]], key: str) -> bool:
    return any(str(item or "").strip().lower() in {"1", "true", "on", "yes"} for item in (form.get(key) or []))


def _admin_redirect(*, message: str = "", error: str = "") -> RedirectResponse:
    params: dict[str, str] = {}
    if message:
        params["msg"] = message
    if error:
        params["error"] = error
    query = urllib.parse.urlencode(params)
    return RedirectResponse(f"/_auth/admin{'?' + query if query else ''}", status_code=303)


def _render_login_page(*, app_title: str, next_target: str, error_message: str, users_exist: bool) -> str:
    hint_html = (
        "<p class=\"helper\">当前还没有初始化管理员账号。请先在本机打开 <code>/_auth/admin</code> 创建首个管理员。</p>"
        if not users_exist
        else "<p class=\"helper\">局域网远端访问需要登录；本机 127.0.0.1 / ::1 直连不需要登录。</p>"
    )
    error_html = f'<div class="alert error">{html.escape(error_message)}</div>' if error_message else ""
    return _render_status_page(
        title=f"登录 {app_title}",
        heading=f"登录 {app_title}",
        detail=(
            f"{error_html}"
            f"{hint_html}"
            f"<form method=\"post\" action=\"/_auth/login\" class=\"auth-form\">"
            f"<input type=\"hidden\" name=\"next\" value=\"{html.escape(next_target)}\" />"
            f"<label>用户名<input type=\"text\" name=\"username\" autocomplete=\"username\" required /></label>"
            f"<label>密码<input type=\"password\" name=\"password\" autocomplete=\"current-password\" required /></label>"
            f"<button type=\"submit\">登录</button>"
            f"</form>"
        ),
    )


def _render_admin_page(*, app_title: str, users: list[dict[str, Any]], users_exist: bool, flash_message: str, error_message: str, is_local: bool) -> str:
    notices = []
    if flash_message:
        notices.append(f'<div class="alert ok">{html.escape(flash_message)}</div>')
    if error_message:
        notices.append(f'<div class="alert error">{html.escape(error_message)}</div>')
    header = (
        "<p class=\"helper\">当前在本机访问，已按 localhost 直连豁免放行；这里只管理远端访问账号。</p>"
        if is_local
        else f"<p class=\"helper\">当前已作为管理员访问 {html.escape(app_title)}。</p>"
    )
    if not users_exist:
        body = (
            "".join(notices)
            + header
            + "<section class=\"card\"><h2>初始化管理员</h2>"
            + "<form method=\"post\" action=\"/_auth/admin/bootstrap\" class=\"auth-form\">"
            + "<label>用户名<input type=\"text\" name=\"username\" value=\"admin\" required /></label>"
            + "<label>密码<input type=\"password\" name=\"password\" required /></label>"
            + "<button type=\"submit\">创建管理员</button>"
            + "</form></section>"
        )
        return _render_status_page(title="LAN Auth 管理", heading="LAN Auth 管理", detail=body)

    user_cards = []
    for user in users:
        app_checks = "".join(
            f'<label class="check"><input type="checkbox" name="allowed_apps" value="{html.escape(app_id)}" {"checked" if app_id in set(user.get("allowed_apps") or []) else ""} {"disabled" if user.get("role") == "admin" else ""} />{html.escape(app_label)}</label>'
            for app_id, app_label in AVAILABLE_APPS.items()
        )
        user_cards.append(
            "<section class=\"card\">"
            f"<h3>{html.escape(str(user.get('username') or ''))}</h3>"
            f"<div class=\"meta\">role={html.escape(str(user.get('role') or 'user'))} · auth_version={int(user.get('auth_version') or 1)} · active_sessions={int(user.get('active_session_count') or 0)}</div>"
            f"<form method=\"post\" action=\"/_auth/admin/users/{html.escape(str(user.get('user_id') or ''))}\" class=\"admin-form\">"
            "<label>角色<select name=\"role\">"
            f"<option value=\"user\" {'selected' if user.get('role') != 'admin' else ''}>user</option>"
            f"<option value=\"admin\" {'selected' if user.get('role') == 'admin' else ''}>admin</option>"
            "</select></label>"
            f"<label class=\"check inline\"><input type=\"checkbox\" name=\"is_active\" value=\"1\" {'checked' if user.get('is_active') else ''} />启用账号</label>"
            "<label>新密码（留空不改）<input type=\"password\" name=\"new_password\" /></label>"
            f"<div class=\"checks\">{app_checks}</div>"
            "<button type=\"submit\">保存</button>"
            "</form></section>"
        )

    create_checks = "".join(
        f'<label class="check"><input type="checkbox" name="allowed_apps" value="{html.escape(app_id)}" />{html.escape(app_label)}</label>'
        for app_id, app_label in AVAILABLE_APPS.items()
    )
    body = (
        "".join(notices)
        + header
        + "<section class=\"card\"><h2>新增账号</h2>"
        + "<form method=\"post\" action=\"/_auth/admin/users/create\" class=\"admin-form\">"
        + "<label>用户名<input type=\"text\" name=\"username\" required /></label>"
        + "<label>密码<input type=\"password\" name=\"password\" required /></label>"
        + "<label>角色<select name=\"role\"><option value=\"user\">user</option><option value=\"admin\">admin</option></select></label>"
        + "<label class=\"check inline\"><input type=\"checkbox\" name=\"is_active\" value=\"1\" checked />启用账号</label>"
        + f"<div class=\"checks\">{create_checks}</div>"
        + "<button type=\"submit\">创建账号</button>"
        + "</form></section>"
        + "<section class=\"card\"><h2>现有账号</h2>"
        + "".join(user_cards)
        + "</section>"
    )
    return _render_status_page(title="LAN Auth 管理", heading="LAN Auth 管理", detail=body)


def _render_status_page(*, title: str, heading: str, detail: str, actions: str = "") -> str:
    return (
        "<!doctype html><html lang=\"zh-CN\"><head><meta charset=\"utf-8\" />"
        "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />"
        f"<title>{html.escape(title)}</title>"
        "<style>"
        "body{margin:0;font-family:Segoe UI,Arial,sans-serif;background:#171915;color:#eef1e6;}"
        ".shell{max-width:980px;margin:0 auto;padding:32px 18px 48px;}"
        ".hero,.card{background:#22241e;border:1px solid #3f4238;border-radius:14px;padding:18px 20px;margin-bottom:16px;}"
        "h1,h2,h3{margin:0 0 12px;}"
        ".helper,.meta{color:#aab19a;line-height:1.6;}"
        ".meta{font-size:12px;margin-bottom:10px;}"
        ".auth-form,.admin-form{display:grid;gap:12px;}"
        "label{display:grid;gap:6px;font-size:13px;color:#d7dcca;}"
        "input,select{background:#11130f;color:#eef1e6;border:1px solid #41453a;border-radius:10px;padding:10px 12px;font-size:14px;}"
        "button,.primary-link{display:inline-flex;align-items:center;justify-content:center;background:#76d8b2;color:#09120d;border:none;border-radius:10px;padding:10px 16px;font-weight:600;text-decoration:none;cursor:pointer;}"
        ".alert{border-radius:10px;padding:12px 14px;margin-bottom:14px;font-size:14px;}"
        ".alert.ok{background:#1c3128;color:#bdebd3;border:1px solid #2d5b48;}"
        ".alert.error{background:#341d1d;color:#f3b3b3;border:1px solid #6b3737;}"
        ".checks{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:8px 12px;}"
        ".check{display:flex;align-items:center;gap:8px;background:#191b16;border:1px solid #33362c;border-radius:10px;padding:10px 12px;}"
        ".check.inline{display:inline-flex;background:transparent;border:none;padding:0;color:#d7dcca;}"
        ".check input{inline-size:16px;block-size:16px;margin:0;}"
        "code{background:#10120f;border:1px solid #33362c;border-radius:6px;padding:2px 6px;}"
        "</style></head><body><main class=\"shell\">"
        f"<section class=\"hero\"><h1>{html.escape(heading)}</h1>{detail}{actions}</section>"
        "</main></body></html>"
    )