"""Shared SQLite-backed cache utilities for RAG QA and LLM Agent pipelines.

Provides:
  EmbedCache           — Permanent per-text embedding cache (SHA-256 key, float32 blob)
  WebSearchCache       — 7-day TTL web search results cache
  get_embed_cache()    — Module-level singleton for EmbedCache
  get_web_cache()      — Module-level singleton for WebSearchCache
  log_no_context_query()  — Append to shared no_context_queries.jsonl

Data root: <scripts>/../data/cache/  (= ai_conversations_summary/data/cache/)
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import struct
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any

_SCRIPTS_DIR = Path(__file__).resolve().parent
CACHE_DIR = _SCRIPTS_DIR.parent / "data" / "cache"

EMBED_CACHE_PATH = CACHE_DIR / "embed_cache.db"
WEB_CACHE_PATH = CACHE_DIR / "web_cache.db"
NO_CONTEXT_LOG_PATH = CACHE_DIR / "no_context_queries.jsonl"

WEB_CACHE_TTL_DAYS = 7

_NO_CONTEXT_LOCK = threading.Lock()


def _ensure_cache_dir() -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)


# ─── Embedding cache ──────────────────────────────────────────────────────────

class EmbedCache:
    """Permanent per-text embedding cache backed by SQLite.

    Key:   SHA-256(model::text)
    Value: struct-packed float32 blob (compact; ~768 floats × 4 bytes = 3 KB per entry)
    Thread-safe via WAL-mode SQLite (one connection per operation).
    """

    def __init__(self, db_path: Path = EMBED_CACHE_PATH) -> None:
        self.db_path = db_path
        _ensure_cache_dir()
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=10, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """CREATE TABLE IF NOT EXISTS embed_cache (
                    cache_key  TEXT PRIMARY KEY,
                    model      TEXT NOT NULL,
                    vector     BLOB NOT NULL,
                    dim        INTEGER NOT NULL,
                    created_at INTEGER NOT NULL
                )"""
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ec_model ON embed_cache(model)")

    @staticmethod
    def _key(text: str, model: str) -> str:
        return hashlib.sha256(f"{model}::{text}".encode("utf-8")).hexdigest()

    def get(self, text: str, model: str) -> list[float] | None:
        key = self._key(text, model)
        try:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT vector, dim FROM embed_cache WHERE cache_key = ?", (key,)
                ).fetchone()
        except Exception:
            return None
        if row is None:
            return None
        blob, dim = row[0], int(row[1])
        try:
            return list(struct.unpack(f"{dim}f", blob))
        except Exception:
            return None

    def get_batch(self, texts: list[str], model: str) -> list[list[float] | None]:
        """Return cached vector for each text; None where not cached."""
        if not texts:
            return []
        keys = [self._key(t, model) for t in texts]
        placeholder = ",".join("?" for _ in keys)
        try:
            with self._connect() as conn:
                rows = conn.execute(
                    f"SELECT cache_key, vector, dim FROM embed_cache WHERE cache_key IN ({placeholder})",
                    keys,
                ).fetchall()
        except Exception:
            return [None] * len(texts)
        row_map: dict[str, list[float]] = {}
        for cache_key, blob, dim in rows:
            try:
                row_map[cache_key] = list(struct.unpack(f"{int(dim)}f", blob))
            except Exception:
                pass
        return [row_map.get(k) for k in keys]

    def set(self, text: str, model: str, vector: list[float]) -> None:
        key = self._key(text, model)
        dim = len(vector)
        blob = struct.pack(f"{dim}f", *vector)
        try:
            with self._connect() as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO embed_cache "
                    "(cache_key, model, vector, dim, created_at) VALUES (?, ?, ?, ?, ?)",
                    (key, model, blob, dim, int(time.time())),
                )
        except Exception:
            pass

    def set_batch(self, texts: list[str], model: str, vectors: list[list[float]]) -> None:
        """Store multiple embeddings in a single transaction."""
        if not texts or not vectors:
            return
        rows_data = []
        for text, vec in zip(texts, vectors):
            dim = len(vec)
            blob = struct.pack(f"{dim}f", *vec)
            rows_data.append((self._key(text, model), model, blob, dim, int(time.time())))
        try:
            with self._connect() as conn:
                conn.executemany(
                    "INSERT OR REPLACE INTO embed_cache "
                    "(cache_key, model, vector, dim, created_at) VALUES (?, ?, ?, ?, ?)",
                    rows_data,
                )
        except Exception:
            pass

    def count(self) -> int:
        try:
            with self._connect() as conn:
                row = conn.execute("SELECT COUNT(*) FROM embed_cache").fetchone()
                return int(row[0]) if row else 0
        except Exception:
            return 0


_EMBED_CACHE: EmbedCache | None = None
_EMBED_CACHE_LOCK = threading.Lock()


def get_embed_cache() -> EmbedCache:
    global _EMBED_CACHE
    if _EMBED_CACHE is None:
        with _EMBED_CACHE_LOCK:
            if _EMBED_CACHE is None:
                _EMBED_CACHE = EmbedCache()
    return _EMBED_CACHE


# ─── Web search cache ─────────────────────────────────────────────────────────

class WebSearchCache:
    """Web search results cache with configurable TTL (default 7 days).

    Key:   SHA-256(max_results::query)
    Value: JSON-serialised list[dict] of result rows
    Entries older than TTL are ignored on read; prune_expired() deletes them.
    """

    def __init__(self, db_path: Path = WEB_CACHE_PATH, ttl_days: int = WEB_CACHE_TTL_DAYS) -> None:
        self.db_path = db_path
        self.ttl_seconds = ttl_days * 86_400
        _ensure_cache_dir()
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=10, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """CREATE TABLE IF NOT EXISTS web_cache (
                    cache_key    TEXT PRIMARY KEY,
                    query        TEXT NOT NULL,
                    results_json TEXT NOT NULL,
                    created_at   INTEGER NOT NULL
                )"""
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_wc_created ON web_cache(created_at)")

    @staticmethod
    def _key(query: str, max_results: int) -> str:
        return hashlib.sha256(f"{max_results}::{query}".encode("utf-8")).hexdigest()

    def get(self, query: str, max_results: int) -> list[dict[str, Any]] | None:
        key = self._key(query, max_results)
        cutoff = int(time.time()) - self.ttl_seconds
        try:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT results_json FROM web_cache WHERE cache_key = ? AND created_at > ?",
                    (key, cutoff),
                ).fetchone()
        except Exception:
            return None
        if row is None:
            return None
        try:
            return json.loads(row[0])
        except Exception:
            return None

    def set(self, query: str, max_results: int, results: list[dict[str, Any]]) -> None:
        key = self._key(query, max_results)
        try:
            with self._connect() as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO web_cache "
                    "(cache_key, query, results_json, created_at) VALUES (?, ?, ?, ?)",
                    (key, query, json.dumps(results, ensure_ascii=False), int(time.time())),
                )
        except Exception:
            pass

    def prune_expired(self) -> int:
        """Delete entries older than TTL. Returns number of rows deleted."""
        cutoff = int(time.time()) - self.ttl_seconds
        try:
            with self._connect() as conn:
                cur = conn.execute("DELETE FROM web_cache WHERE created_at <= ?", (cutoff,))
                return cur.rowcount
        except Exception:
            return 0

    def count_stats(self) -> dict[str, int]:
        """Return {"total": N, "valid": M} where valid = non-expired entries."""
        cutoff = int(time.time()) - self.ttl_seconds
        try:
            with self._connect() as conn:
                total = conn.execute("SELECT COUNT(*) FROM web_cache").fetchone()
                valid = conn.execute(
                    "SELECT COUNT(*) FROM web_cache WHERE created_at > ?", (cutoff,)
                ).fetchone()
            return {
                "total": int(total[0]) if total else 0,
                "valid": int(valid[0]) if valid else 0,
            }
        except Exception:
            return {"total": 0, "valid": 0}


_WEB_CACHE: WebSearchCache | None = None
_WEB_CACHE_LOCK = threading.Lock()


def get_web_cache() -> WebSearchCache:
    global _WEB_CACHE
    if _WEB_CACHE is None:
        with _WEB_CACHE_LOCK:
            if _WEB_CACHE is None:
                _WEB_CACHE = WebSearchCache()
    return _WEB_CACHE


# ─── No-context query logging ─────────────────────────────────────────────────

def log_no_context_query(
    query: str,
    *,
    source: str,
    top1_score: float | None = None,
    threshold: float | None = None,
    trace_id: str = "",
    reason: str = "",
) -> None:
    """Append a no-context record to no_context_queries.jsonl.

    Called when the local doc retrieval top-1 score falls below the similarity
    threshold, signalling the knowledge base does not cover this topic.
    Write failures are silently ignored so the main pipeline is never blocked.
    Thread-safe via file-level lock.
    """
    record: dict[str, Any] = {
        "ts": datetime.now().isoformat(timespec="seconds"),
        "source": str(source or "unknown"),
        "query": str(query or ""),
        "top1_score": round(float(top1_score), 4) if top1_score is not None else None,
        "threshold": round(float(threshold), 4) if threshold is not None else None,
    }
    normalized_trace_id = str(trace_id or "").strip()
    if normalized_trace_id:
        record["trace_id"] = normalized_trace_id
    normalized_reason = str(reason or "").strip()
    if normalized_reason:
        record["reason"] = normalized_reason
    try:
        _ensure_cache_dir()
        with _NO_CONTEXT_LOCK:
            with NO_CONTEXT_LOG_PATH.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception:
        pass
