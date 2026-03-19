"""Book domain ontology wrapper — parallel to music_ontology.py and video_ontology.py.

Exposes helpers for genre/creator/entity hint extraction from query text,
backed by ``ontologies/approved/book.json`` via the shared ``ontology_loader``.

Functions consumed by the agent pipeline
-----------------------------------------
* ``load_book_ontology()``          — raw dict (mtime-reloaded)
* ``collect_book_ontology_hints()`` — structured signal dict for query enrichment
* ``infer_book_filters_from_text()``— filter dict compatible with _infer_media_filters
* ``is_genre_alias(token)``         — membership test (post_retrieval_policy)
* ``is_entity_alias(token)``        — membership test (post_retrieval_policy)
"""

from __future__ import annotations

import logging
import re
from typing import Any

from nav_dashboard.web.services.ontologies.ontology_loader import load_ontology as _load_ontology

# ── Shared token helpers ──────────────────────────────────────────────────────

def _normalize_token(text: str) -> str:
    return "".join(ch.lower() for ch in str(text or "") if ch.isalnum() or "\u4e00" <= ch <= "\u9fff")


def _alias_hit(raw_text: str, normalized_text: str, alias: str) -> bool:
    raw_alias = str(alias or "").strip().lower()
    if not raw_alias:
        return False
    if raw_alias in raw_text:
        return True
    normalized_alias = _normalize_token(raw_alias)
    if normalized_alias and normalized_alias in normalized_text:
        return True
    return False


def _extract_aliases(payload: Any) -> list[str]:
    if isinstance(payload, dict):
        aliases = payload.get("aliases")
        if isinstance(aliases, list):
            return [str(item).strip() for item in aliases if str(item).strip()]
        return []
    if isinstance(payload, list):
        return [str(item).strip() for item in payload if str(item).strip()]
    return []


_logger = logging.getLogger(__name__)

# ── Public API ────────────────────────────────────────────────────────────────

def load_book_ontology() -> dict[str, Any]:
    """Return the approved book ontology, reloading automatically if the file changed."""
    try:
        return _load_ontology("book")
    except Exception as exc:
        _logger.warning(
            "ontology/book: failed to load (%s) — hint/filter functions will return empty results",
            exc,
        )
        return {
            "genres": {},
            "creator_aliases": {},
            "entity_aliases": {},
            "version_terms": {},
        }


def collect_book_ontology_hints(text: str) -> dict[str, Any]:
    """Scan *text* for book-domain ontology matches and return structured hints.

    Returns a dict with keys:
        genre_keys, creator_keys, entity_keys         — matched canonical keys
        genre_hints, creator_hints, entity_hints      — flat alias lists
    """
    ontology = load_book_ontology()
    raw = str(text or "").lower()
    normalized = _normalize_token(text)

    genre_keys: set[str] = set()
    creator_keys: set[str] = set()
    entity_keys: set[str] = set()

    for key, payload in dict(ontology.get("genres") or {}).items():
        aliases = [str(key).strip(), *_extract_aliases(payload)]
        if any(_alias_hit(raw, normalized, alias) for alias in aliases):
            genre_keys.add(str(key).strip())

    for canonical, alias_list in dict(ontology.get("creator_aliases") or {}).items():
        aliases = [str(canonical).strip()]
        if isinstance(alias_list, list):
            aliases.extend(str(item).strip() for item in alias_list if str(item).strip())
        if any(_alias_hit(raw, normalized, alias) for alias in aliases):
            creator_keys.add(str(canonical).strip())

    for key, payload in dict(ontology.get("entity_aliases") or {}).items():
        aliases = [str(key).strip(), *_extract_aliases(payload)]
        if any(_alias_hit(raw, normalized, alias) for alias in aliases):
            entity_keys.add(str(key).strip())

    def _flatten(section_name: str, keys: set[str]) -> list[str]:
        out: list[str] = []
        section = dict(ontology.get(section_name) or {})
        for key in keys:
            aliases = [str(key).strip(), *_extract_aliases(section.get(key))]
            for alias in aliases:
                if alias and alias not in out:
                    out.append(alias)
        return out

    def _flatten_creator_aliases(keys: set[str]) -> list[str]:
        out: list[str] = []
        creator_map = dict(ontology.get("creator_aliases") or {})
        for canonical in keys:
            aliases = [str(canonical).strip()]
            raw_aliases = creator_map.get(canonical)
            if isinstance(raw_aliases, list):
                aliases.extend(str(item).strip() for item in raw_aliases if str(item).strip())
            for alias in aliases:
                if alias and alias not in out:
                    out.append(alias)
        return out

    return {
        "genre_keys": sorted(genre_keys),
        "creator_keys": sorted(creator_keys),
        "entity_keys": sorted(entity_keys),
        "genre_hints": _flatten("genres", genre_keys),
        "creator_hints": _flatten_creator_aliases(creator_keys),
        "entity_hints": _flatten("entity_aliases", entity_keys),
    }


def infer_book_filters_from_text(text: str) -> dict[str, list[str]]:
    """Return a filter dict (media_type, category) inferred from book ontology matches.

    The returned dict is compatible with the ``_merge_filter_values`` / ``_infer_media_filters``
    pipeline in ``agent_service.py``.
    """
    hints = collect_book_ontology_hints(text)
    genre_keys = hints.get("genre_keys") or []

    if not genre_keys:
        return {}

    ontology = load_book_ontology()
    genre_section = dict(ontology.get("genres") or {})
    categories: list[str] = []
    for key in genre_keys:
        payload = genre_section.get(key)
        if isinstance(payload, dict):
            label = str(payload.get("label") or key).strip()
        else:
            label = str(key).strip()
        if label and label not in categories:
            categories.append(label)

    filters: dict[str, list[str]] = {}
    if categories:
        filters["media_type"] = ["book"]
        filters["category"] = categories
    return filters


def is_genre_alias(token: str) -> bool:
    """Return True if *token* is a recognised genre alias in the book ontology."""
    ontology = load_book_ontology()
    normalized = _normalize_token(token)
    if not normalized:
        return False
    for key, payload in dict(ontology.get("genres") or {}).items():
        aliases = [str(key).strip(), *_extract_aliases(payload)]
        for alias in aliases:
            if _normalize_token(alias) == normalized:
                return True
    return False


def is_entity_alias(token: str) -> bool:
    """Return True if *token* is a recognised entity/series alias in the book ontology."""
    ontology = load_book_ontology()
    normalized = _normalize_token(token)
    if not normalized:
        return False
    for key, payload in dict(ontology.get("entity_aliases") or {}).items():
        aliases = [str(key).strip(), *_extract_aliases(payload)]
        for alias in aliases:
            if _normalize_token(alias) == normalized:
                return True
    return False
