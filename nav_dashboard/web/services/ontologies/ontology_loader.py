"""Shared ontology loader with strict schema validation and mtime-based hot-reload.

Replaces the per-module ``lru_cache(maxsize=1)`` pattern with a process-level
mtime cache so that changes accepted via ``ontology_review.py`` take effect
immediately without restarting the web service.

Usage::

    from nav_dashboard.web.services.ontologies.ontology_loader import load_ontology

    data = load_ontology("music")     # returns the approved dict, reloaded if file changed
    data = load_ontology("video")
    data = load_ontology("book")

Schema contract:
  Every approved ontology file MUST contain at the top level:
    - ``"version"``: str
    - ``"domain"``: str  (must match the requested domain)
  All top-level keys other than ``"version"`` and ``"domain"`` are section names
  and must be a subset of ``ALLOWED_SECTIONS_BY_DOMAIN[domain]``.
"""

from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any

# ── Schema constants ─────────────────────────────────────────────────────────

APPROVED_DIR = Path(__file__).parent / "approved"

ALLOWED_SECTIONS_BY_DOMAIN: dict[str, frozenset[str]] = {
    "music": frozenset({
        "instruments",
        "forms",
        "work_families",
        "genres",
        "composer_aliases",
        "composer_work_signature_overrides",
    }),
    "video": frozenset({
        "genres",
        "creator_aliases",
        "entity_aliases",
        "version_terms",
    }),
    "book": frozenset({
        "genres",
        "creator_aliases",
        "entity_aliases",
        "version_terms",
    }),
    "game": frozenset({
        "genres",
        "creator_aliases",
        "entity_aliases",
        "version_terms",
    }),
}

# Metadata keys that are allowed alongside sections in the top-level dict.
_META_KEYS: frozenset[str] = frozenset({"version", "domain"})

# ── Process-level cache ───────────────────────────────────────────────────────

# domain -> (mtime_ns, data)
_cache: dict[str, tuple[int, dict[str, Any]]] = {}
_LOCK = threading.Lock()

# domain -> {"loaded": bool, "fallback_empty": bool, "error": str}
# Updated by load_ontology() on every attempt (hit or miss).
_load_status: dict[str, dict[str, Any]] = {}

# ── Internal helpers ──────────────────────────────────────────────────────────

def _ontology_path(domain: str) -> Path:
    return APPROVED_DIR / f"{domain}.json"


def _validate(domain: str, payload: dict[str, Any]) -> None:
    """Raise ValueError if *payload* does not conform to the domain schema."""
    if not isinstance(payload, dict):
        raise ValueError(f"ontology/{domain}: root must be a JSON object, got {type(payload).__name__}")

    if "version" not in payload:
        raise ValueError(f"ontology/{domain}: missing required key 'version'")

    file_domain = str(payload.get("domain") or "").strip()
    if file_domain and file_domain != domain:
        raise ValueError(
            f"ontology/{domain}: 'domain' field '{file_domain}' does not match requested domain '{domain}'"
        )

    allowed = ALLOWED_SECTIONS_BY_DOMAIN.get(domain)
    if allowed is None:
        raise ValueError(f"ontology/{domain}: unknown domain '{domain}'")

    unknown = {key for key in payload if key not in _META_KEYS and key not in allowed}
    if unknown:
        raise ValueError(
            f"ontology/{domain}: unknown sections {sorted(unknown)} "
            f"(allowed: {sorted(allowed)})"
        )

    # ── Section-level shape validation ────────────────────────────────────────
    # Each section must be a dict.  Within each section every entry is validated
    # against the expected value shape for that section.
    _validate_sections(domain, payload)


def _validate_sections(domain: str, payload: dict[str, Any]) -> None:
    """Validate the shape of entries within each known section."""
    for section_name, section_content in payload.items():
        if section_name in _META_KEYS:
            continue
        if not isinstance(section_content, dict):
            raise ValueError(
                f"ontology/{domain}: section '{section_name}' must be a JSON object, "
                f"got {type(section_content).__name__}"
            )
        for key, value in section_content.items():
            _validate_section_entry(domain, section_name, key, value, payload)


def _validate_section_entry(
    domain: str,
    section: str,
    key: str,
    value: Any,
    full_payload: dict[str, Any],
) -> None:
    """Raise ``ValueError`` if a single section entry has the wrong shape."""
    loc = f"ontology/{domain}/{section}/{key!r}"

    if section == "composer_aliases" or (domain != "music" and section == "creator_aliases"):
        # Value must be a list of alias strings.
        if not isinstance(value, list):
            raise ValueError(f"{loc}: value must be a list of alias strings, got {type(value).__name__}")
        for i, alias in enumerate(value):
            if not isinstance(alias, str):
                raise ValueError(f"{loc}[{i}]: each alias must be a string, got {type(alias).__name__}")
        return

    if section in {"instruments", "forms", "genres", "version_terms", "entity_aliases"}:
        # Value must be a dict with an 'aliases' key that is a list of strings.
        if not isinstance(value, dict):
            raise ValueError(f"{loc}: value must be a JSON object with 'aliases', got {type(value).__name__}")
        aliases = value.get("aliases")
        if aliases is not None and not isinstance(aliases, list):
            raise ValueError(f"{loc}.aliases: must be a list, got {type(aliases).__name__}")
        if isinstance(aliases, list):
            for i, alias in enumerate(aliases):
                if not isinstance(alias, str):
                    raise ValueError(f"{loc}.aliases[{i}]: must be a string, got {type(alias).__name__}")
        # entity_aliases may have an optional 'type' string.
        if section == "entity_aliases":
            etype = value.get("type")
            if etype is not None and not isinstance(etype, str):
                raise ValueError(f"{loc}.type: must be a string, got {type(etype).__name__}")
        return

    if section == "work_families":
        # Value must be a dict with optional 'aliases', 'instrument', 'form'.
        if not isinstance(value, dict):
            raise ValueError(f"{loc}: value must be a JSON object, got {type(value).__name__}")
        aliases = value.get("aliases")
        if aliases is not None and not isinstance(aliases, list):
            raise ValueError(f"{loc}.aliases: must be a list, got {type(aliases).__name__}")
        if isinstance(aliases, list):
            for i, alias in enumerate(aliases):
                if not isinstance(alias, str):
                    raise ValueError(f"{loc}.aliases[{i}]: must be a string, got {type(alias).__name__}")
        instrument = value.get("instrument")
        if instrument is not None:
            if not isinstance(instrument, str):
                raise ValueError(f"{loc}.instrument: must be a string, got {type(instrument).__name__}")
            instruments_section = full_payload.get("instruments")
            if isinstance(instruments_section, dict) and instrument and instrument not in instruments_section:
                raise ValueError(f"{loc}.instrument: '{instrument}' not found in instruments section")
        form = value.get("form")
        if form is not None:
            if not isinstance(form, str):
                raise ValueError(f"{loc}.form: must be a string, got {type(form).__name__}")
            forms_section = full_payload.get("forms")
            if isinstance(forms_section, dict) and form and form not in forms_section:
                raise ValueError(f"{loc}.form: '{form}' not found in forms section")
        return

    if section == "composer_work_signature_overrides":
        # Value is a dict of {family_key: [sig_token, ...]}
        if not isinstance(value, dict):
            raise ValueError(f"{loc}: value must be a JSON object, got {type(value).__name__}")
        for family_key, sig_list in value.items():
            if not isinstance(sig_list, list):
                raise ValueError(
                    f"{loc}/{family_key!r}: override must be a list of signature tokens, "
                    f"got {type(sig_list).__name__}"
                )
            for i, token in enumerate(sig_list):
                if not isinstance(token, str):
                    raise ValueError(f"{loc}/{family_key!r}[{i}]: signature token must be a string")


# ── Public API ────────────────────────────────────────────────────────────────

def load_ontology(domain: str) -> dict[str, Any]:
    """Return the approved ontology for *domain*, reloading if the file changed.

    Raises ``ValueError`` for schema violations and ``FileNotFoundError`` if
    the approved file does not exist.  Returns a copy so callers can't mutate
    the cache.
    """
    path = _ontology_path(domain)

    with _LOCK:
        try:
            mtime_ns = path.stat().st_mtime_ns
        except FileNotFoundError:
            _load_status[domain] = {
                "loaded": False,
                "fallback_empty": True,
                "error": f"approved file not found: {path}",
            }
            raise FileNotFoundError(
                f"Approved ontology file not found: {path}. "
                "Run ontology_review.py to create it."
            )

        cached = _cache.get(domain)
        if cached is not None and cached[0] == mtime_ns:
            _load_status[domain] = {"loaded": True, "fallback_empty": False, "error": ""}
            return dict(cached[1])

        raw = path.read_text(encoding="utf-8")

    # Parse + validate outside the lock so slow I/O doesn't block other domains.
    try:
        payload: dict[str, Any] = json.loads(raw)
    except json.JSONDecodeError as exc:
        _load_status[domain] = {
            "loaded": False,
            "fallback_empty": True,
            "error": f"invalid JSON: {exc}",
        }
        raise ValueError(f"ontology/{domain}: invalid JSON — {exc}") from exc

    try:
        _validate(domain, payload)
    except ValueError as exc:
        _load_status[domain] = {
            "loaded": False,
            "fallback_empty": True,
            "error": f"schema validation failed: {exc}",
        }
        raise

    with _LOCK:
        # Re-check mtime in case another thread reloaded while we were parsing.
        try:
            current_mtime = path.stat().st_mtime_ns
        except FileNotFoundError:
            current_mtime = mtime_ns
        _cache[domain] = (current_mtime, payload)
        _load_status[domain] = {"loaded": True, "fallback_empty": False, "error": ""}

    return dict(payload)


def validate_ontology_payload(domain: str, payload: dict[str, Any]) -> None:
    """Public wrapper around ``_validate``.

    Raises ``ValueError`` when *payload* violates the schema for *domain*.
    Used by external tools (e.g. ``scripts/data_maintenance/ontology_review.py``) to share the
    same validation logic as the runtime loader.
    """
    _validate(domain, payload)


def invalidate_cache(domain: str | None = None) -> None:
    """Evict *domain* from the process cache (or all domains if None)."""
    with _LOCK:
        if domain is None:
            _cache.clear()
        else:
            _cache.pop(domain, None)


def get_load_statuses() -> dict[str, dict[str, Any]]:
    """Return a snapshot of the most recent load outcome for each domain.

    Each value is ``{"loaded": bool, "fallback_empty": bool, "error": str}``.
    Domains that have never been attempted are absent from the dict.
    """
    with _LOCK:
        return {domain: dict(status) for domain, status in _load_status.items()}
