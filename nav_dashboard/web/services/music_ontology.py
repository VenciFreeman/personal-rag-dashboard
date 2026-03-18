from __future__ import annotations

from difflib import SequenceMatcher
import json
from functools import lru_cache
from pathlib import Path
import re
from typing import Any

_ONTOLOGY_PATH = Path(__file__).with_name("music_ontology.json")


def _normalize_token(text: str) -> str:
    return "".join(ch.lower() for ch in str(text or "") if ch.isalnum() or "\u4e00" <= ch <= "\u9fff")


def _contains_cjk(text: str) -> bool:
    return any("\u4e00" <= ch <= "\u9fff" for ch in str(text or ""))


def _latin_word_tokens(text: str) -> list[str]:
    return re.findall(r"[a-z0-9]+", str(text or "").lower())


def _edit_distance_lte_one(a: str, b: str) -> bool:
    if a == b:
        return True
    la, lb = len(a), len(b)
    if abs(la - lb) > 1:
        return False
    if la > lb:
        a, b = b, a
        la, lb = lb, la
    i = 0
    j = 0
    edits = 0
    while i < la and j < lb:
        if a[i] == b[j]:
            i += 1
            j += 1
            continue
        edits += 1
        if edits > 1:
            return False
        if la == lb:
            i += 1
            j += 1
        else:
            j += 1
    if j < lb or i < la:
        edits += 1
    return edits <= 1


def _fuzzy_latin_alias_hit(raw_text: str, alias: str) -> bool:
    alias_words = _latin_word_tokens(alias)
    text_words = _latin_word_tokens(raw_text)
    if not alias_words or not text_words:
        return False

    alias_phrase = " ".join(alias_words)
    n = len(alias_words)
    if len(text_words) >= n:
        for idx in range(0, len(text_words) - n + 1):
            candidate = " ".join(text_words[idx : idx + n])
            if candidate == alias_phrase:
                return True
            if SequenceMatcher(None, candidate, alias_phrase).ratio() >= 0.86:
                return True

    if n == 1:
        target = alias_words[0]
        if len(target) >= 4 and any(_edit_distance_lte_one(word, target) for word in text_words):
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


def _alias_hit(raw_text: str, normalized_text: str, alias: str) -> bool:
    raw_alias = str(alias or "").strip().lower()
    if not raw_alias:
        return False
    if raw_alias in raw_text:
        return True
    normalized_alias = _normalize_token(raw_alias)
    if normalized_alias and normalized_alias in normalized_text:
        return True
    # Allow mild spelling/case variations for Latin aliases, e.g. "tchaikovsky".
    if not _contains_cjk(raw_alias) and _fuzzy_latin_alias_hit(raw_text, raw_alias):
        return True
    return False


@lru_cache(maxsize=1)
def load_music_ontology() -> dict[str, Any]:
    try:
        payload = json.loads(_ONTOLOGY_PATH.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            return payload
    except Exception:
        pass
    return {
        "instruments": {},
        "forms": {},
        "work_families": {},
        "composer_aliases": {},
        "composer_work_signature_overrides": {},
    }


def collect_music_ontology_hints(text: str) -> dict[str, Any]:
    ontology = load_music_ontology()
    raw = str(text or "").lower()
    normalized = _normalize_token(text)

    instrument_keys: set[str] = set()
    form_keys: set[str] = set()
    family_keys: set[str] = set()

    for key, payload in dict(ontology.get("instruments") or {}).items():
        aliases = [str(key).strip(), *_extract_aliases(payload)]
        if any(_alias_hit(raw, normalized, alias) for alias in aliases):
            instrument_keys.add(str(key).strip())

    for key, payload in dict(ontology.get("forms") or {}).items():
        aliases = [str(key).strip(), *_extract_aliases(payload)]
        if any(_alias_hit(raw, normalized, alias) for alias in aliases):
            form_keys.add(str(key).strip())

    families = dict(ontology.get("work_families") or {})
    for key, payload in families.items():
        aliases = [str(key).strip(), *_extract_aliases(payload)]
        if any(_alias_hit(raw, normalized, alias) for alias in aliases):
            family_keys.add(str(key).strip())

    for key, payload in families.items():
        if not isinstance(payload, dict):
            continue
        inst = str(payload.get("instrument") or "").strip()
        form = str(payload.get("form") or "").strip()
        if inst and form and inst in instrument_keys and form in form_keys:
            family_keys.add(str(key).strip())

    def _flatten_aliases(section_name: str, keys: set[str]) -> list[str]:
        out: list[str] = []
        section = dict(ontology.get(section_name) or {})
        for key in keys:
            aliases = [str(key).strip(), *_extract_aliases(section.get(key))]
            for alias in aliases:
                if alias and alias not in out:
                    out.append(alias)
        return out

    return {
        "instrument_keys": sorted(instrument_keys),
        "form_keys": sorted(form_keys),
        "work_family_keys": sorted(family_keys),
        "instrument_hints": _flatten_aliases("instruments", instrument_keys),
        "form_hints": _flatten_aliases("forms", form_keys),
        "work_family_hints": _flatten_aliases("work_families", family_keys),
    }


def collect_composer_alias_hints(text: str) -> list[str]:
    ontology = load_music_ontology()
    raw = str(text or "").lower()
    normalized = _normalize_token(text)
    out: list[str] = []
    for canonical, aliases in dict(ontology.get("composer_aliases") or {}).items():
        alias_list = [str(canonical).strip()]
        if isinstance(aliases, list):
            alias_list.extend(str(item).strip() for item in aliases if str(item).strip())
        if any(_alias_hit(raw, normalized, alias) for alias in alias_list):
            for alias in alias_list:
                if alias and alias not in out:
                    out.append(alias)
    return out


def composer_override_signature_tokens(composer_hints: list[str], work_family_keys: list[str]) -> list[str]:
    ontology = load_music_ontology()
    composer_aliases = dict(ontology.get("composer_aliases") or {})
    overrides = dict(ontology.get("composer_work_signature_overrides") or {})

    normalized_hints = {_normalize_token(item) for item in composer_hints if str(item).strip()}
    matched_composers: set[str] = set()
    for canonical, aliases in composer_aliases.items():
        all_aliases = [str(canonical).strip()]
        if isinstance(aliases, list):
            all_aliases.extend(str(item).strip() for item in aliases if str(item).strip())
        if any(_normalize_token(alias) in normalized_hints for alias in all_aliases if alias):
            matched_composers.add(str(canonical).strip())

    out: list[str] = []
    for composer in matched_composers:
        table = overrides.get(composer)
        if not isinstance(table, dict):
            continue
        for family in work_family_keys:
            tokens = table.get(family)
            if not isinstance(tokens, list):
                continue
            for token in tokens:
                clean = str(token).strip()
                if clean and clean not in out:
                    out.append(clean)
    return out


def _preferred_filter_alias(section_name: str, key: str) -> str:
    ontology = load_music_ontology()
    section = dict(ontology.get(section_name) or {})
    aliases = [str(key).strip(), *_extract_aliases(section.get(key))]
    for alias in aliases:
        if _contains_cjk(alias):
            clean = str(alias).strip()
            if clean:
                return clean
    for alias in aliases:
        clean = str(alias).strip()
        if clean:
            return clean
    return str(key).strip()


def infer_music_filters_from_text(text: str) -> dict[str, list[str]]:
    hints = collect_music_ontology_hints(text)
    instrument_keys = [str(item).strip() for item in hints.get("instrument_keys", []) if str(item).strip()]
    form_keys = [str(item).strip() for item in hints.get("form_keys", []) if str(item).strip()]
    family_keys = [str(item).strip() for item in hints.get("work_family_keys", []) if str(item).strip()]

    ontology = load_music_ontology()
    families = dict(ontology.get("work_families") or {})
    for family in family_keys:
        payload = families.get(family)
        if not isinstance(payload, dict):
            continue
        instrument = str(payload.get("instrument") or "").strip()
        form = str(payload.get("form") or "").strip()
        if instrument and instrument not in instrument_keys:
            instrument_keys.append(instrument)
        if form and form not in form_keys:
            form_keys.append(form)

    filters: dict[str, list[str]] = {}
    if instrument_keys or form_keys or family_keys:
        filters["media_type"] = ["music"]
    if instrument_keys:
        filters["instrument"] = [_preferred_filter_alias("instruments", key) for key in instrument_keys]
    if form_keys:
        filters["work_type"] = [_preferred_filter_alias("forms", key) for key in form_keys]
    return {k: [v for v in values if str(v).strip()] for k, values in filters.items() if values}


def is_instrument_alias(token: str) -> bool:
    ontology = load_music_ontology()
    normalized = _normalize_token(token)
    if not normalized:
        return False
    for key, payload in dict(ontology.get("instruments") or {}).items():
        aliases = [str(key).strip(), *_extract_aliases(payload)]
        for alias in aliases:
            if _normalize_token(alias) == normalized:
                return True
    return False


def is_form_alias(token: str) -> bool:
    ontology = load_music_ontology()
    normalized = _normalize_token(token)
    if not normalized:
        return False
    for key, payload in dict(ontology.get("forms") or {}).items():
        aliases = [str(key).strip(), *_extract_aliases(payload)]
        for alias in aliases:
            if _normalize_token(alias) == normalized:
                return True
    return False


def is_work_family_alias(token: str) -> bool:
    ontology = load_music_ontology()
    normalized = _normalize_token(token)
    if not normalized:
        return False
    for key, payload in dict(ontology.get("work_families") or {}).items():
        aliases = [str(key).strip(), *_extract_aliases(payload)]
        for alias in aliases:
            if _normalize_token(alias) == normalized:
                return True
    return False
