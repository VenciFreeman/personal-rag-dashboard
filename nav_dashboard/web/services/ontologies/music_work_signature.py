from __future__ import annotations

import re
from typing import Any, Callable

from nav_dashboard.web.services.ontologies.music_ontology import (
    is_form_alias,
    is_instrument_alias,
    is_work_family_alias,
)


def _normalize_music_title_token(text: str) -> str:
    return "".join(ch.lower() for ch in str(text or "") if ch.isalnum() or "\u4e00" <= ch <= "\u9fff")


def is_specific_music_marker(token: str) -> bool:
    raw_token = str(token or "").strip().lower()
    normalized_token = _normalize_music_title_token(raw_token)
    if not raw_token and not normalized_token:
        return False
    if re.search(r"op\.?\s*\d+", raw_token):
        return True
    if re.search(r"no\.?\s*\d+", raw_token):
        return True
    if re.search(r"第\s*\d+", str(token or "")):
        return True
    if normalized_token.startswith("op") and any(ch.isdigit() for ch in normalized_token):
        return True
    if normalized_token.startswith("no") and any(ch.isdigit() for ch in normalized_token):
        return True
    return False


def title_matches_music_work_signature(
    title: str,
    work_signature: list[str],
    composer_hints: list[str],
    instrument_hints: list[str] | None = None,
    form_hints: list[str] | None = None,
    work_family_hints: list[str] | None = None,
    resolve_creator_canonicals: Callable[[list[str]], set[str]] | None = None,
) -> bool:
    normalized = _normalize_music_title_token(title)
    if not normalized:
        return False

    def _contains_any(tokens: list[str]) -> bool:
        for token in tokens:
            clean = _normalize_music_title_token(token)
            if clean and clean in normalized:
                return True
        return False

    composer_ok = _contains_any(composer_hints) if composer_hints else True
    if composer_hints and not composer_ok and callable(resolve_creator_canonicals):
        try:
            query_canonicals = resolve_creator_canonicals(composer_hints)
            title_candidates: list[str] = []
            raw_title = str(title or "").strip()
            if raw_title:
                leading = raw_title.split(":", 1)[0].split("：", 1)[0].strip()
                if leading:
                    title_candidates.append(leading)
                for token in re.split(r"[;&/&]| and ", leading):
                    clean = str(token or "").strip()
                    if clean and clean not in title_candidates:
                        title_candidates.append(clean)
            title_canonicals = resolve_creator_canonicals(title_candidates)
            composer_ok = bool(query_canonicals and title_canonicals and query_canonicals.intersection(title_canonicals))
        except Exception:
            composer_ok = False

    signature_tokens = [str(item).strip() for item in work_signature if str(item).strip()]
    signature_instruments = [str(item).strip() for item in (instrument_hints or []) if str(item).strip()]
    signature_forms = [str(item).strip() for item in (form_hints or []) if str(item).strip()]
    signature_families = [str(item).strip() for item in (work_family_hints or []) if str(item).strip()]

    if not signature_instruments:
        signature_instruments = [token for token in signature_tokens if is_instrument_alias(token)]
    if not signature_forms:
        signature_forms = [token for token in signature_tokens if is_form_alias(token)]
    if not signature_families:
        signature_families = [token for token in signature_tokens if is_work_family_alias(token)]

    family_hit = _contains_any(signature_families)
    instrument_hit = _contains_any(signature_instruments) if signature_instruments else False
    form_hit = _contains_any(signature_forms) if signature_forms else False
    if signature_instruments and signature_forms:
        work_ok = family_hit or (instrument_hit and form_hit)
    elif signature_families:
        work_ok = family_hit
    else:
        work_ok = _contains_any(signature_tokens)

    specific_tokens = [token for token in signature_tokens if is_specific_music_marker(token)]
    if work_ok and specific_tokens:
        work_ok = _contains_any(specific_tokens)

    return composer_ok and work_ok


def filter_music_compare_rows(
    rows: list[dict[str, Any]],
    *,
    work_signature: list[str],
    composer_hints: list[str],
    instrument_hints: list[str],
    form_hints: list[str],
    work_family_hints: list[str],
    resolve_creator_canonicals: Callable[[list[str]], set[str]] | None = None,
    has_composer_anchor_conflict: Callable[[str, list[str]], bool] | None = None,
) -> list[dict[str, Any]]:
    strict = [
        row
        for row in rows
        if title_matches_music_work_signature(
            str(row.get("title") or ""),
            work_signature=work_signature,
            composer_hints=composer_hints,
            instrument_hints=instrument_hints,
            form_hints=form_hints,
            work_family_hints=work_family_hints,
            resolve_creator_canonicals=resolve_creator_canonicals,
        )
    ]
    if len(strict) >= 2:
        return strict

    relaxed = [
        row
        for row in rows
        if title_matches_music_work_signature(
            str(row.get("title") or ""),
            work_signature=work_signature,
            composer_hints=[],
            instrument_hints=instrument_hints,
            form_hints=form_hints,
            work_family_hints=work_family_hints,
            resolve_creator_canonicals=resolve_creator_canonicals,
        )
        and not (
            callable(has_composer_anchor_conflict)
            and has_composer_anchor_conflict(str(row.get("title") or ""), composer_hints)
        )
    ]
    if relaxed and len(relaxed) > len(strict):
        merged: list[dict[str, Any]] = []
        seen_keys: set[str] = set()
        for row in rows:
            if row not in strict and row not in relaxed:
                continue
            row_key = str(row.get("id") or row.get("title") or "").strip()
            if row_key and row_key in seen_keys:
                continue
            if row_key:
                seen_keys.add(row_key)
            merged.append(row)
        if merged:
            return merged
    if strict:
        return strict
    return relaxed