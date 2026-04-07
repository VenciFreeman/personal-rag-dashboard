from __future__ import annotations

import re
from typing import Any

from .media_core import _normalize_media_filter_map
from .media_helpers import _canonicalize_media_entity
from ...media.entity_resolver import resolve_creator_hit as _er_resolve_creator_hit
from ...media.entity_resolver import resolve_title_hit as _er_resolve_title_hit
from ...ontologies.music_ontology import (
    collect_composer_alias_hints,
    collect_music_ontology_hints,
    is_form_alias,
    is_instrument_alias,
    load_music_ontology,
)
from ...ontologies.music_work_signature import title_matches_music_work_signature as _title_matches_music_work_signature


def _extract_music_work_hints(question: str, filters: dict[str, list[str]] | None = None) -> dict[str, list[str]]:
    text = str(question or "").strip()
    lowered = text.lower()
    normalized_filters = _normalize_media_filter_map(filters)
    media_type_values = [str(item).strip().lower() for item in normalized_filters.get("media_type", []) if str(item).strip()]
    filter_surface = " ".join(str(value) for key, values in normalized_filters.items() for value in ([key] + (values if isinstance(values, list) else [])))
    mixed_surface = f"{text} {filter_surface}".strip()
    mixed_lower = mixed_surface.lower()
    ontology_hints = collect_music_ontology_hints(mixed_surface)
    instrument_hints = [str(item).strip() for item in ontology_hints.get("instrument_hints", []) if str(item).strip()]
    form_hints = [str(item).strip() for item in ontology_hints.get("form_hints", []) if str(item).strip()]
    work_family_hints = [str(item).strip() for item in ontology_hints.get("work_family_hints", []) if str(item).strip()]
    music_surface = (
        "music" in media_type_values
        or any(key in normalized_filters for key in ("乐器", "作品类型", "instrument", "work_type"))
        or bool(instrument_hints)
        or bool(form_hints)
        or bool(work_family_hints)
        or "听过" in text
    )
    if not music_surface:
        return {"composer_hints": [], "instrument_hints": [], "form_hints": [], "work_family_hints": [], "work_signature": []}
    work_signature: list[str] = [*instrument_hints, *form_hints, *work_family_hints]
    no_match = re.search(r"(?:no\.?|number|第)\s*([0-9]{1,2})", mixed_lower)
    if no_match:
        idx = no_match.group(1)
        work_signature.extend([f"no.{idx}", f"no {idx}", f"第{idx}"])
    op_match = re.search(r"op\.?\s*(\d{1,3})", lowered)
    if op_match:
        op_no = op_match.group(1)
        work_signature.extend([f"op.{op_no}", f"op {op_no}"])
    composer_hints: list[str] = []
    seed_candidates: list[str] = []
    if "柴可夫斯基" in text:
        seed_candidates.append("柴可夫斯基")
    if "tchaikovsky" in lowered:
        seed_candidates.append("Tchaikovsky")
    for cn_name in re.findall(r"[\u4e00-\u9fff]{2,8}", text):
        if cn_name not in seed_candidates:
            seed_candidates.append(cn_name)
    for en_name in re.findall(r"[A-Za-z][A-Za-z\-\s]{2,32}", text):
        clean_en = str(en_name).strip()
        if clean_en and clean_en not in seed_candidates:
            seed_candidates.append(clean_en)
    for candidate in seed_candidates[:10]:
        resolved = _er_resolve_creator_hit(candidate, min_confidence=0.6)
        if resolved and str(resolved.canonical or "").strip():
            canonical = str(resolved.canonical).strip()
            if canonical not in composer_hints:
                composer_hints.append(canonical)
            for work in list(getattr(resolved, "works", []) or [])[:80]:
                try:
                    author = str(getattr(work, "author", "") or "").strip()
                    if author and author not in composer_hints:
                        composer_hints.append(author)
                    title = str(getattr(work, "canonical", "") or "").strip() or str(getattr(work, "title", "") or "").strip()
                    if ":" in title or "：" in title:
                        prefix = title.split(":", 1)[0].split("：", 1)[0].strip()
                        if prefix and len(prefix) <= 40 and prefix not in composer_hints:
                            composer_hints.append(prefix)
                except Exception:
                    continue
    for alias in collect_composer_alias_hints(mixed_surface):
        if alias not in composer_hints:
            composer_hints.append(alias)
    for composer in list(composer_hints)[:8]:
        for family in list(work_family_hints)[:6]:
            probe = f"{composer} {family}".strip()
            resolved_title = _er_resolve_title_hit(probe, min_confidence=0.45)
            if not resolved_title:
                continue
            canon_title = str(getattr(resolved_title, "canonical", "") or "").strip()
            if not canon_title:
                continue
            if ":" in canon_title or "：" in canon_title:
                prefix = canon_title.split(":", 1)[0].split("：", 1)[0].strip()
                if prefix and len(prefix) <= 40 and prefix not in composer_hints:
                    composer_hints.append(prefix)
    dedup_composers: list[str] = []
    seen_composers: set[str] = set()
    for item in composer_hints:
        key = str(item).strip().casefold()
        if not key or key in seen_composers:
            continue
        seen_composers.add(key)
        dedup_composers.append(str(item).strip())
    dedup_signature: list[str] = []
    seen_signature: set[str] = set()
    for item in work_signature:
        key = str(item).strip().casefold()
        if not key or key in seen_signature:
            continue
        seen_signature.add(key)
        dedup_signature.append(str(item).strip())
    return {
        "composer_hints": dedup_composers,
        "instrument_hints": [item for item in dict.fromkeys(instrument_hints) if str(item).strip()],
        "form_hints": [item for item in dict.fromkeys(form_hints) if str(item).strip()],
        "work_family_hints": [item for item in dict.fromkeys(work_family_hints) if str(item).strip()],
        "work_signature": dedup_signature,
    }


def _resolve_creator_canonicals(tokens: list[str]) -> set[str]:
    canonicals: set[str] = set()
    for token in tokens:
        clean = str(token or "").strip()
        if not clean:
            continue
        try:
            resolved = _er_resolve_creator_hit(clean, min_confidence=0.5)
        except Exception:
            resolved = None
        canonical = str(getattr(resolved, "canonical", "") or "").strip()
        if canonical:
            canonicals.add(canonical.casefold())
    return canonicals


def _resolve_music_work_canonical_entity(question: str, music_work_hints: dict[str, list[str]] | None = None) -> str:
    hints = music_work_hints if isinstance(music_work_hints, dict) else {}
    composer_hints = [str(item).strip() for item in list(hints.get("composer_hints") or []) if str(item).strip()]
    form_hints = [str(item).strip() for item in list(hints.get("form_hints") or []) if str(item).strip()]
    work_family_hints = [str(item).strip() for item in list(hints.get("work_family_hints") or []) if str(item).strip()]
    work_signature = [str(item).strip() for item in list(hints.get("work_signature") or []) if str(item).strip()]
    normalized_question = str(question or "").strip()
    candidate_probes: list[str] = []
    expanded_family_hints = list(work_family_hints)
    try:
        ontology_hints = collect_music_ontology_hints(" ".join([normalized_question, *work_family_hints, *work_signature]))
        for alias in list(ontology_hints.get("work_family_hints") or []):
            clean_alias = str(alias).strip()
            if clean_alias and clean_alias not in expanded_family_hints:
                expanded_family_hints.append(clean_alias)
    except Exception:
        ontology_hints = {"work_family_keys": []}
    stripped_question = re.sub(r"^(?:我(?:最近|近期|这段时间)?(?:听过|看过|读过|玩过)?的?)", "", normalized_question)
    stripped_question = re.sub(r"(?:有哪些版本|有啥版本|版本有哪些|评价各自咋样|评价咋样|各自咋样|个人评分与短评|个人评价|评价)$", "", stripped_question)
    stripped_question = stripped_question.strip(" ，,：:；;？?。.!！")
    if stripped_question:
        candidate_probes.append(stripped_question)
    for composer in composer_hints[:4]:
        for family in work_family_hints[:4]:
            probe = f"{composer} {family}".strip()
            if probe and probe not in candidate_probes:
                candidate_probes.append(probe)
        for form in form_hints[:4]:
            probe = f"{composer} {form}".strip()
            if probe and probe not in candidate_probes:
                candidate_probes.append(probe)
        focused_signature = [token for token in work_signature if any(char.isdigit() for char in token) or token in work_family_hints][:4]
        if focused_signature:
            probe = f"{composer} {' '.join(focused_signature)}".strip()
            if probe and probe not in candidate_probes:
                candidate_probes.append(probe)
    for probe in candidate_probes[:12]:
        resolved = _er_resolve_title_hit(probe, min_confidence=0.4)
        if resolved and str(getattr(resolved, "canonical", "") or "").strip():
            canonical_title = str(getattr(resolved, "canonical", "") or "").strip()
            if _title_matches_music_work_signature(canonical_title, work_signature=work_signature, composer_hints=composer_hints, work_family_hints=expanded_family_hints, resolve_creator_canonicals=_resolve_creator_canonicals):
                return canonical_title
        canonical_probe, _ = _canonicalize_media_entity(probe)
        if canonical_probe and canonical_probe != probe and _title_matches_music_work_signature(canonical_probe, work_signature=work_signature, composer_hints=composer_hints, work_family_hints=expanded_family_hints, resolve_creator_canonicals=_resolve_creator_canonicals):
            return canonical_probe
    try:
        family_keys = [str(item).strip() for item in list(ontology_hints.get("work_family_keys") or []) if str(item).strip()]
        latin_composer = next((item for item in composer_hints if re.search(r"[A-Za-z]", str(item))), "")
        number_token = next((item for item in work_signature if re.search(r"no\.?\s*(\d+)", str(item), re.IGNORECASE)), "")
        if not number_token:
            number_token = next((item for item in work_signature if re.search(r"第\s*(\d+)", str(item))), "")
        if latin_composer and family_keys:
            family_payload = dict(load_music_ontology().get("work_families") or {}).get(family_keys[0]) or {}
            family_aliases = [str(alias).strip() for alias in [family_keys[0], *(family_payload.get("aliases") or [])] if str(alias).strip()]
            latin_family = next((alias for alias in family_aliases if re.search(r"[A-Za-z]", alias) and "_" not in alias), "")
            if not latin_family:
                latin_family = str(family_keys[0]).replace("_", " ").title()
            candidate = " ".join(item for item in [latin_composer, latin_family, str(number_token or "").strip()] if str(item).strip()).strip()
            if candidate and _title_matches_music_work_signature(candidate, work_signature=work_signature, composer_hints=composer_hints, work_family_hints=expanded_family_hints, resolve_creator_canonicals=_resolve_creator_canonicals):
                return candidate
    except Exception:
        pass
    return ""


def _has_music_signature_filters(filters: dict[str, list[str]] | None) -> bool:
    normalized = _normalize_media_filter_map(filters)

    def _contains_alias(field: str, predicate: Any) -> bool:
        values = [str(item).strip() for item in normalized.get(field, []) if str(item).strip()]
        return any(predicate(value) for value in values)

    has_instrument = _contains_alias("instrument", is_instrument_alias) or _contains_alias("work_type", is_instrument_alias)
    has_concerto = _contains_alias("work_type", is_form_alias) or _contains_alias("instrument", is_form_alias)
    return has_instrument and has_concerto
