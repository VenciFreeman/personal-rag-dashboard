"""entity_resolver.py

Canonical entity resolution for the personal media library.

Public API
----------
    resolve_title(text, hint_media_type="")   -> TitleResolution | None
    resolve_creator(text, hint_media_type="") -> CreatorResolution | None
    resolve_media_entities(text, hint_media_type="") -> MediaEntityResolution

Resolution passes (in order, stopping on first hit above min_confidence):
    1. Exact normalized match    confidence = 1.00
    2. Cross-language alias      confidence = 0.85
    3. Prefix match              confidence = 0.75 * ratio
    4. Substring containment     confidence = 0.50 * ratio

The index is loaded from library_tracker/data/structured/*.json and
invalidated automatically when any source file's mtime/size changes.

This module is intentionally self-contained (no imports from agent_service).
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from core_service.runtime_data import app_runtime_root

try:
    from nav_dashboard.web.services.ontologies.video_ontology import collect_video_ontology_hints as _collect_video_hints
except Exception:
    def _collect_video_hints(text: str) -> dict[str, Any]:
        return {}

try:
    from nav_dashboard.web.services.ontologies.book_ontology import collect_book_ontology_hints as _collect_book_hints
except Exception:
    def _collect_book_hints(text: str) -> dict[str, Any]:
        return {}

# ---------------------------------------------------------------------------
# Normalizer — mirrors agent_service._normalize_media_title_for_match
# ---------------------------------------------------------------------------

def _norm(text: str) -> str:
    """Lowercase, strip whitespace and punctuation, keep CJK + latin + digits."""
    value = str(text or "").strip().lower()
    if not value:
        return ""
    value = re.sub(r"\s+", "", value)
    value = re.sub(r"[^\u4e00-\u9fffa-z0-9]", "", value)
    return value


# ---------------------------------------------------------------------------
# Supplemental cross-language alias table.
# Approved library aliases are loaded dynamically from library_alias_store and
# merged into the exported alias map at runtime. This static table is now only
# a thin fallback for long-standing hand-tuned alternates.
# ---------------------------------------------------------------------------

_SUPPLEMENTAL_RAW_ALIASES: dict[str, list[str]] = {
    # ── Composers / 作曲家 ─────────────────────────────────────────────────
    "tchaikovsky":      ["柴可夫斯基", "柴科夫斯基"],
    "柴可夫斯基":        ["tchaikovsky", "柴科夫斯基"],
    "柴科夫斯基":        ["tchaikovsky", "柴可夫斯基"],
    "beethoven":        ["贝多芬"],
    "贝多芬":            ["beethoven"],
    "mozart":           ["莫扎特"],
    "莫扎特":            ["mozart"],
    "chopin":           ["肖邦"],
    "肖邦":              ["chopin"],
    "bach":             ["巴赫"],
    "巴赫":              ["bach"],
    "schubert":         ["舒伯特"],
    "舒伯特":            ["schubert"],
    "brahms":           ["勃拉姆斯"],
    "勃拉姆斯":          ["brahms"],
    "debussy":          ["德彪西"],
    "德彪西":            ["debussy"],
    "handel":           ["亨德尔"],
    "亨德尔":            ["handel"],
    "vivaldi":          ["维瓦尔第"],
    "维瓦尔第":          ["vivaldi"],
    "rachmaninoff":     ["拉赫玛尼诺夫", "rachmaninov"],
    "rachmaninov":      ["拉赫玛尼诺夫", "rachmaninoff"],
    "拉赫玛尼诺夫":      ["rachmaninoff", "rachmaninov"],
    "liszt":            ["李斯特"],
    "李斯特":            ["liszt"],
    "verdi":            ["威尔第"],
    "威尔第":            ["verdi"],
    "puccini":          ["普契尼"],
    "普契尼":            ["puccini"],
    "wagner":           ["瓦格纳"],
    "瓦格纳":            ["wagner"],
    "mahler":           ["马勒"],
    "马勒":              ["mahler"],
    "strauss":          ["施特劳斯"],
    "施特劳斯":          ["strauss"],
    "dvorak":           ["德沃夏克"],
    "德沃夏克":          ["dvorak"],
    "sibelius":         ["西贝柳斯"],
    "西贝柳斯":          ["sibelius"],
    "stravinsky":       ["斯特拉文斯基"],
    "斯特拉文斯基":      ["stravinsky"],
    "prokofiev":        ["普罗科菲耶夫"],
    "普罗科菲耶夫":      ["prokofiev"],
    "shostakovich":     ["肖斯塔科维奇"],
    "肖斯塔科维奇":      ["shostakovich"],
    # ── Authors / 作家 ───────────────────────────────────────────────────
    "tolstoy":          ["托尔斯泰"],
    "托尔斯泰":          ["tolstoy"],
    "dostoevsky":       ["陀思妥耶夫斯基", "陀斯妥耶夫斯基"],
    "dostoevski":       ["陀思妥耶夫斯基"],
    "陀思妥耶夫斯基":    ["dostoevsky"],
    "陀斯妥耶夫斯基":    ["dostoevsky"],
    "kafka":            ["卡夫卡"],
    "卡夫卡":            ["kafka"],
    "hemingway":        ["海明威"],
    "海明威":            ["hemingway"],
    "orwell":           ["奥威尔"],
    "奥威尔":            ["orwell"],
    "camus":            ["加缪"],
    "加缪":              ["camus"],
    "sartre":           ["萨特"],
    "萨特":              ["sartre"],
    "nietzsche":        ["尼采"],
    "尼采":              ["nietzsche"],
    "shakespeare":      ["莎士比亚"],
    "莎士比亚":          ["shakespeare"],
    "rowling":          ["罗琳", "jk罗琳"],
    "jkrowling":        ["罗琳"],
    "罗琳":              ["rowling"],
    "hugo":             ["雨果", "维克多雨果"],
    "雨果":              ["hugo"],
    "维克多雨果":        ["hugo"],
    "dumas":            ["大仲马"],
    "大仲马":            ["dumas"],
    "小仲马":            ["dumasfils"],
    "dumasfils":        ["小仲马"],
    "balzac":           ["巴尔扎克"],
    "巴尔扎克":          ["balzac"],
    "flaubert":         ["福楼拜"],
    "福楼拜":            ["flaubert"],
    "zola":             ["左拉"],
    "左拉":              ["zola"],
    "chekhov":          ["契诃夫"],
    "契诃夫":            ["chekhov"],
    "pushkin":          ["普希金"],
    "普希金":            ["pushkin"],
    "turgenev":         ["屠格涅夫"],
    "屠格涅夫":          ["turgenev"],
    "garciamarquez":    ["马尔克斯", "加西亚马尔克斯"],
    "marquez":          ["马尔克斯", "加西亚马尔克斯", "garciamarquez"],
    "马尔克斯":          ["marquez", "garciamarquez"],
    "加西亚马尔克斯":    ["garciamarquez", "marquez"],
    "borges":           ["博尔赫斯"],
    "博尔赫斯":          ["borges"],
    "cortazar":         ["科塔萨尔"],
    "科塔萨尔":          ["cortazar"],
    "neruda":           ["聂鲁达"],
    "聂鲁达":            ["neruda"],
    "llosa":            ["略萨", "巴尔加斯略萨"],
    "略萨":              ["llosa"],
    "calvino":          ["卡尔维诺"],
    "卡尔维诺":          ["calvino"],
    "proust":           ["普鲁斯特"],
    "普鲁斯特":          ["proust"],
    "joyce":            ["乔伊斯"],
    "乔伊斯":            ["joyce"],
    "woolf":            ["伍尔夫", "弗吉尼亚伍尔夫"],
    "伍尔夫":            ["woolf"],
    "faulkner":         ["福克纳"],
    "福克纳":            ["faulkner"],
    "fitzgerald":       ["菲茨杰拉德"],
    "菲茨杰拉德":        ["fitzgerald"],
    "steinbeck":        ["斯坦贝克"],
    "斯坦贝克":          ["steinbeck"],
    "camus":            ["加缪"],
    "murakami":         ["村上春树"],
    "村上春树":          ["murakami"],
    "mishima":          ["三岛由纪夫"],
    "三岛由纪夫":        ["mishima"],
    "kawabata":         ["川端康成"],
    "川端康成":          ["kawabata"],
    "oe":               ["大江健三郎"],
    "大江健三郎":        ["oe"],
    "tanizaki":         ["谷崎润一郎"],
    "谷崎润一郎":        ["tanizaki"],
    "soseki":           ["夏目漱石"],
    "夏目漱石":          ["soseki"],
    "natsumesoseki":    ["夏目漱石"],
    "akutagawa":        ["芥川龙之介"],
    "芥川龙之介":        ["akutagawa"],
    # ── Directors / 导演 ────────────────────────────────────────────────
    "miyazaki":         ["宫崎骏", "宫崎駿"],
    "宫崎骏":            ["miyazaki"],
    "宫崎駿":            ["miyazaki"],
    "kurosawa":         ["黑泽明"],
    "黑泽明":            ["kurosawa"],
    "kitano":           ["北野武"],
    "北野武":            ["kitano"],
    "bergman":          ["柏格曼", "伯格曼", "英格玛柏格曼"],
    "柏格曼":            ["bergman"],
    "tarkovsky":        ["塔可夫斯基"],
    "塔可夫斯基":        ["tarkovsky"],
    "kubrick":          ["库布里克"],
    "库布里克":          ["kubrick"],
    "spielberg":        ["斯皮尔伯格"],
    "斯皮尔伯格":        ["spielberg"],
    "scorsese":         ["斯科塞斯"],
    "斯科塞斯":          ["scorsese"],
    "coppola":          ["科波拉"],
    "科波拉":            ["coppola"],
    "truffaut":         ["特吕弗"],
    "特吕弗":            ["truffaut"],
    "godard":           ["戈达尔"],
    "戈达尔":            ["godard"],
    "lynch":            ["大卫林奇", "林奇"],
    "林奇":              ["lynch"],
    "wongkarwai":       ["王家卫"],
    "王家卫":            ["wongkarwai"],
    "zhangke":          ["贾樟柯"],
    "贾樟柯":            ["zhangke"],
    "yimou":            ["张艺谋"],
    "张艺谋":            ["yimou"],
    "kaige":            ["陈凯歌"],
    "陈凯歌":            ["kaige"],
    "leekangsheng":     ["李康生"],
    "tsaimingliang":    ["蔡明亮"],
    "蔡明亮":            ["tsaimingliang"],
    "hosoda":           ["细田守"],
    "细田守":            ["hosoda"],
    "shinkai":          ["新海诚"],
    "新海诚":            ["shinkai"],
    "anno":             ["庵野秀明"],
    "庵野秀明":          ["anno"],
    "otomo":            ["大友克洋"],
    "大友克洋":          ["otomo"],
    "kon":              ["今敏"],
    "今敏":              ["kon"],
    "oshii":            ["押井守"],
    "押井守":            ["oshii"],
    "nolan":            ["诺兰", "克里斯托弗诺兰"],
    "诺兰":              ["nolan"],
    "fincher":          ["大卫芬奇", "芬奇"],
    "芬奇":              ["fincher"],
    "tarantino":        ["昆汀塔伦蒂诺", "昆汀"],
    "昆汀":              ["tarantino"],
    "anderson":         ["韦斯安德森", "保罗托马斯安德森"],
}

def _append_alias(alias_map: dict[str, list[str]], source: str, target: str) -> None:
    if not source or not target or source == target:
        return
    alias_map.setdefault(source, [])
    if target not in alias_map[source]:
        alias_map[source].append(target)


def _iter_creator_name_segments(author_text: str) -> list[str]:
    raw = str(author_text or "").strip()
    if not raw:
        return []
    parts = [raw]
    for item in re.split(r"\s*(?:,|，|/|、|&|and)\s*", raw):
        clean = str(item or "").strip()
        if clean and clean not in parts:
            parts.append(clean)
    return parts


def _build_alias_map_from_pairs(raw_aliases: dict[str, list[str]]) -> dict[str, list[str]]:
    alias_map: dict[str, list[str]] = {}
    for raw_key, raw_vals in raw_aliases.items():
        normalized_key = _norm(raw_key)
        if not normalized_key:
            continue
        normalized_values = [_norm(value) for value in raw_vals if _norm(value)]
        for normalized_value in normalized_values:
            _append_alias(alias_map, normalized_key, normalized_value)
    return alias_map


_STATIC_ALIASES = _build_alias_map_from_pairs(_SUPPLEMENTAL_RAW_ALIASES)
_ALIASES: dict[str, list[str]] = dict(_STATIC_ALIASES)

# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

MatchKind = Literal["exact", "alias", "prefix", "substring"]


@dataclass
class TitleRecord:
    canonical: str
    media_type: str      # "video" | "book" | "music" | "game"
    category: str
    author: str
    date: str
    rating: Any
    item_id: str = ""
    review: str = ""


@dataclass
class TitleResolution:
    canonical: str
    media_type: str
    category: str
    author: str
    confidence: float
    match_kind: MatchKind
    aliases: list[str] = field(default_factory=list)


@dataclass
class CreatorResolution:
    canonical: str          # Author name as stored in library
    media_type_hint: str    # Most common media_type for this creator
    works: list[TitleRecord]
    confidence: float
    match_kind: MatchKind


@dataclass
class MediaEntityResolution:
    query: str
    title_hits: list[TitleResolution] = field(default_factory=list)
    creator_hits: list[CreatorResolution] = field(default_factory=list)
    concept_hints: list[str] = field(default_factory=list)
    primary_entity: dict[str, Any] | None = None
    evidence: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Entity index
# ---------------------------------------------------------------------------

_WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
_LIBRARY_TRACKER_ROOT = _WORKSPACE_ROOT / "library_tracker"
_LIBRARY_TRACKER_RUNTIME_ROOT = app_runtime_root("library_tracker")

try:
    from library_tracker.web.services.library_alias_store import approved_alias_registry_signature as _approved_alias_registry_signature
    from library_tracker.web.services.library_alias_store import get_approved_alias_registry as _get_approved_alias_registry
except Exception:
    _approved_alias_registry_signature = None
    _get_approved_alias_registry = None

_STRUCTURED_DIR  = _LIBRARY_TRACKER_RUNTIME_ROOT / "structured"
_STRUCTURED_ENTITIES_DIR = _STRUCTURED_DIR / "entities"


def _structured_base_dirs() -> list[Path]:
    preferred: list[Path] = [
        _STRUCTURED_ENTITIES_DIR,
        _STRUCTURED_DIR,
    ]
    ordered: list[Path] = []
    for candidate in preferred:
        if candidate not in ordered:
            ordered.append(candidate)
    return ordered


def _iter_entity_json_paths() -> list[Path]:
    preferred_by_name: dict[str, Path] = {}
    for base_dir in _structured_base_dirs():
        try:
            paths = sorted(base_dir.glob("*.json"), key=lambda item: item.name.lower())
        except Exception:
            paths = []
        for path in paths:
            preferred_by_name.setdefault(path.name.lower(), path)
            if base_dir in {_STRUCTURED_ENTITIES_DIR, _STRUCTURED_DIR}:
                preferred_by_name[path.name.lower()] = path
    return [preferred_by_name[name] for name in sorted(preferred_by_name)]


class _ApprovedAliasRegistryCache:
    def __init__(self) -> None:
        self._sig: tuple[int, int] | None = None
        self._combined_alias_map: dict[str, list[str]] = dict(_STATIC_ALIASES)
        self._field_alias_maps: dict[str, dict[str, list[str]]] = {
            "title": {},
            "creator": dict(_STATIC_ALIASES),
            "publisher": {},
        }

    def _compute_sig(self) -> tuple[int, int] | None:
        if callable(_approved_alias_registry_signature):
            try:
                return _approved_alias_registry_signature()
            except Exception:
                return None
        return None

    def get_combined_aliases(self) -> dict[str, list[str]]:
        sig = self._compute_sig()
        if sig == self._sig:
            return self._combined_alias_map
        self._sig = sig
        combined_alias_map, field_alias_maps = self._load()
        self._combined_alias_map = combined_alias_map
        self._field_alias_maps = field_alias_maps
        return self._combined_alias_map

    def get_field_aliases(self, field_type: str) -> dict[str, list[str]]:
        self.get_combined_aliases()
        return self._field_alias_maps.get(str(field_type or "").strip().lower(), {})

    def _load(self) -> tuple[dict[str, list[str]], dict[str, dict[str, list[str]]]]:
        combined_alias_map: dict[str, list[str]] = {
            key: list(values)
            for key, values in _STATIC_ALIASES.items()
        }
        field_alias_maps: dict[str, dict[str, list[str]]] = {
            "title": {},
            "creator": {
                key: list(values)
                for key, values in _STATIC_ALIASES.items()
            },
            "publisher": {},
        }
        if not callable(_get_approved_alias_registry):
            return combined_alias_map, field_alias_maps
        try:
            registry = _get_approved_alias_registry()
        except Exception:
            return combined_alias_map, field_alias_maps
        by_term = registry.get("by_term") if isinstance(registry, dict) else {}
        for normalized_term, records in by_term.items():
            source_key = _norm(normalized_term)
            if not source_key:
                continue
            for record in records if isinstance(records, list) else []:
                if not isinstance(record, dict):
                    continue
                field_type = str(record.get("field_type") or "").strip().lower()
                if field_type not in field_alias_maps:
                    continue
                normalized_terms = [
                    _norm(term)
                    for term in list(record.get("terms") or [])
                    if _norm(term)
                ]
                for alternate_key in normalized_terms:
                    if not alternate_key or alternate_key == source_key:
                        continue
                    _append_alias(combined_alias_map, source_key, alternate_key)
                    _append_alias(field_alias_maps[field_type], source_key, alternate_key)
        return combined_alias_map, field_alias_maps


_APPROVED_ALIAS_REGISTRY = _ApprovedAliasRegistryCache()


def _refresh_alias_exports() -> dict[str, list[str]]:
    combined_aliases = _APPROVED_ALIAS_REGISTRY.get_combined_aliases()
    if combined_aliases != _ALIASES:
        _ALIASES.clear()
        _ALIASES.update({key: list(values) for key, values in combined_aliases.items()})
    return _ALIASES


def _collect_alias_hints(field_type: str, *terms: str, limit: int = 12) -> list[str]:
    alias_map = _APPROVED_ALIAS_REGISTRY.get_field_aliases(field_type)
    output: list[str] = []
    seen: set[str] = set()
    for term in terms:
        raw_term = str(term or "").strip()
        normalized_term = _norm(raw_term)
        if not raw_term or not normalized_term:
            continue
        if raw_term.casefold() not in seen:
            output.append(raw_term)
            seen.add(raw_term.casefold())
        for alias_key in list(alias_map.get(normalized_term, []) or []):
            records = _INDEX.ensure_fresh()
            if field_type == "title":
                alias_value = next(
                    (record.canonical for record in list(records.title_idx.get(alias_key, []) or []) if str(record.canonical or "").strip()),
                    "",
                )
            else:
                entry = records.author_idx.get(alias_key)
                alias_value = str(entry[0] or "").strip() if entry else ""
            if not alias_value:
                alias_value = str(alias_key or "").strip()
            folded = alias_value.casefold()
            if folded and folded not in seen:
                output.append(alias_value)
                seen.add(folded)
            if len(output) >= max(1, int(limit)):
                return output[:limit]
    return output[:limit]


_refresh_alias_exports()


class _EntityIndex:
    """Lazy-loading, file-mtime–invalidated index of library titles and creators."""

    def __init__(self) -> None:
        self._sig: tuple[tuple[str, int, int], ...] = ()
        self._title_idx: dict[str, list[TitleRecord]] = {}
        # norm_author → (canonical_author_string, [records])
        self._author_idx: dict[str, tuple[str, list[TitleRecord]]] = {}

    # -- cache invalidation --------------------------------------------------

    def _compute_sig(self) -> tuple[tuple[str, int, int], ...]:
        entries: list[tuple[str, int, int]] = []
        for path in _iter_entity_json_paths():
            try:
                stat = path.stat()
                entries.append((path.name, int(stat.st_mtime_ns), int(stat.st_size)))
            except Exception:
                entries.append((path.name, 0, 0))
        return tuple(entries)

    def _build(self) -> None:
        title_idx: dict[str, list[TitleRecord]] = {}
        author_map: dict[str, tuple[str, list[TitleRecord]]] = {}

        for path in _iter_entity_json_paths():
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                rows = payload.get("records", []) if isinstance(payload, dict) else []
            except Exception:
                continue

            for index, row in enumerate(rows):
                if not isinstance(row, dict):
                    continue
                title = str(row.get("title") or "").strip()
                if not title:
                    continue
                media_type = str(row.get("media_type") or "").strip().lower()

                rec = TitleRecord(
                    item_id=str(row.get("id") or f"{media_type}:{index}").strip(),
                    canonical=title,
                    media_type=media_type,
                    category=str(row.get("category") or "").strip(),
                    author=str(row.get("author") or "").strip(),
                    date=str(row.get("date") or "").strip(),
                    rating=row.get("rating"),
                    review=str(row.get("review") or "").strip(),
                )

                nt = _norm(title)
                if nt:
                    title_idx.setdefault(nt, []).append(rec)

                author_segments = _iter_creator_name_segments(rec.author)
                for author_segment in author_segments:
                    na = _norm(author_segment)
                    if not na:
                        continue
                    if na not in author_map:
                        author_map[na] = (author_segment, [])
                    author_map[na][1].append(rec)

        self._title_idx = title_idx
        self._author_idx = author_map

    def ensure_fresh(self) -> "_EntityIndex":
        sig = self._compute_sig()
        if sig != self._sig or not self._title_idx:
            self._build()
            self._sig = sig
        return self

    @property
    def title_idx(self) -> dict[str, list[TitleRecord]]:
        return self._title_idx

    @property
    def author_idx(self) -> dict[str, tuple[str, list[TitleRecord]]]:
        return self._author_idx


_INDEX = _EntityIndex()


# ---------------------------------------------------------------------------
# Title resolution
# ---------------------------------------------------------------------------

def resolve_title(
    text: str,
    hint_media_type: str = "",
    min_confidence: float = 0.3,
) -> TitleResolution | None:
    """Resolve free-text to a canonical library title.

    Returns the best match above min_confidence, or None.
    """
    raw = str(text or "").strip()
    if not raw:
        return None

    idx = _INDEX.ensure_fresh()
    title_aliases = _APPROVED_ALIAS_REGISTRY.get_field_aliases("title")
    _refresh_alias_exports()
    nq = _norm(raw)
    if not nq:
        return None

    mf = _norm(hint_media_type)  # media_type filter ("video", "book", ...)

    def _pick(records: list[TitleRecord]) -> TitleRecord:
        if not mf:
            return records[0]
        filtered = [r for r in records if r.media_type == mf]
        return (filtered or records)[0]

    def _make(norm_key: str, confidence: float, kind: MatchKind) -> TitleResolution | None:
        records = idx.title_idx.get(norm_key)
        if not records:
            return None
        rec = _pick(records)
        return TitleResolution(
            canonical=rec.canonical,
            media_type=rec.media_type,
            category=rec.category,
            author=rec.author,
            confidence=confidence,
            match_kind=kind,
        )

    # 1. Exact
    res = _make(nq, 1.0, "exact")
    if res and res.confidence >= min_confidence:
        return res

    # 2. Cross-language alias
    for alt in title_aliases.get(nq, []):
        res = _make(alt, 0.85, "alias")
        if res:
            return res
    # 3. Prefix: either side is a prefix of the other
    best_prefix: TitleResolution | None = None
    for lib_key in idx.title_idx:
        if lib_key.startswith(nq) or nq.startswith(lib_key):
            shorter = min(len(lib_key), len(nq))
            longer  = max(len(lib_key), len(nq))
            conf = 0.75 * (shorter / longer) if longer else 0.75
            if conf < min_confidence:
                continue
            cand = _make(lib_key, conf, "prefix")
            if cand and (best_prefix is None or cand.confidence > best_prefix.confidence):
                best_prefix = cand
    if best_prefix:
        return best_prefix

    # 4. Substring containment (min key length 3)
    best_sub: TitleResolution | None = None
    for lib_key in idx.title_idx:
        if len(lib_key) < 3:
            continue
        if lib_key in nq or nq in lib_key:
            shorter = min(len(lib_key), len(nq))
            longer  = max(len(lib_key), len(nq))
            conf = 0.50 * (shorter / longer) if longer else 0.50
            if conf < min_confidence:
                continue
            cand = _make(lib_key, conf, "substring")
            if cand and (best_sub is None or cand.confidence > best_sub.confidence):
                best_sub = cand
    return best_sub


# ---------------------------------------------------------------------------
# Creator resolution
# ---------------------------------------------------------------------------

def resolve_creator(
    text: str,
    hint_media_type: str = "",
    min_confidence: float = 0.3,
) -> CreatorResolution | None:
    """Resolve a creator name to canonical author + all their works.

    Returns the best match above min_confidence, or None.
    """
    raw = str(text or "").strip()
    if not raw:
        return None

    idx = _INDEX.ensure_fresh()
    creator_aliases = _APPROVED_ALIAS_REGISTRY.get_field_aliases("creator")
    _refresh_alias_exports()
    nq = _norm(raw)
    if not nq:
        return None

    mf = _norm(hint_media_type)

    def _make(norm_key: str, confidence: float, kind: MatchKind) -> CreatorResolution | None:
        entry = idx.author_idx.get(norm_key)
        if not entry:
            return None
        canonical, records = entry
        filtered = [r for r in records if not mf or r.media_type == mf]
        if not filtered:
            filtered = records
        # Determine primary media_type for this creator
        mt_counts: dict[str, int] = {}
        for r in filtered:
            mt_counts[r.media_type] = mt_counts.get(r.media_type, 0) + 1
        mt_hint = max(mt_counts, key=lambda k: mt_counts[k]) if mt_counts else ""
        return CreatorResolution(
            canonical=canonical,
            media_type_hint=mt_hint,
            works=filtered,
            confidence=confidence,
            match_kind=kind,
        )

    # 1. Exact
    res = _make(nq, 1.0, "exact")
    if res and res.confidence >= min_confidence:
        return res

    # 2. Cross-language alias
    for alt in creator_aliases.get(nq, []):
        res = _make(alt, 0.85, "alias")
        if res:
            return res

    # 3. Prefix
    best_prefix: CreatorResolution | None = None
    for lib_key in idx.author_idx:
        if lib_key.startswith(nq) or nq.startswith(lib_key):
            shorter = min(len(lib_key), len(nq))
            longer  = max(len(lib_key), len(nq))
            conf = 0.75 * (shorter / longer) if longer else 0.75
            if conf < min_confidence:
                continue
            cand = _make(lib_key, conf, "prefix")
            if cand and (best_prefix is None or cand.confidence > best_prefix.confidence):
                best_prefix = cand
    if best_prefix:
        return best_prefix

    # 4. Substring (min key length 2 for creator names)
    best_sub: CreatorResolution | None = None
    for lib_key in idx.author_idx:
        if len(lib_key) < 2:
            continue
        if lib_key in nq or nq in lib_key:
            shorter = min(len(lib_key), len(nq))
            longer  = max(len(lib_key), len(nq))
            conf = 0.50 * (shorter / longer) if longer else 0.50
            if conf < min_confidence:
                continue
            cand = _make(lib_key, conf, "substring")
            if cand and (best_sub is None or cand.confidence > best_sub.confidence):
                best_sub = cand
    return best_sub


# ---------------------------------------------------------------------------
# Convenience: bulk-resolve a list of entity strings
# ---------------------------------------------------------------------------

def resolve_entities(
    entities: list[str],
    hint_media_type: str = "",
    min_confidence: float = 0.3,
) -> list[tuple[str, TitleResolution | None]]:
    """Resolve each entity string, returning (original, resolution) pairs."""
    return [
        (
            entity,
            (resolve_media_entities(entity, hint_media_type=hint_media_type, min_confidence=min_confidence).title_hits or [None])[0],
        )
        for entity in entities
        if str(entity).strip()
    ]


def _collect_concept_hints(text: str, hint_media_type: str = "") -> tuple[list[str], list[str]]:
    normalized_media_type = str(hint_media_type or "").strip().lower()
    sources: list[str] = []
    concept_hints: list[str] = []

    def _extend(source_name: str, payload: dict[str, Any]) -> None:
        if not isinstance(payload, dict):
            return
        values: list[str] = []
        for key in ("genre_hints", "entity_hints"):
            for item in payload.get(key) or []:
                text_value = str(item).strip()
                if text_value and text_value not in values:
                    values.append(text_value)
        if not values:
            return
        sources.append(source_name)
        for item in values:
            if item not in concept_hints:
                concept_hints.append(item)

    if normalized_media_type in ("", "video"):
        _extend("video_ontology", _collect_video_hints(text))
    if normalized_media_type in ("", "book"):
        _extend("book_ontology", _collect_book_hints(text))
    return concept_hints, sources


def _select_primary_entity(
    title_res: TitleResolution | None,
    creator_res: CreatorResolution | None,
) -> tuple[dict[str, Any] | None, str]:
    title_conf = title_res.confidence if title_res else 0.0
    creator_conf = creator_res.confidence if creator_res else 0.0
    use_creator = (
        creator_res is not None
        and (
            creator_conf > title_conf
            or (
                creator_conf == title_conf
                and title_res is not None
                and title_res.match_kind in {"prefix", "substring"}
            )
        )
    )
    if use_creator and creator_res is not None:
        return ({
            "kind": "creator",
            "canonical": creator_res.canonical,
            "media_type_hint": creator_res.media_type_hint,
            "works_count": len(creator_res.works),
            "confidence": creator_res.confidence,
            "match_kind": creator_res.match_kind,
        }, "creator_preferred")
    if title_res is not None:
        return ({
            "kind": "title",
            "canonical": title_res.canonical,
            "media_type": title_res.media_type,
            "category": title_res.category,
            "author": title_res.author,
            "confidence": title_res.confidence,
            "match_kind": title_res.match_kind,
        }, "title_preferred")
    return None, "no_primary_entity"


def resolve_media_entities(
    text: str,
    hint_media_type: str = "",
    min_confidence: float = 0.3,
) -> MediaEntityResolution:
    """Resolve a media-facing query surface into title, creator, and concept signals."""
    query = str(text or "").strip()
    title_res = resolve_title(query, hint_media_type=hint_media_type, min_confidence=min_confidence)
    creator_res = resolve_creator(query, hint_media_type=hint_media_type, min_confidence=min_confidence)
    concept_hints, concept_sources = _collect_concept_hints(query, hint_media_type=hint_media_type)
    primary_entity, selection_reason = _select_primary_entity(title_res, creator_res)
    title_alias_hints = _collect_alias_hints(
        "title",
        query,
        str(title_res.canonical if title_res is not None else ""),
    )
    creator_alias_hints = _collect_alias_hints(
        "creator",
        query,
        str(creator_res.canonical if creator_res is not None else ""),
    )
    return MediaEntityResolution(
        query=query,
        title_hits=[title_res] if title_res is not None else [],
        creator_hits=[creator_res] if creator_res is not None else [],
        concept_hints=concept_hints,
        primary_entity=primary_entity,
        evidence={
            "normalized_query": _norm(query),
            "hint_media_type": str(hint_media_type or "").strip().lower(),
            "selection_reason": selection_reason,
            "title_confidence": title_res.confidence if title_res is not None else 0.0,
            "creator_confidence": creator_res.confidence if creator_res is not None else 0.0,
            "title_match_kind": title_res.match_kind if title_res is not None else "",
            "creator_match_kind": creator_res.match_kind if creator_res is not None else "",
            "concept_sources": concept_sources,
            "title_alias_hints": title_alias_hints,
            "creator_alias_hints": creator_alias_hints,
        },
    )


def resolve_title_hit(
    text: str,
    *,
    hint_media_type: str = "",
    min_confidence: float = 0.3,
) -> TitleResolution | None:
    resolution = resolve_media_entities(
        text,
        hint_media_type=hint_media_type,
        min_confidence=min_confidence,
    )
    return (resolution.title_hits or [None])[0]


def resolve_creator_hit(
    text: str,
    *,
    hint_media_type: str = "",
    min_confidence: float = 0.3,
) -> CreatorResolution | None:
    resolution = resolve_media_entities(
        text,
        hint_media_type=hint_media_type,
        min_confidence=min_confidence,
    )
    return (resolution.creator_hits or [None])[0]


def serialize_media_entity_resolution(resolution: MediaEntityResolution | None) -> dict[str, Any]:
    if resolution is None:
        return {
            "query": "",
            "title_hits": [],
            "creator_hits": [],
            "concept_hints": [],
            "primary_entity": {},
            "evidence": {},
        }
    return {
        "query": str(getattr(resolution, "query", "") or "").strip(),
        "title_hits": [
            {
                "canonical": str(getattr(item, "canonical", "") or "").strip(),
                "media_type": str(getattr(item, "media_type", "") or "").strip(),
                "category": str(getattr(item, "category", "") or "").strip(),
                "author": str(getattr(item, "author", "") or "").strip(),
                "confidence": float(getattr(item, "confidence", 0.0) or 0.0),
                "match_kind": str(getattr(item, "match_kind", "") or "").strip(),
            }
            for item in list(getattr(resolution, "title_hits", []) or [])
        ],
        "creator_hits": [
            {
                "canonical": str(getattr(item, "canonical", "") or "").strip(),
                "media_type_hint": str(getattr(item, "media_type_hint", "") or "").strip(),
                "works_count": len(list(getattr(item, "works", []) or [])),
                "confidence": float(getattr(item, "confidence", 0.0) or 0.0),
                "match_kind": str(getattr(item, "match_kind", "") or "").strip(),
            }
            for item in list(getattr(resolution, "creator_hits", []) or [])
        ],
        "concept_hints": [str(item).strip() for item in list(getattr(resolution, "concept_hints", []) or []) if str(item).strip()],
        "primary_entity": dict(getattr(resolution, "primary_entity", {}) or {}),
        "evidence": dict(getattr(resolution, "evidence", {}) or {}),
    }
