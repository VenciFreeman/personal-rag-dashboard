"""entity_resolver.py

Canonical entity resolution for the personal media library.

Public API
----------
    resolve_title(text, hint_media_type="")   -> TitleResolution | None
    resolve_creator(text, hint_media_type="") -> CreatorResolution | None

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
# Cross-language alias table
# Keys and values are already _norm()'d at module load time.
# Each entry maps one normalized form → [list of normalized alternates].
# ---------------------------------------------------------------------------

_RAW_ALIASES: dict[str, list[str]] = {
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

# Pre-normalize all keys and values once at import
_ALIASES: dict[str, list[str]] = {}
for _raw_key, _raw_vals in _RAW_ALIASES.items():
    _nk = _norm(_raw_key)
    if not _nk:
        continue
    _nv = [_norm(v) for v in _raw_vals if _norm(v)]
    if _nv:
        _ALIASES.setdefault(_nk, [])
        for _v in _nv:
            if _v not in _ALIASES[_nk]:
                _ALIASES[_nk].append(_v)

del _raw_key, _raw_vals, _nk, _nv, _v  # clean up module-level loop vars


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


# ---------------------------------------------------------------------------
# Entity index
# ---------------------------------------------------------------------------

_WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
_STRUCTURED_DIR  = _WORKSPACE_ROOT / "library_tracker" / "data" / "structured"


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
        try:
            for p in sorted(_STRUCTURED_DIR.glob("*.json"), key=lambda x: x.name.lower()):
                try:
                    s = p.stat()
                    entries.append((p.name, int(s.st_mtime_ns), int(s.st_size)))
                except Exception:
                    entries.append((p.name, 0, 0))
        except Exception:
            pass
        return tuple(entries)

    def _build(self) -> None:
        title_idx: dict[str, list[TitleRecord]] = {}
        author_map: dict[str, tuple[str, list[TitleRecord]]] = {}

        try:
            paths = sorted(_STRUCTURED_DIR.glob("*.json"), key=lambda p: p.name.lower())
        except Exception:
            paths = []

        for path in paths:
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                rows = payload.get("records", []) if isinstance(payload, dict) else []
            except Exception:
                continue

            for row in rows:
                if not isinstance(row, dict):
                    continue
                title = str(row.get("title") or "").strip()
                if not title:
                    continue

                rec = TitleRecord(
                    canonical=title,
                    media_type=str(row.get("media_type") or "").strip().lower(),
                    category=str(row.get("category") or "").strip(),
                    author=str(row.get("author") or "").strip(),
                    date=str(row.get("date") or "").strip(),
                    rating=row.get("rating"),
                    review=str(row.get("review") or "").strip(),
                )

                nt = _norm(title)
                if nt:
                    title_idx.setdefault(nt, []).append(rec)

                na = _norm(rec.author)
                if na:
                    if na not in author_map:
                        author_map[na] = (rec.author, [])
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
    for alt in _ALIASES.get(nq, []):
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
    for alt in _ALIASES.get(nq, []):
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
        (entity, resolve_title(entity, hint_media_type=hint_media_type, min_confidence=min_confidence))
        for entity in entities
        if str(entity).strip()
    ]
