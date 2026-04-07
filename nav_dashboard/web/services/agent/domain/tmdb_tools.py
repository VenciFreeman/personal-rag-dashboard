"""Pure TMDB utility helpers.

These functions have no side effects and no dependencies on global process state
(API keys, HTTP clients, etc.).  They encode domain knowledge about the TMDB
API surface and can be imported freely by any layer that needs TMDB query/scoring.

Functions consumed by ``agent/support.py`` and future tool-execution modules:
    strip_tmdb_query_scaffolding — remove Chinese filler phrases from a query before
                                   sending to TMDB (e.g. "帮我查一下...→ title only")
    guess_tmdb_search_path       — infer the best TMDB endpoint from query text cues
    tmdb_media_url               — build the themoviedb.org canonical page URL
    tmdb_result_score            — combined popularity + title-match score for a TMDB row
"""

from __future__ import annotations

import re
from typing import Any
from urllib import parse as urlparse

from nav_dashboard.web.services.media.media_taxonomy import (
    TMDB_MOVIE_CUES,
    TMDB_PERSON_CUES,
    TMDB_TV_CUES,
)


def strip_tmdb_query_scaffolding(query: str) -> str:
    """Remove common Chinese filler/intent phrases so only the title-like payload remains."""
    text = str(query or "").strip()
    if not text:
        return ""
    text = re.sub(
        r"^(我(?:最近|刚刚|想|想要)?(?:看过|看了|在看|想看|想查|想找)?|帮我|请问|查一下|搜一下)",
        " ",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(
        r"(?:是谁演的|是谁导演的|讲了什么|讲什么|有哪些|有什么|资料|信息|介绍|简介|剧情简介|评价|评分|票房|上映时间|director|cast|actor|actress|writer)$",
        " ",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(
        r"(?:这部|这个|这本|这套)?(?:电影|影片|片子|电视剧|剧集|剧|动漫|动画|番剧|漫画|小说|书)?呢$",
        " ",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(r"\s+", " ", text)
    return text.strip(" ，,。！？?；;：:")


def guess_tmdb_search_path(query: str) -> str:
    """Return the most appropriate TMDB search endpoint path for *query*.

    Returns one of: ``"search/movie"``, ``"search/tv"``, ``"search/person"``,
    or ``"search/multi"`` (default).
    """
    text = str(query or "")
    has_person = any(cue in text for cue in TMDB_PERSON_CUES)
    has_movie = any(cue in text for cue in TMDB_MOVIE_CUES)
    has_tv = any(cue in text for cue in TMDB_TV_CUES)
    if has_movie and not has_tv and not has_person:
        return "search/movie"
    if has_tv and not has_movie and not has_person:
        return "search/tv"
    if has_person and text.startswith(TMDB_PERSON_CUES) and not has_movie and not has_tv:
        return "search/person"
    return "search/multi"


def tmdb_media_url(media_type: str, item_id: Any) -> str:
    """Build the canonical themoviedb.org page URL for a media item."""
    clean_type = str(media_type or "").strip().lower()
    clean_id = str(item_id or "").strip()
    if not clean_id:
        return ""
    if clean_type not in {"movie", "tv", "person"}:
        clean_type = "movie"
    return f"https://www.themoviedb.org/{clean_type}/{urlparse.quote(clean_id)}"


def tmdb_result_score(query: str, row: dict[str, Any]) -> float:
    """Compute a blended score for a TMDB search result row (popularity + title match)."""
    title = str(row.get("title") or row.get("name") or "").strip()
    q = strip_tmdb_query_scaffolding(query).casefold()
    t = title.casefold()
    score = float(row.get("popularity", 0.0) or 0.0) * 0.01
    vote_average = float(row.get("vote_average", 0.0) or 0.0)
    score += vote_average * 0.1
    if q and t == q:
        score += 2.5
    elif q and q in t:
        score += 1.0
    return round(score, 6)
