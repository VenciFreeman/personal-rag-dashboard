"""Pure MediaWiki utility helpers.

These functions have no side effects and no dependencies on global process state.
They can be imported freely by any layer that needs MediaWiki text/URL processing.

Functions consumed by ``agent/support.py`` and future tool-execution modules:
    clean_mediawiki_snippet  — strip HTML/style/script noise from search snippets
    mediawiki_result_score   — relevance score for a search result (title+snippet vs query)
    mediawiki_page_url       — derive the /wiki/ page URL from an API URL + page title
"""

from __future__ import annotations

import html
import re
from typing import Any
from urllib import parse as urlparse


def clean_mediawiki_snippet(value: Any) -> str:
    """Strip HTML markup, style/script blocks, and excess whitespace from a MediaWiki snippet."""
    text = str(value or "")
    text = re.sub(r"<style[\s\S]*?</style>", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"<script[\s\S]*?</script>", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"<!--([\s\S]*?)-->", " ", text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def mediawiki_result_score(query: str, title: str, snippet: str) -> float:
    """Score a MediaWiki search result for relevance to *query*."""
    q = str(query or "").strip().casefold()
    t = str(title or "").strip().casefold()
    score = 0.0
    if q and t == q:
        score += 3.0
    elif q and q in t:
        score += 1.4
    elif t and q and t.replace(" ", "") == q.replace(" ", ""):
        score += 2.0
    if q and q in str(snippet or "").casefold():
        score += 0.5
    return score


def mediawiki_page_url(api_url: str, title: str) -> str:
    """Build the canonical /wiki/<Title> URL from the API endpoint URL and page title."""
    parsed = urlparse.urlparse(api_url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    return f"{base}/wiki/{urlparse.quote(str(title or '').replace(' ', '_'))}"
