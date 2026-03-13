#!/usr/bin/env python
"""Prune expired web search cache entries.

Run manually or via Windows Task Scheduler (e.g., daily) to keep the SQLite
web cache lean.  Removes entries older than WEB_CACHE_TTL_DAYS (default 7).

Usage:
    python prune_web_cache.py               # prune, print stats
    python prune_web_cache.py --dry-run     # show stats, do not delete
    python prune_web_cache.py --ttl-days 3  # use custom TTL
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

_SCRIPTS_DIR = Path(__file__).resolve().parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

from cache_db import WEB_CACHE_PATH, WEB_CACHE_TTL_DAYS, WebSearchCache


def main() -> None:
    parser = argparse.ArgumentParser(description="Prune expired web search cache entries")
    parser.add_argument(
        "--ttl-days",
        type=int,
        default=WEB_CACHE_TTL_DAYS,
        help=f"TTL in days (default: {WEB_CACHE_TTL_DAYS})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show statistics without deleting anything",
    )
    args = parser.parse_args()

    cache = WebSearchCache(db_path=WEB_CACHE_PATH, ttl_days=args.ttl_days)
    stats = cache.count_stats()
    expired = stats["total"] - stats["valid"]
    print(
        f"Web cache: {stats['total']} total entries, "
        f"{stats['valid']} valid (TTL={args.ttl_days}d), "
        f"{expired} expired"
    )

    if args.dry_run:
        print("Dry-run: no entries deleted.")
        return

    if expired == 0:
        print("Nothing to prune.")
        return

    deleted = cache.prune_expired()
    print(f"Pruned {deleted} expired entries from {WEB_CACHE_PATH}")


if __name__ == "__main__":
    main()
