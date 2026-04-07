"""Compatibility layer for legacy imports.

Internal callers should depend on library_alias_store directly. This module
remains only to avoid breaking older import paths.
"""

from __future__ import annotations

from .library_alias_store import APPROVED_DIR
from .library_alias_store import DEFAULT_BATCH_SIZE
from .library_alias_store import KEEP_DIR
from .library_alias_store import PROPOSAL_DIR
from .library_alias_store import _fingerprint
from .library_alias_store import _normalize_key
from .library_alias_store import _normalize_text
from .library_alias_store import _now_iso
from .library_alias_store import alias_hits_for_item
from .library_alias_store import alias_proposal_file_signature
from .library_alias_store import approved_aliases_for_item
from .library_alias_store import build_generation_queue
from .library_alias_store import clear_alias_proposals
from .library_alias_store import generate_proposals_for_candidates
from .library_alias_store import generate_proposals_for_item_ids
from .library_alias_store import generate_proposals_for_items
from .library_alias_store import get_alias_proposal_summary
from .library_alias_store import list_proposals
from .library_alias_store import resolve_query_aliases
from .library_alias_store import review_proposal

__all__ = [
    "APPROVED_DIR",
    "DEFAULT_BATCH_SIZE",
    "KEEP_DIR",
    "PROPOSAL_DIR",
    "_fingerprint",
    "_normalize_key",
    "_normalize_text",
    "_now_iso",
    "alias_hits_for_item",
    "alias_proposal_file_signature",
    "approved_aliases_for_item",
    "build_generation_queue",
    "clear_alias_proposals",
    "generate_proposals_for_candidates",
    "generate_proposals_for_item_ids",
    "generate_proposals_for_items",
    "get_alias_proposal_summary",
    "list_proposals",
    "resolve_query_aliases",
    "review_proposal",
]