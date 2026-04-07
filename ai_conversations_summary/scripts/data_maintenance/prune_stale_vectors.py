"""Prune stale vector index entries whose source markdown files no longer exist."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

_SCRIPTS_DIR = Path(__file__).resolve().parents[1]
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))
_WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
if str(_WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKSPACE_ROOT))

from ai_conversations_summary.runtime_paths import DOCUMENTS_DIR, VECTOR_DB_DIR
from rag_vector_index import RAGIndexError, prune_stale_index_entries


def _parse_args() -> argparse.Namespace:
    default_index_dir = os.getenv(
        "AI_SUMMARY_VECTOR_DB_DIR",
        str(VECTOR_DB_DIR),
    )

    # Defaults point to this repository layout to keep the command zero-config.
    parser = argparse.ArgumentParser(description="Prune stale vector index entries")
    parser.add_argument("--documents-dir", default=str(DOCUMENTS_DIR), help="Documents directory")
    parser.add_argument("--index-dir", default=default_index_dir, help="Vector index directory")
    parser.add_argument("--backend", default="auto", choices=["auto", "faiss", "chroma"], help="Vector backend")
    parser.add_argument("--dry-run", action="store_true", help="Preview stale entries without deleting")
    parser.add_argument("--output-json", default="", help="Optional path to write JSON output")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    # Core prune logic lives in rag_vector_index; this file is a thin CLI wrapper.
    stats = prune_stale_index_entries(
        documents_dir=Path(args.documents_dir),
        index_dir=Path(args.index_dir),
        backend=args.backend,
        dry_run=args.dry_run,
    )

    text = json.dumps(stats, ensure_ascii=False, indent=2)
    try:
        print(text)
    except UnicodeEncodeError:
        print(json.dumps(stats, ensure_ascii=True, indent=2))

    if args.output_json:
        # Optional machine-readable output for automation/CI hooks.
        out_path = Path(args.output_json)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(text, encoding="utf-8")


if __name__ == "__main__":
    try:
        main()
    except RAGIndexError as exc:
        print(f"ERROR: {exc}")
        raise SystemExit(2) from exc
