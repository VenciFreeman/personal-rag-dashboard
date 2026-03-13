"""Remove duplicate vector metadata rows and keep FAISS index aligned by row order."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def _parse_args() -> argparse.Namespace:
    root = Path(__file__).resolve().parent.parent
    parser = argparse.ArgumentParser(description="Dedupe FAISS metadata by relative_path")
    parser.add_argument("--index-dir", default=str(root / "data" / "vector_db"), help="Vector index directory")
    parser.add_argument("--keep", choices=["first", "last"], default="last", help="Which duplicate to keep")
    parser.add_argument("--dry-run", action="store_true", help="Preview only")
    return parser.parse_args()


def _row_key(row: dict[str, Any]) -> str:
    rel = str(row.get("relative_path") or "").strip()
    if rel:
        return rel.replace("\\", "/")
    path = str(row.get("file_path") or "").strip()
    return path.replace("\\", "/")


def main() -> None:
    args = _parse_args()

    try:
        import faiss
        import numpy as np
    except ModuleNotFoundError as exc:
        raise SystemExit(f"Missing dependency: {exc}") from exc

    index_dir = Path(args.index_dir)
    meta_path = index_dir / "metadata.json"
    index_path = index_dir / "faiss.index"

    raw_payload = json.loads(meta_path.read_text(encoding="utf-8"))
    manifest: dict[str, Any] = {}
    metadata: list[dict[str, Any]]
    if isinstance(raw_payload, list):
        metadata = [row for row in raw_payload if isinstance(row, dict)]
    elif isinstance(raw_payload, dict):
        manifest_raw = raw_payload.get("manifest")
        docs_raw = raw_payload.get("documents")
        if isinstance(manifest_raw, dict):
            manifest = dict(manifest_raw)
        if not isinstance(docs_raw, list):
            raise SystemExit(f"Invalid metadata format: {meta_path}")
        metadata = [row for row in docs_raw if isinstance(row, dict)]
    else:
        raise SystemExit(f"Invalid metadata format: {meta_path}")

    index = faiss.read_index(str(index_path))
    if int(index.ntotal) != len(metadata):
        raise SystemExit(
            f"Index/metadata row mismatch before dedupe: ntotal={index.ntotal}, metadata={len(metadata)}"
        )

    key_to_indices: dict[str, list[int]] = {}
    for i, row in enumerate(metadata):
        if not isinstance(row, dict):
            continue
        key = _row_key(row)
        key_to_indices.setdefault(key, []).append(i)

    dup_groups = {k: v for k, v in key_to_indices.items() if k and len(v) > 1}
    if args.keep == "first":
        keep_index_set = {indices[0] for indices in key_to_indices.values() if indices}
    else:
        keep_index_set = {indices[-1] for indices in key_to_indices.values() if indices}

    keep_indices = sorted(keep_index_set)
    removed_indices = [i for i in range(len(metadata)) if i not in keep_index_set]

    payload = {
        "rows_before": len(metadata),
        "rows_after": len(keep_indices),
        "removed_rows": len(removed_indices),
        "duplicate_groups": len(dup_groups),
        "sample_duplicate_keys": list(dup_groups.keys())[:20],
        "dry_run": bool(args.dry_run),
    }

    if args.dry_run or not removed_indices:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return

    vectors = np.zeros((len(metadata), int(index.d)), dtype="float32")
    for i in range(len(metadata)):
        vectors[i] = index.reconstruct(i)

    kept_vectors = vectors[keep_indices]
    new_index = faiss.IndexFlatIP(int(index.d))
    new_index.add(kept_vectors)

    new_metadata = [metadata[i] for i in keep_indices]

    faiss.write_index(new_index, str(index_path))
    out_payload = {
        "manifest": manifest,
        "documents": new_metadata,
    }
    meta_path.write_text(json.dumps(out_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    payload["index_ntotal_after"] = int(new_index.ntotal)
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
