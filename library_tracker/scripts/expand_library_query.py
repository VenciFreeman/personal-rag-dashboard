from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from web.services.library_graph import expand_library_query


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Expand library query using the library graph")
    parser.add_argument("--graph-dir", required=True)
    parser.add_argument("--query", required=True)
    parser.add_argument("--max-expand", type=int, default=6)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    payload = expand_library_query(
        graph_dir=Path(args.graph_dir),
        query=str(args.query or ""),
        max_expand=max(1, int(args.max_expand or 6)),
    )
    print(json.dumps(payload, ensure_ascii=False))


if __name__ == "__main__":
    main()