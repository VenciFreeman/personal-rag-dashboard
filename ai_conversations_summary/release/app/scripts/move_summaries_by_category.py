"""Move summary markdown files into category folders under `documents/`.

Category source priority:
1) quote-block metadata (`> - **categories**: ...`)
2) frontmatter style `categories`
"""

import argparse
import ast
import re
import shutil
import sys
from pathlib import Path


QUOTE_CATEGORIES_RE = re.compile(
    r"^\s*>\s*-\s*\*\*categories\*\*:\s*(.+?)\s*$",
    re.IGNORECASE,
)
FRONTMATTER_CATEGORIES_RE = re.compile(
    r"^\s*categories\s*:\s*(.*?)\s*$",
    re.IGNORECASE,
)
BULLET_ITEM_RE = re.compile(r"^\s*[-*]\s+(.+?)\s*$")
INVALID_DIR_CHARS_RE = re.compile(r'[<>:"/\\|?*\x00-\x1F]')
TOKEN_RE = re.compile(r"[^\s,\[\]\"']+")


def configure_stdio_utf8() -> None:
    for stream_name in ("stdout", "stderr"):
        stream = getattr(sys, stream_name, None)
        if stream is None:
            continue
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            try:
                reconfigure(encoding="utf-8", errors="replace")
            except Exception:
                pass


def parse_args() -> argparse.Namespace:
    script_dir = Path(__file__).resolve().parent
    root_dir = script_dir.parent

    parser = argparse.ArgumentParser(
        description=(
            "Move markdown files from data/summarize_dir to documents/<first-category>/ "
            "based on each file's categories field."
        )
    )
    parser.add_argument(
        "--input-dir",
        default=str(root_dir / "data" / "summarize_dir"),
        help="Input directory containing summary markdown files.",
    )
    parser.add_argument(
        "--documents-dir",
        default=str(root_dir / "documents"),
        help="Root documents directory where category folders are located/created.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned moves without changing files.",
    )
    return parser.parse_args()


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def parse_inline_categories(raw_value: str) -> list[str]:
    value = raw_value.strip()
    if not value:
        return []

    # Handle list-like values: ["industry-tech", "finance"]
    if value.startswith("["):
        try:
            parsed = ast.literal_eval(value)
            if isinstance(parsed, list):
                return [str(item).strip() for item in parsed if str(item).strip()]
        except Exception:
            pass

    # Handle comma-separated text values.
    if "," in value:
        items = [x.strip().strip('"\'') for x in value.split(",")]
        return [x for x in items if x]

    # Fallback to token extraction.
    token_match = TOKEN_RE.search(value)
    if token_match:
        return [token_match.group(0).strip()]
    return []


def extract_first_category(markdown_text: str) -> str | None:
	# Return first valid category token used as target folder name.
    lines = markdown_text.splitlines()

    # 1) Current output format: > - **categories**: ["industry-tech"]
    for line in lines[:80]:
        m = QUOTE_CATEGORIES_RE.match(line)
        if m:
            categories = parse_inline_categories(m.group(1))
            if categories:
                return categories[0]

    # 2) Frontmatter inline: categories: [a, b] / categories: a,b
    for i, line in enumerate(lines[:80]):
        m = FRONTMATTER_CATEGORIES_RE.match(line)
        if not m:
            continue

        value = m.group(1)
        if value:
            categories = parse_inline_categories(value)
            if categories:
                return categories[0]

        # 3) Frontmatter multiline:
        # categories:
        # - humanities
        # - finance
        for next_line in lines[i + 1 : i + 15]:
            bullet = BULLET_ITEM_RE.match(next_line)
            if bullet:
                token = bullet.group(1).strip().strip('"\'')
                if token:
                    return token
            elif next_line.strip() and not next_line.startswith(" "):
                break

    return None


def sanitize_dir_name(name: str) -> str:
    cleaned = INVALID_DIR_CHARS_RE.sub("-", name).strip().strip(".")
    cleaned = re.sub(r"\s+", "-", cleaned)
    return cleaned if cleaned else "uncategorized"


def build_target_path(target_dir: Path, source_name: str) -> Path:
    candidate = target_dir / source_name
    if not candidate.exists():
        return candidate

    stem = Path(source_name).stem
    suffix = Path(source_name).suffix
    index = 2
    while True:
        candidate = target_dir / f"{stem}_{index}{suffix}"
        if not candidate.exists():
            return candidate
        index += 1


def main() -> None:
	# Files without category metadata are skipped, not force-classified.
    configure_stdio_utf8()
    args = parse_args()

    input_dir = Path(args.input_dir)
    documents_dir = Path(args.documents_dir)

    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    documents_dir.mkdir(parents=True, exist_ok=True)

    files = sorted(p for p in input_dir.iterdir() if p.is_file() and p.suffix.lower() == ".md")
    if not files:
        print(f"No markdown files found in: {input_dir}")
        return

    moved_count = 0
    planned_count = 0
    skipped_count = 0

    for file_path in files:
        text = read_text(file_path)
        category = extract_first_category(text)
        if not category:
            skipped_count += 1
            print(f"Skipped (no categories): {file_path.name}")
            continue

        category_dir = documents_dir / sanitize_dir_name(category)
        target_path = build_target_path(category_dir, file_path.name)
        planned_count += 1

        print(f"Move: {file_path} -> {target_path}")
        if args.dry_run:
            continue

        category_dir.mkdir(parents=True, exist_ok=True)
        shutil.move(str(file_path), str(target_path))
        moved_count += 1

    mode = "DRY-RUN" if args.dry_run else "DONE"
    print(
        f"{mode}. planned={planned_count}, moved={moved_count}, "
        f"skipped={skipped_count}, total={len(files)}"
    )


if __name__ == "__main__":
    main()
