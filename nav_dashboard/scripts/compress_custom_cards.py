"""
Compress custom card images in nav_dashboard/web/static/custom_cards.

Usage:
  python compress_custom_cards.py
  python compress_custom_cards.py --dry-run
  python compress_custom_cards.py --max-size 524288
"""

from __future__ import annotations

import argparse
import logging
import sys
from io import BytesIO
from pathlib import Path

try:
    from PIL import Image
except ImportError:
    Image = None

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CUSTOM_CARDS_DIR = PROJECT_ROOT / "web" / "static" / "custom_cards"

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_MAX_SIZE = 512 * 1024
MIN_QUALITY = 20
INITIAL_QUALITY = 85
QUALITY_STEP = 10


def _compress_image(data: bytes, suffix: str, quality: int = INITIAL_QUALITY) -> bytes | None:
    if Image is None:
        logger.warning("PIL not available; skipping compression")
        return None

    try:
        img = Image.open(BytesIO(data))
        if img.mode in ("RGBA", "LA", "P"):
            rgb_img = Image.new("RGB", img.size, (255, 255, 255))
            if img.mode == "P":
                img = img.convert("RGBA")
            rgb_img.paste(img, mask=img.split()[-1] if img.mode in ("RGBA", "LA") else None)
            img = rgb_img

        low_suffix = (suffix or "").lower()
        fmt = "JPEG"
        kwargs: dict[str, object] = {"optimize": True}

        if low_suffix in {".jpg", ".jpeg"}:
            fmt = "JPEG"
            kwargs["quality"] = quality
        elif low_suffix == ".webp":
            fmt = "WEBP"
            kwargs["quality"] = quality
        elif low_suffix == ".png":
            fmt = "PNG"
            if img.mode not in {"P", "L"}:
                colors = max(32, min(256, int((quality / 100.0) * 256)))
                img = img.convert("P", palette=Image.ADAPTIVE, colors=colors)
            kwargs["compress_level"] = 9
        elif low_suffix == ".gif":
            fmt = "GIF"
            if img.mode not in {"P", "L"}:
                colors = max(16, min(256, int((quality / 100.0) * 256)))
                img = img.convert("P", palette=Image.ADAPTIVE, colors=colors)
        else:
            fmt = "JPEG"
            kwargs["quality"] = quality

        buf = BytesIO()
        img.save(buf, format=fmt, **kwargs)
        return buf.getvalue()
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to compress image: %s", exc)
        return None


def _resize_image_bytes(data: bytes, scale: float) -> bytes | None:
    if Image is None:
        return None
    try:
        img = Image.open(BytesIO(data))
        width, height = img.size
        new_w = max(1, int(width * scale))
        new_h = max(1, int(height * scale))
        if new_w == width and new_h == height:
            return data
        resized = img.resize((new_w, new_h), Image.LANCZOS)
        buf = BytesIO()
        fmt = (img.format or "PNG").upper()
        if fmt == "JPEG":
            resized = resized.convert("RGB")
            resized.save(buf, format="JPEG", quality=85, optimize=True)
        elif fmt == "WEBP":
            resized.save(buf, format="WEBP", quality=85, optimize=True)
        elif fmt == "GIF":
            resized = resized.convert("P", palette=Image.ADAPTIVE, colors=128)
            resized.save(buf, format="GIF", optimize=True)
        else:
            if resized.mode not in {"P", "L"}:
                resized = resized.convert("P", palette=Image.ADAPTIVE, colors=128)
            resized.save(buf, format="PNG", optimize=True, compress_level=9)
        return buf.getvalue()
    except Exception:
        return None


def compress_single_image(path: Path, max_size: int) -> bool:
    if not path.exists() or not path.is_file():
        return False

    original_size = path.stat().st_size
    if original_size <= max_size:
        return True

    data = path.read_bytes()
    suffix = path.suffix.lower()
    quality = INITIAL_QUALITY

    while quality >= MIN_QUALITY:
        compressed = _compress_image(data, suffix, quality)
        if compressed and len(compressed) <= max_size:
            path.write_bytes(compressed)
            new_size = len(compressed)
            ratio = round((1 - (new_size / original_size)) * 100, 1)
            logger.info("Compressed %s: %d -> %d bytes (%s%%)", path.name, original_size, new_size, ratio)
            return True
        quality -= QUALITY_STEP

    # Fallback: progressively downscale if quality adjustments are insufficient.
    resized_input = data
    for scale in (0.92, 0.85, 0.78, 0.70):
        resized_input = _resize_image_bytes(resized_input, scale)
        if not resized_input:
            break
        quality = INITIAL_QUALITY
        while quality >= MIN_QUALITY:
            compressed = _compress_image(resized_input, suffix, quality)
            if compressed and len(compressed) <= max_size:
                path.write_bytes(compressed)
                new_size = len(compressed)
                ratio = round((1 - (new_size / original_size)) * 100, 1)
                logger.info("Compressed %s: %d -> %d bytes (%s%%)", path.name, original_size, new_size, ratio)
                return True
            quality -= QUALITY_STEP

    logger.warning("Could not compress %s below %d bytes", path.name, max_size)
    return False


def compress_custom_cards(dry_run: bool = False, max_size: int = DEFAULT_MAX_SIZE) -> dict[str, int]:
    if not CUSTOM_CARDS_DIR.exists():
        logger.warning("Custom cards dir not found: %s", CUSTOM_CARDS_DIR)
        return {"processed": 0, "compressed": 0, "failed": 0}

    image_exts = {".jpg", ".jpeg", ".png", ".webp", ".gif"}
    processed = 0
    compressed = 0
    failed = 0

    logger.info("Scanning: %s", CUSTOM_CARDS_DIR)
    logger.info("Size limit: %d bytes", max_size)

    for image_path in sorted(CUSTOM_CARDS_DIR.glob("*")):
        if not image_path.is_file() or image_path.suffix.lower() not in image_exts:
            continue

        processed += 1
        size = image_path.stat().st_size
        if size <= max_size:
            continue

        if dry_run:
            logger.info("[DRY] Would compress: %s (%d bytes)", image_path.name, size)
            compressed += 1
            continue

        if compress_single_image(image_path, max_size):
            compressed += 1
        else:
            failed += 1

    logger.info("Done: processed=%d compressed=%d failed=%d", processed, compressed, failed)
    return {"processed": processed, "compressed": compressed, "failed": failed}


def main() -> None:
    parser = argparse.ArgumentParser(description="Compress nav dashboard custom card images")
    parser.add_argument("--dry-run", action="store_true", help="Report without writing files")
    parser.add_argument("--max-size", type=int, default=DEFAULT_MAX_SIZE, help="Max bytes per image")
    args = parser.parse_args()

    stats = compress_custom_cards(dry_run=args.dry_run, max_size=args.max_size)
    sys.exit(0 if stats["failed"] == 0 else 1)


if __name__ == "__main__":
    main()
