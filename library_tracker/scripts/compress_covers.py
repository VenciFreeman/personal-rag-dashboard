"""
Cover 图片压缩脚本。

检测库中超过 512KB 的 cover 图片，并压缩至 512KB 以下。
支持命令行和程序接口调用。

使用示例：
    python compress_covers.py                    # 压缩所有超大 cover
    python compress_covers.py --dry-run          # 模拟运行（不修改文件）
    python compress_covers.py --max-size 1000    # 自定义限制（字节）
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from io import BytesIO

try:
    from PIL import Image
except ImportError:
    Image = None

# Add workspace root to path
_WORKSPACE_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKSPACE_ROOT))

from library_tracker.web.settings import COVERS_DIR

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s"
)
logger = logging.getLogger(__name__)

# Default size limit: 512 KB
DEFAULT_MAX_SIZE = 512 * 1024
MIN_QUALITY = 20
INITIAL_QUALITY = 85
QUALITY_STEP = 10


def _compress_image(data: bytes, suffix: str, quality: int = INITIAL_QUALITY) -> bytes | None:
    """
    Compress JPEG image to target quality.
    Returns compressed data or None if compression fails.
    """
    if Image is None:
        logger.warning("PIL not available, skipping image compression")
        return None
    
    try:
        img = Image.open(BytesIO(data))
        
        # Convert RGBA to RGB if necessary
        if img.mode in ("RGBA", "LA", "P"):
            rgb_img = Image.new("RGB", img.size, (255, 255, 255))
            if img.mode == "P":
                img = img.convert("RGBA")
            rgb_img.paste(img, mask=img.split()[-1] if img.mode in ("RGBA", "LA") else None)
            img = rgb_img
        
        fmt = "JPEG"
        save_kwargs: dict[str, object] = {"optimize": True}
        low_suffix = (suffix or "").lower()
        if low_suffix in {".jpg", ".jpeg"}:
            fmt = "JPEG"
            save_kwargs["quality"] = quality
        elif low_suffix == ".webp":
            fmt = "WEBP"
            save_kwargs["quality"] = quality
        elif low_suffix == ".png":
            fmt = "PNG"
            if img.mode not in {"P", "L"}:
                colors = max(32, min(256, int((quality / 100.0) * 256)))
                img = img.convert("P", palette=Image.ADAPTIVE, colors=colors)
            save_kwargs["compress_level"] = 9
        elif low_suffix == ".gif":
            fmt = "GIF"
            if img.mode not in {"P", "L"}:
                colors = max(16, min(256, int((quality / 100.0) * 256)))
                img = img.convert("P", palette=Image.ADAPTIVE, colors=colors)
        else:
            fmt = "JPEG"
            save_kwargs["quality"] = quality

        buffer = BytesIO()
        img.save(buffer, format=fmt, **save_kwargs)
        return buffer.getvalue()
    except Exception as e:  # noqa: BLE001
        logger.error(f"Failed to compress image: {e}")
        return None


def compress_single_cover(cover_path: Path, max_size: int = DEFAULT_MAX_SIZE) -> bool:
    """
    Compress a single cover file if it exceeds max_size.
    Returns True if compression was performed or if file is already small enough.
    """
    if not cover_path.exists():
        logger.warning(f"Cover file not found: {cover_path}")
        return False
    
    file_size = cover_path.stat().st_size
    
    # Check if compression is needed
    if file_size <= max_size:
        logger.debug(f"OK: {cover_path.name} ({file_size} bytes)")
        return True
    
    logger.info(f"Compressing: {cover_path.name} ({file_size} bytes)")
    
    # Read original data
    try:
        data = cover_path.read_bytes()
    except Exception as e:  # noqa: BLE001
        logger.error(f"Failed to read {cover_path}: {e}")
        return False
    
    # Try progressive quality reduction
    quality = INITIAL_QUALITY
    suffix = cover_path.suffix.lower()
    quality_loop = suffix in {".jpg", ".jpeg", ".webp", ".png", ".gif"}

    while True:
        compressed = _compress_image(data, suffix, quality)
        if compressed and len(compressed) <= max_size:
            try:
                cover_path.write_bytes(compressed)
                new_size = len(compressed)
                ratio = round((1 - new_size / file_size) * 100, 1)
                logger.info(f"✓ Compressed {cover_path.name}: {new_size} bytes ({ratio}% reduction)")
                return True
            except Exception as e:  # noqa: BLE001
                logger.error(f"Failed to write compressed file: {e}")
                return False
        if not quality_loop:
            break
        quality -= QUALITY_STEP
        if quality < MIN_QUALITY:
            break
    
    logger.warning(f"Could not compress {cover_path.name} below {max_size} bytes")
    return False


def compress_all_covers(dry_run: bool = False, max_size: int = DEFAULT_MAX_SIZE) -> dict[str, int]:
    """
    Scan all covers and compress those exceeding max_size.
    
    Args:
        dry_run: If True, report what would be done without modifying files
        max_size: Maximum allowed file size in bytes
    
    Returns:
        Statistics dict with keys: processed, compressed, failed
    """
    if not COVERS_DIR.exists():
        logger.warning(f"Covers directory not found: {COVERS_DIR}")
        return {"processed": 0, "compressed": 0, "failed": 0}
    
    logger.info(f"Scanning covers in: {COVERS_DIR}")
    logger.info(f"Size limit: {max_size} bytes ({max_size / 1024:.1f} KB)")
    if dry_run:
        logger.info("DRY RUN mode: no files will be modified")
    
    processed = 0
    compressed = 0
    failed = 0
    
    # Supported image extensions
    image_exts = {".jpg", ".jpeg", ".png", ".webp", ".gif"}
    
    for file_path in sorted(COVERS_DIR.glob("*")):
        if not file_path.is_file():
            continue
        
        if file_path.suffix.lower() not in image_exts:
            logger.debug(f"Skipping non-image: {file_path.name}")
            continue
        
        processed += 1
        file_size = file_path.stat().st_size
        
        if file_size <= max_size:
            logger.debug(f"OK: {file_path.name} ({file_size} bytes)")
            continue
        
        if dry_run:
            logger.info(f"[DRY] Would compress: {file_path.name} ({file_size} bytes)")
            compressed += 1
            continue
        
        # Actual compression
        if compress_single_cover(file_path, max_size):
            compressed += 1
        else:
            failed += 1
    
    stats = {
        "processed": processed,
        "compressed": compressed,
        "failed": failed,
    }
    
    logger.info(f"Done: {processed} processed, {compressed} compressed, {failed} failed")
    return stats


def main():
    parser = argparse.ArgumentParser(
        description="Compress cover images exceeding size limit",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python compress_covers.py
  python compress_covers.py --dry-run
  python compress_covers.py --max-size 1000000
        """
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would be done without modifying files"
    )
    parser.add_argument(
        "--max-size",
        type=int,
        default=DEFAULT_MAX_SIZE,
        help=f"Maximum file size in bytes (default: {DEFAULT_MAX_SIZE / 1024:.0f} KB)"
    )
    
    args = parser.parse_args()
    
    stats = compress_all_covers(dry_run=args.dry_run, max_size=args.max_size)
    
    # Exit with non-zero if there were failures
    sys.exit(0 if stats["failed"] == 0 else 1)


if __name__ == "__main__":
    main()
