from __future__ import annotations

from pathlib import Path

APP_DIR = Path(__file__).resolve().parent
LIBRARY_ROOT = APP_DIR.parent
DATA_DIR = LIBRARY_ROOT / "data" / "structured"
MEDIA_DIR = LIBRARY_ROOT / "data" / "media"
COVERS_DIR = MEDIA_DIR / "covers"
VECTOR_DB_DIR = LIBRARY_ROOT / "data" / "vector_db"

MEDIA_FILES = {
    "book": "reading.json",
    "video": "video.json",
    "music": "music.json",
    "game": "game.json",
}
