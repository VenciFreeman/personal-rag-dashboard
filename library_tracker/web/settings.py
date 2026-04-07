from __future__ import annotations

from pathlib import Path

from core_service.runtime_data import app_runtime_root

APP_DIR = Path(__file__).resolve().parent
LIBRARY_DATA_ROOT = app_runtime_root("library_tracker")

STRUCTURED_ROOT = LIBRARY_DATA_ROOT / "structured"
DATA_DIR = STRUCTURED_ROOT
STRUCTURED_ENTITIES_DIR = STRUCTURED_ROOT / "entities"
STRUCTURED_ALIASES_DIR = STRUCTURED_ROOT / "aliases"
STRUCTURED_ALIASES_APPROVED_DIR = STRUCTURED_ALIASES_DIR / "approved"
STRUCTURED_ALIASES_PROPOSAL_DIR = STRUCTURED_ALIASES_DIR / "proposal"
STRUCTURED_ALIASES_KEEP_DIR = STRUCTURED_ALIASES_DIR / "keep_original"
STRUCTURED_CONCEPTS_DIR = STRUCTURED_ROOT / "concepts"
MEDIA_DIR = LIBRARY_DATA_ROOT / "media"
COVERS_DIR = MEDIA_DIR / "covers"
VECTOR_DB_DIR = LIBRARY_DATA_ROOT / "vector_db"

MEDIA_FILES = {
    "book": "reading.json",
    "video": "video.json",
    "music": "music.json",
    "game": "game.json",
}


def get_entity_file_path(media_type: str) -> Path:
    file_name = MEDIA_FILES[str(media_type or "").strip().lower()]
    return STRUCTURED_ENTITIES_DIR / file_name


def get_preferred_entity_file_path(media_type: str) -> Path:
    file_name = MEDIA_FILES[str(media_type or "").strip().lower()]
    return STRUCTURED_ENTITIES_DIR / file_name


def get_alias_bucket_dir(bucket: str) -> Path:
    normalized = str(bucket or "").strip().lower()
    if normalized == "proposal":
        return STRUCTURED_ALIASES_PROPOSAL_DIR
    if normalized == "approved":
        return STRUCTURED_ALIASES_APPROVED_DIR
    if normalized == "keep_original":
        return STRUCTURED_ALIASES_KEEP_DIR
    raise ValueError(f"Unsupported alias bucket: {bucket}")


def get_concept_ontology_path() -> Path:
    return STRUCTURED_CONCEPTS_DIR / "library_concept_ontology.json"


def get_preferred_concept_ontology_path() -> Path:
    return STRUCTURED_CONCEPTS_DIR / "library_concept_ontology.json"
