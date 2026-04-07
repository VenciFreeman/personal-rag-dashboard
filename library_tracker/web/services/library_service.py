from __future__ import annotations

from typing import Any

from . import library_admin_service
from . import library_alias_lifecycle_service
from . import library_contract_service
from . import library_embedding_refresh_service
from . import library_graph_service
from . import library_mutation_service
from . import library_query_service
from . import library_service_core as core

BadItemIdError = core.BadItemIdError
ItemNotFoundError = core.ItemNotFoundError

_extract_keyword_terms = core._extract_keyword_terms
invalidate_search_cache = core.invalidate_search_cache


def get_bootstrap_data(initial_query: str = "", initial_limit: int = 50) -> dict[str, Any]:
    return library_query_service.get_bootstrap_data(initial_query=initial_query, initial_limit=initial_limit)


def get_filter_options() -> dict[str, list[str]]:
    return library_query_service.get_filter_options()


def get_form_suggestions() -> dict[str, list[str]]:
    return library_query_service.get_form_suggestions()


def get_facet_counts(filters: dict[str, list[str]] | None = None) -> dict[str, dict[str, int]]:
    return library_query_service.get_facet_counts(filters)


def get_stats_dashboard(field: str, year: int | None = None) -> dict[str, Any]:
    return library_query_service.get_stats_dashboard(field, year=year)


def get_stats_overview() -> dict[str, Any]:
    return library_query_service.get_stats_overview()


def get_stats_pie(field: str, year: int | None = None) -> dict[str, Any]:
    return library_query_service.get_stats_pie(field, year=year)


def search_items(
    query: str,
    mode: str,
    filters: dict[str, list[str]] | None = None,
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    return library_query_service.search_items(query=query, mode=mode, filters=filters, limit=limit, offset=offset)


def get_item(item_id: str) -> dict[str, Any]:
    return library_query_service.get_item(item_id)


def save_cover_bytes(
    data: bytes,
    content_type: str,
    original_filename: str | None = None,
    title: str | None = None,
    overwrite_path: str | None = None,
) -> str:
    return library_mutation_service.save_cover_bytes(
        data,
        content_type,
        original_filename=original_filename,
        title=title,
        overwrite_path=overwrite_path,
    )


def add_item(item: dict[str, Any]) -> dict[str, Any]:
    return library_mutation_service.add_item(item)


def update_item(item_id: str, patch: dict[str, Any]) -> dict[str, Any]:
    return library_mutation_service.update_item(item_id, patch)


def delete_item(item_id: str) -> dict[str, Any]:
    return library_mutation_service.delete_item(item_id)


def export_library_contract() -> dict[str, Any]:
    return library_contract_service.export_library_contract()


def import_library_contract(payload: dict[str, Any], replace_existing: bool = False) -> dict[str, Any]:
    return library_contract_service.import_library_contract(payload, replace_existing=replace_existing)


def rebuild_library_graph(*, progress_callback=None) -> dict[str, Any]:
    return library_admin_service.rebuild_library_graph(progress_callback=progress_callback)


def sync_missing_library_graph(*, progress_callback=None) -> dict[str, Any]:
    return library_admin_service.sync_missing_library_graph(progress_callback=progress_callback)


def start_graph_job(*, full: bool = False) -> dict[str, Any]:
    return library_admin_service.start_graph_job(full=full)


def get_graph_job(job_id: str) -> dict[str, Any] | None:
    return library_admin_service.get_graph_job(job_id)


def trigger_alias_proposal_background(item_ids: list[str] | None = None) -> None:
    library_admin_service.trigger_alias_proposal_background(item_ids=item_ids)


def trigger_alias_maintenance_background() -> dict[str, Any]:
    return library_admin_service.trigger_alias_maintenance_background()


def enqueue_item_refresh(item_ids: list[str] | None) -> dict[str, Any]:
    return library_admin_service.enqueue_item_refresh(item_ids)


def refresh_pending_embeddings() -> dict[str, Any]:
    return library_admin_service.refresh_pending_embeddings()


def refresh_embeddings_for_item_ids(item_ids: list[str]) -> dict[str, Any]:
    return library_admin_service.refresh_embeddings_for_item_ids(item_ids)
