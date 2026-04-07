from __future__ import annotations

from typing import Any

from . import dashboard_projection_service


def load_rag_index_summary() -> tuple[int, int, int]:
    payload = dashboard_projection_service.load_ai_summary_dashboard_overview()
    index = payload.get("index") if isinstance(payload.get("index"), dict) else {}
    return (
        int(index.get("indexed_documents", 0) or 0),
        int(index.get("changed_pending", 0) or 0),
        int(index.get("source_markdown_files", 0) or 0),
    )


def load_rag_graph_counts() -> tuple[int, int]:
    payload = dashboard_projection_service.load_ai_summary_dashboard_overview()
    index = payload.get("index") if isinstance(payload.get("index"), dict) else {}
    return (
        int(index.get("graph_nodes", 0) or 0),
        int(index.get("graph_edges", 0) or 0),
    )


def load_rag_session_summary() -> tuple[int, int]:
    payload = dashboard_projection_service.load_ai_summary_dashboard_overview()
    sessions = payload.get("sessions") if isinstance(payload.get("sessions"), dict) else {}
    return (
        int(sessions.get("session_count", 0) or 0),
        int(sessions.get("message_count", 0) or 0),
    )


def load_library_summary() -> tuple[int, dict[str, int], int, int, int, int]:
    stats_overview = dashboard_projection_service.load_library_stats_overview()
    raw_by_media = stats_overview.get("total_by_media") if isinstance(stats_overview.get("total_by_media"), dict) else {}
    by_media = {
        media_type: int(raw_by_media.get(media_type, 0) or 0)
        for media_type in ("reading", "video", "music", "game")
    }
    total = int(stats_overview.get("total_all", sum(by_media.values())) or 0)
    this_year = int(stats_overview.get("current_year_all", 0) or 0)
    graph = stats_overview.get("graph") if isinstance(stats_overview.get("graph"), dict) else {}
    vector_rows = int(stats_overview.get("vector_rows", 0) or 0)
    graph_nodes = int(graph.get("nodes", 0) or 0)
    graph_edges = int(graph.get("edges", 0) or 0)
    return total, by_media, vector_rows, graph_nodes, graph_edges, this_year


def load_library_alias_proposal_summary() -> dict[str, Any]:
    payload = dashboard_projection_service.load_library_alias_proposal_summary()
    if not isinstance(payload, dict):
        return {
            "pending_count": 0,
            "approved_count": 0,
            "keep_original_count": 0,
            "updated_at": "",
        }
    return {
        "pending_count": int(payload.get("pending_count", 0) or 0),
        "approved_count": int(payload.get("approved_count", 0) or 0),
        "keep_original_count": int(payload.get("keep_original_count", 0) or 0),
        "updated_at": str(payload.get("updated_at") or ""),
    }


def build_library_graph_quality(
    *,
    total_items: int,
    vector_rows: int,
    safe_div: Any,
    safe_div_capped: Any,
) -> dict[str, Any]:
    stats_overview = dashboard_projection_service.load_library_stats_overview()
    graph = stats_overview.get("graph") if isinstance(stats_overview.get("graph"), dict) else {}
    node_count = int(graph.get("nodes", 0) or 0)
    edge_count = int(graph.get("edges", 0) or 0)
    item_node_count = int(graph.get("item_nodes", 0) or 0)
    processed_item_count = int(graph.get("processed_items", 0) or 0)
    isolated_nodes = int(graph.get("isolated_nodes", 0) or 0)
    if not graph:
        return {
            "item_node_count": 0,
            "processed_item_count": 0,
            "isolated_nodes": 0,
            "isolated_node_rate": None,
            "item_coverage_rate": None,
            "processed_coverage_rate": None,
            "vector_coverage_rate": None,
            "edges_per_node": None,
        }
    return {
        "item_node_count": item_node_count,
        "processed_item_count": processed_item_count,
        "isolated_nodes": isolated_nodes,
        "isolated_node_rate": safe_div(isolated_nodes, node_count),
        "item_coverage_rate": safe_div_capped(item_node_count, total_items),
        "processed_coverage_rate": safe_div_capped(processed_item_count, total_items),
        "vector_coverage_rate": safe_div_capped(item_node_count, vector_rows),
        "edges_per_node": safe_div(edge_count, node_count),
    }
