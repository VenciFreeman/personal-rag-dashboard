from __future__ import annotations

from typing import Any

from . import library_service_core as core


def refresh_pending_embeddings() -> dict[str, Any]:
    scanned = 0
    refreshed = 0
    failed = 0
    pending_item_ids: list[str] = []
    all_items = core._iter_all_items()
    item_ids = [str(item.get("id") or "") for item in all_items if str(item.get("id") or "")]
    states = core._load_embedding_states(item_ids)

    with core._embedding_db_conn() as conn:
        pending_writes = 0
        for item in all_items:
            item_id = str(item.get("id") or "")
            if not item_id:
                continue
            scanned += 1
            state = states.get(item_id)
            status = core._embedding_status_value((state or {}).get("embedding_status"))
            if status == 1:
                continue
            pending_item_ids.append(item_id)

            try:
                semantic_text = core._build_semantic_text(item)
                ai_label = core._generate_ai_label(item)
                vec = core._vectorize(semantic_text)
                core._upsert_embedding_state(
                    conn,
                    item_id=item_id,
                    embedding_status=1,
                    ai_label=ai_label,
                    embedding=vec if vec else None,
                )
                refreshed += 1
                pending_writes += 1
            except Exception as exc:
                core._upsert_embedding_state(
                    conn,
                    item_id=item_id,
                    embedding_status=0,
                    ai_label=None,
                    embedding=None,
                )
                failed += 1
                pending_writes += 1
                print(f"[EMBED_REFRESH_WARNING] {item_id}: {exc}")

            if pending_writes >= 20:
                conn.commit()
                pending_writes = 0

        if pending_writes:
            conn.commit()

    full_graph_rebuild = core.library_graph.graph_requires_full_rebuild(core.VECTOR_DB_DIR)
    graph_stats = core.library_graph.sync_library_graph(
        graph_dir=core.VECTOR_DB_DIR,
        items=all_items,
        target_item_ids=None if full_graph_rebuild else (set(pending_item_ids) if pending_item_ids else None),
        only_missing=not full_graph_rebuild,
    )
    return {
        "scanned": scanned,
        "refreshed": refreshed,
        "failed": failed,
        "graph": graph_stats,
    }


def refresh_embeddings_for_item_ids(item_ids: list[str]) -> dict[str, Any]:
    normalized_ids = [str(item_id).strip() for item_id in (item_ids or []) if str(item_id).strip()]
    if not normalized_ids:
        return {"scanned": 0, "refreshed": 0, "failed": 0}

    target_set = set(normalized_ids)
    scanned = 0
    refreshed = 0
    failed = 0
    all_items = core._iter_all_items()
    item_map = {str(item.get("id") or ""): item for item in all_items if str(item.get("id") or "") in target_set}
    states = core._load_embedding_states(list(item_map.keys()))

    with core._embedding_db_conn() as conn:
        pending_writes = 0
        for item_id in normalized_ids:
            item = item_map.get(item_id)
            if not item:
                continue
            scanned += 1
            try:
                semantic_text = core._build_semantic_text(item)
                ai_label = core._generate_ai_label(item)
                vec = core._vectorize(semantic_text)
                core._upsert_embedding_state(
                    conn,
                    item_id=item_id,
                    embedding_status=1,
                    ai_label=ai_label,
                    embedding=vec if vec else None,
                )
                refreshed += 1
                pending_writes += 1
            except Exception as exc:
                core._upsert_embedding_state(
                    conn,
                    item_id=item_id,
                    embedding_status=0,
                    ai_label=(states.get(item_id) or {}).get("ai_label"),
                    embedding=(states.get(item_id) or {}).get("embedding"),
                )
                failed += 1
                pending_writes += 1
                print(f"[EMBED_REFRESH_WARNING] {item_id}: {exc}")

        if pending_writes:
            conn.commit()

    full_graph_rebuild = core.library_graph.graph_requires_full_rebuild(core.VECTOR_DB_DIR)
    graph_stats = core.library_graph.sync_library_graph(
        graph_dir=core.VECTOR_DB_DIR,
        items=all_items,
        target_item_ids=None if full_graph_rebuild else set(normalized_ids),
        only_missing=not full_graph_rebuild,
    )
    return {
        "scanned": scanned,
        "refreshed": refreshed,
        "failed": failed,
        "graph": graph_stats,
    }
