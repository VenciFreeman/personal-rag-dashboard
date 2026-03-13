from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any

GRAPH_FILE_NAME = "library_knowledge_graph.json"
MAX_ITEM_CONCEPTS = 8
MAX_RELATED_PER_ITEM = 16

METADATA_FIELDS = [
    "media_type",
    "author",
    "nationality",
    "category",
    "publisher",
    "channel",
]


def _safe_read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _safe_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _split_tags(raw: Any) -> list[str]:
    text = str(raw or "").strip()
    if not text:
        return []
    parts = [x.strip() for x in re.split(r"[;；，,、\n]+", text) if x.strip()]
    out: list[str] = []
    seen: set[str] = set()
    for p in parts:
        key = p.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(p)
    return out


def _node_key(node_type: str, value: str) -> str:
    return f"{node_type}:{value}".strip()


def _extract_json_object(raw: str) -> dict[str, Any] | None:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        pass
    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        try:
            parsed = json.loads(text[start : end + 1])
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            return None
    return None


def _normalize_concepts(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = re.sub(r"\s+", " ", str(value or "")).strip().strip('"\'` ')
        if len(text) < 2:
            continue
        if len(text) > 40:
            text = text[:40]
        key = text.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(text)
    return out


def _heuristic_concepts(item: dict[str, Any]) -> list[str]:
    source: list[str] = []
    source.extend(_split_tags(item.get("category")))
    source.extend(_split_tags(item.get("author")))
    source.extend(_split_tags(item.get("publisher")))
    source.extend(_split_tags(item.get("nationality")))
    source.extend(_split_tags(item.get("title")))
    source.extend(_split_tags(item.get("review")))
    return _normalize_concepts(source)[:MAX_ITEM_CONCEPTS]


def _llm_extract_concepts(item: dict[str, Any]) -> tuple[list[str], list[tuple[str, str]]]:
    api_url = (os.getenv("LIBRARY_TRACKER_GRAPH_LLM_URL", "http://127.0.0.1:1234/v1") or "").strip()
    model = (os.getenv("LIBRARY_TRACKER_GRAPH_LLM_MODEL", "qwen2.5-7b-instruct") or "").strip()
    api_key = (os.getenv("LIBRARY_TRACKER_GRAPH_LLM_API_KEY", "local") or "").strip() or "local"
    timeout = int(os.getenv("LIBRARY_TRACKER_GRAPH_LLM_TIMEOUT", "90") or "90")

    if not api_url or not model:
        return [], []

    try:
        from openai import OpenAI  # type: ignore
    except Exception:
        return [], []

    payload = {
        "title": item.get("title"),
        "author": item.get("author"),
        "nationality": item.get("nationality"),
        "category": item.get("category"),
        "publisher": item.get("publisher"),
        "channel": item.get("channel"),
        "review": item.get("review"),
    }

    system_text = (
        "你是知识图谱抽取器。"
        "从输入条目抽取概念节点和 related 边。"
        "只返回JSON: {\"concepts\":[...],\"related\":[[\"A\",\"B\"]]}。"
    )

    try:
        client = OpenAI(api_key=api_key, base_url=api_url, timeout=timeout)
        completion = client.chat.completions.create(
            model=model,
            temperature=0.1,
            messages=[
                {"role": "system", "content": system_text},
                {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
            ],
        )
        text = ""
        if completion.choices and completion.choices[0].message:
            text = str(completion.choices[0].message.content or "")
        parsed = _extract_json_object(text)
        if not parsed:
            return [], []

        concepts = _normalize_concepts([str(x) for x in (parsed.get("concepts") or [])])[:MAX_ITEM_CONCEPTS]
        related: list[tuple[str, str]] = []
        for pair in parsed.get("related") or []:
            if not isinstance(pair, (list, tuple)) or len(pair) != 2:
                continue
            a = str(pair[0] or "").strip()
            b = str(pair[1] or "").strip()
            if not a or not b or a == b:
                continue
            related.append((a, b))
        return concepts, related
    except Exception:
        return [], []


def _load_graph(graph_dir: Path) -> dict[str, Any]:
    path = graph_dir / GRAPH_FILE_NAME
    data = _safe_read_json(path, default={})
    if not isinstance(data, dict):
        data = {}
    data.setdefault("version", 1)
    data.setdefault("nodes", {})
    data.setdefault("edges", [])
    data.setdefault("processed_items", [])
    return data


def _save_graph(graph_dir: Path, data: dict[str, Any]) -> None:
    _safe_write_json(graph_dir / GRAPH_FILE_NAME, data)


def _edge_key(src: str, rel: str, dst: str) -> str:
    return f"{src}|{rel}|{dst}"


def sync_library_graph(
    *,
    graph_dir: Path,
    items: list[dict[str, Any]],
    target_item_ids: set[str] | None = None,
    only_missing: bool = True,
) -> dict[str, Any]:
    graph = _load_graph(graph_dir)
    nodes = graph.get("nodes", {}) if isinstance(graph.get("nodes"), dict) else {}
    edges = graph.get("edges", []) if isinstance(graph.get("edges"), list) else []
    processed = set(str(x) for x in graph.get("processed_items", []) if str(x).strip())

    edge_set = set()
    for edge in edges:
        if not isinstance(edge, dict):
            continue
        edge_set.add(_edge_key(str(edge.get("src", "")), str(edge.get("rel", "")), str(edge.get("dst", ""))))

    added_items = 0
    added_nodes = 0
    added_edges = 0

    def upsert_node(node_id: str, node_type: str, label: str, attrs: dict[str, Any] | None = None) -> None:
        nonlocal added_nodes
        if node_id in nodes:
            return
        nodes[node_id] = {"id": node_id, "type": node_type, "label": label, "attrs": attrs or {}}
        added_nodes += 1

    def add_edge(src: str, rel: str, dst: str, attrs: dict[str, Any] | None = None) -> None:
        nonlocal added_edges
        key = _edge_key(src, rel, dst)
        if key in edge_set:
            return
        edge_set.add(key)
        edges.append({"src": src, "rel": rel, "dst": dst, "attrs": attrs or {}})
        added_edges += 1

    for item in items:
        item_id = str(item.get("id") or "").strip()
        if not item_id:
            continue
        if target_item_ids and item_id not in target_item_ids:
            continue
        item_node = _node_key("item", item_id)
        if only_missing and item_node in processed:
            continue

        title = str(item.get("title") or item_id).strip()
        upsert_node(item_node, "item", title, {"item_id": item_id, "media_type": item.get("media_type")})

        # Metadata nodes as constrained, finite graph anchors.
        for field in METADATA_FIELDS:
            values = _split_tags(item.get(field)) if field in {"author", "nationality", "category", "publisher"} else [str(item.get(field) or "").strip()]
            for value in values:
                if not value:
                    continue
                meta_id = _node_key("meta", f"{field}:{value}")
                upsert_node(meta_id, "meta", value, {"field": field})
                add_edge(item_node, f"has_{field}", meta_id)

        tags = _split_tags(item.get("category"))
        for tag in tags:
            tag_id = _node_key("tag", tag)
            upsert_node(tag_id, "tag", tag)
            add_edge(item_node, "has_tag", tag_id)

        llm_concepts, llm_related = _llm_extract_concepts(item)
        concepts = _normalize_concepts((llm_concepts or []) + _heuristic_concepts(item))[:MAX_ITEM_CONCEPTS]
        concept_ids: list[str] = []
        for concept in concepts:
            cid = _node_key("concept", concept)
            concept_ids.append(cid)
            upsert_node(cid, "concept", concept)
            add_edge(item_node, "about", cid)

        relation_count = 0
        for a_raw, b_raw in llm_related:
            if relation_count >= MAX_RELATED_PER_ITEM:
                break
            a = _node_key("concept", a_raw)
            b = _node_key("concept", b_raw)
            if a == b:
                continue
            if a not in concept_ids or b not in concept_ids:
                continue
            add_edge(a, "related", b)
            add_edge(b, "related", a)
            relation_count += 2

        if not llm_related:
            for i, src in enumerate(concept_ids):
                for j in range(i + 1, min(i + 3, len(concept_ids))):
                    dst = concept_ids[j]
                    add_edge(src, "related", dst)
                    add_edge(dst, "related", src)

        processed.add(item_node)
        added_items += 1

    graph["nodes"] = nodes
    graph["edges"] = edges
    graph["processed_items"] = sorted(processed)
    _save_graph(graph_dir, graph)

    return {
        "items_added": added_items,
        "nodes_added": added_nodes,
        "edges_added": added_edges,
        "nodes_total": len(nodes),
        "edges_total": len(edges),
        "graph_path": str(graph_dir / GRAPH_FILE_NAME),
    }


def expand_library_query(*, graph_dir: Path, query: str, max_expand: int = 6) -> dict[str, Any]:
    graph = _load_graph(graph_dir)
    nodes = graph.get("nodes", {}) if isinstance(graph.get("nodes"), dict) else {}
    edges = graph.get("edges", []) if isinstance(graph.get("edges"), list) else []
    terms = [x for x in re.findall(r"[a-z0-9_]+|[\u4e00-\u9fff]{2,}", str(query or "").lower()) if x]

    constraints: dict[str, set[str]] = {}
    seed_concepts: set[str] = set()

    for node_id, node in nodes.items():
        if not isinstance(node, dict):
            continue
        label = str(node.get("label") or "")
        low = label.lower()
        if not any(term in low for term in terms):
            continue

        node_type = str(node.get("type") or "")
        if node_type == "meta":
            field = str((node.get("attrs") or {}).get("field") or "").strip()
            if field:
                constraints.setdefault(field, set()).add(label)
        elif node_type == "concept":
            seed_concepts.add(node_id)

    expanded: list[str] = []
    seen: set[str] = set()
    for edge in edges:
        if not isinstance(edge, dict):
            continue
        if str(edge.get("rel") or "") != "related":
            continue
        src = str(edge.get("src") or "")
        dst = str(edge.get("dst") or "")
        if src not in seed_concepts:
            continue
        node = nodes.get(dst)
        if not isinstance(node, dict):
            continue
        if str(node.get("type") or "") != "concept":
            continue
        label = str(node.get("label") or "").strip()
        if not label:
            continue
        key = label.casefold()
        if key in seen:
            continue
        seen.add(key)
        expanded.append(label)
        if len(expanded) >= max_expand:
            break

    expanded_query = query if not expanded else f"{query}\n相关概念: {' '.join(expanded)}"
    return {
        "expanded_query": expanded_query,
        "expanded_concepts": expanded,
        "constraints": {k: sorted(v) for k, v in constraints.items()},
    }
