from __future__ import annotations

import ast
import json
import os
import re
import threading
from functools import lru_cache
from pathlib import Path
from typing import Any

# Serialize concurrent writes to avoid WinError 5 (Access Denied) on Windows
# when multiple threads race to rename the .tmp file.
_GRAPH_WRITE_LOCK = threading.Lock()

GRAPH_FILE_NAME = "library_knowledge_graph.json"
GRAPH_SCHEMA_VERSION = 3
MAX_ITEM_CONCEPTS = 8
MAX_LLM_CONCEPT_TOPUP = 2
MAX_RELATED_PER_ITEM = 16
MAX_CONCEPT_LEN = 24
MAX_NOTE_LABEL_LEN = 72
MAX_NOTE_EXCERPT_LEN = 48

STRUCTURED_RELATIONS = (
    {"field": "author", "rel": "author", "node_type": "entity", "multi": True},
    {"field": "publisher", "rel": "publisher", "node_type": "entity", "multi": True},
    {"field": "channel", "rel": "channel", "node_type": "entity", "multi": False},
    {"field": "nationality", "rel": "country", "node_type": "concept", "multi": True},
    {"field": "category", "rel": "genre", "node_type": "concept", "multi": True},
)
CONSTRAINT_FIELDS = {"author", "publisher", "channel", "nationality", "category"}
NOTE_HINT_RE = re.compile(r"[。！？!?；;]|(?:，|,).{6,}|(?:^|\s)(?:我|自己|觉得|感觉|喜欢|讨厌|一般|不错|出彩|后来|不过|还是)")
NOTE_MARKER_TERMS = (
    "觉得",
    "感觉",
    "喜欢",
    "讨厌",
    "一般",
    "不错",
    "出彩",
    "后来",
    "不过",
    "还是",
    "小时候",
    "第一次",
    "第一遍",
    "回看",
    "读下来",
    "看下来",
    "玩下来",
)


@lru_cache(maxsize=1)
def _load_concept_ontology() -> dict[str, Any]:
    path = Path(__file__).resolve().parent / "library_concept_ontology.json"
    payload = _safe_read_json(path, default={})
    if not isinstance(payload, dict):
        return {}
    media_type_concepts = payload.get("media_type_concepts")
    if not isinstance(media_type_concepts, dict):
        media_type_concepts = {}
    concept_keyword_aliases = payload.get("concept_keyword_aliases")
    if not isinstance(concept_keyword_aliases, dict):
        concept_keyword_aliases = {}
    return {
        "media_type_concepts": media_type_concepts,
        "concept_keyword_aliases": concept_keyword_aliases,
    }


def _safe_read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _safe_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with _GRAPH_WRITE_LOCK:
        tmp_path = path.with_suffix(path.suffix + ".tmp")
        tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp_path.replace(path)


def _split_tags(raw: Any) -> list[str]:
    if isinstance(raw, (list, tuple, set)):
        out: list[str] = []
        seen: set[str] = set()
        for value in raw:
            for token in _split_tags(value):
                key = token.casefold()
                if key in seen:
                    continue
                seen.add(key)
                out.append(token)
        return out

    text = str(raw or "").strip()
    if not text:
        return []

    if text[:1] in {"[", "(", "{"} and text[-1:] in {"]", ")", "}"}:
        try:
            parsed = ast.literal_eval(text)
        except Exception:
            parsed = None
        if isinstance(parsed, (list, tuple, set)):
            return _split_tags(parsed)

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


def _note_node_key(item_id: str, field: str) -> str:
    return f"note:{item_id}:{field}".strip()


def _truncate_note_label(text: str, limit: int = MAX_NOTE_LABEL_LEN) -> str:
    normalized = re.sub(r"\s+", " ", str(text or "")).strip()
    if len(normalized) <= limit:
        return normalized
    return normalized[: max(8, limit - 1)].rstrip() + "…"


def _build_note_label(*, item_title: str, field: str) -> str:
    field_name = str(field or "note").strip().lower() or "note"
    title = re.sub(r"\s+", " ", str(item_title or "")).strip()
    if title:
        return f"{field_name}: {title}"
    return field_name


def _normalize_note_nodes(nodes: dict[str, dict[str, Any]]) -> bool:
    item_title_by_item_id: dict[str, str] = {}
    for node in nodes.values():
        if not isinstance(node, dict):
            continue
        if str(node.get("type") or "") != "item":
            continue
        attrs = node.get("attrs") if isinstance(node.get("attrs"), dict) else {}
        item_id = str(attrs.get("item_id") or "").strip()
        if not item_id:
            continue
        title = re.sub(r"\s+", " ", str(node.get("label") or "")).strip()
        if title:
            item_title_by_item_id[item_id] = title

    changed = False
    for node in nodes.values():
        if not isinstance(node, dict):
            continue
        if str(node.get("type") or "") != "note":
            continue

        attrs = node.get("attrs") if isinstance(node.get("attrs"), dict) else {}
        field = str(attrs.get("field") or "note").strip() or "note"
        item_id = str(attrs.get("item_id") or "").strip()
        item_title = item_title_by_item_id.get(item_id, "")
        desired_label = _build_note_label(item_title=item_title, field=field)

        current_label = str(node.get("label") or "").strip()
        if current_label != desired_label:
            node["label"] = desired_label
            changed = True

        text = str(attrs.get("text") or "").strip()
        if text:
            desired_excerpt = _truncate_note_label(text, limit=MAX_NOTE_EXCERPT_LEN)
            if str(attrs.get("excerpt") or "") != desired_excerpt:
                attrs["excerpt"] = desired_excerpt
                changed = True

        node["attrs"] = attrs

    return changed


def _looks_like_note_text(text: str) -> bool:
    normalized = re.sub(r"\s+", " ", str(text or "")).strip()
    if not normalized:
        return False
    if len(normalized) > MAX_CONCEPT_LEN:
        return True
    if len(normalized) >= 8 and any(marker in normalized for marker in NOTE_MARKER_TERMS):
        return True
    return bool(NOTE_HINT_RE.search(normalized))


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
        if _looks_like_note_text(text):
            continue
        if len(text) > MAX_CONCEPT_LEN:
            text = text[:MAX_CONCEPT_LEN]
        key = text.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(text)
    return out


def _heuristic_concepts(item: dict[str, Any]) -> list[str]:
    source: list[str] = []

    ontology = _load_concept_ontology()
    media_type = str(item.get("media_type") or "").strip().lower()
    media_type_concepts = ontology.get("media_type_concepts") if isinstance(ontology, dict) else {}
    if isinstance(media_type_concepts, dict):
        source.extend(_split_tags(media_type_concepts.get(media_type)))

    source.extend(_split_tags(item.get("category")))
    source.extend(_split_tags(item.get("nationality")))

    text_fields = [str(item.get("title") or ""), str(item.get("category") or "")]
    merged_text = " ".join(text_fields).casefold()
    aliases = ontology.get("concept_keyword_aliases") if isinstance(ontology, dict) else {}
    if isinstance(aliases, dict) and merged_text:
        for concept, keys in aliases.items():
            concept_text = str(concept or "").strip()
            if not concept_text:
                continue
            key_tokens = _split_tags(keys)
            if not key_tokens:
                continue
            if any(str(token).casefold() in merged_text for token in key_tokens):
                source.append(concept_text)

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
        "nationality": item.get("nationality"),
        "category": item.get("category"),
        "review": item.get("review"),
    }

    system_text = (
        "你是知识图谱抽取器。"
        "从输入条目抽取少量抽象主题概念和 concept-concept 的 related 边。"
        "concept 只能是简短主题词或类型词，例如奇幻、成长、校园、悬疑。"
        "严禁输出作品标题、人名、出版社/平台名、完整评论句子、带主观评价的长短语。"
        "如果没有合适主题，可返回空数组。"
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
    data.setdefault("version", GRAPH_SCHEMA_VERSION)
    data.setdefault("nodes", {})
    data.setdefault("edges", [])
    data.setdefault("processed_items", [])
    if _graph_needs_reset(data):
        return initialize_empty_graph(graph_dir)
    return data


def graph_requires_full_rebuild(graph_dir: Path) -> bool:
    path = graph_dir / GRAPH_FILE_NAME
    data = _safe_read_json(path, default={})
    if not isinstance(data, dict) or not data:
        return True
    data.setdefault("version", GRAPH_SCHEMA_VERSION)
    data.setdefault("nodes", {})
    data.setdefault("edges", [])
    data.setdefault("processed_items", [])
    return _graph_needs_reset(data)


def _graph_needs_reset(data: dict[str, Any]) -> bool:
    version = int(data.get("version") or 0)
    if version != GRAPH_SCHEMA_VERSION:
        return True

    nodes = data.get("nodes", {}) if isinstance(data.get("nodes"), dict) else {}
    edges = data.get("edges", []) if isinstance(data.get("edges"), list) else []
    for node in nodes.values():
        if not isinstance(node, dict):
            continue
        node_type = str(node.get("type") or "").strip()
        if node_type in {"meta", "tag"}:
            return True
    for edge in edges:
        if not isinstance(edge, dict):
            continue
        rel = str(edge.get("rel") or "").strip()
        if rel.startswith("has_") or rel in {"has_tag", "about"}:
            return True
    return False


def initialize_empty_graph(graph_dir: Path) -> dict[str, Any]:
    graph = {
        "version": GRAPH_SCHEMA_VERSION,
        "nodes": {},
        "edges": [],
        "processed_items": [],
    }
    _save_graph(graph_dir, graph)
    return graph


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
    progress_callback=None,
    save_every_items: int = 25,
) -> dict[str, Any]:
    graph = _load_graph(graph_dir)
    nodes = graph.get("nodes", {}) if isinstance(graph.get("nodes"), dict) else {}
    edges = graph.get("edges", []) if isinstance(graph.get("edges"), list) else []
    processed = set(str(x) for x in graph.get("processed_items", []) if str(x).strip())
    _normalize_note_nodes(nodes)

    edge_set: set[str] = set()
    edge_map: dict[str, dict[str, Any]] = {}
    normalized_edges: list[dict[str, Any]] = []
    for edge in edges:
        if not isinstance(edge, dict):
            continue
        src = str(edge.get("src", "")).strip()
        rel = str(edge.get("rel", "")).strip()
        dst = str(edge.get("dst", "")).strip()
        if not (src and rel and dst):
            continue
        attrs = edge.get("attrs") if isinstance(edge.get("attrs"), dict) else {}
        normalized = {"src": src, "rel": rel, "dst": dst, "attrs": dict(attrs)}
        key = _edge_key(src, rel, dst)
        if key in edge_map:
            existing_attrs = edge_map[key].setdefault("attrs", {})
            existing_ids = [str(x).strip() for x in existing_attrs.get("item_ids", []) if str(x).strip()]
            more_ids = [str(x).strip() for x in normalized["attrs"].get("item_ids", []) if str(x).strip()]
            for value in more_ids:
                if value not in existing_ids:
                    existing_ids.append(value)
            if existing_ids:
                existing_attrs["item_ids"] = existing_ids
            continue
        edge_set.add(key)
        edge_map[key] = normalized
        normalized_edges.append(normalized)
    edges = normalized_edges

    added_items = 0
    added_nodes = 0
    added_edges = 0
    save_every = max(1, int(save_every_items or 25))

    def publish_progress(*, force: bool = False) -> None:
        if not force and added_items <= 0:
            return
        graph["nodes"] = nodes
        graph["edges"] = edges
        graph["processed_items"] = sorted(processed)
        _save_graph(graph_dir, graph)
        if callable(progress_callback):
            progress_callback(
                {
                    "items_added": added_items,
                    "nodes_total": len(nodes),
                    "edges_total": len(edges),
                    "processed_item_count": len(processed),
                }
            )

    def upsert_node(node_id: str, node_type: str, label: str, attrs: dict[str, Any] | None = None) -> None:
        nonlocal added_nodes
        if node_id in nodes:
            existing = nodes[node_id] if isinstance(nodes[node_id], dict) else {"id": node_id}
            existing["id"] = node_id
            existing["type"] = node_type
            existing["label"] = label
            existing["attrs"] = attrs or {}
            nodes[node_id] = existing
            return
        nodes[node_id] = {"id": node_id, "type": node_type, "label": label, "attrs": attrs or {}}
        added_nodes += 1

    def add_edge(src: str, rel: str, dst: str, attrs: dict[str, Any] | None = None, support_item_id: str | None = None) -> None:
        nonlocal added_edges
        key = _edge_key(src, rel, dst)
        payload = dict(attrs or {})
        if support_item_id:
            payload["item_ids"] = [str(support_item_id).strip()]
        if key in edge_set:
            existing = edge_map.get(key)
            if not existing:
                return
            existing_attrs = existing.setdefault("attrs", {})
            existing_ids = [str(x).strip() for x in existing_attrs.get("item_ids", []) if str(x).strip()]
            more_ids = [str(x).strip() for x in payload.get("item_ids", []) if str(x).strip()]
            for value in more_ids:
                if value not in existing_ids:
                    existing_ids.append(value)
            if existing_ids:
                existing_attrs["item_ids"] = existing_ids
            for name, value in payload.items():
                if name == "item_ids":
                    continue
                if name not in existing_attrs:
                    existing_attrs[name] = value
            return
        edge_set.add(key)
        edge = {"src": src, "rel": rel, "dst": dst, "attrs": payload}
        edge_map[key] = edge
        edges.append(edge)
        added_edges += 1

    def rebuild_target_item_graph(item_id: str) -> None:
        item_node = _node_key("item", item_id)
        note_nodes = {_note_node_key(item_id, "review")}
        kept_edges: list[dict[str, Any]] = []
        kept_edge_map: dict[str, dict[str, Any]] = {}
        kept_edge_set: set[str] = set()

        for edge in edges:
            if not isinstance(edge, dict):
                continue
            src = str(edge.get("src", "")).strip()
            rel = str(edge.get("rel", "")).strip()
            dst = str(edge.get("dst", "")).strip()
            if not (src and rel and dst):
                continue
            attrs = edge.get("attrs") if isinstance(edge.get("attrs"), dict) else {}
            support_ids = [str(x).strip() for x in attrs.get("item_ids", []) if str(x).strip()]

            drop_edge = src == item_node or dst == item_node or src in note_nodes or dst in note_nodes
            if not drop_edge and item_id in support_ids:
                remaining_ids = [value for value in support_ids if value != item_id]
                if remaining_ids:
                    attrs = dict(attrs)
                    attrs["item_ids"] = remaining_ids
                else:
                    drop_edge = True

            if drop_edge:
                continue

            normalized = {"src": src, "rel": rel, "dst": dst, "attrs": dict(attrs)}
            key = _edge_key(src, rel, dst)
            kept_edges.append(normalized)
            kept_edge_map[key] = normalized
            kept_edge_set.add(key)

        edges[:] = kept_edges
        edge_map.clear()
        edge_map.update(kept_edge_map)
        edge_set.clear()
        edge_set.update(kept_edge_set)
        processed.discard(item_node)
        for note_node in note_nodes:
            nodes.pop(note_node, None)

        referenced_nodes: set[str] = set()
        for edge in edges:
            referenced_nodes.add(str(edge.get("src", "")).strip())
            referenced_nodes.add(str(edge.get("dst", "")).strip())
        removable = [
            node_id
            for node_id, node in nodes.items()
            if node_id not in referenced_nodes and str((node or {}).get("type") or "") != "item"
        ]
        for node_id in removable:
            nodes.pop(node_id, None)

    for item in items:
        item_id = str(item.get("id") or "").strip()
        if not item_id:
            continue
        if target_item_ids and item_id not in target_item_ids:
            continue
        item_node = _node_key("item", item_id)
        if target_item_ids and item_id in target_item_ids:
            rebuild_target_item_graph(item_id)
        if only_missing and item_node in processed:
            continue

        title = str(item.get("title") or item_id).strip()
        upsert_node(item_node, "item", title, {"item_id": item_id, "media_type": item.get("media_type")})

        concept_ids: list[str] = []
        concept_label_to_id: dict[str, str] = {}

        for spec in STRUCTURED_RELATIONS:
            field = str(spec["field"])
            rel = str(spec["rel"])
            node_type = str(spec["node_type"])
            values = _split_tags(item.get(field)) if bool(spec.get("multi")) else [str(item.get(field) or "").strip()]
            for value in values:
                if not value:
                    continue
                node_id = _node_key(node_type, value)
                upsert_node(node_id, node_type, value, {"field": field, "role": rel})
                add_edge(item_node, rel, node_id, {"field": field}, support_item_id=item_id)
                if node_type == "concept":
                    concept_ids.append(node_id)
                    concept_label_to_id.setdefault(value.casefold(), node_id)

        review = str(item.get("review") or "").strip()
        if review:
            note_id = _note_node_key(item_id, "review")
            note_excerpt = _truncate_note_label(review, limit=MAX_NOTE_EXCERPT_LEN)
            upsert_node(
                note_id,
                "note",
                _build_note_label(item_title=title, field="review"),
                {"field": "review", "item_id": item_id, "excerpt": note_excerpt, "text": review},
            )
            add_edge(item_node, "review", note_id, {"field": "review"}, support_item_id=item_id)

        llm_concepts, llm_related = _llm_extract_concepts(item)
        heuristic = _heuristic_concepts(item)
        llm_pool = _normalize_concepts(llm_concepts or [])
        llm_topup: list[str] = []
        heuristic_set = {str(x).casefold() for x in heuristic}
        for concept in llm_pool:
            if str(concept).casefold() in heuristic_set:
                continue
            llm_topup.append(concept)
            if len(llm_topup) >= MAX_LLM_CONCEPT_TOPUP:
                break
        concepts = _normalize_concepts(heuristic + llm_topup)[:MAX_ITEM_CONCEPTS]
        for concept in concepts:
            cid = concept_label_to_id.get(concept.casefold()) or _node_key("concept", concept)
            concept_ids.append(cid)
            concept_label_to_id.setdefault(concept.casefold(), cid)
            upsert_node(cid, "concept", concept, {"field": "theme", "role": "theme"})
            source = "heuristic" if concept.casefold() in heuristic_set else "llm_topup"
            add_edge(item_node, "theme", cid, {"source": source}, support_item_id=item_id)

        deduped_concept_ids: list[str] = []
        seen_concepts: set[str] = set()
        for cid in concept_ids:
            if cid in seen_concepts:
                continue
            seen_concepts.add(cid)
            deduped_concept_ids.append(cid)
        concept_ids = deduped_concept_ids

        relation_count = 0
        for a_raw, b_raw in llm_related:
            if relation_count >= MAX_RELATED_PER_ITEM:
                break
            a = concept_label_to_id.get(str(a_raw or "").strip().casefold())
            b = concept_label_to_id.get(str(b_raw or "").strip().casefold())
            if a == b:
                continue
            if not a or not b or a not in concept_ids or b not in concept_ids:
                continue
            add_edge(a, "related", b, support_item_id=item_id)
            add_edge(b, "related", a, support_item_id=item_id)
            relation_count += 2

        if not llm_related:
            for i, src in enumerate(concept_ids):
                for j in range(i + 1, min(i + 3, len(concept_ids))):
                    dst = concept_ids[j]
                    add_edge(src, "related", dst, support_item_id=item_id)
                    add_edge(dst, "related", src, support_item_id=item_id)

        processed.add(item_node)
        added_items += 1
        if added_items % save_every == 0:
            publish_progress()

    publish_progress(force=True)

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
        node_type = str(node.get("type") or "")
        if node_type not in {"entity", "concept"}:
            continue
        label = str(node.get("label") or "")
        low = label.lower()
        if not any(term in low for term in terms):
            continue

        attrs = node.get("attrs") or {}
        field = str(attrs.get("field") or "").strip()
        if node_type in {"entity", "concept"} and field in CONSTRAINT_FIELDS:
            constraints.setdefault(field, set()).add(label)
        if node_type == "concept":
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
