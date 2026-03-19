from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any

WORKSPACE_ROOT = Path(__file__).resolve().parent.parent.parent
if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))

from core_service.config import get_settings

GRAPH_FILE_NAME = "knowledge_graph_rag.json"
MAX_TAGS_PER_DOC = 12
MAX_CONCEPTS_PER_DOC = 8
MAX_RELATED_PER_CONCEPT = 6
_CORE_SETTINGS = get_settings()


def _split_terms(raw: str | list[str] | None) -> list[str]:
    if isinstance(raw, list):
        values = [str(x).strip() for x in raw]
    else:
        values = re.split(r"[;,，、|\n]+", str(raw or ""))
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        token = str(value or "").strip().strip('"\'` ')
        if not token:
            continue
        lowered = token.casefold()
        if lowered in seen:
            continue
        seen.add(lowered)
        out.append(token)
    return out


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


def _node_key(node_type: str, value: str) -> str:
    return f"{node_type}:{value}".strip()


def _normalize_concepts(raw: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for token in raw:
        text = re.sub(r"\s+", " ", str(token or "")).strip().strip('"\'` ')
        if len(text) < 2:
            continue
        if len(text) > 40:
            text = text[:40]
        lowered = text.casefold()
        if lowered in seen:
            continue
        seen.add(lowered)
        out.append(text)
    return out


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
        snippet = text[start : end + 1]
        try:
            parsed = json.loads(snippet)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            return None
    return None


def _heuristic_concepts(title: str, summary: str, topic: str, keywords: list[str]) -> list[str]:
    merged: list[str] = []
    merged.extend(_split_terms(topic))
    merged.extend(keywords)
    merged.extend(_split_terms(title))
    merged.extend(_split_terms(summary))

    # Keep phrases that are likely concepts and avoid very generic short words.
    filtered: list[str] = []
    for token in merged:
        if len(token) < 2:
            continue
        if re.fullmatch(r"[0-9\W_]+", token):
            continue
        filtered.append(token)
    return _normalize_concepts(filtered)[:MAX_CONCEPTS_PER_DOC]


def _llm_extract_concepts(
    *,
    title: str,
    summary: str,
    topic: str,
    keywords: list[str],
) -> tuple[list[str], list[tuple[str, str]]]:
    api_url = (os.getenv("AI_SUMMARY_GRAPH_LLM_URL", "http://127.0.0.1:1234/v1") or "").strip()
    model = (os.getenv("AI_SUMMARY_GRAPH_LLM_MODEL", "") or _CORE_SETTINGS.local_llm_model).strip()
    api_key = (os.getenv("AI_SUMMARY_GRAPH_LLM_API_KEY", "local") or "").strip() or "local"
    timeout = int(os.getenv("AI_SUMMARY_GRAPH_LLM_TIMEOUT", "90") or "90")

    if not api_url or not model:
        return [], []

    try:
        from openai import OpenAI  # type: ignore
    except Exception:
        return [], []

    prompt = {
        "title": title,
        "summary": summary,
        "topic": topic,
        "keywords": keywords,
    }

    system_text = (
        "你是知识图谱抽取器。"
        "从输入文档摘要中提取概念节点，并给出概念间 related 边。"
        "只返回JSON对象，格式为: "
        "{\"concepts\":[\"...\"],\"related\":[[\"概念A\",\"概念B\"]]}。"
        "不要输出解释。"
    )

    try:
        client = OpenAI(api_key=api_key, base_url=api_url, timeout=timeout)
        completion = client.chat.completions.create(
            model=model,
            temperature=0.1,
            messages=[
                {"role": "system", "content": system_text},
                {"role": "user", "content": json.dumps(prompt, ensure_ascii=False)},
            ],
        )
        text = ""
        if completion.choices and completion.choices[0].message:
            text = str(completion.choices[0].message.content or "")
        parsed = _extract_json_object(text)
        if not parsed:
            return [], []

        concepts = _normalize_concepts([str(x) for x in parsed.get("concepts", [])])
        related: list[tuple[str, str]] = []
        for pair in parsed.get("related", []) or []:
            if not isinstance(pair, (list, tuple)) or len(pair) != 2:
                continue
            a = str(pair[0] or "").strip()
            b = str(pair[1] or "").strip()
            if not a or not b or a == b:
                continue
            related.append((a, b))
        return concepts[:MAX_CONCEPTS_PER_DOC], related
    except Exception:
        return [], []


def _load_graph(index_dir: Path) -> dict[str, Any]:
    graph_path = index_dir / GRAPH_FILE_NAME
    data = _safe_read_json(graph_path, default={})
    if not isinstance(data, dict):
        data = {}
    data.setdefault("version", 1)
    data.setdefault("nodes", {})
    data.setdefault("edges", [])
    data.setdefault("processed_documents", [])
    return data


def _save_graph(index_dir: Path, data: dict[str, Any]) -> None:
    graph_path = index_dir / GRAPH_FILE_NAME
    _safe_write_json(graph_path, data)


def _build_edge_key(src: str, rel: str, dst: str) -> str:
    return f"{src}|{rel}|{dst}"


def sync_rag_graph(
    index_dir: Path,
    *,
    only_missing: bool = True,
    use_llm: bool = True,
    prune_missing: bool = False,
) -> dict[str, Any]:
    meta_path = index_dir / "metadata.json"
    payload = _safe_read_json(meta_path, default=[])
    if isinstance(payload, list):
        metadata = payload
    elif isinstance(payload, dict):
        docs = payload.get("documents")
        if not isinstance(docs, list):
            raise RuntimeError(f"Invalid metadata payload: {meta_path}")
        metadata = docs
    else:
        raise RuntimeError(f"Invalid metadata: {meta_path}")

    graph = _load_graph(index_dir)
    nodes: dict[str, dict[str, Any]] = graph.get("nodes", {}) if isinstance(graph.get("nodes"), dict) else {}
    edges: list[dict[str, Any]] = graph.get("edges", []) if isinstance(graph.get("edges"), list) else []
    processed = set(str(x) for x in graph.get("processed_documents", []) if str(x).strip())

    pruned_documents = 0
    pruned_nodes = 0
    pruned_edges = 0

    if prune_missing:
        valid_doc_ids: set[str] = set()
        for row in metadata:
            if not isinstance(row, dict):
                continue
            rel_path = str(row.get("relative_path") or row.get("file_path") or "").strip()
            if not rel_path:
                continue
            valid_doc_ids.add(_node_key("document", rel_path))

        stale_doc_ids = {
            str(node_id)
            for node_id, node in nodes.items()
            if isinstance(node, dict)
            and str(node.get("type") or "") == "document"
            and str(node_id) not in valid_doc_ids
        }

        if stale_doc_ids:
            before_edges = len(edges)
            edges = [
                edge
                for edge in edges
                if isinstance(edge, dict)
                and str(edge.get("src") or "") not in stale_doc_ids
                and str(edge.get("dst") or "") not in stale_doc_ids
            ]
            pruned_edges += max(0, before_edges - len(edges))

            for node_id in stale_doc_ids:
                if node_id in nodes:
                    nodes.pop(node_id, None)
            pruned_documents = len(stale_doc_ids)
            pruned_nodes += len(stale_doc_ids)
            processed = {x for x in processed if x not in stale_doc_ids}

            # Remove orphan non-document nodes after stale document deletion.
            connected_ids: set[str] = set()
            for edge in edges:
                if not isinstance(edge, dict):
                    continue
                connected_ids.add(str(edge.get("src") or ""))
                connected_ids.add(str(edge.get("dst") or ""))
            orphan_ids = [
                str(node_id)
                for node_id, node in nodes.items()
                if isinstance(node, dict)
                and str(node.get("type") or "") != "document"
                and str(node_id) not in connected_ids
            ]
            for node_id in orphan_ids:
                nodes.pop(node_id, None)
            pruned_nodes += len(orphan_ids)

    edge_set = set()
    for edge in edges:
        if not isinstance(edge, dict):
            continue
        edge_set.add(_build_edge_key(str(edge.get("src", "")), str(edge.get("rel", "")), str(edge.get("dst", ""))))

    added_docs = 0
    added_nodes = 0
    added_edges = 0

    def upsert_node(node_id: str, node_type: str, label: str, attrs: dict[str, Any] | None = None) -> None:
        nonlocal added_nodes
        if node_id in nodes:
            return
        nodes[node_id] = {
            "id": node_id,
            "type": node_type,
            "label": label,
            "attrs": attrs or {},
        }
        added_nodes += 1

    def add_edge(src: str, rel: str, dst: str, attrs: dict[str, Any] | None = None) -> None:
        nonlocal added_edges
        key = _build_edge_key(src, rel, dst)
        if key in edge_set:
            return
        edge_set.add(key)
        edges.append({"src": src, "rel": rel, "dst": dst, "attrs": attrs or {}})
        added_edges += 1

    for row in metadata:
        if not isinstance(row, dict):
            continue
        rel_path = str(row.get("relative_path") or row.get("file_path") or "").strip()
        if not rel_path:
            continue
        doc_id = _node_key("document", rel_path)
        if only_missing and doc_id in processed:
            continue

        title = str(row.get("title") or "").strip()
        summary = str(row.get("summary") or "").strip()
        topic = str(row.get("topic") or "").strip()
        keywords = _split_terms(row.get("keywords"))[:MAX_TAGS_PER_DOC]

        upsert_node(doc_id, "document", title or rel_path, {"relative_path": rel_path})

        for tag in keywords:
            tag_id = _node_key("tag", tag)
            upsert_node(tag_id, "tag", tag)
            add_edge(doc_id, "has_tag", tag_id)

        if use_llm:
            llm_concepts, llm_related = _llm_extract_concepts(
                title=title,
                summary=summary,
                topic=topic,
                keywords=keywords,
            )
        else:
            llm_concepts, llm_related = [], []
        concepts = _normalize_concepts((llm_concepts or []) + _heuristic_concepts(title, summary, topic, keywords))
        concepts = concepts[:MAX_CONCEPTS_PER_DOC]

        concept_ids: list[str] = []
        for concept in concepts:
            cid = _node_key("concept", concept)
            concept_ids.append(cid)
            upsert_node(cid, "concept", concept)
            add_edge(doc_id, "about", cid)

        # Keep relation density bounded: only relations among concepts touched by this new doc.
        relation_budget = MAX_RELATED_PER_CONCEPT * max(1, len(concepts))
        used = 0
        for a_raw, b_raw in llm_related:
            if used >= relation_budget:
                break
            a = _node_key("concept", a_raw)
            b = _node_key("concept", b_raw)
            if a == b:
                continue
            if a not in concept_ids or b not in concept_ids:
                continue
            add_edge(a, "related", b)
            add_edge(b, "related", a)
            used += 2

        if not llm_related:
            # Fallback: sparse clique-like links only for nearby concept pairs.
            for i, src in enumerate(concept_ids):
                for j in range(i + 1, min(i + 3, len(concept_ids))):
                    dst = concept_ids[j]
                    add_edge(src, "related", dst)
                    add_edge(dst, "related", src)

        processed.add(doc_id)
        added_docs += 1

    graph["nodes"] = nodes
    graph["edges"] = edges
    graph["processed_documents"] = sorted(processed)
    _save_graph(index_dir, graph)

    return {
        "documents_scanned": len(metadata),
        "documents_added": added_docs,
        "nodes_total": len(nodes),
        "edges_total": len(edges),
        "nodes_added": added_nodes,
        "edges_added": added_edges,
        "prune_missing": bool(prune_missing),
        "documents_pruned": pruned_documents,
        "nodes_pruned": pruned_nodes,
        "edges_pruned": pruned_edges,
        "graph_path": str(index_dir / GRAPH_FILE_NAME),
    }


def _query_terms(query: str) -> list[str]:
    chunks = re.findall(r"[a-z0-9_]+|[\u4e00-\u9fff]{2,}", str(query or "").lower())
    return [x.strip() for x in chunks if x.strip()]


def expand_query_by_graph(index_dir: Path, query: str, *, max_expand: int = 6) -> dict[str, Any]:
    graph = _load_graph(index_dir)
    nodes = graph.get("nodes", {}) if isinstance(graph.get("nodes"), dict) else {}
    edges = graph.get("edges", []) if isinstance(graph.get("edges"), list) else []
    if not nodes or not edges:
        return {"expanded_query": query, "seed_concepts": [], "expanded_concepts": []}

    concept_nodes = {
        node_id: node for node_id, node in nodes.items() if isinstance(node, dict) and str(node.get("type")) == "concept"
    }
    terms = _query_terms(query)

    seeds: list[str] = []
    seed_ids: set[str] = set()
    for node_id, node in concept_nodes.items():
        label = str(node.get("label") or "")
        low = label.lower()
        if any(term and term in low for term in terms):
            if node_id not in seed_ids:
                seed_ids.add(node_id)
                seeds.append(label)

    if not seed_ids:
        return {"expanded_query": query, "seed_concepts": [], "expanded_concepts": []}

    neighbors: list[str] = []
    seen_neighbor_ids: set[str] = set()
    for edge in edges:
        if not isinstance(edge, dict):
            continue
        if str(edge.get("rel") or "") != "related":
            continue
        src = str(edge.get("src") or "")
        dst = str(edge.get("dst") or "")
        if src not in seed_ids:
            continue
        if dst in seed_ids or dst in seen_neighbor_ids:
            continue
        node = concept_nodes.get(dst)
        if not node:
            continue
        label = str(node.get("label") or "").strip()
        if not label:
            continue
        seen_neighbor_ids.add(dst)
        neighbors.append(label)
        if len(neighbors) >= max_expand:
            break

    if not neighbors:
        return {"expanded_query": query, "seed_concepts": seeds[:max_expand], "expanded_concepts": []}

    expanded_query = f"{query}\n相关概念: {' '.join(neighbors)}"
    return {
        "expanded_query": expanded_query,
        "seed_concepts": seeds[:max_expand],
        "expanded_concepts": neighbors,
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="RAG knowledge graph sync/expand")
    root = Path(__file__).resolve().parent.parent
    default_index_dir = os.getenv(
        "AI_SUMMARY_VECTOR_DB_DIR",
        str(root.parent / "core_service" / "data" / "vector_db"),
    )
    parser.add_argument("--index-dir", default=default_index_dir)
    parser.add_argument("--sync-missing", action="store_true")
    parser.add_argument("--prune-missing", action="store_true")
    parser.add_argument("--no-llm", action="store_true", help="Disable per-document LLM concept extraction for faster sync")
    parser.add_argument("--query", default="")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    index_dir = Path(args.index_dir)
    if args.sync_missing:
        stats = sync_rag_graph(
            index_dir,
            only_missing=True,
            prune_missing=bool(args.prune_missing),
            use_llm=not bool(args.no_llm),
        )
        print(json.dumps(stats, ensure_ascii=False, indent=2))
        return
    if str(args.query or "").strip():
        payload = expand_query_by_graph(index_dir, str(args.query))
        print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
