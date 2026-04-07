from __future__ import annotations

import json
from typing import Any


def build_media_query_classification_prompt(
    *,
    prior_scope: dict[str, Any],
    previous_answer_summary: str,
    query: str,
) -> str:
    return (
        "You are a query understanding planner for a personal knowledge + media agent.\n"
        "Return JSON only.\n"
        "Fields: label, domain, lookup_mode, entities, filters, time_window, ranking, followup_target, needs_comparison, needs_explanation, confidence.\n"
        "label must be one of MEDIA, TECH, OTHER.\n"
        "lookup_mode must be one of general_lookup, entity_lookup, filter_search, concept_lookup.\n"
        "Do not generate rewrites or natural-language restatements. Focus only on structured understanding.\n"
        "If the user asks for evaluation or ranking, prefer local personal-review phrasing.\n\n"
        f"Previous structured state:\n{json.dumps(prior_scope, ensure_ascii=False)}\n\n"
        f"Previous assistant answer summary:\n{previous_answer_summary or 'N/A'}\n\n"
        f"Current question:\n{query}"
    )


def build_media_query_rewrite_prompt(
    *,
    prior_scope: dict[str, Any],
    decision_payload: dict[str, Any],
    question: str,
) -> str:
    return (
        "You rewrite media questions into tool-grade retrieval queries only. Return JSON only.\n"
        "Fields: media_query, doc_query, tmdb_query, web_query.\n"
        "Rules:\n"
        "- media_query must be short, retrieval-grade, and include the concrete title/scope.\n"
        "- Avoid generic phrasing like 请概述一下内容 / 介绍一下 / 那个怎么样.\n"
        "- Preserve entity, filter, and time constraints from the structured decision.\n"
        "- Prefer local personal-review wording for evaluation questions.\n"
        "- For person/creator entity queries that do not mention a specific media type, keep media_query as the person name only — do NOT add category words like 电影, 书籍, 音乐.\n\n"
        f"Previous structured state:\n{json.dumps(prior_scope, ensure_ascii=False)}\n\n"
        f"Current structured decision:\n{json.dumps(decision_payload, ensure_ascii=False)}\n\n"
        f"Current question:\n{question}"
    )
