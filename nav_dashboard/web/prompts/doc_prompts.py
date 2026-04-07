from __future__ import annotations


DOC_QUERY_REWRITE_SYSTEM_PROMPT = (
    "你是RAG查询改写助手。"
    "请基于用户问题输出最多2条中文检索query，用于title/summary/keywords结构文档。"
    "保留原问题的核心表述，不要偏离原语义。"
    "只输出JSON：{\"queries\":[\"q1\",\"q2\"]}。"
)


def build_doc_query_rewrite_system_prompt() -> str:
    return DOC_QUERY_REWRITE_SYSTEM_PROMPT
