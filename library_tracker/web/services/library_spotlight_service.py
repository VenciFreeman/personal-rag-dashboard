from __future__ import annotations

import json
import re
from typing import Any

from . import library_analysis_core as shared


def _json_compact(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def _context_for_prompt(context: dict[str, Any]) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "kind": context.get("kind"),
        "period": context.get("period_label"),
        "previous_period": context.get("previous_period_key"),
        "overview_fact_text": context.get("overview_fact_text") or "",
        "comparison_rows": context.get("comparison_rows") or [],
        "distribution": [],
        "rating_rows": context.get("rating_rows") or [],
        "representatives": [],
        "focus_candidates": context.get("focus_candidates") or [],
    }
    for media_type in shared.MEDIA_TYPES:
        distribution = context["distribution"].get(media_type, {})
        representative = context["representatives"].get(media_type) or {}
        payload["distribution"].append(
            {
                "media_type": media_type,
                "media_label": shared.MEDIA_LABELS[media_type],
                "nationality": shared._format_counter_details(distribution.get("nationality_details") or []),
                "nationality_concentration": shared._concentration_label(distribution.get("nationality_details") or []),
                "category": shared._format_counter_details(distribution.get("category_details") or []),
                "category_concentration": shared._concentration_label(distribution.get("category_details") or []),
                "channel": shared._format_counter_details(distribution.get("channel_details") or []),
                "channel_concentration": shared._concentration_label(distribution.get("channel_details") or []),
                "author": shared._format_counter_details(distribution.get("author_details") or []),
                "author_concentration": shared._concentration_label(distribution.get("author_details") or []),
            }
        )
        payload["representatives"].append(
            {
                "media_type": media_type,
                "media_label": shared.MEDIA_LABELS[media_type],
                "title": representative.get("title") or "—",
                "rating": representative.get("rating"),
                "review": shared.truncate_text_by_chars(str(representative.get("review") or "").strip(), 180) or "本期没有代表条目。",
                "external_reference": context["external_references"].get(media_type, {}),
                "representative_basis": shared._representative_basis(
                    shared.MEDIA_LABELS[media_type],
                    representative,
                    distribution,
                    context["current_rows_by_media"].get(media_type, []),
                ),
            }
        )
    if context.get("kind") == shared.REPORT_KIND_YEARLY:
        payload["quarter_inputs"] = context.get("quarter_inputs") or []
        payload["trend_rows"] = context.get("trend_rows") or []
        payload["annual_trend_signals"] = context.get("annual_trend_signals") or []
    return payload


def _fit_prompt_payload(kind: str, payload: dict[str, Any]) -> dict[str, Any]:
    target = int(shared.PROMPT_PAYLOAD_CHAR_BUDGET.get(kind, 4200))
    compact = json.loads(json.dumps(payload, ensure_ascii=False))
    review_limit = 160 if kind == shared.REPORT_KIND_QUARTERLY else 120
    reference_limit = 120 if kind == shared.REPORT_KIND_QUARTERLY else 90

    for item in compact.get("representatives", []):
        item["review"] = shared.truncate_text_by_chars(str(item.get("review") or "").strip(), review_limit)
        reference = item.get("external_reference") if isinstance(item.get("external_reference"), dict) else {}
        if reference:
            reference["intro"] = shared.truncate_text_by_chars(str(reference.get("intro") or "").strip(), reference_limit)
            reference["background"] = shared.truncate_text_by_chars(str(reference.get("background") or "").strip(), 80)
            reference["extra"] = shared.truncate_text_by_chars(str(reference.get("extra") or "").strip(), 50)
            item["external_reference"] = reference

    if len(_json_compact(compact)) > target:
        for item in compact.get("representatives", []):
            reference = item.get("external_reference") if isinstance(item.get("external_reference"), dict) else {}
            if reference:
                reference["intro"] = shared.truncate_text_by_chars(str(reference.get("intro") or "").strip(), 60)
                reference["background"] = shared.truncate_text_by_chars(str(reference.get("background") or "").strip(), 40)
                item["external_reference"] = reference

    if len(_json_compact(compact)) > target:
        for item in compact.get("representatives", []):
            item["review"] = shared.truncate_text_by_chars(str(item.get("review") or "").strip(), 80)

    if len(_json_compact(compact)) > target and isinstance(compact.get("quarter_inputs"), list):
        compact["quarter_inputs"] = compact["quarter_inputs"][:4]
    if len(_json_compact(compact)) > target and isinstance(compact.get("trend_rows"), list):
        compact["trend_rows"] = compact["trend_rows"][:8]
    return compact


def build_default_sections(context: dict[str, Any]) -> dict[str, Any]:
    kind = context.get("kind")
    distribution_lines = []
    for media_type in shared.MEDIA_TYPES:
        dist = (context.get("distribution") or {}).get(media_type, {})
        distribution_lines.append(
            f"- {shared.MEDIA_LABELS[media_type]}：{shared._bold_leading_label(shared._distribution_dimension_line(dist.get('category_details') or [], '题材', annual=kind == shared.REPORT_KIND_YEARLY))}"
        )
    rating_rows = context.get("rating_rows") or []
    highest = max(rating_rows, key=lambda row: row["current_avg_rating"] if row["current_avg_rating"] is not None else -1, default=None)
    most_volatile = max(rating_rows, key=lambda row: row.get("rating_stddev") or -1, default=None)
    structure_features = []
    for media_type in shared.MEDIA_TYPES:
        dist = (context.get("distribution") or {}).get(media_type, {})
        structure_features.append(
            f"{('年度' if kind == shared.REPORT_KIND_YEARLY else '本期')}记录在{shared.MEDIA_LABELS[media_type]}上更集中于 {dist.get('top_category', '—')} / {dist.get('top_nationality', '—')}，{shared._concentration_note('题材', dist.get('category_details') or [], annual=kind == shared.REPORT_KIND_YEARLY)}"
        )
    highlights: dict[str, dict[str, str]] = {}
    for media_type in shared.MEDIA_TYPES:
        representative = (context.get("representatives") or {}).get(media_type) or {}
        highlights[media_type] = {
            "evaluation_summary": shared._excerpt(representative.get("review"), 80),
            "representative_reason": shared._representative_basis(
                shared.MEDIA_LABELS[media_type],
                representative,
                (context.get("distribution") or {}).get(media_type, {}),
                (context.get("current_rows_by_media") or {}).get(media_type, []),
            ),
        }
    return {
        "overview_text": "\n".join(
            [
                f"- {shared._top_media_breakdown_text(context.get('comparison_rows') or [], limit=2, annual=True)}" if kind == shared.REPORT_KIND_YEARLY else f"- {context.get('overview_fact_text') or ''}",
                f"- {shared._most_significant_change_text(context.get('comparison_rows') or [], annual=kind == shared.REPORT_KIND_YEARLY)}",
                f"- {shared._standout_structure_text(context, annual=kind == shared.REPORT_KIND_YEARLY)}",
            ]
        ).strip(),
        "distribution_text": "\n".join(distribution_lines),
        "rating_text": (
            f"{('年度' if kind == shared.REPORT_KIND_YEARLY else '本期')}平均评分最高的是 {highest['media_label']}，整体表现最稳的是 {min(rating_rows, key=lambda row: row.get('rating_stddev') if row.get('rating_stddev') is not None else 999).get('media_label', '—')}；波动更明显的是 {most_volatile['media_label']}。"
            if highest and most_volatile and rating_rows
            else f"{('年度' if kind == shared.REPORT_KIND_YEARLY else '本期')}评分样本有限，先以表格事实为主。"
        ),
        "structure_features": structure_features[:4],
        "highlights": highlights,
        "next_focus": shared._default_next_focus(context),
        "annual_trend_text": "" if context.get("kind") != shared.REPORT_KIND_YEARLY else "\n".join(f"- {item}" for item in (context.get("annual_trend_signals") or [])) or "- 年内节奏整体比较平均，暂时没有特别强的阶段切换。",
        "yearly_category_summaries": {media_type: shared._yearly_media_summary(context, media_type) for media_type in shared.MEDIA_TYPES},
    }


def _extract_json_payload(text: str) -> dict[str, Any] | None:
    raw = str(text or "").strip()
    if not raw:
        return None
    fence_match = re.search(r"```(?:json)?\s*([\s\S]*?)```", raw, flags=re.IGNORECASE)
    if fence_match:
        raw = fence_match.group(1).strip()
    start = raw.find("{")
    end = raw.rfind("}")
    if start < 0 or end <= start:
        return None
    try:
        payload = json.loads(raw[start : end + 1])
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def merge_llm_sections(context: dict[str, Any], text: str) -> dict[str, Any]:
    merged = build_default_sections(context)
    payload = _extract_json_payload(text)
    if not payload:
        return merged
    for key in ("rating_text",):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            merged[key] = value.strip()
    for key in ("structure_features",):
        value = payload.get(key)
        if isinstance(value, list):
            merged[key] = [shared.truncate_text_by_chars(str(item).strip(), 90) for item in value if str(item).strip()][:4]
    highlights = payload.get("highlights")
    if isinstance(highlights, dict):
        for media_type in shared.MEDIA_TYPES:
            item = highlights.get(media_type)
            if not isinstance(item, dict):
                continue
            merged["highlights"].setdefault(media_type, {})
            for field in ("evaluation_summary", "representative_reason"):
                value = item.get(field)
                if isinstance(value, str) and value.strip():
                    merged["highlights"][media_type][field] = shared.truncate_text_by_chars(value.strip(), 160)
    yearly_summaries = payload.get("yearly_category_summaries")
    if isinstance(yearly_summaries, dict):
        for media_type in shared.MEDIA_TYPES:
            value = yearly_summaries.get(media_type)
            if isinstance(value, str) and value.strip():
                merged["yearly_category_summaries"][media_type] = shared.truncate_text_by_chars(value.strip(), 180)
    return merged


def llm_messages(period: shared.ReportPeriod, context: dict[str, Any]) -> list[dict[str, str]]:
    payload = _fit_prompt_payload(context.get("kind") or shared.REPORT_KIND_QUARTERLY, _context_for_prompt(context))
    system_prompt = (
        "你是中文个人阅读影音分析报告助手。"
        "你的职责不是重新组织统计逻辑，而是解释已经算出的事实。"
        "你只能输出 JSON 对象，不要输出 Markdown，不要输出代码块外的任何解释。"
        "必须严格基于给定事实，不得编造不存在的条目、数量、评分、趋势或外部资料。"
        "section 职责必须严格切开："
        "如果是季度，overview_text 必须覆盖：本期总量和媒体分布、与上一周期相比最明显的变化、本期最突出的结构特征。"
        "如果是年度，overview_text 必须优先回答：这一年主要花在哪几类媒介上、相比去年最大的年度变化、这一年最明显的结构特征。不要把同比当成开头主叙事。"
        "distribution_text 只回答国家/题材/渠道/作者的结构特点，并解释为什么值得注意，不能只重复集中度标签。"
        "rating_text 只回答平均评分高低，以及哪类更稳定/更波动。"
        "structure_features 必须用“本期记录更集中在…… / 本期呈现出…… / 本期样本更常见的是……”这类表述，禁止写“偏好明显转向 / 更加青睐 / 依然偏爱”。"
        "highlights 中每类媒介只生成 evaluation_summary 和 representative_reason。"
        "next_focus 只能从给定 focus_candidates 中挑 3 条最值得写的并重写，不得把候选原句直接抄回，也不得自造方向。"
        "如果是 yearly，还要输出 annual_trend_text 和 yearly_category_summaries；annual_trend_text 要回答哪个季度最活跃、哪些媒介在哪个季度明显升降、年内是否有阶段切换、Q1 到 Q4 是否有重心变化；yearly_category_summaries 只能基于 quarter_inputs 的概览与亮点做综合。"
    )
    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": f"报告周期: {period.label}\n\n请按要求输出 JSON。字段：overview_text, distribution_text, rating_text, structure_features, highlights, next_focus{', annual_trend_text, yearly_category_summaries' if context.get('kind') == shared.REPORT_KIND_YEARLY else ''}。\n\n参考数据: {_json_compact(payload)}"},
    ]