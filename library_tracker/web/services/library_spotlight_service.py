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


def _media_count(context: dict[str, Any], media_type: str) -> int:
    return len((context.get("current_rows_by_media") or {}).get(media_type, []))


def _distribution_narrative(context: dict[str, Any], media_type: str) -> str:
    kind = context.get("kind")
    media_label = shared.MEDIA_LABELS[media_type]
    rows = (context.get("current_rows_by_media") or {}).get(media_type, [])
    dist = (context.get("distribution") or {}).get(media_type, {})
    count = len(rows)
    if count == 0:
        return f"{media_label}这一期没有形成足够样本，先不强行总结规律。"
    category_ratio = shared._top_two_ratio(dist.get("category_details") or [])
    channel_ratio = shared._top_two_ratio(dist.get("channel_details") or [])
    nationality_ratio = shared._top_two_ratio(dist.get("nationality_details") or [])
    opening = f"{media_label}{'这一年' if kind == shared.REPORT_KIND_YEARLY else '这季'}共记录 {count} 条。"
    facts = [item for item in [category_ratio, channel_ratio, nationality_ratio] if item]
    if count <= 2 and kind != shared.REPORT_KIND_YEARLY:
        return f"{opening}样本很少，均分和分布都不宜过度解读；目前更能说明问题的是 {facts[0] if facts else '它仍停留在个别条目层面'}。"
    if facts:
        return f"{opening}{facts[0]}。{facts[1] if len(facts) > 1 else '这比单看表格更能说明它的入口和题材有没有开始固化。'}。"
    return f"{opening}目前还看不出稳定的题材、地区或渠道惯性。"


def _quarterly_rating_text(context: dict[str, Any]) -> str:
    rows_by_media = context.get("current_rows_by_media") or {}
    overall_avg = shared._overall_average_rating(rows_by_media)
    rated: list[tuple[str, int, float, float, float]] = []
    for media_type in shared.MEDIA_TYPES:
        rows = rows_by_media.get(media_type, [])
        avg = shared._average_rating(rows)
        spread = shared._rating_stddev(rows)
        low, high = shared._rating_range(rows)
        if avg is None or spread is None or low is None or high is None:
            continue
        rated.append((shared.MEDIA_LABELS[media_type], len(rows), avg, spread, high - low))
    if not rated:
        return "本期有效评分样本太少，先保留原始记录，不拉平均分结论。"
    highest = max(rated, key=lambda item: item[2])
    widest = max(rated, key=lambda item: item[4])
    steadiest = min(rated, key=lambda item: item[3])
    overall_text = f"，整体个人平均分约 {overall_avg:.2f}" if overall_avg is not None else ""
    return (
        f"{highest[0]} 的均分最高，为 {highest[2]:.2f}（{highest[1]} 条样本）{overall_text}；"
        f"{steadiest[0]} 的离散度最低，标准差 {steadiest[3]:.2f}；"
        f"{widest[0]} 的内部落差最大，最高分与最低分相差 {widest[4]:.2f} 分。"
    )


def _yearly_rating_text(context: dict[str, Any]) -> str:
    rows_by_media = context.get("current_rows_by_media") or {}
    rated: list[tuple[str, int, float, float, float]] = []
    for media_type in shared.MEDIA_TYPES:
        rows = rows_by_media.get(media_type, [])
        avg = shared._average_rating(rows)
        spread = shared._rating_stddev(rows)
        low, high = shared._rating_range(rows)
        if avg is None or spread is None or low is None or high is None:
            continue
        rated.append((shared.MEDIA_LABELS[media_type], len(rows), avg, spread, high - low))
    if not rated:
        return "年度有效评分样本有限，暂时不做稳定性判断。"
    highest = max(rated, key=lambda item: item[2])
    steadiest = min(rated, key=lambda item: item[3])
    widest = max(rated, key=lambda item: item[4])
    return (
        f"全年均分最高的是 {highest[0]}（{highest[2]:.2f} / {highest[1]} 条）；"
        f"最稳定的是 {steadiest[0]}，标准差 {steadiest[3]:.2f}；"
        f"分裂感最强的是 {widest[0]}，最高分和最低分之间拉开了 {widest[4]:.2f} 分。"
    )


def _highlight_summary(context: dict[str, Any], media_type: str) -> str:
    representative = (context.get("representatives") or {}).get(media_type) or {}
    rows = (context.get("current_rows_by_media") or {}).get(media_type, [])
    count = len(rows)
    rating = shared._safe_rating(representative.get("rating"))
    review = str(representative.get("review") or "").strip()
    avg = shared._average_rating(rows)
    if rating is None:
        return f"这一类本期只有 {count} 条记录，先把它当作样本锚点，不强拉结论。"
    if count <= 2:
        return f"这一类本期只有 {count} 条记录，{shared.MEDIA_LABELS[media_type]}代表作打了 {rating:.2f} 分，更像单条偏好记录，不适合上升成整类判断。"
    if avg is not None:
        delta = rating - avg
        if abs(delta) >= 1.0:
            return f"这条代表作打了 {rating:.2f} 分，比该类本期均分 {avg:.2f} {'高' if delta > 0 else '低'}了 {abs(delta):.2f} 分，能看出它和同类样本的距离。"
    if len(review) >= 40:
        return f"这条代表作保留了较完整的个人评价，信息密度比同类样本更高，适合作为本期落点。"
    return f"这条代表作更适合作为本期样本的落点，而不是替整类作品下结论。"


def _is_weak_rating_text(text: str) -> bool:
    raw = str(text or "").strip()
    if not raw:
        return True
    banned = ("波动较明显", "评价较高", "评价一般", "占主导地位", "表现较为一般")
    if any(token in raw for token in banned):
        return True
    return not any(token in raw for token in ("标准差", "样本", "分", "最高", "最低", "分差"))


def _is_weak_highlight_summary(text: str) -> bool:
    raw = str(text or "").strip()
    if not raw:
        return True
    banned = ("评价较高", "评价一般", "表现较为一般", "反响较好", "显示出观众对")
    return any(token in raw for token in banned)


def _is_weak_distribution_section(text: str) -> bool:
    raw = str(text or "").strip()
    if not raw:
        return True
    if any(token in raw for token in ("占主导地位", "主要集中在", "成为下载的主要渠道", "显示出高度集中的特点")) and not re.search(r"\d", raw):
        return True
    return False


def _is_weak_structure_feature(text: str) -> bool:
    raw = str(text or "").strip()
    if not raw:
        return True
    banned = ("高度集中的特点", "表现较好", "评分波动较大", "值得关注", "显示出用户兴趣的变化")
    return any(token in raw for token in banned)


def _is_weak_next_focus(text: str) -> bool:
    raw = str(text or "").strip()
    if not raw:
        return True
    if any(token in raw for token in ("可继续顺着", "可优先沿着", "方向补充")):
        return True
    return bool(re.search(r"\([^)]*\d+[^)]*\)", raw))


def _quarterly_structure_features(context: dict[str, Any]) -> list[str]:
    comparison_rows = context.get("comparison_rows") or []
    rating_rows = context.get("rating_rows") or []
    rows_by_media = context.get("current_rows_by_media") or {}
    output: list[str] = []
    growth = max(comparison_rows, key=lambda row: int(row.get("count_delta") or 0), default=None)
    if growth and int(growth.get("count_delta") or 0) != 0:
        output.append(f"数量变化最大的是 {growth['media_label']}，相对上一季 {'增加' if int(growth['count_delta']) > 0 else '减少'}了 {abs(int(growth['count_delta']))} 条。")
    stable = min(rating_rows, key=lambda row: row.get("rating_stddev") if row.get("rating_stddev") is not None else 999, default=None)
    if stable and stable.get("rating_stddev") is not None:
        output.append(f"评分最稳的是 {stable['media_label']}，标准差只有 {float(stable['rating_stddev'] or 0):.2f}。")
    for media_type in shared.MEDIA_TYPES:
        if len(output) >= 3:
            break
        dist = (context.get("distribution") or {}).get(media_type, {})
        category_ratio = shared._top_two_ratio(dist.get("category_details") or [])
        if category_ratio:
            output.append(f"{shared.MEDIA_LABELS[media_type]} 里最值得记的一点是：{category_ratio}。")
    return output[:4]


def _yearly_quarterly_comparison_text(context: dict[str, Any]) -> str:
    trend_rows = context.get("trend_rows") or []
    if not trend_rows:
        return "这一年季度样本不足，暂时看不出明显阶段变化。"
    quarter_totals: dict[str, int] = {}
    for row in trend_rows:
        quarter = str(row.get("quarter") or "")
        quarter_totals[quarter] = quarter_totals.get(quarter, 0) + int(row.get("count") or 0)
    if not quarter_totals:
        return "这一年季度样本不足，暂时看不出明显阶段变化。"
    peak_quarter = max(quarter_totals.items(), key=lambda item: item[1])
    low_quarter = min(quarter_totals.items(), key=lambda item: item[1])
    strongest_media = max(trend_rows, key=lambda row: int(row.get("count") or 0))
    best_game = max([row for row in trend_rows if row.get("media_type") == "game"], key=lambda row: float(row.get("avg_rating") or -1), default=None)
    tail = ""
    if best_game and str(best_game.get("quarter") or "") not in {peak_quarter[0], low_quarter[0]}:
        tail = f" {best_game['quarter']} 的游戏均分达到 {float(best_game.get('avg_rating') or 0):.2f}，说明注意力并不只跟着条数走。"
    return (
        f"{peak_quarter[0]} 是全年内容消费高峰（{peak_quarter[1]} 条），主要由 {strongest_media['media_label']} 拉动；"
        f"{low_quarter[0]} 最淡（{low_quarter[1]} 条），年内节奏并不平均。{tail}".strip()
    )


def _annual_extremes_text(context: dict[str, Any]) -> str:
    all_rows = []
    for media_type in shared.MEDIA_TYPES:
        all_rows.extend((context.get("current_rows_by_media") or {}).get(media_type, []))
    highest, lowest = shared._quarterly_high_low_items(all_rows)
    anchor = max(
        [row for row in all_rows if str(row.get("review") or "").strip()],
        key=lambda row: len(str(row.get("review") or "").strip()),
        default=None,
    )
    if not highest or not lowest:
        return "这一年有效评分样本有限，暂时做不出可靠的年度两极记录。"
    parts = [
        f"- 最值回时间的是 {shared._item_markdown_link(str(highest.get('title') or '—'), str(highest.get('id') or ''))}，个人评分 {float(highest.get('rating') or 0):.2f}。",
        f"- 最让人失望的是 {shared._item_markdown_link(str(lowest.get('title') or '—'), str(lowest.get('id') or ''))}，个人评分 {float(lowest.get('rating') or 0):.2f}。",
    ]
    if anchor:
        parts.append(
            f"- 年度最佳单条是 {shared._item_markdown_link(str(anchor.get('title') or '—'), str(anchor.get('id') or ''))}，它留下了这一年最长的一条评论，更像全年体验的精神锚点。"
        )
    return "\n".join(parts)


def _yearly_tracks(context: dict[str, Any]) -> dict[str, str]:
    output: dict[str, str] = {}
    for media_type in shared.MEDIA_TYPES:
        output[media_type] = shared._yearly_media_summary(context, media_type)
    return output


def build_default_sections(context: dict[str, Any]) -> dict[str, Any]:
    kind = context.get("kind")
    distribution_sections: dict[str, str] = {}
    for media_type in shared.MEDIA_TYPES:
        distribution_sections[media_type] = _distribution_narrative(context, media_type)
    highlights: dict[str, dict[str, str]] = {}
    for media_type in shared.MEDIA_TYPES:
        representative = (context.get("representatives") or {}).get(media_type) or {}
        highlights[media_type] = {
            "evaluation_summary": _highlight_summary(context, media_type),
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
        "distribution_text": "",
        "distribution_sections": distribution_sections,
        "rating_text": _yearly_rating_text(context) if kind == shared.REPORT_KIND_YEARLY else _quarterly_rating_text(context),
        "structure_features": _quarterly_structure_features(context) if kind != shared.REPORT_KIND_YEARLY else (context.get("annual_trend_signals") or [])[:3],
        "highlights": highlights,
        "next_focus": shared._default_next_focus(context),
        "annual_trend_text": "" if context.get("kind") != shared.REPORT_KIND_YEARLY else _yearly_quarterly_comparison_text(context),
        "annual_extremes_text": "" if context.get("kind") != shared.REPORT_KIND_YEARLY else _annual_extremes_text(context),
        "yearly_tracks": {} if context.get("kind") != shared.REPORT_KIND_YEARLY else _yearly_tracks(context),
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
    quarterly_default_first = context.get("kind") == shared.REPORT_KIND_QUARTERLY
    for key in ("overview_text", "distribution_text", "rating_text", "annual_trend_text", "annual_extremes_text"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            if quarterly_default_first and key in {"distribution_text", "rating_text"}:
                continue
            if key == "rating_text" and _is_weak_rating_text(value):
                continue
            merged[key] = value.strip()
    distribution_sections = payload.get("distribution_sections")
    if isinstance(distribution_sections, dict) and not quarterly_default_first:
        for media_type in shared.MEDIA_TYPES:
            value = distribution_sections.get(media_type)
            if isinstance(value, str) and value.strip() and not _is_weak_distribution_section(value):
                merged["distribution_sections"][media_type] = shared.truncate_text_by_chars(value.strip(), 220)
    for key in ("structure_features",):
        value = payload.get(key)
        if isinstance(value, list) and not quarterly_default_first:
            cleaned_features = [shared.truncate_text_by_chars(str(item).strip(), 90) for item in value if str(item).strip() and not _is_weak_structure_feature(str(item).strip())]
            if cleaned_features:
                merged[key] = cleaned_features[:4]
    next_focus = payload.get("next_focus")
    if isinstance(next_focus, list) and not quarterly_default_first:
        cleaned_focus = []
        for item in next_focus:
            value = str(item).strip()
            if not value or _is_weak_next_focus(value):
                continue
            cleaned_focus.append(shared.truncate_text_by_chars(value, 120))
        if cleaned_focus:
            merged["next_focus"] = cleaned_focus[:3]
    highlights = payload.get("highlights")
    if isinstance(highlights, dict) and not quarterly_default_first:
        for media_type in shared.MEDIA_TYPES:
            item = highlights.get(media_type)
            if not isinstance(item, dict):
                continue
            merged["highlights"].setdefault(media_type, {})
            for field in ("evaluation_summary", "representative_reason"):
                value = item.get(field)
                if isinstance(value, str) and value.strip():
                    if field == "evaluation_summary" and _is_weak_highlight_summary(value):
                        continue
                    merged["highlights"][media_type][field] = shared.truncate_text_by_chars(value.strip(), 160)
    yearly_tracks = payload.get("yearly_tracks")
    if isinstance(yearly_tracks, dict):
        for media_type in shared.MEDIA_TYPES:
            value = yearly_tracks.get(media_type)
            if isinstance(value, str) and value.strip():
                merged["yearly_tracks"][media_type] = shared.truncate_text_by_chars(value.strip(), 260)
    return merged


def llm_messages(period: shared.ReportPeriod, context: dict[str, Any]) -> list[dict[str, str]]:
    payload = _fit_prompt_payload(context.get("kind") or shared.REPORT_KIND_QUARTERLY, _context_for_prompt(context))
    system_prompt = (
        "你是中文个人阅读影音分析报告助手。"
        "你的职责是在固定章节内解释已经算出的事实，但正文表达可以自然，不要写成槽位填空。"
        "你只能输出 JSON 对象，不要输出 Markdown，不要输出代码块外的任何解释。"
        "必须严格基于给定事实，不得编造不存在的条目、数量、评分、趋势或外部资料。"
        "section 职责必须严格切开："
        "如果是季度，overview_text 必须覆盖：本期总量和媒体分布、与上一周期相比最明显的变化、本期最突出的结构特征。"
        "如果是年度，overview_text 必须优先回答：这一年主要花在哪几类媒介上、相比去年最大的年度变化、这一年最明显的结构特征。不要把同比当成开头主叙事。"
        "distribution_sections 必须是按媒介拆开的对象，键为 book/video/music/game；每个值写成 1 段短正文，优先提炼表格看不出的规律，不要把国家/题材/渠道/作者前两名再念一遍。"
        "rating_text 不能用‘波动较明显’‘评价较高’这类空词，必须写具体分数、样本数、分差或标准差。"
        "structure_features 写 3 到 4 条结构观察，允许自然表达，但不要写成空泛抒情。"
        "highlights 中每类媒介只生成 evaluation_summary 和 representative_reason；如果样本数很少，要明确提醒不要过度解释平均分。"
        "next_focus 只能从给定 focus_candidates 中挑 3 条最值得写的并重写，改成判断句，不得把候选原句直接抄回，也不得只是重复括号里的数字。"
        "如果是 yearly，还要输出 annual_trend_text、annual_extremes_text 和 yearly_tracks；annual_trend_text 要写成季度对比解读，不要只重复表格。annual_extremes_text 要回答什么最值回时间、什么最让人失望。yearly_tracks 是按媒介写的年度轨迹，重点是这一年看了什么、感受有没有变化，不要变回季度代表作罗列。"
    )
    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": f"报告周期: {period.label}\n\n请按要求输出 JSON。字段：overview_text, distribution_text, distribution_sections, rating_text, structure_features, highlights, next_focus{', annual_trend_text, annual_extremes_text, yearly_tracks' if context.get('kind') == shared.REPORT_KIND_YEARLY else ''}。\n\n参考数据: {_json_compact(payload)}"},
    ]