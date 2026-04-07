from __future__ import annotations

from datetime import datetime
from typing import Any

from core_service.reporting import build_report_meta, build_report_filename, persist_report_record

from . import library_analysis_core as shared


def resolve_period(kind: str, period_key: str | None = None) -> shared.ReportPeriod:
    if kind == shared.REPORT_KIND_QUARTERLY:
        return shared._quarter_period_from_key(period_key) if period_key else shared._previous_quarter_period()
    if kind == shared.REPORT_KIND_YEARLY:
        return shared._year_period_from_key(period_key) if period_key else shared._previous_year_period()
    raise ValueError("invalid report kind")


def render_report_markdown(period: shared.ReportPeriod, context: dict[str, Any], sections: dict[str, Any], note: str = "") -> str:
    title = f"# {period.label}{shared.REPORT_KIND_TITLES[context['kind']]}"
    overview_heading = "## 年度概览" if context.get("kind") == shared.REPORT_KIND_YEARLY else "## 本期概览"
    lines = [title, "", overview_heading, sections.get("overview_text") or context.get("overview_fact_text") or "—", ""]
    if context.get("kind") == shared.REPORT_KIND_QUARTERLY:
        representatives_line = "；".join(
            f"{shared.MEDIA_LABELS[media_type]}{shared._item_markdown_link(((context.get('representatives') or {}).get(media_type) or {}).get('title') or '—', ((context.get('representatives') or {}).get(media_type) or {}).get('id') or '')}"
            for media_type in shared.MEDIA_TYPES
        )
        lines.append(f"- **代表作：** {representatives_line}")
    else:
        lines.append("- 年内各季度代表作品见后文“年度代表作品回看”，不再额外挑选全年单一代表作。")
    lines.append("")
    if context.get("kind") == shared.REPORT_KIND_YEARLY:
        lines.extend(["## 年内变化趋势", sections.get("annual_trend_text") or "—", ""])
        trend_lines = [
            "| 媒介 | 季度 | 条数 | 平均评分 | 代表作品 |",
            "| :---: | :---: | :---: | :---: | :---: |",
        ]
        for row in shared._sorted_yearly_trend_rows(context.get("trend_rows") or []):
            trend_lines.append(
                f"| {row['media_label']} | {row['quarter']} | {row['count']} | {shared._format_rating(row['avg_rating'])} | {shared._item_markdown_link(row['representative_title'], row.get('representative_id')) if str(row.get('representative_title') or '').strip() and str(row.get('representative_title')) != '—' else '—'} |"
            )
        lines.extend(trend_lines)
        lines.append("")
    lines.extend(["## 与上一周期对比" if context.get("kind") == shared.REPORT_KIND_QUARTERLY else "## 同比参考", *(shared._markdown_comparison_table(context.get("comparison_rows") or [])), ""])
    lines.extend(["## 各媒体分布特点", ""])
    for media_type in shared.MEDIA_TYPES:
        dist = (context.get("distribution") or {}).get(media_type, {})
        lines.append(f"### {shared.MEDIA_LABELS[media_type]}")
        if context.get("kind") == shared.REPORT_KIND_YEARLY:
            for item in shared._yearly_distribution_points(media_type, dist):
                lines.append(f"- {shared._bold_leading_label(item)}")
        else:
            lines.extend(
                [
                    f"- {shared._bold_leading_label(shared._distribution_dimension_line(dist.get('nationality_details') or [], '国家/地区', annual=False))}",
                    f"- {shared._bold_leading_label(shared._distribution_dimension_line(dist.get('category_details') or [], '题材', annual=False))}",
                    f"- {shared._bold_leading_label(shared._distribution_dimension_line(dist.get('channel_details') or [], '渠道', annual=False))}",
                    f"- {shared._bold_leading_label(shared._distribution_dimension_line(dist.get('author_details') or [], '作者', annual=False))}",
                ]
            )
        lines.append("")
    lines.extend(["## 评分概览", sections.get("rating_text") or "—", "", *(shared._markdown_rating_table(context.get("rating_rows") or [])), ""])
    if context.get("kind") == shared.REPORT_KIND_QUARTERLY:
        lines.extend(["## 本期亮点作品"])
        for media_type in shared.MEDIA_TYPES:
            representative = (context.get("representatives") or {}).get(media_type)
            if not representative:
                lines.extend([f"### {shared.MEDIA_LABELS[media_type]}", "- 本期无代表作品。", ""])
                continue
            reference = (context.get("external_references") or {}).get(media_type) or {}
            highlight = (sections.get("highlights") or {}).get(media_type, {})
            reference_parts = [part for part in [reference.get("intro"), reference.get("background"), reference.get("extra")] if str(part or "").strip()]
            reference_text = " ".join(reference_parts) if reference_parts else "暂无稳定外部补充。"
            lines.extend(
                [
                    f"### {shared.MEDIA_LABELS[media_type]}{shared._item_markdown_link(representative.get('title') or '—', representative.get('id') or '')}",
                    f"- **评价：** {highlight.get('evaluation_summary') or shared._excerpt(representative.get('review'), 80)}",
                    f"- **简介：** {reference_text}",
                    f"- **理由：** {highlight.get('representative_reason') or shared._representative_basis(shared.MEDIA_LABELS[media_type], representative, (context.get('distribution') or {}).get(media_type, {}), (context.get('current_rows_by_media') or {}).get(media_type, []))}",
                    "",
                ]
            )
    else:
        lines.append("## 年度代表作品回看")
        for media_type in shared.MEDIA_TYPES:
            lines.append(f"### {shared.MEDIA_LABELS[media_type]}")
            lines.append(sections.get("yearly_category_summaries", {}).get(media_type) or "—")
            lines.append("")
            lines.append("| 季度 | 代表作品 | 评分 |")
            lines.append("| :---: | :---: | :---: |")
            for row in [item for item in (context.get("trend_rows") or []) if item.get("media_type") == media_type]:
                title_cell = shared._item_markdown_link(row["representative_title"], row.get("representative_id")) if str(row.get("representative_title") or "").strip() and str(row.get("representative_title")) != "—" else "—"
                lines.append(f"| {row['quarter']} | {title_cell} | {shared._format_rating(row['representative_rating'])} |")
            lines.append("")
    lines.append("## 本期结构特征" if context.get("kind") == shared.REPORT_KIND_QUARTERLY else "## 年度结构特征")
    for item in (sections.get("structure_features") or [])[:4]:
        lines.append(f"- {item}")
    lines.extend(["", "## 下期可以关注的方向" if context.get("kind") == shared.REPORT_KIND_QUARTERLY else "## 下一年度可以延续/补足的方向"])
    for item in (sections.get("next_focus") or [])[:3]:
        lines.append(f"- {item}")
    if note:
        lines.extend(["", "---", "", f"> {note}"])
    return "\n".join(lines).strip()


def save_report(kind: str, backend: str, period: shared.ReportPeriod, markdown: str, model_label: str, summary: str) -> dict[str, Any]:
    path = shared._report_dir(kind) / build_report_filename(period.key, period.job_type, backend)
    meta = build_report_meta(
        report_kind=kind,
        job_type=period.job_type,
        report_label=shared.REPORT_KIND_LABELS[kind],
        period_key=period.key,
        period_label=period.label,
        source=backend,
        generated_at=datetime.now().isoformat(timespec="seconds"),
        model_label=model_label,
        title=f"{period.label} {shared.REPORT_KIND_TITLES[kind]}",
        summary=summary,
    )
    return persist_report_record(path=path, meta=meta, markdown=markdown, parse_record=shared._parse_report_file)


def summary_from_markdown(markdown: str) -> str:
    for line in str(markdown or "").splitlines():
        text = line.strip().lstrip("#>").strip()
        if text:
            return shared.truncate_text_by_chars(text, 90)
    return ""