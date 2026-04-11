from __future__ import annotations

from datetime import datetime
from typing import Any

from core_service.reporting import build_report_meta, build_report_filename, persist_report_record

from . import library_analysis_core as shared


def _clean_review_excerpt(text: Any, limit: int = 220) -> str:
    raw = " ".join(str(text or "").replace("\r", "").split())
    return shared.truncate_text_by_chars(raw, limit) if raw else ""


def _render_quarterly_notes(lines: list[str], context: dict[str, Any]) -> None:
    lines.extend(["## 本期笔记摘录"])
    for media_type in shared.MEDIA_TYPES:
        representative = (context.get("representatives") or {}).get(media_type)
        if not representative:
            lines.extend([f"### {shared.MEDIA_LABELS[media_type]}", "> 本期没有留下有效短评。", ""])
            continue
        raw_note = _clean_review_excerpt(representative.get("review"), 280)
        if not raw_note:
            raw_note = "本期没有留下有效短评。"
        lines.extend(
            [
                f"### {shared.MEDIA_LABELS[media_type]}{shared._item_markdown_link(representative.get('title') or '—', representative.get('id') or '')}",
                f"> {raw_note}",
                "",
            ]
        )


def _render_yearly_tracks(lines: list[str], sections: dict[str, Any]) -> None:
    lines.append("## 年度轨迹")
    for media_type in shared.MEDIA_TYPES:
        lines.append(f"### {shared.MEDIA_LABELS[media_type]}")
        lines.append((sections.get("yearly_tracks") or {}).get(media_type) or "—")
        lines.append("")


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
        lines.append("- 年报更关心这一年兴趣怎么移动，不再额外挑单一“年度代表作”。")
    lines.append("")
    if context.get("kind") == shared.REPORT_KIND_YEARLY:
        lines.extend(["## 季度对比", sections.get("annual_trend_text") or "—", ""])
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
        lines.extend(["## 年度之最", sections.get("annual_extremes_text") or "—", ""])
    lines.extend(["## 与上一周期对比" if context.get("kind") == shared.REPORT_KIND_QUARTERLY else "## 同比参考", *(shared._markdown_comparison_table(context.get("comparison_rows") or [])), ""])
    lines.extend(["## 各媒体分布", ""])
    distribution_sections = sections.get("distribution_sections") if isinstance(sections.get("distribution_sections"), dict) else {}
    for media_type in shared.MEDIA_TYPES:
        lines.append(f"### {shared.MEDIA_LABELS[media_type]}")
        section_text = distribution_sections.get(media_type)
        if isinstance(section_text, str) and section_text.strip():
            lines.append(section_text.strip())
        else:
            lines.append("本期样本有限，先保留原始记录。")
        lines.append("")
    lines.extend(["## 评分概览", sections.get("rating_text") or "—", "", *(shared._markdown_rating_table(context.get("rating_rows") or [])), ""])
    if context.get("kind") == shared.REPORT_KIND_QUARTERLY:
        _render_quarterly_notes(lines, context)
    else:
        _render_yearly_tracks(lines, sections)
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