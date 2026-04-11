from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from ..planner import planner_contracts
from ..agent.agent_types import (
    TOOL_EXPAND_MEDIAWIKI_CONCEPT,
    TOOL_PARSE_MEDIAWIKI,
    TOOL_QUERY_MEDIA,
    TOOL_SEARCH_BY_CREATOR,
    TOOL_SEARCH_BANGUMI,
    TOOL_SEARCH_MEDIAWIKI,
    TOOL_SEARCH_TMDB,
    TOOL_SEARCH_WEB,
    ToolExecution,
)
from .media_answer_planner import build_structured_media_render_plan
from .media_render_contract import MediaRenderContract
@dataclass(frozen=True)
class MediaAnswerRenderDeps:
    clip_text: Callable[[Any, int], str]


def _structured_row_excerpt_limit(contract: MediaRenderContract) -> int:
    if (
        contract.subject_scope == planner_contracts.ROUTER_SUBJECT_SCOPE_PERSONAL_RECORD
        and contract.answer_shape == planner_contracts.ROUTER_ANSWER_SHAPE_LIST_PLUS_EXPAND
    ):
        return 0
    return 0


def _should_suppress_structured_main_answer(contract: MediaRenderContract) -> bool:
    return bool(
        contract.answer_shape in {
            planner_contracts.ROUTER_ANSWER_SHAPE_SUMMARY,
            planner_contracts.ROUTER_ANSWER_SHAPE_LIST_PLUS_EXPAND,
            planner_contracts.ROUTER_ANSWER_SHAPE_COMPARE,
        }
        or (
            contract.subject_scope == planner_contracts.ROUTER_SUBJECT_SCOPE_PERSONAL_RECORD
            and contract.answer_shape == planner_contracts.ROUTER_ANSWER_SHAPE_LIST_PLUS_EXPAND
        )
    )


def _format_media_rating(value: Any) -> str:
    if value in {None, ""}:
        return ""
    try:
        number = float(value)
    except Exception:
        return str(value).strip()
    if number.is_integer():
        return f"{int(number)}/10"
    return f"{number:.1f}/10"


def _build_media_row_detail_lines(
    row: dict[str, Any],
    *,
    include_review: bool,
    include_metadata: bool = False,
    citation_lookup: Callable[[dict[str, Any]], str] | None = None,
) -> list[str]:
    title = str(row.get("title", "") or "").strip() or "未命名条目"
    rating = _format_media_rating(row.get("rating"))
    date = str(row.get("date", "") or "").strip()
    citation_suffix = str(citation_lookup(row) if callable(citation_lookup) else "").strip()
    heading = f"### 《{title}》"
    if citation_suffix:
        heading += f" {citation_suffix}"
    lines = [heading]
    if date:
        lines.append(f"- **观看/阅读日期**：{date}")
    if rating:
        lines.append(f"- **个人评分**：{rating}")
    if include_metadata:
        author = str(row.get("author", "") or "").strip()
        publisher = str(row.get("publisher", "") or "").strip()
        channel = str(row.get("channel", "") or "").strip()
        category = str(row.get("category", "") or "").strip()
        if author:
            lines.append(f"- **作者**：{author}")
        if publisher:
            lines.append(f"- **出版社**：{publisher}")
        if channel:
            lines.append(f"- **渠道**：{channel}")
        if category:
            lines.append(f"- **分类**：{category}")
    review = str(row.get("review", "") or "").strip()
    if include_review:
        if review:
            lines.append(f"- **个人短评**：{review}")
        else:
            lines.append("- **个人短评**：未记录")
    return lines


def build_structured_media_external_item_block(
    row: dict[str, Any],
    *,
    clip_text: Callable[[Any, int], str],
    include_divider: bool = False,
) -> str:
    if not isinstance(row, dict):
        return ""
    lines: list[str] = []
    if include_divider:
        lines.extend(["---", "## 外部补充背景", ""])
    title = str(row.get("local_title") or row.get("title") or "").strip() or "未命名条目"
    overview = str(row.get("external_overview") or row.get("overview") or "").strip()
    source = str(row.get("external_source") or row.get("per_item_source") or "wiki").strip().lower()
    source_label = "TMDB" if source == "tmdb" else ("Bangumi" if source == "bangumi" else "Wiki")
    lines.append(f"### 《{title}》")
    if overview:
        lines.append(f"- **外部简介**：{clip_text(overview, 160)}（{source_label} 外部参考）")
    else:
        lines.append(f"- **外部简介**：未返回概要（{source_label} 外部参考）")
    return "\n".join(lines).strip()


def build_structured_media_external_blocks(*, deps: MediaAnswerRenderDeps, contract: MediaRenderContract) -> list[str]:
    if not contract.include_external:
        return []
    blocks: list[str] = []
    for index, row in enumerate(list(contract.external_rows)[: max(1, int(contract.external_limit or len(contract.external_rows) or 1))]):
        if not isinstance(row, dict):
            continue
        block = build_structured_media_external_item_block(
            row,
            clip_text=deps.clip_text,
            include_divider=(not blocks and index == 0),
        )
        if block:
            blocks.append(block)
            if len(blocks) >= contract.external_limit:
                break
    return blocks


def build_structured_media_external_appendix(*, deps: MediaAnswerRenderDeps, contract: MediaRenderContract) -> str:
    return "\n\n".join(build_structured_media_external_blocks(deps=deps, contract=contract)).strip()


def build_structured_media_mention_block(
    mention_rows: list[dict[str, Any]],
    *,
    clip_text: Callable[[Any, int], str],
    mention_limit: int = 4,
    citation_lookup: Callable[[dict[str, Any]], str] | None = None,
) -> str:
    if not mention_rows:
        return ""
    lines = ["---", "## 扩展提及", "", "你在其他作品的评价里也提到或关联到了该系列："]
    for row in mention_rows[: max(1, int(mention_limit or 4))]:
        if not isinstance(row, dict):
            continue
        title = str(row.get("title") or "").strip() or "未命名条目"
        citation_suffix = str(citation_lookup(row) if callable(citation_lookup) else "").strip()
        review = str(row.get("review") or "").strip()
        lines.append(f"### 《{title}》{(' ' + citation_suffix) if citation_suffix else ''}")
        if review:
            lines.append(f"- **关联提及**：{clip_text(review, 120)}")
        else:
            lines.append("- **关联提及**：未记录")
    return "\n".join(lines).strip()


def collect_structured_media_render_parts(
    *,
    deps: MediaAnswerRenderDeps,
    contract: MediaRenderContract,
    citation_lookup: Callable[[dict[str, Any]], str] | None = None,
) -> dict[str, Any] | None:
    plan = build_structured_media_render_plan(contract)
    if plan is None:
        return None
    row_groups = [list(group) for group in plan.row_groups]
    row_group_headings = [str(item).strip() for item in plan.row_group_headings if str(item).strip()]
    row_group_blocks: list[list[str]] = []
    for group in row_groups:
        blocks: list[str] = []
        for row in group:
            detail_lines = _build_media_row_detail_lines(
                row,
                include_review=contract.include_reviews,
                include_metadata=contract.include_metadata,
                citation_lookup=citation_lookup,
            )
            if detail_lines:
                blocks.append("\n".join(detail_lines).strip())
        row_group_blocks.append(blocks)

    row_blocks = [block for blocks in row_group_blocks for block in blocks]
    top_row_blocks = list(row_group_blocks[0]) if row_group_headings[:1] == ["## 评分最高"] else []
    bottom_row_blocks = list(row_group_blocks[1]) if len(row_group_headings) > 1 and row_group_headings[1] == "## 评分较低" else []
    remaining_row_blocks = list(row_group_blocks[2]) if len(row_group_headings) > 2 and row_group_headings[2] == "## 其余条目" else []
    if not row_group_headings:
        top_row_blocks = []
        bottom_row_blocks = []
        remaining_row_blocks = []

    mention_block = ""
    mention_rows = [row for row in plan.mention_rows if isinstance(row, dict)]
    if mention_rows:
        mention_block = build_structured_media_mention_block(
            mention_rows,
            clip_text=deps.clip_text,
            mention_limit=contract.mention_limit,
            citation_lookup=citation_lookup,
        )

    external_blocks = []
    for index, row in enumerate(list(plan.external_rows)):
        if not isinstance(row, dict):
            continue
        block = build_structured_media_external_item_block(
            row,
            clip_text=deps.clip_text,
            include_divider=(index == 0),
        )
        if block:
            external_blocks.append(block)

    return {
        "intro_lines": list(plan.intro_lines),
        "row_blocks": row_blocks,
        "top_row_blocks": top_row_blocks,
        "bottom_row_blocks": bottom_row_blocks,
        "remaining_row_blocks": remaining_row_blocks,
        "mention_block": mention_block,
        "external_blocks": external_blocks,
    }


def build_structured_media_answer_chunks(
    *,
    deps: MediaAnswerRenderDeps,
    contract: MediaRenderContract,
    citation_lookup: Callable[[dict[str, Any]], str] | None = None,
) -> list[str]:
    if _should_suppress_structured_main_answer(contract):
        return []
    parts = collect_structured_media_render_parts(
        deps=deps,
        contract=contract,
        citation_lookup=citation_lookup,
    )
    if not parts:
        return []
    chunks: list[str] = []
    intro_lines = [str(line).strip() for line in parts.get("intro_lines", []) if str(line).strip()]
    row_blocks = [str(block).strip() for block in parts.get("row_blocks", []) if str(block).strip()]
    top_row_blocks = [str(block).strip() for block in parts.get("top_row_blocks", []) if str(block).strip()]
    bottom_row_blocks = [str(block).strip() for block in parts.get("bottom_row_blocks", []) if str(block).strip()]
    remaining_row_blocks = [str(block).strip() for block in parts.get("remaining_row_blocks", []) if str(block).strip()]
    mention_block = str(parts.get("mention_block") or "").strip()
    excerpt_limit = _structured_row_excerpt_limit(contract)
    if excerpt_limit > 0 and row_blocks:
        hidden_count = max(0, len(row_blocks) - excerpt_limit)
        row_blocks = row_blocks[:excerpt_limit]
        top_row_blocks = top_row_blocks[:excerpt_limit]
        if len(top_row_blocks) < excerpt_limit and bottom_row_blocks:
            remaining_budget = excerpt_limit - len(top_row_blocks)
            bottom_row_blocks = bottom_row_blocks[:remaining_budget]
        else:
            bottom_row_blocks = []
        if len(top_row_blocks) + len(bottom_row_blocks) < excerpt_limit and remaining_row_blocks:
            remaining_budget = excerpt_limit - len(top_row_blocks) - len(bottom_row_blocks)
            remaining_row_blocks = remaining_row_blocks[:remaining_budget]
        else:
            remaining_row_blocks = []
    if intro_lines:
        chunks.append("\n".join(intro_lines).strip())
    if top_row_blocks or bottom_row_blocks:
        if top_row_blocks:
            chunks.append("## 评分最高\n\n" + "\n\n".join(top_row_blocks).strip())
        if bottom_row_blocks:
            chunks.append("## 评分较低\n\n" + "\n\n".join(bottom_row_blocks).strip())
        if remaining_row_blocks:
            chunks.append("## 其余条目\n\n" + "\n\n".join(remaining_row_blocks).strip())
    else:
        if row_blocks[:3]:
            chunks.append("\n\n".join(row_blocks[:3]).strip())
        if row_blocks[3:]:
            chunks.append("\n\n".join(row_blocks[3:]).strip())
    if excerpt_limit > 0:
        hidden_count = max(0, len(parts.get("row_blocks", []) or []) - excerpt_limit)
        if hidden_count > 0:
            chunks.append(f"其余 {hidden_count} 条本地记录未在主回答中展开。")
    if mention_block:
        chunks.append(mention_block)
    return [chunk for chunk in chunks if chunk]


def build_structured_media_answer(
    *,
    deps: MediaAnswerRenderDeps,
    contract: MediaRenderContract,
    include_external_appendix: bool = True,
    citation_lookup: Callable[[dict[str, Any]], str] | None = None,
) -> str:
    if _should_suppress_structured_main_answer(contract):
        return ""
    parts = collect_structured_media_render_parts(
        deps=deps,
        contract=contract,
        citation_lookup=citation_lookup,
    )
    if not parts:
        return ""
    chunks = build_structured_media_answer_chunks(
        deps=deps,
        contract=contract,
        citation_lookup=citation_lookup,
    )
    if include_external_appendix:
        chunks.extend(str(block).strip() for block in parts.get("external_blocks", []) if str(block).strip())
    return "\n\n".join(chunk for chunk in chunks if chunk).strip()


def build_media_mention_appendix(*, deps: MediaAnswerRenderDeps, contract: MediaRenderContract) -> str:
    parts = collect_structured_media_render_parts(deps=deps, contract=contract)
    if not parts:
        return ""
    return str(parts.get("mention_block") or "").strip()


def append_media_mentions_to_answer(
    answer: str,
    *,
    deps: MediaAnswerRenderDeps,
    contract: MediaRenderContract,
) -> str:
    body = str(answer or "").strip()
    mention_appendix = build_media_mention_appendix(deps=deps, contract=contract)
    if not mention_appendix:
        return body
    if "扩展提及：" in body or "--- 扩展提及 ---" in body or "## 扩展提及" in body:
        return body
    if body:
        return f"{body}\n\n{mention_appendix}".strip()
    return mention_appendix
