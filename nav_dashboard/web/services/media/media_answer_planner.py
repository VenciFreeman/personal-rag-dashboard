from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .media_render_contract import MediaRenderContract


@dataclass(frozen=True)
class StructuredMediaRenderPlan:
    intro_lines: tuple[str, ...] = ()
    row_groups: tuple[tuple[dict[str, Any], ...], ...] = ()
    row_group_headings: tuple[str, ...] = ()
    mention_rows: tuple[dict[str, Any], ...] = ()
    external_rows: tuple[dict[str, Any], ...] = ()


def _row_identity(row: dict[str, Any]) -> str:
    item_id = str(row.get("id") or "").strip()
    if item_id:
        return item_id
    return f"{str(row.get('media_type') or '').strip()}::{str(row.get('title') or '').strip()}"


def _rating_order_value(row: dict[str, Any], *, descending: bool) -> float:
    value = row.get("rating")
    try:
        return float(value)
    except Exception:
        return -1.0 if descending else 11.0


def _score_order_value(row: dict[str, Any], *, descending: bool) -> float:
    value = row.get("score")
    try:
        return float(value)
    except Exception:
        return -1.0 if descending else 0.0


def _split_ranked_review_rows(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    rated_rows = [row for row in rows if row.get("rating") not in {None, ""}]
    if len(rated_rows) < 2:
        return list(rows), [], []
    if len(rated_rows) <= 4:
        bucket_size = 1
    elif len(rated_rows) <= 6:
        bucket_size = 2
    else:
        bucket_size = 3
    sorted_high = sorted(
        rated_rows,
        key=lambda row: (
            _rating_order_value(row, descending=True),
            _score_order_value(row, descending=True),
            str(row.get("date") or ""),
        ),
        reverse=True,
    )
    sorted_low = sorted(
        rated_rows,
        key=lambda row: (
            _rating_order_value(row, descending=False),
            _score_order_value(row, descending=False),
            str(row.get("date") or ""),
        ),
    )
    top_rows: list[dict[str, Any]] = []
    bottom_rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in sorted_high:
        key = _row_identity(row)
        if key in seen:
            continue
        top_rows.append(row)
        seen.add(key)
        if len(top_rows) >= min(bucket_size, len(rated_rows) - 1):
            break
    for row in sorted_low:
        key = _row_identity(row)
        if key in seen:
            continue
        bottom_rows.append(row)
        seen.add(key)
        if len(bottom_rows) >= min(bucket_size, max(1, len(rated_rows) - len(top_rows))):
            break
    remaining_rows = [row for row in rows if _row_identity(row) not in seen]
    return top_rows, bottom_rows, remaining_rows


def build_structured_media_render_plan(contract: MediaRenderContract) -> StructuredMediaRenderPlan | None:
    if not contract.show_main or not contract.main_rows:
        return None

    limited_rows = list(contract.main_rows[: max(1, int(contract.row_limit or len(contract.main_rows) or 1))])
    intro_lines = tuple(str(line).strip() for line in contract.intro_lines if str(line).strip())
    mention_rows = tuple(row for row in list(contract.mention_rows) if isinstance(row, dict)) if contract.include_mentions else ()
    external_rows = tuple(
        row for row in list(contract.external_rows)[: max(1, int(contract.external_limit or len(contract.external_rows) or 1))] if isinstance(row, dict)
    )

    if contract.group_mode == "best_worst":
        top_rows, bottom_rows, remaining_rows = _split_ranked_review_rows(limited_rows)
        row_groups = tuple(tuple(group) for group in (top_rows, bottom_rows, remaining_rows) if group)
        headings = tuple(
            heading
            for heading, group in (
                ("## 评分最高", top_rows),
                ("## 评分较低", bottom_rows),
                ("## 其余条目", remaining_rows),
            )
            if group
        )
        return StructuredMediaRenderPlan(
            intro_lines=intro_lines,
            row_groups=row_groups,
            row_group_headings=headings,
            mention_rows=mention_rows,
            external_rows=external_rows,
        )

    if not limited_rows:
        return None

    primary_group = tuple(limited_rows[:3])
    secondary_group = tuple(limited_rows[3:])
    row_groups = tuple(group for group in (primary_group, secondary_group) if group)
    return StructuredMediaRenderPlan(
        intro_lines=intro_lines,
        row_groups=row_groups,
        row_group_headings=(),
        mention_rows=mention_rows,
        external_rows=external_rows,
    )
