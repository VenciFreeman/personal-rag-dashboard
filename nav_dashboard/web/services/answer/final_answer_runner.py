from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Callable

@dataclass
class FinalAnswerResult:
    final_answer: str
    references_markdown: str


def _parse_reference_sections(references_markdown: str) -> list[tuple[str, list[tuple[str, str]]]]:
    sections: list[tuple[str, list[tuple[str, str]]]] = []
    current_heading = ""
    current_entries: list[tuple[str, str]] = []
    for line in str(references_markdown or "").splitlines():
        if line.startswith("### "):
            if current_entries:
                sections.append((current_heading, current_entries))
            current_heading = line
            current_entries = []
            continue
        match = re.match(r"^(?:<a href=\"([^\"]+)\">\[(\d+)\]</a>|\[(\d+)\]\(([^)]+)\)|\[(\d+)\])\s+(.+)$", line)
        if match:
            current_entries.append((str(match.group(2) or match.group(3) or match.group(5) or ""), str(match.group(6) or "")))
    if current_entries:
        sections.append((current_heading, current_entries))
    return sections


def _reference_section_title(heading: str) -> str:
    return str(heading or "").replace("###", "", 1).strip()


def _extract_inline_reference_numbers(answer: str) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    pattern = re.compile(
        r'<a href="([^"]+)">\[(\d+)\]</a>|\[\[(\d+)\]\]\([^)]*\)|(?<!\[)\[(\d+)\]\(([^)]*)\)|(?<!\[)\[(\d+)\](?!\])(?!\()'
    )
    for match in pattern.finditer(str(answer or "")):
        index = str(match.group(2) or match.group(3) or match.group(4) or match.group(6) or "").strip()
        if not index or index in seen:
            continue
        seen.add(index)
        ordered.append(index)
    return ordered


def _filter_and_renumber_references(answer: str, references_markdown: str) -> tuple[str, str]:
    cited_numbers = _extract_inline_reference_numbers(answer)
    if not references_markdown:
        return answer, ""

    sections = _parse_reference_sections(references_markdown)
    if not cited_numbers:
        substantive_answer = len(re.sub(r"\s+", "", str(answer or ""))) >= 20
        if not substantive_answer:
            return answer, ""
        kept_sections: list[str] = []
        next_index = 1
        for heading, entries in sections:
            title = _reference_section_title(heading)
            if title not in {"本地媒体库参考", "本地文档参考"}:
                continue
            kept_entries = list(entries)[:5]
            if not kept_entries:
                continue
            rendered_entries = []
            for _original, body in kept_entries:
                rendered_entries.append(f"[{next_index}] {body}")
                next_index += 1
            kept_sections.append("\n".join([heading, *rendered_entries]))
        if not kept_sections:
            return answer, ""
        filtered_references = "\n\n" + "\n\n".join(kept_sections)
        return answer, filtered_references

    available_numbers = [index for _heading, entries in sections for index, _body in entries]
    available_set = set(available_numbers)

    kept_numbers = [index for index in cited_numbers if index in available_set]
    local_entries = next(
        (entries for heading, entries in sections if _reference_section_title(heading) == "本地媒体库参考"),
        [],
    )
    local_numbers = [index for index, _body in local_entries]
    local_kept = [index for index in kept_numbers if index in set(local_numbers)]
    minimum_local_refs = min(5, len(local_numbers))
    if local_numbers and len(local_kept) < minimum_local_refs:
        for index in local_numbers:
            if index in kept_numbers:
                continue
            kept_numbers.append(index)
            local_kept.append(index)
            if len(local_kept) >= minimum_local_refs:
                break
    if not kept_numbers or len(kept_numbers) == len(available_numbers):
        return answer, references_markdown

    number_map = {old: str(new_index) for new_index, old in enumerate(kept_numbers, start=1)}
    original_link_map = _extract_reference_links(references_markdown)

    def _replace_answer_marker(match: re.Match[str]) -> str:
        original = match.group(0)
        anchor_href = str(match.group(1) or "").strip()
        anchor_index = str(match.group(2) or "").strip()
        double_bracket_index = str(match.group(3) or "").strip()
        single_link_index = str(match.group(5) or "").strip()
        plain_index = str(match.group(7) or "").strip()
        if anchor_index:
            mapped = number_map.get(anchor_index)
            target = original_link_map.get(anchor_index) or anchor_href
            return f'<a href="{target}">[{mapped}]</a>' if mapped and target else original
        if double_bracket_index:
            mapped = number_map.get(double_bracket_index)
            return f"[[{mapped}]]({match.group(4)})" if mapped else original
        if single_link_index:
            mapped = number_map.get(single_link_index)
            return f"[{mapped}]({match.group(6)})" if mapped else original
        if plain_index:
            mapped = number_map.get(plain_index)
            return f"[{mapped}]" if mapped else original
        return original

    remapped_answer = re.sub(
        r'<a href="([^"]+)">\[(\d+)\]</a>|\[\[(\d+)\]\]\(([^)]*)\)|(?<!\[)\[(\d+)\]\(([^)]*)\)|(?<!\[)\[(\d+)\](?!\])(?!\()',
        _replace_answer_marker,
        str(answer or ""),
    )

    rendered_sections: list[str] = []
    for heading, entries in sections:
        kept_entries = [(number_map[index], body) for index, body in entries if index in number_map]
        kept_entries.sort(key=lambda item: int(item[0]))
        rendered_entries = [f"[{mapped}] {body}" for mapped, body in kept_entries]
        if rendered_entries:
            rendered_sections.append("\n".join(([heading] if heading else []) + rendered_entries))
    filtered_references = "\n\n" + "\n\n".join(rendered_sections) if rendered_sections else ""
    return remapped_answer, filtered_references


def _wrap_reference_sections(references_markdown: str) -> str:
    body = str(references_markdown or "").strip()
    if not body:
        return ""
    return f"\n\n## 参考资料\n\n{body}"


def _extract_reference_links(references_markdown: str) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for line in str(references_markdown or "").splitlines():
        anchor_match = re.match(r'^<a href="([^"]+)">\[(\d+)\]</a>', line)
        if anchor_match:
            mapping[str(anchor_match.group(2))] = str(anchor_match.group(1))
            continue
        marker_link_match = re.match(r"^\[(\d+)\]\(([^)]+)\)", line)
        if marker_link_match:
            mapping[str(marker_link_match.group(1))] = str(marker_link_match.group(2))
            continue
        label_link_match = re.match(r"^\[(\d+)\]\s+\[[^\]]+\]\(([^)]+)\)", line)
        if label_link_match:
            mapping[str(label_link_match.group(1))] = str(label_link_match.group(2))
    return mapping


def _linkify_reference_section_markers(references_markdown: str) -> str:
    link_map = _extract_reference_links(references_markdown)
    if not link_map:
        return str(references_markdown or "")

    linked_lines: list[str] = []
    for raw_line in str(references_markdown or "").splitlines():
        line = str(raw_line or "")
        match = re.match(r"^\[(\d+)\]\s+(.+)$", line)
        if not match:
            linked_lines.append(line)
            continue
        index = str(match.group(1) or "")
        url = link_map.get(index)
        if not url:
            linked_lines.append(line)
            continue
        if str(url).startswith("doc://"):
            linked_lines.append(line)
            continue
        linked_lines.append(f'<a href="{url}">[{index}]</a> {match.group(2)}')
    return "\n".join(linked_lines)


def _relocate_title_embedded_reference_markers(answer: str) -> str:
    text = str(answer or "")
    if not text:
        return text

    marker_pattern = r'(?:<a href="[^"]+">\[\d+\]</a>|\[\d+\]\([^)]+\)|\[\d+\])'

    def _move_marker_inside_title(match: re.Match[str]) -> str:
        title_head = re.sub(r"\s+$", "", str(match.group(1) or ""))
        marker = str(match.group(2) or "")
        title_tail = str(match.group(3) or "")
        return f"《{title_head}{title_tail}》{marker}"

    text = re.sub(rf"《([^》\n]*?)\s*({marker_pattern})([:：][^》\n]+)》", _move_marker_inside_title, text)
    text = re.sub(rf"《([^》\n]*?)\s*({marker_pattern})》", _move_marker_inside_title, text)
    return text


def _normalize_raw_doc_anchor_markers(answer: str) -> str:
    return re.sub(
        r'<a href="(doc://[^"]+)">\[(\d+)\]</a>',
        lambda match: f"[{match.group(2)}]({match.group(1)})",
        str(answer or ""),
    )


def _normalize_forbidden_local_recordese(answer: str) -> str:
    text = str(answer or "")
    if not text:
        return text
    text = re.sub(r"(?:本地知识库记录|本地资料库记录)\s*\[([0-9]{4}-[0-9]{2}-[0-9]{2})\]", r"你在 \1 的记录显示", text)
    text = re.sub(r"(?:本地知识库记录|本地资料库记录)\s*[:：]?", "按你的记录，", text)
    text = re.sub(r"记录时间\s*\[([^\]]+)\]", r"你在 \1 记录过", text)
    text = re.sub(r"评分\s*\[([^\]]+)\]", r"你当时给它 \1", text)
    text = re.sub(r"短评\s*\[([^\]]+)\]", r"你当时的感受是“\1”", text)
    text = re.sub(r"按你的记录，\s*按你的记录，", "按你的记录，", text)
    text = re.sub(r"，\s*，", "，", text)
    line_rewrites: list[str] = []
    for raw_line in text.splitlines():
        line = str(raw_line or "")
        stripped = line.strip()
        rating_match = re.match(r"^(?:[-*]\s*)?(?:\*\*)?(?:个人)?评分(?:\*\*)?[:：]\s*(.+)$", stripped)
        if rating_match:
            line_rewrites.append(f"你当时给它 {rating_match.group(1).strip()}。")
            continue
        review_match = re.match(r"^(?:[-*]\s*)?(?:\*\*)?(?:个人)?短评(?:\*\*)?[:：]\s*(.+)$", stripped)
        if review_match:
            line_rewrites.append(f"你当时的感受是“{review_match.group(1).strip()}”。")
            continue
        date_match = re.match(r"^(?:[-*]\s*)?(?:\*\*)?(?:观看/阅读日期|日期|记录时间)(?:\*\*)?[:：]\s*(.+)$", stripped)
        if date_match:
            line_rewrites.append(f"你是在 {date_match.group(1).strip()} 接触这部作品的。")
            continue
        line_rewrites.append(line)
    text = "\n".join(line_rewrites)
    text = re.sub(r"我当时给它", "你当时给它", text)
    text = re.sub(r"我当时的感受是", "你当时的感受是", text)
    text = re.sub(r"我是在", "你是在", text)
    text = re.sub(r"我在\s+([0-9]{4}-[0-9]{2}-[0-9]{2})\s+的记录显示", r"你在 \1 的记录显示", text)
    return text.strip()


def _strip_low_value_external_boilerplate(answer: str) -> str:
    text = str(answer or "").strip()
    if not text:
        return text

    sentence_patterns = [
        r"(?:外部)?维基百科(?:的)?参考资料指出[^。！？!?]*[。！？!?]?",
        r"仅供参考[，,、]?不能作为专业意见[。！？!?]?",
        r"值得注意的是[，,、]?(?:维基百科|Wiki)[^。！？!?]*(?:准确性争议|自相矛盾|不准确|有误|争议)[^。！？!?]*[。！？!?]?",
        r"(?:维基百科|Wiki)[^。！？!?]*(?:准确性争议|自相矛盾|不准确|有误|争议)[^。！？!?]*[。！？!?]?",
    ]
    for pattern in sentence_patterns:
        text = re.sub(pattern, "", text, flags=re.IGNORECASE)

    paragraphs: list[str] = []
    for raw_paragraph in re.split(r"\n\s*\n", text):
        paragraph = str(raw_paragraph or "").strip()
        if not paragraph:
            continue
        if (
            ("维基百科" in paragraph or "Wiki" in paragraph)
            and any(token in paragraph for token in ("仅供参考", "专业意见", "准确性争议", "自相矛盾", "参考资料指出"))
        ):
            continue
        paragraphs.append(paragraph)
    return "\n\n".join(paragraphs).strip()


def _normalize_model_generated_reference_tail(answer: str, references_markdown: str) -> str:
    text = str(answer or "").strip()
    if not text:
        return text
    available_numbers = set(_extract_reference_links(references_markdown).keys())

    marker_prefixed_source_pattern = re.compile(
        r'^(?:[-*]\s*)?(?:<a href="[^"]+">\[(\d+)\]</a>|\[(\d+)\](?:\([^)]+\))?)\s+(本地媒体库|本地文档|外部(?:参考|网页|TMDB|Bangumi)|外部\s*Wiki|Wiki|https?://)'
    )

    def _is_removable_tail_line(line: str) -> bool:
        stripped = str(line or "").strip()
        if not stripped:
            return True
        if stripped in {"---", "***", "___"}:
            return True
        if re.match(r"^(?:#{2,3}\s*)?(参考资料|参考来源|来源清单|尾注|引用)[:：]?$", stripped):
            return True
        pseudo_ref = re.match(r'^(?:[-*]\s*)?(?:<a href="[^"]+">\[(\d+)\]</a>|\[(\d+)\](?:\([^)]+\))?)\s+(本地媒体库:|本地文档:|外部参考:|外部\s*Wiki:|本地短评摘要|对应本地条目：|对应本地文档条目：|对应外部条目：)(.*)$', stripped)
        pseudo_ref_index = str(pseudo_ref.group(1) or pseudo_ref.group(2) or "") if pseudo_ref else ""
        if pseudo_ref_index and pseudo_ref_index in available_numbers:
            return True
        local_record_evidence = re.match(r'^(?:[-*]\s*)?(?:<a href="[^"]+">\[(\d+)\]</a>|\[(\d+)\](?:\([^)]+\))?)\s+本地记录证据(?:[:：].*)?$', stripped)
        local_record_index = str(local_record_evidence.group(1) or local_record_evidence.group(2) or "") if local_record_evidence else ""
        if local_record_index and local_record_index in available_numbers:
            return True
        compact_record_summary = re.match(r'^(?:[-*]\s*)?(?:<a href="[^"]+">\[(\d+)\]</a>|\[(\d+)\](?:\([^)]+\))?)\s+.+(?:\|\s*(?:日期|评分|短评)=.+)+$', stripped)
        compact_record_index = str(compact_record_summary.group(1) or compact_record_summary.group(2) or "") if compact_record_summary else ""
        if compact_record_index and compact_record_index in available_numbers:
            return True
        numbered_ref = re.match(r"^(\d+)\s+(本地媒体库|本地文档|外部(?:参考|网页|TMDB|Bangumi)|外部\s*Wiki|Wiki|https?://)", stripped)
        if numbered_ref and numbered_ref.group(1) in available_numbers:
            return True
        linked_ref = re.match(r'^(?:[-*]\s*)?\[(\d+)\](?:\([^)]+\))?\s+\[(?:本地媒体库[^\]]*|本地文档[^\]]*|外部[^\]]*)\]\([^)]+\)', stripped)
        if linked_ref and linked_ref.group(1) in available_numbers:
            return True
        linked_ref_text = re.match(r'^(?:[-*]\s*)?\[(\d+)\](?:\([^)]+\))?\s+\[(?:本地媒体库[^\]]*|本地文档[^\]]*|外部[^\]]*)\]$', stripped)
        if linked_ref_text and linked_ref_text.group(1) in available_numbers:
            return True
        anchor_ref = re.match(r'^(?:[-*]\s*)?<a href="[^"]+">\[(\d+)\]</a>\s+\[(?:本地媒体库[^\]]*|本地文档[^\]]*|外部[^\]]*)\]\([^)]+\)', stripped)
        if anchor_ref and anchor_ref.group(1) in available_numbers:
            return True
        anchor_ref_text = re.match(r'^(?:[-*]\s*)?<a href="[^"]+">\[(\d+)\]</a>\s+\[(?:本地媒体库[^\]]*|本地文档[^\]]*|外部[^\]]*)\]$', stripped)
        if anchor_ref_text and anchor_ref_text.group(1) in available_numbers:
            return True
        marker_prefixed_ref = marker_prefixed_source_pattern.match(stripped)
        marker_prefixed_index = str(marker_prefixed_ref.group(1) or marker_prefixed_ref.group(2) or "") if marker_prefixed_ref else ""
        if marker_prefixed_index and marker_prefixed_index in available_numbers:
            return True
        compact_title_ref = re.match(r'^(?:[-*]\s*)?(?:<a href="[^"]+">\[(\d+)\]</a>|\[(\d+)\](?:\([^)]+\))?)\s+(.+?)\s*$', stripped)
        compact_title_index = str(compact_title_ref.group(1) or compact_title_ref.group(2) or "") if compact_title_ref else ""
        compact_title_body = str(compact_title_ref.group(3) or "").strip() if compact_title_ref else ""
        is_compact_title_only = bool(re.fullmatch(r"《[^》]+》", compact_title_body) or re.fullmatch(r"[^。！？!?：:]{1,80}", compact_title_body))
        if compact_title_index and compact_title_index in available_numbers and compact_title_body and is_compact_title_only:
            return True
        mention_ref = re.match(r"^\[(\d+)\]\s+你在其他作品(?:的)?短评中也提到过", stripped)
        if mention_ref:
            return True
        if available_numbers and re.match(r"^(?:[-*]\s*)?(?:\*\*)?(?:个人)?评分(?:\*\*)?[:：=]\s*.+$", stripped):
            return True
        if available_numbers and re.match(r"^(?:[-*]\s*)?(?:\*\*)?(?:个人)?短评(?:\*\*)?[:：=]\s*.+$", stripped):
            return True
        if available_numbers and re.match(r"^(?:[-*]\s*)?(?:\*\*)?(?:观看/阅读日期|日期|记录时间)(?:\*\*)?[:：=]\s*.+$", stripped):
            return True
        return False

    lines = text.splitlines()
    while lines:
        tail = str(lines[-1] or "").strip()
        if _is_removable_tail_line(tail):
            lines.pop()
            continue
        break
    for index, line in enumerate(lines):
        stripped = str(line or "").strip()
        if not re.match(r"^(?:#{1,3}\s*)?(参考资料|参考来源|来源清单|尾注|引用)(?:[:：].*)?$", stripped):
            continue
        block_lines = lines[index + 1 :]
        if block_lines and all(
            not str(item or "").strip()
            or _is_removable_tail_line(item)
            or re.match(r"^(?:[-*]\s*)?\[(?:本地媒体库|本地文档|外部[^\]]*)\]\([^)]+\)$", str(item or "").strip())
            or re.match(r"^\d+\s+.+$", str(item or "").strip())
            for item in block_lines
        ):
            lines = lines[:index]
            break
    text = "\n".join(lines).strip()
    if available_numbers:
        text = re.sub(
            r"(?<=[。！？!?）】])\s*(\d{1,2})\s*$",
            lambda match: f" [{match.group(1)}]" if match.group(1) in available_numbers else match.group(0),
            text,
        )
    return text.strip()


def _strip_model_generated_local_reference_blocks(answer: str, references_markdown: str) -> str:
    text = str(answer or "").strip()
    if not text:
        return text
    available_numbers = set(_extract_reference_links(references_markdown).keys())

    marker_prefixed_source_pattern = re.compile(
        r'^(?:[-*]\s*)?(?:<a href="[^"]+">\[(\d+)\]</a>|\[(\d+)\](?:\([^)]+\))?)\s+(本地媒体库|本地文档|外部(?:参考|网页|TMDB|Bangumi)|外部\s*Wiki|Wiki|https?://)'
    )

    def _is_reference_artifact_line(line: str) -> bool:
        stripped = str(line or "").strip()
        if not stripped:
            return True
        if stripped in {"---", "***", "___"}:
            return True
        if re.match(r"^(?:#{1,3}\s*)?(本地媒体库参考|本地文档参考|外部参考|参考资料|参考来源|来源清单|引用)(?:[:：].*)?$", stripped):
            return True
        pseudo_ref = re.match(r'^(?:[-*]\s*)?(?:<a href="[^"]+">\[(\d+)\]</a>|\[(\d+)\](?:\([^)]+\))?)\s+(本地媒体库:|本地文档:|外部参考:|外部\s*Wiki:|本地短评摘要|对应本地条目：|对应本地文档条目：|对应外部条目：)(.*)$', stripped)
        pseudo_ref_index = str(pseudo_ref.group(1) or pseudo_ref.group(2) or "") if pseudo_ref else ""
        if pseudo_ref_index and pseudo_ref_index in available_numbers:
            return True
        local_record_evidence = re.match(r'^(?:[-*]\s*)?(?:<a href="[^"]+">\[(\d+)\]</a>|\[(\d+)\](?:\([^)]+\))?)\s+本地记录证据(?:[:：].*)?$', stripped)
        local_record_index = str(local_record_evidence.group(1) or local_record_evidence.group(2) or "") if local_record_evidence else ""
        if local_record_index and local_record_index in available_numbers:
            return True
        compact_record_summary = re.match(r'^(?:[-*]\s*)?(?:<a href="[^"]+">\[(\d+)\]</a>|\[(\d+)\](?:\([^)]+\))?)\s+.+(?:\|\s*(?:日期|评分|短评)=.+)+$', stripped)
        compact_record_index = str(compact_record_summary.group(1) or compact_record_summary.group(2) or "") if compact_record_summary else ""
        if compact_record_index and compact_record_index in available_numbers:
            return True
        if re.fullmatch(r"(?:<a href=\"[^\"]+\">\[\d+\]</a>\s*)+", stripped):
            return True
        if re.fullmatch(r"(?:\[\d+\]\([^)]+\)\s*)+", stripped):
            return True
        if re.fullmatch(r"(?:\[\[\d+\]\]\([^)]*\)\s*)+", stripped):
            return True
        numbered_ref = re.match(r"^(\d+)\s+(本地媒体库|本地文档|外部(?:参考|网页|TMDB|Bangumi)|外部\s*Wiki|Wiki|https?://)", stripped)
        if numbered_ref and numbered_ref.group(1) in available_numbers:
            return True
        linked_ref = re.match(r'^(?:[-*]\s*)?\[(\d+)\](?:\([^)]+\))?\s+\[(?:本地媒体库[^\]]*|本地文档[^\]]*|外部[^\]]*)\]\([^)]+\)', stripped)
        if linked_ref and linked_ref.group(1) in available_numbers:
            return True
        anchor_ref = re.match(r'^(?:[-*]\s*)?<a href="[^"]+">\[(\d+)\]</a>\s+\[(?:本地媒体库[^\]]*|本地文档[^\]]*|外部[^\]]*)\]\([^)]+\)', stripped)
        if anchor_ref and anchor_ref.group(1) in available_numbers:
            return True
        marker_prefixed_ref = marker_prefixed_source_pattern.match(stripped)
        marker_prefixed_index = str(marker_prefixed_ref.group(1) or marker_prefixed_ref.group(2) or "") if marker_prefixed_ref else ""
        if marker_prefixed_index and marker_prefixed_index in available_numbers:
            return True
        compact_title_ref = re.match(r'^(?:[-*]\s*)?(?:<a href="[^"]+">\[(\d+)\]</a>|\[(\d+)\](?:\([^)]+\))?)\s+(.+?)\s*$', stripped)
        compact_title_index = str(compact_title_ref.group(1) or compact_title_ref.group(2) or "") if compact_title_ref else ""
        compact_title_body = str(compact_title_ref.group(3) or "").strip() if compact_title_ref else ""
        is_compact_title_only = bool(re.fullmatch(r"《[^》]+》", compact_title_body) or re.fullmatch(r"[^。！？!?：:]{1,80}", compact_title_body))
        if compact_title_index and compact_title_index in available_numbers and compact_title_body and is_compact_title_only:
            return True
        mention_ref = re.match(r"^\[(\d+)\]\s+你在其他作品(?:的)?短评中也提到过", stripped)
        if mention_ref:
            return True
        return False

    lines = text.splitlines()
    block_start = None
    for index, raw_line in enumerate(lines):
        stripped = str(raw_line or "").strip()
        if not re.match(r"^(?:#{1,3}\s*)?(本地媒体库参考|本地文档参考|外部参考|参考资料|参考来源|来源清单|引用)(?:[:：].*)?$", stripped):
            continue
        tail_lines = lines[index:]
        if all(_is_reference_artifact_line(item) for item in tail_lines):
            block_start = index
            break
    if block_start is not None:
        lines = lines[:block_start]

    while lines:
        tail = str(lines[-1] or "").strip()
        if not tail:
            lines.pop()
            continue
        if re.fullmatch(r"(?:<a href=\"[^\"]+\">\[\d+\]</a>\s*)+", tail):
            lines.pop()
            continue
        if re.fullmatch(r"(?:\[\d+\]\([^)]+\)\s*)+", tail):
            lines.pop()
            continue
        if re.fullmatch(r"(?:\[\[\d+\]\]\([^)]*\)\s*)+", tail):
            lines.pop()
            continue
        if _is_reference_artifact_line(tail):
            lines.pop()
            continue
        break

    return "\n".join(lines).strip()


def _strip_trailing_reference_marker_clusters(answer: str, references_markdown: str) -> str:
    text = str(answer or "").rstrip()
    available_numbers = set(_extract_reference_links(references_markdown).keys())
    if not text or not available_numbers:
        return text

    marker_expr = r'(?:<a href="[^"]+">\[\d+\]</a>|\[\[\d+\]\]\([^)]*\)|\[\d+\]\([^)]+\)|\[\d+\])'
    marker_pattern = re.compile(
        r'<a href="[^"]+">\[(\d+)\]</a>|\[\[(\d+)\]\]\([^)]*\)|\[(\d+)\]\([^)]+\)|\[(\d+)\]'
    )
    trailing_cluster_pattern = re.compile(rf'(?P<cluster>(?:\s*{marker_expr}){{2,}})\s*$')

    while True:
        match = trailing_cluster_pattern.search(text)
        if not match:
            return text.strip()
        cluster = str(match.group("cluster") or "")
        cluster_numbers = [
            str(item.group(1) or item.group(2) or item.group(3) or item.group(4) or "")
            for item in marker_pattern.finditer(cluster)
        ]
        cluster_numbers = [number for number in cluster_numbers if number]
        if len(cluster_numbers) < 2 or any(number not in available_numbers for number in cluster_numbers):
            return text.strip()

        prefix = text[: match.start()].rstrip()
        if not prefix:
            return text.strip()

        earlier_numbers = set(_extract_inline_reference_numbers(prefix))
        if not earlier_numbers and not re.search(r"[。！？!?；;：:]$", prefix):
            return text.strip()

        if not set(cluster_numbers).issubset(earlier_numbers) and not re.search(r"[。！？!?；;：:]$", prefix):
            return text.strip()

        text = prefix


def _promote_list_heading_reference_markers(answer: str, references_markdown: str) -> str:
    available_numbers = set(_extract_reference_links(references_markdown).keys())
    if not available_numbers:
        return str(answer or "")

    marker_pattern = re.compile(r"(?<!\[)(\[(\d+)\](?:\([^)]+\))?)(?!\])")
    promoted_lines: list[str] = []
    for raw_line in str(answer or "").splitlines():
        line = str(raw_line or "")
        match = re.match(r"^(\s*(?:[-*]|\d+\.)\s*(?:\*\*[^*]+\*\*|[^：:]+?))(：|:)\s*(.+)$", line)
        if not match:
            promoted_lines.append(line)
            continue
        heading = str(match.group(1) or "")
        delimiter = str(match.group(2) or ":")
        rest = str(match.group(3) or "")
        if re.search(r"(?<!\[)\[\d+\](?:\([^)]+\))?(?!\])", heading):
            promoted_lines.append(line)
            continue
        marker_match = marker_pattern.search(rest)
        if not marker_match:
            promoted_lines.append(line)
            continue
        index = str(marker_match.group(2) or "")
        if index not in available_numbers:
            promoted_lines.append(line)
            continue
        promoted_marker = marker_match.group(1)
        trimmed_rest = (rest[: marker_match.start()] + rest[marker_match.end() :]).strip()
        trimmed_rest = re.sub(r"\s+([。！？；，,:：])", r"\1", trimmed_rest)
        sentence = f"{heading}{delimiter}{trimmed_rest}".rstrip()
        punctuation_match = re.search(r"([。！？!?])$", sentence)
        if punctuation_match:
            insert_at = punctuation_match.start(1)
            sentence = f"{sentence[:insert_at]} {promoted_marker}{sentence[insert_at:]}"
        else:
            sentence = f"{sentence} {promoted_marker}".rstrip()
        promoted_lines.append(sentence)
    return "\n".join(promoted_lines)


def _append_marker_to_line_end(line: str, marker: str) -> tuple[str, bool]:
    text = str(line or "")
    if not text.strip() or not marker:
        return text, False
    stripped = text.rstrip()
    if marker in stripped or re.search(r'<a href="[^"]+">\[\d+\]</a>|(?<!\[)\[\d+\](?:\([^)]+\))?(?!\])', stripped):
        return text, False
    punctuation_match = re.search(r"([。！？!?])$", stripped)
    if punctuation_match:
        insert_at = punctuation_match.start(1)
        return f"{stripped[:insert_at]} {marker}{stripped[insert_at:]}", True
    return f"{stripped} {marker}", True


def _inject_sequential_list_item_references(answer: str, references_markdown: str) -> str:
    text = str(answer or "")
    if not text or _extract_inline_reference_numbers(text):
        return text

    sections = _parse_reference_sections(references_markdown)
    candidate_entries: list[tuple[str, str]] = []
    for preferred_title in ("本地媒体库参考", "本地文档参考"):
        candidate_entries = next(
            (entries for heading, entries in sections if _reference_section_title(heading) == preferred_title),
            [],
        )
        if candidate_entries:
            break
    if not candidate_entries:
        return text

    candidate_numbers = [index for index, _body in candidate_entries if str(index or "").strip()]
    if not candidate_numbers:
        return text

    lines = text.splitlines()
    next_reference_index = 0
    updated = False
    for idx, raw_line in enumerate(lines):
        if next_reference_index >= len(candidate_numbers):
            break
        line = str(raw_line or "")
        stripped = line.strip()
        if not stripped or stripped.startswith("### "):
            continue
        if not re.match(r"^\s*(?:\d+\.\s+|[-*]\s+)", line):
            continue
        content = re.sub(r"^\s*(?:\d+\.\s+|[-*]\s+)", "", stripped)
        if len(re.sub(r"[^0-9A-Za-z\u4e00-\u9fff]+", "", content)) < 3:
            continue
        lines[idx], changed = _append_marker_to_line_end(line, f"[{candidate_numbers[next_reference_index]}]")
        if not changed:
            continue
        updated = True
        next_reference_index += 1
    return "\n".join(lines) if updated else text


def _normalize_inline_reference_artifacts(answer: str, references_markdown: str) -> str:
    text = str(answer or "")
    available_numbers = set(_extract_reference_links(references_markdown).keys())
    if not text or not available_numbers:
        return text

    def _bracket_if_known(match: re.Match[str]) -> str:
        index = str(match.group(1) or "")
        if index not in available_numbers:
            return match.group(0)
        return f"评价 [{index}]"

    def _replace_rating_artifact(match: re.Match[str]) -> str:
        rating = str(match.group(1) or "").strip()
        index = str(match.group(2) or "").strip()
        if index not in available_numbers:
            return match.group(0)
        return f"{rating}的评价 [{index}]"

    text = re.sub(r"评价(\d{1,2})(?=[。；，,\s]|$)", _bracket_if_known, text)
    text = re.sub(r"(\d{1,2}/10)的(?:我|你)当时给它\s+(\d{1,2})(?=[。；，,\s]|$)", _replace_rating_artifact, text)
    text = re.sub(r"(?:我|你)当时给它\s+(\d{1,2})(?=[。；，,\s]|$)", lambda m: f"[{m.group(1)}]" if str(m.group(1)) in available_numbers else m.group(0), text)
    return text


def _render_local_media_index_section(references_markdown: str, cited_references_markdown: str) -> str:
    sections = _parse_reference_sections(references_markdown)
    local_section = next((entries for heading, entries in sections if _reference_section_title(heading) == "本地媒体库参考"), [])
    if not local_section:
        return ""
    cited_sections = _parse_reference_sections(cited_references_markdown)
    kept_local_bodies: set[str] = set()
    for heading, entries in cited_sections:
        if _reference_section_title(heading) == "本地媒体库参考":
            kept_local_bodies = {body for _index, body in entries}
            break
    if not kept_local_bodies:
        return ""
    remaining_local_bodies = [body for _index, body in local_section if body not in kept_local_bodies]
    if not remaining_local_bodies:
        return ""

    lines = ["### 本地媒体库命中索引"]
    for body in remaining_local_bodies:
        lines.append(f"- {body}")
    return "\n".join(lines)


def _linkify_inline_reference_markers(answer: str, references_markdown: str) -> str:
    link_map = _extract_reference_links(references_markdown)
    if not link_map:
        return answer

    text = str(answer or "")

    def _preserve_existing_link(match: re.Match[str]) -> str:
        index = str(match.group(1) or "")
        url = str(match.group(2) or "")
        if index not in link_map:
            return match.group(0)
        target = url or link_map.get(index, "")
        if not target:
            return match.group(0)
        if str(target).startswith("doc://"):
            return f"[{index}]({target})"
        return f'<a href="{target}">[{index}]</a>'

    text = re.sub(r"(?<!\[)\[(\d+)\]\(([^)]+)\)", _preserve_existing_link, text)

    def _replace(match: re.Match[str]) -> str:
        index = str(match.group(1) or "")
        url = link_map.get(index)
        if not url:
            return match.group(0)
        if str(url).startswith("doc://"):
            return f"[{index}]({url})"
        return f'<a href="{url}">[{index}]</a>'

    text = re.sub(r"(?<!\[)(?<!>)\[(\d+)\](?!\])(?!\()", _replace, text)
    text = _normalize_raw_doc_anchor_markers(text)
    return _relocate_title_embedded_reference_markers(text)


def finalize_round_answer(
    *,
    answer: str,
    response_timing_breakdown: dict[str, Any],
    request_base_url: str,
    perf_counter: Callable[[], float],
    answer_has_inline_reference_markers: Callable[[str], bool],
    build_references_markdown: Callable[..., str],
    tool_results: list[Any],
    append_reference_sections: bool = True,
) -> FinalAnswerResult:
    response_finalize_t0 = perf_counter()
    references_t0 = perf_counter()
    original_references_md = build_references_markdown(
        tool_results,
        request_base_url=request_base_url,
        include_citation_line=not answer_has_inline_reference_markers(answer),
    )
    response_timing_breakdown["references_markdown_seconds"] = round(float(perf_counter() - references_t0), 6)
    normalized_answer = _normalize_model_generated_reference_tail(answer, original_references_md)
    normalized_answer = _normalize_forbidden_local_recordese(normalized_answer)
    normalized_answer = _strip_low_value_external_boilerplate(normalized_answer)
    normalized_answer = _strip_model_generated_local_reference_blocks(normalized_answer, original_references_md)
    normalized_answer = _strip_trailing_reference_marker_clusters(normalized_answer, original_references_md)
    remapped_answer, references_md = _filter_and_renumber_references(normalized_answer, original_references_md)
    remapped_answer = _inject_sequential_list_item_references(remapped_answer, references_md)
    remapped_answer = _normalize_inline_reference_artifacts(remapped_answer, references_md)
    remapped_answer = _promote_list_heading_reference_markers(remapped_answer, references_md)
    linked_answer = _linkify_inline_reference_markers(remapped_answer, references_md)
    linked_references_md = _linkify_reference_section_markers(references_md)
    local_media_index_md = _render_local_media_index_section(original_references_md, linked_references_md)
    if linked_references_md and local_media_index_md:
        linked_references_md = f"{linked_references_md}\n\n{local_media_index_md}".strip()
    elif local_media_index_md:
        linked_references_md = local_media_index_md
    rendered_suffix = _wrap_reference_sections(linked_references_md) if append_reference_sections else ""
    final_answer = f"{linked_answer}{rendered_suffix}" if rendered_suffix else linked_answer
    response_timing_breakdown["response_finalize_seconds"] = round(float(perf_counter() - response_finalize_t0), 6)
    return FinalAnswerResult(
        final_answer=final_answer,
        references_markdown=_wrap_reference_sections(linked_references_md) if linked_references_md else "",
    )