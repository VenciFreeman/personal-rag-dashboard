from __future__ import annotations

import json
from json import JSONDecodeError
from typing import Any


BUG_TICKET_PREFIX = "BUG-TICKET:"
_VALID_SIMPLE_ESCAPES = {'"', "\\", "/", "b", "f", "n", "r", "t"}
_HEX_DIGITS = set("0123456789abcdefABCDEF")


def _strip_bug_ticket_prefix(text: str) -> str:
    raw = str(text or "").strip().strip("`")
    marker_index = raw.find(BUG_TICKET_PREFIX)
    if marker_index >= 0:
        raw = raw[marker_index + len(BUG_TICKET_PREFIX):].strip().strip("`")
    return raw


def _escape_invalid_json_backslashes(text: str) -> str:
    result: list[str] = []
    in_string = False
    index = 0
    while index < len(text):
        ch = text[index]
        if ch == '"':
            backslash_count = 0
            cursor = index - 1
            while cursor >= 0 and text[cursor] == "\\":
                backslash_count += 1
                cursor -= 1
            if backslash_count % 2 == 0:
                in_string = not in_string
            result.append(ch)
            index += 1
            continue
        if in_string and ch == "\\":
            next_index = index + 1
            if next_index >= len(text):
                result.append("\\\\")
                index += 1
                continue
            nxt = text[next_index]
            if nxt in _VALID_SIMPLE_ESCAPES:
                result.append("\\")
                result.append(nxt)
                index += 2
                continue
            if nxt == "u" and next_index + 4 < len(text) and all(char in _HEX_DIGITS for char in text[next_index + 1: next_index + 5]):
                result.append(text[index: next_index + 5])
                index = next_index + 5
                continue
            result.append("\\\\")
            index += 1
            continue
        result.append(ch)
        index += 1
    return "".join(result)


def parse_bug_ticket_payload(raw_text: str) -> dict[str, Any]:
    payload_text = _strip_bug_ticket_prefix(raw_text)
    if not payload_text:
        raise ValueError("未找到 BUG-TICKET JSON 内容")
    try:
        payload = json.loads(payload_text)
    except JSONDecodeError as exc:
        repaired = _escape_invalid_json_backslashes(payload_text)
        if repaired != payload_text:
            try:
                payload = json.loads(repaired)
            except JSONDecodeError:
                raise ValueError(f"BUG-TICKET JSON 解析失败: {exc.msg}") from exc
        else:
            raise ValueError(f"BUG-TICKET JSON 解析失败: {exc.msg}") from exc
    if not isinstance(payload, dict):
        raise ValueError("BUG-TICKET 内容必须是 JSON 对象")
    return payload