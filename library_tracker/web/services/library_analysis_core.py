from __future__ import annotations

import html
import json
import os
import re
import shutil
import ssl
import statistics
import threading
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlencode, urlparse
from urllib.request import HTTPSHandler, ProxyHandler, Request, build_opener

from core_service import get_settings
from core_service.runtime_data import app_runtime_root, legacy_app_runtime_root
from core_service.llm import chat_completion_with_retry
from core_service.reporting import (
    JOB_TYPE_LIBRARY_QUARTERLY,
    JOB_TYPE_LIBRARY_YEARLY,
    REPORT_BACKENDS,
    load_json_file,
    parse_report_record,
    truncate_text_by_chars,
)

from ..settings import MEDIA_FILES, get_entity_file_path

LIBRARY_RUNTIME_ROOT = app_runtime_root("library_tracker")
ANALYSIS_ROOT = LIBRARY_RUNTIME_ROOT / "analysis"
REPORTS_ROOT = ANALYSIS_ROOT / "reports"
LEGACY_ANALYSIS_ROOTS = (
    Path(__file__).resolve().parents[2] / "data" / "analysis",
    legacy_app_runtime_root("library_tracker") / "analysis",
)

REPORT_KIND_QUARTERLY = "quarterly"
REPORT_KIND_YEARLY = "yearly"
REPORT_KINDS = {REPORT_KIND_QUARTERLY, REPORT_KIND_YEARLY}
REPORT_KIND_TO_JOB_TYPE = {
    REPORT_KIND_QUARTERLY: JOB_TYPE_LIBRARY_QUARTERLY,
    REPORT_KIND_YEARLY: JOB_TYPE_LIBRARY_YEARLY,
}
REPORT_KIND_LABELS = {
    REPORT_KIND_QUARTERLY: "季度报告",
    REPORT_KIND_YEARLY: "年度报告",
}
REPORT_KIND_TITLES = {
    REPORT_KIND_QUARTERLY: "阅读影音季报",
    REPORT_KIND_YEARLY: "阅读影音年报",
}
STATE_PATH = ANALYSIS_ROOT / "scheduler_state.json"
EXTERNAL_REFERENCE_CACHE_PATH = ANALYSIS_ROOT / "external_reference_cache.json"
MEDIA_TYPES = ["book", "video", "music", "game"]
MEDIA_LABELS = {
    "book": "书籍",
    "video": "影视",
    "music": "音乐",
    "game": "游戏",
}
HTTP_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
TAVILY_MAX_CHARS = 2000
TAVILY_RESULT_LIMIT = 5
EXTERNAL_REFERENCE_MAX_UNCACHED_FETCHES = 1
REFERENCE_SUMMARY_INPUT_CHARS = 5200
REFERENCE_SUMMARY_OUTPUT_CHARS = 220
PROMPT_PAYLOAD_CHAR_BUDGET = {
    REPORT_KIND_QUARTERLY: 5600,
    REPORT_KIND_YEARLY: 3600,
}
SCHEDULER_INTERVAL_SECONDS = 1800

_LOCK = threading.Lock()
_SCHEDULER_STARTED = False
_SCHEDULER_STOP = threading.Event()


@dataclass(frozen=True)
class ReportPeriod:
    key: str
    label: str
    job_type: str


def ensure_analysis_storage() -> None:
    _migrate_legacy_analysis_root()
    for kind in REPORT_KINDS:
        (REPORTS_ROOT / kind).mkdir(parents=True, exist_ok=True)


def _merge_tree_missing_only(source: Path, target: Path) -> None:
    for item in source.rglob("*"):
        relative = item.relative_to(source)
        target_item = target / relative
        if item.is_dir():
            target_item.mkdir(parents=True, exist_ok=True)
            continue
        if target_item.exists():
            continue
        target_item.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(item, target_item)


def _migrate_legacy_analysis_root() -> None:
    ANALYSIS_ROOT.mkdir(parents=True, exist_ok=True)
    for source in LEGACY_ANALYSIS_ROOTS:
        if source == ANALYSIS_ROOT or not source.exists():
            continue
        try:
            _merge_tree_missing_only(source, ANALYSIS_ROOT)
        except Exception:
            continue


def _report_dir(kind: str) -> Path:
    return REPORTS_ROOT / kind


def _external_reference_cache_key(media_type: str, title: str) -> str:
    return f"{str(media_type or '').strip().lower()}::{str(title or '').strip().lower()}"


def _load_external_reference_cache() -> dict[str, dict[str, Any]]:
    ensure_analysis_storage()
    payload = load_json_file(EXTERNAL_REFERENCE_CACHE_PATH, {})
    if not isinstance(payload, dict):
        return {}
    output: dict[str, dict[str, Any]] = {}
    for key, value in payload.items():
        if not isinstance(key, str) or not isinstance(value, dict):
            continue
        output[key] = value
    return output


def _save_external_reference_cache(cache: dict[str, dict[str, Any]]) -> None:
    ensure_analysis_storage()
    EXTERNAL_REFERENCE_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = EXTERNAL_REFERENCE_CACHE_PATH.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(EXTERNAL_REFERENCE_CACHE_PATH)


def _http_json(
    url: str,
    headers: dict[str, str] | None = None,
    timeout: int = 20,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    body = None
    request_headers = dict(headers or {})
    if payload is not None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        request_headers.setdefault("Content-Type", "application/json")
    request = Request(url, data=body, headers=request_headers)
    opener = build_opener(ProxyHandler({}), HTTPSHandler(context=ssl.create_default_context()))
    try:
        with opener.open(request, timeout=timeout) as response:
            raw = response.read().decode("utf-8", errors="replace")
        data = json.loads(raw)
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _item_preview_url(item_id: str) -> str:
    return f"/?item={quote(str(item_id or '').strip())}"


def _item_markdown_link(title: Any, item_id: Any) -> str:
    clean_title = str(title or "").strip()
    clean_item_id = str(item_id or "").strip()
    if not clean_title:
        return "—"
    wrapped_title = f"《{clean_title}》"
    if not clean_item_id:
        return wrapped_title
    return f"[{wrapped_title}]({_item_preview_url(clean_item_id)})"


def _bold_leading_label(text: str) -> str:
    match = re.match(r"^([^：]{1,8})：(.*)$", str(text or "").strip())
    if not match:
        return str(text or "").strip()
    label = str(match.group(1) or "").strip()
    body = str(match.group(2) or "").strip()
    return f"**{label}：** {body}" if body else f"**{label}：**"


def _parse_item_date(value: Any) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    match = re.match(r"^(\d{4})-(\d{1,2})-(\d{1,2})$", text)
    if not match:
        return None
    try:
        return date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
    except Exception:
        return None


def _split_tags(value: Any) -> list[str]:
    text = str(value or "").strip()
    if not text:
        return []
    parts = re.split(r"[、,，/｜|;；]+", text)
    return [item.strip() for item in parts if item and item.strip()]


def _safe_rating(value: Any) -> float | None:
    try:
        rating = float(value)
    except Exception:
        return None
    return rating if rating >= 0 else None


def _format_rating(value: float | None) -> str:
    return f"{value:.2f}" if value is not None else "—"


def _format_rating_delta(value: float | None) -> str:
    return f"{value:+.2f}" if value is not None else "—"


def _quarter_period_from_key(period_key: str) -> ReportPeriod:
    match = re.match(r"^(\d{4})-Q([1-4])$", str(period_key or "").strip())
    if not match:
        raise ValueError("invalid quarterly period")
    year = int(match.group(1))
    quarter = int(match.group(2))
    key = f"{year:04d}-Q{quarter}"
    return ReportPeriod(key=key, label=f"{year} 年第 {quarter} 季度", job_type=JOB_TYPE_LIBRARY_QUARTERLY)


def _year_period_from_key(period_key: str) -> ReportPeriod:
    match = re.match(r"^(\d{4})$", str(period_key or "").strip())
    if not match:
        raise ValueError("invalid yearly period")
    year = int(match.group(1))
    key = f"{year:04d}"
    return ReportPeriod(key=key, label=f"{year} 年度", job_type=JOB_TYPE_LIBRARY_YEARLY)


def _previous_quarter_period(today: date | None = None) -> ReportPeriod:
    base = today or date.today()
    quarter = ((base.month - 1) // 3) + 1
    if quarter == 1:
        return _quarter_period_from_key(f"{base.year - 1}-Q4")
    return _quarter_period_from_key(f"{base.year}-Q{quarter - 1}")


def _previous_year_period(today: date | None = None) -> ReportPeriod:
    base = today or date.today()
    return _year_period_from_key(str(base.year - 1))


def _quarter_date_range(period_key: str) -> tuple[date, date]:
    period = _quarter_period_from_key(period_key)
    year, quarter = period.key.split("-Q")
    start_month = (int(quarter) - 1) * 3 + 1
    start = date(int(year), start_month, 1)
    if start_month == 10:
        end = date(int(year) + 1, 1, 1) - timedelta(days=1)
    else:
        end = date(int(year), start_month + 3, 1) - timedelta(days=1)
    return start, end


def _year_date_range(period_key: str) -> tuple[date, date]:
    year = int(period_key)
    return date(year, 1, 1), date(year, 12, 31)


def _previous_period_key(kind: str, period_key: str) -> str:
    if kind == REPORT_KIND_QUARTERLY:
        year, quarter_text = period_key.split("-Q")
        year_no = int(year)
        quarter = int(quarter_text)
        if quarter == 1:
            return f"{year_no - 1}-Q4"
        return f"{year_no}-Q{quarter - 1}"
    return str(int(period_key) - 1)


def _period_date_range(kind: str, period_key: str) -> tuple[date, date]:
    return _quarter_date_range(period_key) if kind == REPORT_KIND_QUARTERLY else _year_date_range(period_key)


def _load_records() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for media_type, file_name in MEDIA_FILES.items():
        del file_name
        payload = load_json_file(get_entity_file_path(media_type), {})
        records = payload.get("records") if isinstance(payload, dict) else []
        if not isinstance(records, list):
            continue
        for index, item in enumerate(records):
            if not isinstance(item, dict):
                continue
            normalized = dict(item)
            normalized["media_type"] = str(normalized.get("media_type") or media_type).strip().lower() or media_type
            normalized["id"] = f"{media_type}:{index}"
            rows.append(normalized)
    return rows


def _records_for_period(kind: str, period_key: str) -> list[dict[str, Any]]:
    start, end = _period_date_range(kind, period_key)
    rows: list[dict[str, Any]] = []
    for item in _load_records():
        item_date = _parse_item_date(item.get("date"))
        if item_date is None or item_date < start or item_date > end:
            continue
        rows.append(item)
    rows.sort(key=lambda row: (str(row.get("date") or ""), str(row.get("title") or "")), reverse=True)
    return rows


def _average_rating(rows: list[dict[str, Any]]) -> float | None:
    values = [_safe_rating(row.get("rating")) for row in rows]
    valid = [value for value in values if value is not None]
    if not valid:
        return None
    return sum(valid) / len(valid)


def _rating_values(rows: list[dict[str, Any]]) -> list[float]:
    values = [_safe_rating(row.get("rating")) for row in rows]
    return [value for value in values if value is not None]


def _rating_stddev(rows: list[dict[str, Any]]) -> float | None:
    values = _rating_values(rows)
    if len(values) <= 1:
        return 0.0 if values else None
    try:
        return float(statistics.pstdev(values))
    except Exception:
        return None


def _rating_stability_label(rows: list[dict[str, Any]]) -> str:
    values = _rating_values(rows)
    if len(values) <= 1:
        return "样本较少"
    spread = _rating_stddev(rows)
    if spread is None:
        return "样本不足"
    if spread <= 0.45:
        return "较稳定"
    if spread <= 0.95:
        return "中等波动"
    return "波动较明显"


def _count_delta_value(current: int, previous: int) -> int:
    return int(current) - int(previous)


def _rating_delta_value(current: float | None, previous: float | None) -> float | None:
    if current is None or previous is None:
        return None
    return current - previous


def _top_counter(rows: list[dict[str, Any]], field: str, *, split_multi: bool = True) -> str:
    counter: Counter[str] = Counter()
    for row in rows:
        values = _split_tags(row.get(field)) if split_multi else [str(row.get(field) or "").strip()]
        values = [value for value in values if value]
        for value in values:
            counter[value] += 1
    if not counter:
        return "—"
    value, count = sorted(counter.items(), key=lambda item: (-item[1], item[0]))[0]
    return f"{value} ({count})"


def _counter_details(rows: list[dict[str, Any]], field: str, *, split_multi: bool = True, top_n: int = 3) -> list[dict[str, Any]]:
    counter: Counter[str] = Counter()
    total = 0
    for row in rows:
        values = _split_tags(row.get(field)) if split_multi else [str(row.get(field) or "").strip()]
        values = [value for value in values if value]
        for value in values:
            counter[value] += 1
            total += 1
    if not counter or total <= 0:
        return []
    details: list[dict[str, Any]] = []
    for value, count in sorted(counter.items(), key=lambda item: (-item[1], item[0]))[:top_n]:
        details.append({"label": value, "count": count, "share": count / total})
    return details


def _format_counter_details(details: list[dict[str, Any]]) -> str:
    if not details:
        return "—"
    return "；".join(f"{item['label']} ({item['count']})" for item in details)


def _concentration_label(details: list[dict[str, Any]]) -> str:
    if not details:
        return "无明显集中"
    top_share = float(details[0].get("share") or 0.0)
    if top_share >= 0.65:
        return "高度集中"
    if top_share >= 0.4:
        return "相对集中"
    return "较分散"


def _representative_item(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not rows:
        return None
    ranked = sorted(rows, key=lambda row: (len(str(row.get("review") or "")), _safe_rating(row.get("rating")) or -1, str(row.get("date") or "")), reverse=True)
    return ranked[0]


def _excerpt(text: Any, limit: int = 50) -> str:
    clean = re.sub(r"\s+", " ", str(text or "").strip())
    return truncate_text_by_chars(clean, limit) if clean else "—"


def _split_sentences(text: Any) -> list[str]:
    clean = re.sub(r"\s+", " ", str(text or "").strip())
    if not clean:
        return []
    parts = re.split(r"(?<=[。！？!?])\s+|(?<=[。！？!?])", clean)
    return [part.strip() for part in parts if part and part.strip()]


def _structured_reference(reference: dict[str, str]) -> dict[str, str]:
    title = str(reference.get("title") or "").strip()
    summary = str(reference.get("summary") or "").strip()
    url = str(reference.get("url") or "").strip()
    lowered = summary.lower()
    if not summary or any(token in lowered for token in ("未配置", "暂时不可用", "timed out", "timeout", "<urlopen", "未找到可用", "缺少 id", "缺少简介")):
        return {"title": title, "intro": "", "background": "", "extra": "", "url": url}
    raw_sentences = _split_sentences(summary)
    sentences: list[str] = []
    seen_sentences: set[str] = set()
    for sentence in raw_sentences:
        normalized = str(sentence or "").strip().rstrip("；;，,。 ")
        if not normalized or normalized in seen_sentences:
            continue
        seen_sentences.add(normalized)
        sentences.append(normalized)
    intro = truncate_text_by_chars(" ".join(sentences[:2]) or summary.rstrip("；;，,。 "), 140)
    background = truncate_text_by_chars(sentences[2] if len(sentences) >= 3 else "", 90)
    extra = truncate_text_by_chars(sentences[3] if len(sentences) >= 4 else "", 60)
    return {"title": title, "intro": intro or "", "background": background or "", "extra": extra or "", "url": url}


def _empty_reference_payload(title: str = "") -> dict[str, str]:
    return {"title": title, "intro": "", "background": "", "extra": "", "url": ""}


def _reference_has_content(reference: dict[str, str]) -> bool:
    return any(str(reference.get(key) or "").strip() for key in ("intro", "background", "extra"))


def _representative_reference_priority(media_type: str, item: dict[str, Any] | None) -> tuple[int, float, str, int]:
    if not item:
        return (-1, -1.0, "", MEDIA_TYPES.index(media_type) if media_type in MEDIA_TYPES else len(MEDIA_TYPES))
    review_length = len(str(item.get("review") or "").strip())
    rating = _safe_rating(item.get("rating")) or -1.0
    item_date = str(item.get("date") or "")
    media_index = MEDIA_TYPES.index(media_type) if media_type in MEDIA_TYPES else len(MEDIA_TYPES)
    return (-review_length, rating, item_date, -media_index)


def _select_uncached_reference_fetches(
    representatives: dict[str, dict[str, Any] | None],
    cache: dict[str, dict[str, Any]],
    max_uncached_fetches: int = EXTERNAL_REFERENCE_MAX_UNCACHED_FETCHES,
) -> set[str]:
    if max_uncached_fetches <= 0:
        return set()
    candidates: list[tuple[tuple[int, float, str, int], str]] = []
    for media_type, item in representatives.items():
        if not item:
            continue
        title = str(item.get("title") or "").strip()
        if not title:
            continue
        cache_key = _external_reference_cache_key(media_type, title)
        if cache_key in cache:
            continue
        candidates.append((_representative_reference_priority(media_type, item), media_type))
    candidates.sort(reverse=True)
    return {media_type for _priority, media_type in candidates[:max_uncached_fetches]}


def _tavily_search_key() -> str:
    return str(os.getenv("TAVILY_API_KEY") or "").strip()


def _reference_site_keyword(media_type: str) -> str:
    return "steam" if media_type == "game" else "豆瓣"


def _external_search_query(media_type: str, title: str) -> str:
    media_hint = {
        "book": "书籍 小说 简介 作者",
        "video": "电影 动画 剧情 简介",
        "music": "专辑 音乐 简介 乐评",
        "game": "game overview story features",
    }.get(media_type, "作品 简介")
    return f'"{title}" {_reference_site_keyword(media_type)} {media_hint}'


def _reference_title_tokens(title: str) -> list[str]:
    parts = re.findall(r"[A-Za-z0-9\u4e00-\u9fff]{2,}", str(title or ""))
    seen: set[str] = set()
    output: list[str] = []
    for part in parts:
        token = str(part).strip().lower()
        if not token or token in seen:
            continue
        seen.add(token)
        output.append(token)
    return output[:8]


def _clean_reference_text(text: str) -> str:
    raw = str(text or "")
    if not raw:
        return ""
    cleaned = html.unescape(raw)
    cleaned = re.sub(r"https?://\S+", " ", cleaned)
    cleaned = re.sub(r"<[^>]+>", " ", cleaned)
    cleaned = re.sub(r"[`*_#>]+", " ", cleaned)
    cleaned = cleaned.replace("|", " ")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if not cleaned:
        return ""
    noise_tokens = ("首页", "搜索结果", "排行榜", "客户端", "立即下载", "查看详情", "全部", "登录", "注册", "下载", "用户评分", "短评", "影评", "评论区", "广告", "菜单", "导航")
    segments = re.split(r"(?<=[。！？!?])\s+|(?<=[。！？!?])|\s*[;；]+\s*", cleaned)
    kept: list[str] = []
    seen: set[str] = set()
    for segment in segments:
        item = re.sub(r"\s+", " ", str(segment or "")).strip(" -:：，,。；;[]()")
        if len(item) < 12:
            continue
        if sum(1 for token in noise_tokens if token in item) >= 2:
            continue
        if item in seen:
            continue
        seen.add(item)
        kept.append(item)
    return truncate_text_by_chars(" ".join(kept), TAVILY_MAX_CHARS)


def _reference_candidate_score(media_type: str, title: str, row: dict[str, Any], cleaned_text: str) -> tuple[int, int, int]:
    domain = (urlparse(str(row.get("url") or "")).netloc or "").lower()
    score = 0
    if media_type == "game":
        if "steam" in domain:
            score += 8
    elif "douban" in domain:
        score += 8
    tokens = _reference_title_tokens(title)
    haystack = f"{row.get('title') or ''} {cleaned_text}".lower()
    score += sum(2 for token in tokens if token and token in haystack)
    hint_tokens = {
        "book": ("作者", "小说", "出版", "讲述", "故事"),
        "video": ("动画", "电影", "剧集", "剧情", "讲述"),
        "music": ("专辑", "歌曲", "音乐", "发行", "收录"),
        "game": ("游戏", "steam", "扮演", "冒险", "玩法"),
    }.get(media_type, ())
    score += sum(1 for token in hint_tokens if token in cleaned_text)
    return (score, len(cleaned_text), -len(domain))


def _fallback_reference_summary(title: str, media_type: str, cleaned_text: str) -> str:
    sentences = _split_sentences(cleaned_text)
    filtered = [sentence for sentence in sentences if len(sentence) >= 12][:3]
    if filtered:
        return truncate_text_by_chars(" ".join(filtered), REFERENCE_SUMMARY_OUTPUT_CHARS)
    media_label = MEDIA_LABELS.get(media_type, "作品")
    return f"《{title}》是本期选出的代表{media_label}样本，检索结果可用信息有限，暂时只确认到它是与当前记录方向高度相关的一部作品。"


def _reference_summary_messages(title: str, media_type: str, snippets: list[str]) -> list[dict[str, str]]:
    media_label = MEDIA_LABELS.get(media_type, "作品")
    joined = "\n\n".join(f"片段 {index + 1}: {snippet}" for index, snippet in enumerate(snippets) if snippet)
    return [
        {
            "role": "system",
            "content": (
                "你是中文作品简介整理助手。"
                "请基于检索片段，写 2 到 3 句中文简介。"
                "只保留作品本身的信息：它是什么、核心设定/主题/背景。"
                "不要照抄原文，不要保留网站导航、评分、购买、榜单、评论摘录、下载提示。"
                "不要写‘根据搜索结果’、‘该网页显示’之类的话。"
                "如果资料不足，就保守概括，不要编造。"
            ),
        },
        {"role": "user", "content": f"作品名：{title}\n媒介：{media_label}\n\n检索片段：\n{joined}\n\n请直接输出中文简介正文。"},
    ]


def _summarize_reference_text(title: str, media_type: str, snippets: list[str]) -> str:
    usable = [truncate_text_by_chars(str(snippet or "").strip(), 1800) for snippet in snippets if str(snippet or "").strip()]
    if not usable:
        return ""
    settings = get_settings()
    local_backend = None
    if settings.local_llm_api_key and settings.local_llm_model and settings.local_llm_url:
        local_backend = (settings.local_llm_api_key, settings.local_llm_url, settings.local_llm_model)
    messages = _reference_summary_messages(title, media_type, usable)
    if local_backend is not None:
        api_key, base_url, model = local_backend
        try:
            text = chat_completion_with_retry(
                api_key=api_key,
                base_url=base_url,
                model=model,
                timeout=min(20, max(8, int(settings.timeout or 20))),
                messages=messages,
                temperature=0.1,
                max_retries=1,
                retry_delay=1.0,
                max_tokens=180,
            ).strip()
            compact = re.sub(r"\s+", " ", text).strip()
            unique_sentences: list[str] = []
            seen_sentences: set[str] = set()
            for sentence in _split_sentences(compact):
                normalized = str(sentence or "").strip().rstrip("；;，,。 ")
                if not normalized or normalized in seen_sentences:
                    continue
                seen_sentences.add(normalized)
                unique_sentences.append(normalized)
            cleaned = truncate_text_by_chars("。 ".join(unique_sentences[:3]).strip(), REFERENCE_SUMMARY_OUTPUT_CHARS)
            if cleaned:
                return cleaned
        except Exception:
            pass
    return _fallback_reference_summary(title, media_type, " ".join(usable))


def _fetch_tavily_reference(
    media_type: str,
    title: str,
    cache: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    cache_key = _external_reference_cache_key(media_type, title)
    if cache is not None:
        cached = cache.get(cache_key)
        if isinstance(cached, dict):
            return {
                "title": str(cached.get("title") or title).strip() or title,
                "summary": str(cached.get("summary") or "").strip(),
                "url": str(cached.get("url") or "").strip(),
                "web_search_calls": 0,
            }
    api_key = _tavily_search_key()
    if not api_key:
        return {"title": title, "summary": "", "url": "", "web_search_calls": 0}
    payload = _http_json(
        "https://api.tavily.com/search",
        headers={"User-Agent": HTTP_USER_AGENT},
        timeout=12,
        payload={
            "api_key": api_key,
            "query": _external_search_query(media_type, title),
            "max_results": TAVILY_RESULT_LIMIT,
            "search_depth": "advanced",
            "include_answer": False,
            "include_raw_content": True,
        },
    )
    rows = payload.get("results") if isinstance(payload.get("results"), list) else []
    if not rows:
        result = {"title": title, "summary": "", "url": "", "web_search_calls": 1}
        if cache is not None:
            cache[cache_key] = {
                "title": title,
                "summary": "",
                "url": "",
                "cached_at": datetime.now().isoformat(timespec="seconds"),
            }
        return result
    candidates: list[tuple[tuple[int, int, int], dict[str, Any], str]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        combined = " ".join(part for part in [str(row.get("content") or "").strip(), str(row.get("raw_content") or "").strip()] if part)
        cleaned = _clean_reference_text(truncate_text_by_chars(combined, REFERENCE_SUMMARY_INPUT_CHARS))
        if not cleaned:
            continue
        candidates.append((_reference_candidate_score(media_type, title, row, cleaned), row, cleaned))
    if not candidates:
        result = {"title": title, "summary": "", "url": "", "web_search_calls": 1}
        if cache is not None:
            cache[cache_key] = {
                "title": title,
                "summary": "",
                "url": "",
                "cached_at": datetime.now().isoformat(timespec="seconds"),
            }
        return result
    candidates.sort(key=lambda item: item[0], reverse=True)
    best_row = candidates[0][1]
    snippets = [item[2] for item in candidates[:2]]
    summary = _summarize_reference_text(title, media_type, snippets)
    result = {"title": title, "summary": summary, "url": str(best_row.get("url") or "").strip(), "web_search_calls": 1}
    if cache is not None:
        cache[cache_key] = {
            "title": title,
            "summary": summary,
            "url": str(best_row.get("url") or "").strip(),
            "cached_at": datetime.now().isoformat(timespec="seconds"),
        }
    return result


def _representative_references(representatives: dict[str, dict[str, Any] | None]) -> tuple[dict[str, dict[str, str]], dict[str, Any]]:
    output: dict[str, dict[str, str]] = {}
    usage = {"web_search_calls": 0, "titles": []}
    cache = _load_external_reference_cache()
    original_cache_snapshot = json.dumps(cache, ensure_ascii=False, sort_keys=True)
    uncached_fetch_media_types = _select_uncached_reference_fetches(representatives, cache)
    for media_type, item in representatives.items():
        if not item:
            output[media_type] = _empty_reference_payload(MEDIA_LABELS[media_type])
            continue
        title = str(item.get("title") or "").strip() or MEDIA_LABELS[media_type]
        cache_key = _external_reference_cache_key(media_type, title)
        try:
            if cache_key not in cache and media_type not in uncached_fetch_media_types:
                output[media_type] = _empty_reference_payload(title)
                continue
            raw_reference = _fetch_tavily_reference(media_type, title, cache=cache)
            usage["web_search_calls"] = int(usage.get("web_search_calls", 0) or 0) + int(raw_reference.get("web_search_calls", 0) or 0)
            if int(raw_reference.get("web_search_calls", 0) or 0) > 0:
                usage_titles = usage.get("titles") if isinstance(usage.get("titles"), list) else []
                usage_titles.append(title)
                usage["titles"] = usage_titles
            reference = _structured_reference(raw_reference)
            output[media_type] = reference if _reference_has_content(reference) else _empty_reference_payload(title)
        except Exception:
            output[media_type] = _empty_reference_payload(title)
    if json.dumps(cache, ensure_ascii=False, sort_keys=True) != original_cache_snapshot:
        _save_external_reference_cache(cache)
    usage["titles"] = [str(title).strip() for title in usage.get("titles") or [] if str(title).strip()]
    return output, usage


def _media_blocks(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    output: dict[str, list[dict[str, Any]]] = {media_type: [] for media_type in MEDIA_TYPES}
    for row in rows:
        media_type = str(row.get("media_type") or "").strip().lower()
        if media_type in output:
            output[media_type].append(row)
    return output


def _representative_basis(media_label: str, representative: dict[str, Any] | None, distribution: dict[str, Any], rows: list[dict[str, Any]]) -> str:
    del rows
    if not representative:
        return f"本期 {media_label} 样本为空。"
    category = distribution.get("top_category") or "—"
    nationality = distribution.get("top_nationality") or "—"
    review_length = len(str(representative.get("review") or "").strip())
    if review_length >= 40:
        return f"这部作品同时落在本期更常见的 {category} / {nationality} 方向，并且留下了较完整的个人评价，适合作为该媒介的代表样本。"
    return f"这部作品落在本期更常见的 {category} / {nationality} 方向，也处在本期较高评价的一档，足以代表该媒介的本期样本。"


def _comparison_rows(current_summary: dict[str, Any], previous_summary: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for media_type in MEDIA_TYPES:
        current = current_summary.get(media_type, {})
        previous = previous_summary.get(media_type, {})
        rows.append(
            {
                "media_type": media_type,
                "media_label": MEDIA_LABELS[media_type],
                "current_count": int(current.get("count") or 0),
                "previous_count": int(previous.get("count") or 0),
                "count_delta": _count_delta_value(int(current.get("count") or 0), int(previous.get("count") or 0)),
                "current_avg_rating": current.get("avg_rating"),
                "previous_avg_rating": previous.get("avg_rating"),
                "rating_delta": _rating_delta_value(current.get("avg_rating"), previous.get("avg_rating")),
            }
        )
    return rows


def _rating_rows(current_summary: dict[str, Any], previous_summary: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for media_type in MEDIA_TYPES:
        current = current_summary.get(media_type, {})
        previous = previous_summary.get(media_type, {})
        rows.append(
            {
                "media_type": media_type,
                "media_label": MEDIA_LABELS[media_type],
                "current_avg_rating": current.get("avg_rating"),
                "previous_avg_rating": previous.get("avg_rating"),
                "rating_delta": _rating_delta_value(current.get("avg_rating"), previous.get("avg_rating")),
                "rating_stddev": current.get("rating_stddev"),
                "stability_label": current.get("stability_label") or "样本不足",
            }
        )
    return rows


def _best_direction(rows: list[dict[str, Any]], field: str, *, split_multi: bool = True) -> str:
    buckets: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        rating = _safe_rating(row.get("rating"))
        if rating is None:
            continue
        values = _split_tags(row.get(field)) if split_multi else [str(row.get(field) or "").strip()]
        values = [value for value in values if value]
        for value in values:
            buckets[value].append(rating)
    if not buckets:
        return "—"
    ranked = sorted(((label, sum(values) / len(values), len(values)) for label, values in buckets.items()), key=lambda item: (-item[1], -item[2], item[0]))
    label, avg_rating, count = ranked[0]
    return f"{label}（均分 {avg_rating:.2f} / {count} 条）"


def _focus_candidates(current_summary: dict[str, Any], previous_summary: dict[str, Any], distribution: dict[str, Any], rows_by_media: dict[str, list[dict[str, Any]]]) -> list[str]:
    candidates: list[str] = []
    growth_rows = sorted(_comparison_rows(current_summary, previous_summary), key=lambda row: (row["count_delta"], row["current_count"]), reverse=True)
    if growth_rows and growth_rows[0]["count_delta"] > 0:
        top = growth_rows[0]
        dist = distribution.get(top["media_type"], {})
        candidates.append(f"数量增长显著的是{top['media_label']}，可继续顺着 {dist.get('top_category', '—')} / {dist.get('top_channel', '—')} 方向补充。")
    rating_rows = sorted(_rating_rows(current_summary, previous_summary), key=lambda row: (row["current_avg_rating"] if row["current_avg_rating"] is not None else -1), reverse=True)
    if rating_rows and rating_rows[0]["current_avg_rating"] is not None:
        top = rating_rows[0]
        top_rows = rows_by_media.get(top["media_type"], [])
        best_category = _best_direction(top_rows, "category")
        candidates.append(f"当前均分最高的是{top['media_label']}，可优先沿着 {best_category} 继续追踪。")
    seen: set[str] = set()
    output: list[str] = []
    for candidate in candidates:
        text = truncate_text_by_chars(candidate, 80)
        if not text or text in seen:
            continue
        seen.add(text)
        output.append(text)
        if len(output) >= 10:
            break
    return output


def _quarter_keys_for_year(year: int) -> list[str]:
    return [f"{year}-Q{quarter}" for quarter in range(1, 5)]


def _extract_markdown_section(markdown: str, heading: str) -> str:
    text = str(markdown or "")
    pattern = rf"(?ms)^##\s+{re.escape(heading)}\s*$\n(.*?)(?=^##\s+|\Z)"
    match = re.search(pattern, text)
    return match.group(1).strip() if match else ""


def _concentration_rank(label: str) -> int:
    return {"高度集中": 3, "相对集中": 2, "较分散": 1}.get(str(label or "").strip(), 0)


def _concentration_note(field_label: str, details: list[dict[str, Any]], *, annual: bool = False) -> str:
    label = _concentration_label(details)
    period_text = "这一年" if annual else "本期"
    if label == "较分散":
        return f"说明{period_text}没有压缩成单一{field_label}主线。"
    if label == "相对集中":
        return f"说明{period_text}已经出现更明确的{field_label}中心，但还保留一定分散度。"
    if label == "高度集中":
        return f"说明{period_text}的{field_label}重心已经明显收束。"
    return f"说明{period_text}暂时看不出稳定的{field_label}重心。"


def _distribution_dimension_line(details: list[dict[str, Any]], field_label: str, *, annual: bool = False) -> str:
    formatted = _format_counter_details(details)
    if formatted == "—":
        return f"{field_label}：暂无有效记录。"
    return f"{field_label}：{formatted}。{_concentration_note(field_label, details, annual=annual)}"


def _dimension_signal_score(details: list[dict[str, Any]]) -> tuple[float, int, int]:
    if not details:
        return (0.0, 0, 0)
    top_share = float(details[0].get("share") or 0.0)
    top_count = int(details[0].get("count") or 0)
    distinct = len([item for item in details if str(item.get("label") or "").strip()])
    return (top_share, top_count, -distinct)


def _yearly_distribution_points(media_type: str, distribution: dict[str, Any]) -> list[str]:
    dimensions = [("nationality_details", "国家/地区"), ("category_details", "题材"), ("channel_details", "渠道"), ("author_details", "作者")]
    ranked = sorted(dimensions, key=lambda item: _dimension_signal_score(distribution.get(item[0]) or []), reverse=True)
    output: list[str] = []
    for detail_key, field_label in ranked:
        details = distribution.get(detail_key) or []
        if not details:
            continue
        output.append(_distribution_dimension_line(details, field_label, annual=True))
        if len(output) >= 2:
            break
    return output or [f"{MEDIA_LABELS[media_type]} 今年样本偏少，暂时看不出稳定分布特征。"]


def _top_media_breakdown_text(rows: list[dict[str, Any]], limit: int = 2, *, annual: bool = False) -> str:
    total = sum(int(row.get("current_count") or 0) for row in rows)
    ranked = sorted(rows, key=lambda row: (int(row.get("current_count") or 0), row.get("media_label") or ""), reverse=True)
    chosen = [row for row in ranked if int(row.get("current_count") or 0) > 0][:limit]
    if not total or not chosen:
        return "这一年有效记录较少，暂时看不出明显的媒介重心。" if annual else "这一期有效记录较少，暂时看不出明显的媒介重心。"
    parts = []
    for row in chosen:
        count = int(row.get("current_count") or 0)
        share = (count / total) * 100 if total else 0
        parts.append(f"{row['media_label']} {count} 条（约 {share:.0f}%）")
    return ("这一年的记录重心主要落在 " if annual else "这一期的记录重心主要落在 ") + "、".join(parts) + "。"


def _most_significant_change_text(rows: list[dict[str, Any]], *, annual: bool = False) -> str:
    if not rows:
        return "与上一周期相比，样本还不够形成明显变化。"
    ranked = sorted(rows, key=lambda row: (abs(int(row.get("count_delta") or 0)), abs(float(row.get("rating_delta") or 0) if row.get("rating_delta") is not None else 0.0)), reverse=True)
    top = ranked[0]
    count_delta = int(top.get("count_delta") or 0)
    period_word = "去年" if annual else "上一周期"
    if count_delta > 0:
        return f"相比{period_word}，最明显的变化是{top['media_label']}增加了 {count_delta} 条，数量抬升最明显。"
    if count_delta < 0:
        return f"相比{period_word}，最明显的变化是{top['media_label']}减少了 {abs(count_delta)} 条，节奏明显回落。"
    return f"相比{period_word}，整体延续了此前节奏，没有出现特别剧烈的变化。"


def _standout_structure_text(context: dict[str, Any], *, annual: bool = False) -> str:
    candidates: list[tuple[int, int, str]] = []
    distribution = context.get("distribution") or {}
    for media_type in MEDIA_TYPES:
        dist = distribution.get(media_type, {})
        for detail_key, field_label in (("category_details", "题材"), ("nationality_details", "国家/地区"), ("channel_details", "渠道"), ("author_details", "作者")):
            details = dist.get(detail_key) or []
            if not details:
                continue
            label = _concentration_label(details)
            top_label = str(details[0].get("label") or "—")
            top_count = int(details[0].get("count") or 0)
            score = _concentration_rank(label)
            if score <= 0:
                continue
            prefix = "年度" if annual else "本期"
            text = f"{prefix}最明显的结构特征是{MEDIA_LABELS[media_type]}更集中在{top_label}这一{field_label}上，{_concentration_note(field_label, details, annual=annual)}"
            candidates.append((score, top_count, text))
    if not candidates:
        return f"{'年度' if annual else '本期'}样本分布比较均匀，暂时没有压倒性的单一结构。"
    candidates.sort(key=lambda item: (item[0], item[1]), reverse=True)
    return candidates[0][2]


def _annual_trend_signals(quarter_contexts: list[dict[str, Any]]) -> list[str]:
    if not quarter_contexts:
        return []
    quarter_totals: list[tuple[str, int]] = []
    per_media: dict[str, list[tuple[str, int]]] = {media_type: [] for media_type in MEDIA_TYPES}
    for quarter_context in quarter_contexts:
        period_label = str(quarter_context.get("period_label") or quarter_context.get("period_key") or "")
        short_label = period_label.replace(" 年第 ", "Q").replace(" 季度", "")
        rows = quarter_context.get("comparison_rows") or []
        total = sum(int(row.get("current_count") or 0) for row in rows)
        quarter_totals.append((short_label, total))
        for row in rows:
            per_media[str(row.get("media_type") or "")].append((short_label, int(row.get("current_count") or 0)))
    signals: list[str] = []
    active = max(quarter_totals, key=lambda item: item[1])
    quiet = min(quarter_totals, key=lambda item: item[1])
    signals.append(f"{active[0]} 是全年最活跃的季度，共记录 {active[1]} 条；{quiet[0]} 相对最淡，年内节奏并不平均。")
    return signals[:4]


def _default_next_focus(context: dict[str, Any]) -> list[str]:
    comparison_rows = context.get("comparison_rows") or []
    rating_rows = context.get("rating_rows") or []
    distribution = context.get("distribution") or {}
    rows_by_media = context.get("current_rows_by_media") or {}
    annual = context.get("kind") == REPORT_KIND_YEARLY
    output: list[str] = []
    growth_rows = sorted(comparison_rows, key=lambda row: (int(row.get("count_delta") or 0), int(row.get("current_count") or 0)), reverse=True)
    if growth_rows and int(growth_rows[0].get("count_delta") or 0) > 0:
        top = growth_rows[0]
        dist = distribution.get(top["media_type"], {})
        if annual:
            output.append(f"{top['media_label']} 是这一年最明显还在扩张的投入方向，下一年度可以继续沿着 {dist.get('top_category', '—')} 题材和 {dist.get('top_channel', '—')} 入口往下深挖。")
        else:
            output.append(f"下一期可以继续沿着 {top['media_label']} 的 {dist.get('top_category', '—')} / {dist.get('top_channel', '—')} 方向延伸。")
    rating_rows = sorted(rating_rows, key=lambda row: (row["current_avg_rating"] if row["current_avg_rating"] is not None else -1), reverse=True)
    if rating_rows and rating_rows[0]["current_avg_rating"] is not None:
        top = rating_rows[0]
        best_category = _best_direction(rows_by_media.get(top["media_type"], []), "category")
        output.append(f"如果按质量优先，下一期值得优先继续追 {top['media_label']} 里评分表现最好的 {best_category}。")
    seen: set[str] = set()
    deduped: list[str] = []
    for item in output:
        text = truncate_text_by_chars(item, 100)
        if text and text not in seen:
            seen.add(text)
            deduped.append(text)
    return deduped[:3]


def _yearly_media_summary(context: dict[str, Any], media_type: str) -> str:
    media_rows = [row for row in (context.get("trend_rows") or []) if row.get("media_type") == media_type]
    if not media_rows:
        return f"{MEDIA_LABELS[media_type]} 今年记录较少，暂时没有形成清晰的季度轨迹。"
    reps = [f"{row['quarter']}{_item_markdown_link(row.get('representative_title'), row.get('representative_id'))}" for row in media_rows if str(row.get('representative_title') or '').strip() and str(row.get('representative_title')) != '—']
    peak = max(media_rows, key=lambda row: int(row.get("count") or 0))
    low = min(media_rows, key=lambda row: int(row.get("count") or 0))
    rep_text = "、".join(reps) if reps else "各季度代表作品"
    if int(peak.get("count") or 0) == int(low.get("count") or 0):
        return f"{rep_text} 构成了 {MEDIA_LABELS[media_type]} 的年度回看，四个季度的记录量比较接近，整体节奏相对平均。"
    return f"{rep_text} 构成了 {MEDIA_LABELS[media_type]} 的年度回看，其中 {peak['quarter']} 最活跃，{low['quarter']} 相对较淡，年内节奏有明显起伏。"


def _sorted_yearly_trend_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    quarter_order = {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4}
    media_order = {media_type: index for index, media_type in enumerate(MEDIA_TYPES)}
    return sorted(rows, key=lambda row: (media_order.get(str(row.get("media_type") or ""), 999), quarter_order.get(str(row.get("quarter") or ""), 999)))


def _quarter_report_inputs_for_year(year: int) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for period_key in _quarter_keys_for_year(year):
        report = read_report(REPORT_KIND_QUARTERLY, period_key=period_key)
        if report:
            output.append({
                "period_key": period_key,
                "period_label": report.get("period_label") or period_key,
                "overview": _extract_markdown_section(str(report.get("markdown") or ""), "本期概览"),
                "highlights": _extract_markdown_section(str(report.get("markdown") or ""), "本期亮点作品"),
            })
            continue
        quarter_period = _quarter_period_from_key(period_key)
        current_rows = _records_for_period(REPORT_KIND_QUARTERLY, period_key)
        previous_rows = _records_for_period(REPORT_KIND_QUARTERLY, _previous_period_key(REPORT_KIND_QUARTERLY, period_key))
        quarter_context = _build_common_context(REPORT_KIND_QUARTERLY, quarter_period, current_rows, previous_rows, include_external_references=False)
        output.append({
            "period_key": period_key,
            "period_label": quarter_period.label,
            "overview": truncate_text_by_chars(str(quarter_context.get("overview_fact_text") or ""), 220),
            "highlights": truncate_text_by_chars("；".join(f"{MEDIA_LABELS[media_type]}《{(quarter_context['representatives'].get(media_type) or {}).get('title') or '—'}》" for media_type in MEDIA_TYPES), 220),
        })
    return output


def _build_common_context(
    kind: str,
    period: ReportPeriod,
    current_rows: list[dict[str, Any]],
    previous_rows: list[dict[str, Any]],
    *,
    include_external_references: bool = True,
) -> dict[str, Any]:
    current_by_media = _media_blocks(current_rows)
    previous_by_media = _media_blocks(previous_rows)
    representatives = {media_type: _representative_item(current_by_media.get(media_type, [])) for media_type in MEDIA_TYPES}
    distribution = {
        media_type: {
            "top_nationality": _top_counter(current_by_media.get(media_type, []), "nationality"),
            "top_category": _top_counter(current_by_media.get(media_type, []), "category"),
            "top_channel": _top_counter(current_by_media.get(media_type, []), "channel", split_multi=False),
            "top_author": _top_counter(current_by_media.get(media_type, []), "author"),
            "nationality_details": _counter_details(current_by_media.get(media_type, []), "nationality"),
            "category_details": _counter_details(current_by_media.get(media_type, []), "category"),
            "channel_details": _counter_details(current_by_media.get(media_type, []), "channel", split_multi=False),
            "author_details": _counter_details(current_by_media.get(media_type, []), "author"),
        }
        for media_type in MEDIA_TYPES
    }
    current_summary = {
        media_type: {
            "count": len(current_by_media.get(media_type, [])),
            "avg_rating": _average_rating(current_by_media.get(media_type, [])),
            "rating_stddev": _rating_stddev(current_by_media.get(media_type, [])),
            "stability_label": _rating_stability_label(current_by_media.get(media_type, [])),
        }
        for media_type in MEDIA_TYPES
    }
    previous_summary = {
        media_type: {
            "count": len(previous_by_media.get(media_type, [])),
            "avg_rating": _average_rating(previous_by_media.get(media_type, [])),
            "rating_stddev": _rating_stddev(previous_by_media.get(media_type, [])),
            "stability_label": _rating_stability_label(previous_by_media.get(media_type, [])),
        }
        for media_type in MEDIA_TYPES
    }
    if include_external_references:
        external_references, external_reference_usage = _representative_references(representatives)
    else:
        external_references = {media_type: _empty_reference_payload(((representatives.get(media_type) or {}).get("title") or MEDIA_LABELS[media_type])) for media_type in MEDIA_TYPES}
        external_reference_usage = {"web_search_calls": 0, "titles": []}
    comparison_rows = _comparison_rows(current_summary, previous_summary)
    rating_rows = _rating_rows(current_summary, previous_summary)
    total_current = sum(row["current_count"] for row in comparison_rows)
    overview_fact_text = f"本期共记录 {total_current} 条，四类媒介分别为 " + "，".join(f"{row['media_label']} {row['current_count']} 条" for row in comparison_rows) + "。"
    return {
        "kind": kind,
        "period_key": period.key,
        "period_label": period.label,
        "previous_period_key": _previous_period_key(kind, period.key),
        "current_summary": current_summary,
        "previous_summary": previous_summary,
        "distribution": distribution,
        "representatives": representatives,
        "external_references": external_references,
        "external_reference_usage": external_reference_usage,
        "comparison_rows": comparison_rows,
        "rating_rows": rating_rows,
        "current_rows_by_media": current_by_media,
        "overview_fact_text": overview_fact_text,
        "focus_candidates": _focus_candidates(current_summary, previous_summary, distribution, current_by_media),
    }


def _markdown_comparison_table(rows: list[dict[str, Any]]) -> list[str]:
    lines = [
        "| 媒介 | 本期条数 | 上期条数 | 差值 | 本期平均评分 | 上期平均评分 | 差值 |",
        "| :---: | :---: | :---: | :---: | :---: | :---: | :---: |",
    ]
    for row in rows:
        lines.append(
            f"| {row['media_label']} | {row['current_count']} | {row['previous_count']} | {row['count_delta']:+d} | {_format_rating(row['current_avg_rating'])} | {_format_rating(row['previous_avg_rating'])} | {_format_rating_delta(row['rating_delta'])} |"
        )
    return lines


def _markdown_rating_table(rows: list[dict[str, Any]]) -> list[str]:
    lines = [
        "| 媒介 | 本期平均评分 | 上期平均评分 | 差值 | 本期波动 |",
        "| :---: | :---: | :---: | :---: | :---: |",
    ]
    for row in rows:
        lines.append(
            f"| {row['media_label']} | {_format_rating(row['current_avg_rating'])} | {_format_rating(row['previous_avg_rating'])} | {_format_rating_delta(row['rating_delta'])} | {row['stability_label']} |"
        )
    return lines


def _parse_report_file(path: Path) -> dict[str, Any] | None:
    def _normalize(record: dict[str, Any], meta: dict[str, str], file_path: Path) -> dict[str, Any]:
        del meta
        generated_at = str(record.get("generated_at") or datetime.fromtimestamp(file_path.stat().st_mtime).isoformat(timespec="seconds"))
        return {**record, "generated_at": generated_at}

    return parse_report_record(path, valid_kinds=REPORT_KINDS, job_type_by_kind=REPORT_KIND_TO_JOB_TYPE, record_normalizer=_normalize)


def _list_reports(kind: str) -> list[dict[str, Any]]:
    ensure_analysis_storage()
    rows: list[dict[str, Any]] = []
    for path in sorted(_report_dir(kind).glob("*.md")):
        parsed = _parse_report_file(path)
        if parsed:
            rows.append(parsed)
    rows.sort(key=lambda row: (str(row.get("period_key") or ""), 1 if row.get("source") == "deepseek" else 0, str(row.get("generated_at") or "")), reverse=True)
    return rows


def _preferred_report(kind: str, period_key: str | None = None) -> dict[str, Any] | None:
    rows = _list_reports(kind)
    if period_key:
        rows = [row for row in rows if row.get("period_key") == period_key]
    if not rows:
        return None
    rows.sort(key=lambda row: (1 if row.get("source") == "deepseek" else 0, str(row.get("generated_at") or "")), reverse=True)
    return rows[0]


def read_report(kind: str, period_key: str | None = None, source: str | None = None) -> dict[str, Any] | None:
    if kind not in REPORT_KINDS:
        raise ValueError("invalid report kind")
    report = _preferred_report(kind, period_key=period_key)
    if source:
        rows = [row for row in _list_reports(kind) if row.get("source") == source]
        if period_key:
            rows = [row for row in rows if row.get("period_key") == period_key]
        report = rows[0] if rows else None
    return report