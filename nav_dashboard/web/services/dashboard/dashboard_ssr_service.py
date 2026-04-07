from __future__ import annotations

import re
from typing import Any


def _esc(s: object) -> str:
    import html as _html

    return _html.escape(str(s) if s is not None else "")


def _fmt_n(v: object) -> str:
    try:
        return f"{int(round(float(v))):,}"
    except (TypeError, ValueError):
        return "—"


def _fmt_r(v: object) -> str:
    try:
        return f"{float(v) * 100:.1f}%"
    except (TypeError, ValueError):
        return "—"


def _fmt_duration(v: object) -> str:
    try:
        n = float(v)
    except (TypeError, ValueError):
        return "—"
    if n < 0:
        return "—"
    if n >= 1:
        return f"{n:.2f}s"
    if n >= 0.001:
        return f"{n * 1000:.1f}ms"
    if n > 0:
        return f"{int(round(n * 1_000_000))}µs"
    return "0s"


def _fmt_size(v: object) -> str:
    try:
        n = float(v)
    except (TypeError, ValueError):
        return "0 MB"
    if n <= 0:
        return "0 MB"
    if n >= 1024 * 1024 * 1024:
        return f"{n / (1024 * 1024 * 1024):.2f} GB"
    if n >= 1024 * 1024:
        return f"{n / (1024 * 1024):.2f} MB"
    if n >= 1024:
        return f"{n / 1024:.1f} KB"
    return f"{int(round(n))} B"


def _fmt_signed(v: object, digits: int = 4) -> str:
    try:
        n = float(v)
    except (TypeError, ValueError):
        return "—"
    text = f"{n:.{digits}f}"
    return f"+{text}" if n > 0 else text


def _css_token(value: object) -> str:
    return re.sub(r"[^a-z0-9_-]+", "-", str(value or "").strip().lower())


def _stat_card(title: str, value: str, sub: str = "", role: str = "", state: str = "") -> str:
    extra = f' data-role="{_esc(role)}"' if role else ""
    classes = "stat-card" + (f" {_esc(state)}" if state else "")
    return (
        f'<article class="{classes}"{extra}>'
        f'<div class="stat-title">{_esc(title)}</div>'
        f'<div class="stat-value">{_esc(value)}</div>'
        f'<div class="stat-sub">{_esc(sub)}</div>'
        f"</article>"
    )


def render_dashboard_core_cards_html(prefill: dict[str, Any]) -> str:
    if not prefill or not isinstance(prefill, dict):
        return ""
    rag = prefill.get("rag") or {}
    lib = prefill.get("library") or {}
    alias_proposal = lib.get("alias_proposal") or {}
    gq = lib.get("graph_quality") or {}
    api = prefill.get("api_usage") or {}
    agent = prefill.get("agent") or {}
    rqa = prefill.get("rag_qa") or {}
    latency = prefill.get("retrieval_latency") or {}
    latency_stages = latency.get("stages") or {}
    cache_stats = prefill.get("cache_stats") or {}
    rerank_quality = prefill.get("rerank_quality") or {}
    rag_rerank = rerank_quality.get("rag") or {}
    agent_rerank = rerank_quality.get("agent") or {}
    missing_queries = prefill.get("missing_queries_last_30d") or {}
    agent_wall_clock = prefill.get("agent_wall_clock") or {}
    runtime_data = prefill.get("runtime_data") or {}
    chat_feedback = prefill.get("chat_feedback") or {}
    warns = prefill.get("warnings") or []

    pending = int(rag.get("changed_pending") or 0)
    warn_sub = " | ".join(str(w) for w in warns[:2]) if warns else "无告警"
    dash = "—"
    neutral_sub = "详情统计加载中"
    total_stage = latency_stages.get("total") or {}
    rerank_stage = latency_stages.get("rerank_seconds") or {}
    elapsed_stage = latency_stages.get("elapsed_seconds") or {}

    cards = [
        _stat_card("RAG 已索引文档", _fmt_n(rag.get("indexed_documents")), f"总文档数 {_fmt_n(rag.get('source_markdown_files'))}"),
        _stat_card("书影音游戏总条目", _fmt_n(lib.get("total_items")), f"今年条目 {_fmt_n(lib.get('this_year_items'))}"),
        _stat_card("RAG Graph 节点数", _fmt_n(rag.get("graph_nodes")), f"边数 {_fmt_n(rag.get('graph_edges'))}"),
        _stat_card("Library Graph 节点数", _fmt_n(lib.get("graph_nodes")), f"边数 {_fmt_n(lib.get('graph_edges'))} | 覆盖 {_fmt_r(gq.get('item_coverage_rate'))}", "library-graph-summary"),
        _stat_card("本月 Tavily API 调用", _fmt_n(api.get("month_web_search_calls")), f"今日 {_fmt_n(api.get('today_web_search'))} / 限额 {_fmt_n(api.get('daily_web_limit'))}", "web-search-usage"),
        _stat_card("本月 DeepSeek API 调用", _fmt_n(api.get("month_deepseek_calls")), f"今日 {_fmt_n(api.get('today_deepseek'))} / 限额 {_fmt_n(api.get('daily_deepseek_limit'))}", "deepseek-usage"),
        _stat_card("Agent 消息总数", _fmt_n(agent.get("message_count")), f"会话数 {_fmt_n(agent.get('session_count'))}"),
        _stat_card("RAG Q&A 消息总数", _fmt_n(rqa.get("message_count")), f"会话数 {_fmt_n(rqa.get('session_count'))}"),
        _stat_card("RAG 检索总时长均值", _fmt_duration(total_stage.get("avg")) if total_stage else dash, f"近 {_fmt_n(total_stage.get('count'))} 次 | p50 {_fmt_duration(total_stage.get('p50'))}" if total_stage else neutral_sub),
        _stat_card("RAG 模型重排均值", _fmt_duration(rerank_stage.get("avg")) if rerank_stage else dash, f"近 {_fmt_n(rerank_stage.get('count'))} 次" if rerank_stage else neutral_sub),
        _stat_card("检索分位 p50", _fmt_duration(total_stage.get("p50")) if total_stage else dash, f"p95 {_fmt_duration(total_stage.get('p95'))} | p99 {_fmt_duration(total_stage.get('p99'))}" if total_stage else neutral_sub),
        _stat_card("RAG 全流程 p50", _fmt_duration(elapsed_stage.get("p50")) if elapsed_stage else dash, f"p95 {_fmt_duration(elapsed_stage.get('p95'))} | Agent p50 {_fmt_duration(agent_wall_clock.get('p50'))}" if elapsed_stage or agent_wall_clock else neutral_sub),
        _stat_card("RAG 重排序换榜率", _fmt_r(rag_rerank.get("top1_identity_change_rate")) if rag_rerank else dash, f"Agent 换榜率 {_fmt_r(agent_rerank.get('top1_identity_change_rate'))}" if rag_rerank or agent_rerank else neutral_sub),
        _stat_card("RAG 平均换榜", _fmt_signed(rag_rerank.get("avg_rank_shift"), 2) if rag_rerank else dash, f"Agent 平均换榜 {_fmt_signed(agent_rerank.get('avg_rank_shift'), 2)}" if rag_rerank or agent_rerank else neutral_sub),
        _stat_card("Embedding 缓存命中率", _fmt_r(cache_stats.get("rag_embed_cache_hit_rate")) if cache_stats else dash, f"近 {_fmt_n(latency.get('record_count'))} 次" if cache_stats or latency else neutral_sub),
        _stat_card("Agent 文档调用率", _fmt_r(cache_stats.get("agent_rag_trigger_rate")) if cache_stats else dash, f"Media {_fmt_r(cache_stats.get('agent_media_trigger_rate'))} | Web {_fmt_r(cache_stats.get('agent_web_trigger_rate'))}" if cache_stats else neutral_sub),
        _stat_card("RAG Top1 均值", f"{float(rag_rerank.get('avg_top1_local_doc_score')):.4f}" if rag_rerank.get("avg_top1_local_doc_score") is not None else dash, f"Agent Top1 均值 {float(agent_rerank.get('avg_top1_local_doc_score')):.4f}" if agent_rerank.get("avg_top1_local_doc_score") is not None else neutral_sub),
        _stat_card("RAG 未命中率", _fmt_r(cache_stats.get("rag_no_context_rate")) if cache_stats else dash, f"Agent 未命中率 {_fmt_r(cache_stats.get('agent_no_context_rate'))}" if cache_stats else neutral_sub),
        _stat_card("月检索缺失问题数", _fmt_n(missing_queries.get("count")) if missing_queries else dash, "长按查看导出" if missing_queries else neutral_sub, "missing-queries-summary"),
        _stat_card("聊天反馈数", _fmt_n(chat_feedback.get("count")) if chat_feedback else dash, "长按查看导出" if chat_feedback else neutral_sub, "feedback-summary"),
        _stat_card("RAG 待重建文档", _fmt_n(pending), "等待后台同步" if pending > 0 else "已全部同步", "rag-changed-pending"),
        _stat_card("待审核媒体同义词", _fmt_n(alias_proposal.get("pending_count")), "长按查看待审核列表", "library-alias-proposal-summary"),
        _stat_card("运行时数据", _fmt_size(runtime_data.get("total_size_bytes")) if runtime_data else dash, f"非空 {_fmt_n(runtime_data.get('nonzero_items'))} 项 | 长按查看" if runtime_data else neutral_sub, "runtime-data-summary"),
        _stat_card("系统告警", _fmt_n(len(warns)), warn_sub, "warnings-summary"),
    ]
    return "\n".join(cards)


def render_dashboard_latency_table_html(data: dict[str, Any]) -> str:
    latency = data.get("retrieval_latency") or {}
    stages = latency.get("stages") or {}
    if not isinstance(stages, dict) or not stages:
        return "<tbody><tr><td colspan=\"5\">暂无最近20次记录</td></tr></tbody>"

    stage_order = [
        ("total", "RAG 检索总时长"),
        ("rerank_seconds", "模型重排"),
        ("context_assembly_seconds", "上下文组装"),
        ("web_search_seconds", "网络检索"),
        ("elapsed_seconds", "端到端总时长"),
    ]
    known = {key for key, _label in stage_order}
    extra = [key for key in stages.keys() if key not in known and key != "reranker_load_seconds"]
    ordered = stage_order[:-1] + [(key, key) for key in extra] + [stage_order[-1]]

    rows: list[str] = []
    for key, label in ordered:
        stat = stages.get(key)
        if not isinstance(stat, dict):
            continue
        tr_class = ' class="latency-row-total"' if key == "elapsed_seconds" else ""
        rows.append(
            f"<tr{tr_class}>"
            f"<td>{_esc(label)}</td>"
            f"<td>{_esc(_fmt_duration(stat.get('avg')))}</td>"
            f"<td>{_esc(_fmt_duration(stat.get('p50')))}</td>"
            f"<td>{_esc(_fmt_duration(stat.get('p95')))}</td>"
            f"<td>{_esc(_fmt_duration(stat.get('p99')))}</td>"
            "</tr>"
        )

    body = "".join(rows) or '<tr><td colspan="5">暂无数据</td></tr>'
    return "<thead><tr><th>阶段</th><th>均值</th><th>p50</th><th>p95</th><th>p99</th></tr></thead>" f"<tbody>{body}</tbody>"


def _ordered_bucket_entries(bucket: dict[str, Any], ordered_keys: list[str]) -> list[tuple[str, Any]]:
    seen: set[str] = set()
    rows: list[tuple[str, Any]] = []
    for key in ordered_keys:
        seen.add(key)
        rows.append((key, bucket.get(key) or {}))
    for key, value in bucket.items():
        if key in seen:
            continue
        rows.append((key, value))
    return rows


def render_dashboard_observability_table_html(data: dict[str, Any]) -> str:
    rag_profiles = data.get("retrieval_by_profile") or {}
    rag_modes = data.get("retrieval_by_search_mode") or {}
    agent_profiles = data.get("agent_by_profile") or {}
    agent_modes = data.get("agent_by_search_mode") or {}
    rows: list[str] = []

    def push_section(title: str) -> None:
        rows.append("<tr class=\"dashboard-observability-section\">" f"<td colspan=\"6\"><span class=\"dashboard-observability-title\">{_esc(title)}</span></td>" "</tr>")

    def push_row(name: str, c1: str, c2: str, c3: str, c4: str, c5: str) -> None:
        rows.append(f"<tr><td>{_esc(name)}</td><td>{_esc(c1)}</td><td>{_esc(c2)}</td><td>{_esc(c3)}</td><td>{_esc(c4)}</td><td>{_esc(c5)}</td></tr>")

    profile_names = {"short": "短查询", "medium": "中查询", "long": "长查询"}
    profile_keys = ["short", "medium", "long"]
    mode_keys = ["local_only", "hybrid"]

    if isinstance(rag_profiles, dict) and rag_profiles:
        push_section("RAG 分层观测")
        for key, value in _ordered_bucket_entries(rag_profiles, profile_keys):
            value = value if isinstance(value, dict) else {}
            push_row(
                f"{profile_names.get(key, key)} ({_fmt_n(value.get('count'))})",
                _fmt_r(value.get("no_context_rate")),
                _fmt_r(value.get("embed_cache_hit_rate")),
                _fmt_r(value.get("query_rewrite_rate")),
                _fmt_duration(((value.get("stages") or {}).get("elapsed_seconds") or {}).get("p50")),
                _fmt_duration(((value.get("stages") or {}).get("total") or {}).get("p50")),
            )

    if isinstance(rag_modes, dict) and rag_modes:
        push_section("RAG 检索模式")
        for key, value in _ordered_bucket_entries(rag_modes, mode_keys):
            value = value if isinstance(value, dict) else {}
            push_row(
                f"{key} ({_fmt_n(value.get('count'))})",
                _fmt_r(value.get("no_context_rate")),
                _fmt_r(value.get("embed_cache_hit_rate")),
                _fmt_r(value.get("query_rewrite_rate")),
                _fmt_duration((value.get("elapsed") or {}).get("p50")),
                _fmt_duration((value.get("total") or {}).get("p50")),
            )

    if isinstance(agent_profiles, dict) and agent_profiles:
        push_section("Agent 分层观测")
        for key, value in _ordered_bucket_entries(agent_profiles, profile_keys):
            value = value if isinstance(value, dict) else {}
            push_row(
                f"{profile_names.get(key, key)} ({_fmt_n(value.get('count'))})",
                _fmt_r(value.get("no_context_rate")),
                _fmt_r(value.get("embed_cache_hit_rate")),
                _fmt_r(value.get("query_rewrite_rate")),
                _fmt_duration((value.get("wall_clock") or {}).get("p50")),
                _fmt_duration((value.get("vector_recall") or {}).get("p50")),
            )

    if isinstance(agent_modes, dict) and agent_modes:
        push_section("Agent 检索模式")
        for key, value in _ordered_bucket_entries(agent_modes, mode_keys):
            value = value if isinstance(value, dict) else {}
            push_row(
                f"{key} ({_fmt_n(value.get('count'))})",
                _fmt_r(value.get("no_context_rate")),
                _fmt_r(value.get("embed_cache_hit_rate")),
                _fmt_r(value.get("query_rewrite_rate")),
                _fmt_duration((value.get("wall_clock") or {}).get("p50")),
                _fmt_duration((value.get("vector_recall") or {}).get("p50")),
            )

    if not rows:
        return "<tbody><tr><td colspan=\"6\">暂无分层观测数据</td></tr></tbody>"

    return "<thead><tr><th>维度</th><th>检索未命中</th><th>Embed 缓存命中率</th><th>问题重写率</th><th>端到端用时</th><th>召回用时</th></tr></thead>" f"<tbody>{''.join(rows)}</tbody>"


def _job_type_label(value: object) -> str:
    labels = {
        "benchmark": "Benchmark",
        "rag_sync": "RAG 同步",
        "library_graph_rebuild": "Library Graph",
        "runtime_cleanup": "运行时清理",
    }
    key = str(value or "")
    return labels.get(key, key or "未知任务")


def _job_status_label(value: object) -> str:
    labels = {
        "queued": "排队中",
        "running": "运行中",
        "completed": "已完成",
        "failed": "失败",
        "cancelled": "已取消",
    }
    key = str(value or "")
    return labels.get(key, key or "未知")


def render_dashboard_jobs_html(payload: dict[str, Any]) -> str:
    jobs = payload.get("jobs") if isinstance(payload, dict) else []
    if not isinstance(jobs, list) or not jobs:
        return '<div class="dashboard-job-empty">当前暂无后台任务</div>'

    selected_id = str((jobs[0] or {}).get("id") or "")
    parts: list[str] = []
    selected_job: dict[str, Any] | None = None
    for job in jobs:
        if not isinstance(job, dict):
            continue
        job_id = str(job.get("id") or "")
        status = str(job.get("status") or "queued")
        selected_cls = " is-selected" if job_id == selected_id else ""
        running_cls = " is-running" if status == "running" else " is-failed" if status == "failed" else " is-cancelled" if status == "cancelled" else ""
        summary = str(job.get("message") or "等待开始")
        metadata = job.get("metadata") if isinstance(job.get("metadata"), dict) else {}
        module_meta = "+".join(str(item or "") for item in (metadata.get("modules") or []) if str(item or "").strip())
        created_at = str(job.get("created_at") or "")
        if job_id == selected_id:
            selected_job = job
        parts.append(
            f'<div class="dashboard-job-card{selected_cls}{running_cls}" data-job-id="{_esc(job_id)}">'
            '<div class="dashboard-job-title">'
            f'<strong class="dashboard-job-title-text">{_esc(job.get("label") or _job_type_label(job.get("type")))}</strong>'
            f'<span class="dashboard-job-badge status-{_esc(_css_token(status))}">{_esc(_job_status_label(status))}</span>'
            '</div>'
            f'<div class="dashboard-job-meta-line">{_esc(_job_type_label(job.get("type")))}{(" | " + _esc(module_meta)) if module_meta else ""}</div>'
            f'<div class="dashboard-job-meta-line dashboard-job-summary-line">{_esc(summary)}</div>'
            f'<div class="dashboard-job-meta-line">{_esc(created_at)}</div>'
            '</div>'
        )
    selected_job = selected_job or (jobs[0] if isinstance(jobs[0], dict) else None)
    if not isinstance(selected_job, dict):
        return f'<div class="dashboard-job-list">{"".join(parts)}</div>'
    selected_status = str(selected_job.get("status") or "queued")
    selected_metadata = selected_job.get("metadata") if isinstance(selected_job.get("metadata"), dict) else {}
    selected_module_meta = "+".join(str(item or "") for item in (selected_metadata.get("modules") or []) if str(item or "").strip())
    selected_created_at = str(selected_job.get("created_at") or "")
    selected_updated_at = str(selected_job.get("updated_at") or "")
    selected_logs = selected_job.get("logs") if isinstance(selected_job.get("logs"), list) else []
    selected_log_text = "\n".join(str(line or "") for line in selected_logs) if selected_logs else (str(selected_job.get("error") or "") or "暂无日志")
    can_cancel_selected = selected_status in {"queued", "running"}
    detail = (
        '<section class="dashboard-job-detail-panel">'
        '<div class="dashboard-job-detail-head">'
        '<div class="dashboard-job-detail-title">'
        f'<strong class="dashboard-job-detail-heading">{_esc(selected_job.get("label") or _job_type_label(selected_job.get("type")))}</strong>'
        f'<div class="dashboard-job-meta-line">{_esc(_job_type_label(selected_job.get("type")))}{(" | " + _esc(selected_module_meta)) if selected_module_meta else ""}</div>'
        '</div>'
        f'<span class="dashboard-job-badge status-{_esc(_css_token(selected_status))}">{_esc(_job_status_label(selected_status))}</span>'
        '</div>'
        f'<div class="dashboard-job-detail-summary">{_esc(str(selected_job.get("message") or "等待开始"))}</div>'
        '<div class="dashboard-job-expanded">'
        f'<div class="dashboard-meta">创建 {_esc(selected_created_at or "-")} | 更新 {_esc(selected_updated_at or selected_created_at or "-")}</div>'
        f'<pre class="dashboard-job-log-window">{_esc(selected_log_text)}</pre>'
        '<div class="card-modal-actions dashboard-job-actions">'
        f'<button class="ghost" data-job-cancel-id="{_esc(str(selected_job.get("id") or ""))}"{"" if can_cancel_selected else " disabled"}>取消任务</button>'
        '</div></div></section>'
    )
    return f'<div class="dashboard-job-list">{"".join(parts)}</div>{detail}'


def _dashboard_ticket_breakdown_html(counts: dict[str, Any]) -> str:
    if not isinstance(counts, dict) or not counts:
        return '<span class="ticket-empty-state">暂无数据</span>'
    items = sorted(counts.items(), key=lambda item: int(item[1] or 0), reverse=True)
    return "".join(
        f'<span class="dashboard-ticket-chip"><span class="dashboard-ticket-chip-label">{_esc(label or "unknown")}</span><strong>{_esc(_fmt_n(count))}</strong></span>'
        for label, count in items
    )


def render_dashboard_ticket_summary_meta_text(stats: dict[str, Any]) -> str:
    summary = stats.get("summary") if isinstance(stats, dict) else {}
    if not isinstance(summary, dict) or not summary:
        return "暂无 ticket 统计"
    return f"当前遗留 {_fmt_n(summary.get('current_open_total') or 0)} | 近 1 周提交 {_fmt_n(summary.get('submitted_last_week') or 0)} | 近 1 周关闭 {_fmt_n(summary.get('closed_last_week') or 0)}"


def render_dashboard_ticket_summary_html(stats: dict[str, Any]) -> str:
    weeks = stats.get("weeks") if isinstance(stats, dict) else []
    if not isinstance(weeks, list) or not weeks:
        return '<div class="ticket-trend-empty">暂无统计数据</div>'
    summary = stats.get("summary") if isinstance(stats.get("summary"), dict) else {}
    status_counts = stats.get("status_counts") if isinstance(stats.get("status_counts"), dict) else {}
    priority_counts = stats.get("priority_counts") if isinstance(stats.get("priority_counts"), dict) else {}
    return (
        '<div class="dashboard-ticket-summary-grid">'
        f'<div class="dashboard-ticket-summary-card"><span>当前遗留</span><strong>{_esc(_fmt_n(summary.get("current_open_total") or 0))}</strong></div>'
        f'<div class="dashboard-ticket-summary-card"><span>Ticket 总数</span><strong>{_esc(_fmt_n(summary.get("ticket_total") or 0))}</strong></div>'
        f'<div class="dashboard-ticket-summary-card"><span>近 1 周提交</span><strong>{_esc(_fmt_n(summary.get("submitted_last_week") or 0))}</strong></div>'
        f'<div class="dashboard-ticket-summary-card"><span>近 1 月提交</span><strong>{_esc(_fmt_n(summary.get("submitted_last_month") or 0))}</strong></div>'
        f'<div class="dashboard-ticket-summary-card"><span>近 1 周关闭</span><strong>{_esc(_fmt_n(summary.get("closed_last_week") or 0))}</strong></div>'
        f'<div class="dashboard-ticket-summary-card"><span>当前最长遗留天数</span><strong>{_esc(_fmt_n(summary.get("current_longest_open_days") or 0))}</strong></div>'
        '</div>'
        '<div class="dashboard-ticket-breakdown">'
        '<section class="dashboard-ticket-breakdown-group"><h4>状态分布（未关闭）</h4>'
        f'<div class="dashboard-ticket-chip-row">{_dashboard_ticket_breakdown_html(status_counts)}</div></section>'
        '<section class="dashboard-ticket-breakdown-group"><h4>优先级分布（未关闭）</h4>'
        f'<div class="dashboard-ticket-chip-row">{_dashboard_ticket_breakdown_html(priority_counts)}</div></section>'
        '</div>'
    )


def render_dashboard_ticket_trend_meta_text(stats: dict[str, Any]) -> str:
    weeks = stats.get("weeks") if isinstance(stats, dict) else []
    if not isinstance(weeks, list) or not weeks:
        return "暂无 ticket 趋势统计"
    first = str((weeks[0] or {}).get("label") or (weeks[0] or {}).get("bucket_start") or "")
    last = str((weeks[-1] or {}).get("label") or (weeks[-1] or {}).get("bucket_start") or "")
    return f"周趋势 | {first or '-'} 到 {last or '-'} | 优先级堆叠面积图"


def render_dashboard_ticket_trend_html(stats: dict[str, Any]) -> str:
    weeks = stats.get("weeks") if isinstance(stats, dict) else []
    if not isinstance(weeks, list) or not weeks:
        return '<div class="ticket-trend-empty">暂无图表数据</div>'
    return '<div class="ticket-trend-echart-canvas" role="img" aria-label="Ticket 趋势图（优先级堆叠面积）"></div>'


def render_dashboard_startup_logs_text(data: dict[str, Any]) -> str:
    startup = data.get("startup") or {}
    logs = startup.get("logs") if isinstance(startup, dict) else []
    logs = logs if isinstance(logs, list) else []
    status = str((startup or {}).get("status") or "unknown")
    checked_at = str((startup or {}).get("last_checked_at") or "")
    head = f"status={status}{f' | checked_at={checked_at}' if checked_at else ''}"
    if not logs:
        return f"{head}\n暂无日志"
    return f"{head}\n" + "\n".join(str(line or "") for line in logs)