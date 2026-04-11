from __future__ import annotations

from typing import Any

from core_service.dashboard_job_sync import publish_nav_dashboard_job_update
from core_service import display_model_name, get_settings
from core_service.llm import chat_completion_with_retry
from core_service.observability import record_usage as notify_nav_dashboard_usage

from . import library_period_summary_builder, library_report_service, library_spotlight_service
from . import library_analysis_core as shared


def _report_job_label(kind: str, period_label: str) -> str:
    if kind == shared.REPORT_KIND_YEARLY:
        return f"书影音年报生成 | {period_label}"
    return f"书影音季度报告生成 | {period_label}"


def _report_job_metadata(kind: str, backend: str, period: Any, manual: bool) -> dict[str, Any]:
    return {
        "module": "library_tracker",
        "kind": str(kind or ""),
        "backend": str(backend or ""),
        "period_key": str(getattr(period, "key", "") or ""),
        "period_label": str(getattr(period, "label", "") or ""),
        "manual": bool(manual),
        "report_path_group": "analysis",
    }


def _backend_config(backend: str) -> tuple[str, str, str, str]:
    settings = get_settings()
    if backend == "deepseek":
        return settings.api_key, settings.api_base_url, settings.chat_model, "DeepSeek"
    return settings.local_llm_api_key, settings.local_llm_url, settings.local_llm_model, "Local"


def generate_report(kind: str, backend: str, period_key: str | None = None, manual: bool = True) -> dict[str, object]:
    if kind not in shared.REPORT_KINDS:
        raise ValueError("invalid report kind")
    if backend not in shared.REPORT_BACKENDS:
        raise ValueError("invalid backend")
    with shared._LOCK:
        period = library_report_service.resolve_period(kind, period_key)
        metadata = _report_job_metadata(kind, backend, period, manual)
        label = _report_job_label(kind, str(getattr(period, "label", "") or getattr(period, "key", "") or ""))
        dashboard_job_id = publish_nav_dashboard_job_update(
            job_type="report_generation",
            label=label,
            status="running",
            message="正在生成报告",
            metadata=metadata,
        )
        context = library_period_summary_builder.build_period_context(kind, period)
        external_reference_usage = context.get("external_reference_usage") if isinstance(context.get("external_reference_usage"), dict) else {}
        external_web_calls = int(external_reference_usage.get("web_search_calls", 0) or 0)
        external_titles = [str(title).strip() for title in external_reference_usage.get("titles") or [] if str(title).strip()]
        if external_web_calls > 0:
            notify_nav_dashboard_usage(
                web_search_delta=external_web_calls,
                count_daily=False,
                events=[
                    {
                        "provider": "web_search",
                        "feature": f"library_tracker.report.{kind}.external_reference",
                        "page": "analysis",
                        "source": "library_tracker",
                        "message": f"{period.label}: {', '.join(external_titles[:4])}",
                        "count": external_web_calls,
                    }
                ],
                background=True,
            )
        sections = library_spotlight_service.build_default_sections(context)
        markdown = library_report_service.render_report_markdown(period, context, sections)
        settings = get_settings()
        requested_backend = backend
        attempted_backends = [backend]
        if backend == "deepseek":
            attempted_backends.append("local")
        actual_backend = backend
        failure_notes: list[str] = []
        model_label = "规则输出"
        for candidate in attempted_backends:
            api_key, base_url, model, backend_label = _backend_config(candidate)
            attempted_label = display_model_name(model) if model else backend_label
            if not (api_key and model and base_url):
                failure_notes.append(f"{backend_label} 未配置")
                continue
            try:
                llm_text = chat_completion_with_retry(
                    api_key=api_key,
                    base_url=base_url,
                    model=model,
                    timeout=settings.timeout,
                    messages=library_spotlight_service.llm_messages(period, context),
                    temperature=0.2,
                    max_retries=2,
                    retry_delay=1.2,
                    max_tokens=2600 if kind == shared.REPORT_KIND_QUARTERLY else 3200,
                ).strip()
                sections = library_spotlight_service.merge_llm_sections(context, llm_text)
                note = ""
                if requested_backend == "deepseek" and candidate == "local":
                    note = "DeepSeek 调用失败，已回退本地模型输出。"
                markdown = library_report_service.render_report_markdown(period, context, sections, note=note)
                actual_backend = candidate
                model_label = attempted_label
                if candidate == "deepseek":
                    notify_nav_dashboard_usage(
                        deepseek_delta=1,
                        count_daily=False,
                        events=[
                            {
                                "provider": "deepseek",
                                "feature": f"library_tracker.report.{kind}",
                                "page": "analysis",
                                "source": "library_tracker",
                                "message": period.label,
                            }
                        ],
                        background=True,
                    )
                break
            except Exception as exc:
                failure_notes.append(f"{backend_label} 调用失败: {str(exc).strip()}")
        else:
            note = "；".join(failure_notes) if failure_notes else "模型调用失败，已使用规则兜底输出。"
            markdown = library_report_service.render_report_markdown(period, context, sections, note=note)
            model_label = "规则输出"
            actual_backend = requested_backend
        try:
            summary = library_report_service.summary_from_markdown(markdown)
            report = library_report_service.save_report(kind, actual_backend, period, markdown, model_label, summary)
        except Exception as exc:
            publish_nav_dashboard_job_update(
                job_id=dashboard_job_id,
                job_type="report_generation",
                label=label,
                status="failed",
                message="报告生成失败",
                metadata=metadata,
                error=str(exc),
                log=f"报告生成失败: {exc}",
            )
            raise
        publish_nav_dashboard_job_update(
            job_id=dashboard_job_id,
            job_type="report_generation",
            label=label,
            status="completed",
            message="报告已生成",
            metadata={**metadata, "source": str(report.get("source") or backend or "")},
            result={
                "period_key": str(report.get("period_key") or getattr(period, "key", "") or ""),
                "title": str(report.get("title") or ""),
                "path": str(report.get("path") or ""),
            },
            log=f"报告已生成: {str(report.get('title') or label)}",
        )
        return report
