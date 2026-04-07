from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from core_service.ticket_store import create_ticket, list_tickets, update_ticket


_YEAR = 2026
_STATUS_RANK = {
    "open": 1,
    "in_progress": 2,
    "blocked": 2,
    "resolved": 3,
    "closed": 4,
}
_PRIORITY_RANK = {
    "low": 1,
    "medium": 2,
    "high": 3,
    "critical": 4,
}
_PURE_ENGLISH_TRANSLATIONS: dict[str, dict[str, str]] = {
    "agent_request_base_url_forwarding": {
        "title": "agent facade 切到 entry-layer 后丢失 request_base_url 透传",
        "summary": "重构后的 agent facade 没有把 request_base_url 继续传给 entry/runtime 层，依赖该 base URL 的 lifecycle deps 因此失效。",
        "repro_query": "在 facade 脱离 legacy 之后，调用任意带 request_base_url 的 agent round。",
        "expected_behavior": "run_agent_round 和 run_agent_round_stream 都应把 request_base_url 透传到 build_round_lifecycle_deps。",
        "actual_behavior": "facade 构建 lifecycle deps 时始终使用空的 base URL。",
        "root_cause": "refactor 后改走 agent.runtime.entry 新入口时，漏传了 request_base_url 参数。",
        "fix_notes": "在同步和流式 facade 入口补齐 request_base_url 透传，并更新 entry-layer 回归测试。",
        "additional_notes": "同一轮清理还移除了 legacy 和 flat wrapper 模块。",
    },
    "support_header_compat_recovery": {
        "title": "agent support 头部损坏破坏了 router 兼容面",
        "summary": "router-core 抽离期间，过渡期 support 层丢失了一批 import、常量和 helper 绑定，导致 router 与 entry-layer 多条路径运行时报 NameError 或导入失败。",
        "expected_behavior": "在 router-core 已抽离的前提下，support 仍应保持可导入，并保留回归测试依赖的兼容行为。",
        "actual_behavior": "多条 router 路径因为 follow-up/media helper、ontology predicate、模块缓存和 token 估算逻辑缺失而失败。",
        "root_cause": "一次错误补丁破坏了 support 模块顶部区域，删掉了 router_core 仍经由传递依赖访问的大量模块级 import、常量与 helper 定义。",
        "fix_notes": "重建缺失的 support header surface，恢复 facade-aware 兼容 seam，补回所需媒体与音乐 helper/import/state，并修复 CJK token 计数回归。",
        "additional_notes": "边界测试已同步到委托式 router owner 结构，文件见 tests/dev/test_nav_dashboard_boundaries.py。",
    },
    "dashboard_owner_source_corruption": {
        "title": "Dashboard owner 源码损坏阻断 nav_dashboard 启动",
        "summary": "多个 dashboard service owner 文件发生文本损坏，导致语法或导入失败，站点无法打开。",
        "repro_query": "在 services-root 清理后导入 nav_dashboard.web.main 或直接打开 nav_dashboard 站点。",
        "expected_behavior": "dashboard service owner 模块可以正常导入，FastAPI 应用可正常启动。",
        "actual_behavior": "导入时在 dashboard_page_service、dashboard_jobs、dashboard_custom_cards_service 等模块出现语法错误。",
        "root_cause": "dashboard 子包中的源码文本被破坏，出现非法 token、断裂字符串和损坏 import。",
        "fix_notes": "根据现有测试与调用契约重建受影响的 dashboard owner 模块，并用 targeted pytest 与应用导入烟测验证。",
        "additional_notes": "修复文件包括 nav_dashboard/web/services/dashboard/dashboard_page_service.py、dashboard_overview_service.py、dashboard_runtime_data_service.py、dashboard_jobs.py、dashboard_custom_cards_service.py。",
    },
    "support_import_time_nameerror": {
        "title": "Router owner 抽离后留下 support 导入期 NameError",
        "summary": "从 support.py 删除已迁移的 router helper 后，残留的装配引用和缺失的 compat loader 让 agent_service 导入直接失败。",
        "repro_query": "在 router-owner 重构后导入 nav_dashboard.web.services.agent_service。",
        "expected_behavior": "support.py 应能成功导入，同时把 router 逻辑委托给 owner 模块。",
        "actual_behavior": "导入时因为已迁移 helper 名称残留和 compat accessor 缺失而报 NameError。",
        "root_cause": "bulk cleanup 后，support.py 仍用已删除的本地 helper 名称装配 trace/runtime 依赖，同时还丢了 lazy compat loader。",
        "fix_notes": "把残留引用改接 router owner helper，恢复 lazy compat loader，为导入期 trace 装配补 lazy wrapper，并补回 observability 仍使用的 classification-conformance helper。",
        "additional_notes": "相关文件：nav_dashboard/web/services/agent/support.py、nav_dashboard/web/services/agent/domain/router_core.py、nav_dashboard/web/services/agent/domain/router_helpers.py。",
    },
    "ticket_storage_under_runtime_data": {
        "title": "Ticket 事件日志被当成运行态数据存放",
        "summary": "持久 ticket 历史被放在 data/nav_dashboard 这类运行态路径下，容易被误判成可清理数据，也让恢复与治理变得混乱。",
        "expected_behavior": "Ticket 历史应作为持久核心资产单独存放，并在兼容旧路径时自动迁移。",
        "actual_behavior": "tickets 与 hook、文档都指向会被视为运行态残留的数据路径。",
        "root_cause": "ticket_store.py 和 nav_dashboard 运行路径契约把 ticket event log 当成了 app runtime state。",
        "fix_notes": "恢复 canonical ticket log 到 data/nav_dashboard/tickets/tickets.jsonl，并同步 legacy 迁移、hook 默认路径与存储契约说明。",
        "additional_notes": "关键文件：core_service/ticket_store.py、nav_dashboard/web/services/runtime_paths.py、scripts/dev/bug_ticket_sync_hook.py。",
    },
    "ticket_runtime_storage_path": {
        "title": "Ticket canonical 存储路径错误漂移到 records",
        "summary": "Ticket 存储被错误迁到非 canonical 路径，导致恢复、展示和运维语义全部混乱。",
        "repro_query": "把 tickets 恢复到 nav_dashboard 后刷新 dashboard tickets 视图。",
        "expected_behavior": "Tickets 应直接持久化在 data/nav_dashboard/tickets/tickets.jsonl，并从这里恢复展示。",
        "actual_behavior": "代码一度回指 records 路径，清理与备份语义也把 tickets 当成运行残留。",
        "root_cause": "Ticket path 常量和迁移逻辑偏移到了错误的 records/nav_dashboard 路径。",
        "fix_notes": "恢复 canonical 路径到 data/nav_dashboard/tickets/tickets.jsonl，增加从 records 的单向迁移，移除 tickets 的运行时清理归类，并对齐文档和 hooks。",
        "additional_notes": "同时清理了修复后不再需要的 stray records 目录。",
    },
    "personal_media_benchmark_router_contract_regressions": {
        "title": "个人媒体问题上的 router 与 benchmark 合约出现回归",
        "summary": "近期 benchmark/router 改动让 follow-up、创作者集合、strict-scope 和污染控制在个人媒体问题上出现回归。",
        "repro_query": "运行最近覆盖个人收藏、follow-up 和 media contamination 的回归 benchmark case。",
        "expected_behavior": "Router 与 benchmark 合约应稳定保留 personal-media scope、strict-scope 证据和 contamination 检查。",
        "actual_behavior": "相关 case 出现失败，或在 benchmark 质量检查中产生误报和漏报风险。",
        "root_cause": "router helper、benchmark payload 抽取和回归 case 期望之间发生了 contract drift。",
        "fix_notes": "更新 router/benchmark 测试及配套逻辑，使目标回归集合重新稳定通过。",
        "additional_notes": "这也是本轮回填到 live ticket store 并标记 closed 的第二类当前会话问题。",
    },
    "ticket_hook_legacy_flat_path": {
        "title": "Bug-ticket hook 仍写入旧的扁平 nav_dashboard data 根路径",
        "summary": "仓库与本地 hook 配置仍把 bug-ticket sync 指向 data/nav_dashboard，而不是 canonical tickets 子目录，导致新票据继续绕过主存储。",
        "repro_query": "删除 legacy records 或 flat ticket 文件后，再创建或同步 BUG-TICKET 标记。",
        "expected_behavior": "所有 hook 触发的 ticket 写入都应落到 tickets.jsonl，sidecar 也应位于同一个 tickets 目录。",
        "actual_behavior": "hook 环境变量仍指向扁平旧路径，导致 canonical ticket store 可能为空，而新 tickets 被写到别处。",
        "root_cause": "hook 配置里的 BUG_TICKET_SYNC_DIR 与 sidecar env path 仍是旧值，sync 脚本也没有强制把旧路径归一到 canonical store。",
        "fix_notes": "把 hook 配置统一改到 data/nav_dashboard/tickets，并在 bug_ticket_sync_hook.py 中加入路径归一化，让旧 flat 路径或 records 路径都自动映射到 canonical store。",
        "additional_notes": "已为此补上 tests/dev/test_bug_ticket_sync_hook.py 回归覆盖。",
    },
    "ticket_canonical_store_missing": {
        "title": "路径漂移后 canonical nav_dashboard ticket store 实体缺失",
        "summary": "data/nav_dashboard/tickets 下的 canonical ticket store 一度不存在，所以删掉 legacy 路径残留后，dashboard 就失去了稳定 ticket 数据源。",
        "repro_query": "移除 legacy records 路径后再加载 nav_dashboard tickets。",
        "expected_behavior": "无论是否清理 legacy 路径，dashboard 都应能从 tickets.jsonl 持续读取 tickets。",
        "actual_behavior": "canonical 文件缺失时，一旦清理旧残留，dashboard tickets 就会看起来像被清空。",
        "root_cause": "此前路径漂移把数据写到了 canonical 之外的位置，而且 canonical 文件本身没有真正落盘。",
        "fix_notes": "根据当前机器上仍可恢复的完整事件日志源重建 canonical ticket 文件，并把当前会话里明确可恢复的 tickets 重新写回。",
        "additional_notes": "当前本机可恢复状态是 617 条 event 折叠成 388 条 active ticket；若要回到 500+，还需要这台机器之外更完整的备份源。",
    },
    "personal_media_benchmark_routing_regression": {
        "title": "个人媒体 benchmark 路由在 follow-up 与创作者集合题上发生回归",
        "summary": "多条 Agent/Hybrid benchmark case 在个人媒体 follow-up、创作者排序和系列总结问题上发生误分类，连带影响 query_class、subject_scope、answer_shape、media_family 与工具选择。",
        "expected_behavior": "共享 router/planner 逻辑应稳定保留继承来的 personal scope，推断正确的 media family，并让创作者类与总结类问题走一致的路由。",
        "actual_behavior": "router 丢失了 follow-up 继承，在宽松措辞里漏掉创作者锚点，还让陈旧 media-type 提示污染个人评论总结路径。",
        "root_cause": "短 follow-up cue 检测、创作者表面形式解析、personal scope 推断，以及 personal review collection 的 planner contract 成形之间都存在缺口。",
        "fix_notes": "收紧 follow-up cue 检测，补更稳的 creator fallback parsing，在合适场景提升 creator metadata anchors，并统一 personal scope、media-family 推断与 legacy benchmark alias 归一化。",
    },
    "agent_stream_evidence_status": {
        "title": "Agent 流式状态被跳过工具噪声掩盖真实本地文档证据",
        "summary": "流式预览把 skipped tool 的摘要展示在前面，却没有概括真正使用到的本地文档证据，用户容易误以为这轮没有用到本地资料。",
        "expected_behavior": "可见的流式状态应优先反映真正参与回答的有效工具结果，并简明总结本轮证据来源。",
        "actual_behavior": "界面先显示被跳过的 Wiki 或媒体工具行，随后只给出笼统完成提示，即使本地文档已被使用也看不出来。",
        "root_cause": "round_lifecycle_runner 仍会发出 skipped tool_done 事件，前端又原样展示这些噪声状态，缺少对实际证据的汇总输出。",
        "fix_notes": "隐藏 skipped tool_done 的可见状态，并在生成最终回答前补发一条概括本地文档、媒体或外部证据的摘要状态。",
    },
    "rag_input_font_mismatch": {
        "title": "RAG 输入框字体样式与应用其余区域不一致",
        "summary": "RAG 文本框没有继承应用统一字体栈，浏览器默认 textarea 字体直接暴露出来，视觉上和 Agent 输入区不一致。",
        "expected_behavior": "RAG 输入框应继承应用的统一字体样式，与其余输入组件保持一致。",
        "actual_behavior": "RAG 输入框显示浏览器默认字体，看起来突兀且不统一。",
        "root_cause": "textarea 样式缺少 font 继承。",
        "fix_notes": "为 RAG textarea 增加字体继承样式，并补一条边界回归覆盖。",
    },
    "personal_record_second_person": {
        "title": "个人记录类回答错误使用第一人称而不是第二人称",
        "summary": "个人记录答案经常写成“我/我的”，与产品面向用户复述其记录的口吻不一致，读起来很别扭。",
        "expected_behavior": "这类回答应统一使用“你/你的”来描述用户自己的记录。",
        "actual_behavior": "提示词和答案归一化链路仍会鼓励或保留“我/我的”表述。",
        "root_cause": "prompt_assembly 与 final_answer_runner 都沿用了第一人称的旧约束。",
        "fix_notes": "把个人记录类提示词和最终清洗规则统一切到第二人称，并避免误删合法的记录尾句。",
    },
    "trace_timing_overexpanded_view": {
        "title": "Trace timing 视图缺少默认信息层级",
        "summary": "Trace timing 界面把概览阶段和所有细粒度耗时一次性全部展开，用户很难先快速看出瓶颈在哪里。",
        "expected_behavior": "默认先展示顶层阶段，再把叶子级细节折叠起来，需要时再展开查看。",
        "actual_behavior": "概览行和细项行同时展开在同一块区域里，信息密度过高。",
        "root_cause": "renderTraceStageBars 直接把完整 timing row 集合一次性渲染出来，没有摘要/细节分层。",
        "fix_notes": "默认仅渲染顶层阶段，把细项放进 details 折叠区，并额外高亮最慢的细节阶段。",
    },
    "personal_list_expand_main_body_duplication": {
        "title": "personal list-plus-expand 主回答仍重复输出本地记录内容",
        "summary": "个人记录集合题的主回答里同时出现 LLM 总结和本地记录块，正文显得重复，LLM 总结也被冲淡。",
        "expected_behavior": "主回答应主要保留 LLM 的总结性正文，把本地与外部支持链接放在正文之外。",
        "actual_behavior": "personal_record + list_plus_expand 仍会把 row_blocks 直接追加到主回答中。",
        "root_cause": "media_answer_renderer 还在 personal-record list_plus_expand 路径追加结构化本地记录块。",
        "fix_notes": "抑制该路径在主正文里追加结构化块，同时调整 prompt，让主回答保持 LLM-only，支持链接移到页脚。",
    },
    "uncited_personal_record_links_lost_after_reference_trim": {
        "title": "参考资料裁剪后未引用的个人记录链接丢失可点击入口",
        "summary": "当参考资料只保留正文引用过的条目时，被省略的个人记录项和未引用外链可能直接消失，用户无法再点回去看。",
        "expected_behavior": "即使正文只保留 cited refs，仍应给未引用但有价值的本地或外部链接一个可浏览的补充区域。",
        "actual_behavior": "最终答案可能只剩一个“省略若干项”的说明，却没有任何可点击入口。",
        "root_cause": "final_answer_runner 只保留正文里被引用的编号参考，没有额外的 browse links 通道。",
        "fix_notes": "保留 cited refs 的编号参考区，同时把 uncited 的本地/外部链接追加到“更多本地资料 / 更多外部参考”补充区。",
    },
    "benchmark_module_aware_query_count_cap": {
        "title": "Benchmark query count 上限忽略已选模块的实际可用样本数",
        "summary": "Benchmark UI 仍允许选择超过某些已选模块可安全运行的 query count，界面显示和真实执行规模不一致。",
        "expected_behavior": "count 选项应根据当前选中的模块能力收紧到安全上限。",
        "actual_behavior": "UI 可能允许一个某模块根本跑不满的数量，实际运行条数会比界面暗示的更少。",
        "root_cause": "case-set 元数据没有暴露模块级安全 cap，app.js 只能使用全局 max_query_count_per_type。",
        "fix_notes": "在 benchmark 元数据和 API 中增加 module_length_counts / module_max_query_count_per_type，并让前端按已勾选模块的最小安全上限限制选项。",
    },
    "non_followup_prompt_history_pollution": {
        "title": "非 follow-up 问题的回答提示词仍混入上一轮历史",
        "summary": "即使路由已经判定当前问题是独立提问，回答提示词里仍带上最近的用户历史，模型会被误导成继续上一轮话题。",
        "expected_behavior": "当 followup_mode 为 none 且 carry_over_from_previous_turn 为 false 时，回答提示词不应再拼接上一轮历史。",
        "actual_behavior": "standalone turn 仍保留最多两条前序 user message，导致模型延续上一问。",
        "root_cause": "prompt_assembly._trim_history_for_prompt 对独立提问仍保留了一部分历史。",
        "fix_notes": "独立提问场景直接清空 prior history，并顺便去掉 agent_service 中重复的 media follow-up fallback。",
    },
    "entity_detail_overlay_over_disabled": {
        "title": "entity-detail overlay 修复范围过宽，误伤普通详情问答的自然总结",
        "summary": "为了修掉个人 detail card 的重复问题，entity_detail_answer 整个分支都被禁掉了 llm summary overlay，普通详情回答也因此变得过硬、过表格化。",
        "expected_behavior": "个人记录 detail card 保持 structured-only，普通单实体详情仍可保留自然语言总结覆盖层。",
        "actual_behavior": "所有 entity_detail_answer 路径都变成纯结构化输出。",
        "root_cause": "answer_policy.py 直接把整个 entity_detail_answer 分支的 llm_summary_on_structured 设成了 False。",
        "fix_notes": "恢复条件化行为：非 personal 的单实体详情保留 summary overlay，personal detail card 继续禁用。",
    },
    "personal_list_expand_full_appendix_dump": {
        "title": "personal list-plus-expand 主回答仍倾倒整块本地记录附录",
        "summary": "personal-record list_plus_expand 的正文仍会直接拼上一大段结构化本地记录，导致输出冗长、重复，和参考资料区的分工失衡。",
        "expected_behavior": "主回答应保留高层总结，只附少量代表性摘录，而不是整份本地记录块。",
        "actual_behavior": "structured appendix 逻辑默认把保留下来的本地记录大块追加进主回答。",
        "root_cause": "prompt 和 renderer 都把 structured_appendix_expected 理解成“完整本地记录都应塞进正文”。",
        "fix_notes": "调整 prompt 文案，改为只允许附精选摘要；renderer 对 personal_record + list_plus_expand 最多追加前 5 行并注明还有若干省略项。",
    },
    "benchmark_case_set_module_metadata_missing": {
        "title": "Benchmark case-set 的模块元数据没有暴露给前端",
        "summary": "Benchmark catalog 已经推导了 supported_modules 和 module_case_counts，但 API 与前端都没消费，用户看不到样本池和模块的适配关系。",
        "expected_behavior": "case-set API 应返回模块适配元数据，前端据此过滤或禁用当前模块不可运行的样本池。",
        "actual_behavior": "前端仍只把 case set 当作普通下拉项和 query-count 上限来源。",
        "root_cause": "benchmark case-set 响应契约没有暴露 supported_modules / module_case_counts，app.js 也没有消费这些字段。",
        "fix_notes": "把模块适配字段加到 API 契约里，并让前端据此显示和禁用当前模块下没有样本的 case set。",
    },
    "agent_reference_section_not_trimmed_to_citations": {
        "title": "Agent 参考资料附录未裁剪到正文实际引用项",
        "summary": "最终回答会把完整参考列表整段附上，即使正文只引用了其中一部分，也会出现尾部未引用条目和编号错配。",
        "expected_behavior": "最终参考资料区只保留正文里真正引用到的来源，并按显示编号重新映射。",
        "actual_behavior": "finalizer 虽然会 linkify 正文里的 [n]，但附录仍原样带上全部重建后的参考列表。",
        "root_cause": "final_answer_runner 缺少从正文引用反向裁剪 reference appendix 的步骤。",
        "fix_notes": "新增 citation extraction、reference filtering、inline renumbering，再在最后执行 linkify。",
    },
    "entity_detail_summary_overlay_duplication": {
        "title": "entity-detail 媒体回答会重复展示同一份本地记录",
        "summary": "单实体详情回答里，LLM summary overlay 和 structured detail block 会同时描述同一份本地记录，造成内容重复。",
        "expected_behavior": "entity-detail 输出应只保留一套一致的本地记录加外部补充展示，不重复叙述同一条本地信息。",
        "actual_behavior": "自然语言 summary overlay 重复了 detail card 里已有的本地条目描述。",
        "root_cause": "answer_policy 对 entity_detail_answer 打开了 llm_summary_on_structured，而 detail-card 已经包含完整本地记录。",
        "fix_notes": "默认关闭 entity-detail 的 summary overlay，让 compose_round_answer 直接返回结构化 detail answer。",
    },
    "mediawiki_parse_timeout_budget": {
        "title": "MediaWiki 直解析在 entity detail 查询上会吃满整段超时预算",
        "summary": "parse_mediawiki_page 会把全局 MediaWiki timeout 直接用于单次 parse，导致 entity detail 查询里一次外部解析就可能卡住很久。",
        "expected_behavior": "Wiki parse 应在更短预算内失败回退，避免拖垮整条详情查询链路。",
        "actual_behavior": "单次 direct parse 可能等待约 60 秒才报错。",
        "root_cause": "_mediawiki_action_request 总是使用全局 MEDIAWIKI_TIMEOUT，而 parse_mediawiki_page 又原样透传这段预算。",
        "fix_notes": "为 MediaWiki action request 增加 timeout_override，并把 parse-mediawiki 的 direct parse 限制在更短的专用超时内。",
    },
}


@dataclass(frozen=True)
class ReferenceImportResult:
    parsed_count: int
    deduped_count: int
    created_count: int
    updated_count: int
    unchanged_count: int


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_signature(*parts: Any) -> str:
    raw = " ".join(_safe_text(part).lower() for part in parts if _safe_text(part))
    if not raw:
        return ""
    return re.sub(r"[^0-9a-z\u4e00-\u9fff]+", "", raw)


def _collect_json_block(lines: list[str], start_index: int, initial_payload: str = "") -> tuple[str, int]:
    payload = _safe_text(initial_payload)
    if not payload and 0 <= start_index < len(lines):
        payload = _safe_text(lines[start_index])
    current_index = start_index
    if payload:
        try:
            json.loads(payload)
            return payload, current_index
        except json.JSONDecodeError:
            pass
    buffer = payload
    current_index += 1
    while current_index < len(lines):
        candidate_line = lines[current_index].rstrip("\n")
        stripped = candidate_line.strip()
        if not buffer and not stripped:
            current_index += 1
            continue
        if buffer and (stripped.startswith("BUG-TICKET:") or re.match(r"^#\s*4月\d+日$", stripped)):
            break
        buffer = f"{buffer}\n{candidate_line}".strip() if buffer else candidate_line
        try:
            json.loads(buffer)
            return buffer, current_index
        except json.JSONDecodeError:
            current_index += 1
            continue
    raise ValueError("无法解析 BUG-TICKET JSON")


def parse_reference_tickets(reference_path: str | Path) -> list[dict[str, Any]]:
    path = Path(reference_path)
    lines = path.read_text(encoding="utf-8").splitlines()
    entries: list[dict[str, Any]] = []
    current_day = 0
    line_index = 0
    while line_index < len(lines):
        line = lines[line_index].strip()
        heading_match = re.match(r"^#\s*4月(\d+)日$", line)
        if heading_match:
            current_day = int(heading_match.group(1))
            line_index += 1
            continue
        if not line.startswith("BUG-TICKET:"):
            line_index += 1
            continue
        payload_text = line[len("BUG-TICKET:") :].strip()
        try:
            json_text, consumed_index = _collect_json_block(lines, line_index if payload_text else line_index + 1, payload_text)
            payload = json.loads(json_text)
        except (ValueError, json.JSONDecodeError):
            line_index += 1
            continue
        if isinstance(payload, dict):
            item = dict(payload)
            item["_reference_day"] = current_day
            item["_reference_order"] = len(entries)
            entries.append(item)
        line_index = consumed_index + 1
    return entries


def _translate_reference_entry(entry: dict[str, Any]) -> dict[str, Any]:
    category = _safe_text(entry.get("category")).lower()
    translations = _PURE_ENGLISH_TRANSLATIONS.get(category)
    if not translations:
        return dict(entry)
    translated = dict(entry)
    translated.update(translations)
    return translated


def _reference_timestamp(day: int) -> str:
    safe_day = max(1, min(30, int(day or 1)))
    return datetime(_YEAR, 4, safe_day, 0, 0, 0).isoformat(timespec="seconds")


def _normalize_reference_entry(entry: dict[str, Any]) -> dict[str, Any]:
    translated = _translate_reference_entry(entry)
    timestamp = _reference_timestamp(int(translated.get("_reference_day") or 1))
    normalized = {
        "title": _safe_text(translated.get("title")),
        "status": (_safe_text(translated.get("status")) or "open").lower(),
        "priority": (_safe_text(translated.get("priority")) or "medium").lower(),
        "domain": _safe_text(translated.get("domain")),
        "category": _safe_text(translated.get("category")),
        "summary": _safe_text(translated.get("summary")),
        "related_traces": [
            _safe_text(item)
            for item in list(translated.get("related_traces") or [])
            if _safe_text(item)
        ],
        "repro_query": _safe_text(translated.get("repro_query")),
        "expected_behavior": _safe_text(translated.get("expected_behavior")),
        "actual_behavior": _safe_text(translated.get("actual_behavior")),
        "root_cause": _safe_text(translated.get("root_cause")),
        "fix_notes": _safe_text(translated.get("fix_notes")),
        "additional_notes": _safe_text(translated.get("additional_notes")),
        "created_at": timestamp,
        "updated_at": timestamp,
        "created_by": "reference_import",
        "updated_by": "reference_import",
        "_reference_day": int(translated.get("_reference_day") or 1),
        "_reference_order": int(translated.get("_reference_order") or 0),
    }
    return normalized


def _reference_match_key(entry: dict[str, Any]) -> str:
    category = _safe_text(entry.get("category")).lower()
    if category:
        return f"category:{category}"
    return f"signature:{_normalize_signature(entry.get('title'), entry.get('summary'), entry.get('root_cause'))}"


def dedupe_reference_tickets(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[str, dict[str, Any]] = {}
    for raw_entry in entries:
        normalized = _normalize_reference_entry(raw_entry)
        key = _reference_match_key(normalized)
        existing = deduped.get(key)
        sort_key = (int(normalized.get("_reference_day") or 0), int(normalized.get("_reference_order") or 0))
        if existing is None:
            deduped[key] = normalized
            continue
        existing_sort_key = (int(existing.get("_reference_day") or 0), int(existing.get("_reference_order") or 0))
        if sort_key >= existing_sort_key:
            deduped[key] = normalized
    return sorted(deduped.values(), key=lambda item: (int(item.get("_reference_day") or 0), int(item.get("_reference_order") or 0)))


def _pick_status(existing_status: str, incoming_status: str) -> str:
    existing_key = _safe_text(existing_status).lower()
    incoming_key = _safe_text(incoming_status).lower()
    if _STATUS_RANK.get(existing_key, 0) >= _STATUS_RANK.get(incoming_key, 0):
        return existing_key or incoming_key or "open"
    return incoming_key or existing_key or "open"


def _pick_priority(existing_priority: str, incoming_priority: str) -> str:
    existing_key = _safe_text(existing_priority).lower()
    incoming_key = _safe_text(incoming_priority).lower()
    if _PRIORITY_RANK.get(existing_key, 0) >= _PRIORITY_RANK.get(incoming_key, 0):
        return existing_key or incoming_key or "medium"
    return incoming_key or existing_key or "medium"


def _merge_ticket(existing: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
    merged = dict(existing)
    for field in (
        "title",
        "domain",
        "category",
        "summary",
        "repro_query",
        "expected_behavior",
        "actual_behavior",
        "root_cause",
        "fix_notes",
        "additional_notes",
    ):
        incoming_value = _safe_text(incoming.get(field))
        if incoming_value:
            merged[field] = incoming_value
    merged["status"] = _pick_status(_safe_text(existing.get("status")), _safe_text(incoming.get("status")))
    merged["priority"] = _pick_priority(_safe_text(existing.get("priority")), _safe_text(incoming.get("priority")))
    trace_values = []
    seen: set[str] = set()
    for candidate in list(existing.get("related_traces") or []) + list(incoming.get("related_traces") or []):
        value = _safe_text(candidate)
        if not value or value in seen:
            continue
        seen.add(value)
        trace_values.append(value)
    merged["related_traces"] = trace_values
    merged["created_by"] = _safe_text(existing.get("created_by")) or _safe_text(incoming.get("created_by")) or "reference_import"
    merged["updated_by"] = "reference_import"
    return merged


def _existing_ticket_signature(ticket: dict[str, Any]) -> str:
    return _normalize_signature(ticket.get("title"), ticket.get("summary"), ticket.get("root_cause"))


def _find_matching_ticket(entry: dict[str, Any], tickets: list[dict[str, Any]]) -> dict[str, Any] | None:
    category = _safe_text(entry.get("category")).lower()
    if category:
        category_matches = [ticket for ticket in tickets if _safe_text(ticket.get("category")).lower() == category]
        if category_matches:
            return max(category_matches, key=lambda item: (_safe_text(item.get("updated_at")), _safe_text(item.get("ticket_id"))))
    incoming_signature = _normalize_signature(entry.get("title"), entry.get("summary"), entry.get("root_cause"))
    if not incoming_signature:
        return None
    signature_matches = [ticket for ticket in tickets if _existing_ticket_signature(ticket) == incoming_signature]
    if not signature_matches:
        return None
    return max(signature_matches, key=lambda item: (_safe_text(item.get("updated_at")), _safe_text(item.get("ticket_id"))))


def import_reference_tickets(reference_path: str | Path) -> ReferenceImportResult:
    parsed = parse_reference_tickets(reference_path)
    deduped = dedupe_reference_tickets(parsed)
    existing_tickets = list_tickets(limit=5000, sort="updated_desc")
    created_count = 0
    updated_count = 0
    unchanged_count = 0
    for entry in deduped:
        match = _find_matching_ticket(entry, existing_tickets)
        if match is None:
            created = create_ticket({key: value for key, value in entry.items() if not key.startswith("_")})
            existing_tickets.insert(0, created)
            created_count += 1
            continue
        merged = _merge_ticket(match, entry)
        patch = {
            key: value
            for key, value in merged.items()
            if key in match and key != "ticket_id" and value != match.get(key)
        }
        if not patch:
            unchanged_count += 1
            continue
        updated = update_ticket(_safe_text(match.get("ticket_id")), patch)
        existing_tickets = [updated if _safe_text(ticket.get("ticket_id")) == _safe_text(updated.get("ticket_id")) else ticket for ticket in existing_tickets]
        updated_count += 1
    return ReferenceImportResult(
        parsed_count=len(parsed),
        deduped_count=len(deduped),
        created_count=created_count,
        updated_count=updated_count,
        unchanged_count=unchanged_count,
    )
