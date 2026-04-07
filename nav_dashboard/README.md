# Nav Dashboard

`nav_dashboard` 是整个工作台的统一入口。它既是一个 Web 门户，也是跨服务 Agent 编排层，负责把文档 RAG、媒体库、本地分析页和观测能力聚合到一处。

默认地址：`http://127.0.0.1:8092/`

## 角色

- 为用户提供统一聊天入口和多应用导航
- 对接 `ai_conversations_summary` 文档检索能力
- 对接 `library_tracker` 媒体库搜索与结构化结果
- 聚合 `property` 和 `journey` 的统计、报告与任务状态
- 提供 Trace、Ticket、Benchmark、任务中心和反馈管理

## 当前能力

### Agent

- Router 使用 `LLM understanding + follow-up policy + schema projection + deterministic routing policy` 的混合链路
- 根据问题在以下工具间规划：
  - `query_document_rag`
  - `query_media_record`
  - `search_web`
  - `expand_mediawiki_concept`
  - `search_tmdb_media`
- 支持 follow-up 继承、媒体实体解析、工具计划、结果分层和 guardrail 模式切换
- 媒体问答优先使用本地结构化字段直接生成答案，而不是把所有结果丢给 LLM 再自由概括

### Dashboard

- 系统总览：RAG 文档数、Library 条目数、图谱质量、API 用量、Agent / RAG 会话数
- 启动状态：读取 RAG 服务 `startup-status`，展示一致性检查与预热日志
- 任务中心：展示后台分析/刷新任务并支持移动端展开
- Trace：查询、导出、定位阶段耗时、查看 router / planner / guardrail 细节
- Tickets：列表、详情、创建、AI draft、更新、软删除、Trace 跳转、按周趋势图
- Benchmark：固定 case set、历史结果、router 分类回归、单项测试入口
  - Benchmark case 支持 planner contract 字段以及可选的 `quality_assertions`，可直接断言终态 answer / rewrite / guardrail / reference 顺序等用户感知结果
  - Benchmark 结果同时输出 taxonomy 级聚合与断言，可对污染回放、strict-scope、compare 终态质量分别设预算并在 Dashboard 直接盯盘
  - `session_contamination_v1` 会回放真实多轮污染 / 串题 / strict-scope 漏判 Trace，优先覆盖真实用户可见回归
- Chat Feedback：导出、清空、查看原始问题与 Trace 详情

## 关键目录

- `web/main.py`：FastAPI 应用入口、SSR 页面、静态资源版本注入与路由装配
- `web/api/agent.py`：聊天接口
- `web/api/dashboard.py`：Dashboard / Trace / Ticket / Usage / Runtime Data 等聚合 API
- `web/api/benchmark.py`：Benchmark API 与流式进度
- `web/services/benchmark_case_catalog.py`：Benchmark case catalog 加载与 case-level 质量断言元数据
- `web/api/response_models.py`：版本化 API response model（`api_schema_version`）
- `web/services/agent_service.py`：Agent 主编排入口
- `web/services/`：router、planner、answer、entity resolve、post-retrieval 等服务模块
- `web/static/`：Dashboard 与聊天前端资源；trace / tickets / benchmark / data-admin 已拆为独立 controller + bootstrap 模块
- `web/templates/`：主页面模板
- `data/`：会话、Trace、Benchmark 结果等运行态数据，以及 `tickets/` 下需要长期保留并纳入备份的 Ticket 核心资产

## 启动

### 本地启动

```powershell
..\.venv\Scripts\python.exe launch_web.py
```

或直接运行：

```text
launch_web.bat
```

`launch_web.py` 会自动：

- 读取仓库根目录 `env.local.ps1`
- 尝试重启到根 `.venv`
- 清理占用 8092 端口的旧进程
- 服务可用后打开浏览器

### 局域网部署

```text
deploy_lan_web.bat
```

该脚本会连同 `ai_conversations_summary`、`library_tracker`、`property`、`journey` 一起拉起。

## 依赖安装

推荐在仓库根目录完成安装：

```powershell
..\.venv\Scripts\python.exe -m pip install -r requirements.txt
..\.venv\Scripts\python.exe -m pip install -r ..\ai_conversations_summary\requirements.txt
..\.venv\Scripts\python.exe -m pip install -r ..\library_tracker\requirements.txt
```

如果需要完整联动，也建议安装 `property` 和 `journey` 的依赖。

## 常用环境变量

- `NAV_DASHBOARD_WEB_HOST`
- `NAV_DASHBOARD_WEB_PORT`
- `NAV_DASHBOARD_AI_SUMMARY_URL`
- `NAV_DASHBOARD_LIBRARY_TRACKER_URL`
- `NAV_DASHBOARD_LOCAL_LLM_URL`
- `NAV_DASHBOARD_LOCAL_LLM_MODEL`
- `NAV_DASHBOARD_QUERY_REWRITE_COUNT`
- `NAV_DASHBOARD_PRIMARY_QUERY_SCORE_BONUS`
- `NAV_DASHBOARD_TMDB_API_KEY`
- `NAV_DASHBOARD_TMDB_READ_ACCESS_TOKEN`
- `TAVILY_API_KEY`

## 数据落盘

以下路径通常属于运行态数据，不建议和源码改动一起提交：

- `../data/nav_dashboard/agent_sessions/`
- `../data/nav_dashboard/config/custom_cards.json`
- `../data/nav_dashboard/state/agent_quota.json`
- `../data/nav_dashboard/state/agent_quota_history.json`
- `../data/nav_dashboard/benchmark/results.json`
- `../data/nav_dashboard/trace_records/trace_records_YYYY_MM.jsonl`
- `../data/nav_dashboard/observability/chat_feedback.json`

以下路径属于需要长期保留的核心业务资产：

- `../data/nav_dashboard/tickets/tickets.jsonl`

Ticket 事件日志不会被归入运行时清理目标，并会进入 Dashboard 数据备份/恢复链路。自动备份频率为每周一一次。

## 健康检查

```powershell
Invoke-WebRequest -Uri "http://127.0.0.1:8092/healthz" -UseBasicParsing
```

返回 `{"status":"ok"}` 表示服务可用。
