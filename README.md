# personal-ai-stack

本仓库是一个本地优先的多服务 AI 工作台，包含三个可独立运行、也可协同运行的子系统：

- `ai_conversations_summary`（默认 `:8000`）：对话文档处理 + RAG 检索问答
- `library_tracker`（默认 `:8091`）：个人媒体库管理与检索
- `nav_dashboard`（默认 `:8092`）：统一入口、跨系统 Agent 问答与观测面板

## 1. 总体架构

```text
User / Browser
    |
    v
nav_dashboard (FastAPI, :8092)
    |-- 调用 ai_conversations_summary (:8000)
    |-- 调用 library_tracker (:8091)
    |-- 可选调用 Tavily Web Search

ai_conversations_summary
    |-- 文档预处理与分类归档
    |-- 向量索引（FAISS + embedding）
    |-- RAG 检索与 rerank

library_tracker
    |-- CSV/结构化条目处理
    |-- 媒体库查询 API
    |-- 向量检索与图谱扩展（按配置启用）

core_service (共享模块)
    |-- 配置加载
    |-- OpenAI-compatible LLM client
    |-- 通用向量索引能力
```

## 2. 核心实现细节

### 2.1 `nav_dashboard` 的 Agent 协调链路

当前 Agent 已经从早期的单一 `query_type` 路由，演进为 `RouterDecision -> RoutingPolicy -> PostRetrievalAssessment -> Guardrail/Answer` 的分层链路：

1. 对用户问题做 query profile 分析与配额检查。
2. 由 Router 构建 `RouterDecision`：
   - LLM 结构化理解（`domain / lookup_mode / ranking / rewritten_queries`）
   - follow-up 策略判定（`standalone / inherit_entity / inherit_filters / inherit_timerange`）
   - 媒体语义槽提取与 schema 投影
   - domain arbitration（如 `tech_primary`、`mixed_due_to_entity_plus_tech`、`media_surface_wins`、`llm_media_weak_general`）
3. `RoutingPolicy` 根据决策结果规划工具：
   - 文档 RAG：`query_document_rag`
   - 媒体库检索：`query_media_record`
   - 可选联网检索：`search_web`
   - 可选媒体概念扩展：`expand_mediawiki_concept`
   - 可选外部影视元数据：`search_tmdb_media`
4. 执行工具后生成 `PostRetrievalAssessment`，记录媒体候选数量、TMDB 结果、doc similarity、reference-limit 截断等信息。
5. 由 guardrail 和 answer policy 决定最终输出模式：
   - `normal`
   - `annotated`
   - `restricted`
6. 持久化 trace、会话状态、guardrail flags、工具调用结果与仲裁字段。

补充说明：

- 当前路由不是“纯算法”也不是“纯 LLM”，而是 `LLM understanding + follow-up policy + schema projection + deterministic routing policy` 的混合系统。
- 技术/通用知识问题不再因为低置信度而直接触发媒体式 restricted 回答；restricted 主要保留给真正存在媒体上下文歧义的场景。
- trace 现已包含 `arbitration` 字段，便于定位“为什么最终走了 tech / media / mixed / general”。

### 2.2 `ai_conversations_summary` 的 RAG 链路

1. 可选 Query Rewrite（默认最多 2 条检索 query，且原 query 保留更高优先级）。
2. 多 query 并行向量召回（FAISS）。
3. 结果合并后进行 rerank（默认 `BAAI/bge-reranker-base`，可通过环境变量回切）。
4. 可选融合联网搜索结果。
5. 组装上下文并调用本地/云端 LLM 生成回答。

### 2.3 共享层 `core_service`

- `config.py`：统一读取本地配置与环境变量。
- `llm_client.py`：统一封装 OpenAI-compatible API 调用。
- `rag_vector_index.py`：共享 embedding 与向量索引读写能力。
- `trace_store.py`：共享 trace 存储，按月滚动写入 `nav_dashboard/data/trace_records_YYYY_MM.jsonl`。
- `ticket_store.py`：共享 Ticket 事件存储，使用 append-only 事件日志 `nav_dashboard/data/tickets.jsonl`。

## 3. 目录导读

- `ai_conversations_summary/`：文档加工与 RAG 服务
- `library_tracker/`：媒体库服务
- `nav_dashboard/`：统一 Web 门户与 Agent
- `core_service/`：跨项目共享配置与模型调用
- `data/`：仓库级运行数据（如 benchmark 结果）

## 4. 当前能力快照

- `nav_dashboard` 现已提供：Trace 查询、Tickets 管理、按周 Ticket 提交/关闭趋势图、任务中心、聊天反馈查看、运行时数据清理。
- Agent trace 会记录：`RouterDecision`、`arbitration`、follow-up state before/after、工具计划、guardrail mode、reference-limit 截断原因、TMDB/MediaWiki/媒体检索验证信息。
- 媒体类查询已支持语义到库 schema 的自动投影，例如：
  - `movie -> video + category=电影`
  - `anime -> video + category=动画`
  - `director / composer / developer -> author`
- 媒体实体解析已支持标题与创作者双通道归一，开始具备跨语言/别名对齐能力（例如标题别名、创作者名的规范化）。
- 媒体类回答支持更强的结构化输出：评分、短评、作者、出版方、渠道等本地字段可直接展开。
- 历史会话标题现在完整持久化，由前端按容器宽度单行省略显示，而不是后端硬截断。

### 4.1 媒体高层工具定义

仓库中已经引入一组可供 LLM 调用的高层媒体工具定义，位于 `nav_dashboard/web/services/media_tool_definitions.py`，主要包括：

- `resolve_entity`：将自由文本名称解析为本地媒体库中的 canonical title 或 canonical creator
- `get_title_detail`：按 canonical title 获取个人库中的完整条目
- `search_by_creator`：按创作者聚合作品
- `search_by_filters`：按语义过滤条件检索，内部自动完成 schema projection
- `resolve_and_search`：组合式单步工具

当前主链默认仍以 `query_media_record` 为兼容入口，但高层工具定义已经可以作为后续 tool-calling agent 的迁移基础。

## 5. 安装与启动（Windows）

### 4.1 创建统一虚拟环境

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
```

### 4.2 安装三个服务依赖

```powershell
.\.venv\Scripts\python.exe -m pip install -r .\ai_conversations_summary\requirements.txt
.\.venv\Scripts\python.exe -m pip install -r .\library_tracker\requirements.txt
.\.venv\Scripts\python.exe -m pip install -r .\nav_dashboard\requirements.txt
```

### 4.3 启动方式

- 启动统一入口（推荐）：

```powershell
.\nav_dashboard\launch_web.bat
```

说明：`launch_web.py` / `launch_web.bat` 会在启动时自动读取仓库根目录的 `env.local.ps1`，避免重启后继续沿用旧 shell 中残留的环境变量。

- 或分别启动：

```powershell
.\ai_conversations_summary\launch_web.bat
.\library_tracker\launch_web.bat
.\nav_dashboard\launch_web.bat
```

### 4.4 访问地址

- Dashboard: `http://127.0.0.1:8092/`
- AI Conversations Summary: `http://127.0.0.1:8000/`
- Library Tracker: `http://127.0.0.1:8091/`

## 6. 配置约定

- 推荐在仓库根目录维护 `env.local.ps1`；当前 launcher 会自动读取该文件。
- embedding 模型可通过环境变量覆盖：
  - `LOCAL_EMBEDDING_MODEL`
  - `DEEPSEEK_EMBEDDING_MODEL`
- RAG 检索默认配置：
    - `AI_SUMMARY_RERANKER_MODEL`：默认 `BAAI/bge-reranker-base`
    - `AI_SUMMARY_QUERY_REWRITE_COUNT`：默认 `2`
    - `AI_SUMMARY_PRIMARY_QUERY_SCORE_BONUS`：原 query 的合并加权
- Agent 文档检索默认配置：
    - `NAV_DASHBOARD_QUERY_REWRITE_COUNT`：默认 `2`
    - `NAV_DASHBOARD_PRIMARY_QUERY_SCORE_BONUS`：原 query 的合并加权
- Agent 路由/提示词相关配置：
    - `NAV_DASHBOARD_LONG_QUERY_MIN_TOKENS`
    - `NAV_DASHBOARD_PROMPT_HISTORY_MAX_MESSAGES`
    - `NAV_DASHBOARD_PROMPT_HISTORY_ITEM_MAX_CHARS`
    - `NAV_DASHBOARD_PROMPT_MEMORY_MAX_CHARS`
- 外部媒体资料扩展配置：
    - `NAV_DASHBOARD_MEDIAWIKI_ZH_API_URL`
    - `NAV_DASHBOARD_MEDIAWIKI_EN_API_URL`
    - `NAV_DASHBOARD_MEDIAWIKI_TIMEOUT`
    - `NAV_DASHBOARD_MEDIAWIKI_USER_AGENT`
    - `NAV_DASHBOARD_MEDIAWIKI_API_USER_AGENT`
    - `NAV_DASHBOARD_MEDIAWIKI_CONTACT`
    - `NAV_DASHBOARD_TMDB_API_KEY`
    - `NAV_DASHBOARD_TMDB_READ_ACCESS_TOKEN`
    - `NAV_DASHBOARD_TMDB_API_BASE_URL`
    - `NAV_DASHBOARD_TMDB_TIMEOUT`
    - `NAV_DASHBOARD_TMDB_LANGUAGE`
- 若出现默认模型未生效，优先检查以上环境变量是否指向旧模型。

## 7. 迭代与优化建议

- 当前系统已经具备“可学习”的数据基础，但默认不会自动在线改写路由策略。
- 持续优化的主要抓手是：`trace_records`、`agent_metrics`、`chat_feedback`、`tickets`、`evals`、回归样例与 benchmark 结果。
- 如果要把它演进成反馈驱动的决策系统，建议路线是：
    1. 先把误路由/误判样本沉淀成结构化评测集。
    2. 用这些样本离线调整 arbitration policy、planner 规则、schema projection 与 guardrail 触发条件。
    3. 把 `PostRetrievalPolicy` 和 `AnswerPolicy` 继续从 `agent_service` 中拆出来，减少单体编排器复杂度。
    4. 再考虑训练轻量 router model 或 bandit/ranker，而不是直接做无约束在线学习。

## 8. 常见协作建议

- 运行时数据（会话、缓存、向量库）建议与源码提交分离。
- 优先通过子项目 README 查看模块内的实现细节和接口说明。
- 需要发布安装包时，使用 `ai_conversations_summary/release/` 下的流程文档。
