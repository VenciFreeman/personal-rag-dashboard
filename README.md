# personal-ai-stack

本仓库是一个本地优先的个人 AI 工作台，当前包含 5 个 FastAPI Web 应用和 1 组共享核心模块：

- `nav_dashboard`：统一入口、跨服务 Agent、Trace/Ticket/Benchmark/任务中心
- `ai_conversations_summary`：文档整理、向量检索、RAG 问答
- `library_tracker`：个人媒体库管理、检索、图谱与 embedding 后台任务
- `property`：资产快照、收支记录、分析报告
- `journey`：旅行规划、归档编辑、复盘报告
- `core_service`：共享配置、认证、LLM 客户端、运行态数据与票据/Trace 基础设施

## 总览

```text
Browser
    |
    +-- nav_dashboard (8092)
    |     +-- 对接 ai_conversations_summary (8000)
    |     +-- 对接 library_tracker (8091)
    |     +-- 聚合 property (8093) / journey (8094) 状态与入口
    |
    +-- ai_conversations_summary (8000)
    |     +-- 文档整理 / 索引 / RAG / startup 自检与预热
    |
    +-- library_tracker (8091)
    |     +-- 媒体库 CRUD / 搜索 / embedding / 图谱 / 分析任务与 worker
    |
    +-- property (8093)
    |     +-- 资产与收支记录 / 图表 / 分析报告 / worker 任务
    |
    +-- journey (8094)
                +-- 行程编辑 / 归档 / 复盘报告 / worker 任务

Shared Core
    +-- core_service/config.py
    +-- core_service/llm_client.py
    +-- core_service/trace_store.py
    +-- core_service/ticket_store.py
    +-- core_service/runtime_data.py
```

## 当前能力

### nav_dashboard

- 统一聊天入口，按问题类型在文档 RAG、媒体检索、联网搜索之间规划工具
- 记录完整 Agent Trace，包括 router decision、arbitration、tool plan、guardrail、结果分层与参考截断原因
- Dashboard 页展示系统总览、启动状态、任务中心、Trace、Ticket 周趋势、聊天反馈与运行态数据清理
- Benchmark 支持固定 case set、历史结果和回归样例对比
- Ticket 系统基于 append-only 日志，支持列表、详情、创建、更新、AI draft、Trace 反查

### ai_conversations_summary

- 将对话整理为 Markdown 文档并建立向量索引
- 检索链路支持 query rewrite、query expansion、向量召回、轻量 BM25、cross-encoder rerank
- 回答阶段区分本地资料、通用知识、推断三类证据，并做 citation reconciliation
- 服务启动时会执行索引一致性检查、必要的自动修复，以及 embedding/reranker 预热

### library_tracker

- 管理阅读、音乐、视频、游戏等个人媒体条目
- 提供结构化字段检索、关键词检索、向量检索与图谱辅助能力
- 支持增量 embedding 刷新、别名生命周期、分析报告生成和后台任务调度
- 结果字段直接服务于 Dashboard Agent 的结构化回答，例如评分、短评、作者、渠道、出版方等

### property

- 资产总览图表、月度快照管理、收支流水与年度统计
- 支持 JSON 导入导出、工资 CSV 导入、分析报告与观察清单
- Web 进程负责页面与 API，分析生成由独立 worker / 后台任务链路承接

### journey

- 旅行列表、逐日时间线、天级住宿/摘要/活动编辑
- 行程归档区支持旅程元信息维护、Day 编辑、导入导出
- 分析页展示旅行复盘报告，归档后可触发独立 worker 生成报告

## 目录

- `nav_dashboard/`：统一入口应用
- `ai_conversations_summary/`：RAG 服务
- `library_tracker/`：媒体库服务
- `property/`：资产管理服务
- `journey/`：旅行管理服务
- `core_service/`：共享模块
- `data/`：仓库级共享运行态数据与持久化票据/追踪数据
- `scripts/`：开发脚本、烟测脚本、回归脚本、数据维护脚本
- `.github/`：Agent 工作流与仓库协作规则

## 安装

当前工作区推荐使用仓库根目录统一 `.venv`。

Windows 下可直接运行仓库根目录一键安装脚本：

```bat
setup_workspace.bat
```

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -e .
.\.venv\Scripts\python.exe -m pip install -r .\ai_conversations_summary\requirements.txt
.\.venv\Scripts\python.exe -m pip install -r .\library_tracker\requirements.txt
.\.venv\Scripts\python.exe -m pip install -r .\nav_dashboard\requirements.txt
.\.venv\Scripts\python.exe -m pip install -r .\property\requirements.txt
.\.venv\Scripts\python.exe -m pip install -r .\journey\requirements.txt
```

如果只运行单个子项目，也仍然建议复用同一套 `.venv`，避免 launcher、脚本和共享模块解析路径不一致。

## 启动

### 推荐方式

```powershell
.\nav_dashboard\launch_web.bat
```

说明：各应用的 `launch_web.py` 都会做这几件事：

- 自动读取仓库根目录 `env.local.ps1`
- 优先切换到根 `.venv\Scripts\python.exe`
- 端口被占用时尝试清理旧监听进程
- 服务就绪后自动打开浏览器

### 单独启动

```powershell
.\ai_conversations_summary\launch_web.bat
.\library_tracker\launch_web.bat
.\nav_dashboard\launch_web.bat
.\property\launch_web.bat
.\journey\launch_web.bat
```

### 默认地址

- Dashboard: `http://127.0.0.1:8092/`
- AI Conversations Summary: `http://127.0.0.1:8000/`
- Library Tracker: `http://127.0.0.1:8091/`
- Property: `http://127.0.0.1:8093/`
- Journey: `http://127.0.0.1:8094/`

### 局域网启动

```text
nav_dashboard/deploy_lan_web.bat
```

该脚本会以 `0.0.0.0` 启动 5 个 Web 应用，并将 Dashboard 作为主入口。

## 配置

推荐把本地环境变量统一写在仓库根目录 `env.local.ps1`。

常见变量：

- LLM 与模型：
    - `AI_SUMMARY_LOCAL_LLM_URL`
    - `AI_SUMMARY_LOCAL_LLM_MODEL`
    - `NAV_DASHBOARD_LOCAL_LLM_URL`
    - `NAV_DASHBOARD_LOCAL_LLM_MODEL`
- Embedding / reranker：
    - `LOCAL_EMBEDDING_MODEL`
    - `DEEPSEEK_EMBEDDING_MODEL`
    - `AI_SUMMARY_RERANKER_MODEL`
- Dashboard 服务地址：
    - `NAV_DASHBOARD_AI_SUMMARY_URL`
    - `NAV_DASHBOARD_LIBRARY_TRACKER_URL`
    - `PROPERTY_WEB_PORT`
    - `JOURNEY_WEB_PORT`
- 联网扩展：
    - `TAVILY_API_KEY`
    - `NAV_DASHBOARD_TMDB_API_KEY`
    - `NAV_DASHBOARD_TMDB_READ_ACCESS_TOKEN`

如果默认 embedding 模型没有生效，优先检查 `LOCAL_EMBEDDING_MODEL` 和 `DEEPSEEK_EMBEDDING_MODEL` 是否仍指向旧值。

## 运行态数据

- `data/nav_dashboard/trace_records/trace_records_YYYY_MM.jsonl`：按月滚动的 Trace
- `data/nav_dashboard/tickets/tickets.jsonl`：append-only Ticket 事件流
- `data/ai_conversations_summary/sessions/rag/`：RAG 会话与调试数据
- `data/core_service/`：共享运行态数据根目录

默认正式路径只有仓库级 `data/`。`nav_dashboard/data/`、`library_tracker/data/`、`property/data/`、`journey/data/`、`core_service/data/` 只作为一次性 repair / 冻结迁移的旧位置记录，不再作为常规运行模式的一部分。

冻结后的 legacy repair 痕迹统一集中在 `data/core_service/legacy_frozen/`，不会被运行时自动作为活数据根重新纳入搜索路径。

### 数据落盘 Contract

- 业务主数据：统一放在 `data/library_tracker/`、`data/property/`、`data/journey/` 下，`ai_conversations_summary` 的长期保留主数据为 `data/ai_conversations_summary/documents/`。
- 共享运行态元数据：统一放在 `data/core_service/`，包括跨应用运行态文件与备份快照 `data/core_service/backup_snapshots/`。
- 缓存与索引：如向量库、封面缓存、HF cache、RAG cache，允许重建，默认不进入主数据备份。`ai_conversations_summary` 统一使用 `data/ai_conversations_summary/` 下的 `vector_db/`、`cache/`、`hf_cache/`。
- 日志与可观测性：Trace、Ticket、反馈等继续按应用运行态落盘，建议统一放进 app 内 `observability/`、`tickets/` 这类显式分层，按需单独保留，不与主数据备份绑定。
- 分析产物：报表、分析快照和衍生结果默认视为可重建产物，不进入主数据备份。
- 临时文件：抽取中间件、调试输出、临时目录应保持可清理，不应承担长期存储职责；`ai_conversations_summary` 统一收敛到 `processing/`，跨应用烟测落到 `data/_smoke_runs/`。
- 根层残留项：`data/lan_auth.sqlite3` 仅作为 legacy residual 保留用于显式核对/迁移，当前正式运行态数据库路径是 `data/core_service/lan_auth.sqlite3`；`data/_smoke_runs/` 则保留为跨应用 smoke / 运维演练产物目录，不归属任何单 app 主数据 contract。

### 主数据导出 / 备份 / 恢复

- Dashboard 已在 Authorization 面板下提供统一卡片，覆盖 `library_tracker`、`property`、`journey`，以及 `ai_conversations_summary/documents` 这四类主数据。
- `导出数据包`：生成一份 zip 包，不写入仓库运行态目录；包内按 app/source 分开存放 `apps/<app>/main_data.json`、`backup_manifest.json`、`storage_contract.json`。
- `创建备份`：将同一份结构化 zip 快照落盘到 `data/core_service/backup_snapshots/`，并在 Dashboard 显示最近备份时间。
- `恢复备份`：从导出的 zip 或历史快照恢复当前勾选应用的主数据；默认覆盖对应应用主数据，并在恢复前自动再做一份 safety snapshot。
- `校验/演练恢复`：命令行支持只做合同与载荷校验，或基于当前主数据做 rehearsal，对比“备份内摘要”和“当前环境摘要”，但不落盘变更。

也可以使用命令行脚本：

```powershell
.\.venv\Scripts\python.exe scripts\data_maintenance\personal_data_backup.py summary
.\.venv\Scripts\python.exe scripts\data_maintenance\personal_data_backup.py export --apps library_tracker,property,ai_conversations_summary --file personal_data_export.zip
.\.venv\Scripts\python.exe scripts\data_maintenance\personal_data_backup.py backup
.\.venv\Scripts\python.exe scripts\data_maintenance\personal_data_backup.py validate --file personal_data_export.zip
.\.venv\Scripts\python.exe scripts\data_maintenance\personal_data_backup.py rehearse-restore --file personal_data_export.zip --replace-existing
.\.venv\Scripts\python.exe scripts\data_maintenance\personal_data_backup.py restore --file data\core_service\backup_snapshots\personal_data_backup_YYYYMMDD_HHMMSS.zip
```

### 一次性 Legacy Repair / 冻结

`freeze_legacy_data_roots.py` 只用于一次性迁移后的核对、冻结和应急修复，不应作为日常运行命令：

```powershell
.\.venv\Scripts\python.exe scripts\data_maintenance\freeze_legacy_data_roots.py status
.\.venv\Scripts\python.exe scripts\data_maintenance\freeze_legacy_data_roots.py apply
```

- `status`：查看旧目录是否还有未冻结内容。
- `apply`：把剩余旧内容冻结成 repair 痕迹，避免再次被误认为正式数据。
- 常规开发、启动和脚本调试都应直接面向仓库级 `data/`。

### Benchmark 样本池

- Benchmark 面板中的 `3 / 5 / 10 / 20` 现在表示本次总 Query 条数，而不是“每类 Query 条数”。
- 样本池会按 short / medium / long 轮转抽取，确保不同样本池在同一选择下运行规模一致。
- `session_contamination_v1` 已扩充到 20 条真实/半真实污染回放样本，和其他样本池保持同一上限口径。

## 常用协作约定

- 文档入口与 Agent 工作流见 `.github/README.md`
- 脚本分层说明见 `scripts/README.md`
- 子项目各自的架构、能力和启动说明见对应 README
- 运行态数据通常不应与源码改动混在同一次提交中
