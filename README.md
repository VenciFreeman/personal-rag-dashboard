# personal-ai-stack

本仓库是一个本地优先的个人 AI 工作台，围绕统一入口、个人知识库、媒体库、资产管理和旅行归档，组织成 5 个 Web 应用加 1 组共享核心模块。

## 仓库定位

- `nav_dashboard`：统一入口，负责跨服务 Agent、Trace、Ticket、Benchmark、任务中心和运维面板
- `ai_conversations_summary`：文档沉淀、向量检索、RAG 问答与索引维护
- `library_tracker`：个人媒体库管理、结构化检索、图谱和 embedding 任务
- `property`：资产快照、收支流水、观察清单和分析报告
- `journey`：旅程归档、逐日编辑、复盘生成
- `core_service`：共享配置、认证、LLM 客户端、运行态数据、Trace/Ticket 基础设施

## 系统结构

```text
Browser
  |
  +-- nav_dashboard (8092)
  |     +-- ai_conversations_summary (8000)
  |     +-- library_tracker (8091)
  |     +-- property (8093)
  |     +-- journey (8094)
  |
  +-- Shared Core
        +-- core_service/config.py
        +-- core_service/llm_client.py
        +-- core_service/runtime_data.py
        +-- core_service/trace_store.py
        +-- core_service/ticket_store.py
```

`nav_dashboard` 是默认主入口。其余应用既可以独立启动，也可以由 Dashboard 聚合访问。

## 主要能力

### Dashboard

- 统一聊天入口，按问题类型在本地资料检索、结构化查询和联网搜索之间路由
- 记录完整 Agent Trace，包括路由决策、工具计划、护栏与结果分层
- 提供 Ticket、反馈、Benchmark、任务中心、运行状态与备份入口

### AI Conversations Summary

- 将对话整理为 Markdown 文档并建立向量索引
- 支持 query rewrite、query expansion、向量召回、轻量 BM25、rerank
- 启动时会执行索引一致性检查、必要修复与模型预热

### Library Tracker

- 管理阅读、音乐、视频、游戏等个人媒体条目
- 支持结构化检索、关键词检索、向量检索、图谱辅助和分析任务

### Property

- 管理资产快照、工资/收支记录、观察清单和分析报告
- 支持工资 CSV 导入与图表化分析

### Journey

- 管理旅程元信息、逐日行程、归档和复盘报告
- 支持导入导出与独立 worker 生成分析内容

## 目录导览

- `nav_dashboard/`：统一入口应用
- `ai_conversations_summary/`：RAG 与文档服务
- `library_tracker/`：媒体库服务
- `property/`：资产管理服务
- `journey/`：旅行管理服务
- `core_service/`：共享基础模块
- `data/`：仓库级运行态数据、主数据和报表产物
- `scripts/`：开发、回归、烟测、数据维护脚本
- `.github/`：Agent workflow、协作规则和 hook 配置

## 快速开始

推荐统一使用仓库根目录的 `.venv`。

### 一键安装

```bat
setup_workspace.bat
```

或：

```bat
scripts\install_workspace.bat
```

### 手动安装

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

如果只跑单个子项目，也建议继续复用这套 `.venv`，避免 launcher、脚本和共享模块解析到不同 Python 环境。

## 启动方式

### 推荐入口

```powershell
.\nav_dashboard\launch_web.bat
```

各应用的 `launch_web.py` 会统一处理以下事情：

- 自动读取仓库根目录 `env.local.ps1`
- 优先切换到根 `.venv\Scripts\python.exe`
- 端口被占用时尝试清理旧监听进程
- 服务就绪后自动打开浏览器

### 独立启动

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

### 局域网部署

```bat
nav_dashboard\deploy_lan_web.bat
```

该脚本会以 `0.0.0.0` 启动各 Web 应用，并将 Dashboard 作为主入口。

## 配置约定

推荐把本地变量统一写在仓库根目录 `env.local.ps1`。

常见配置项包括：

- 本地 LLM：`AI_SUMMARY_LOCAL_LLM_URL`、`AI_SUMMARY_LOCAL_LLM_MODEL`、`NAV_DASHBOARD_LOCAL_LLM_URL`、`NAV_DASHBOARD_LOCAL_LLM_MODEL`
- Embedding / reranker：`LOCAL_EMBEDDING_MODEL`、`DEEPSEEK_EMBEDDING_MODEL`、`AI_SUMMARY_RERANKER_MODEL`
- 服务地址：`NAV_DASHBOARD_AI_SUMMARY_URL`、`NAV_DASHBOARD_LIBRARY_TRACKER_URL`、`PROPERTY_WEB_PORT`、`JOURNEY_WEB_PORT`
- 外部接口：`TAVILY_API_KEY`、`NAV_DASHBOARD_TMDB_API_KEY`、`NAV_DASHBOARD_TMDB_READ_ACCESS_TOKEN`

如果 embedding 模型未按预期生效，优先检查 `LOCAL_EMBEDDING_MODEL` 和 `DEEPSEEK_EMBEDDING_MODEL` 是否仍指向旧值。

## 数据与持久化

运行态正式路径统一收敛到仓库级 `data/`，旧的 app 内部 `data/` 目录只保留为迁移/冻结痕迹，不再是常规运行路径。

关键目录：

- `data/nav_dashboard/trace_records/`：按月滚动的 Trace 记录
- `data/nav_dashboard/tickets/tickets.jsonl`：append-only Ticket 事件流
- `data/ai_conversations_summary/documents/`：长期保留文档主数据
- `data/core_service/`：共享运行态元数据、认证库、备份快照
- `data/project_reports/`：跨项目评估与阶段性报告

### 数据 Contract

- 主数据放在 `data/library_tracker/`、`data/property/`、`data/journey/` 和 `data/ai_conversations_summary/documents/`
- 托管用户产物如封面、分析报告和项目报告随主数据备份导出
- 向量库、缓存、新闻抓取结果等默认视为可重建产物，不进入主数据备份
- 跨应用 smoke / 运维演练结果收敛到 `data/_smoke_runs/`

### 导出 / 备份 / 恢复

Dashboard 的 Authorization 面板已经集成统一的数据导出、备份、恢复与配置维护入口。

命令行脚本：

```powershell
.\.venv\Scripts\python.exe scripts\data_maintenance\personal_data_backup.py summary
.\.venv\Scripts\python.exe scripts\data_maintenance\personal_data_backup.py export --apps library_tracker,property,ai_conversations_summary --file personal_data_export.zip
.\.venv\Scripts\python.exe scripts\data_maintenance\personal_data_backup.py backup
.\.venv\Scripts\python.exe scripts\data_maintenance\personal_data_backup.py validate --file personal_data_export.zip
.\.venv\Scripts\python.exe scripts\data_maintenance\personal_data_backup.py rehearse-restore --file personal_data_export.zip --replace-existing
.\.venv\Scripts\python.exe scripts\data_maintenance\personal_data_backup.py restore --file data\core_service\backup_snapshots\personal_data_backup_YYYYMMDD_HHMMSS.zip
```

`freeze_legacy_data_roots.py` 仅用于一次性迁移后的核对和冻结，不应作为日常操作命令。

## 开发与协作

- 仓库主说明入口是当前 `README.md`
- Agent workflow 入口见 `.github/AGENT_WORKFLOW.md`
- 规则库见 `.github/AGENT_RULES.md`
- 脚本分层说明见 `scripts/README.md`
- 子项目的专有架构、接口和启动细节见各自目录下的 README
- 运行态数据通常不应与源码修改混在同一次提交中
