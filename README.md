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

1. 对用户问题做意图分析与配额检查。
2. 由 LLM 或回退规则规划工具调用（文档检索、媒体检索、联网检索）。
3. 并行执行工具并进行阈值过滤。
4. 汇总上下文后生成最终回答，并保存会话。

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

## 3. 目录导读

- `ai_conversations_summary/`：文档加工与 RAG 服务
- `library_tracker/`：媒体库服务
- `nav_dashboard/`：统一 Web 门户与 Agent
- `core_service/`：跨项目共享配置与模型调用
- `data/`：仓库级运行数据（如 benchmark 结果）

## 4. 安装与启动（Windows）

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

## 5. 配置约定

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
- 若出现默认模型未生效，优先检查以上环境变量是否指向旧模型。

## 6. 常见协作建议

- 运行时数据（会话、缓存、向量库）建议与源码提交分离。
- 优先通过子项目 README 查看模块内的实现细节和接口说明。
- 需要发布安装包时，使用 `ai_conversations_summary/release/` 下的流程文档。
