# AI Conversations Summary

`ai_conversations_summary` 是仓库中的文档整理与 RAG 服务，负责把个人资料沉淀为可检索知识库，并提供 Web 形态的问答接口。

默认地址：`http://127.0.0.1:8000/`

## 角色

- 管理文档目录与向量索引
- 执行 query rewrite、query expansion、向量召回、rerank 和回答生成
- 为 `nav_dashboard` 提供文档检索与引用能力
- 在启动时完成一致性检查、必要修复和模型预热

## 当前检索链路

1. 可选 query rewrite，默认最多 2 条改写 query，且原 query 保留优先权。
2. 每条 query 会先做 query expansion，再进入 FAISS 召回。
3. 合并后使用 cross-encoder rerank。
4. 关键词检索使用轻量 BM25 变体作为稀疏信号。
5. 上下文装配默认采用 `passage_first`，优先抽取真实相关段落。
6. 回答阶段区分本地资料、通用知识和推断。
7. 最终输出会进行 citation reconciliation，清理无效引用并补足高重合未标注段落。

## 启动时行为

服务启动后会异步执行：

- 向量元数据 / FAISS / 图谱一致性检查
- 检测文档变更并尝试自动重向量化
- 图谱缺失补齐
- embedding 模型和 reranker 预热

这些状态可通过 `/api/workflow/startup-status` 被 Dashboard 拉取展示。

## 关键目录

- `web/main.py`：FastAPI 入口
- `web/api/rag.py`：RAG 问答接口
- `web/api/workflow.py`：启动状态与流程接口
- `scripts/`：索引、图谱、整理、维护脚本
- `../data/ai_conversations_summary/documents/`：分类知识文档
- `../data/ai_conversations_summary/vector_db/`：向量索引与知识图谱
- `../data/ai_conversations_summary/sessions/rag/`：问答会话与调试数据
- `../data/ai_conversations_summary/processing/`：抽取、切分、摘要阶段的中间产物
- `../data/ai_conversations_summary/state/`：工作流状态与后台任务 PID
- `../data/ai_conversations_summary/observability/`：审计与调试输出

其中 `documents/` 视为主数据，已纳入统一备份/恢复设计；`vector_db/`、`cache/`、`sessions/`、`processing/`、`observability/` 仍按可重建运行态处理。

## 安装

推荐复用仓库根 `.venv`：

```powershell
..\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

如未创建 `.venv`，先在仓库根目录执行：

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
```

## 启动

```powershell
..\.venv\Scripts\python.exe launch_web.py
```

或直接运行：

```text
launch_web.bat
```

`launch_web.py` 会自动读取根目录 `env.local.ps1`，优先切换到根 `.venv`，清理旧监听并在服务准备好后打开浏览器。

## 常用配置

- 本地 LLM：
  - `AI_SUMMARY_LOCAL_LLM_URL`
  - `AI_SUMMARY_LOCAL_LLM_MODEL`
  - `AI_SUMMARY_LOCAL_LLM_API_KEY`
- 检索相关：
  - `AI_SUMMARY_VECTOR_TOP_N`
  - `AI_SUMMARY_RERANK_TOP_K`
  - `AI_SUMMARY_RERANKER_MODEL`
  - `AI_SUMMARY_ENABLE_QUERY_REWRITE`
  - `AI_SUMMARY_QUERY_REWRITE_COUNT`
  - `AI_SUMMARY_PRIMARY_QUERY_SCORE_BONUS`
  - `AI_SUMMARY_PASSAGE_FIRST_K`
- 联网搜索：
  - `TAVILY_API_KEY`

## 最小工作流

1. 将原始资料整理到 `../data/ai_conversations_summary/processing/raw/` 或 `../data/ai_conversations_summary/documents/`。
2. 运行文档处理与索引脚本，构建向量库和图谱。
3. 启动 Web 服务。
4. 通过本服务页面或 `nav_dashboard` 发起文档问答。

## 说明

- 当前项目已收敛为 Web-only 入口，不再维护本地 GUI / release 打包流作为主要使用方式。
- 本服务的检索策略和 citation contract 会直接影响 Dashboard 的文档回答质量与 no-context 判定。
- 运行态正式路径统一落在 `data/ai_conversations_summary/`；`ai_conversations_summary/data/` 与 `ai_conversations_summary/documents/` 不再作为常规运行目录。
- 当前 contract 推荐按 `documents / vector_db / cache / processing / sessions / state / observability` 理解，避免把中间产物和长期保留数据混放在 app 根下。
