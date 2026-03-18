# AI Conversations Summary

`ai_conversations_summary` 负责将原始对话内容沉淀为可检索知识库，并提供 RAG 问答能力（GUI 与 Web 两种入口）。

## 1. 模块架构

```text
Raw Inputs
  -> data/raw_dir
  -> scripts/for_deepseek.py 等预处理
  -> data/extracted_dir
  -> scripts/summarize.py 生成结构化总结
  -> data/summarize_dir
  -> scripts/move_summaries_by_category.py 分类归档
  -> documents/<category>/
  -> scripts/build_rag_index.py 建索引
  -> core_service/data/vector_db
  -> web/main.py / scripts/ask_rag.py 对外问答
```

## 2. RAG 实现细节

1. Query Rewrite（可选，默认开启）：最多生成 2 条检索 query，并保留原 query 的更高优先级。
2. Query Expansion Graph：每条 rewrite query 会先做图谱扩展，再进入向量召回；当前图谱定位是 query expansion，而不是 GraphRAG 式证据推理。
3. 多路向量召回：每条 query 在 FAISS 中独立召回。
4. Merge + Rerank：合并去重后进行交叉编码精排，默认使用 `BAAI/bge-reranker-base`，并带有 top1 guard / dynamic fusion alpha。
5. 稀疏检索：keyword search 已从 substring scan 升级为轻量 BM25（ASCII token + CJK bigram）。
6. 上下文装配默认使用 `passage_first`：
   - top 文档优先抽取真实相关段落
   - 其余文档保留 `title / summary / topic / keywords` 元数据
7. Passage 选择采用两段式：
   - 先按 query token overlap 预筛段落
   - 再复用已热加载的 reranker 做 passage-level semantic selection
8. 回答生成：本地或云端 LLM 基于上下文生成答案，prompt 中区分：
   - 本地资料事实
   - 通用知识补充
   - 推断/外推
9. 回答后处理包含 citation reconciliation：
   - 清理不存在的 `[资料N]`
   - 自动补标强匹配但未标注的段落
   - 生成 `citation_map`
10. `no_context` 由统一函数判定，综合 `threshold` 与 `retrieval_confidence`，并同步进入 trace / metrics / prompt framing。
11. 会话落盘：写入 `data/rag_sessions/`，并记录检索指标。

补充说明：当前 RAG 服务已经与 `nav_dashboard` 共用部分共享能力，并通过 Dashboard 汇总检索时延、缓存命中率、未命中率、rerank 换榜率等观测指标。

## 3. 关键目录与文件

- `web/main.py`：FastAPI Web 服务入口
- `scripts/ask_rag.py`：CLI 问答入口
- `scripts/build_rag_index.py`：向量索引构建与更新
- `scripts/cache_db.py`：embedding / web cache
- `scripts/rag_knowledge_graph.py`：查询扩展用知识图谱
- `scripts/rag_vector_index.py`：本项目 RAG 索引实现
- `data/rag_sessions/`：会话与调试数据
- `documents/`：分类知识文档库

## 4. 安装（Windows）

在仓库根目录创建并复用 `.venv`（推荐）：

```powershell
python -m venv ..\.venv
..\.venv\Scripts\python.exe -m pip install --upgrade pip
..\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

如已在根目录安装过依赖，可跳过。

## 5. 启动

### 5.1 Web 模式

```powershell
..\.venv\Scripts\python.exe launch_web.py
```

或双击：

```text
launch_web.bat
```

默认地址：`http://127.0.0.1:8000/`

说明：launcher 会在导入配置前自动读取仓库根目录的 `env.local.ps1`。

### 5.2 GUI 模式

```powershell
..\.venv\Scripts\pythonw.exe launch_gui.py
```

## 6. 常用配置

- 本地 LLM：
  - `AI_SUMMARY_LOCAL_LLM_URL`
  - `AI_SUMMARY_LOCAL_LLM_MODEL`
  - `AI_SUMMARY_LOCAL_LLM_API_KEY`
- 检索参数：
  - `AI_SUMMARY_VECTOR_TOP_N`
  - `AI_SUMMARY_RERANK_TOP_K`
  - `AI_SUMMARY_RERANKER_MODEL`
  - `AI_SUMMARY_ENABLE_QUERY_REWRITE`
  - `AI_SUMMARY_QUERY_REWRITE_COUNT`
  - `AI_SUMMARY_PRIMARY_QUERY_SCORE_BONUS`
  - `AI_SUMMARY_PASSAGE_FIRST_K`
- 联网检索：`TAVILY_API_KEY`

默认值：

- `AI_SUMMARY_RERANKER_MODEL=BAAI/bge-reranker-base`
- `AI_SUMMARY_QUERY_REWRITE_COUNT=2`
- 如需回切，可在 `env.local.ps1` 中将 `AI_SUMMARY_RERANKER_MODEL` 改回 `BAAI/bge-reranker-v2-m3`

## 7. 最小使用流程

1. 导入原始对话文件到 `data/raw_dir/`。
2. 运行预处理与总结脚本，生成结构化 Markdown。
3. 归档到 `documents/` 后执行索引构建。
4. 在 Web 或 GUI 中发起 RAG 问答。

## 8. 与 Dashboard/Agent 的关系

- `nav_dashboard` 的文档工具会调用本服务的检索能力。
- Agent 的 mixed/tech 路由会参考文档 embedding similarity 决定是否加入文档检索。
- 因此这里的 query rewrite、BM25/向量融合、passage 选择、retrieval confidence 与 citation contract，会直接影响 Dashboard Agent 的文档回答质量与 no-context 判定。

## 9. 当前实现边界

- 当前图谱主要用于 query expansion，不负责证据路径推理。
- 当前 citation contract 已经做到 paragraph-level reconciliation，但还不是 claim-level hard binding。
- 当前实现已经具备较完整的个人项目 RAG 骨架；继续优化时，优先级应放在：
  1. claim-level citation binding
  2. retrieval confidence 校准
  3. regression / eval 样本沉淀

## 10. 说明

本 README 聚焦架构与落地流程；更细的调参、故障场景和发布细节请查看：

- `release/README_RELEASE.md`
- `release/README_INSTALLER.md`
- `scripts/README_SPLIT.md`
