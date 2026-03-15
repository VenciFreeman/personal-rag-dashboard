# Nav Dashboard

`nav_dashboard` 是整个工作台的统一入口，负责聚合多个后端服务并提供跨域 Agent 问答、系统概览与基准测试。

## 1. 服务定位

- 对外入口：`http://127.0.0.1:8092/`
- 聚合目标：
  - AI Conversations Summary（文档 RAG）
  - Library Tracker（媒体库检索）
- 扩展能力：可选 Tavily 联网检索

## 2. 架构与实现

### 2.1 组件划分

- `web/main.py`：FastAPI 应用入口与页面路由
- `web/api/agent.py`：Agent 对话接口
- `web/api/benchmark.py`：基准测试与 SSE 进度流
- `web/services/agent_service.py`：工具规划、并行执行、上下文组装、会话落盘
- `data/`：会话、配额、benchmark 结果等运行态数据

### 2.2 Agent 链路

1. 接收问题并做请求级限流与配额校验。
2. 先做混合路由判断：LLM 分类、embedding 相似度、媒体实体识别、follow-up 上下文继承共同参与。
3. 再规划工具调用（失败时回退到规则规划）。
4. 并行调用文档检索、媒体检索、联网搜索、概念扩展等工具；文档检索默认最多改写 2 条 query，并保留原 query 的更高优先级。
5. 过滤低相关结果并按引用上限截断。
6. 对纯本地媒体 filter/evaluation 查询优先走 deterministic structured answer，直接展开评分、短评和细节字段。
7. 拼装上下文并生成最终回答。
8. 写入会话、trace、指标与调试信息。

### 2.3 路由模型现状

- 当前实现是 `LLM + 算法/规则` 的混合路由，不是纯算法。
- 主要信号包括：LLM classifier、doc embedding similarity、media entity confidence、query intent cues、follow-up 继承状态。
- 当前还不是自动在线学习系统；优化主要通过 trace、feedback、tickets、evals 做离线迭代。
- 这套结构的目的是让路由策略可以持续演进，同时保留可解释性和可回放能力。

### 2.4 Dashboard 指标来源

- 文档侧：从 RAG 服务读取文档量与检索统计。
- 媒体侧：从 Library 服务读取条目与图谱统计。
- Agent 侧：基于本地会话和调用计数汇总。
- Tickets 侧：从 append-only 事件日志聚合列表、详情和按周提交/关闭趋势。
- 观测侧：Trace 支持按月滚动存储，并可在 Dashboard 中查询、导出、转 Ticket。

## 3. 安装（Windows）

推荐使用仓库根目录统一 `.venv`：

```powershell
cd nav_dashboard
..\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

如果需要通过本项目一键拉起三站，请确保同时安装：

```powershell
..\.venv\Scripts\python.exe -m pip install -r ..\ai_conversations_summary\requirements.txt
..\.venv\Scripts\python.exe -m pip install -r ..\library_tracker\requirements.txt
```

## 4. 启动

### 4.1 本地单服务启动

```powershell
..\.venv\Scripts\python.exe launch_web.py
```

或双击：

```text
launch_web.bat
```

说明：launcher 会在导入配置前自动读取仓库根目录的 `env.local.ps1`。

### 4.2 局域网一键部署（三站）

```text
deploy_lan_web.bat
```

## 5. 常用环境变量

- `NAV_DASHBOARD_WEB_HOST`
- `NAV_DASHBOARD_WEB_PORT`
- `NAV_DASHBOARD_AI_SUMMARY_URL`
- `NAV_DASHBOARD_LIBRARY_TRACKER_URL`
- `NAV_DASHBOARD_LOCAL_LLM_URL`
- `NAV_DASHBOARD_LOCAL_LLM_MODEL`
- `NAV_DASHBOARD_QUERY_REWRITE_COUNT`
- `NAV_DASHBOARD_PRIMARY_QUERY_SCORE_BONUS`
- `TAVILY_API_KEY`

## 6. 运行数据说明

以下目录/文件属于运行态数据，通常不建议作为源码变更提交：

- `data/agent_sessions/`
- `data/agent_quota.json`
- `data/agent_quota_history.json`
- `data/benchmark_results.json`
- `data/trace_records.json` 与 `data/trace_records_YYYY_MM.jsonl`
- `data/tickets.jsonl`
- `data/chat_feedback.json`

## 7. 当前 Dashboard 能力

- Dashboard tab：系统总览卡片、检索/观测表、Trace 查询、任务中心、Ticket 周趋势图。
- Tickets tab：筛选、创建、AI draft、更新、软删除、trace 关联跳转。
- 聊天反馈：支持导出、清空、长按查看原始问题与 trace 详情，使用站内浮窗而不是浏览器原生提示。
- Agent 会话标题：后端持久化完整标题，前端按侧边栏宽度自然单行省略。

## 8. 健康检查

```powershell
Invoke-WebRequest -Uri "http://127.0.0.1:8092/healthz" -UseBasicParsing
```

返回 `{"status":"ok"}` 表示服务可用。
