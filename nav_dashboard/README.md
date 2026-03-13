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
2. 由 LLM 规划工具调用（失败时回退到规则规划）。
3. 并行调用文档检索、媒体检索、联网搜索工具；文档检索默认最多改写 2 条 query，并保留原 query 的更高优先级。
4. 过滤低相关结果并按引用上限截断。
5. 拼装上下文并生成最终回答。
6. 写入会话文件与必要的调试信息。

### 2.3 Dashboard 指标来源

- 文档侧：从 RAG 服务读取文档量与检索统计。
- 媒体侧：从 Library 服务读取条目与图谱统计。
- Agent 侧：基于本地会话和调用计数汇总。

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

## 7. 健康检查

```powershell
Invoke-WebRequest -Uri "http://127.0.0.1:8092/healthz" -UseBasicParsing
```

返回 `{"status":"ok"}` 表示服务可用。
