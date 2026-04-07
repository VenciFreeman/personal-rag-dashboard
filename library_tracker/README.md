# Library Tracker

`library_tracker` 用于管理个人媒体库，并向 Dashboard Agent 提供结构化搜索与条目详情能力。

默认地址：`http://127.0.0.1:8091/`

## 角色

- 管理阅读、音乐、视频、游戏等个人条目
- 提供本地媒体库的检索、详情、CRUD 和统计 API
- 维护 embedding、别名、图谱和后台分析任务
- 为 `nav_dashboard` 的媒体类问答提供结构化数据源

## 当前能力

- 关键词检索、向量检索、结构化过滤检索可组合使用
- 条目可在 Web UI 中新增、编辑、删除
- 详情浮窗支持桌面端方向键切换，移动端支持低敏左右滑切换前后条目
- 媒体字段较完整，包含作者、分类、出版方、渠道、评分、短评、日期等
- 启动后会延迟检查未完成的 embedding/图谱刷新任务
- Web 进程负责查询与 CRUD；季度/年度分析、embedding / alias / 图谱刷新通过后台任务或独立 worker 承接
- 别名与 embedding 刷新队列可由外部任务中心或 API 驱动

## 关键目录

- `web/main.py`：FastAPI 应用入口
- `web/api/library.py`：搜索、CRUD、embedding、图谱、分析接口
- `web/services/`：查询、变更、embedding、别名生命周期、分析服务等模块
- `scripts/`：导入、抽取、批处理脚本
- `../data/library_tracker/structured/`：结构化媒体主数据（canonical）
- `../data/library_tracker/analysis/`：分析相关运行态数据（canonical）

默认正式主数据路径是仓库级 `data/library_tracker/`。`library_tracker/data/` 仅保留为一次性 repair / 冻结迁移时的旧位置提示，不再作为常规运行路径。

## 与 Dashboard 的关系

- Dashboard Agent 会直接调用 `/api/library/search`
- 媒体结构化回答依赖这里的字段质量和召回质量
- 个人评价类回答会直接展示本地评分和短评，因此：
    - `rating`
    - `review`
    - `author`
    - `publisher`
    - `channel`
    等字段都属于高价值数据

## 安装

推荐复用仓库根 `.venv`：

```powershell
..\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

也可以运行：

```text
setup.bat
```

## 启动

```powershell
..\.venv\Scripts\python.exe launch_web.py
```

或直接运行：

```text
launch_web.bat
```

`launch_web.py` 会自动读取根目录 `env.local.ps1`，优先重启到根 `.venv`，清理占用端口的旧进程，并在服务可用后打开浏览器。

如需跑分析或后台任务，使用同一套根 `.venv` 启动对应 worker / 调度脚本，避免与 Web 进程出现依赖漂移。

## 常用命令

```powershell
..\.venv\Scripts\python.exe scripts\importers\csv_extract.py --profile book
..\.venv\Scripts\python.exe scripts\importers\csv_extract.py --profile game
..\.venv\Scripts\python.exe scripts\importers\csv_extract.py --profile music
..\.venv\Scripts\python.exe scripts\importers\csv_extract.py --profile video
```

## 常用环境变量

- `LIBRARY_WEB_HOST`
- `LIBRARY_WEB_PORT`

## 健康检查

```powershell
Invoke-WebRequest -Uri "http://127.0.0.1:8091/healthz" -UseBasicParsing
```

返回 `{"status":"ok"}` 表示服务可用。
