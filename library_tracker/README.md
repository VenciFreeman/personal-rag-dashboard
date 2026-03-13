# Library Tracker

`library_tracker` 用于管理个人媒体数据（书籍、游戏、音乐、视频等），并提供可检索的 Web 服务接口。

## 1. 模块架构

```text
CSV / 手工新增条目
    -> scripts/csv_extract.py 等提取脚本
    -> data/structured/ 结构化数据
    -> 向量化/标签处理（按配置）
    -> web/main.py 提供浏览与检索 API
```

## 2. 实现细节

### 2.1 数据处理

- 支持按 profile 进行 CSV 批处理（book/game/music/video）。
- 条目可在 Web UI 中新增/编辑，后端执行数据标准化。
- 支持封面图压缩和基础清洗流程（按脚本能力启用）。

### 2.2 检索能力

- 关键词检索与向量检索可组合使用。
- 查询链路支持阈值过滤与结果限制。
- 可与 `nav_dashboard` Agent 工具链联动。

## 3. 关键目录与文件

- `web/main.py`：FastAPI 服务入口
- `scripts/csv_extract.py`：通用 CSV 提取脚本
- `scripts/setup_env.py`：环境初始化逻辑
- `data/structured/`：结构化输出
- `launch_web.bat`：Windows 双击启动脚本

## 4. 安装（Windows）

```powershell
cd library_tracker
setup.bat
```

`setup.bat` 会优先复用仓库根目录 `.venv` 并安装 `requirements.txt`。

## 5. 启动

```text
launch_web.bat
```

或命令行：

```powershell
..\.venv\Scripts\python.exe launch_web.py
```

默认地址：`http://127.0.0.1:8091/`

## 6. 常用命令

```powershell
..\.venv\Scripts\python.exe scripts\csv_extract.py --profile book
..\.venv\Scripts\python.exe scripts\csv_extract.py --profile game
..\.venv\Scripts\python.exe scripts\csv_extract.py --profile music
..\.venv\Scripts\python.exe scripts\csv_extract.py --profile video
```

## 7. 环境变量

- `LIBRARY_WEB_HOST`
- `LIBRARY_WEB_PORT`

## 8. 健康检查

```powershell
Invoke-WebRequest -Uri "http://127.0.0.1:8091/healthz" -UseBasicParsing
```

返回 `{"status":"ok"}` 表示服务可用。
