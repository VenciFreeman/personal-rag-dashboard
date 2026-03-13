# Installer Build Guide (Windows)

本文档说明如何在 `release/` 工作区构建 Windows 安装包（Setup.exe）。

## 1. 安装器架构

安装包由以下部分组成：

- `release/app`：应用代码与默认资源。
- Python Runtime：由项目使用的 Python/venv 提供。
- 运行依赖：来自 `.venv` 的站点包。
- Inno Setup 脚本：`installer/AI-Summary-GUI.iss`。

安装目标目录为：`%APPDATA%\\AI-Conversations-Summary`（当前默认策略）。

## 2. 前置条件

1. 已准备仓库根目录 `.venv`。
2. `.venv` 内依赖安装完整。
3. 已安装 Inno Setup 6（可使用 `ISCC.exe`）。

## 3. 构建命令

在 `release/` 目录运行：

```bat
build_installer.bat
```

输出：

- `release/dist_installer/AI-Conversations-Summary-Setup.exe`

## 4. 可选资源

若需预置本地 embedding 模型，可放入：

- `release/app/data/local_models/`

说明：模型文件会显著增大安装包体积，建议按分发场景选择是否内置。
