# Release Workspace

`release/` 是 AI Conversations Summary 的发布工作区，用于构建可分发版本（便携包与安装包）。

## 1. 目录职责

- `app/`：用于打包的应用快照（与开发目录隔离）。
- `installer/`：安装包脚本与安装器配置。
- `build_release_exe.bat`：构建便携版 EXE。
- `build_installer.bat`：构建 Setup 安装包。

## 2. 构建产物

- 便携版：`release/dist/AI-Summary-GUI/AI-Summary-GUI.exe`
- 安装包：`release/dist_installer/AI-Conversations-Summary-Setup.exe`

## 3. 构建依赖

- 项目虚拟环境（通常为仓库根目录 `.venv`）。
- 已安装运行依赖（GUI、RAG、向量检索相关）。
- Windows 打包工具链（安装包流程需要 Inno Setup）。
- 发布前建议先从主工程同步最新脚本、Web 资源、默认数据骨架与 README，确保快照与当前实现一致。

## 4. 构建步骤

在 `release/` 目录执行：

```bat
build_release_exe.bat
```

需要安装器时执行：

```bat
build_installer.bat
```

## 5. 相关文档

- 安装器细节：`README_INSTALLER.md`
- 发布应用说明：`app/README.md`

## 6. 当前发布注意点

- 根目录 `.venv` 是推荐且默认的构建环境。
- 如果主工程中更新了 RAG 流程、启动器行为或默认配置，请同步检查 `release/app/` 快照是否也已更新。
