# AI Conversations Summary (Release Snapshot)

本目录是发布快照，不是日常开发目录。

## 1. 目录定位

- 用于打包可分发版本（便携 EXE 或安装包）。
- 内容与主工程隔离，避免发布流程受开发中间文件影响。

## 2. 快照结构

- `scripts/`：运行所需脚本快照。
- `web/`：Web 入口与静态资源快照。
- `data/` 与 `documents/`：应用运行所需基础目录骨架。

## 3. 与发布流程的关系

1. `release/` 层的打包脚本读取本目录内容。
2. 便携版构建输出到 `release/dist/`。
3. 安装包构建输出到 `release/dist_installer/`。

## 4. 快照同步建议

- 主工程中如果更新了启动器、RAG 脚本、默认资源或说明文档，这里也要同步。
- 特别是 `launch_web.py`、`web/` 静态资源与默认 `data/` 目录骨架，容易在版本演进时出现快照滞后。

## 5. 使用建议

- 日常开发与调试请在主工程目录进行。
- 需要打包时，仅同步必要代码与资源到本快照。
- 构建命令与前置条件以 `../README_RELEASE.md` 和 `../README_INSTALLER.md` 为准。
