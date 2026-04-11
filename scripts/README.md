# Scripts

仓库根 `scripts/` 只保留稳定的脚本分层入口，不再堆放临时 patch 文件。

## 目录分层

- `scripts/dev/`：开发辅助脚本、工作流 Hook、一次性迁移或调试脚本
- `scripts/smoke/`：前端解析检查、UI smoke、Playwright 探针及其产物
- `scripts/regression/`：独立回归脚本，通常用于在测试框架外快速复现或批量校验
- `scripts/data_maintenance/`：数据修复、报告回填、ontology proposal/review、票据入队等维护脚本

## 当前常见脚本

- `scripts/install_workspace.bat`：从 `scripts/` 目录进入时调用仓库根 `setup_workspace.bat`
- `scripts/dev/agent_workflow_hook.py`：仓库内 Agent 工作流 Hook 实现
- `scripts/dev/bug_ticket_sync_hook.py`：`BUG-TICKET` 标记同步到 Ticket 存储的 Hook
- `scripts/smoke/check_app_parse.py`：对 `nav_dashboard/web/static/app.js` 做解析级 smoke
- `scripts/smoke/ui_smoke_playwright.py`：Dashboard 主路径 UI smoke
- `scripts/regression/regression_router.py`：路由回归集
- `scripts/data_maintenance/generate_missing_library_reports.py`：缺失媒体分析报告回填
- `scripts/data_maintenance/ontology_propose.py`：ontology 候选生成
- `scripts/data_maintenance/ontology_review.py`：ontology 提案审核与合并
- `scripts/data_maintenance/personal_data_backup.py`：主数据导出 / 备份 / 恢复（支持 zip）
- `scripts/data_maintenance/freeze_legacy_data_roots.py`：将 legacy 工作区数据集中移入 `data/core_service/legacy_frozen/`，并支持恢复
- `scripts/data_maintenance/queue_bug_ticket.py`：向本地 Ticket outbox 追加 bug payload

## 约定

- 不要再向 `scripts/` 根目录新增临时 patch 文件
- 需要长期保留的稳定脚本，应当落到明确的子目录并在所属 README 中说明用途
- 运行脚本时默认以仓库根目录为工作目录，优先使用根 `.venv`