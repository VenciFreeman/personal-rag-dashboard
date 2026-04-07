# Agent Workflow

本文件是仓库内 Agent 工作流入口。

## 必做步骤

1. 先给任务分类：
	- `debug`
	- `feature`
	- `refactor`
2. 再读取 `.github/AGENT_RULES.md`：
	- 总是应用 `GLOBAL RULES`
	- 只加载与当前任务类型匹配的那一节
3. 完成改动后执行 post-check：
	- lint
	- typecheck
	- smoke test
4. 如果改动命中路由/策略/benchmark 热点文件：
	- 不能用具体问句字面量硬编码修 bug
	- 必须把规则提升到共享 helper / 谓词层
	- 必须同步补最近的回归测试文件
5. 收尾时更新必要文档；如果本次识别或修复了明确 bug，按仓库约定输出 `BUG-TICKET:` 标记

## 当前 Hook 约束

- Hook 会注入 `TASK_TYPE=<debug|feature|refactor>`
- `debug` 任务在编辑前必须先搜索类似 Ticket
- 编辑后会触发 post-check gate，失败时不应直接结束会话
- 热点文件会额外经过 architecture guard：拦截问句字面量补丁，并要求 companion regression tests

## 相关文件

- `.github/AGENT_RULES.md`：规则正文
- `.github/hooks/agent_workflow.json`：工作流 Hook 配置
- `scripts/dev/agent_workflow_hook.py`：工作流 Hook 实现
- `scripts/dev/bug_ticket_sync_hook.py`：Bug Ticket 同步 Hook

## 使用建议

- 把 `.github/README.md` 当作入口，不要在这里堆详细规则
- 详细规则与例外集中维护在 `.github/AGENT_RULES.md`
- 当行为、架构或脚本入口发生变化时，及时同步更新对应 README