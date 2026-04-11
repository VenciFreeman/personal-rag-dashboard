# Agent Bootstrap

- Workflow entrypoint: `.github/AGENT_WORKFLOW.md`
- Rule library: `.github/AGENT_RULES.md`
- Always classify the task as `debug`, `feature`, or `refactor`.
- Only apply `GLOBAL RULES` plus the active task section from `.github/AGENT_RULES.md`.
- Hooks in `.github/hooks/agent_workflow.json` enforce task declaration, debug ticket search before coding, architecture guards for hotspot files, and post-check gates after edits.

## Architecture Guard
- For router / policy / benchmark hotspots, do not patch by matching exact repro questions or other full user literals.
- Promote the fix into shared parsing, routing, or policy helpers so nearby phrasings improve together.
- When a hotspot file changes, update the nearest regression tests in the same session.

## Environment
- Use the root `.venv` for Python work in this workspace.
- Main entrypoints are `nav_dashboard/launch_web.bat`, `ai_conversations_summary/launch_web.bat`, and `library_tracker/launch_web.bat`.
- Ticket records are stored in `data/nav_dashboard/tickets/tickets.jsonl` using the schema implemented in `core_service/ticket_store.py`.

## Safety Guardrails
- Never edit files outside the current workspace unless the user explicitly asks for it and names the target path.
- Treat runtime data and local config as sensitive: `data/**`, `env.local.ps1`, `core_service/config.local.json`, and backup snapshots must not be deleted, truncated, or overwritten without explicit user confirmation in the current session.
- Before any restore/import operation with overwrite semantics, prefer validation or rehearsal first; do not run destructive restore flows silently.
- If a restore payload for `nav_dashboard` tickets is empty while the current tickets file is non-empty, stop and surface the mismatch instead of overwriting.

## Bug Ticket Sync
- When your work discovers or fixes one or more concrete bugs, add exactly one `BUG-TICKET:` line per bug in your final response.
- Each `BUG-TICKET:` line must be followed by a single-line compact JSON object. Do not wrap it in a code fence.
- Use these keys when known: `title`, `status`, `priority`, `domain`, `category`, `summary`, `related_traces`, `repro_query`, `expected_behavior`, `actual_behavior`, `root_cause`, `fix_notes`, `additional_notes`.
- Use `status: "resolved"` when the bug was fixed in the current session. Use `status: "open"` when the session only identified the bug.
- Keep `category` stable and snake_case so the stop hook can match recent tickets and update them instead of creating duplicates.
- Do not emit `BUG-TICKET:` lines for non-bug tasks.

## Example Marker
- `BUG-TICKET: {"title":"Sidebar 会话标题被后端硬截断","status":"resolved","priority":"medium","domain":"nav_dashboard_ui","category":"session_title_truncation","summary":"历史会话标题在写入 session 时就被硬截断。","repro_query":"任意较长问题作为新会话首问。","expected_behavior":"标题完整持久化，展示层按宽度省略。","actual_behavior":"前端拿到的标题已经被截短。","root_cause":"agent_service.py 的标题生成链路默认固定 max_len。","fix_notes":"去掉默认硬截断，保留完整标题。","additional_notes":"相关文件: nav_dashboard/web/services/agent_service.py"}`