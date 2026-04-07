# GLOBAL RULES (always apply)

- Diff-based changes only.
- No large rewrites unless the task explicitly requires them.
- Fix root causes before adding surface workarounds.
- Follow project lint and formatting tools strictly.
- Preserve architecture boundaries between apps and shared `core_service` modules.
- Validate the smallest useful scope that proves the change.
- When touching router, policy, or benchmark hotspots, prefer shared predicates/helpers over adding another query-specific branch.
- Do not hardcode full user repro questions or string-match exact bug phrases in production logic.
- Hotspot logic changes must ship with a companion regression test in the closest existing test suite.

---

# DEBUG RULES

- MUST search similar tickets before fixing.
- Prefer the smallest fix that removes the regression.
- Keep unrelated behavior unchanged.
- MUST add or update a regression test when the bug is reproducible in code.
- Reuse existing traces, tickets, and repro data before inventing new diagnostics.
- If the fix only works for one literal query, it is not complete; lift the rule into shared routing or parsing helpers.

---

# FEATURE RULES

- Follow existing architecture boundaries and extension points.
- Add tests for the new behavior.
- Update README or API docs when behavior or interfaces change.
- Prefer additive changes over invasive reshaping.

---

# REFACTOR RULES

- No intended behavior change.
- Reduce complexity, duplication, or coupling.
- Keep public interfaces stable unless the task explicitly allows changes.
- Existing tests must keep passing.