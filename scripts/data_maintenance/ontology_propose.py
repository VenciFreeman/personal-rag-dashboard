#!/usr/bin/env python3
"""
ontology_propose.py — LLM-driven ontology candidate generator

Reads signal sources (library items, trace failures) for a given domain,
calls the local LLM with a schema-constrained prompt, and writes proposed
entries to nav_dashboard/web/services/ontologies/proposed/.

Usage:
    python scripts/data_maintenance/ontology_propose.py --domain music
    python scripts/data_maintenance/ontology_propose.py --domain video --max-items 80 --max-traces 30
    python scripts/data_maintenance/ontology_propose.py --domain book --dry-run
    python scripts/data_maintenance/ontology_propose.py --domain music --source-type title_cluster
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

WORKSPACE_ROOT = Path(__file__).resolve().parents[2]
if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))

from core_service.config import get_settings
from core_service.runtime_data import app_runtime_root

APPROVED_DIR = WORKSPACE_ROOT / "nav_dashboard" / "web" / "services" / "ontologies" / "approved"
PROPOSED_DIR = WORKSPACE_ROOT / "nav_dashboard" / "web" / "services" / "ontologies" / "proposed"
LIBRARY_DATA_DIR = WORKSPACE_ROOT / "library_tracker" / "data" / "structured"
TRACE_DATA_DIR = app_runtime_root("nav_dashboard") / "trace_records"
PROPOSE_STATE_FILE = PROPOSED_DIR / "_propose_state.json"
_CORE_SETTINGS = get_settings()

# ── Per-domain config ─────────────────────────────────────────────────────────

DOMAIN_LIBRARY_FILES: dict[str, list[str]] = {
    "music": ["music.json"],
    "video": ["video.json"],
    "book": ["reading.json"],
    "game": ["game.json"],
}

# Sections the LLM is allowed to populate per domain.
DOMAIN_ALLOWED_SECTIONS: dict[str, list[str]] = {
    "music": ["instruments", "forms", "work_families", "genres", "composer_aliases", "composer_work_signature_overrides"],
    "video": ["genres", "creator_aliases", "entity_aliases", "version_terms"],
    "book": ["genres", "creator_aliases", "entity_aliases", "version_terms"],
    "game": ["genres", "creator_aliases", "entity_aliases", "version_terms"],
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_approved(domain: str) -> dict[str, Any]:
    path = APPROVED_DIR / f"{domain}.json"
    if not path.exists():
        return {"version": "1.0", "domain": domain}
    return _load_json(path)


def _load_library_items(domain: str, max_items: int) -> list[dict[str, Any]]:
    """Return a sample of library items for the domain."""
    items: list[dict[str, Any]] = []
    for fname in DOMAIN_LIBRARY_FILES.get(domain, []):
        path = LIBRARY_DATA_DIR / fname
        if not path.exists():
            continue
        try:
            data = _load_json(path)
        except Exception as exc:
            print(f"[warn] Cannot read {path}: {exc}", file=sys.stderr)
            continue
        records = data.get("records") if isinstance(data, dict) else data
        if not isinstance(records, list):
            continue
        items.extend(records)
    # Deduplicate by title and truncate
    seen: set[str] = set()
    unique: list[dict[str, Any]] = []
    for item in items:
        title = str(item.get("title") or "").strip()
        if title and title not in seen:
            seen.add(title)
            unique.append(item)
    return unique[:max_items]


def _load_recent_traces(max_traces: int, skip_ids: set[str] | None = None) -> list[dict[str, Any]]:
    """Load recent trace records, light-weight fields only.

    ``skip_ids`` is the set of trace_ids already processed in previous runs;
    those records are skipped so the returned batch contains only fresh traces.
    """
    _skip: set[str] = skip_ids or set()
    traces: list[dict[str, Any]] = []
    # Read monthly JSONL files newest first
    files = sorted(
        TRACE_DATA_DIR.glob("trace_records_*.jsonl"),
        key=lambda p: p.name,
        reverse=True,
    )

    for path in files:
        if len(traces) >= max_traces:
            break
        try:
            lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        except Exception:
            continue
        for line in reversed(lines):
            if len(traces) >= max_traces:
                break
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except Exception:
                continue
            if not isinstance(record, dict):
                continue
            tid = str(record.get("trace_id") or "").strip()
            if tid and tid in _skip:
                continue  # already processed in a previous propose run
            traces.append({
                "trace_id": tid,
                "query": _safe_str(record, "query_profile", "raw_query"),
                "call_type": str(record.get("call_type", "") or ""),
                "result_hit_count": _safe_int(record, "retrieval", "hit_count"),
                "result_score": _safe_float(record, "retrieval", "top_score"),
                "filters_applied": _safe_list(record, "router", "filters"),
            })
    return traces


def _safe_str(record: dict, *keys: str) -> str:
    val = record
    for key in keys:
        if not isinstance(val, dict):
            return ""
        val = val.get(key)
    return str(val or "").strip()


def _safe_int(record: dict, *keys: str) -> int:
    val = record
    for key in keys:
        if not isinstance(val, dict):
            return 0
        val = val.get(key)
    try:
        return int(val or 0)
    except (TypeError, ValueError):
        return 0


def _safe_float(record: dict, *keys: str) -> float:
    val = record
    for key in keys:
        if not isinstance(val, dict):
            return 0.0
        val = val.get(key)
    try:
        return float(val or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _safe_list(record: dict, *keys: str) -> list:
    val = record
    for key in keys:
        if not isinstance(val, dict):
            return []
        val = val.get(key)
    return val if isinstance(val, list) else []


# ── Ontology summary for prompt ───────────────────────────────────────────────

def _ontology_summary(ontology: dict[str, Any], max_per_section: int = 5) -> str:
    """Render a concise summary of the approved ontology for the LLM prompt."""
    lines: list[str] = []
    for section, content in ontology.items():
        if section in ("version", "domain"):
            continue
        if not isinstance(content, dict):
            continue
        keys = list(content.keys())
        shown = keys[:max_per_section]
        ellipsis = f" … (+{len(keys) - max_per_section} more)" if len(keys) > max_per_section else ""
        lines.append(f'  "{section}": {{{", ".join(repr(k) for k in shown)}{ellipsis}}}')
    return "{\n" + "\n".join(lines) + "\n}"


# ── LLM call ─────────────────────────────────────────────────────────────────

def _local_llm_settings() -> tuple[str, str, str]:
    """Return (base_url, model, api_key) from env / core config."""
    url = (os.getenv("AI_SUMMARY_LOCAL_LLM_URL") or "").strip() or _CORE_SETTINGS.local_llm_url or "http://127.0.0.1:1234/v1"
    model = (os.getenv("AI_SUMMARY_LOCAL_LLM_MODEL") or "").strip() or _CORE_SETTINGS.local_llm_model
    api_key = (os.getenv("AI_SUMMARY_LOCAL_LLM_API_KEY") or "").strip() or _CORE_SETTINGS.local_llm_api_key or "local"

    return url, model, api_key


def _call_llm(prompt: str, url: str, model: str, api_key: str, timeout: int = 120) -> str:
    """POST to OpenAI-compatible /chat/completions and return the assistant text."""
    import urllib.request

    endpoint = url.rstrip("/") + "/chat/completions"
    body = json.dumps({
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.2,
        "max_tokens": 2048,
    }, ensure_ascii=False).encode("utf-8")

    req = urllib.request.Request(
        endpoint,
        data=body,
        headers={
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": f"Bearer {api_key}",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except Exception as exc:
        raise RuntimeError(f"LLM request failed: {exc}") from exc

    choices = payload.get("choices") or []
    if not choices:
        raise RuntimeError("LLM returned no choices")
    return str(choices[0].get("message", {}).get("content", "") or "").strip()


# ── Prompt builder ────────────────────────────────────────────────────────────

_SECTION_SHAPES: dict[str, str] = {
    "instruments": '{"aliases": ["<alias1>", "<alias2>"]}',
    "forms": '{"aliases": ["<alias1>", "<alias2>"]}',
    "work_families": '{"aliases": ["<alias>"], "instrument": "<instrument_key>", "form": "<form_key>"}',
    "genres": '{"aliases": ["<alias1>", "<alias2>"]}',
    "composer_aliases": '["<alias1>", "<alias2>"]',
    "creator_aliases": '["<alias1>", "<alias2>"]',
    "composer_work_signature_overrides": '{<composer_key>: {<family_key>: ["<sig1>"]}}',
    "entity_aliases": '{"aliases": ["<alias>"], "type": "franchise|studio|series|person"}',
    "version_terms": '{"aliases": ["<alias1>", "<alias2>"]}',
}


def _build_prompt(
    domain: str,
    ontology: dict[str, Any],
    items: list[dict[str, Any]],
    traces: list[dict[str, Any]],
    source_type: str,
    allowed_sections: list[str],
) -> str:
    ontology_str = _ontology_summary(ontology)

    section_shapes = "\n".join(
        f'  - "{s}": value shape = {_SECTION_SHAPES.get(s, "{}")}'
        for s in allowed_sections
    )

    # Compact item listing
    item_lines: list[str] = []
    for it in items[:60]:
        parts = [str(it.get("title", "") or "").strip()]
        for field in ("author", "category", "nationality", "publisher"):
            v = str(it.get(field, "") or "").strip()
            if v:
                parts.append(v)
        item_lines.append("  " + " | ".join(parts))
    items_str = "\n".join(item_lines) or "  (none)"

    # Compact trace listing
    trace_lines: list[str] = []
    for tr in traces[:20]:
        q = str(tr.get("query", "") or "").strip()
        hits = tr.get("result_hit_count", 0)
        if q:
            trace_lines.append(f"  query={q!r}  hits={hits}")
    traces_str = "\n".join(trace_lines) or "  (none)"

    return f"""You are an ontology maintenance assistant for a personal media library.
Your task: suggest NEW entries to extend the approved {domain.upper()} ontology.

=== CURRENT APPROVED ONTOLOGY (top-level keys only) ===
{ontology_str}

=== ALLOWED SECTIONS (you may ONLY add entries to these) ===
{section_shapes}

=== SIGNAL: library items ({domain}) ===
{items_str}

=== SIGNAL: recent query traces ===
{traces_str}

=== RULES ===
1. Only output entries for sections listed under ALLOWED SECTIONS. Never invent new sections.
2. Keys must be snake_case. Do not duplicate keys that already exist in the approved ontology.
3. Every alias must actually appear in the signal data above — do not invent aliases.
4. For work_families: "instrument" and "form" values must be existing keys in the approved ontology.
5. For composer_work_signature_overrides: composer key and family key must already exist in approved ontology.
6. Confidence: 0.9 = strong evidence from many items, 0.5 = seen once or twice, 0.3 = uncertain.
7. Generate at most 12 entries focused on high-confidence gaps.
8. Output ONLY a valid JSON array. No markdown, no explanation.

Output format — a JSON array where each element has exactly these keys:
[
  {{
    "id": "prop_NNN",
    "section": "<allowed section name>",
    "key": "<snake_case>",
    "value": <see shape above>,
    "reason": "<one sentence citing specific signal>",
    "confidence": <0.0–1.0>
  }}
]
"""


# ── Parse LLM output ──────────────────────────────────────────────────────────

def _extract_json_array(text: str) -> list[dict]:
    """Extract first JSON array from LLM text output."""
    # Strip markdown fences
    text = re.sub(r"```(?:json)?\s*", "", text, flags=re.IGNORECASE)
    text = text.strip()
    # Find the first [ … ] span
    start = text.find("[")
    if start == -1:
        raise ValueError("No JSON array found in LLM output")
    depth = 0
    for idx in range(start, len(text)):
        if text[idx] == "[":
            depth += 1
        elif text[idx] == "]":
            depth -= 1
            if depth == 0:
                arr_text = text[start : idx + 1]
                try:
                    return json.loads(arr_text)
                except json.JSONDecodeError as exc:
                    raise ValueError(f"JSON parse error: {exc}") from exc
    raise ValueError("Unterminated JSON array in LLM output")


def _validate_entries(
    entries: list[dict],
    allowed_sections: list[str],
    ontology: dict[str, Any],
) -> list[dict]:
    """Filter and normalise LLM-proposed entries."""
    existing_keys_per_section: dict[str, set[str]] = {}
    for sec in allowed_sections:
        content = ontology.get(sec)
        existing_keys_per_section[sec] = set(content.keys()) if isinstance(content, dict) else set()

    valid: list[dict] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        section = str(entry.get("section", "") or "").strip()
        key = str(entry.get("key", "") or "").strip()
        if not section or not key:
            continue
        if section not in allowed_sections:
            print(f"[skip] section {section!r} not in allowed list", file=sys.stderr)
            continue
        if key in existing_keys_per_section.get(section, set()):
            print(f"[skip] {section}.{key} already exists in approved ontology", file=sys.stderr)
            continue
        # Ensure required fields
        entry.setdefault("id", f"prop_{uuid.uuid4().hex[:6]}")
        entry.setdefault("reason", "")
        try:
            conf = float(entry.get("confidence", 0.5))
        except (TypeError, ValueError):
            conf = 0.5
        entry["confidence"] = round(max(0.0, min(1.0, conf)), 2)
        entry["status"] = "pending"
        valid.append(entry)
    return valid


# ── Write proposal file ───────────────────────────────────────────────────────

def _write_proposal(
    domain: str,
    entries: list[dict],
    source_type: str,
    source_ref: str,
) -> Path:
    PROPOSED_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    filename = f"{domain}_{ts}.json"
    proposal = {
        "version": "1.0",
        "domain": domain,
        "proposed_at": datetime.now(timezone.utc).isoformat(),
        "proposed_by": "ontology_propose_v1",
        "source_type": source_type,
        "source_ref": source_ref,
        "entries": entries,
    }
    path = PROPOSED_DIR / filename
    path.write_text(json.dumps(proposal, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


# ── Incremental state ─────────────────────────────────────────────────────────

def _load_propose_state() -> dict[str, Any]:
    """Load persistent propose state, returning empty structure if missing."""
    if not PROPOSE_STATE_FILE.exists():
        return {"per_domain": {}}
    try:
        data = json.loads(PROPOSE_STATE_FILE.read_text(encoding="utf-8"))
        if isinstance(data, dict) and isinstance(data.get("per_domain"), dict):
            return data
    except Exception:
        pass
    return {"per_domain": {}}


def _save_propose_state(state: dict[str, Any]) -> None:
    PROPOSED_DIR.mkdir(parents=True, exist_ok=True)
    PROPOSE_STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def _domain_state(state: dict[str, Any], domain: str) -> dict[str, Any]:
    """Return (and initialise if absent) the per-domain state dict."""
    per = state.setdefault("per_domain", {})
    if domain not in per or not isinstance(per[domain], dict):
        per[domain] = {
            "processed_source_refs": [],
            "processed_trace_ids": [],
            "proposed_entry_keys": [],
            "last_run_at": "",
            "last_lib_mtime": 0.0,
        }
    # Migrate existing state dicts that predate new fields.
    d = per[domain]
    d.setdefault("processed_trace_ids", [])
    d.setdefault("last_lib_mtime", 0.0)
    return d


def _library_mtime(domain: str) -> float:
    """Return the max mtime of library files for *domain* (0 if not found)."""
    best = 0.0
    for fname in DOMAIN_LIBRARY_FILES.get(domain, []):
        p = LIBRARY_DATA_DIR / fname
        try:
            best = max(best, p.stat().st_mtime)
        except OSError:
            pass
    return best


def _dedup_entries_against_state(
    entries: list[dict[str, Any]],
    domain_st: dict[str, Any],
) -> list[dict[str, Any]]:
    """Drop entries whose section+key was already proposed in a previous run."""
    already_proposed: set[str] = set(domain_st.get("proposed_entry_keys") or [])
    fresh: list[dict[str, Any]] = []
    skipped = 0
    for e in entries:
        key = f"{e.get('section', '')}/{e.get('key', '')}"
        if key in already_proposed:
            skipped += 1
            continue
        fresh.append(e)
    if skipped:
        print(f"[propose] dedup: skipped {skipped} entries already proposed in previous runs")
    return fresh


def _record_run_in_state(
    state: dict[str, Any],
    domain: str,
    source_ref: str,
    new_entries: list[dict[str, Any]],
    trace_ids: list[str] | None = None,
    lib_mtime: float = 0.0,
) -> None:
    """Persist the current run's source_ref, trace IDs, lib mtime and proposed keys into state."""
    domain_st = _domain_state(state, domain)
    # Keep a bounded list of source refs (last 50)
    refs: list[str] = list(domain_st.get("processed_source_refs") or [])
    if source_ref and source_ref not in refs:
        refs.append(source_ref)
    domain_st["processed_source_refs"] = refs[-50:]
    # Accumulate processed trace IDs (cap at 2000 to bound file growth)
    tids: list[str] = list(domain_st.get("processed_trace_ids") or [])
    for tid in (trace_ids or []):
        if tid and tid not in tids:
            tids.append(tid)
    domain_st["processed_trace_ids"] = tids[-2000:]
    # Record library mtime watermark so next run can skip unchanged items
    if lib_mtime > 0:
        domain_st["last_lib_mtime"] = lib_mtime
    # Accumulate proposed keys
    keys: list[str] = list(domain_st.get("proposed_entry_keys") or [])
    for e in new_entries:
        key = f"{e.get('section', '')}/{e.get('key', '')}"
        if key and key not in keys:
            keys.append(key)
    domain_st["proposed_entry_keys"] = keys
    domain_st["last_run_at"] = datetime.now(timezone.utc).isoformat()
    state["per_domain"][domain] = domain_st

# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Generate ontology proposals using local LLM")
    parser.add_argument("--domain", required=True, choices=list(DOMAIN_ALLOWED_SECTIONS), help="Ontology domain")
    parser.add_argument("--source-type", default="title_cluster",
                        choices=["title_cluster", "trace_failure", "alias_mismatch", "manual"],
                        help="Signal source type label")
    parser.add_argument("--max-items", type=int, default=100, help="Max library items to include in prompt")
    parser.add_argument("--max-traces", type=int, default=30, help="Max trace records to include in prompt")
    parser.add_argument("--dry-run", action="store_true", help="Print prompt and candidate counts without calling LLM")
    parser.add_argument("--timeout", type=int, default=120, help="LLM request timeout seconds")
    parser.add_argument("--reset-state", action="store_true", help="Clear incremental state for this domain before running")
    args = parser.parse_args()

    domain = args.domain
    allowed_sections = DOMAIN_ALLOWED_SECTIONS[domain]

    # ── Incremental state ─────────────────────────────────────────────────────
    propose_state = _load_propose_state()
    if args.reset_state:
        propose_state["per_domain"].pop(domain, None)
        print(f"[propose] incremental state cleared for domain={domain}")
    domain_st = _domain_state(propose_state, domain)
    lib_mtime = _library_mtime(domain)
    last_run = domain_st.get("last_run_at") or ""
    already_proposed_count = len(domain_st.get("proposed_entry_keys") or [])
    already_trace_count = len(domain_st.get("processed_trace_ids") or [])
    print(f"[propose] domain={domain}  source_type={args.source_type}")
    print(f"[propose] state: last_run={last_run or '(never)'}  already_proposed={already_proposed_count} keys  "
          f"already_traces={already_trace_count}  lib_mtime={lib_mtime:.0f}")

    # ── Library items: skip unchanged files ──────────────────────────────────
    last_lib_mtime = float(domain_st.get("last_lib_mtime") or 0.0)
    lib_changed = lib_mtime > last_lib_mtime + 1.0  # 1-second tolerance
    if not lib_changed and last_run:
        print(f"[propose] library files unchanged since last run — skipping item load (use --reset-state to force)")
        items = []
    else:
        items = _load_library_items(domain, args.max_items)

    # ── Traces: skip already-processed IDs ───────────────────────────────────
    skip_trace_ids: set[str] = set(domain_st.get("processed_trace_ids") or [])
    traces = _load_recent_traces(args.max_traces, skip_ids=skip_trace_ids)
    loaded_trace_ids = [str(t.get("trace_id") or "").strip() for t in traces if t.get("trace_id")]

    print(f"[propose] loaded {len(items)} library items, {len(traces)} new traces "
          f"(skipped {len(skip_trace_ids)} already-processed trace IDs)")

    # ── Early exit when no new signals at all ────────────────────────────────
    if not items and not traces and last_run and not args.reset_state:
        print("[propose] No new signals (library unchanged, no new traces) — skipping LLM call.")
        _record_run_in_state(propose_state, domain, "", [], trace_ids=[], lib_mtime=lib_mtime)
        _save_propose_state(propose_state)
        return

    # Load approved ontology (deferred until we know we have signals)
    ontology = _load_approved(domain)

    # Build prompt
    prompt = _build_prompt(
        domain=domain,
        ontology=ontology,
        items=items,
        traces=traces,
        source_type=args.source_type,
        allowed_sections=allowed_sections,
    )

    if args.dry_run:
        print("\n===== PROMPT =====")
        print(prompt)
        print("===== END PROMPT =====\n")
        print(f"(dry-run) already proposed in state: {already_proposed_count} keys")
        print("(dry-run: skipping LLM call)")
        return

    # Call LLM
    llm_url, llm_model, llm_api_key = _local_llm_settings()
    print(f"[propose] calling LLM  url={llm_url}  model={llm_model}  timeout={args.timeout}s")
    try:
        raw_output = _call_llm(prompt, llm_url, llm_model, llm_api_key, timeout=args.timeout)
    except RuntimeError as exc:
        print(f"[error] {exc}", file=sys.stderr)
        sys.exit(1)

    # Parse and validate
    try:
        raw_entries = _extract_json_array(raw_output)
    except ValueError as exc:
        print(f"[error] Could not parse LLM output: {exc}", file=sys.stderr)
        print("[raw output]:", file=sys.stderr)
        print(raw_output[:2000], file=sys.stderr)
        sys.exit(1)

    entries = _validate_entries(raw_entries, allowed_sections, ontology)
    print(f"[propose] {len(raw_entries)} raw entries → {len(entries)} valid after schema filter")

    # Dedup against incremental state
    entries = _dedup_entries_against_state(entries, domain_st)
    print(f"[propose] {len(entries)} entries remaining after dedup")

    if not entries:
        print("[propose] Nothing new to write — update state and exit.")
        source_ref = f"library_items:{domain}:{len(items)}_items"
        _record_run_in_state(propose_state, domain, source_ref, [], trace_ids=loaded_trace_ids, lib_mtime=lib_mtime)
        _save_propose_state(propose_state)
        return

    # Write proposal
    source_ref = f"library_items:{domain}:{len(items)}_items"
    path = _write_proposal(domain, entries, args.source_type, source_ref)
    print(f"[propose] wrote {len(entries)} proposals → {path}")

    # Persist state
    _record_run_in_state(propose_state, domain, source_ref, entries, trace_ids=loaded_trace_ids, lib_mtime=lib_mtime)
    _save_propose_state(propose_state)
    print(f"[propose] state updated  total_proposed_keys={len(propose_state['per_domain'][domain]['proposed_entry_keys'])}  "
          f"processed_trace_ids={len(propose_state['per_domain'][domain]['processed_trace_ids'])}")
    print()
    print("Next step: run  python scripts/data_maintenance/ontology_review.py --domain", domain)


if __name__ == "__main__":
    main()
