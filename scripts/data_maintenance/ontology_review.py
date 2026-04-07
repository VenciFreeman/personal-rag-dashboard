#!/usr/bin/env python3
"""
ontology_review.py — Interactive review and merge tool for ontology proposals

Reads pending proposal files from ontologies/proposed/, presents each entry
for human review, and merges accepted entries into ontologies/approved/*.json.

Usage:
    python scripts/data_maintenance/ontology_review.py                       # review all domains
    python scripts/data_maintenance/ontology_review.py --domain music        # filter by domain
    python scripts/data_maintenance/ontology_review.py --domain music --list # list pending without reviewing
    python scripts/data_maintenance/ontology_review.py --stats               # show proposal stats

Controls during review:
    a  — Accept: merge into approved ontology
    r  — Reject: mark as rejected
    e  — Edit value JSON before accepting
    s  — Skip for now (leaves as pending)
    q  — Quit and save progress

Output:
    - Approved entries are merged into approved/<domain>.json
    - Proposal file statuses are updated in-place
    - Conflicts (duplicate alias, existing key) are reported before merge
"""

from __future__ import annotations

import argparse
import copy
import json
import sys
from pathlib import Path
from typing import Any

WORKSPACE_ROOT = Path(__file__).resolve().parents[2]
APPROVED_DIR = WORKSPACE_ROOT / "nav_dashboard" / "web" / "services" / "ontologies" / "approved"
PROPOSED_DIR = WORKSPACE_ROOT / "nav_dashboard" / "web" / "services" / "ontologies" / "proposed"

# Import shared schema validator from the runtime loader so both tools
# always enforce the same rules.
import importlib.util as _importlib_util

def _load_schema_validator():
    """Dynamically load validate_ontology_payload from the ontology_loader module."""
    loader_path = WORKSPACE_ROOT / "nav_dashboard" / "web" / "services" / "ontologies" / "ontology_loader.py"
    spec = _importlib_util.spec_from_file_location("ontology_loader", loader_path)
    if spec and spec.loader:
        mod = _importlib_util.module_from_spec(spec)
        try:
            spec.loader.exec_module(mod)  # type: ignore[union-attr]
            return getattr(mod, "validate_ontology_payload", None)
        except Exception:
            pass
    return None

_validate_ontology_payload = _load_schema_validator()

# ── IO helpers ────────────────────────────────────────────────────────────────

def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _save_json(path: Path, data: Any) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _load_approved(domain: str) -> dict[str, Any]:
    path = APPROVED_DIR / f"{domain}.json"
    if not path.exists():
        return {"version": "1.0", "domain": domain}
    return _load_json(path)


def _save_approved(domain: str, ontology: dict[str, Any]) -> None:
    APPROVED_DIR.mkdir(parents=True, exist_ok=True)
    _save_json(APPROVED_DIR / f"{domain}.json", ontology)


def _list_proposal_files(domain: str | None) -> list[Path]:
    if not PROPOSED_DIR.exists():
        return []
    files = sorted(PROPOSED_DIR.glob("*.json"))
    result: list[Path] = []
    for path in files:
        if path.name == "README.json":
            continue
        try:
            data = _load_json(path)
        except Exception:
            continue
        if not isinstance(data, dict):
            continue
        if domain and str(data.get("domain", "") or "") != domain:
            continue
        result.append(path)
    return result


# ── Conflict detection ────────────────────────────────────────────────────────

def _collect_aliases_in_section(section: dict[str, Any]) -> dict[str, str]:
    """Return {normalised_alias: key} for all aliases in a section."""
    alias_map: dict[str, str] = {}
    for key, payload in section.items():
        aliases: list[str] = []
        if isinstance(payload, dict):
            raw = payload.get("aliases")
            if isinstance(raw, list):
                aliases = [str(a).strip().lower() for a in raw if str(a).strip()]
        elif isinstance(payload, list):
            aliases = [str(a).strip().lower() for a in payload if str(a).strip()]
        for alias in aliases:
            alias_map[alias] = key
    return alias_map


def _detect_conflicts(
    entry: dict[str, Any],
    ontology: dict[str, Any],
) -> list[str]:
    """Return a list of human-readable conflict warnings (empty = no conflicts)."""
    conflicts: list[str] = []
    section_name = str(entry.get("section", "") or "")
    key = str(entry.get("key", "") or "")
    value = entry.get("value")
    section = ontology.get(section_name)

    # Key collision
    if isinstance(section, dict) and key in section:
        conflicts.append(f"Key {key!r} already exists in section {section_name!r}")

    # Alias collision
    if isinstance(section, dict) and isinstance(value, dict):
        existing_aliases = _collect_aliases_in_section(section)
        new_aliases: list[str] = []
        raw = value.get("aliases")
        if isinstance(raw, list):
            new_aliases = [str(a).strip().lower() for a in raw if str(a).strip()]
        elif isinstance(value, list):
            new_aliases = [str(a).strip().lower() for a in value if str(a).strip()]
        for alias in new_aliases:
            if alias in existing_aliases:
                conflicts.append(f"Alias {alias!r} already used by key {existing_aliases[alias]!r}")

    # work_families: validate instrument/form keys exist
    if section_name == "work_families" and isinstance(value, dict):
        instrument = str(value.get("instrument", "") or "")
        form = str(value.get("form", "") or "")
        instruments = ontology.get("instruments", {})
        forms = ontology.get("forms", {})
        if instrument and isinstance(instruments, dict) and instrument not in instruments:
            conflicts.append(f"Instrument key {instrument!r} not found in approved ontology")
        if form and isinstance(forms, dict) and form not in forms:
            conflicts.append(f"Form key {form!r} not found in approved ontology")

    return conflicts


# ── Merge logic ───────────────────────────────────────────────────────────────

def _merge_entry(entry: dict[str, Any], ontology: dict[str, Any]) -> None:
    """Write accepted entry into the in-memory ontology dict."""
    section_name = str(entry.get("section", "") or "")
    key = str(entry.get("key", "") or "")
    value = entry.get("value")

    if section_name not in ontology:
        ontology[section_name] = {}

    section = ontology[section_name]
    if not isinstance(section, dict):
        ontology[section_name] = {}
        section = ontology[section_name]

    if key in section:
        # Merge aliases rather than overwrite
        existing = section[key]
        if isinstance(existing, dict) and isinstance(value, dict):
            ex_aliases = list(existing.get("aliases") or [])
            new_aliases = list(value.get("aliases") or [])
            merged_aliases = list(dict.fromkeys(ex_aliases + [a for a in new_aliases if a not in ex_aliases]))
            updated = {**existing, **value, "aliases": merged_aliases}
            section[key] = updated
        elif isinstance(existing, list) and isinstance(value, list):
            section[key] = list(dict.fromkeys(existing + [a for a in value if a not in existing]))
        else:
            section[key] = value
    else:
        section[key] = value


# ── Display helpers ───────────────────────────────────────────────────────────

def _color(text: str, code: str) -> str:
    """ANSI color wrapper; falls back to plain text if terminal doesn't support it."""
    if not sys.stdout.isatty():
        return text
    return f"\033[{code}m{text}\033[0m"


def _print_entry(entry: dict[str, Any], idx: int, total: int) -> None:
    print()
    print(_color(f"══ Entry {idx}/{total} ══════════════════════════════════════", "1"))
    print(f"  id:        {entry.get('id', '')}")
    print(f"  section:   {_color(str(entry.get('section', '')), '36')}")
    print(f"  key:       {_color(str(entry.get('key', '')), '33')}")
    print(f"  confidence:{entry.get('confidence', '?')}")
    print(f"  reason:    {entry.get('reason', '')}")
    print(f"  value:")
    value_str = json.dumps(entry.get("value"), ensure_ascii=False, indent=4)
    for line in value_str.splitlines():
        print(f"    {line}")


def _print_conflicts(conflicts: list[str]) -> None:
    if not conflicts:
        return
    print()
    for c in conflicts:
        print(_color(f"  ⚠  CONFLICT: {c}", "33"))


# ── Stats ─────────────────────────────────────────────────────────────────────

def _show_stats(domain: str | None) -> None:
    files = _list_proposal_files(domain)
    if not files:
        print("No proposal files found.")
        return

    total = pending = accepted = rejected = 0
    by_domain: dict[str, dict[str, int]] = {}
    for path in files:
        try:
            data = _load_json(path)
        except Exception:
            continue
        dom = str(data.get("domain", path.stem) or "")
        if dom not in by_domain:
            by_domain[dom] = {"total": 0, "pending": 0, "accepted": 0, "rejected": 0}
        for entry in (data.get("entries") or []):
            status = str(entry.get("status", "pending") or "pending")
            total += 1
            by_domain[dom]["total"] += 1
            if status == "pending":
                pending += 1
                by_domain[dom]["pending"] += 1
            elif status == "accepted":
                accepted += 1
                by_domain[dom]["accepted"] += 1
            elif status == "rejected":
                rejected += 1
                by_domain[dom]["rejected"] += 1

    print(f"\nProposal stats  ({len(files)} files)")
    print(f"  total={total}  pending={pending}  accepted={accepted}  rejected={rejected}")
    print()
    for dom, counts in sorted(by_domain.items()):
        print(f"  {dom:12s}  total={counts['total']}  pending={counts['pending']}  accepted={counts['accepted']}  rejected={counts['rejected']}")
    print()


# ── List mode ─────────────────────────────────────────────────────────────────

def _show_list(domain: str | None) -> None:
    files = _list_proposal_files(domain)
    if not files:
        print("No proposal files found.")
        return
    for path in files:
        try:
            data = _load_json(path)
        except Exception:
            print(f"  [error reading {path.name}]")
            continue
        dom = str(data.get("domain", "") or "")
        proposed_at = str(data.get("proposed_at", "") or "")[:19]
        entries = data.get("entries") or []
        pending = sum(1 for e in entries if str(e.get("status", "pending") or "pending") == "pending")
        print(f"  {path.name}  domain={dom}  at={proposed_at}  entries={len(entries)}  pending={pending}")


# ── Review loop ───────────────────────────────────────────────────────────────

def _review_file(
    path: Path,
    ontology_cache: dict[str, dict[str, Any]],
    dirty_domains: set[str],
) -> bool:
    """Review one proposal file. Returns False if user quit."""
    try:
        proposal = _load_json(path)
    except Exception as exc:
        print(f"[error] Cannot read {path}: {exc}")
        return True

    domain = str(proposal.get("domain", "") or "")
    if not domain:
        print(f"[skip] {path.name} has no domain field")
        return True

    entries = proposal.get("entries")
    if not isinstance(entries, list):
        return True

    pending_indices = [i for i, e in enumerate(entries) if str(e.get("status", "pending") or "pending") == "pending"]
    if not pending_indices:
        return True  # nothing to review

    print(f"\n{'─'*60}")
    print(f"File: {path.name}  domain={domain}  source_type={proposal.get('source_type', '')}  proposed_at={str(proposal.get('proposed_at',''))[:19]}")
    print(f"Pending: {len(pending_indices)} / {len(entries)} entries")

    if domain not in ontology_cache:
        ontology_cache[domain] = _load_approved(domain)

    ontology = ontology_cache[domain]
    proposal_dirty = False

    for pos, idx in enumerate(pending_indices, 1):
        entry = entries[idx]
        total_pending = len(pending_indices)

        _print_entry(entry, pos, total_pending)
        conflicts = _detect_conflicts(entry, ontology)
        _print_conflicts(conflicts)

        print()
        if conflicts:
            print("  [a]ccept anyway  [r]eject  [e]dit  [s]kip  [q]uit")
        else:
            print("  [a]ccept  [r]eject  [e]dit  [s]kip  [q]uit")

        while True:
            try:
                choice = input("  > ").strip().lower()
            except (EOFError, KeyboardInterrupt):
                choice = "q"

            if choice == "a":
                _merge_entry(entry, ontology)
                entry["status"] = "accepted"
                dirty_domains.add(domain)
                proposal_dirty = True
                print(_color("  ✓ Accepted and merged.", "32"))
                break
            elif choice == "r":
                entry["status"] = "rejected"
                proposal_dirty = True
                print(_color("  ✗ Rejected.", "31"))
                break
            elif choice == "e":
                print("  Enter new JSON value (single line, or empty to cancel):")
                try:
                    raw = input("  value > ").strip()
                except (EOFError, KeyboardInterrupt):
                    print("  (cancelled)")
                    continue
                if not raw:
                    continue
                try:
                    new_value = json.loads(raw)
                    entry["value"] = new_value
                    print(_color("  ✓ Value updated. Enter 'a' to accept or 's' to skip.", "36"))
                except json.JSONDecodeError as exc:
                    print(f"  [error] Invalid JSON: {exc}. Try again.")
                    continue
            elif choice == "s":
                print("  (skipped)")
                break
            elif choice == "q":
                if proposal_dirty:
                    _save_json(path, proposal)
                return False
            else:
                print("  Invalid input. Use a/r/e/s/q.")

    if proposal_dirty:
        _save_json(path, proposal)
        print(f"\n[saved] {path.name}")

    return True  # continue


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Review and merge ontology proposals")
    parser.add_argument("--domain", help="Filter to a specific domain (music/video/book)")
    parser.add_argument("--list", action="store_true", help="List proposal files without reviewing")
    parser.add_argument("--stats", action="store_true", help="Show proposal statistics")
    args = parser.parse_args()

    if args.stats:
        _show_stats(args.domain)
        return

    if args.list:
        _show_list(args.domain)
        return

    files = _list_proposal_files(args.domain)
    if not files:
        print("No pending proposal files found.")
        if args.domain:
            print(f"  (searched for domain={args.domain!r})")
        print(f"  Proposal directory: {PROPOSED_DIR}")
        return

    pending_files = []
    for path in files:
        try:
            data = _load_json(path)
        except Exception:
            continue
        entries = data.get("entries") or []
        if any(str(e.get("status", "pending") or "pending") == "pending" for e in entries):
            pending_files.append(path)

    if not pending_files:
        print("All proposals have already been reviewed.")
        _show_stats(args.domain)
        return

    print(f"\nFound {len(pending_files)} file(s) with pending proposals.")
    print("Controls: [a]ccept  [r]eject  [e]dit  [s]kip  [q]uit\n")

    ontology_cache: dict[str, dict[str, Any]] = {}
    dirty_domains: set[str] = set()

    for path in pending_files:
        should_continue = _review_file(path, ontology_cache, dirty_domains)
        if not should_continue:
            print("\n[quit] Saving in-progress ontologies...")
            break

    # Persist accepted entries to approved ontologies
    for domain in dirty_domains:
        if domain in ontology_cache:
            ontology = ontology_cache[domain]
            if _validate_ontology_payload is not None:
                try:
                    _validate_ontology_payload(domain, ontology)
                except ValueError as exc:
                    print(f"[error] Schema validation failed for {domain}: {exc} — skipping save.")
                    continue
            _save_approved(domain, ontology)
            print(f"[saved] approved/{domain}.json")

    if dirty_domains:
        print(f"\nUpdated approved ontologies: {', '.join(sorted(dirty_domains))}")

        # Ontology changes are picked up automatically via mtime-based hot-reload
        # (ontology_loader.py). No process restart required.
    else:
        print("\nNo changes to approved ontologies.")

    _show_stats(args.domain)


if __name__ == "__main__":
    main()
