from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from hashlib import sha256
from pathlib import Path
from typing import Any
from uuid import uuid4

from web.config import DATA_DIR, DOCUMENTS_DIR, SCRIPTS_DIR, VECTOR_DB_DIR, WORKSPACE_ROOT
if str(WORKSPACE_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT.parent))

from core_service.config import get_settings

RAW_DIR = DATA_DIR / "raw_dir"
EXTRACTED_DIR = DATA_DIR / "extracted_dir"
SUMMARIZE_DIR = DATA_DIR / "summarize_dir"
CORE_CONFIG_PATH = WORKSPACE_ROOT.parent / "core_service" / "config.local.json"
DEFAULT_TIMEOUT = int(os.getenv("DEEPSEEK_TIMEOUT", "120") or "120")
REEMBED_QUEUE_PATH = VECTOR_DB_DIR / "reembed_queue.json"
WORKFLOW_STATE_PATH = DATA_DIR / "workflow_state.json"
AUTO_REEMBED_ENABLED = os.getenv("AI_SUMMARY_AUTO_REEMBED", "1") != "0"
AUTO_REEMBED_INTERVAL_SECONDS = max(10, int(os.getenv("AI_SUMMARY_AUTO_REEMBED_INTERVAL", "20") or "20"))
_AUTO_REEMBED_STARTED = False
_STARTUP_WORKER_STARTED = False
_STARTUP_STATUS: dict[str, Any] = {
    "status": "idle",
    "last_checked_at": "",
    "last_warmup_at": "",
    "last_repair_at": "",
    "checks": {},
}
_STARTUP_LOGS: deque[str] = deque(maxlen=120)
_CORE_SETTINGS = get_settings()


@dataclass
class WorkflowJob:
    id: str
    action: str
    status: str = "running"
    created_at: str = field(default_factory=lambda: datetime.now().isoformat(timespec="seconds"))
    updated_at: str = field(default_factory=lambda: datetime.now().isoformat(timespec="seconds"))
    logs: list[str] = field(default_factory=list)
    result: dict[str, Any] = field(default_factory=dict)


_JOBS: dict[str, WorkflowJob] = {}
_ACTIVE_JOB_ID: str = ""
_LOCK = threading.Lock()
_ANSI_ESCAPE_RE = re.compile(r"\x1B\[[0-?]*[ -/]*[@-~]")
_LOAD_REPORT_START_RE = re.compile(r"\bLOAD REPORT\b", re.IGNORECASE)


def _now_str() -> str:
    return datetime.now().strftime("%H:%M:%S")


def _append_log(job: WorkflowJob, message: str) -> None:
    text = str(message or "").rstrip("\n")
    line = f"[{_now_str()}] {text}"
    with _LOCK:
        job.logs.append(line)
        if len(job.logs) > 500:
            job.logs = job.logs[-500:]
        job.updated_at = datetime.now().isoformat(timespec="seconds")


def _set_job_done(job: WorkflowJob, status: str, result: dict[str, Any] | None = None) -> None:
    global _ACTIVE_JOB_ID
    with _LOCK:
        job.status = status
        job.result = result or {}
        job.updated_at = datetime.now().isoformat(timespec="seconds")
        if _ACTIVE_JOB_ID == job.id:
            _ACTIVE_JOB_ID = ""


def _append_startup_log(message: str) -> None:
    line = f"[{_now_str()}] {str(message or '').strip()}"
    with _LOCK:
        _STARTUP_LOGS.append(line)


def _set_startup_status(**kwargs: Any) -> None:
    with _LOCK:
        _STARTUP_STATUS.update(kwargs)


def get_startup_status() -> dict[str, Any]:
    with _LOCK:
        return {
            **_STARTUP_STATUS,
            "logs": list(_STARTUP_LOGS),
        }


def _is_any_job_running() -> bool:
    with _LOCK:
        return bool(_ACTIVE_JOB_ID and _ACTIVE_JOB_ID in _JOBS and _JOBS[_ACTIVE_JOB_ID].status == "running")


def _read_vector_metadata_docs() -> tuple[dict[str, Any], list[dict[str, Any]]]:
    path = VECTOR_DB_DIR / "metadata.json"
    if not path.exists():
        return {}, []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}, []
    if isinstance(payload, list):
        docs = [row for row in payload if isinstance(row, dict)]
        return {}, docs
    if isinstance(payload, dict):
        manifest = payload.get("manifest") if isinstance(payload.get("manifest"), dict) else {}
        docs_raw = payload.get("documents") if isinstance(payload.get("documents"), list) else []
        docs = [row for row in docs_raw if isinstance(row, dict)]
        return dict(manifest), docs
    return {}, []


def _write_vector_metadata_docs(manifest: dict[str, Any], docs: list[dict[str, Any]]) -> None:
    VECTOR_DB_DIR.mkdir(parents=True, exist_ok=True)
    out = {
        "manifest": manifest if isinstance(manifest, dict) else {},
        "documents": docs,
    }
    (VECTOR_DB_DIR / "metadata.json").write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")


def _iter_document_markdown_files() -> list[Path]:
    if not DOCUMENTS_DIR.exists():
        return []
    files = [p for p in DOCUMENTS_DIR.rglob("*.md") if p.is_file() and p.name != ".gitkeep"]
    return sorted(files, key=lambda p: p.as_posix().lower())


def _sha256_file(path: Path) -> str:
    # Read as text so line-ending normalisation matches _compute_source_hash in
    # rag_vector_index.py (which also decodes via read_text before re-encoding).
    # Using raw read_bytes() produces a different digest on CRLF files, causing
    # every workflow check to falsely mark all documents as changed.
    text = path.read_text(encoding="utf-8", errors="ignore")
    return sha256(text.encode("utf-8", errors="ignore")).hexdigest()


def _load_reembed_queue() -> dict[str, Any]:
    if not REEMBED_QUEUE_PATH.exists():
        return {"items": [], "updated_at": ""}
    try:
        payload = json.loads(REEMBED_QUEUE_PATH.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            items = payload.get("items") if isinstance(payload.get("items"), list) else []
            return {"items": items, "updated_at": str(payload.get("updated_at", ""))}
    except Exception:
        pass
    return {"items": [], "updated_at": ""}


def _save_reembed_queue(items: list[dict[str, Any]]) -> None:
    REEMBED_QUEUE_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "items": items,
        "updated_at": datetime.now().isoformat(timespec="seconds"),
    }
    REEMBED_QUEUE_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _load_workflow_state() -> dict[str, Any]:
    if not WORKFLOW_STATE_PATH.exists():
        return {"last_classify_archive_at": ""}
    try:
        payload = json.loads(WORKFLOW_STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {"last_classify_archive_at": ""}
    if not isinstance(payload, dict):
        return {"last_classify_archive_at": ""}
    return {
        "last_classify_archive_at": str(payload.get("last_classify_archive_at", "") or "").strip(),
    }


def _save_workflow_state(state: dict[str, Any]) -> None:
    WORKFLOW_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    WORKFLOW_STATE_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def _record_last_classify_archive_at(ts: str | None = None) -> str:
    timestamp = str(ts or datetime.now().isoformat(timespec="seconds")).strip()
    with _LOCK:
        state = _load_workflow_state()
        state["last_classify_archive_at"] = timestamp
        _save_workflow_state(state)
    return timestamp


def _get_last_classify_archive_at() -> str:
    with _LOCK:
        state = _load_workflow_state()
        return str(state.get("last_classify_archive_at", "") or "").strip()


def _enqueue_reembed_paths(paths: list[str], reason: str) -> int:
    if not paths:
        return 0
    queue = _load_reembed_queue()
    items = list(queue.get("items") or [])
    existing = {str(item.get("relative_path", "")).strip() for item in items if isinstance(item, dict)}
    added = 0
    now = datetime.now().isoformat(timespec="seconds")
    for rel in paths:
        key = str(rel or "").strip().replace("\\", "/")
        if not key or key in existing:
            continue
        items.append({"relative_path": key, "reason": reason, "queued_at": now})
        existing.add(key)
        added += 1
    if added > 0:
        _save_reembed_queue(items)
    return added


def _scan_changed_documents_and_mark_metadata() -> dict[str, Any]:
    manifest, docs = _read_vector_metadata_docs()
    docs_by_rel: dict[str, dict[str, Any]] = {}
    for row in docs:
        rel = str(row.get("relative_path") or row.get("file_path") or "").strip().replace("\\", "/")
        if rel:
            docs_by_rel[rel] = dict(row)

    changed_existing: list[str] = []
    missing_in_index: list[str] = []
    docs_changed = False

    for file_path in _iter_document_markdown_files():
        rel = file_path.relative_to(DOCUMENTS_DIR).as_posix()
        source_hash = _sha256_file(file_path)
        row = docs_by_rel.get(rel)
        if row is None:
            missing_in_index.append(rel)
            continue

        hash_before = str(row.get("source_hash") or "").strip()
        changed_before = bool(row.get("changed", False))
        if hash_before != source_hash:
            row["source_hash"] = source_hash
            row["changed"] = True
            docs_by_rel[rel] = row
            changed_existing.append(rel)
            docs_changed = True
        elif changed_before:
            changed_existing.append(rel)

    if docs_changed:
        # Preserve existing ordering where possible.
        ordered: list[dict[str, Any]] = []
        seen: set[str] = set()
        for row in docs:
            rel = str(row.get("relative_path") or row.get("file_path") or "").strip().replace("\\", "/")
            if not rel or rel in seen:
                continue
            seen.add(rel)
            ordered.append(docs_by_rel.get(rel, row))
        for rel, row in docs_by_rel.items():
            if rel in seen:
                continue
            ordered.append(row)
        _write_vector_metadata_docs(manifest, ordered)

    queued = _enqueue_reembed_paths(changed_existing + missing_in_index, reason="document_changed")
    return {
        "changed_existing": changed_existing,
        "missing_in_index": missing_in_index,
        "queued": queued,
    }


def _clear_changed_flags(relative_paths: list[str]) -> None:
    """Reset changed=False in metadata.json for the given paths after successful reembed."""
    keys = {str(p).strip().replace("\\", "/") for p in relative_paths if p}
    if not keys:
        return
    manifest, docs = _read_vector_metadata_docs()
    modified = False
    for row in docs:
        rel = str(row.get("relative_path") or row.get("file_path") or "").strip().replace("\\", "/")
        if rel in keys and bool(row.get("changed", False)):
            row["changed"] = False
            modified = True
    if modified:
        _write_vector_metadata_docs(manifest, docs)


def _run_auto_reembed_once() -> dict[str, Any]:
    if _is_any_job_running():
        return {"status": "skipped_active_job"}

    queue = _load_reembed_queue()
    items = [item for item in (queue.get("items") or []) if isinstance(item, dict)]
    if not items:
        return {"status": "idle"}

    command = [
        _resolve_python_executable(),
        "-u",
        str(SCRIPTS_DIR / "rag_vector_index.py"),
        "--documents-dir",
        str(DOCUMENTS_DIR),
        "--index-dir",
        str(VECTOR_DB_DIR),
        "--backend",
        "faiss",
        "--sync-missing",
        "--embedding-model",
        os.getenv("LOCAL_EMBEDDING_MODEL", "").strip()
        or os.getenv("DEEPSEEK_EMBEDDING_MODEL", "").strip()
        or "BAAI/bge-base-zh-v1.5",
        "--timeout",
        str(DEFAULT_TIMEOUT),
    ]

    proc = subprocess.run(
        command,
        cwd=str(WORKSPACE_ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    if proc.returncode != 0:
        return {
            "status": "failed",
            "returncode": proc.returncode,
            "stderr": (proc.stderr or "")[-2000:],
            "stdout": (proc.stdout or "")[-2000:],
        }

    _save_reembed_queue([])
    _clear_changed_flags([str(item.get("relative_path", "")) for item in items])
    return {"status": "ok", "processed": len(items)}


def _auto_reembed_worker_loop() -> None:
    while True:
        try:
            _scan_changed_documents_and_mark_metadata()
            _run_auto_reembed_once()
        except Exception:
            # Keep the worker resilient; next loop will retry.
            pass
        time.sleep(float(AUTO_REEMBED_INTERVAL_SECONDS))


def _ensure_auto_reembed_worker_started() -> None:
    global _AUTO_REEMBED_STARTED
    if not AUTO_REEMBED_ENABLED:
        return
    if _AUTO_REEMBED_STARTED:
        return
    _AUTO_REEMBED_STARTED = True
    thread = threading.Thread(target=_auto_reembed_worker_loop, daemon=True, name="ai-summary-auto-reembed")
    thread.start()


def _get_faiss_vector_count() -> int:
    index_path = VECTOR_DB_DIR / "faiss.index"
    if not index_path.exists():
        return 0
    try:
        import faiss  # type: ignore

        idx = faiss.read_index(str(index_path))
        return int(getattr(idx, "ntotal", 0) or 0)
    except Exception:
        return -1


def _get_graph_node_count() -> int:
    graph_path = VECTOR_DB_DIR / "knowledge_graph_rag.json"
    if not graph_path.exists():
        return 0
    try:
        payload = json.loads(graph_path.read_text(encoding="utf-8"))
    except Exception:
        return -1
    if not isinstance(payload, dict):
        return -1
    nodes = payload.get("nodes")
    if isinstance(nodes, dict):
        return len(nodes)
    return -1


def _run_startup_integrity_check_and_repair() -> dict[str, Any]:
    _append_startup_log("启动一致性检查开始")
    manifest, docs = _read_vector_metadata_docs()
    metadata_rows = len(docs)
    faiss_count = _get_faiss_vector_count()
    graph_nodes = _get_graph_node_count()

    issues: list[str] = []
    repaired = False

    if faiss_count >= 0 and metadata_rows != faiss_count:
        issues.append(f"FAISS/metadata 不一致: faiss={faiss_count}, metadata={metadata_rows}")
    if graph_nodes == 0 and metadata_rows > 0:
        issues.append("知识图谱节点为空")

    changed_scan = _scan_changed_documents_and_mark_metadata()
    queued = int(changed_scan.get("queued", 0) or 0)
    if queued > 0:
        issues.append(f"检测到文档变更并入队: {queued}")

    if issues:
        _append_startup_log("发现问题: " + " | ".join(issues))
        try:
            auto_sync = _run_auto_reembed_once()
            _append_startup_log(f"自动重向量化结果: {auto_sync.get('status', 'unknown')}")
            if str(auto_sync.get("status")) == "ok":
                repaired = True
        except Exception as exc:  # noqa: BLE001
            _append_startup_log(f"自动重向量化失败: {exc}")

        if graph_nodes == 0 and metadata_rows > 0:
            graph_command = [
                _resolve_python_executable(),
                "-u",
                str(SCRIPTS_DIR / "rag_knowledge_graph.py"),
                "--index-dir",
                str(VECTOR_DB_DIR),
                "--sync-missing",
                "--prune-missing",
                "--no-llm",
            ]
            proc = subprocess.run(
                graph_command,
                cwd=str(WORKSPACE_ROOT),
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                check=False,
            )
            if proc.returncode == 0:
                repaired = True
                _append_startup_log("知识图谱已补齐")
            else:
                _append_startup_log("知识图谱补齐失败")

    # Refresh counts after repair attempt.
    _manifest_after, docs_after = _read_vector_metadata_docs()
    result = {
        "metadata_rows": len(docs_after),
        "faiss_vectors": _get_faiss_vector_count(),
        "graph_nodes": _get_graph_node_count(),
        "issues": issues,
        "repaired": repaired,
        "embedding_model": str(manifest.get("embedding_model") or ""),
    }
    _append_startup_log(
        "一致性检查完成: "
        f"metadata={result['metadata_rows']}, faiss={result['faiss_vectors']}, graph_nodes={result['graph_nodes']}"
    )
    return result


def _run_startup_model_warmup() -> dict[str, Any]:
    _append_startup_log("启动预热开始（embedding + reranker）")
    embedding_model = (
        os.getenv("LOCAL_EMBEDDING_MODEL", "").strip()
        or os.getenv("DEEPSEEK_EMBEDDING_MODEL", "").strip()
        or "BAAI/bge-base-zh-v1.5"
    )
    reranker_model = os.getenv("AI_SUMMARY_RERANKER_MODEL", "BAAI/bge-reranker-v2-m3").strip() or "BAAI/bge-reranker-v2-m3"

    code = (
        "from sentence_transformers import SentenceTransformer, CrossEncoder\n"
        f"emb = SentenceTransformer({embedding_model!r})\n"
        "_ = emb.encode(['warmup dummy text'], normalize_embeddings=True)\n"
        f"rer = CrossEncoder({reranker_model!r})\n"
        "_ = rer.predict([('dummy query','dummy doc')])\n"
        "print('warmup_ok')\n"
    )
    proc = subprocess.run(
        [_resolve_python_executable(), "-c", code],
        cwd=str(WORKSPACE_ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    ok = proc.returncode == 0
    if ok:
        _append_startup_log("模型预热完成")
    else:
        _append_startup_log("模型预热失败（将在首次请求时惰性加载）")
    return {
        "ok": ok,
        "embedding_model": embedding_model,
        "reranker_model": reranker_model,
        "stderr_tail": (proc.stderr or "")[-800:],
    }


def _startup_worker() -> None:
    _set_startup_status(status="running", last_checked_at=datetime.now().isoformat(timespec="seconds"))
    check_result = _run_startup_integrity_check_and_repair()
    _set_startup_status(
        checks=check_result,
        last_checked_at=datetime.now().isoformat(timespec="seconds"),
        last_repair_at=datetime.now().isoformat(timespec="seconds") if check_result.get("repaired") else "",
    )

    warmup_result = _run_startup_model_warmup()
    _set_startup_status(
        status="ready",
        warmup=warmup_result,
        last_warmup_at=datetime.now().isoformat(timespec="seconds"),
    )


def _ensure_startup_worker_started() -> None:
    global _STARTUP_WORKER_STARTED
    if _STARTUP_WORKER_STARTED:
        return
    _STARTUP_WORKER_STARTED = True
    thread = threading.Thread(target=_startup_worker, daemon=True, name="ai-summary-startup-worker")
    thread.start()


def _resolve_python_executable() -> str:
    env_python = (os.getenv("AI_SUMMARY_PYTHON", "") or "").strip()
    if env_python:
        return env_python
    return sys.executable


def _parse_date(value: str) -> datetime.date | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        return None


def _extract_date_from_name(path: Path) -> datetime.date | None:
    name = path.name
    if len(name) < 10:
        return None
    try:
        return datetime.strptime(name[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def _collect_extracted_files(start_date: str, end_date: str) -> tuple[list[Path], str | None]:
    start = _parse_date(start_date)
    end = _parse_date(end_date)
    if start is None:
        return [], "开始日期格式无效，请使用 YYYY-MM-DD。"
    if end is None:
        return [], "结束日期格式无效，请使用 YYYY-MM-DD。"
    if start > end:
        return [], "开始日期不能晚于结束日期。"

    files: list[Path] = []
    for file_path in sorted(EXTRACTED_DIR.glob("*.md")):
        file_date = _extract_date_from_name(file_path)
        if file_date is None:
            continue
        if start <= file_date <= end:
            files.append(file_path)
    return files, None


def _run_subprocess(job: WorkflowJob, command: list[str], env: dict[str, str] | None = None, cwd: Path | None = None) -> int:
    _append_log(job, "命令: " + " ".join(command))
    run_env = os.environ.copy()
    if env:
        run_env.update(env)
    run_env.setdefault("PYTHONIOENCODING", "utf-8")
    run_env.setdefault("PYTHONUTF8", "1")
    run_env.setdefault("HF_HUB_DISABLE_PROGRESS_BARS", "1")

    process = subprocess.Popen(
        command,
        cwd=str(cwd) if cwd is not None else None,
        env=run_env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=False,
        bufsize=0,
    )

    def _decode_line(raw: bytes) -> str:
        data = raw.rstrip(b"\r\n")
        if not data:
            return ""
        text = ""
        for encoding in ("utf-8", "utf-8-sig", "gb18030", "cp936"):
            try:
                text = data.decode(encoding)
                break
            except UnicodeDecodeError:
                continue
        if not text:
            text = data.decode("latin-1", errors="replace")
        return _ANSI_ESCAPE_RE.sub("", text)

    try:
        load_report_active = False
        load_report_unexpected = 0
        load_report_lines = 0

        def _flush_load_report_summary() -> None:
            nonlocal load_report_active, load_report_unexpected, load_report_lines
            if not load_report_active:
                return
            _append_log(
                job,
                f"模型加载报告已折叠（UNEXPECTED: {load_report_unexpected}, lines: {load_report_lines}）",
            )
            load_report_active = False
            load_report_unexpected = 0
            load_report_lines = 0

        if process.stdout is not None:
            while True:
                raw = process.stdout.readline()
                if not raw:
                    break
                line = _decode_line(raw)
                if not line:
                    continue

                if load_report_active:
                    load_report_lines += 1
                    if "UNEXPECTED" in line.upper():
                        load_report_unexpected += 1

                    # Next structured block starts (common case: JSON output), flush summary first.
                    if line.lstrip().startswith("{"):
                        _flush_load_report_summary()
                        _append_log(job, line)
                        continue

                    # Guard to avoid swallowing unrelated logs forever.
                    if load_report_lines >= 120:
                        _flush_load_report_summary()
                        _append_log(job, line)
                    continue

                if _LOAD_REPORT_START_RE.search(line):
                    load_report_active = True
                    load_report_lines = 1
                    load_report_unexpected = 1 if "UNEXPECTED" in line.upper() else 0
                    continue

                _append_log(job, line)

        _flush_load_report_summary()
        return process.wait()
    finally:
        try:
            if process.stdout is not None:
                process.stdout.close()
        except Exception:
            pass


def _load_core_config() -> dict[str, Any]:
    if not CORE_CONFIG_PATH.exists():
        return {}
    try:
        data = json.loads(CORE_CONFIG_PATH.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def get_workflow_config() -> dict[str, str]:
    data = _load_core_config()
    api_cfg = data.get("api", {}) if isinstance(data.get("api"), dict) else {}
    base_url = str(api_cfg.get("base_url", os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com")) or "").strip()
    model = str(api_cfg.get("chat_model", os.getenv("DEEPSEEK_MODEL", "deepseek-chat")) or "").strip()
    api_key = str(api_cfg.get("api_key", os.getenv("DEEPSEEK_API_KEY", "")) or "").strip()
    source = "deepseek"
    return {
        "base_url": base_url,
        "model": model,
        "api_key": api_key,
        "source": source,
    }


def save_workflow_config(base_url: str, model: str, api_key: str) -> dict[str, Any]:
    base_url = str(base_url or "").strip()
    model = str(model or "").strip()
    api_key = str(api_key or "").strip()
    if not base_url or not model or not api_key:
        raise ValueError("请完整填写 API_BASE_URL、MODEL、API_KEY。")

    payload = {
        "api": {
            "base_url": base_url,
            "api_key": api_key,
            "chat_model": model,
            "timeout": DEFAULT_TIMEOUT,
        },
        "rag": {
            "embedding_model": (
                os.getenv("LOCAL_EMBEDDING_MODEL", "").strip() or os.getenv("DEEPSEEK_EMBEDDING_MODEL", "").strip()
            ),
        },
        "local_llm": {
            "url": os.getenv("AI_SUMMARY_LOCAL_LLM_URL", "http://127.0.0.1:1234/v1").strip(),
            "model": os.getenv("AI_SUMMARY_LOCAL_LLM_MODEL", "").strip() or _CORE_SETTINGS.local_llm_model,
            "api_key": os.getenv("AI_SUMMARY_LOCAL_LLM_API_KEY", "local").strip() or "local",
        },
    }
    CORE_CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    CORE_CONFIG_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    os.environ["DEEPSEEK_API_KEY"] = api_key
    return {"ok": True, "path": str(CORE_CONFIG_PATH)}


def get_extracted_stats(start_date: str, end_date: str) -> dict[str, Any]:
    total = len(list(EXTRACTED_DIR.glob("*.md")))
    files, err = _collect_extracted_files(start_date, end_date)
    return {
        "total": total,
        "matched": len(files),
        "error": err or "",
        "last_classify_archive_at": _get_last_classify_archive_at(),
    }


def save_uploaded_raw_files(files: list[tuple[str, bytes]]) -> dict[str, Any]:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    saved: list[str] = []
    for file_name, content in files:
        name = Path(str(file_name or "")).name
        if not name:
            continue
        dest = RAW_DIR / name
        if dest.exists():
            stem = dest.stem
            suffix = dest.suffix
            stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            dest = RAW_DIR / f"{stem}_{stamp}{suffix}"
        dest.write_bytes(content)
        saved.append(dest.name)
    return {"ok": True, "saved_count": len(saved), "files": saved}


def _sanitize_markdown_name(raw_name: str) -> str:
    name = str(raw_name or "").strip()
    if not name:
        return ""
    name = name.replace("\\", "/")
    name = name.split("/")[-1]
    name = re.sub(r"\s+", " ", name).strip()
    name = re.sub(r"[^0-9A-Za-z\u4e00-\u9fa5._\- ]+", "", name)
    if not name:
        return ""
    if name.lower().endswith(".md"):
        name = name[:-3].rstrip(" .")
    return name


def create_extracted_markdown(file_name: str, content: str) -> dict[str, Any]:
    EXTRACTED_DIR.mkdir(parents=True, exist_ok=True)
    body = str(content or "")

    base_name = _sanitize_markdown_name(file_name)
    if not base_name:
        base_name = datetime.now().strftime("%Y-%m-%d")

    candidate = EXTRACTED_DIR / f"{base_name}.md"
    if candidate.exists():
        suffix = 2
        while True:
            candidate = EXTRACTED_DIR / f"{base_name}_{suffix}.md"
            if not candidate.exists():
                break
            suffix += 1

    candidate.write_text(body, encoding="utf-8")
    return {
        "ok": True,
        "file_name": candidate.name,
        "path": str(candidate),
    }


def start_job(action: str, payload: dict[str, Any]) -> dict[str, Any]:
    global _ACTIVE_JOB_ID
    with _LOCK:
        if _ACTIVE_JOB_ID and _ACTIVE_JOB_ID in _JOBS and _JOBS[_ACTIVE_JOB_ID].status == "running":
            raise RuntimeError("已有任务在运行，请等待结束。")
        job = WorkflowJob(id=str(uuid4()), action=str(action or "").strip())
        _JOBS[job.id] = job
        _ACTIVE_JOB_ID = job.id

    if job.action == "classify_archive":
        locked_at = _record_last_classify_archive_at()
        _append_log(job, f"分类归档时间已锁存: {locked_at}")

    thread = threading.Thread(target=_run_job_worker, args=(job, payload), daemon=True)
    thread.start()
    return {"job_id": job.id, "status": job.status}


def get_job(job_id: str) -> dict[str, Any]:
    with _LOCK:
        job = _JOBS.get(job_id)
        if not job:
            raise KeyError(job_id)
        return {
            "id": job.id,
            "action": job.action,
            "status": job.status,
            "created_at": job.created_at,
            "updated_at": job.updated_at,
            "logs": list(job.logs),
            "result": dict(job.result),
        }


def get_active_job() -> dict[str, Any] | None:
    with _LOCK:
        if not _ACTIVE_JOB_ID:
            return None
        job = _JOBS.get(_ACTIVE_JOB_ID)
        if not job:
            return None
        return {
            "id": job.id,
            "action": job.action,
            "status": job.status,
            "created_at": job.created_at,
            "updated_at": job.updated_at,
        }


def _run_job_worker(job: WorkflowJob, payload: dict[str, Any]) -> None:
    try:
        action = str(job.action or "").strip()
        if action == "batch_process":
            result = _action_batch_process(job, payload)
        elif action == "ai_summary":
            result = _action_ai_summary(job, payload)
        elif action == "split_topics":
            result = _action_split_topics(job, payload)
        elif action == "classify_archive":
            result = _action_classify_archive(job, payload)
        elif action == "sync_embeddings":
            result = _action_sync_embeddings(job, payload)
        elif action == "estimate_tokens":
            result = _action_estimate_tokens(job, payload)
        else:
            raise RuntimeError(f"不支持的任务类型: {action}")
        _set_job_done(job, "succeeded", result)
    except Exception as exc:  # noqa: BLE001
        _append_log(job, f"任务失败: {exc}")
        _set_job_done(job, "failed", {"error": str(exc)})


def _action_batch_process(job: WorkflowJob, payload: dict[str, Any]) -> dict[str, Any]:
    source = str(payload.get("source", "deepseek") or "").strip().lower()
    script_map = {
        "deepseek": "for_deepseek.py",
        "chatgpt": "for_chatgpt.py",
    }
    script_name = script_map.get(source)
    if not script_name:
        raise RuntimeError(f"当前来源 {source} 暂不支持。")

    python_cmd = _resolve_python_executable()
    command = [
        python_cmd,
        "-u",
        str(SCRIPTS_DIR / script_name),
        "--input-dir",
        str(RAW_DIR),
        "--output-dir",
        str(EXTRACTED_DIR),
    ]
    code = _run_subprocess(job, command, cwd=WORKSPACE_ROOT)
    if code != 0:
        raise RuntimeError(f"格式批处理失败，退出码: {code}")
    return {"ok": True, "source": source}


def _action_ai_summary(job: WorkflowJob, payload: dict[str, Any]) -> dict[str, Any]:
    start_date = str(payload.get("start_date", "") or "").strip()
    end_date = str(payload.get("end_date", "") or "").strip()
    base_url = str(payload.get("base_url", "") or "").strip()
    model = str(payload.get("model", "") or "").strip()
    api_key = str(payload.get("api_key", "") or "").strip()

    if not base_url or not model or not api_key:
        raise RuntimeError("请先完整填写 API_BASE_URL、MODEL、API_KEY。")

    selected_files, range_err = _collect_extracted_files(start_date, end_date)
    if range_err:
        raise RuntimeError(range_err)
    if not selected_files:
        raise RuntimeError("当前日期范围内无可总结文件。")

    temp_dir = tempfile.TemporaryDirectory(prefix="ai_summary_web_filtered_")
    try:
        temp_path = Path(temp_dir.name)
        for file_path in selected_files:
            shutil.copy2(file_path, temp_path / file_path.name)

        env = os.environ.copy()
        env["DEEPSEEK_BASE_URL"] = base_url
        env["DEEPSEEK_MODEL"] = model
        env["DEEPSEEK_API_KEY"] = api_key
        env["PYTHONUNBUFFERED"] = "1"
        env["PYTHONIOENCODING"] = "utf-8"

        command = [
            _resolve_python_executable(),
            "-u",
            str(SCRIPTS_DIR / "summarize.py"),
            "--input-dir",
            str(temp_path),
            "--output-dir",
            str(SUMMARIZE_DIR),
        ]
        _append_log(job, f"日期范围内匹配文件: {len(selected_files)}")
        code = _run_subprocess(job, command, env=env, cwd=WORKSPACE_ROOT)
        if code != 0:
            raise RuntimeError(f"AI 总结失败，退出码: {code}")
        return {"ok": True, "matched_files": len(selected_files)}
    finally:
        temp_dir.cleanup()


def _action_split_topics(job: WorkflowJob, payload: dict[str, Any]) -> dict[str, Any]:
    dry_run = bool(payload.get("dry_run", False))
    command = [
        _resolve_python_executable(),
        "-u",
        str(SCRIPTS_DIR / "batch_split_documents.py"),
        str(SUMMARIZE_DIR),
        "--output-dir",
        str(DATA_DIR / "split_dir"),
        "--move-originals",
        "--no-recursive",
    ]
    if dry_run:
        command.append("--dry-run")
    code = _run_subprocess(job, command, cwd=WORKSPACE_ROOT)
    if code != 0:
        raise RuntimeError(f"拆分主题失败，退出码: {code}")
    return {"ok": True, "dry_run": dry_run, "action": "split_topics"}


def _action_classify_archive(job: WorkflowJob, payload: dict[str, Any]) -> dict[str, Any]:
    dry_run = bool(payload.get("dry_run", False))
    split_dir = DATA_DIR / "split_dir"
    split_dir.mkdir(parents=True, exist_ok=True)

    command_split = [
        _resolve_python_executable(),
        "-u",
        str(SCRIPTS_DIR / "move_summaries_by_category.py"),
        "--input-dir",
        str(split_dir),
        "--documents-dir",
        str(DOCUMENTS_DIR),
    ]
    if dry_run:
        command_split.append("--dry-run")
    code_split = _run_subprocess(job, command_split, cwd=WORKSPACE_ROOT)
    if code_split != 0:
        raise RuntimeError(f"分类归档失败（split_dir），退出码: {code_split}")

    command_summary = [
        _resolve_python_executable(),
        "-u",
        str(SCRIPTS_DIR / "move_summaries_by_category.py"),
        "--input-dir",
        str(SUMMARIZE_DIR),
        "--documents-dir",
        str(DOCUMENTS_DIR),
    ]
    if dry_run:
        command_summary.append("--dry-run")
    code_summary = _run_subprocess(job, command_summary, cwd=WORKSPACE_ROOT)
    if code_summary != 0:
        raise RuntimeError(f"分类归档失败（summarize_dir），退出码: {code_summary}")

    return {"ok": True, "dry_run": dry_run, "action": "classify_archive"}


def _action_sync_embeddings(job: WorkflowJob, payload: dict[str, Any]) -> dict[str, Any]:
    def _load_json(path: Path, default: Any) -> Any:
        if not path.exists():
            return default
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return default

    def _metadata_count() -> int:
        _manifest, docs = _read_vector_metadata_docs()
        if isinstance(docs, list):
            return len(docs)
        return 0

    def _graph_counts() -> tuple[int, int]:
        graph = _load_json(VECTOR_DB_DIR / "knowledge_graph_rag.json", {})
        if not isinstance(graph, dict):
            return 0, 0
        nodes = graph.get("nodes", {})
        edges = graph.get("edges", [])
        return (len(nodes) if isinstance(nodes, dict) else 0, len(edges) if isinstance(edges, list) else 0)

    metadata_before = _metadata_count()
    graph_nodes_before, graph_edges_before = _graph_counts()
    _append_startup_log(f"手动触发 RAG 同步 (job={job.id[:8]}) | 当前索引: {metadata_before} 条")

    model = (
        str(payload.get("embedding_model", "") or "").strip()
        or os.getenv("LOCAL_EMBEDDING_MODEL", "").strip()
        or os.getenv("DEEPSEEK_EMBEDDING_MODEL", "").strip()
        or "BAAI/bge-base-zh-v1.5"
    )
    prune_command = [
        _resolve_python_executable(),
        "-u",
        str(SCRIPTS_DIR / "rag_vector_index.py"),
        "--documents-dir",
        str(DOCUMENTS_DIR),
        "--index-dir",
        str(VECTOR_DB_DIR),
        "--backend",
        "faiss",
        "--prune-stale",
    ]
    _append_startup_log("手动同步: 开始清理失效向量 (prune-stale)")
    prune_code = _run_subprocess(job, prune_command, cwd=WORKSPACE_ROOT)
    if prune_code != 0:
        _append_startup_log(f"手动同步失败: prune-stale 退出码 {prune_code}")
        raise RuntimeError(f"清理失效向量失败，退出码: {prune_code}")
    metadata_after_prune = _metadata_count()
    _append_startup_log(f"手动同步: prune-stale 完成，剩余 {metadata_after_prune} 条")

    command = [
        _resolve_python_executable(),
        "-u",
        str(SCRIPTS_DIR / "rag_vector_index.py"),
        "--documents-dir",
        str(DOCUMENTS_DIR),
        "--index-dir",
        str(VECTOR_DB_DIR),
        "--backend",
        "faiss",
        "--sync-missing",
        "--embedding-model",
        model,
        "--timeout",
        str(DEFAULT_TIMEOUT),
    ]
    _append_startup_log("手动同步: 开始补齐向量 (sync-missing)")
    code = _run_subprocess(job, command, cwd=WORKSPACE_ROOT)
    if code != 0:
        _append_startup_log(f"手动同步失败: sync-missing 退出码 {code}")
        raise RuntimeError(f"补齐向量失败，退出码: {code}")
    metadata_after_sync = _metadata_count()
    _append_startup_log(f"手动同步: sync-missing 完成，新增 {max(0, metadata_after_sync - metadata_after_prune)} 条")

    graph_command = [
        _resolve_python_executable(),
        "-u",
        str(SCRIPTS_DIR / "rag_knowledge_graph.py"),
        "--index-dir",
        str(VECTOR_DB_DIR),
        "--sync-missing",
        "--prune-missing",
        "--no-llm",
    ]
    _append_startup_log("手动同步: 开始补齐知识图谱")
    graph_code = _run_subprocess(job, graph_command, cwd=WORKSPACE_ROOT)
    if graph_code != 0:
        _append_startup_log(f"手动同步失败: graph 退出码 {graph_code}")
        raise RuntimeError(f"补齐知识图谱失败，退出码: {graph_code}")
    graph_nodes_after, graph_edges_after = _graph_counts()

    stale_removed = max(0, metadata_before - metadata_after_prune)
    vectors_added = max(0, metadata_after_sync - metadata_after_prune)
    graph_nodes_added = max(0, graph_nodes_after - graph_nodes_before)
    graph_edges_added = max(0, graph_edges_after - graph_edges_before)
    status_summary = (
        f"执行完成 | stale清理: {stale_removed} | 向量新增: {vectors_added} "
        f"| graph新增: 节点{graph_nodes_added}/边{graph_edges_added}"
    )
    _append_log(job, status_summary)

    # Post-sync verification: re-scan to confirm changed flags are cleared.
    verify = _scan_changed_documents_and_mark_metadata()
    still_changed = len(verify.get("changed_existing", []))
    still_missing = len(verify.get("missing_in_index", []))
    if still_changed == 0 and still_missing == 0:
        _append_startup_log(f"手动同步完成: {status_summary} | 验证通过，0 个文档待重建")
    else:
        _append_startup_log(
            f"手动同步完成: {status_summary} "
            f"| 验证: 仍有 {still_changed} 个变更文档，{still_missing} 个未索引文档"
        )

    return {
        "ok": True,
        "embedding_model": model,
        "graph_synced": True,
        "stale_removed": stale_removed,
        "vectors_added": vectors_added,
        "graph_nodes_added": graph_nodes_added,
        "graph_edges_added": graph_edges_added,
        "status_summary": status_summary,
    }


def _action_estimate_tokens(job: WorkflowJob, payload: dict[str, Any]) -> dict[str, Any]:
    start_date = str(payload.get("start_date", "") or "").strip()
    end_date = str(payload.get("end_date", "") or "").strip()
    files, range_err = _collect_extracted_files(start_date, end_date)
    if range_err:
        raise RuntimeError(range_err)
    if not files:
        raise RuntimeError("当前日期范围内无文件，无法估计 token。")

    try:
        import transformers  # type: ignore
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"Token估计失败：当前解释器缺少 transformers。{exc}") from exc

    tokenizer_dir = SCRIPTS_DIR / "deepseek_v3_tokenizer"
    if not tokenizer_dir.exists():
        raise RuntimeError(f"Token估计失败：未找到目录 {tokenizer_dir}")

    start_ts = time.perf_counter()
    tokenizer = transformers.AutoTokenizer.from_pretrained(str(tokenizer_dir), trust_remote_code=True)
    total_tokens = 0
    total_chars = 0
    for file_path in files:
        text = file_path.read_text(encoding="utf-8", errors="replace")
        total_chars += len(text)
        total_tokens += len(tokenizer.encode(text, add_special_tokens=False))

    elapsed = time.perf_counter() - start_ts
    _append_log(job, f"Token估计完成：文件 {len(files)} 个，字符 {total_chars}，约 {total_tokens} tokens，用时 {elapsed:.1f}s")
    return {
        "ok": True,
        "files": len(files),
        "chars": total_chars,
        "tokens": total_tokens,
        "elapsed_seconds": round(elapsed, 2),
    }


_ensure_auto_reembed_worker_started()
_ensure_startup_worker_started()
