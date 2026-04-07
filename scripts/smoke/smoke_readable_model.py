import re

_RE = re.compile(r"[-_](?:GGUF|GPTQ|AWQ|EXL2|EXL|MLX).*$", re.IGNORECASE)

def _raw(v):
    v = (v or "").strip()
    if not v: return ""
    n = v.replace("\\", "/")
    if "/models--" in n:
        tail = n.split("/models--", 1)[1]
        repo = tail.split("/", 1)[0].replace("--", "/")
        name = repo.split("/", 1)[-1].strip()
        if name: return name
    parts = [p for p in n.split("/") if p]
    if "snapshots" in parts:
        idx = parts.index("snapshots")
        if idx >= 1 and parts[idx - 1]: return parts[idx - 1]
    if len(parts) >= 2 and parts[-2] in {"local_models", "models"}: return parts[-1]
    if "/" in v: return parts[-1] if parts else v
    return v

def readable(v):
    raw = _raw(v)
    if not raw: return raw
    s = _RE.sub("", raw).strip()
    return s or raw

cases = [
    ("unsloth/Qwen3.5-4B-GGUF-no-thinking", "Qwen3.5-4B"),
    ("qwen2.5-7b-instruct", "qwen2.5-7b-instruct"),
    ("Qwen2.5-7B-Instruct", "Qwen2.5-7B-Instruct"),
    ("unsloth/Qwen2.5-7B", "Qwen2.5-7B"),
    ("C:/models/Qwen2.5-7B-GPTQ-INT4", "Qwen2.5-7B"),
]
ok = True
for inp, expected in cases:
    got = readable(inp)
    status = "OK" if got == expected else "FAIL"
    if status == "FAIL": ok = False
    print(f"{status}  {inp!r} -> {got!r}  (expected {expected!r})")
raise SystemExit(0 if ok else 1)
