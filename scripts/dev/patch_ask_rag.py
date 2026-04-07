"""Patch ask_rag.py: rewrite _ask_llm prompts with 3-tier evidence + retrieval confidence.

This patcher intentionally scopes edits to the `_ask_llm` function body so it
won't accidentally touch other helper prompts in the same file.
"""
from __future__ import annotations

import pathlib
import re
import sys
import textwrap

ROOT = pathlib.Path(__file__).resolve().parents[2]
RAG = ROOT / "ai_conversations_summary" / "scripts" / "ask_rag.py"
src = RAG.read_text(encoding="utf-8")


def _slice_ask_llm_block(text: str) -> tuple[int, int, str]:
    start = text.find("def _ask_llm(")
    if start < 0:
        raise RuntimeError("ERROR: _ask_llm definition not found")
    end = text.find("\ndef ", start + 1)
    if end < 0:
        end = len(text)
    return start, end, text[start:end]


def _replace_once(block: str, pattern: str, replacement: str, *, label: str) -> str:
    updated, count = re.subn(pattern, lambda _m: replacement, block, count=1, flags=re.DOTALL)
    if count != 1:
        raise RuntimeError(f"ERROR: expected one match for {label}, got {count}")
    return updated


def _indent_snippet(snippet: str, *, spaces: int = 4) -> str:
    cleaned = textwrap.dedent(snippet).strip("\n") + "\n"
    return textwrap.indent(cleaned, " " * spaces)


func_start, func_end, func_block = _slice_ask_llm_block(src)
print(f"_ask_llm chars {func_start}-{func_end}, len={len(func_block)}")

NEW_SYS = _indent_snippet(
    """
    system_prompt = (
        "你是一个专业知识助手。基于你的通用知识，结合以下三类补充证据来源回答：\n"
        "1. 本地资料（检索返回的片段） — 直接陈述事实，句末标注「[资料k]」（k为片段序号，从1开始）\n"
        "2. 通用知识（你的内置知识）\n"
        "3. 合理推断 — 语气委婉（如“可能是”“推测”）\n"
        "禁止混淆三类来源，禁止编造资料中不存在的内容。\n"
        "引用方式：先综合归纳多个资料，再在该句末加「[资料k]」；"
        "不要写成“根据资料1说……”“资料2指出……”这种逐条复述。\n"
        f"当前调用类型 call_type={call_type or 'answer'}。\n\n"
        "- 如果资料无法回答，应优先使用通用知识补充并声明\n"
        "- 输出格式为Markdown（标题、列表、引用、加粗）。尽量给出完整解释，包含结论、原因、影响或局限。\n"
    )

    """
)
func_block = _replace_once(
    func_block,
    r"\n\s*system_prompt = \(\n(?:.*?\n)*?\s*\)\n",
    "\n" + NEW_SYS,
    label="system_prompt block",
)
print("system_prompt replaced OK")

NEW_BLOCK = _indent_snippet(
    """
    memory_block = (memory_context or "").strip()
    memory_section = f"\\n会话记忆(可为空):\\n{memory_block}\\n" if memory_block else ""

    # Retrieval confidence note — injected when results are weak or absent
    _conf = (retrieval_confidence or "").strip().lower()
    if _conf == "none":
        confidence_note = "检索结果：本地知识库中未找到相关资料，请主要依赖通用知识（须标注「[通用知识]」）。\\n"
    elif _conf == "weak":
        confidence_note = "检索置信度：弱（相关性偏低），资料仅供参考，请适当降低确定性，补充通用知识须标注「[通用知识]」。\\n"
    else:
        confidence_note = ""

    if context_text.strip():
        user_prompt = (
            "请阅读下面的检索资料和会话记忆，再回答用户问题。\n"
            f"{memory_section}"
            f"{confidence_note}"
            "## 检索资料\n"
            f"{context_text}\n\n"
            "## 用户问题\n"
            f"{question}\n\n"
            "## 回答要求\n"
            "1) 如果问题复杂，先用<think>包裹简短思考（≤200 tokens），再输出最终答案。\n"
            "2) 基于资料和记忆回答，不足时用通用知识补充并标注「[通用]」。\n"
            "3) 答案至少包含3个要点或2段，说明背景、机制、影响。\n"
            "4) 引用格式：综合后句末加「[资料k]」，不要写成“根据资料N说”。\n"
            "5) 禁止编造细节。\n"
        )
    else:
        user_prompt = (
            "会话记忆：\n"
            f"{memory_section}"
            f"{confidence_note}"
            "## 用户问题\n"
            f"{question}\n\n"
            "## 回答要求\n"
            "1) 先用<think>思考（≤200 tokens），再输出答案。\n"
            "2) 依赖通用知识回答，标注「[通用]」。\n"
            "3) 答案至少3个要点或2段。\n"
            "4) 不要编造信息。\n"
        )
    """
)
func_block = _replace_once(
    func_block,
    r"\n\s*memory_block = \(memory_context or \"\"\)\.strip\(\)\n(?:.*?\n)*?\s*else:\n\s*user_prompt = \(\n(?:.*?\n)*?\s*\)\n",
    "\n" + NEW_BLOCK,
    label="user_prompt if/else block",
)
print("user_prompt block replaced OK")

src = src[:func_start] + func_block + src[func_end:]

if "call_type={call_type or 'answer'}" not in func_block:
    raise RuntimeError("ERROR: post-check failed for system_prompt")
if "retrieval_confidence" not in func_block:
    raise RuntimeError("ERROR: post-check failed for retrieval confidence block")

RAG.write_text(src, encoding="utf-8")
print("File written — total chars:", len(src))
