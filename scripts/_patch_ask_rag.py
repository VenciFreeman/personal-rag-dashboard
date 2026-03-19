"""Patch ask_rag.py: rewrite _ask_llm prompts with 3-tier evidence + retrieval confidence."""
from __future__ import annotations
import pathlib
import sys

RAG = pathlib.Path("ai_conversations_summary/scripts/ask_rag.py")
src = RAG.read_text(encoding="utf-8")

# ── Locate the _ask_llm system_prompt block by unique anchor ─────────────────
# The file has the curly LEFT DOUBLE QUOTATION MARK (U+201C) inside the string,
# followed by literal \n" — so the anchor is the Chinese text.
ANCHOR_ZH = "\u4f60\u662f\u4e00\u4e2a\u77e5\u8bc6\u52a9\u624b\uff0c\u56de\u7b54\u539f\u5219"  # 你是一个知识助手，回答原则
anchor_pos = src.find(ANCHOR_ZH)
if anchor_pos < 0:
    print("ERROR: anchor '你是一个知识助手，回答原则' not found", file=sys.stderr)
    sys.exit(1)

# The block starts at 'system_prompt = (' before the anchor
sys_start = src.rfind("system_prompt = (", 0, anchor_pos)
if sys_start < 0:
    print("ERROR: system_prompt start not found", file=sys.stderr)
    sys.exit(1)

# The sys_prompt block ends at '    )\n' — find the first one after sys_start
sys_end_marker = "\n    )\n"
sys_end_pos = src.index(sys_end_marker, anchor_pos) + len(sys_end_marker)

old_sys_block = src[sys_start:sys_end_pos]
print(f"sys_block chars {sys_start}-{sys_end_pos}, len={len(old_sys_block)}")

NEW_SYS = (
    "system_prompt = (\n"
    "        \"\u4f60\u662f\u4e00\u4e2a\u77e5\u8bc6\u52a9\u624b\u3002\u56de\u7b54\u65f6\u533a\u5206\u4e09\u7c7b\u8bc1\u636e\u6765\u6e90\uff1a\\n\"\n"
    "        \"\u2460 \u672c\u5730\u8d44\u6599\u4e8b\u5b9e \u2014 \u76f4\u63a5\u9648\u8ff0\uff0c\u53ef\u7528\u300c[\u8d44\u6599N]\u300d\u8f7b\u5ea6\u6807\u6ce8\u6765\u6e90\u7f16\u53f7\\n\"\n"
    "        \"\u2461 \u901a\u7528\u77e5\u8bc6\u8865\u5145 \u2014 \u884c\u5185\u6216\u672b\u5c3e\u6807\u6ce8\u300c[\u901a\u7528\u77e5\u8bc6]\u300d\\n\"\n"
    "        \"\u2462 \u63a8\u65ad/\u5916\u63a8 \u2014 \u6807\u6ce8\u300c[\u63a8\u65ad]\u300d\uff0c\u8bed\u6c14\u4fdd\u5b88\\n\"\n"
    "        \"\u4e25\u7981\u6df7\u6dc6\u4e09\u7c7b\u6765\u6e90\u6216\u7f16\u9020\u8d44\u6599\u4e2d\u4e0d\u5b58\u5728\u7684\u4e8b\u5b9e\u3002\\n\"\n"
    "        f\"\u5f53\u524d\u8c03\u7528\u7c7b\u578b call_type={call_type or 'answer'}\u3002\\n\\n\"\n"
    "        \"4) \u5982\u679c\u8d44\u6599\u65e0\u6cd5\u56de\u7b54\uff0c\u5e94\u660e\u786e\u8bf4\u660e\\n\"\n"
    "        \"5) \u8f93\u51fa\u4f7f\u7528Markdown\\n\"\n"
    "        \"6) \u9ed8\u8ba4\u7ed9\u51fa\u66f4\u5b8c\u6574\u7684\u89e3\u91ca\uff1b\u4f18\u5148\u5305\u542b\u7ed3\u8bba\u3001\u539f\u56e0\u673a\u5236\u3001\u5f71\u54cd\u6216\u5c40\u9650\u3002\\n\"\n"
    "    )\n"
)
src = src[:sys_start] + NEW_SYS + src[sys_end_pos:]
print("system_prompt replaced OK")

# ── Locate the '# Build user prompt' comment + both user_prompt blocks ────────
BUILD_ANCHOR = "    # Build user prompt based on whether we have context or not."
build_pos = src.find(BUILD_ANCHOR)
if build_pos < 0:
    print("ERROR: '# Build user prompt' comment not found", file=sys.stderr)
    sys.exit(1)

# The block ends after the closing ')' of the else user_prompt
# Find "        )\n" after a known closing marker "不要编造不存在的细节"
no_fabricate = "\u4e0d\u8981\u7f16\u9020\u4e0d\u5b58\u5728\u7684\u7ec6\u8282\u3002\\n\\n\"\n        )\n"  # 不要编造不存在的细节
block_end_pos = src.find(no_fabricate, build_pos) + len(no_fabricate)
if block_end_pos < len(no_fabricate):
    print("ERROR: block end not found", file=sys.stderr)
    sys.exit(1)

old_block = src[build_pos:block_end_pos]
print(f"user_prompt block chars {build_pos}-{block_end_pos}, {len(old_block)} chars")

NEW_BLOCK = (
    "    memory_block = (memory_context or \"\").strip()\n"
    "    memory_section = f\"\\n\u4f1a\u8bdd\u8bb0\u5fc6(\u53ef\u4e3a\u7a7a):\\n{memory_block}\\n\" if memory_block else \"\"\n"
    "\n"
    "    # Retrieval confidence note \u2014 injected when results are weak or absent\n"
    "    _conf = (retrieval_confidence or \"\").strip().lower()\n"
    "    if _conf == \"none\":\n"
    "        confidence_note = \"\u68c0\u7d22\u7ed3\u679c\uff1a\u672c\u5730\u77e5\u8bc6\u5e93\u4e2d\u672a\u627e\u5230\u76f8\u5173\u8d44\u6599\uff0c\u8bf7\u4e3b\u8981\u4f9d\u8d56\u901a\u7528\u77e5\u8bc6\uff08\u987b\u6807\u6ce8\u300c[\u901a\u7528\u77e5\u8bc6]\u300d\uff09\u3002\\n\"\n"
    "    elif _conf == \"weak\":\n"
    "        confidence_note = \"\u68c0\u7d22\u7f6e\u4fe1\u5ea6\uff1a\u5f31\uff08\u76f8\u5173\u6027\u504f\u4f4e\uff09\uff0c\u8d44\u6599\u4ec5\u4f9b\u53c2\u8003\uff0c\u8bf7\u9002\u5f53\u964d\u4f4e\u786e\u5b9a\u6027\uff0c\u8865\u5145\u901a\u7528\u77e5\u8bc6\u987b\u6807\u6ce8\u300c[\u901a\u7528\u77e5\u8bc6]\u300d\u3002\\n\"\n"
    "    else:\n"
    "        confidence_note = \"\"\n"
    "\n"
    "    if context_text.strip():\n"
    "        user_prompt = (\n"
    "            \"\u8bf7\u5148\u9605\u8bfb\u4e0b\u9762\u7684\u672c\u5730\u68c0\u7d22\u8d44\u6599\uff0c\u518d\u56de\u7b54\u7528\u6237\u95ee\u9898\u3002\\n\"\n"
    "            \"\u4f1a\u8bdd\u8bb0\u5fc6\uff1a\\n\"\n"
    "            f\"{memory_section}\"\n"
    "            f\"{confidence_note}\"\n"
    "            \"\u68c0\u7d22\u8d44\u6599\uff1a\\n\"\n"
    "            f\"\u8d44\u6599:\\n{context_text}\\n\\n\"\n"
    "            \"\u7528\u6237\u95ee\u9898\uff1a\\n\"\n"
    "            f\"\u95ee\u9898:\\n{question}\\n\"\n"
    "            \"\u56de\u7b54\u8981\u6c42\uff1a\\n\"\n"
    "            \"1) \u5982\u679c\u95ee\u9898\u590d\u6742\uff0c\u5141\u8bb8\u8fdb\u884c\u7b80\u77ed\u601d\u8003\uff08\u4e0d\u8d85\u8fc7400\u5b57\uff09\uff0c\u7528<think>\u601d\u8003\u5185\u5bb9</think>\u6807\u7b7e\u5305\u88f9\uff1b\\n\"\n"
    "            \"2) \u57fa\u4e8e\u8d44\u6599\u548c\u4f1a\u8bdd\u8bb0\u5fc6\uff08\u5982\u679c\u4e0d\u4e3a\u7a7a\uff09\u7ed9\u51fa\u6700\u7ec8\u7b54\u6848\uff1b\\n\"\n"
    "            \"3) \u8d44\u6599\u4e0d\u8db3\u6216\u6709\u5fc5\u8981\u65f6\u53ef\u7528\u901a\u7528\u77e5\u8bc6\u8865\u5145\uff0c\u987b\u6807\u6ce8\u300c[\u901a\u7528\u77e5\u8bc6]\u300d\uff1b\\n\"\n"
    "            \"4) \u9ed8\u8ba4\u81f3\u5c11\u5199\u6210 3 \u4e2a\u8981\u70b9\u6216 2 \u6bb5\u4ee5\u4e0a\uff0c\u8bf4\u660e\u80cc\u666f\u3001\u5173\u952e\u673a\u5236\u3001\u5b9e\u9645\u5f71\u54cd\uff1b\\n\"\n"
    "            \"5) \u4e0d\u8981\u7f16\u9020\u4e0d\u5b58\u5728\u7684\u7ec6\u8282\uff1b\\n\"\n"
    "            \"6) \u53ef\u7528\u300c[\u8d44\u6599N]\u300d\u683c\u5f0f\u8f7b\u5ea6\u5f15\u7528\u6765\u6e90\uff0c\u672b\u5c3e\u4e0d\u5fc5\u91cd\u590d\u5b8c\u6574\u8d44\u6599\u5217\u8868\u3002\\n\\n\"\n"
    "        )\n"
    "    else:\n"
    "        user_prompt = (\n"
    "            \"\u4f1a\u8bdd\u8bb0\u5fc6\uff1a\\n\"\n"
    "            f\"{memory_section}\"\n"
    "            f\"{confidence_note}\"\n"
    "            \"\u7528\u6237\u95ee\u9898\uff1a\\n\"\n"
    "            f\"\u95ee\u9898:\\n{question}\\n\"\n"
    "            \"\u672c\u5730\u77e5\u8bc6\u5e93\u4e2d\u672a\u627e\u5230\u4e0e\u95ee\u9898\u9ad8\u5ea6\u76f8\u5173\u7684\u8d44\u6599\u3002\\n\"\n"
    "            \"\u8bf7\u57fa\u4e8e\u901a\u7528\u77e5\u8bc6\u56de\u7b54\u4e0b\u9762\u95ee\u9898\uff0c\u5e76\u660e\u786e\u6807\u6ce8\u300c[\u901a\u7528\u77e5\u8bc6]\u300d\uff0c\u533a\u5206\u5df2\u77e5\u4e8b\u5b9e\u4e0e\u63a8\u65ad\uff08\u6807\u300c[\u63a8\u65ad]\u300d\uff09\u3002\\n\"\n"
    "            \"\u56de\u7b54\u8981\u6c42\uff1a\\n\"\n"
    "            \"1) \u660e\u786e\u8bf4\u660e\u672a\u627e\u5230\u76f8\u5173\u672c\u5730\u8d44\u6599\uff1b\\n\"\n"
    "            \"2) \u57fa\u4e8e\u901a\u7528\u77e5\u8bc6\u7ed9\u51fa\u5c3d\u91cf\u5b8c\u6574\u7684\u5206\u6790\uff0c\u4e0d\u8981\u53ea\u7ed9\u7ed3\u8bba\uff1b\\n\"\n"
    "            \"3) \u9ed8\u8ba4\u81f3\u5c11\u5199\u6210 3 \u4e2a\u8981\u70b9\uff0c\u8986\u76d6\u80cc\u666f\u3001\u539f\u56e0\u673a\u5236\u3001\u5f71\u54cd\u6216\u5efa\u8bae\uff1b\\n\"\n"
    "            \"4) \u4e0d\u8981\u7f16\u9020\u4e0d\u5b58\u5728\u7684\u7ec6\u8282\u3002\\n\\n\"\n"
    "        )\n"
)
src = src[:build_pos] + NEW_BLOCK + src[block_end_pos:]
print("user_prompt block replaced OK")

RAG.write_text(src, encoding="utf-8")
print("File written — total chars:", len(src))
