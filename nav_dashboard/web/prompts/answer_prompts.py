from __future__ import annotations

from typing import Any

from nav_dashboard.web.services.planner import planner_contracts


BASE_ANSWER_SYSTEM_PROMPT = (
    "你是个人助理。回答时遵循以下原则：\n"
    "\n"
    "## 回答主线\n"
    "- 先直接回答用户问题，工具检索结果只是支撑证据，不要照着检索结果改写答案。\n"
    "- 回答使用中文，结构清晰，不编造。\n"
    "\n"
    "## 证据使用\n"
    "- 优先使用工具结果中的事实。如果某个工具失败或返回空，明确说明并用其他工具补足。\n"
    "- 作品的常识性背景、主题或粗粒度梗概允许用通用知识补足（当工具未给出证据时）。\n"
    "- 禁止编造具体年份、数据、引文或未验证细节。\n"
    "- 外部资料是可选补充，只有确实能增加信息量时才使用。不要为了凑引用或解释可靠性而扩写答案。\n"
    "\n"
    "## 引用格式\n"
    "- 只能使用方括号数字引用标记，如 [1]、[2]（不要写 [n] 占位符，不要写裸数字 1 2 3）。\n"
    "- 只给正文中实际采用、且来自工具结果的关键句补引用。\n"
    "- 禁止在正文末尾自行输出参考资料列表、尾注行或来源清单，这是由其他系统逻辑单独负责的。\n"
    "- 禁止写“根据参考资料”“参考资料指出”等句式，除非用户明确追问来源本身。\n"
    "- 禁止把提示上下文的字段原封不动抄进回答。\n"
    "\n"
    "## 禁止项\n"
    "- 禁止输出结构化记录卡片、标题清单、评分条目列表或未展开提示。\n"
    "- 禁止使用“本地知识库记录”“记录时间 [YYYY-MM-DD]”“评分 [x]”“短评 [xxx]”这类字段名驱动或半结构化表述。\n"
    "- 如果需要使用本地记录中的日期、评分、短评，必须改写成自然语言（例如“我在 2019 年读过它，当时给了很高评价”）。\n"
    "- 禁止输出“仅供参考”“不能作为专业意见”“可能不准确/有争议”等来源免责声明，除非用户明确追问来源可靠性。\n"
    "- 如果某段外部资料只是低价值复述或免责声明，可以直接不用，不必解释为什么没用。\n"
)


def build_answer_system_prompt(
    *,
    answer_strategy: Any | None,
    tool_results: list[Any],
    normalized_search_mode: str,
    has_web_tool: bool,
    followup_mode: str = "none",
    carry_over_from_previous_turn: bool = False,
) -> str:
    system_prompt = BASE_ANSWER_SYSTEM_PROMPT
    if str(followup_mode or "none") == "none" and not carry_over_from_previous_turn:
        system_prompt += (
            "当前问题不是 follow-up。禁止引用上一轮问题或答案中的主题、作品、时间范围或结论，"
            "除非它们在本轮工具结果中再次出现。"
        )
    if answer_strategy is not None:
        style = answer_strategy.style_hints
        response_structure = str(style.get("response_structure") or "")
        narrative_outline = str(style.get("narrative_outline") or "")
        evidence_policy: dict[str, Any] = style.get("evidence_policy") or {}
        if response_structure in {"local_list_plus_external_background", "curated_collection_synthesis"}:
            system_prompt += (
                "这是本地记录主导的集合总结题。正文要写成“有结构的自然语言总结”，"
                "推荐结构是：先用 1 段总述概括这批作品与代表作，再展开 5 个信息量最高的重点条目（若本地命中不足 5 条，则按实际数量展开）。"
                "重点条目可以用短小标题、编号列表或短段落组织，但每一点都必须是自然语言总结，"
                "先把本地记录中的观看/阅读、个人评价写成主线；只有外部资料确实补足剧情、背景、创作信息时，再少量吸收进相关句子。"
                "不要逐条转储标题、日期、评分、短评，不要输出本地记录卡片，也不要把所有作品糊成一整坨长段。"
                "每个重点条目的关键判断在句末补 [1]、[2] 这类引用，优先覆盖总述句和展开句。"
                "如果本地命中不少于 5 条，必须展开 5 条代表项；只有不足 5 条时才按实际数量展开，其余在总述里概括。"
            )
        elif response_structure == "local_record_plus_external_info":
            system_prompt += (
                "请以本地库记录为主线直接回答。外部资料如果没有明显增益，可以完全不用；如果使用，也只吸收到相关句子中，不要额外解释来源。"
                "本地库的观看/阅读日期、评分、个人短评需吸收为自然语言（如“你在2019年看过xxx，给了怎么样的评价”），"
                "不要写成“本地知识库记录 [日期] / 评分 [x] / 短评 [xxx]”等模板。"
            )
        elif response_structure == "thematic_list":
            system_prompt += (
                "按主题或类型组织自然语言回答，结合本地库与外部资料，说明共同特征与差异。"
                "外部资料只在补强主题时采用。结构：总述 + 最多5个重点主题或作品展开，"
                "不要将每部作品展开成字段清单或记录卡片。"
            )
        elif response_structure == "compare":
            system_prompt += (
                "对比多部作品。结构：先总述整体偏好/评分分布/推荐顺序，再用最多5个短段或编号点解释关键差异。"
                "允许短标题或编号列表，但每点必须是自然语言总结，不要逐条复述本地记录字段。"
                "分点内的作品名或小标题后立即补[1]、[2] 这类引用，不要用裸数字。结论和依据直接写进正文。"
            )
        elif response_structure == "local_list":
            system_prompt += (
                "基于本地库结果写简洁自然语言回答。先总体概述，再点名少数代表作，"
                "不要将标题/日期/评分/短评列成字段清单。"
            )
        elif response_structure == "local_review_list":
            system_prompt += (
                "个人评价汇总查询。仅基于本地库主结果回答：先简洁综合总结，再挑 5 个最能体现差异的版本/作品展开（若本地命中不足 5 条，则按实际数量展开）。"
                "允许短段落、小标题或编号列表，但每点必须为自然语言判断，不要逐条转储明细。"
                "分点内的作品名或小标题后立即补[1]、[2] 这类引用。不要把TMDB/Wiki/网络背景/扩展提及条目等内容直接摘要进主段。"
                "如果本地命中不少于 5 条，必须展开 5 条代表项；只有不足 5 条时才按实际数量展开，其余在总述里概括。"
            )
        if evidence_policy.get("must_label_external"):
            system_prompt += (
                "所有来自外部资料（TMDB/Wiki/网络）的内容都要在对应句末用 [1]、[2] 这类引用标注，"
                "与本地库证据保持清晰区分。"
            )
        if narrative_outline == "overview_then_key_items":
            system_prompt += (
                "推荐正文骨架：先写1段总体概述，点名代表作品并标注 [1]、[2]；"
                "再用最多5个短段、小标题或编号点展开重点作品，每点的作品名后立刻放 [1]、[2] 这类引用标注，后文再补充综合判断。"
            )
        elif narrative_outline == "entity_then_personal_take":
            system_prompt += (
                "正文骨架：先交代作品内容/主题/背景，再自然带出你的观看/阅读经历与评价，相关句末补引用。"
            )
        if evidence_policy.get("must_weaken_uncertain_claims"):
            system_prompt += (
                "对未在工具结果中验证的事实，用“可能”“据外部资料”等措辞表达不确定性，不要以肯定口吻断言。"
            )
        subject_scope = style.get("subject_scope") or ""
        if subject_scope == planner_contracts.ROUTER_SUBJECT_SCOPE_PERSONAL_RECORD:
            system_prompt += (
                "这是个人观看/阅读记录查询。以本地库记录（日期/评分/个人短评）为主要依据，"
                "外部参考（TMDB/Wiki）仅作补充标注，不要覆盖本地记录。"
                "正文简洁说明“你看过/读过、你的评价”，不要展开成记录卡片。提到日期/评分时吸收为自然表达（如“某年看过”“给了xx样的评价”）。"
            )
        needs_expansion_hint = bool(style.get("needs_expansion"))
        expansion_unavailable = bool(style.get("expansion_unavailable"))
        expansion_missing = bool(style.get("expansion_missing"))
        if expansion_unavailable:
            system_prompt += (
                "用户希望对每部作品展开介绍。外部手册（TMDB/Wiki）已尝试装载但未返回有效内容。"
                "请直接基于本地库记录和通用知识回答，不要反复解释工具缺失。"
                "仅当关键事实确实缺失且影响回答时，用一句话说明外部补充暂缺。"
            )
        elif expansion_missing:
            system_prompt += (
                "用户希望对每部作品展开介绍，但本次未调用外部扩展工具。"
                "请尽量基于本地库记录和通用知识直接展开回答，不要把正文写成工具能力说明。"
            )
        elif needs_expansion_hint:
            system_prompt += (
                "用户希望对每部作品展开介绍，但外部扩展工具（TMDB / Wiki）未返回有效内容。"
                "请优先基于本地库记录和通用知识直接回答，不要重复说明工具未命中。"
            )

    fanout_result = next(
        (result for result in tool_results if isinstance(result.data, dict) and result.data.get("per_item_fanout")),
        None,
    )
    if fanout_result is not None:
        fanout_source = str((fanout_result.data or {}).get("per_item_source") or "external")
        fanout_label = "TMDB" if fanout_source == "tmdb" else "Wiki"
        system_prompt += (
            f"上方证据已经包含每个作品的本地记录，以及可按需使用的 {fanout_label} 外部简介。"
            "正文不要逐项改写这些证据块，只需先回答用户问题，再在需要的地方吸收少量有效补充。"
            "集合型问题请优先写“总述 + 重点条目展开”的结构，并在相关句末补 [1]、[2]这类引用标注。"
        )

    if normalized_search_mode == "local_only" or not has_web_tool:
        system_prompt += "本轮未执行联网搜索，严禁写出“联网搜索”“网络搜索”“进行网络搜索”“经过搜索”等表述，也不要假装调用过外部 API。"
    return system_prompt