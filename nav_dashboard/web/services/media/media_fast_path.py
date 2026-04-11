from __future__ import annotations

from typing import Any, Callable

from .media_answer_renderer import MediaAnswerRenderDeps


def build_media_answer_render_deps(
    *,
    clip_text: Callable[[Any, int], str],
) -> MediaAnswerRenderDeps:
    return MediaAnswerRenderDeps(clip_text=clip_text)
