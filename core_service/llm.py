from __future__ import annotations

from typing import Any, Iterator

from .llm_client import chat_completion
from .llm_client import chat_completion_with_retry
from .llm_client import create_client as create_llm_client
from .llm_client import stream_chat_completion_text


def request_text(
    *,
    api_key: str,
    base_url: str,
    model: str,
    timeout: int,
    messages: list[dict[str, str]],
    temperature: float = 0.2,
    max_tokens: int | None = None,
) -> str:
    return chat_completion(
        api_key=api_key,
        base_url=base_url,
        model=model,
        timeout=timeout,
        messages=messages,
        temperature=temperature,
        max_tokens=max_tokens,
    )


def request_text_with_retry(
    *,
    api_key: str,
    base_url: str,
    model: str,
    timeout: int,
    messages: list[dict[str, str]],
    temperature: float = 0.2,
    max_retries: int = 3,
    retry_delay: float = 2.0,
    max_tokens: int | None = None,
) -> str:
    return chat_completion_with_retry(
        api_key=api_key,
        base_url=base_url,
        model=model,
        timeout=timeout,
        messages=messages,
        temperature=temperature,
        max_retries=max_retries,
        retry_delay=retry_delay,
        max_tokens=max_tokens,
    )


def stream_text(
    *,
    api_key: str,
    base_url: str,
    model: str,
    timeout: int,
    messages: list[dict[str, str]],
    temperature: float = 0.2,
    max_tokens: int | None = None,
) -> Iterator[str]:
    return stream_chat_completion_text(
        api_key=api_key,
        base_url=base_url,
        model=model,
        timeout=timeout,
        messages=messages,
        temperature=temperature,
        max_tokens=max_tokens,
    )


__all__ = [
    "chat_completion",
    "chat_completion_with_retry",
    "create_llm_client",
    "request_text",
    "request_text_with_retry",
    "stream_chat_completion_text",
    "stream_text",
]