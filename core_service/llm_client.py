from __future__ import annotations

import time
from urllib.parse import urlparse
from typing import Any, Iterator


def _extract_text_fragments(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, (int, float, bool)):
        return [str(value)]
    if isinstance(value, list):
        parts: list[str] = []
        for item in value:
            parts.extend(_extract_text_fragments(item))
        return parts
    if isinstance(value, dict):
        parts: list[str] = []
        for key in ("text", "content", "value", "output_text", "reasoning_content"):
            if key in value:
                parts.extend(_extract_text_fragments(value.get(key)))
        return parts

    parts = []
    for attr in ("text", "content", "value", "output_text", "reasoning_content"):
        attr_value = getattr(value, attr, None)
        if attr_value is not None:
            parts.extend(_extract_text_fragments(attr_value))
    return parts


def _collapse_text(value: Any) -> str:
    return "".join(part for part in _extract_text_fragments(value) if str(part))


def _extract_message_text(message: Any) -> str:
    for candidate in (
        getattr(message, "content", None),
        getattr(message, "output_text", None),
        getattr(message, "text", None),
        getattr(message, "reasoning_content", None),
    ):
        text = _collapse_text(candidate).strip()
        if text:
            return text
    return ""


def _extract_stream_text(choice: Any) -> str:
    delta = getattr(choice, "delta", None)
    for candidate in (
        getattr(delta, "content", None) if delta is not None else None,
        getattr(delta, "text", None) if delta is not None else None,
        getattr(delta, "output_text", None) if delta is not None else None,
        getattr(delta, "reasoning_content", None) if delta is not None else None,
        getattr(choice, "text", None),
    ):
        text = _collapse_text(candidate)
        if text:
            return text
    return ""


def _openai_types() -> tuple[type[Exception], type[Exception], type[Exception], type[Exception], Any]:
    try:
        from openai import APIConnectionError, APIStatusError, APITimeoutError, OpenAI, RateLimitError
    except ModuleNotFoundError as exc:
        raise RuntimeError("Missing dependency: openai. Install with: pip install openai") from exc
    return APIConnectionError, APIStatusError, APITimeoutError, RateLimitError, OpenAI


def create_client(*, api_key: str, base_url: str, timeout: int) -> Any:
    _APIConnectionError, _APIStatusError, _APITimeoutError, _RateLimitError, OpenAI = _openai_types()
    host = (urlparse(base_url).hostname or "").strip().lower()
    is_local = host in {"127.0.0.1", "localhost", "::1"}
    if is_local:
        try:
            import httpx

            # Local inference endpoints should never go through corporate/system proxy.
            http_client = httpx.Client(trust_env=False, timeout=timeout)
            return OpenAI(api_key=api_key, base_url=base_url, timeout=timeout, http_client=http_client)
        except Exception:
            # Fallback to default client if httpx client customization is unavailable.
            pass
    return OpenAI(api_key=api_key, base_url=base_url, timeout=timeout)


def chat_completion(
    *,
    api_key: str,
    base_url: str,
    model: str,
    timeout: int,
    messages: list[dict[str, str]],
    temperature: float = 0.2,
    max_tokens: int | None = None,
) -> str:
    client = create_client(api_key=api_key, base_url=base_url, timeout=timeout)
    kwargs: dict[str, Any] = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
        "stream": False,
    }
    if max_tokens is not None:
        kwargs["max_tokens"] = max_tokens

    response = client.chat.completions.create(**kwargs)
    if not response.choices or not response.choices[0].message:
        raise RuntimeError("LLM response is empty")
    text = _extract_message_text(response.choices[0].message).strip()
    if not text:
        raise RuntimeError("LLM response text is empty")
    return text


def _is_retryable_empty_response_error(exc: Exception) -> bool:
    message = str(exc or "").strip()
    return message in {"LLM response is empty", "LLM response text is empty"}


def chat_completion_with_retry(
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
    APIConnectionError, APIStatusError, APITimeoutError, RateLimitError, _OpenAI = _openai_types()

    attempt = 0
    max_attempts = max(1, int(max_retries) + 1)
    last_error: Exception | None = None
    while attempt < max_attempts:
        attempt += 1
        try:
            return chat_completion(
                api_key=api_key,
                base_url=base_url,
                model=model,
                timeout=timeout,
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens,
            )
        except (APIConnectionError, APITimeoutError, RateLimitError) as exc:
            last_error = exc
            if attempt >= max_attempts:
                break
            time.sleep(float(retry_delay) * (2 ** (attempt - 1)))
        except APIStatusError as exc:
            last_error = exc
            status_code = getattr(exc, "status_code", None)
            if status_code is not None and int(status_code) >= 500 and attempt < max_attempts:
                time.sleep(float(retry_delay) * (2 ** (attempt - 1)))
                continue
            raise
        except RuntimeError as exc:
            if not _is_retryable_empty_response_error(exc):
                raise
            last_error = exc
            if attempt >= max_attempts:
                break
            time.sleep(float(retry_delay) * (2 ** (attempt - 1)))

    if last_error is not None:
        raise RuntimeError(f"LLM request failed after {max_attempts} attempts") from last_error
    raise RuntimeError("LLM request failed for unknown reason")


def stream_chat_completion_text(
    *,
    api_key: str,
    base_url: str,
    model: str,
    timeout: int,
    messages: list[dict[str, str]],
    temperature: float = 0.2,
    max_tokens: int | None = None,
) -> Iterator[str]:
    client = create_client(api_key=api_key, base_url=base_url, timeout=timeout)
    kwargs: dict[str, Any] = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
        "stream": True,
    }
    if max_tokens is not None:
        kwargs["max_tokens"] = max_tokens
    response_stream = client.chat.completions.create(
        **kwargs,
    )

    for chunk in response_stream:
        if not chunk.choices:
            continue
        text = _extract_stream_text(chunk.choices[0])
        if text:
            yield text
