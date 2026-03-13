"""Batch summarization pipeline for extracted conversation documents.

This script reads normalized source text, calls chat completion API,
and writes markdown summaries into `data/summarize_dir`.
"""

import argparse
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path


WORKSPACE_ROOT = Path(__file__).resolve().parent.parent.parent
if str(WORKSPACE_ROOT) not in sys.path:
	sys.path.insert(0, str(WORKSPACE_ROOT))

from api_config import API_BASE_URL, API_KEY, MODEL, TIMEOUT
try:
	from core_service.llm_client import chat_completion_with_retry
except ModuleNotFoundError:
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
	) -> str:
		try:
			from openai import APIConnectionError, APIStatusError, APITimeoutError, OpenAI, RateLimitError
		except ModuleNotFoundError as exc:
			raise RuntimeError("Missing dependency: openai. Install it with: pip install openai") from exc

		client = OpenAI(api_key=api_key, base_url=base_url, timeout=timeout)
		attempt = 0
		max_attempts = max(1, int(max_retries) + 1)
		last_error: Exception | None = None
		while attempt < max_attempts:
			attempt += 1
			try:
				response = client.chat.completions.create(
					model=model,
					messages=messages,
					stream=False,
					temperature=temperature,
				)
				if response.choices and response.choices[0].message and response.choices[0].message.content:
					return response.choices[0].message.content.strip()
				raise RuntimeError("DeepSeek API response did not include text content")
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

		if last_error is not None:
			raise RuntimeError(f"DeepSeek request failed after {max_attempts} attempts") from last_error
		raise RuntimeError("DeepSeek request failed for unknown reason")


INVALID_FILENAME_CHARS_RE = re.compile(r'[<>:"/\\|?*\x00-\x1F]')
MULTI_SPACE_RE = re.compile(r"\s+")
FENCE_RE = re.compile(r"^```(?:markdown|md)?\s*\n([\s\S]*?)\n```\s*$", re.IGNORECASE)
TITLE_LINE_RE = re.compile(r'^\s*>\s*-\s*\*\*title\*\*:\s*"?(.+?)"?\s*$', re.IGNORECASE)
FRONTMATTER_TITLE_RE = re.compile(r'^\s*title:\s*"?(.+?)"?\s*$', re.IGNORECASE)
HEADING_RE = re.compile(r'^\s*#\s+(.+?)\s*$')
UPDATED_AT_LINE_RE = re.compile(r'^\s*updated_at\s*:\s*(.+?)\s*$', re.IGNORECASE)
DATE_RE = re.compile(r'(\d{4})-(\d{2})-(\d{2})')
FILENAME_YYMMDD_RE = re.compile(r'^(\d{6})(?:[_-]|$)')
FILENAME_YYYYMMDD_RE = re.compile(r'^(\d{8})(?:[_-]|$)')
FILENAME_YYYY_MM_DD_RE = re.compile(r'^(\d{4})[-_](\d{2})[-_](\d{2})(?:[-_]\d{4,6})?$')


def configure_stdio_utf8() -> None:
	# Prevent UnicodeEncodeError on Windows default code pages (e.g. cp1252).
	for stream_name in ("stdout", "stderr"):
		stream = getattr(sys, stream_name, None)
		if stream is None:
			continue
		reconfigure = getattr(stream, "reconfigure", None)
		if callable(reconfigure):
			try:
				reconfigure(encoding="utf-8", errors="replace")
			except Exception:
				pass


def parse_args() -> argparse.Namespace:
	# Keep parameters CLI-friendly so GUI and terminal use the same behavior.
	script_dir = Path(__file__).resolve().parent
	root_dir = script_dir.parent
	data_dir = root_dir / "data"

	parser = argparse.ArgumentParser(
		description="Summarize extracted documents with a fixed prompt via DeepSeek API."
	)
	parser.add_argument(
		"--input-dir",
		default=str(data_dir / "extracted_dir"),
		help="Input directory that contains extracted source files.",
	)
	parser.add_argument(
		"--output-dir",
		default=str(data_dir / "summarize_dir"),
		help="Output directory for markdown summary files.",
	)
	parser.add_argument(
		"--prompt-file",
		default=str(root_dir / "prompt.md"),
		help="Path to fixed prompt text file.",
	)
	parser.add_argument(
		"--api-url",
		default=os.getenv("DEEPSEEK_BASE_URL", API_BASE_URL),
		help="DeepSeek base URL (OpenAI-compatible).",
	)
	parser.add_argument(
		"--api-key",
		default=os.getenv("DEEPSEEK_API_KEY", API_KEY),
		help="API key. If omitted, DEEPSEEK_API_KEY environment variable is used.",
	)
	parser.add_argument(
		"--model",
		default=os.getenv("DEEPSEEK_MODEL", MODEL),
		help="Model name used in DeepSeek chat completion request.",
	)
	parser.add_argument(
		"--timeout",
		type=int,
		default=TIMEOUT,
		help="HTTP timeout in seconds.",
	)
	parser.add_argument(
		"--max-retries",
		type=int,
		default=int(os.getenv("DEEPSEEK_MAX_RETRIES", "3")),
		help="Retries for transient API/network failures (default: 3).",
	)
	parser.add_argument(
		"--retry-delay",
		type=float,
		default=float(os.getenv("DEEPSEEK_RETRY_DELAY", "2")),
		help="Initial retry delay seconds for exponential backoff (default: 2).",
	)
	return parser.parse_args()


def read_text_file(path: Path) -> str:
	return path.read_text(encoding="utf-8").strip()


def iter_input_files(input_dir: Path):
	for file_path in sorted(input_dir.iterdir()):
		if file_path.is_file():
			yield file_path


def request_chat_completion(
	*,
	base_url: str,
	api_key: str,
	model: str,
	system_prompt: str,
	user_content: str,
	timeout: int,
	max_retries: int,
	retry_delay: float,
) -> str:
	return chat_completion_with_retry(
		api_key=api_key,
		base_url=base_url,
		model=model,
		timeout=timeout,
		messages=[
			{"role": "system", "content": system_prompt},
			{"role": "user", "content": user_content},
		],
		temperature=0.2,
		max_retries=max_retries,
		retry_delay=retry_delay,
	)


def normalize_markdown(md_text: str) -> str:
	# Some providers wrap markdown in fenced blocks; strip them for clean file output.
	text = md_text.strip()
	matched = FENCE_RE.match(text)
	if matched:
		return matched.group(1).strip()
	return text


def extract_title(markdown_text: str) -> str:
	lines = markdown_text.splitlines()

	# 1) Prefer title in the metadata quote block expected by prompt.md.
	for line in lines:
		m = TITLE_LINE_RE.match(line)
		if m:
			return m.group(1).strip().strip('"')

	# 2) Fallback to frontmatter style title within the first section.
	for line in lines[:20]:
		m = FRONTMATTER_TITLE_RE.match(line)
		if m:
			return m.group(1).strip().strip('"')

	# 3) Final fallback to first markdown heading.
	for line in lines:
		m = HEADING_RE.match(line)
		if m:
			return m.group(1).strip()

	return "chat-ai-summary"


def sanitize_filename(title: str) -> str:
	# Keep filename Windows-safe and human-readable.
	cleaned = INVALID_FILENAME_CHARS_RE.sub("", title).strip().strip(".")
	cleaned = MULTI_SPACE_RE.sub(" ", cleaned)
	return cleaned if cleaned else "chat-ai-summary"


def _extract_date_prefix_from_filename(file_path: Path) -> str:
	stem = file_path.stem.strip()
	if not stem:
		return ""

	m6 = FILENAME_YYMMDD_RE.match(stem)
	if m6:
		return m6.group(1)

	m8 = FILENAME_YYYYMMDD_RE.match(stem)
	if m8:
		yyyymmdd = m8.group(1)
		return f"{yyyymmdd[2:4]}{yyyymmdd[4:6]}{yyyymmdd[6:8]}"

	m_dash = FILENAME_YYYY_MM_DD_RE.match(stem)
	if m_dash:
		yyyy, mm, dd = m_dash.groups()
		return f"{yyyy[2:]}{mm}{dd}"

	return ""


def extract_date_prefix_from_source(file_path: Path, source_text: str) -> str:
	# Prefer source filename date (manual corrections), fallback to `updated_at`, then today.
	from_name = _extract_date_prefix_from_filename(file_path)
	if from_name:
		return from_name

	for line in source_text.splitlines()[:30]:
		m = UPDATED_AT_LINE_RE.match(line)
		if not m:
			continue

		raw_value = m.group(1).strip().strip('"').strip("'")
		date_match = DATE_RE.search(raw_value)
		if date_match:
			yyyy, mm, dd = date_match.groups()
			return f"{yyyy[2:]}{mm}{dd}"

	return datetime.now().strftime("%y%m%d")


def build_output_path(output_dir: Path, title: str, date_prefix: str) -> Path:
	safe_title = sanitize_filename(title)
	base_name = f"{date_prefix}_{safe_title}"

	candidate = output_dir / f"{base_name}.md"
	if not candidate.exists():
		return candidate

	index = 2
	# Avoid overwrite when the same title appears multiple times in one day.
	while True:
		candidate = output_dir / f"{base_name}_{index}.md"
		if not candidate.exists():
			return candidate
		index += 1


def write_failure_log(log_path: Path, failed_items: list[tuple[str, str]]) -> None:
	if not failed_items:
		if log_path.exists():
			log_path.unlink()
		return

	lines = [
		"# summarize.py failed items",
		f"generated_at: {datetime.now().isoformat(timespec='seconds')}",
		f"failed_count: {len(failed_items)}",
		"",
	]
	for file_name, reason in failed_items:
		lines.append(f"- file: {file_name}")
		lines.append(f"  reason: {reason}")

	with log_path.open("w", encoding="utf-8", newline="\n") as f:
		f.write("\n".join(lines) + "\n")


def main() -> None:
	# Batch mode: continue on per-file failure and report a consolidated failure log.
	configure_stdio_utf8()
	args = parse_args()

	input_dir = Path(args.input_dir)
	output_dir = Path(args.output_dir)
	prompt_file = Path(args.prompt_file)

	if not input_dir.exists():
		raise FileNotFoundError(f"Input directory not found: {input_dir}")
	if not prompt_file.exists():
		raise FileNotFoundError(f"Prompt file not found: {prompt_file}")
	if not args.api_key.strip():
		raise ValueError(
			"Missing API key. Fill scripts/api_config.py API_KEY, "
			"or set DEEPSEEK_API_KEY, or pass --api-key."
		)

	system_prompt = read_text_file(prompt_file)
	if not system_prompt:
		raise ValueError(f"Prompt file is empty: {prompt_file}")

	files = list(iter_input_files(input_dir))
	if not files:
		print(f"No input files found in: {input_dir}")
		return

	output_dir.mkdir(parents=True, exist_ok=True)

	success_count = 0
	failed_items: list[tuple[str, str]] = []
	failure_log_path = Path(__file__).resolve().parent / "summarize_failed.log"
	for file_path in files:
		# Each input file is summarized independently and written as one markdown output.
		source_text = read_text_file(file_path)
		if not source_text:
			print(f"Skipped empty file: {file_path.name}")
			continue
		date_prefix = extract_date_prefix_from_source(file_path, source_text)

		print(f"Summarizing: {file_path.name}")
		try:
			result_markdown = request_chat_completion(
				base_url=args.api_url,
				api_key=args.api_key,
				model=args.model,
				system_prompt=system_prompt,
				user_content=source_text,
				timeout=args.timeout,
				max_retries=args.max_retries,
				retry_delay=args.retry_delay,
			)
		except Exception as exc:  # noqa: BLE001
			print(f"Failed: {file_path.name} ({exc})")
			failed_items.append((file_path.name, str(exc)))
			continue
		result_markdown = normalize_markdown(result_markdown)

		title = extract_title(result_markdown)
		output_path = build_output_path(output_dir, title, date_prefix)
		output_path.write_text(result_markdown + "\n", encoding="utf-8")
		success_count += 1
		print(f"Written: {output_path.name}")

	write_failure_log(failure_log_path, failed_items)
	if failed_items:
		print(f"Failed items log: {failure_log_path}")
		print(f"Completed. Written summaries: {success_count}, Failed: {len(failed_items)}")
	else:
		print("Completed. Written summaries: {0}, Failed: 0".format(success_count))


if __name__ == "__main__":
	main()
