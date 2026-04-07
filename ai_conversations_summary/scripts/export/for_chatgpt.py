"""Extract conversation content from ChatGPT export packages.

Supported inputs under raw_dir:
- ChatGPT export zip (preferred, reads conversations*.json shards)
- standalone conversations*.json
- chat.html with embedded `jsonData`
"""

import argparse
import json
import re
import sys
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any


WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
if str(WORKSPACE_ROOT) not in sys.path:
	sys.path.insert(0, str(WORKSPACE_ROOT))

from ai_conversations_summary.runtime_paths import EXTRACTED_DIR, RAW_DIR


DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")
UNKNOWN_DATE = "unknown-date"
HTML_JSONDATA_RE = re.compile(r"var\s+jsonData\s*=\s*(\[.*?\]);", re.DOTALL)


def extract_date(value: Any) -> str:
	if value is None:
		return ""
	text = str(value)
	matched = DATE_RE.search(text)
	return matched.group(0) if matched else text


def unix_to_date_text(value: Any) -> str:
	if value is None:
		return ""
	try:
		ts = float(value)
		if ts <= 0:
			return ""
		return datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
	except Exception:
		return ""


def _flatten_text(value: Any) -> list[str]:
	if value is None:
		return []
	if isinstance(value, str):
		text = value.strip()
		return [text] if text else []
	if isinstance(value, (int, float, bool)):
		return [str(value)]
	if isinstance(value, list):
		parts: list[str] = []
		for item in value:
			parts.extend(_flatten_text(item))
		return parts
	if isinstance(value, dict):
		parts: list[str] = []
		for key in ("parts", "text", "result", "content", "value"):
			if key in value:
				parts.extend(_flatten_text(value.get(key)))
		if parts:
			return parts
		for child in value.values():
			parts.extend(_flatten_text(child))
		return parts
	return []


def _extract_message_text(message: dict[str, Any]) -> str:
	content = message.get("content")
	parts = _flatten_text(content)
	text = "\n".join(p for p in parts if p.strip()).strip()
	return text


def _extract_messages(conversation: dict[str, Any]) -> list[str]:
	mapping = conversation.get("mapping")
	if not isinstance(mapping, dict):
		return []

	items: list[tuple[float, int, str]] = []
	order = 0
	for node in mapping.values():
		if not isinstance(node, dict):
			continue
		message = node.get("message")
		if not isinstance(message, dict):
			continue

		author = message.get("author")
		role = "unknown"
		if isinstance(author, dict):
			role = str(author.get("role") or "unknown").strip() or "unknown"

		text = _extract_message_text(message)
		if not text:
			continue

		create_time = message.get("create_time")
		try:
			sort_key = float(create_time)
		except Exception:
			sort_key = float("inf")

		items.append((sort_key, order, f"[{role}]\n{text}"))
		order += 1

	items.sort(key=lambda x: (x[0], x[1]))
	return [x[2] for x in items]


def iter_conversations(data: Any):
	if isinstance(data, list):
		for item in data:
			if isinstance(item, dict) and ("mapping" in item or "id" in item):
				yield item
		return

	if isinstance(data, dict):
		if "mapping" in data or "id" in data:
			yield data
			return
		for value in data.values():
			if isinstance(value, list):
				for item in value:
					if isinstance(item, dict) and ("mapping" in item or "id" in item):
						yield item


def _conversation_date(conversation: dict[str, Any]) -> str:
	for key in ("update_time", "create_time"):
		date_from_unix = unix_to_date_text(conversation.get(key))
		if date_from_unix:
			return date_from_unix
		date_from_text = extract_date(conversation.get(key))
		if date_from_text:
			return date_from_text
	return ""


def format_conversation(conversation: dict[str, Any]) -> str:
	date_text = _conversation_date(conversation)
	messages = _extract_messages(conversation)

	lines = [f"updated_at: {date_text}"]
	for idx, msg in enumerate(messages, start=1):
		lines.append(f"\n## content {idx}\n{msg}")
	return "\n".join(lines)


def get_next_index(output_dir: Path, date_slug: str, counters: dict[str, int]) -> int:
	if date_slug not in counters:
		max_existing = 0
		pattern = re.compile(rf"^{re.escape(date_slug)}_(\d+)$")
		for existing in output_dir.glob(f"{date_slug}_*.md"):
			matched = pattern.match(existing.stem)
			if matched:
				max_existing = max(max_existing, int(matched.group(1)))
		counters[date_slug] = max_existing

	counters[date_slug] += 1
	return counters[date_slug]


def build_output_path(output_dir: Path, conversation: dict[str, Any], counters: dict[str, int]) -> Path:
	date_text = _conversation_date(conversation)
	date_slug = date_text if DATE_RE.fullmatch(date_text) else UNKNOWN_DATE

	while True:
		next_index = get_next_index(output_dir, date_slug, counters)
		candidate = output_dir / f"{date_slug}_{next_index:03d}.md"
		if not candidate.exists():
			return candidate


def write_conversation_markdown(output_path: Path, conversation: dict[str, Any]) -> None:
	content = format_conversation(conversation).strip()
	with output_path.open("w", encoding="utf-8") as f:
		f.write(content)
		if content:
			f.write("\n")


def load_from_html(html_text: str) -> Any:
	match = HTML_JSONDATA_RE.search(html_text)
	if not match:
		raise ValueError("No jsonData array found in chat.html")
	return json.loads(match.group(1))


def load_conversation_payloads_from_zip(zip_path: Path) -> list[tuple[str, Any]]:
	payloads: list[tuple[str, Any]] = []
	with zipfile.ZipFile(zip_path) as zf:
		names = zf.namelist()
		json_entries = sorted(
			[
				n for n in names
				if re.search(r"(^|/)conversations(?:-\d+)?\.json$", n)
			],
		)

		for name in json_entries:
			with zf.open(name) as fp:
				text = fp.read().decode("utf-8", errors="ignore")
				payloads.append((f"{zip_path.name}:{name}", json.loads(text)))

		if not payloads:
			html_entry = next((n for n in names if n.endswith("chat.html")), "")
			if html_entry:
				with zf.open(html_entry) as fp:
					html_text = fp.read().decode("utf-8", errors="ignore")
					payloads.append((f"{zip_path.name}:{html_entry}", load_from_html(html_text)))

	return payloads


def load_payloads(input_dir: Path) -> list[tuple[str, Any]]:
	payloads: list[tuple[str, Any]] = []

	for zip_file in sorted(input_dir.glob("*.zip")):
		try:
			payloads.extend(load_conversation_payloads_from_zip(zip_file))
		except Exception as exc:  # noqa: BLE001
			print(f"Skipped zip: {zip_file.name} ({exc})")

	for json_file in sorted(input_dir.glob("conversations*.json")):
		try:
			text = json_file.read_text(encoding="utf-8", errors="ignore")
			if not text.strip():
				continue
			payloads.append((json_file.name, json.loads(text)))
		except Exception as exc:  # noqa: BLE001
			print(f"Skipped json: {json_file.name} ({exc})")

	for html_file in sorted(input_dir.glob("chat*.html")):
		try:
			html_text = html_file.read_text(encoding="utf-8", errors="ignore")
			if not html_text.strip():
				continue
			payloads.append((html_file.name, load_from_html(html_text)))
		except Exception as exc:  # noqa: BLE001
			print(f"Skipped html: {html_file.name} ({exc})")

	return payloads


def main() -> None:
	parser = argparse.ArgumentParser(
		description="Extract ChatGPT export data into markdown files (updated_at + content blocks).",
	)
	parser.add_argument(
		"--input-dir",
		default=str(RAW_DIR),
		help="Directory containing ChatGPT export files (zip/json/html).",
	)
	parser.add_argument(
		"--output-dir",
		default=str(EXTRACTED_DIR),
		help="Directory for extracted markdown files.",
	)
	args = parser.parse_args()

	input_dir = Path(args.input_dir)
	output_dir = Path(args.output_dir)
	if not input_dir.exists():
		raise FileNotFoundError(f"Input directory not found: {input_dir}")

	payloads = load_payloads(input_dir)
	if not payloads:
		print(f"No ChatGPT payloads found in: {input_dir}")
		return

	output_dir.mkdir(parents=True, exist_ok=True)
	date_counters: dict[str, int] = {}
	seen_ids: set[str] = set()
	written_count = 0

	for source_name, payload in payloads:
		conversations = list(iter_conversations(payload))
		source_written = 0
		for conversation in conversations:
			conv_id = str(conversation.get("id") or conversation.get("conversation_id") or "")
			if conv_id and conv_id in seen_ids:
				continue
			if conv_id:
				seen_ids.add(conv_id)

			if not _extract_messages(conversation):
				continue

			output_path = build_output_path(output_dir, conversation, date_counters)
			write_conversation_markdown(output_path, conversation)
			written_count += 1
			source_written += 1

		print(f"Loaded: {source_name} (sessions: {len(conversations)}, written: {source_written})")

	print(f"Written markdown files: {written_count}")


if __name__ == "__main__":
	main()
