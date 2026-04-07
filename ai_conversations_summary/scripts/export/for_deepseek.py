"""Extract `updated_at` and `content` fields from raw DeepSeek export files.

The parser is intentionally tolerant to support:
- standard JSON object/array files,
- JSONL / NDJSON,
- concatenated JSON payloads.
"""

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List


WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
if str(WORKSPACE_ROOT) not in sys.path:
	sys.path.insert(0, str(WORKSPACE_ROOT))

from ai_conversations_summary.runtime_paths import EXTRACTED_DIR, RAW_DIR


DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")
UNKNOWN_DATE = "unknown-date"


def load_json_flexible(input_path: Path) -> Any:
	# Load JSON with permissive fallbacks to maximize recoverability.
	# Supported input shapes:
	#  1) Standard JSON document (object or array)
	#  2) NDJSON / JSONL (one object per line)
	#  3) Concatenated JSON values without separators

	text = input_path.read_text(encoding="utf-8").lstrip("\ufeff")
	if not text.strip():
		return []

	# 1) Standard JSON document.
	try:
		return json.loads(text)
	except json.JSONDecodeError:
		pass

	# 2) JSON Lines / NDJSON (one JSON object per line).
	lines = [line.strip() for line in text.splitlines() if line.strip()]
	if lines:
		ndjson_items: List[Any] = []
		ndjson_ok = True
		for line in lines:
			candidate = line.rstrip(",")
			try:
				ndjson_items.append(json.loads(candidate))
			except json.JSONDecodeError:
				ndjson_ok = False
				break
		if ndjson_ok:
			return ndjson_items

	# 3) Concatenated JSON objects without separators.
	decoder = json.JSONDecoder()
	idx = 0
	n = len(text)
	items: List[Any] = []
	while idx < n:
		while idx < n and text[idx].isspace():
			idx += 1
		while idx < n and text[idx] in ",":
			idx += 1
		while idx < n and text[idx].isspace():
			idx += 1
		if idx >= n:
			break
		try:
			obj, end = decoder.raw_decode(text, idx)
			items.append(obj)
			idx = end
		except json.JSONDecodeError:
			# Skip to the next likely JSON boundary instead of failing immediately,
			# so partially malformed files can still yield recoverable records.
			next_obj = text.find("{", idx + 1)
			next_arr = text.find("[", idx + 1)
			candidates = [p for p in (next_obj, next_arr) if p != -1]
			if not candidates:
				break
			idx = min(candidates)

	if not items:
		raise ValueError(f"Could not parse JSON content: {input_path}")

	return items


def extract_date(value: Any) -> str:
	# Extract YYYY-MM-DD from timestamp-like input; fallback to raw text.
	text = "" if value is None else str(value)
	match = DATE_RE.search(text)
	return match.group(0) if match else text


def flatten_text(value: Any) -> List[str]:
	# Flatten nested JSON content into a clean list of text fragments.
	# This function is defensive because `content` fields can appear in many
	# structures (string, list, dict, mixed nested objects).
	if value is None:
		return []
	if isinstance(value, str):
		text = value.strip()
		return [text] if text else []
	if isinstance(value, (int, float, bool)):
		return [str(value)]
	if isinstance(value, list):
		texts: List[str] = []
		for item in value:
			texts.extend(flatten_text(item))
		return texts
	if isinstance(value, dict):
		texts: List[str] = []
		preferred_keys = ["parts", "text", "value"]

		for key in preferred_keys:
			if key in value:
				texts.extend(flatten_text(value[key]))

		# Fallback for non-standard structures: recursively collect anything
		# that can be flattened to text.
		if not texts:
			for child in value.values():
				texts.extend(flatten_text(child))
		return texts
	return []


def collect_content_fields(node: Any) -> List[str]:
	# Recursively collect all values under keys named `content` (case-insensitive).
	results: List[str] = []

	def walk(value: Any) -> None:
		if isinstance(value, dict):
			for key, child in value.items():
				if isinstance(key, str) and key.lower() == "content":
					results.extend(flatten_text(child))
				walk(child)
		elif isinstance(value, list):
			for item in value:
				walk(item)

	walk(node)

	# Keep first occurrence order while removing exact duplicates.
	unique_results: List[str] = []
	seen = set()
	for item in results:
		if item not in seen:
			seen.add(item)
			unique_results.append(item)
	return unique_results


def iter_conversations(data: Any) -> Iterable[Dict[str, Any]]:
	# Yield conversation-like dicts that contain an `updated_at` key.
	if isinstance(data, list):
		for item in data:
			if isinstance(item, dict) and "updated_at" in item:
				yield item
		return

	if isinstance(data, dict):
		if "updated_at" in data:
			yield data
			return

		# Common wrapper shapes, e.g. {"conversations": [...]}.
		for value in data.values():
			if isinstance(value, list):
				for item in value:
					if isinstance(item, dict) and "updated_at" in item:
						yield item


def format_conversation(conversation: Dict[str, Any]) -> str:
	# Format one conversation block for markdown output.
	date_text = extract_date(conversation.get("updated_at", ""))
	contents = collect_content_fields(conversation)

	lines = [f"updated_at: {date_text}"]
	for index, content in enumerate(contents, start=1):
		lines.append(f"\n## content {index}\n{content}")
	return "\n".join(lines)


def get_next_index(output_dir: Path, date_slug: str, counters: Dict[str, int]) -> int:
	# Get the next available index for files named YYYY-MM-DD_NNN.md.
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


def build_output_path(output_dir: Path, conversation: Dict[str, Any], counters: Dict[str, int]) -> Path:
	date_text = extract_date(conversation.get("updated_at", ""))
	date_slug = date_text if DATE_RE.fullmatch(date_text) else UNKNOWN_DATE

	while True:
		next_index = get_next_index(output_dir, date_slug, counters)
		candidate = output_dir / f"{date_slug}_{next_index:03d}.md"
		if not candidate.exists():
			return candidate


def write_conversation_markdown(output_path: Path, conversation: Dict[str, Any]) -> None:
	output_text = format_conversation(conversation).strip()
	with output_path.open("w", encoding="utf-8") as f:
		f.write(output_text)
		if output_text:
			f.write("\n")


def main() -> None:
	# One source file may contain multiple conversation objects.
	parser = argparse.ArgumentParser(
		description="Extract updated_at and content fields from chat metadata JSON files."
	)
	parser.add_argument(
		"--input-dir",
		default=str(RAW_DIR),
		help="Directory containing source files (default: data/raw_dir).",
	)
	parser.add_argument(
		"--output-dir",
		default=str(EXTRACTED_DIR),
		help="Directory for output markdown files (default: data/extracted_dir).",
	)
	args = parser.parse_args()

	input_dir = Path(args.input_dir)
	output_dir = Path(args.output_dir)

	if not input_dir.exists():
		raise FileNotFoundError(f"Input directory not found: {input_dir}")

	json_files = sorted(input_dir.glob("*.json"))
	if not json_files:
		print(f"No JSON files found in: {input_dir}")
		return

	output_dir.mkdir(parents=True, exist_ok=True)
	date_counters: Dict[str, int] = {}
	written_count = 0
	for json_file in json_files:
		try:
			data = load_json_flexible(json_file)
		except ValueError as exc:
			print(f"Skipped: {json_file.name} ({exc})")
			continue
		conversations = list(iter_conversations(data))
		for conversation in conversations:
			output_path = build_output_path(output_dir, conversation, date_counters)
			write_conversation_markdown(output_path, conversation)
			written_count += 1
		print(f"Loaded: {json_file.name} (sessions: {len(conversations)})")

	print(f"Written markdown files: {written_count}")


if __name__ == "__main__":
	main()
