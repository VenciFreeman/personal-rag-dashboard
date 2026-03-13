from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class ExtractProfile:
	name: str
	media_type: str
	default_input: Path
	default_output: Path
	title_col: str
	author_col: str | None
	nationality_col: str | None
	category_col: str | None
	channel_col: str | None


PROFILES: dict[str, ExtractProfile] = {
	"book": ExtractProfile(
		name="book",
		media_type="book",
		default_input=PROJECT_ROOT / "data" / "csv_data" / "阅读.csv",
		default_output=PROJECT_ROOT / "data" / "structured" / "reading.json",
		title_col="书名",
		author_col="作者",
		nationality_col="国籍",
		category_col="类别",
		channel_col="渠道",
	),
	"video": ExtractProfile(
		name="video",
		media_type="video",
		default_input=PROJECT_ROOT / "data" / "csv_data" / "观影.csv",
		default_output=PROJECT_ROOT / "data" / "structured" / "video.json",
		title_col="片名",
		author_col="导演/主创",
		nationality_col="国家",
		category_col="类别",
		channel_col=None,
	),
	"music": ExtractProfile(
		name="music",
		media_type="music",
		default_input=PROJECT_ROOT / "data" / "csv_data" / "音乐.csv",
		default_output=PROJECT_ROOT / "data" / "structured" / "music.json",
		title_col="专辑",
		author_col="艺人",
		nationality_col="国家",
		category_col="流派",
		channel_col=None,
	),
	"game": ExtractProfile(
		name="game",
		media_type="game",
		default_input=PROJECT_ROOT / "data" / "csv_data" / "游戏.csv",
		default_output=PROJECT_ROOT / "data" / "structured" / "game.json",
		title_col="名称",
		author_col=None,
		nationality_col="国家",
		category_col=None,
		channel_col="平台",
	),
}


def _normalize_int(value: str) -> int | None:
	value = (value or "").strip()
	if not value or value == "00":
		return None
	try:
		return int(value)
	except ValueError:
		return None


def _clean_value(row: dict[str, str], column: str | None) -> str | None:
	if not column:
		return None
	return (row.get(column) or "").strip() or None


def _build_date(year: int | None, month: int | None, day: int | None) -> str | None:
	if year is None:
		return None
	mm = month if month is not None else 1
	dd = day if day is not None else 1
	return f"{year:04d}-{mm:02d}-{dd:02d}"


def _row_to_record(row: dict[str, str], profile: ExtractProfile) -> dict[str, Any] | None:
	title = _clean_value(row, profile.title_col)
	if not title:
		return None

	year = _normalize_int(row.get("年", ""))
	month = _normalize_int(row.get("月", ""))
	day = _normalize_int(row.get("日", ""))
	rating = _normalize_int(row.get("评分", ""))

	return {
		"media_type": profile.media_type,
		"date": _build_date(year, month, day),
		"year": year,
		"month": month,
		"day": day,
		"title": title,
		"author": _clean_value(row, profile.author_col),
		"nationality": _clean_value(row, profile.nationality_col),
		"category": _clean_value(row, profile.category_col),
		"channel": _clean_value(row, profile.channel_col),
		"publisher": None,
		"rating": rating,
		"review": _clean_value(row, "评价"),
		"url": None,
		"embedding": None,
	}


def _read_records(csv_path: Path, profile: ExtractProfile) -> list[dict[str, Any]]:
	encodings = ["utf-8-sig", "gb18030"]
	last_error: Exception | None = None
	for encoding in encodings:
		try:
			with csv_path.open("r", encoding=encoding, newline="") as f:
				reader = csv.DictReader(f)
				records: list[dict[str, Any]] = []
				for row in reader:
					if row is None:
						continue
					record = _row_to_record(row, profile)
					if record is not None:
						records.append(record)
				return records
		except UnicodeDecodeError as exc:
			last_error = exc

	raise RuntimeError(f"Cannot decode CSV file: {csv_path}") from last_error


def extract_csv(csv_path: Path, output_path: Path, profile: ExtractProfile) -> int:
	records = _read_records(csv_path, profile)
	output_path.parent.mkdir(parents=True, exist_ok=True)
	payload = {
		"source": str(csv_path),
		"profile": profile.name,
		"record_count": len(records),
		"records": records,
	}
	with output_path.open("w", encoding="utf-8") as f:
		json.dump(payload, f, ensure_ascii=False, indent=2)
	return len(records)


def extract_by_profile(profile_name: str, csv_path: Path | None = None, output_path: Path | None = None) -> int:
	profile = PROFILES[profile_name]
	in_path = csv_path or profile.default_input
	out_path = output_path or profile.default_output
	return extract_csv(in_path, out_path, profile)


def main() -> None:
	parser = argparse.ArgumentParser(description="Extract media CSV into structured JSON.")
	parser.add_argument("--profile", choices=sorted(PROFILES.keys()), default="book", help="Extraction profile to use.")
	parser.add_argument("--input", type=Path, default=None, help="Optional custom input CSV path.")
	parser.add_argument("--output", type=Path, default=None, help="Optional custom output JSON path.")
	args = parser.parse_args()

	profile = PROFILES[args.profile]
	input_path = args.input or profile.default_input
	output_path = args.output or profile.default_output
	count = extract_csv(input_path, output_path, profile)
	print(f"Extracted {count} records for '{args.profile}' to: {output_path}")


if __name__ == "__main__":
	main()
