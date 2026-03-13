from scripts.csv_extract import PROFILES, extract_by_profile


if __name__ == "__main__":
    count = extract_by_profile("game")
    print(f"Extracted {count} records to: {PROFILES['game'].default_output}")
