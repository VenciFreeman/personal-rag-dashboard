from scripts.csv_extract import PROFILES, extract_by_profile


if __name__ == "__main__":
    count = extract_by_profile("music")
    print(f"Extracted {count} records to: {PROFILES['music'].default_output}")
