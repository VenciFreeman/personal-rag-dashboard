from csv_extract import PROFILES, extract_by_profile


if __name__ == "__main__":
    count = extract_by_profile("video")
    print(f"Extracted {count} records to: {PROFILES['video'].default_output}")
