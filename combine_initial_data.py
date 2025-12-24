#!/usr/bin/env python3
"""
One-time script to combine existing CSV exports into the listings-feed-store format.
Run this once to initialize the data/ folder with combined Phoenix and Tucson CSVs.
"""

import csv
import os
from pathlib import Path

# Source files (from Google Sheets export)
PHOENIX_ALL_IN_ONE = Path("/Users/jordannielsen/Desktop/Community/MLS Listings - Phoenix All in One.csv")
PHOENIX_ARCHIVE = Path("/Users/jordannielsen/Desktop/Community/MLS Listings - Phoenix ARCHIVE.csv")
TUCSON_ALL_IN_ONE = Path("/Users/jordannielsen/Desktop/Community/MLS Listings - Tucson All in One (1).csv")
TUCSON_ARCHIVE = Path("/Users/jordannielsen/Desktop/Community/MLS Listings - Tucson ARCHIVE.csv")

# Output files
OUTPUT_DIR = Path("/Users/jordannielsen/Desktop/Community/listings-feed-store/data")
PHOENIX_OUTPUT = OUTPUT_DIR / "phoenix_listings.csv"
TUCSON_OUTPUT = OUTPUT_DIR / "tucson_listings.csv"

# Standard headers (normalized)
STANDARD_HEADERS = [
    'timestamp', 'mls_number', 'price', 'listing_address', 'status',
    'agent_name', 'agent_first_name', 'agent_phone', 'agent_email', 'agent_website',
    'office_name', 'office_phone', 'office_email', 'office_website',
    'formatted_address', 'image_filename',
    'exif_artist', 'exif_copyright', 'exif_make', 'exif_model',
    'exif_lens_model', 'exif_body_serial_number', 'exif_date_time_digitized',
    'scraped_image_filename', 'lp_flag', 'cleaned', 'preferred_photographer'
]

# Header mappings
HEADER_MAP = {
    'timestamp': 'timestamp',
    'date': 'timestamp',
    'date_time': 'timestamp',
    'what is': 'timestamp',
    'mls number': 'mls_number',
    'price': 'price',
    'listing address': 'listing_address',
    'status': 'status',
    'agent name': 'agent_name',
    'agent first name': 'agent_first_name',
    'agent phone': 'agent_phone',
    'agent email': 'agent_email',
    'agent website': 'agent_website',
    'office name': 'office_name',
    'office phone': 'office_phone',
    'office email': 'office_email',
    'office website': 'office_website',
    'formatted address': 'formatted_address',
    'image filename': 'image_filename',
    'exif artist': 'exif_artist',
    'exif copyright': 'exif_copyright',
    'exif make': 'exif_make',
    'exif model': 'exif_model',
    'exif lens model': 'exif_lens_model',
    'exif body serial number': 'exif_body_serial_number',
    'exif date time digitized': 'exif_date_time_digitized',
    'scraped image filename': 'scraped_image_filename',
    'lp?': 'lp_flag',
    'cleaned': 'cleaned',
    'preferred photographer': 'preferred_photographer',
}


def normalize_header(header: str) -> str:
    """Convert CSV header to standard field name."""
    normalized = header.lower().strip()
    return HEADER_MAP.get(normalized, normalized.replace(' ', '_'))


def normalize_mls(mls: str) -> str:
    """Normalize MLS number."""
    if not mls:
        return ''
    mls = str(mls).strip()
    if mls.endswith('.0'):
        mls = mls[:-2]
    return mls


def read_and_normalize(filepath: Path) -> list:
    """Read CSV and normalize to standard format."""
    rows = []

    if not filepath.exists():
        print(f"  Warning: {filepath} not found")
        return rows

    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        reader = csv.DictReader(f)
        header_mapping = {h: normalize_header(h) for h in reader.fieldnames or []}

        for row in reader:
            normalized = {}
            for original_header, value in row.items():
                standard_field = header_mapping.get(original_header, original_header)
                normalized[standard_field] = value.strip() if value else ''

            # Normalize MLS
            normalized['mls_number'] = normalize_mls(normalized.get('mls_number', ''))

            # Skip empty MLS
            if not normalized.get('mls_number'):
                continue

            rows.append(normalized)

    return rows


def dedupe_by_mls(rows: list) -> list:
    """Keep only one row per MLS number (most recent)."""
    seen = {}
    for row in rows:
        mls = row.get('mls_number', '')
        if not mls:
            continue

        if mls not in seen:
            seen[mls] = row
        else:
            # Keep newer timestamp
            existing_ts = seen[mls].get('timestamp', '')
            new_ts = row.get('timestamp', '')
            if new_ts > existing_ts:
                seen[mls] = row

    return list(seen.values())


def write_csv(rows: list, output_path: Path):
    """Write rows to CSV with standard headers."""
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=STANDARD_HEADERS, extrasaction='ignore')
        writer.writeheader()
        for row in rows:
            # Ensure all standard fields exist
            clean_row = {field: row.get(field, '') for field in STANDARD_HEADERS}
            writer.writerow(clean_row)


def main():
    print("=" * 60)
    print("COMBINING INITIAL LISTING DATA")
    print("=" * 60)

    # Ensure output directory exists
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Process Phoenix
    print("\n[PHOENIX]")
    print(f"  Reading: {PHOENIX_ALL_IN_ONE.name}")
    phoenix_current = read_and_normalize(PHOENIX_ALL_IN_ONE)
    print(f"    Loaded {len(phoenix_current)} rows")

    print(f"  Reading: {PHOENIX_ARCHIVE.name}")
    phoenix_archive = read_and_normalize(PHOENIX_ARCHIVE)
    print(f"    Loaded {len(phoenix_archive)} rows")

    phoenix_combined = phoenix_current + phoenix_archive
    print(f"  Combined: {len(phoenix_combined)} total rows")

    phoenix_deduped = dedupe_by_mls(phoenix_combined)
    print(f"  After deduplication: {len(phoenix_deduped)} unique MLS numbers")

    write_csv(phoenix_deduped, PHOENIX_OUTPUT)
    print(f"  Wrote: {PHOENIX_OUTPUT}")

    # Process Tucson
    print("\n[TUCSON]")
    print(f"  Reading: {TUCSON_ALL_IN_ONE.name}")
    tucson_current = read_and_normalize(TUCSON_ALL_IN_ONE)
    print(f"    Loaded {len(tucson_current)} rows")

    print(f"  Reading: {TUCSON_ARCHIVE.name}")
    tucson_archive = read_and_normalize(TUCSON_ARCHIVE)
    print(f"    Loaded {len(tucson_archive)} rows")

    tucson_combined = tucson_current + tucson_archive
    print(f"  Combined: {len(tucson_combined)} total rows")

    tucson_deduped = dedupe_by_mls(tucson_combined)
    print(f"  After deduplication: {len(tucson_deduped)} unique MLS numbers")

    write_csv(tucson_deduped, TUCSON_OUTPUT)
    print(f"  Wrote: {TUCSON_OUTPUT}")

    # Summary
    print("\n" + "=" * 60)
    print("INITIAL DATA CREATED")
    print("=" * 60)
    print(f"  Phoenix: {len(phoenix_deduped)} unique listings")
    print(f"  Tucson:  {len(tucson_deduped)} unique listings")
    print(f"  Total:   {len(phoenix_deduped) + len(tucson_deduped)} listings")
    print("=" * 60)
    print("\nNext steps:")
    print("  1. Run process_data.py to generate JSON output files")
    print("  2. Create GitHub repo 'listings-feed-store'")
    print("  3. Push this folder to the repo")


if __name__ == "__main__":
    main()
