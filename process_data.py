#!/usr/bin/env python3
"""
Listings Feed Store - Data Processor
Processes Phoenix and Tucson listing CSVs into JSON output files.

Outputs (market-segmented):
- phx-internal/verified_agents.json: Phoenix agents (for Community Photos verification)
- phx-internal/photographers.json: Phoenix photo metadata analysis
- phx-internal/customer_loyalty.json: Phoenix per-agent ListerPros usage stats
- tuc-internal/verified_agents.json: Tucson agents
- tuc-internal/photographers.json: Tucson photo metadata analysis
- tuc-internal/customer_loyalty.json: Tucson per-agent ListerPros usage stats
- output/listings_summary.json: Combined summary stats

Run locally or via GitHub Actions when CSVs are updated.
"""

import csv
import json
import re
from datetime import datetime, timezone
from collections import defaultdict
from pathlib import Path

# Paths
SCRIPT_DIR = Path(__file__).parent
DATA_DIR = SCRIPT_DIR / "data"
OUTPUT_DIR = SCRIPT_DIR / "output"
PHX_OUTPUT_DIR = SCRIPT_DIR / "phx-internal"
TUC_OUTPUT_DIR = SCRIPT_DIR / "tuc-internal"

# Ensure output directories exist
OUTPUT_DIR.mkdir(exist_ok=True)
PHX_OUTPUT_DIR.mkdir(exist_ok=True)
TUC_OUTPUT_DIR.mkdir(exist_ok=True)

# Standard field names (normalize across different CSV header variations)
STANDARD_FIELDS = [
    'timestamp', 'mls_number', 'price', 'listing_address', 'status',
    'agent_name', 'agent_first_name', 'agent_phone', 'agent_email', 'agent_website',
    'office_name', 'office_phone', 'office_email', 'office_website',
    'formatted_address', 'image_filename',
    'exif_artist', 'exif_copyright', 'exif_make', 'exif_model',
    'exif_lens_model', 'exif_body_serial_number', 'exif_date_time_digitized',
    'scraped_image_filename', 'lp_flag', 'cleaned', 'preferred_photographer'
]

# Header mappings (various CSV headers -> standard field names)
HEADER_MAP = {
    # Timestamp variations
    'timestamp': 'timestamp',
    'date': 'timestamp',
    'date_time': 'timestamp',
    'what is': 'timestamp',  # Typo in Tucson archive

    # Standard fields
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

# Address normalization - same as Google Apps Script
ABBREVIATION_MAP = {
    'st': 'street', 'str': 'street', 'rd': 'road', 'dr': 'drive', 'av': 'avenue',
    'ave': 'avenue', 'ln': 'lane', 'ct': 'court', 'pl': 'place', 'blvd': 'boulevard',
    'pkwy': 'parkway', 'cir': 'circle', 'trl': 'trail', 'wy': 'way',
    'n': 'north', 's': 'south', 'e': 'east', 'w': 'west',
    'ne': 'northeast', 'nw': 'northwest', 'se': 'southeast', 'sw': 'southwest'
}


def normalize_header(header: str) -> str:
    """Convert CSV header to standard field name."""
    normalized = header.lower().strip()
    return HEADER_MAP.get(normalized, normalized.replace(' ', '_'))


def clean_value(value: str) -> str:
    """Clean up a CSV value."""
    if not value:
        return ''
    value = str(value).strip()
    if value.startswith('"') and value.endswith('"'):
        value = value[1:-1]
    return value


def normalize_email(email: str) -> str:
    """Normalize email address for consistent lookups."""
    if not email:
        return ''
    return email.lower().strip()


def normalize_mls(mls: str) -> str:
    """Normalize MLS number (remove .0 suffix if present)."""
    if not mls:
        return ''
    mls = str(mls).strip()
    if mls.endswith('.0'):
        mls = mls[:-2]
    return mls


def normalize_address(address: str) -> str:
    """
    Normalize an address for matching - mirrors Google Apps Script logic.
    Expands abbreviations and standardizes formatting.
    """
    if not address:
        return ''

    # Lowercase and clean
    cleaned = str(address).strip().lower()

    # Remove punctuation
    cleaned = re.sub(r'[.,\/#!$%\^&\*;:{}=\-_`~()]', '', cleaned)

    # Split into words and expand abbreviations
    parts = cleaned.split()
    normalized_parts = []
    for part in parts:
        # Check if this word is an abbreviation
        expanded = ABBREVIATION_MAP.get(part, part)
        normalized_parts.append(expanded)

    # Join with single spaces
    return ' '.join(normalized_parts)


def read_csv_file(filepath: Path) -> list:
    """Read a CSV file and return normalized rows."""
    rows = []

    if not filepath.exists():
        print(f"  Warning: {filepath} not found")
        return rows

    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        reader = csv.DictReader(f)
        header_mapping = {h: normalize_header(h) for h in reader.fieldnames or []}

        for row in reader:
            normalized_row = {}
            for original_header, value in row.items():
                standard_field = header_mapping.get(original_header, original_header)
                normalized_row[standard_field] = clean_value(value)

            if not normalized_row.get('mls_number'):
                continue

            normalized_row['mls_number'] = normalize_mls(normalized_row['mls_number'])

            if normalized_row.get('agent_email'):
                normalized_row['agent_email'] = normalize_email(normalized_row['agent_email'])

            rows.append(normalized_row)

    return rows


def read_listerpros_orders(filepath: Path) -> set:
    """
    Read ListerPros order addresses and return normalized address set.
    Expected CSV format: at minimum a 'Formatted Address' or 'formatted_address' column
    """
    addresses = set()

    if not filepath.exists():
        print(f"  Note: {filepath} not found - LP matching will use existing lp_flag values")
        return addresses

    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        reader = csv.DictReader(f)
        headers_lower = {h.lower(): h for h in reader.fieldnames or []}

        # Find the address column
        address_col = None
        for possible in ['formatted address', 'formatted_address', 'address']:
            if possible in headers_lower:
                address_col = headers_lower[possible]
                break

        if not address_col:
            print(f"  Warning: No address column found in {filepath}")
            return addresses

        for row in reader:
            addr = row.get(address_col, '')
            if addr:
                normalized = normalize_address(addr)
                if normalized:
                    addresses.add(normalized)

    return addresses


def read_preferred_photographers(filepath: Path) -> dict:
    """
    Read preferred photographer mappings (agent email -> photographer name).
    Expected CSV format: 'Agent Email' and 'Preferred Photographer' columns
    """
    mapping = {}

    if not filepath.exists():
        print(f"  Note: {filepath} not found - using existing preferred_photographer values")
        return mapping

    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        reader = csv.DictReader(f)
        headers_lower = {h.lower(): h for h in reader.fieldnames or []}

        email_col = headers_lower.get('agent email') or headers_lower.get('agent_email')
        photo_col = headers_lower.get('preferred photographer') or headers_lower.get('preferred_photographer')

        if not email_col or not photo_col:
            print(f"  Warning: Missing required columns in {filepath}")
            return mapping

        for row in reader:
            email = normalize_email(row.get(email_col, ''))
            photographer = row.get(photo_col, '').strip()
            if email and photographer:
                mapping[email] = photographer

    return mapping


def dedupe_by_mls(rows: list) -> list:
    """Deduplicate rows by MLS number, keeping the most recent."""
    seen = {}
    for row in rows:
        mls = row.get('mls_number', '')
        if not mls:
            continue

        if mls not in seen:
            seen[mls] = row
        else:
            existing_ts = seen[mls].get('timestamp', '')
            new_ts = row.get('timestamp', '')
            if new_ts > existing_ts:
                seen[mls] = row

    return list(seen.values())


def check_lp_in_filename(filename: str) -> bool:
    """
    Check if 'ListerPros' appears in the photo filename.
    This is a definitive match since LP adds 'ListerPros' to all final photo names.
    """
    if not filename:
        return False
    return 'listerpros' in filename.lower()


# Valid LP camera models - orders shot by ListerPros
# SONY ILCE-7M4 is ListerPros' camera
# Blank/empty camera means metadata was stripped (likely LP)
LP_VALID_CAMERAS = {'SONY ILCE-7M4', 'Sony ILCE-7M4', 'ILCE-7M4'}


def is_valid_lp_camera(camera: str) -> bool:
    """
    Check if the camera model is valid for an LP order.

    Returns True if:
    - Camera is blank/empty/dash (metadata stripped = likely LP)
    - Camera is SONY ILCE-7M4 (LP's camera)

    Returns False if:
    - Camera is any other model (iPhone, Canon, etc. = NOT shot by LP)
    """
    if not camera or camera.strip() in ('', '-'):
        # Blank camera = metadata stripped, could be LP
        return True

    camera_clean = camera.strip()

    # Check if it's LP's camera
    if camera_clean in LP_VALID_CAMERAS:
        return True

    # Check for Sony A7M4 variations
    if 'ILCE-7M4' in camera_clean or 'ilce-7m4' in camera_clean.lower():
        return True

    # Any other camera (iPhone, Canon, DJI, etc.) = NOT LP
    return False


def enrich_listings(rows: list, lp_addresses: set, photographer_map: dict) -> list:
    """
    Enrich listings with LP matching and preferred photographer lookups.

    LP Detection Priority:
    1. Check if 'ListerPros' is in the scraped_image_filename (definitive match)
    2. Check if normalized address matches Xeviofy order addresses (fallback)

    Only updates if not already set or if we have lookup data.
    """
    enriched_by_filename = 0
    enriched_by_address = 0

    for row in rows:
        # Skip if already flagged as LP
        if row.get('lp_flag', '').lower() in ['yes', 'true', '1']:
            continue

        # Priority 1: Check filename for 'ListerPros' (definitive match)
        scraped_filename = row.get('scraped_image_filename', '')
        if check_lp_in_filename(scraped_filename):
            row['lp_flag'] = 'Yes'
            enriched_by_filename += 1
            continue

        # Also check the image_filename field (from HTML extraction)
        image_filename = row.get('image_filename', '')
        if check_lp_in_filename(image_filename):
            row['lp_flag'] = 'Yes'
            enriched_by_filename += 1
            continue

        # Priority 2: LP Address Matching (fallback for renamed files)
        if lp_addresses:
            formatted_addr = row.get('formatted_address', '')
            if formatted_addr:
                normalized = normalize_address(formatted_addr)
                if normalized in lp_addresses:
                    row['lp_flag'] = 'Yes'
                    enriched_by_address += 1
                    continue

        # Preferred Photographer Matching (separate from LP detection)
        if photographer_map:
            email = row.get('agent_email', '')
            if email and email in photographer_map:
                row['preferred_photographer'] = photographer_map[email]

    total_enriched = enriched_by_filename + enriched_by_address
    if total_enriched > 0:
        print(f"    LP matches: {enriched_by_filename} by filename, {enriched_by_address} by address ({total_enriched} total)")

    return rows


def build_verified_agents(rows: list, market_name: str) -> dict:
    """Build verified agents list from listings for a single market."""
    agents_by_email = defaultdict(lambda: {
        'email': '',
        'names': set(),
        'phones': set(),
        'listing_count': 0,
        'listings': [],
        'offices': set(),
        'listing_volume': 0,
        'lp_listings': 0,
    })

    for row in rows:
        email = row.get('agent_email', '')
        if not email or '@' not in email:
            continue

        agent = agents_by_email[email]
        agent['email'] = email

        if row.get('agent_name'):
            agent['names'].add(row['agent_name'])

        if row.get('agent_phone'):
            agent['phones'].add(row['agent_phone'])

        if row.get('office_name'):
            agent['offices'].add(row['office_name'])

        agent['listing_count'] += 1

        # Track listing volume from price
        price_str = row.get('price', '').replace('$', '').replace(',', '').strip()
        try:
            price = float(price_str)
            agent['listing_volume'] += price
        except (ValueError, TypeError):
            pass

        # Track LP listings - with camera validation
        # Build camera string for validation
        camera = f"{row.get('exif_make', '')} {row.get('exif_model', '')}".strip()
        address_matched = row.get('lp_flag', '').lower() in ['yes', 'true', '1']
        camera_valid = is_valid_lp_camera(camera)

        # Only count as LP if BOTH address matched AND camera is valid
        is_lp = address_matched and camera_valid
        if is_lp:
            agent['lp_listings'] += 1

        if row.get('listing_address'):
            agent['listings'].append({
                'mls': row.get('mls_number', ''),
                'address': row.get('listing_address', ''),
                'status': row.get('status', ''),
                'price': row.get('price', ''),
                'lp': is_lp,
                'camera': camera if camera else '-',
            })

    agents_list = []
    for email, data in agents_by_email.items():
        agents_list.append({
            'email': email,
            'name': list(data['names'])[0] if data['names'] else '',
            'all_names': list(data['names']),
            'phone': list(data['phones'])[0] if data['phones'] else '',
            'office': list(data['offices'])[0] if data['offices'] else '',
            'total_listings': data['listing_count'],
            'listing_volume': data['listing_volume'],
            'lp_listings': data['lp_listings'],
            'recent_listings': sorted(data['listings'], key=lambda x: x.get('mls', ''), reverse=True)[:10],
        })

    agents_list.sort(key=lambda x: x['total_listings'], reverse=True)

    return {
        'market': market_name,
        'agents': agents_list,
        'total_agents': len(agents_list),
        'updated': datetime.now(timezone.utc).isoformat(),
    }


def build_customer_loyalty(rows: list, market_name: str) -> dict:
    """
    Build customer loyalty analytics for a single market.
    Shows which agents use ListerPros, how often, and loyalty percentage.

    LP Order Validation:
    - Address/filename match is initial flag
    - BUT camera must also be valid (SONY ILCE-7M4 or blank)
    - If camera is iPhone, Canon, etc. -> NOT an LP order even if address matched
    """
    agents = defaultdict(lambda: {
        'email': '',
        'name': '',
        'phone': '',
        'office': '',
        'total_listings': 0,
        'lp_listings': 0,
        'non_lp_listings': 0,
        'lp_percentage': 0.0,
        'listing_volume': 0,
        'preferred_photographer': '',
        'listings_detail': [],
    })

    camera_filtered_out = 0  # Track how many were filtered by camera

    for row in rows:
        email = row.get('agent_email', '')
        if not email or '@' not in email:
            continue

        agent = agents[email]
        agent['email'] = email
        agent['name'] = row.get('agent_name', '') or agent['name']
        agent['phone'] = row.get('agent_phone', '') or agent['phone']
        agent['office'] = row.get('office_name', '') or agent['office']
        agent['preferred_photographer'] = row.get('preferred_photographer', '') or agent['preferred_photographer']

        agent['total_listings'] += 1

        # Track listing volume
        price_str = row.get('price', '').replace('$', '').replace(',', '').strip()
        try:
            price = float(price_str)
            agent['listing_volume'] += price
        except (ValueError, TypeError):
            pass

        # Build camera string for validation
        camera = f"{row.get('exif_make', '')} {row.get('exif_model', '')}".strip()

        # LP validation: check both address match AND camera validity
        address_matched = row.get('lp_flag', '').lower() in ['yes', 'true', '1']
        camera_valid = is_valid_lp_camera(camera)

        # Only count as LP if BOTH address matched AND camera is valid
        is_lp = address_matched and camera_valid

        # Track filtered orders for logging
        if address_matched and not camera_valid:
            camera_filtered_out += 1

        if is_lp:
            agent['lp_listings'] += 1
        else:
            agent['non_lp_listings'] += 1

        # Store listing detail (limit to recent 20)
        if len(agent['listings_detail']) < 20:
            agent['listings_detail'].append({
                'mls': row.get('mls_number', ''),
                'address': row.get('listing_address', ''),
                'lp': is_lp,
                'status': 'LP Order' if is_lp else 'Other',
                'photographer': row.get('exif_artist', '') or row.get('preferred_photographer', ''),
                'camera': camera if camera else '-',
            })

    if camera_filtered_out > 0:
        print(f"      Camera filter: {camera_filtered_out} address-matched orders filtered out (wrong camera)")

    # Calculate percentages and build output
    loyalty_list = []
    for email, data in agents.items():
        if data['total_listings'] > 0:
            data['lp_percentage'] = round((data['lp_listings'] / data['total_listings']) * 100, 1)

        loyalty_list.append({
            'email': data['email'],
            'name': data['name'],
            'phone': data['phone'],
            'office': data['office'],
            'total_listings': data['total_listings'],
            'listing_volume': data['listing_volume'],
            'lp_listings': data['lp_listings'],
            'non_lp_listings': data['non_lp_listings'],
            'lp_percentage': data['lp_percentage'],
            'preferred_photographer': data['preferred_photographer'],
            'recent_listings': data['listings_detail'][:10],
        })

    # Sort by total listings descending
    loyalty_list.sort(key=lambda x: x['total_listings'], reverse=True)

    # Calculate summary stats
    total_agents = len(loyalty_list)
    agents_using_lp = len([a for a in loyalty_list if a['lp_listings'] > 0])
    total_lp_listings = sum(a['lp_listings'] for a in loyalty_list)
    total_all_listings = sum(a['total_listings'] for a in loyalty_list)

    # Loyalty tiers
    loyal_agents = [a for a in loyalty_list if a['lp_percentage'] >= 75 and a['total_listings'] >= 3]
    occasional_agents = [a for a in loyalty_list if 25 <= a['lp_percentage'] < 75 and a['total_listings'] >= 3]
    rare_agents = [a for a in loyalty_list if 0 < a['lp_percentage'] < 25 and a['total_listings'] >= 3]
    never_used = [a for a in loyalty_list if a['lp_listings'] == 0 and a['total_listings'] >= 3]

    return {
        'market': market_name,
        'summary': {
            'total_agents': total_agents,
            'agents_using_lp': agents_using_lp,
            'total_lp_listings': total_lp_listings,
            'total_listings': total_all_listings,
            'overall_lp_percentage': round((total_lp_listings / total_all_listings * 100) if total_all_listings > 0 else 0, 1),
        },
        'loyalty_tiers': {
            'loyal_75_plus': len(loyal_agents),
            'occasional_25_to_75': len(occasional_agents),
            'rare_under_25': len(rare_agents),
            'never_used': len(never_used),
        },
        'top_loyal_agents': loyal_agents[:50],
        'opportunity_agents': [a for a in never_used if a['total_listings'] >= 5][:50],
        'all_agents': loyalty_list,
        'updated': datetime.now(timezone.utc).isoformat(),
    }


def build_photographers_data(rows: list, market_name: str) -> dict:
    """Build photographer/camera analytics from EXIF data for a single market."""
    cameras = defaultdict(int)
    photographers = defaultdict(int)
    preferred_photographers = defaultdict(int)

    for row in rows:
        make = row.get('exif_make', '').strip()
        model = row.get('exif_model', '').strip()
        if make and model:
            camera_key = f"{make} {model}"
            cameras[camera_key] += 1
        elif make:
            cameras[make] += 1

        artist = row.get('exif_artist', '').strip()
        if artist:
            photographers[artist] += 1

        preferred = row.get('preferred_photographer', '').strip()
        if preferred:
            preferred_photographers[preferred] += 1

    return {
        'market': market_name,
        'cameras': dict(sorted(cameras.items(), key=lambda x: x[1], reverse=True)[:50]),
        'photographers': dict(sorted(photographers.items(), key=lambda x: x[1], reverse=True)[:100]),
        'preferred_photographers': dict(sorted(preferred_photographers.items(), key=lambda x: x[1], reverse=True)),
        'updated': datetime.now(timezone.utc).isoformat(),
    }


def main():
    print("=" * 60)
    print("LISTINGS FEED STORE - DATA PROCESSOR")
    print("Market-Segmented Output (phx-internal / tuc-internal)")
    print("=" * 60)

    # Load lookup data
    print("\n[*] Loading lookup data...")
    lp_addresses = read_listerpros_orders(DATA_DIR / "listerpros_orders.csv")
    print(f"    ListerPros addresses: {len(lp_addresses)}")

    photographer_map = read_preferred_photographers(DATA_DIR / "preferred_photographers.csv")
    print(f"    Preferred photographer mappings: {len(photographer_map)}")

    # Read Phoenix data
    print("\n[*] Reading Phoenix listings...")
    phoenix_rows = read_csv_file(DATA_DIR / "phoenix_listings.csv")
    print(f"    Loaded {len(phoenix_rows)} rows")

    phoenix_rows = dedupe_by_mls(phoenix_rows)
    print(f"    After deduplication: {len(phoenix_rows)} unique MLS numbers")

    # Enrich Phoenix
    phoenix_rows = enrich_listings(phoenix_rows, lp_addresses, photographer_map)

    # Read Tucson data
    print("\n[*] Reading Tucson listings...")
    tucson_rows = read_csv_file(DATA_DIR / "tucson_listings.csv")
    print(f"    Loaded {len(tucson_rows)} rows")

    tucson_rows = dedupe_by_mls(tucson_rows)
    print(f"    After deduplication: {len(tucson_rows)} unique MLS numbers")

    # Enrich Tucson
    tucson_rows = enrich_listings(tucson_rows, lp_addresses, photographer_map)

    # Write listings summary (combined stats for reference)
    print("\n[*] Writing output/listings_summary.json...")

    # Calculate status counts
    phoenix_status = defaultdict(int)
    tucson_status = defaultdict(int)
    for row in phoenix_rows:
        phoenix_status[row.get('status', 'Unknown')] += 1
    for row in tucson_rows:
        tucson_status[row.get('status', 'Unknown')] += 1

    listings_summary = {
        'phoenix': {
            'total': len(phoenix_rows),
            'by_status': dict(phoenix_status),
            'lp_matched': len([r for r in phoenix_rows if r.get('lp_flag', '').lower() in ['yes', 'true', '1']]),
        },
        'tucson': {
            'total': len(tucson_rows),
            'by_status': dict(tucson_status),
            'lp_matched': len([r for r in tucson_rows if r.get('lp_flag', '').lower() in ['yes', 'true', '1']]),
        },
        'combined': {
            'total': len(phoenix_rows) + len(tucson_rows),
            'lp_matched': len([r for r in phoenix_rows + tucson_rows if r.get('lp_flag', '').lower() in ['yes', 'true', '1']]),
        },
        'updated': datetime.now(timezone.utc).isoformat(),
        'note': 'Market-specific data in phx-internal/ and tuc-internal/ folders',
    }
    with open(OUTPUT_DIR / "listings_summary.json", 'w', encoding='utf-8') as f:
        json.dump(listings_summary, f, indent=2, ensure_ascii=False)
    print(f"    Phoenix: {len(phoenix_rows)}, Tucson: {len(tucson_rows)}")

    # =========================================================================
    # PHOENIX MARKET OUTPUT (phx-internal/)
    # =========================================================================
    print("\n[*] Building Phoenix market data (phx-internal/)...")

    # Phoenix verified agents
    print("    Building verified_agents.json...")
    phx_verified_agents = build_verified_agents(phoenix_rows, 'phoenix')
    with open(PHX_OUTPUT_DIR / "verified_agents.json", 'w', encoding='utf-8') as f:
        json.dump(phx_verified_agents, f, indent=2, ensure_ascii=False)
    print(f"      Wrote {phx_verified_agents['total_agents']} Phoenix agents")

    # Phoenix customer loyalty
    print("    Building customer_loyalty.json...")
    phx_customer_loyalty = build_customer_loyalty(phoenix_rows, 'phoenix')
    with open(PHX_OUTPUT_DIR / "customer_loyalty.json", 'w', encoding='utf-8') as f:
        json.dump(phx_customer_loyalty, f, indent=2, ensure_ascii=False)
    print(f"      {phx_customer_loyalty['summary']['agents_using_lp']} Phoenix agents have used LP")

    # Phoenix photographers
    print("    Building photographers.json...")
    phx_photographers = build_photographers_data(phoenix_rows, 'phoenix')
    with open(PHX_OUTPUT_DIR / "photographers.json", 'w', encoding='utf-8') as f:
        json.dump(phx_photographers, f, indent=2, ensure_ascii=False)
    print(f"      Wrote Phoenix camera/photographer analytics")

    # =========================================================================
    # TUCSON MARKET OUTPUT (tuc-internal/)
    # =========================================================================
    print("\n[*] Building Tucson market data (tuc-internal/)...")

    # Tucson verified agents
    print("    Building verified_agents.json...")
    tuc_verified_agents = build_verified_agents(tucson_rows, 'tucson')
    with open(TUC_OUTPUT_DIR / "verified_agents.json", 'w', encoding='utf-8') as f:
        json.dump(tuc_verified_agents, f, indent=2, ensure_ascii=False)
    print(f"      Wrote {tuc_verified_agents['total_agents']} Tucson agents")

    # Tucson customer loyalty
    print("    Building customer_loyalty.json...")
    tuc_customer_loyalty = build_customer_loyalty(tucson_rows, 'tucson')
    with open(TUC_OUTPUT_DIR / "customer_loyalty.json", 'w', encoding='utf-8') as f:
        json.dump(tuc_customer_loyalty, f, indent=2, ensure_ascii=False)
    print(f"      {tuc_customer_loyalty['summary']['agents_using_lp']} Tucson agents have used LP")

    # Tucson photographers
    print("    Building photographers.json...")
    tuc_photographers = build_photographers_data(tucson_rows, 'tucson')
    with open(TUC_OUTPUT_DIR / "photographers.json", 'w', encoding='utf-8') as f:
        json.dump(tuc_photographers, f, indent=2, ensure_ascii=False)
    print(f"      Wrote Tucson camera/photographer analytics")

    # =========================================================================
    # SUMMARY
    # =========================================================================
    print("\n" + "=" * 60)
    print("PROCESSING COMPLETE")
    print("=" * 60)
    print(f"  Phoenix listings:  {len(phoenix_rows)}")
    print(f"  Phoenix agents:    {phx_verified_agents['total_agents']}")
    print(f"  Phoenix LP rate:   {phx_customer_loyalty['summary']['overall_lp_percentage']}%")
    print()
    print(f"  Tucson listings:   {len(tucson_rows)}")
    print(f"  Tucson agents:     {tuc_verified_agents['total_agents']}")
    print(f"  Tucson LP rate:    {tuc_customer_loyalty['summary']['overall_lp_percentage']}%")
    print()
    print("  Output folders:")
    print("    - phx-internal/  (Phoenix market data)")
    print("    - tuc-internal/  (Tucson market data)")
    print("    - output/        (Combined summary)")
    print("=" * 60)


if __name__ == "__main__":
    main()
