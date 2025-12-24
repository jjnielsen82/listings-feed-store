#!/usr/bin/env python3
"""
Phoenix Listings Scraper - With GitHub Sync
Scrapes Phoenix MLS active listings and syncs to GitHub.

Based on phoenix1_fixed.py but simplified for listings data.
Runs every 6 hours and syncs only new listings to GitHub.

Dependencies:
    pip install playwright beautifulsoup4 requests pillow exifread
    python -m playwright install chromium

Usage:
    python3 phoenix_listings_scraper.py

Leave running in Terminal. Press Ctrl+C to stop.
"""

import re
import os
import sys
import csv
import json
import time
import base64
import pathlib
import datetime
import traceback
from typing import Dict, List, Set
from zoneinfo import ZoneInfo
from io import StringIO
from urllib.parse import urljoin

try:
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    from bs4 import BeautifulSoup
    import requests
    from PIL import Image
    from PIL.ExifTags import TAGS
    import exifread
except ImportError as e:
    print(f"Missing required package: {e}")
    print("Install with: pip install playwright beautifulsoup4 requests pillow exifread")
    print("Then run: python -m playwright install chromium")
    sys.exit(1)

# ==== CONFIGURATION ====
USERNAME = "jn247"
PASSWORD = "Stangman55!!"
LOGIN_URL = "https://armls.flexmls.com"
# UPDATE THIS to your active listings saved search ID
SEARCH_URL = "https://armls.flexmls.com/start/search/saved/index.html?id=20250818212320015475000000"
OUTPUT_DIR = pathlib.Path.home() / "Desktop" / "MLS_Export"
TZ = ZoneInfo("America/Phoenix")

# GitHub config - Set GITHUB_TOKEN environment variable or update here
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")  # Set via: export GITHUB_TOKEN=your_token
GITHUB_REPO = "jjnielsen82/listings-feed-store"
GITHUB_BRANCH = "main"
GITHUB_CSV_PATH = "data/phoenix_listings.csv"

# Timing
INTERVAL_HOURS = 6
INTERVAL_SECONDS = INTERVAL_HOURS * 60 * 60

# Browser settings
HEADLESS = False
SLOW_MO_MS = 250
NAV_TIMEOUT = 90000
PAUSE = 1.5
PREVIEW_SETTLE_SECONDS = 15


def timestamp():
    return datetime.datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")


class GitHubSync:
    """Handles syncing CSV data to GitHub."""

    def __init__(self):
        self.api_base = f"https://api.github.com/repos/{GITHUB_REPO}"
        self.headers = {
            "Authorization": f"token {GITHUB_TOKEN}",
            "Accept": "application/vnd.github.v3+json",
        }

    def get_file_metadata(self, path: str):
        """Get file metadata (SHA) without content."""
        url = f"{self.api_base}/contents/{path}"
        params = {"ref": GITHUB_BRANCH}
        resp = requests.get(url, headers=self.headers, params=params)
        if resp.status_code == 404:
            return None, None
        resp.raise_for_status()
        data = resp.json()
        return data.get("sha"), data.get("download_url")

    def get_file_content(self, path: str):
        """Get file content using Git blob API."""
        sha, _ = self.get_file_metadata(path)
        if not sha:
            return None, None

        try:
            resp = requests.get(
                f"{self.api_base}/git/trees/{GITHUB_BRANCH}?recursive=1",
                headers=self.headers
            )
            resp.raise_for_status()
            tree = resp.json()

            blob_sha = None
            for item in tree.get('tree', []):
                if item['path'] == path:
                    blob_sha = item.get('sha')
                    break

            if not blob_sha:
                return None, sha

            resp = requests.get(
                f"{self.api_base}/git/blobs/{blob_sha}",
                headers=self.headers
            )
            resp.raise_for_status()
            blob = resp.json()

            content = base64.b64decode(blob['content']).decode('utf-8')
            return content, sha
        except Exception as e:
            print(f"    [!] Error reading blob: {e}")
            return None, sha

    def put_file(self, path: str, content: str, message: str, sha: str = None):
        """Upload file content to GitHub."""
        url = f"{self.api_base}/contents/{path}"
        payload = {
            "message": message,
            "content": base64.b64encode(content.encode("utf-8")).decode("utf-8"),
            "branch": GITHUB_BRANCH,
        }
        if sha:
            payload["sha"] = sha
        resp = requests.put(url, headers=self.headers, json=payload)
        resp.raise_for_status()
        return resp.json()

    def normalize_mls(self, mls: str) -> str:
        """Normalize MLS number."""
        mls = str(mls).strip()
        if mls.endswith('.0'):
            mls = mls[:-2]
        return mls

    def get_existing_mls_numbers(self, github_path: str) -> Set[str]:
        """Get existing MLS numbers from GitHub CSV."""
        content, _ = self.get_file_content(github_path)
        if not content:
            print(f"    [!] Warning: Could not read existing file")
            return set()
        mls_numbers = set()
        reader = csv.DictReader(content.splitlines())
        for row in reader:
            mls = row.get("mls_number", "").strip()
            if mls:
                mls_numbers.add(self.normalize_mls(mls))
        return mls_numbers

    def sync_csv(self, local_rows: List[Dict], fieldnames: List[str]) -> int:
        """Sync new rows to GitHub CSV."""
        print(f"\n[*] {timestamp()} - Syncing to GitHub...")

        existing_mls = self.get_existing_mls_numbers(GITHUB_CSV_PATH)
        print(f"    Existing records in GitHub: {len(existing_mls)}")
        print(f"    Records from scrape: {len(local_rows)}")

        new_rows = [
            row for row in local_rows
            if self.normalize_mls(row.get("mls_number", "")) not in existing_mls
        ]
        print(f"    New records to add: {len(new_rows)}")

        if not new_rows:
            print("    [✓] No new records to sync")
            return 0

        # Safety check
        if len(existing_mls) == 0 and len(local_rows) > 0:
            sha, _ = self.get_file_metadata(GITHUB_CSV_PATH)
            if sha:
                print(f"    [!] ERROR: File exists but couldn't read content")
                print(f"    [!] Aborting sync to prevent data loss")
                return 0

        existing_content, sha = self.get_file_content(GITHUB_CSV_PATH)
        if existing_content:
            existing_reader = csv.DictReader(existing_content.splitlines())
            all_rows = list(existing_reader) + new_rows
        else:
            all_rows = new_rows

        output = StringIO()
        writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(all_rows)

        ts = datetime.datetime.now(TZ).strftime("%Y-%m-%d %H:%M")
        message = f"Add {len(new_rows)} new Phoenix listings ({ts})"

        print(f"    [*] Pushing to GitHub ({len(all_rows)} total records)...")
        self.put_file(GITHUB_CSV_PATH, output.getvalue(), message, sha)
        print(f"    [✓] Successfully pushed {len(new_rows)} new records")

        return len(new_rows)


class PhoenixListingsScraper:
    """Scrapes Phoenix MLS active listings."""

    def __init__(self):
        self.output_dir = OUTPUT_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.html_content = None
        self.images_folder = None
        self.listings = []
        self.fieldnames = [
            'timestamp', 'mls_number', 'price', 'listing_address', 'status',
            'agent_name', 'agent_first_name', 'agent_phone', 'agent_email', 'agent_website',
            'office_name', 'office_phone', 'office_email', 'office_website',
            'formatted_address', 'image_filename',
            'exif_artist', 'exif_copyright', 'exif_make', 'exif_model',
            'exif_lens_model', 'exif_body_serial_number', 'exif_date_time_digitized',
            'scraped_image_filename', 'lp_flag', 'cleaned', 'preferred_photographer'
        ]

    def find_button_anywhere(self, page, regex: str):
        """Find button by text pattern."""
        rx = re.compile(regex, re.I)
        for getter in (
            lambda p: p.get_by_role("button", name=rx),
            lambda p: p.get_by_role("link", name=rx),
            lambda p: p.get_by_text(rx).first,
        ):
            loc = getter(page)
            if loc.count():
                return loc
        for fr in page.frames:
            try:
                for getter in (
                    lambda f: f.get_by_role("button", name=rx),
                    lambda f: f.get_by_role("link", name=rx),
                    lambda f: f.get_by_text(rx).first,
                ):
                    loc = getter(fr)
                    if loc.count():
                        return loc
            except:
                pass
        return None

    def login(self, page):
        """Login to FlexMLS."""
        print(f"[*] {timestamp()} - Logging in...")
        page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)
        user = page.locator("input[type='text'], input[name*='user' i]").first
        pw = page.locator("input[type='password'], input[name*='pass' i]").first
        if user.count() and pw.count():
            user.fill(USERNAME)
            pw.fill(PASSWORD)
            page.locator("button[type='submit'], input[type='submit']").first.click()
        try:
            page.wait_for_load_state("load", timeout=30000)
        except PWTimeout:
            pass

    def save_complete_webpage(self, page, out_html: pathlib.Path):
        """Save webpage with images."""
        print(f"[*] Saving webpage to {out_html}")
        out_html.parent.mkdir(parents=True, exist_ok=True)
        html = page.content()
        soup = BeautifulSoup(html, 'html.parser')

        folder = out_html.parent / (out_html.stem + "_files")
        folder.mkdir(parents=True, exist_ok=True)
        self.images_folder = folder

        cookies = {c['name']: c['value'] for c in page.context.cookies()}
        sess = requests.Session()
        sess.cookies.update(cookies)
        headers = {'User-Agent': 'Mozilla/5.0'}

        resources = []
        for img in soup.find_all('img', src=True):
            src = img.get('src')
            if src and not src.startswith('data:'):
                abs_url = urljoin(page.url, src)
                filename = pathlib.Path(abs_url).name.split('?')[0]
                local_path = folder / filename
                resources.append((abs_url, local_path, img, 'src'))

        print(f"[*] Downloading {len(resources)} images...")
        for abs_url, local_path, tag, attr in resources:
            try:
                resp = sess.get(abs_url, headers=headers, timeout=10)
                resp.raise_for_status()
                local_path.write_bytes(resp.content)
                tag[attr] = f"{folder.name}/{local_path.name}"
            except:
                pass

        out_html.write_text(str(soup), encoding="utf-8")
        self.html_content = str(soup)
        print(f"[✓] Webpage saved")

    def enhanced_contact_parser(self, html: str) -> Dict[str, str]:
        """Parse contact info from cell HTML."""
        if not html:
            return {'name': '', 'phone': '', 'email': '', 'website': ''}

        soup = BeautifulSoup(html, 'html.parser')
        parts = [text.strip() for text in soup.stripped_strings if text.strip()]

        contact = {'name': '', 'phone': '', 'email': '', 'website': ''}
        for part in parts:
            if re.match(r'^\(?(\d{3})\)?[-.\s]?(\d{3})[-.\s]?(\d{4})$', part):
                if not contact['phone']:
                    contact['phone'] = part
            elif re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', part):
                if not contact['email']:
                    contact['email'] = part.lower()
            elif part.startswith('http://') or part.startswith('https://') or part.startswith('www.'):
                if not contact['website']:
                    contact['website'] = part
            elif not contact['name'] and len(part) > 2:
                contact['name'] = part

        return contact

    def extract_image_metadata(self, image_path: pathlib.Path) -> Dict:
        """Extract EXIF metadata from image."""
        metadata = {}
        try:
            with Image.open(image_path) as img:
                if hasattr(img, '_getexif') and img._getexif():
                    exif_dict = img._getexif()
                    for tag_id, value in exif_dict.items():
                        tag = TAGS.get(tag_id, tag_id)
                        if isinstance(value, (str, int, float)):
                            metadata[f'exif_{tag.lower()}'] = str(value)

            with open(image_path, 'rb') as f:
                exif_tags = exifread.process_file(f)
                for tag, value in exif_tags.items():
                    if tag not in ['JPEGThumbnail', 'TIFFThumbnail', 'EXIF MakerNote']:
                        clean_tag = tag.replace(' ', '_').replace('EXIF_', '').lower()
                        metadata[f'exif_{clean_tag}'] = str(value)
        except:
            pass
        return metadata

    def parse_html(self):
        """Parse listings from HTML."""
        if not self.html_content:
            return

        print(f"[*] {timestamp()} - Parsing HTML...")
        soup = BeautifulSoup(self.html_content, 'html.parser')
        rows = soup.select('#resizable tbody tr')

        current_timestamp = datetime.datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")

        for row in rows:
            cells = row.select('td.gridtd')
            if len(cells) < 5:
                continue

            main_cell = cells[2] if len(cells) > 2 else None
            if not main_cell:
                continue

            # Extract MLS number
            mls_number = ''
            mls_span = main_cell.select_one('span[style*="white-space: nowrap"]')
            if mls_span:
                mls_text = mls_span.get_text().strip()
                match = re.search(r'(\d{7,})', mls_text)
                if match:
                    mls_number = match[1]

            if not mls_number:
                continue

            # Extract basic info
            price_el = main_cell.select_one('[ls="price"]')
            price = price_el.get_text(strip=True) if price_el else ''

            address_el = main_cell.select_one('[ls="address"]')
            address = address_el.get_text(strip=True) if address_el else ''

            csz_el = main_cell.select_one('[ls="csz"]')
            city_state_zip = csz_el.get_text(strip=True) if csz_el else ''

            listing_address = address
            if city_state_zip:
                listing_address += f", {city_state_zip}"

            status_el = main_cell.select_one('.status_A, .status_P, .status_S, .status_C')
            status = status_el.get_text(strip=True) if status_el else ''

            # Parse agent (cell 3) and office (cell 8) for Phoenix
            agent_info = {'name': '', 'phone': '', 'email': '', 'website': ''}
            office_info = {'name': '', 'phone': '', 'email': '', 'website': ''}

            if len(cells) > 3:
                agent_info = self.enhanced_contact_parser(str(cells[3]))
            if len(cells) > 8:
                office_info = self.enhanced_contact_parser(str(cells[8]))

            # Get image and extract metadata
            image_filename = None
            metadata = {}
            photo_cell = cells[1] if len(cells) > 1 else None
            if photo_cell:
                img_tag = photo_cell.select_one('img[src]')
                if img_tag:
                    src = img_tag.get('src', '')
                    if src and 'nophoto' not in src.lower():
                        image_filename = pathlib.Path(src).name
                        # Try to get metadata from saved image
                        if self.images_folder:
                            img_path = self.images_folder / image_filename
                            if img_path.exists():
                                metadata = self.extract_image_metadata(img_path)

            formatted_address = re.sub(r'[^\w\s]', ' ', listing_address.lower())
            formatted_address = re.sub(r'\s+', ' ', formatted_address).strip()

            listing = {
                'timestamp': current_timestamp,
                'mls_number': mls_number,
                'price': price,
                'listing_address': listing_address,
                'status': status,
                'agent_name': agent_info['name'],
                'agent_first_name': agent_info['name'].split()[0] if agent_info['name'] else '',
                'agent_phone': agent_info['phone'],
                'agent_email': agent_info['email'],
                'agent_website': agent_info['website'],
                'office_name': office_info['name'],
                'office_phone': office_info['phone'],
                'office_email': office_info['email'],
                'office_website': office_info['website'],
                'formatted_address': formatted_address,
                'image_filename': image_filename or '',
                'exif_artist': metadata.get('exif_artist', ''),
                'exif_copyright': metadata.get('exif_copyright', ''),
                'exif_make': metadata.get('exif_make', ''),
                'exif_model': metadata.get('exif_model', ''),
                'exif_lens_model': metadata.get('exif_lensmodel', ''),
                'exif_body_serial_number': metadata.get('exif_bodyserialnumber', ''),
                'exif_date_time_digitized': metadata.get('exif_datetimedigitized', ''),
                'scraped_image_filename': '',
                'lp_flag': '',
                'cleaned': '',
                'preferred_photographer': '',
            }

            self.listings.append(listing)

        print(f"[✓] {timestamp()} - Extracted {len(self.listings)} listings")

    def run_extraction(self):
        """Run the full extraction."""
        print(f"\n{'='*60}")
        print(f"PHOENIX LISTINGS SCRAPER - {timestamp()}")
        print(f"{'='*60}")

        ts = datetime.datetime.now(TZ).strftime("%Y%m%d_%H%M%S")
        out_html = self.output_dir / f"phoenix_listings_{ts}.html"

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=HEADLESS, slow_mo=SLOW_MO_MS)
            context = browser.new_context(viewport={"width": 1600, "height": 1000})
            page = context.new_page()

            try:
                self.login(page)
                time.sleep(PAUSE)

                print(f"[*] {timestamp()} - Navigating to saved search...")
                page.goto(SEARCH_URL, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)
                time.sleep(5)

                print(f"[*] {timestamp()} - Clicking Print...")
                print_btn = self.find_button_anywhere(page, r"^\s*print(\s+listings)?\s*$")
                if print_btn:
                    print_btn.click()
                time.sleep(3)

                print(f"[*] {timestamp()} - Clicking Preview...")
                preview_btn = self.find_button_anywhere(page, r"^\s*preview\s*$")
                if preview_btn:
                    newp = None
                    try:
                        with page.context.expect_page() as wait_new:
                            preview_btn.click()
                        newp = wait_new.value
                    except:
                        pass
                    target = newp or page
                else:
                    target = page

                print(f"[*] {timestamp()} - Waiting {PREVIEW_SETTLE_SECONDS}s...")
                time.sleep(PREVIEW_SETTLE_SECONDS)

                self.save_complete_webpage(target, out_html)

            finally:
                context.close()
                browser.close()

    def run(self):
        """Main run method."""
        self.listings = []
        self.html_content = None
        self.run_extraction()
        self.parse_html()
        return self.listings, self.fieldnames


def main():
    print("\n" + "=" * 60)
    print("PHOENIX LISTINGS SCRAPER - CONTINUOUS MODE")
    print("=" * 60)
    print(f"  Interval: Every {INTERVAL_HOURS} hours")
    print(f"  GitHub repo: {GITHUB_REPO}")
    print(f"  CSV path: {GITHUB_CSV_PATH}")
    print("=" * 60)
    print("\nPress Ctrl+C to stop.\n")

    run_count = 0

    try:
        while True:
            run_count += 1
            print(f"\n{'*'*60}")
            print(f"* PHOENIX LISTINGS RUN #{run_count} - {timestamp()}")
            print(f"{'*'*60}")

            try:
                scraper = PhoenixListingsScraper()
                listings, fieldnames = scraper.run()

                if listings:
                    sync = GitHubSync()
                    new_count = sync.sync_csv(listings, fieldnames)
                    print(f"\n[✓] {timestamp()} - Complete: {len(listings)} scraped, {new_count} new synced")
                else:
                    print(f"\n[!] {timestamp()} - No listings extracted")

            except Exception as e:
                print(f"\n[!] {timestamp()} - Error: {e}")
                traceback.print_exc()

            next_run = datetime.datetime.now(TZ) + datetime.timedelta(seconds=INTERVAL_SECONDS)
            print(f"\n{'='*60}")
            print(f"Next run: {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"{'='*60}")

            time.sleep(INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print(f"\n\nStopped by user at {timestamp()}")
        print(f"Total runs: {run_count}")
        sys.exit(0)


if __name__ == "__main__":
    main()
