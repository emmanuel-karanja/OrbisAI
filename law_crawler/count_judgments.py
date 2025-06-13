import os
import json
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import re
import logging
import time

BASE_URL = "https://new.kenyalaw.org"
INDEX_DIR = "akn_index"
OUT_DIR = "akn_documents"
LOG_FILE = "failures.log"
os.makedirs(INDEX_DIR, exist_ok=True)
os.makedirs(OUT_DIR, exist_ok=True)

# Setup logging
logging.basicConfig(filename=LOG_FILE, level=logging.ERROR, format='%(asctime)s - %(message)s')

# Retry logic with exponential backoff
def fetch_with_retries(url, retries=3, backoff=2):
    for attempt in range(retries):
        try:
            res = requests.get(url, timeout=30)
            res.raise_for_status()
            return res
        except Exception as e:
            if attempt < retries - 1:
                sleep_time = backoff ** attempt
                time.sleep(sleep_time)
            else:
                logging.error(f"Failed to fetch {url} after {retries} attempts: {e}")
    return None

def get_total_pages(year):
    url = f"{BASE_URL}/judgments/all/{year}/"
    res = fetch_with_retries(url)
    if not res:
        return 1
    soup = BeautifulSoup(res.text, "html.parser")
    pages = soup.select("ul.pagination li a")
    return max([int(a.text) for a in pages if a.text.isdigit()] or [1])

def get_summary_links(page_url):
    res = fetch_with_retries(page_url)
    if not res:
        return []
    soup = BeautifulSoup(res.text, "html.parser")
    return list({a["href"] for a in soup.select("a[href^='/judgments/']")})

def get_akn_from_summary(summary_path):
    full_url = urljoin(BASE_URL, summary_path)
    res = fetch_with_retries(full_url)
    if not res:
        return None
    soup = BeautifulSoup(res.text, "html.parser")
    akn = next((a["href"] for a in soup.select("a[href^='/akn/ke/judgment/']") if "eng@" in a["href"]), None)
    return urljoin(BASE_URL, akn) if akn else None

def download_and_save_akn(akn_url):
    res = fetch_with_retries(akn_url)
    if not res:
        return None
    try:
        soup = BeautifulSoup(res.text, "html.parser")
        year_match = re.search(r'eng@(\d{4})-\d{2}-\d{2}', akn_url)
        year = year_match.group(1) if year_match else "unknown"
        year_dir = os.path.join(OUT_DIR, year)
        os.makedirs(year_dir, exist_ok=True)

        title = soup.title.text.strip() if soup.title else "Unnamed_Judgment"
        safe_title = re.sub(r"[^\w\-]+", "_", title)[:80]
        file_path = os.path.join(year_dir, f"{safe_title}.txt")

        if os.path.exists(file_path):
            return None  # Skip if already exists

        with open(file_path, "w", encoding="utf-8") as f:
            f.write(soup.get_text(separator="\n"))
        return akn_url
    except Exception as e:
        logging.error(f"Failed to parse or save AKN {akn_url}: {e}")
        return None

def collect_akn_links_for_year(year):
    akn_links = set()
    total_pages = get_total_pages(year)

    # Get all summary links for the year
    for page in tqdm(range(1, total_pages + 1), desc=f"Year {year} - Pages", leave=False):
        page_url = f"{BASE_URL}/judgments/all/{year}/?page={page}"
        summaries = get_summary_links(page_url)

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(get_akn_from_summary, summary): summary for summary in summaries}
            for future in as_completed(futures):
                akn = future.result()
                if akn:
                    akn_links.add(akn)

    # Download AKN files in parallel
    saved_links = []
    with ThreadPoolExecutor(max_workers=5) as downloader:
        futures = [downloader.submit(download_and_save_akn, akn) for akn in akn_links]
        for future in tqdm(as_completed(futures), total=len(futures), desc=f"Year {year} - Downloading AKNs", leave=False):
            result = future.result()
            if result:
                saved_links.append(result)

    # Save index
    out_path = os.path.join(INDEX_DIR, f"akn_links_{year}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(sorted(saved_links), f, indent=2)

    return year, len(saved_links), out_path

if __name__ == "__main__":
    YEARS = list(range(1930, 2026))

    print("🔄 Starting parallel processing of years...\n")
    results = []

    with ThreadPoolExecutor(max_workers=10) as year_executor:
        futures = {year_executor.submit(collect_akn_links_for_year, year): year for year in YEARS}
        for future in tqdm(as_completed(futures), total=len(YEARS), desc="📅 Overall Progress"):
            year, count, path = future.result()
            tqdm.write(f"✅ Year {year}: {count} AKNs saved → {path}")

    print("\n🎉 All years processed.")
    print(f"📁 Failures (if any) logged to: {LOG_FILE}")
