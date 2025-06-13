import os
import json
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from time import sleep
from concurrent.futures import ThreadPoolExecutor, as_completed
import re

BASE_URL = "https://new.kenyalaw.org"
INDEX_DIR = "akn_index"
OUT_DIR = "akn_documents"
os.makedirs(INDEX_DIR, exist_ok=True)
os.makedirs(OUT_DIR, exist_ok=True)

def get_total_pages(year):
    try:
        url = f"{BASE_URL}/judgments/all/{year}/"
        res = requests.get(url, timeout=30)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")
        pages = soup.select("ul.pagination li a")
        return max([int(a.text) for a in pages if a.text.isdigit()] or [1])
    except:
        return 1

def get_summary_links(page_url):
    try:
        res = requests.get(page_url, timeout=30)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")
        return list({a["href"] for a in soup.select("a[href^='/judgments/']")})
    except:
        return []

def get_akn_from_summary(summary_path):
    try:
        full_url = urljoin(BASE_URL, summary_path)
        res = requests.get(full_url, timeout=30)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")
        akn = next((a["href"] for a in soup.select("a[href^='/akn/ke/judgment/']") if "eng@" in a["href"]), None)
        if akn:
            return urljoin(BASE_URL, akn)
    except:
        return None

def download_and_save_akn(akn_url):
    try:
        res = requests.get(akn_url, timeout=30)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")

        year_match = re.search(r'eng@(\d{4})-\d{2}-\d{2}', akn_url)
        year = year_match.group(1) if year_match else "unknown"
        year_dir = os.path.join(OUT_DIR, year)
        os.makedirs(year_dir, exist_ok=True)

        title = soup.title.text.strip() if soup.title else "Unnamed_Judgment"
        safe_title = re.sub(r"[^\w\-]+", "_", title)[:80]
        file_path = os.path.join(year_dir, f"{safe_title}.txt")

        if os.path.exists(file_path):
            print(f"✅ Skipped (already saved): {file_path}")
            return None

        with open(file_path, "w", encoding="utf-8") as f:
            f.write(soup.get_text(separator="\n"))
        print(f"💾 Saved AKN: {file_path}")
        return akn_url
    except Exception as e:
        print(f"❌ Failed to download {akn_url}: {e}")
        return None

def collect_akn_links_for_year(year):
    print(f"\n📅 Year {year}")
    akn_links = set()
    total_pages = get_total_pages(year)

    for page in range(1, total_pages + 1):
        print(f"  🔎 Page {page}/{total_pages}")
        page_url = f"{BASE_URL}/judgments/all/{year}/?page={page}"
        summaries = get_summary_links(page_url)

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(get_akn_from_summary, summary): summary for summary in summaries}
            for future in as_completed(futures):
                akn = future.result()
                if akn:
                    akn_links.add(akn)

    saved_links = []
    with ThreadPoolExecutor(max_workers=5) as downloader:
        futures = [downloader.submit(download_and_save_akn, akn) for akn in akn_links]
        for future in as_completed(futures):
            result = future.result()
            if result:
                saved_links.append(result)

    # Save index for the year
    out_path = os.path.join(INDEX_DIR, f"akn_links_{year}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(sorted(saved_links), f, indent=2)
    print(f"✅ Year {year}: {len(saved_links)} AKNs saved → {out_path}")

if __name__ == "__main__":
    for year in range(1930, 2026):
        collect_akn_links_for_year(year)
