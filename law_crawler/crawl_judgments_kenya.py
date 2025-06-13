import os
import re
import csv
import json
import requests
from bs4 import BeautifulSoup
from time import sleep
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_URL = "https://new.kenyalaw.org"
OUT_DIR = "kenya_judgments"
TXT_DIR = os.path.join(OUT_DIR, "akn")
PDF_DIR = os.path.join(OUT_DIR, "pdfs")
JSON_LOG = os.path.join(OUT_DIR, "downloaded_akn.json")
CSV_LOG = os.path.join(OUT_DIR, "downloaded_akn.csv")

os.makedirs(TXT_DIR, exist_ok=True)
os.makedirs(PDF_DIR, exist_ok=True)

# Load previous log
downloaded = {}
if os.path.exists(JSON_LOG):
    with open(JSON_LOG, "r", encoding="utf-8") as f:
        downloaded = json.load(f)

csv_rows = []
csv_header = ["Filename", "AKN URL", "Year", "Page", "Summary URL"]
if os.path.exists(CSV_LOG):
    with open(CSV_LOG, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)  # skip header
        csv_rows = list(reader)

def get_total_pages(listing_url):
    try:
        res = requests.get(listing_url, timeout=30)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")
        pages = soup.select("ul.pagination li a")
        return max([int(a.text) for a in pages if a.text.isdigit()] or [1])
    except:
        return 1

def get_summary_links(page_url):
    res = requests.get(page_url, timeout=30)
    res.raise_for_status()
    soup = BeautifulSoup(res.text, "html.parser")
    return list({a["href"] for a in soup.select("a[href^='/judgments/']")})

def get_links_from_summary(summary_url):
    full = urljoin(BASE_URL, summary_url)
    res = requests.get(full, timeout=30)
    res.raise_for_status()
    soup = BeautifulSoup(res.text, "html.parser")
    akn = next((a["href"] for a in soup.select("a[href^='/akn/ke/judgment/']") if "eng@" in a["href"]), None)
    pdfs = [a["href"] for a in soup.select("a[href$='.pdf']")]
    return akn, pdfs

def save_akn(akn_url):
    full = urljoin(BASE_URL, akn_url)
    res = requests.get(full, timeout=30)
    soup = BeautifulSoup(res.text, "html.parser")
    title = soup.title.text if soup.title else "Judgment"
    safe_title = re.sub(r"[^\w\-]+", "_", title)[:80]
    filename = f"{safe_title}.txt"
    fpath = os.path.join(TXT_DIR, filename)
    if os.path.exists(fpath):
        return filename, False
    with open(fpath, "w", encoding="utf-8") as f:
        f.write(soup.get_text(separator="\n"))
    return filename, True

def save_pdf(pdf_url):
    full = urljoin(BASE_URL, pdf_url)
    name = os.path.basename(pdf_url)
    path = os.path.join(PDF_DIR, name)
    if os.path.exists(path):
        return
    r = requests.get(full, stream=True, timeout=30)
    with open(path, "wb") as f:
        for chunk in r.iter_content(8192):
            f.write(chunk)

def process_summary(summary_url, year, page):
    try:
        akn, pdfs = get_links_from_summary(summary_url)
        result = None
        if akn:
            filename, new = save_akn(akn)
            if new:
                downloaded[filename] = urljoin(BASE_URL, akn)
                result = [filename, downloaded[filename], year, page, urljoin(BASE_URL, summary_url)]
        for pdf in pdfs:
            save_pdf(pdf)
        return result
    except Exception as e:
        print(f"❌ Error: {summary_url} → {e}")
        return None

if __name__ == "__main__":
    new_entries = 0
    with ThreadPoolExecutor(max_workers=10) as executor:
        for year in range(1930, 2026):
            print(f"\n📅 Processing year {year}")
            base_url = f"{BASE_URL}/judgments/all/{year}/"
            pages = get_total_pages(base_url)
            for page in range(1, pages + 1):
                page_url = f"{base_url}?page={page}"
                try:
                    summaries = get_summary_links(page_url)
                except Exception as e:
                    print(f"⚠️ Skipping page {page} of {year}: {e}")
                    continue
                futures = [executor.submit(process_summary, s, year, page) for s in summaries]
                for f in as_completed(futures):
                    row = f.result()
                    if row:
                        csv_rows.append(row)
                        new_entries += 1
                        print(f"✅ Downloaded: {row[0]}")
                        if new_entries % 50 == 0:
                            print("💾 Saving logs...")
                            with open(JSON_LOG, "w", encoding="utf-8") as jf:
                                json.dump(downloaded, jf, indent=2)
                            with open(CSV_LOG, "w", newline='', encoding="utf-8") as cf:
                                writer = csv.writer(cf)
                                writer.writerow(csv_header)
                                writer.writerows(csv_rows)

    # Final save
    with open(JSON_LOG, "w", encoding="utf-8") as jf:
        json.dump(downloaded, jf, indent=2)
    with open(CSV_LOG, "w", newline='', encoding="utf-8") as cf:
        writer = csv.writer(cf)
        writer.writerow(csv_header)
        writer.writerows(csv_rows)

    print("\n✅ Completed all years. Logs saved.")
