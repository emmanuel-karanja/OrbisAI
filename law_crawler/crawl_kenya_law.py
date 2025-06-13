import os
import re
import json
import requests
from bs4 import BeautifulSoup
from time import sleep
from urllib.parse import urljoin

BASE_URL = "https://new.kenyalaw.org"
OUT_DIR = "kenya_judgments"
TXT_DIR = os.path.join(OUT_DIR, "akn")
PDF_DIR = os.path.join(OUT_DIR, "pdfs")
LOG_JSON = os.path.join(OUT_DIR, "downloaded_akn.json")

os.makedirs(TXT_DIR, exist_ok=True)
os.makedirs(PDF_DIR, exist_ok=True)

# Load log
downloaded = {}
if os.path.exists(LOG_JSON):
    with open(LOG_JSON, "r", encoding="utf-8") as f:
        downloaded = json.load(f)

def get_total_pages(listing_url):
    res = requests.get(listing_url, timeout=30)
    res.raise_for_status()
    soup = BeautifulSoup(res.text, "html.parser")
    pages = soup.select("ul.pagination li a")
    return max([int(a.text) for a in pages if a.text.isdigit()] or [1])

def get_summary_links(listing_url):
    res = requests.get(listing_url, timeout=30)
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
        print(f"✅ Skipped existing AKN: {filename}")
        return filename, False

    with open(fpath, "w", encoding="utf-8") as f:
        f.write(soup.get_text(separator="\n"))
    print(f"💾 AKN saved: {filename}")
    return filename, True

def save_pdf(pdf_url):
    full = urljoin(BASE_URL, pdf_url)
    name = os.path.basename(pdf_url)
    path = os.path.join(PDF_DIR, name)
    if os.path.exists(path):
        print(f"✅ Skipped existing PDF: {name}")
        return
    r = requests.get(full, stream=True, timeout=30)
    with open(path, "wb") as f:
        for chunk in r.iter_content(8192):
            f.write(chunk)
    print(f"📎 PDF saved: {name}")

if __name__ == "__main__":
    save_count = 0
    for year in range(1930, 2026):
        print(f"\n📅 Crawling year: {year}")
        base_year_url = f"{BASE_URL}/judgments/all/{year}/"

        try:
            total_pages = get_total_pages(base_year_url)
        except Exception as e:
            print(f"❌ Could not get pages for {year}: {e}")
            continue

        for page in range(1, total_pages + 1):
            print(f"\n🔍 Year {year} – Page {page}/{total_pages}")
            page_url = f"{base_year_url}?page={page}"

            try:
                summaries = get_summary_links(page_url)
            except Exception as e:
                print(f"⚠️ Failed to get summaries for page {page} of {year}: {e}")
                continue

            for summary in summaries:
                try:
                    akn, pdfs = get_links_from_summary(summary)

                    if akn:
                        filename, new = save_akn(akn)
                        if new:
                            downloaded[filename] = urljoin(BASE_URL, akn)
                            save_count += 1
                    else:
                        print("⚠️ No AKN found.")

                    for pdf_url in pdfs:
                        save_pdf(pdf_url)

                    if save_count and save_count % 50 == 0:
                        with open(LOG_JSON, "w", encoding="utf-8") as f:
                            json.dump(downloaded, f, indent=2)
                        print("🔄 JSON log saved (every 50 AKNs).")

                    sleep(0.5)
                except Exception as e:
                    print(f"❌ Error processing summary: {summary}: {e}")

    with open(LOG_JSON, "w", encoding="utf-8") as f:
        json.dump(downloaded, f, indent=2)
    print("\n✅ Done. All years crawled. Final log saved.")
