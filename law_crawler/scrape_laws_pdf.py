import os
import csv
import re
import requests
from bs4 import BeautifulSoup
from time import sleep
from urllib.parse import urljoin, urlparse

# ──────────────────────────────────────────────
# 📁 Setup
# ──────────────────────────────────────────────
LISTING_URL = "https://new.kenyalaw.org/legislation/"
AKN_PREFIX = "https://new.kenyalaw.org"
PDF_DIR = os.path.join("kenya_laws", "pdfs")
LOG_CSV = os.path.join("kenya_laws", "pdf_download_log.csv")
os.makedirs(PDF_DIR, exist_ok=True)

# ──────────────────────────────────────────────
# 🔗 Get All AKN Links from the Listing Page
# ──────────────────────────────────────────────
def get_all_akn_links():
    print("🔍 Scraping legislation list...")
    res = requests.get(LISTING_URL, timeout=30)
    res.raise_for_status()
    soup = BeautifulSoup(res.text, "html.parser")
    links = soup.find_all("a", href=True)
    akn_links = sorted(
        set(
            urljoin(AKN_PREFIX, a["href"])
            for a in links
            if a["href"].startswith("/akn/ke/")
        )
    )
    print(f"🔗 Found {len(akn_links)} AKN links.")
    return akn_links

# ──────────────────────────────────────────────
# 📥 Download PDF from found link
# ──────────────────────────────────────────────
def download_pdf(pdf_url):
    try:
        filename = os.path.basename(urlparse(pdf_url).path)
        fpath = os.path.join(PDF_DIR, filename)

        if os.path.exists(fpath):
            print(f"✅ PDF exists, skipping: {filename}")
            return filename

        print(f"⬇️ Downloading PDF: {filename} from {pdf_url}")
        r = requests.get(pdf_url, stream=True, timeout=30)
        r.raise_for_status()

        with open(fpath, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

        return filename
    except Exception as e:
        print(f"❌ Failed to download PDF {pdf_url}: {e}")
        return None

# ──────────────────────────────────────────────
# 🔍 Visit AKN HTML, Find and Download PDFs
# ──────────────────────────────────────────────
def find_pdfs_in_akn(url, csv_writer):
    print(f"\n🔎 Scanning AKN: {url}")
    try:
        res = requests.get(url, timeout=30)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")

        # Find all <a> tags where visible text includes 'Download PDF'
        pdf_links = [
            urljoin(url, a["href"])
            for a in soup.find_all("a", string=re.compile("download pdf", re.IGNORECASE))
            if a.has_attr("href") and a["href"].lower().endswith(".pdf")
        ]

        if not pdf_links:
            print("ℹ️ No 'Download PDF' links found on this page.")

        for pdf_url in pdf_links:
            filename = download_pdf(pdf_url)
            if filename:
                csv_writer.writerow([url, pdf_url, filename])

        sleep(0.5)
    except Exception as e:
        print(f"❌ Failed to process AKN page {url}: {e}")

# ──────────────────────────────────────────────
# 🚀 Main Execution
# ──────────────────────────────────────────────
if __name__ == "__main__":
    akn_links = get_all_akn_links()

    with open(LOG_CSV, "w", newline="", encoding="utf-8") as logfile:
        writer = csv.writer(logfile)
        writer.writerow(["AKN Source URL", "PDF URL", "Saved Filename"])

        for akn_url in akn_links:
            find_pdfs_in_akn(akn_url, writer)

    print("\n✅ All PDFs downloaded and logged to CSV.")
