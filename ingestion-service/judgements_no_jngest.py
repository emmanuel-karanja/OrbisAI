import os
import requests
from bs4 import BeautifulSoup
from time import sleep
import re

# ──────────────────────────────────────────────
# 📁 Configuration
# ──────────────────────────────────────────────
BASE_URL = "https://new.kenyalaw.org"
LISTING_URL = f"{BASE_URL}/judgments/all/"
OUT_DIR = "kenya_judgments"
os.makedirs(OUT_DIR, exist_ok=True)

# ──────────────────────────────────────────────
# 📊 Get total pages from pagination
# ──────────────────────────────────────────────
def get_total_pages():
    try:
        res = requests.get(LISTING_URL, timeout=30)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")
        pages = soup.select("ul.pagination li a")
        numbers = [int(a.text) for a in pages if a.text.isdigit()]
        total = max(numbers) if numbers else 1
        print(f"📄 Total pages: {total}")
        return total
    except Exception as e:
        print(f"❌ Failed to fetch total pages: {e}")
        return 1

# ──────────────────────────────────────────────
# 🔗 Scrape summary page links from listing
# ──────────────────────────────────────────────
def get_listing_links(page):
    try:
        res = requests.get(LISTING_URL, params={"page": page}, timeout=30)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")
        links = soup.select("a[href^='/judgments/']")
        return list({l["href"] for l in links})
    except Exception as e:
        print(f"❌ Failed to fetch links on page {page}: {e}")
        return []

# ──────────────────────────────────────────────
# 📦 Extract AKN link from a summary page
# ──────────────────────────────────────────────
def get_akn_link_from_summary(url):
    try:
        full_url = BASE_URL + url
        res = requests.get(full_url, timeout=30)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")
        akn_links = soup.select("a[href^='/akn/ke/judgment/']")
        for a in akn_links:
            href = a["href"]
            if re.match(r"^/akn/ke/judgment/.+?/@?\d{4}-\d{2}-\d{2}$", href) or "eng@" in href:
                return BASE_URL + href
        return None
    except Exception as e:
        print(f"⚠️ Error extracting AKN from {url}: {e}")
        return None

# ──────────────────────────────────────────────
# 💾 Download AKN HTML Judgment to local file
# ──────────────────────────────────────────────
def save_akn_html(url):
    try:
        res = requests.get(url, timeout=30)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")
        title = soup.title.get_text(strip=True) if soup.title else "Judgment"
        content = soup.get_text(separator="\n").strip()

        # Sanitize filename
        safe_title = "".join(c for c in title if c.isalnum() or c in " _-").strip()
        filename = f"{safe_title[:80].replace(' ', '_')}.txt"
        fpath = os.path.join(OUT_DIR, filename)

        with open(fpath, "w", encoding="utf-8") as f:
            f.write(content)

        print(f"💾 Saved: {filename}")
    except Exception as e:
        print(f"❌ Error saving AKN {url}: {e}")

# ──────────────────────────────────────────────
# 🚀 Run Script
# ──────────────────────────────────────────────
if __name__ == "__main__":
    total_pages = get_total_pages()

    for page in range(1, total_pages + 1):
        print(f"\n🔍 Page {page}/{total_pages}")
        summary_links = get_listing_links(page)

        for summary in summary_links:
            akn_url = get_akn_link_from_summary(summary)
            if akn_url:
                sleep(0.5)
                save_akn_html(akn_url)
            else:
                print(f"⚠️ No AKN link found in summary: {summary}")

    print("\n✅ All AKN judgments downloaded and saved.")
