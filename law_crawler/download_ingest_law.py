import os
import re
import base64
import requests
from bs4 import BeautifulSoup
from time import sleep

# 🗄️ URLs & Directories
LISTING_URL = "https://new.kenyalaw.org/legislation/"
AKN_PREFIX = "https://new.kenyalaw.org"
DOWNLOAD_DIR = "kenya_laws"
ACTS_DIR = os.path.join(DOWNLOAD_DIR, "acts")
INGEST_API = "http://localhost:8001/ingest"

os.makedirs(ACTS_DIR, exist_ok=True)

# 📥 Fetch all document URLs from listing page
def get_all_akn_links():
    print("🔍 Scraping legislation list...")
    res = requests.get(LISTING_URL, timeout=30)
    res.raise_for_status()
    soup = BeautifulSoup(res.text, "html.parser")
    links = soup.find_all("a", href=True)
    akn_links = sorted(
        set(
            AKN_PREFIX + a["href"]
            for a in links
            if a["href"].startswith("/akn/ke/")
        )
    )
    print(f"🔗 Found {len(akn_links)} AKN links.")
    return akn_links

# 📤 Ingest function for HTML
def ingest_html_content(url, save_dir):
    try:
        res = requests.get(url, timeout=30)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")
        title = soup.title.string if soup.title else "Unknown_Law"
        title_clean = re.sub(r"[^\w\-]+", "_", title)
        content = soup.get_text(separator="\n").strip()

        filename = f"{title_clean}.txt"
        fpath = os.path.join(save_dir, filename)
        with open(fpath, "w", encoding="utf-8") as f:
            f.write(content)
        print("⬇️ Downloaded:", filename)

        with open(fpath, "rb") as f:
            encoded = base64.b64encode(f.read()).decode("utf-8")
            res = requests.post(
                INGEST_API,
                json={"filename": filename, "content": encoded},
                timeout=60
            )
            print(f"📤 Ingested {filename} → {res.status_code}")
        sleep(0.5)
    except Exception as e:
        print(f"❌ Failed on {url}: {e}")

# 🏁 Main execution
if __name__ == "__main__":
    links = get_all_akn_links()
    for link in links:
        ingest_html_content(link, ACTS_DIR)
    print("🎉 All HTML AKN documents downloaded and ingested.")
