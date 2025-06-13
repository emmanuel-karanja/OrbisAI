import os
import requests
import base64
from bs4 import BeautifulSoup
from time import sleep

BASE_LIST = "https://new.kenyalaw.org/judgments/all/"
INGEST_API = "http://localhost:8001/ingest"
OUT_DIR = "kenya_judgments"
os.makedirs(OUT_DIR, exist_ok=True)

def get_listing_links(page):
    params = {"page": page}
    r = requests.get(BASE_LIST, params=params, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    links = soup.select("a[href*='/judgments/']")
    return [l["href"] for l in links]

def download_and_ingest(url):
    full = "https://new.kenyalaw.org" + url
    r = requests.get(full, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    title = soup.title.get_text(strip=True)
    text = soup.get_text(separator="\n")
    file = os.path.join(OUT_DIR, f"{title[:50]}.txt")
    with open(file, "w", encoding="utf8") as f:
        f.write(text)
    encoded = base64.b64encode(text.encode("utf8")).decode()
    resp = requests.post(INGEST_API, json={"filename": os.path.basename(file), "content": encoded})
    print(f"Ingested {os.path.basename(file)} → {resp.status_code}")

page = 1
while True:
    print(f"Fetching page {page}...")
    links = get_listing_links(page)
    if not links:
        break
    for lnk in links:
        sleep(0.5)
        download_and_ingest(lnk)
    page += 1

print("✅ Done: All judgments scraped and ingested.")
