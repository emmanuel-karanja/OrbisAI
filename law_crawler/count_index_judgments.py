import os
import json
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from time import sleep

BASE_URL = "https://new.kenyalaw.org"
OUT_DIR = "akn_index"
os.makedirs(OUT_DIR, exist_ok=True)

def get_total_pages(year):
    url = f"{BASE_URL}/judgments/all/{year}/"
    try:
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

def collect_akn_links_for_year(year):
    print(f"\n📅 Year {year}")
    total_pages = get_total_pages(year)
    akn_links = []

    for page in range(1, total_pages + 1):
        print(f"  🔎 Page {page}/{total_pages}")
        page_url = f"{BASE_URL}/judgments/all/{year}/?page={page}"
        summaries = get_summary_links(page_url)

        for summary in summaries:
            akn_link = get_akn_from_summary(summary)
            if akn_link:
                print(f"Found: {akn_link}")
                akn_links.append(akn_link)
            sleep(0.1)

    out_path = os.path.join(OUT_DIR, f"akn_links_{year}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(sorted(set(akn_links)), f, indent=2)
    print(f"✅ Saved {len(akn_links)} AKN links → {out_path}")

if __name__ == "__main__":
    for year in range(1930, 2026):
        collect_akn_links_for_year(year)
