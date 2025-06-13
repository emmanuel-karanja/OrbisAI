import os
import re
import time
import requests
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from tqdm import tqdm

START_URL = "http://www.kenyalaw.org/lex//index.xql"
DOWNLOAD_DIR = "kenya_laws/acts"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

RETRY_COUNT = 3
TIMEOUT = 10
HEADERS = {"User-Agent": "Mozilla/5.0"}

def setup_browser():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    return webdriver.Chrome(options=options)

def get_page_links(url):
    try:
        browser = setup_browser()
        browser.get(url)
        time.sleep(2)
        html = browser.page_source
        browser.quit()

        soup = BeautifulSoup(html, "html.parser")
        links = [urljoin(url, a["href"]) for a in soup.find_all("a", href=True)]
        return links
    except Exception as e:
        tqdm.write(f"⚠️ Selenium failed for {url}: {e}")
        return []

def sanitize_filename(text):
    return re.sub(r"[^\w\-]+", "_", text.strip())[:80]

def download_file(url, retries=RETRY_COUNT):
    for attempt in range(1, retries + 1):
        try:
            res = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            res.raise_for_status()
            return res
        except Exception as e:
            tqdm.write(f"⚠️ Retry {attempt}/{retries} failed for {url}: {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
    tqdm.write(f"❌ Giving up on: {url}")
    return None

def save_pdf(url):
    name = os.path.basename(urlparse(url).path).split("?")[0]
    fpath = os.path.join(DOWNLOAD_DIR, name)

    if os.path.exists(fpath):
        tqdm.write(f"⏭️ Skipped PDF (exists): {name}")
        return

    res = download_file(url)
    if not res or "application/pdf" not in res.headers.get("Content-Type", ""):
        tqdm.write(f"⚠️ Not a valid PDF: {url}")
        return

    with open(fpath, "wb") as f:
        f.write(res.content)
    tqdm.write(f"📎 Saved PDF: {name}")

def save_akn_html(url):
    res = download_file(url)
    if not res or "html" not in res.headers.get("Content-Type", ""):
        tqdm.write(f"⚠️ Not a valid AKN HTML: {url}")
        return

    soup = BeautifulSoup(res.text, "html.parser")
    title = soup.title.string if soup.title else "akn_document"
    filename = sanitize_filename(title) + ".html"
    fpath = os.path.join(DOWNLOAD_DIR, filename)

    if os.path.exists(fpath):
        tqdm.write(f"⏭️ Skipped AKN HTML (exists): {filename}")
        return

    with open(fpath, "w", encoding="utf-8") as f:
        f.write(res.text)
    tqdm.write(f"💾 Saved AKN HTML: {filename}")

def is_akn_link(href):
    return "/akn/ke/" in href or href.lower().endswith(".akn") or "akn" in href.lower()

def is_pdf_link(href):
    return href.lower().endswith(".pdf")

def crawl_and_collect_links(start_url, max_depth=2, visited=None, current_depth=0):
    if visited is None:
        visited = set()

    if start_url in visited or current_depth > max_depth:
        return [], []

    visited.add(start_url)

    page_links = get_page_links(start_url)
    akn_links, pdf_links = [], []

    for link in page_links:
        if not link.startswith("http") or link in visited:
            continue

        if is_pdf_link(link):
            pdf_links.append(link)
        elif is_akn_link(link):
            akn_links.append(link)
        elif current_depth < max_depth:
            sub_akn, sub_pdf = crawl_and_collect_links(link, max_depth, visited, current_depth + 1)
            akn_links.extend(sub_akn)
            pdf_links.extend(sub_pdf)

    return list(set(akn_links)), list(set(pdf_links))

if __name__ == "__main__":
    tqdm.write("🌐 Starting recursive crawl...")
    akn_links, pdf_links = crawl_and_collect_links(START_URL)

    tqdm.write(f"🔎 Found {len(akn_links)} AKN links and {len(pdf_links)} PDF links\n")

    for link in tqdm(akn_links, desc="⬇️ Downloading AKN", unit="file"):
        save_akn_html(link)

    for link in tqdm(pdf_links, desc="⬇️ Downloading PDFs", unit="file"):
        save_pdf(link)

    tqdm.write("\n✅ All downloads complete.")
