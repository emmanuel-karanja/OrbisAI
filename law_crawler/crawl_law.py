import os
import re
import time
import json
import requests
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------------------- Configuration ----------------------
START_URL = "http://www.kenyalaw.org/lex//index.xql"
MAX_DEPTH = 3
DOWNLOAD_ROOT = "kenya_laws"
PDF_DIR = os.path.join(DOWNLOAD_ROOT, "pdfs")
HTML_DIR = os.path.join(DOWNLOAD_ROOT, "htmls")
LOG_FILE = os.path.join(DOWNLOAD_ROOT, "failures.log")
INDEX_JSON = os.path.join(DOWNLOAD_ROOT, "index.json")

HEADERS = {"User-Agent": "Mozilla/5.0"}
TIMEOUT = 10
RETRY_COUNT = 3
MAX_WORKERS = 5

# ---------------------- Setup ----------------------
os.makedirs(PDF_DIR, exist_ok=True)
os.makedirs(HTML_DIR, exist_ok=True)
os.makedirs(DOWNLOAD_ROOT, exist_ok=True)
visited_urls = set()
downloaded_files = set()

# ---------------------- Utilities ----------------------

def setup_browser():
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    return webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)

def sanitize_filename(text):
    text = re.sub(r"[^\w\-]+", "_", text)
    return text.strip("_")[:80] or "untitled"

def download_file(url):
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            res = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            res.raise_for_status()
            return res
        except Exception as e:
            tqdm.write(f"⚠️ Retry {attempt} for {url}: {e}")
            time.sleep(2 ** attempt)
    return None

def extract_act_body(html, title="Act"):
    soup = BeautifulSoup(html, "html.parser")
    selectors = ["div.akn", "div#akn", "div.akn-body", "div#akn-body", "div.akn-document"]
    body = None
    for sel in selectors:
        body = soup.select_one(sel)
        if body:
            break
    if not body:
        body = soup.find("main") or soup.body
    page_title = title or (soup.title.string if soup.title else "Act")
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8" />
<title>{page_title}</title>
</head>
<body>
{body.prettify() if body else '<p>[No content found]</p>'}
</body>
</html>"""

def render_with_browser(url):
    browser = setup_browser()
    browser.get(url)
    time.sleep(4)
    html = browser.page_source
    browser.quit()
    return html

def save_pdf(link, from_page):
    fname = os.path.basename(link).split("?")[0]
    if not fname.lower().endswith(".pdf"):
        fname += ".pdf"
    path = os.path.join(PDF_DIR, fname)

    if os.path.exists(path) or fname in downloaded_files:
        return None

    tqdm.write(f"📄 Downloading PDF from: {from_page} → {link}")
    res = download_file(link)
    if res and "application/pdf" in res.headers.get("Content-Type", ""):
        with open(path, "wb") as f:
            f.write(res.content)
        downloaded_files.add(fname)
        tqdm.write(f"✅ PDF saved: {path}")
        return fname
    return None

def save_akn_html(link, from_page):
    fname = sanitize_filename(link) + ".html"
    path = os.path.join(HTML_DIR, fname)

    if os.path.exists(path) or fname in downloaded_files:
        return None

    tqdm.write(f"🌐 Rendering AKN HTML from: {from_page} → {link}")
    html = render_with_browser(link)
    soup = BeautifulSoup(html, "html.parser")
    title = soup.title.string.strip() if soup.title else link
    clean_html = extract_act_body(html, title)

    with open(path, "w", encoding="utf-8") as f:
        f.write(clean_html)

    downloaded_files.add(fname)
    tqdm.write(f"✅ HTML saved: {path}")
    return fname

# ---------------------- Crawler ----------------------

def crawl_worker(current_url, current_depth, stack, results):
    if current_depth > MAX_DEPTH or current_url in visited_urls:
        return
    visited_urls.add(current_url)

    tqdm.write(f"🔎 Crawling (depth={current_depth}): {current_url}")
    try:
        res = requests.get(current_url, headers=HEADERS, timeout=TIMEOUT)
        res.raise_for_status()
    except Exception as e:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(f"[FAIL] {current_url}: {e}\n")
        return

    soup = BeautifulSoup(res.text, "html.parser")

    for a in soup.find_all("a", href=True):
        href = a["href"]
        link = urljoin(current_url, href)
        if not link.startswith("http"):
            continue

        if ".pdf" in link.lower():
            fname = save_pdf(link, current_url)
            if fname:
                results.append({"type": "pdf", "url": link, "source": current_url, "file": fname})

        elif ".akn" in link.lower() or "/akn/ke/" in link.lower():
            fname = save_akn_html(link, current_url)
            if fname:
                results.append({"type": "akn", "url": link, "source": current_url, "file": fname})

        elif urlparse(link).netloc == urlparse(START_URL).netloc and link not in visited_urls:
            stack.append((link, current_depth + 1))

# ---------------------- Main ----------------------

if __name__ == "__main__":
    tqdm.write("🚀 Starting DFS crawl...")

    index = []
    stack = [(START_URL, 0)]

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        while stack:
            batch = []
            while stack and len(batch) < MAX_WORKERS:
                batch.append(stack.pop())
            futures = [executor.submit(crawl_worker, url, depth, stack, index) for url, depth in batch]
            for future in as_completed(futures):
                pass  # Just wait

    with open(INDEX_JSON, "w", encoding="utf-8") as f:
        json.dump(index, f, indent=2)

    tqdm.write(f"\n✅ Done. Total documents saved: {len(index)}")
    tqdm.write(f"📦 PDFs: {len([i for i in index if i['type'] == 'pdf'])}")
    tqdm.write(f"🌐 AKN HTMLs: {len([i for i in index if i['type'] == 'akn'])}")
    tqdm.write(f"📄 Crawl index saved to {INDEX_JSON}")
