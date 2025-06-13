import os
import re
import json
import time
import requests
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

START_URL = "http://www.kenyalaw.org/lex//index.xql"
BASE_DIR = "kenya_laws"
PDF_DIR = os.path.join(BASE_DIR, "pdfs")
AKN_HTML_DIR = os.path.join(BASE_DIR, "akn_html")
AKN_FILE_DIR = os.path.join(BASE_DIR, "akn_files")
FAILURE_LOG = os.path.join(BASE_DIR, "failure_log.txt")
INDEX_JSON = os.path.join(BASE_DIR, "index.json")

os.makedirs(PDF_DIR, exist_ok=True)
os.makedirs(AKN_HTML_DIR, exist_ok=True)
os.makedirs(AKN_FILE_DIR, exist_ok=True)

MAX_DEPTH = 5
MAX_WORKERS = 5
HEADERS = {"User-Agent": "Mozilla/5.0"}
TIMEOUT = 10
RETRY_COUNT = 3
REQUEST_DELAY = 1.0  # seconds


def setup_browser(headless=True):
    options = Options()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--log-level=3")
    return webdriver.Chrome(options=options)


def sanitize_filename(text):
    text = text.strip().replace(" ", "_")
    text = re.sub(r"[^\w\-\.]", "_", text)
    return text[:100] or "file"


def log_failure(url, error):
    with open(FAILURE_LOG, "a", encoding="utf-8") as f:
        f.write(f"{url}\t{error}\n")


def download_file(url, save_dir, extension_hint=None):
    """Download a file (PDF or AKN file) with retries and save it to save_dir."""
    # Determine file name from URL or hint
    parsed_url = urlparse(url)
    base_name = os.path.basename(parsed_url.path)
    if not base_name or '.' not in base_name:
        # fallback to extension hint if provided
        base_name = sanitize_filename(url) + (extension_hint if extension_hint else ".bin")

    fpath = os.path.join(save_dir, base_name)
    if os.path.exists(fpath):
        return fpath  # already downloaded

    for attempt in range(RETRY_COUNT):
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            if r.status_code == 200:
                content_type = r.headers.get("Content-Type", "").lower()
                if extension_hint == ".pdf" and "pdf" not in content_type:
                    raise ValueError(f"Expected PDF but got Content-Type: {content_type}")
                if extension_hint == ".akn" and "xml" not in content_type and "text" not in content_type:
                    # AKN files are XML but sometimes served as text/html, accept text/html too
                    raise ValueError(f"Expected AKN/XML but got Content-Type: {content_type}")

                with open(fpath, "wb") as f:
                    f.write(r.content)
                time.sleep(REQUEST_DELAY)
                return fpath
            else:
                raise ValueError(f"Status code {r.status_code}")
        except Exception as e:
            time.sleep(2 ** attempt)
            if attempt == RETRY_COUNT - 1:
                log_failure(url, f"Download failed: {e}")
                return None
    return None


def extract_act_body(html):
    soup = BeautifulSoup(html, "html.parser")
    selectors = ["div.akn", "div#akn", "div.akn-body", "div#akn-body", "div.akn-document"]
    body_div = None
    for sel in selectors:
        body_div = soup.select_one(sel)
        if body_div:
            break
    if not body_div:
        body_div = soup.find("main") or soup.body

    page_title = soup.title.string.strip() if soup.title else "Act"
    minimal_html = f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><title>{page_title}</title></head>
<body>
{body_div.prettify() if body_div else '<p>[No content found]</p>'}
</body>
</html>"""
    return minimal_html


def download_and_save_akn_html(url, browser):
    try:
        browser.get(url)
        time.sleep(3)  # wait for JS render
        html = browser.page_source

        soup = BeautifulSoup(html, "html.parser")
        title = soup.title.string.strip() if soup.title else None
        filename = sanitize_filename(title or url) + ".html"

        clean_html = extract_act_body(html)
        fpath = os.path.join(AKN_HTML_DIR, filename)

        with open(fpath, "w", encoding="utf-8") as f:
            f.write(clean_html)
        time.sleep(REQUEST_DELAY)
        return fpath
    except Exception as e:
        log_failure(url, f"AKN render/save failed: {e}")
        return None


def is_valid_link(url):
    if not url or url.startswith("#") or url.startswith("mailto:") or url.startswith("javascript:"):
        return False
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https", ""):
        return False
    return True


def crawl(start_url, max_depth=5):
    visited = set()
    to_visit = [(start_url, 0)]
    index = []
    browser = setup_browser(headless=True)

    pbar = tqdm(unit="page", desc="Crawling")

    while to_visit:
        url, depth = to_visit.pop(0)
        if url in visited or depth > max_depth:
            continue
        visited.add(url)

        try:
            resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            if resp.status_code != 200 or 'text/html' not in resp.headers.get("Content-Type", ""):
                continue
        except Exception:
            continue

        soup = BeautifulSoup(resp.text, "html.parser")

        # Extract all valid links on page
        links = set()
        for a in soup.find_all("a", href=True):
            link = urljoin(url, a["href"].strip())
            if is_valid_link(link) and link not in visited:
                links.add(link)

        # Enqueue new links for crawling
        for link in links:
            if link not in visited and depth + 1 <= max_depth:
                to_visit.append((link, depth + 1))

        # Check if current url is PDF, AKN HTML page, or AKN file
        lower_url = url.lower()
        meta = {"source_url": url, "depth": depth}

        downloaded_path = None

        try:
            if lower_url.endswith(".pdf"):
                # PDF file - download and save
                downloaded_path = download_file(url, PDF_DIR, extension_hint=".pdf")
                if downloaded_path:
                    meta.update({"type": "pdf", "file_path": downloaded_path})

            elif lower_url.endswith(".akn"):
                # Direct .akn file - download and save as XML
                downloaded_path = download_file(url, AKN_FILE_DIR, extension_hint=".akn")
                if downloaded_path:
                    meta.update({"type": "akn_file", "file_path": downloaded_path})

            elif "/akn/ke/" in lower_url or ".akn." in lower_url:
                # AKN HTML page - render and save clean HTML
                downloaded_path = download_and_save_akn_html(url, browser)
                if downloaded_path:
                    meta.update({"type": "akn_html", "file_path": downloaded_path})

        except Exception as e:
            log_failure(url, f"Error processing page: {e}")

        if downloaded_path:
            index.append(meta)

        pbar.update(1)

    browser.quit()
    pbar.close()

    # Save index.json
    with open(INDEX_JSON, "w", encoding="utf-8") as f:
        json.dump(index, f, indent=2, ensure_ascii=False)

    print(f"✅ Crawl complete. Downloaded {len(index)} documents.")
    print(f"Failures logged in {FAILURE_LOG} if any.")


if __name__ == "__main__":
    crawl(START_URL, max_depth=MAX_DEPTH)
