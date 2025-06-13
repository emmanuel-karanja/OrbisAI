import os
import re
import time
import json
import requests
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

START_URL = "https://new.kenyalaw.org"
DOWNLOAD_DIR = "kenya_laws/acts"
INDEX_PATH = os.path.join(DOWNLOAD_DIR, "download_index.json")

RETRY_COUNT = 3
TIMEOUT = 10
MAX_DEPTH = 2
MAX_WORKERS = 10

HEADERS = {"User-Agent": "Mozilla/5.0"}
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
downloaded_index = []

def setup_browser():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    return webdriver.Chrome(options=options)

def get_page_links(url):
    try:
        browser = setup_browser()
        browser.get(url)
        time.sleep(3)
        html = browser.page_source
        browser.quit()
        soup = BeautifulSoup(html, "html.parser")
        return [urljoin(url, a["href"]) for a in soup.find_all("a", href=True)]
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

def save_to_index(name, url, ftype, local_path):
    downloaded_index.append({
        "name": name,
        "url": url,
        "type": ftype,
        "local_path": local_path
    })

def save_pdf(url):
    name = os.path.basename(urlparse(url).path).split("?")[0]
    fpath = os.path.join(DOWNLOAD_DIR, name)
    if os.path.exists(fpath):
        tqdm.write(f"⏭️ Skipped PDF (exists): {name}")
        return

    res = download_file(url)
    if not res or "application/pdf" not in res.headers.get("Content-Type", ""):
        return

    with open(fpath, "wb") as f:
        f.write(res.content)

    save_to_index(name, url, "pdf", fpath)
    tqdm.write(f"📎 Saved PDF: {name}")

def save_akn_html_and_xml(url):
    res = download_file(url)
    if not res or "html" not in res.headers.get("Content-Type", ""):
        return

    soup = BeautifulSoup(res.text, "html.parser")
    title = soup.title.string.strip() if soup.title else os.path.basename(url)
    fname_html = sanitize_filename(title) + ".html"
    fpath_html = os.path.join(DOWNLOAD_DIR, fname_html)

    if not os.path.exists(fpath_html):
        with open(fpath_html, "w", encoding="utf-8") as f:
            f.write(res.text)
        tqdm.write(f"💾 Saved AKN HTML: {fname_html}")
        save_to_index(title, url, "akn-html", fpath_html)
    else:
        tqdm.write(f"⏭️ Skipped AKN HTML (exists): {fname_html}")

    # Try to find .akn.xml links and download
    akn_xml_links = [urljoin(url, a["href"]) for a in soup.find_all("a", href=True) if a["href"].endswith(".akn.xml")]
    for akn_xml_url in akn_xml_links:
        xml_name = os.path.basename(akn_xml_url)
        fpath_xml = os.path.join(DOWNLOAD_DIR, xml_name)
        if os.path.exists(fpath_xml):
            tqdm.write(f"⏭️ Skipped AKN XML (exists): {xml_name}")
            continue

        res_xml = download_file(akn_xml_url)
        if res_xml:
            with open(fpath_xml, "wb") as f:
                f.write(res_xml.content)
            tqdm.write(f"📄 Saved AKN XML: {xml_name}")
            save_to_index(title, akn_xml_url, "akn-xml", fpath_xml)

def is_pdf_link(link): return link.lower().endswith(".pdf")
def is_akn_link(link): return "/akn/ke/" in link or link.lower().endswith(".akn") or link.endswith("eng@")

def crawl(start_url, visited=None, depth=0):
    if visited is None: visited = set()
    if depth > MAX_DEPTH or start_url in visited: return [], []

    visited.add(start_url)
    akns, pdfs = [], []

    links = get_page_links(start_url)
    for link in links:
        if not link.startswith("http") or link in visited:
            continue
        if is_pdf_link(link):
            pdfs.append(link)
        elif is_akn_link(link):
            akns.append(link)
        elif depth < MAX_DEPTH:
            sub_akns, sub_pdfs = crawl(link, visited, depth + 1)
            akns.extend(sub_akns)
            pdfs.extend(sub_pdfs)

    return list(set(akns)), list(set(pdfs))

if __name__ == "__main__":
    tqdm.write("🌐 Starting crawl and download...")
    akn_links, pdf_links = crawl(START_URL)

    tqdm.write(f"🔎 Found {len(akn_links)} AKN and {len(pdf_links)} PDF links\n")

    # Multithreaded AKN downloads
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = [pool.submit(save_akn_html_and_xml, link) for link in akn_links]
        for _ in tqdm(as_completed(futures), total=len(futures), desc="⬇️ AKNs"):
            pass

    # Multithreaded PDF downloads
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = [pool.submit(save_pdf, link) for link in pdf_links]
        for _ in tqdm(as_completed(futures), total=len(futures), desc="⬇️ PDFs"):
            pass

    # Save download index
    with open(INDEX_PATH, "w", encoding="utf-8") as f:
        json.dump(downloaded_index, f, indent=2)

    tqdm.write(f"\n✅ Done. Index saved to {INDEX_PATH}")
