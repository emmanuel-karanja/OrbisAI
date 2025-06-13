import os
import re
import time
import json
import threading
import requests
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed


class WebCrawler:
    def __init__(self,
                 start_url="http://www.kenyalaw.org/lex//index.xql",
                 max_depth=3,
                 download_root="kenya_laws",
                 max_workers=5):
        self.START_URL = start_url
        self.MAX_DEPTH = max_depth
        self.DOWNLOAD_ROOT = download_root
        self.PDF_DIR = os.path.join(download_root, "pdfs")
        self.HTML_DIR = os.path.join(download_root, "htmls")
        self.LOG_FILE = os.path.join(download_root, "failures.log")
        self.INDEX_JSON = os.path.join(download_root, "index.json")
        self.HEADERS = {"User-Agent": "Mozilla/5.0"}
        self.TIMEOUT = 10
        self.RETRY_COUNT = 3
        self.MAX_WORKERS = max_workers

        os.makedirs(self.PDF_DIR, exist_ok=True)
        os.makedirs(self.HTML_DIR, exist_ok=True)
        os.makedirs(self.DOWNLOAD_ROOT, exist_ok=True)

        self.visited_urls = set()
        self.downloaded_files = set()
        self.visited_lock = threading.Lock()
        self.downloaded_lock = threading.Lock()
        self.stack_lock = threading.Lock()

        self.index = []

    def setup_browser(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        return webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)

    def sanitize_filename(self, text):
        text = re.sub(r"[^\w\-]+", "_", text)
        return text.strip("_")[:80] or "untitled"

    def download_file(self, url):
        for attempt in range(1, self.RETRY_COUNT + 1):
            try:
                res = requests.get(url, headers=self.HEADERS, timeout=self.TIMEOUT)
                res.raise_for_status()
                return res
            except Exception as e:
                tqdm.write(f"⚠️ Retry {attempt} for {url}: {e}")
                time.sleep(2 ** attempt)
        return None

    def extract_act_body(self, html, title="Act"):
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

    def render_with_browser(self, url):
        browser = self.setup_browser()
        browser.get(url)
        time.sleep(4)
        html = browser.page_source
        browser.quit()
        return html

    def save_pdf(self, link, from_page):
        fname = os.path.basename(link).split("?")[0]
        if not fname.lower().endswith(".pdf"):
            fname += ".pdf"
        path = os.path.join(self.PDF_DIR, fname)

        with self.downloaded_lock:
            if os.path.exists(path) or fname in self.downloaded_files:
                return None

        tqdm.write(f"📄 Downloading PDF from: {from_page} → {link}")
        res = self.download_file(link)
        if res and "application/pdf" in res.headers.get("Content-Type", ""):
            with open(path, "wb") as f:
                f.write(res.content)
            with self.downloaded_lock:
                self.downloaded_files.add(fname)
            tqdm.write(f"✅ PDF saved: {path}")
            return fname
        return None

    def save_akn_html(self, link, from_page):
        fname = self.sanitize_filename(link) + ".html"
        path = os.path.join(self.HTML_DIR, fname)

        with self.downloaded_lock:
            if os.path.exists(path) or fname in self.downloaded_files:
                return None

        tqdm.write(f"🌐 Rendering AKN HTML from: {from_page} → {link}")
        html = self.render_with_browser(link)
        soup = BeautifulSoup(html, "html.parser")
        title = soup.title.string.strip() if soup.title else link
        clean_html = self.extract_act_body(html, title)

        with open(path, "w", encoding="utf-8") as f:
            f.write(clean_html)

        with self.downloaded_lock:
            self.downloaded_files.add(fname)
        tqdm.write(f"✅ HTML saved: {path}")
        return fname

    def crawl_worker(self, current_url, current_depth, stack):
        with self.visited_lock:
            if current_depth > self.MAX_DEPTH or current_url in self.visited_urls:
                return
            self.visited_urls.add(current_url)

        tqdm.write(f"🔎 Crawling (depth={current_depth}): {current_url}")
        try:
            res = requests.get(current_url, headers=self.HEADERS, timeout=self.TIMEOUT)
            res.raise_for_status()
        except Exception as e:
            with open(self.LOG_FILE, "a", encoding="utf-8") as f:
                f.write(f"[FAIL] {current_url}: {e}\n")
            return

        soup = BeautifulSoup(res.text, "html.parser")

        for a in soup.find_all("a", href=True):
            href = a["href"]
            link = urljoin(current_url, href)
            if not link.startswith("http"):
                continue

            if ".pdf" in link.lower():
                fname = self.save_pdf(link, current_url)
                if fname:
                    self.index.append({"type": "pdf", "url": link, "source": current_url, "file": fname})

            elif ".akn" in link.lower() or "/akn/ke/" in link.lower():
                fname = self.save_akn_html(link, current_url)
                if fname:
                    self.index.append({"type": "akn", "url": link, "source": current_url, "file": fname})

            elif urlparse(link).netloc == urlparse(self.START_URL).netloc:
                with self.visited_lock:
                    if link not in self.visited_urls:
                        with self.stack_lock:
                            stack.append((link, current_depth + 1))

    def run(self):
        tqdm.write("🚀 Starting DFS crawl...")
        stack = [(self.START_URL, 0)]

        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            while stack:
                batch = []
                with self.stack_lock:
                    while stack and len(batch) < self.MAX_WORKERS:
                        batch.append(stack.pop())
                futures = [executor.submit(self.crawl_worker, url, depth, stack) for url, depth in batch]
                for future in as_completed(futures):
                    pass  # Wait for all to finish

        with open(self.INDEX_JSON, "w", encoding="utf-8") as f:
            json.dump(self.index, f, indent=2)

        tqdm.write(f"\n✅ Done. Total documents saved: {len(self.index)}")
        tqdm.write(f"📦 PDFs: {len([i for i in self.index if i['type'] == 'pdf'])}")
        tqdm.write(f"🌐 AKN HTMLs: {len([i for i in self.index if i['type'] == 'akn'])}")
        tqdm.write(f"📄 Crawl index saved to {self.INDEX_JSON}")


if __name__ == "__main__":
    crawler = WebCrawler(
        start_url="http://www.kenyalaw.org/lex//index.xql",
        max_depth=3,
        download_root="C:\\Users\\ZBOOK\\Downloads\\kenya_laws",
        max_workers=5
    )
    crawler.run(download_root="c:\\Users\\ZBOOK\\Downloads\\kenya_laws")
