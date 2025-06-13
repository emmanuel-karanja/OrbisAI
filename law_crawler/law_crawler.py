import os
import re
import time
import json
import threading
import requests
from urllib.parse import urljoin, urlparse, parse_qs
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.chrome.service import Service


class KenyaLawWebCrawler:
    def __init__(self, start_url, max_depth=3, download_root="kenya_laws", max_workers=5):
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
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")

        try:
            service = Service(ChromeDriverManager().install())
            browser = webdriver.Chrome(service=service, options=options)
            return browser
        except WebDriverException as e:
            tqdm.write(f"❌ Failed to launch browser: {e}")
            return None
        except Exception as e:
            tqdm.write(f"❌ Unexpected error initializing browser: {e}")
            return None



    def sanitize_filename(self, text):
        text = re.sub(r"[^\w\-]+", "_", text)
        text = re.sub(r"_+", "_", text)
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

    def convert_to_markdown(self, soup: BeautifulSoup, title="Document"):
        lines = [f"# {title}", ""]
        for tag in soup.body.descendants:
            if tag.name in ["h1", "h2", "h3"]:
                lines.append(f"{'#' * int(tag.name[1])} {tag.get_text(strip=True)}\n")
            elif tag.name == "p":
                text = tag.get_text(strip=True)
                if text:
                    lines.append(f"{text}\n")
            elif tag.name == "ul":
                for li in tag.find_all("li"):
                    lines.append(f"- {li.get_text(strip=True)}")
            elif tag.name == "ol":
                for i, li in enumerate(tag.find_all("li"), 1):
                    lines.append(f"{i}. {li.get_text(strip=True)}")
            elif tag.name == "br":
                lines.append("")
        return "\n".join(lines)

    def extract_metadata(self, soup, url):
        title = soup.title.string.strip() if soup.title else "Untitled"
        parsed = urlparse(url)
        act_id = parse_qs(parsed.query).get("actid", [""])[0].replace("%20", " ")
        cap_match = re.search(r"CAP\.?\s?\d+[A-Z]?", title, re.IGNORECASE)
        year_match = re.search(r"\b(19|20)\d{2}\b", title)
        return {
            "title": title,
            "act_id": act_id if act_id else None,
            "cap": cap_match.group(0) if cap_match else None,
            "year": int(year_match.group(0)) if year_match else None
        }

    def render_with_browser(self, url):
        try:
            browser = self.setup_browser()
            browser.get(url)
            time.sleep(4)
            html = browser.page_source
            browser.quit()
            tqdm.write(f"✅ Rendered content from: {url}")
            return html
        except Exception as e:
            tqdm.write(f"❌ Failed to render with browser: {url} — {e}")
            return None

    def save_pdf(self, link, from_page):
        fname = os.path.basename(link).split("?")[0]
        if not fname.lower().endswith(".pdf"):
            fname += ".pdf"
        path = os.path.join(self.PDF_DIR, fname)

        with self.downloaded_lock:
            if os.path.exists(path) or fname in self.downloaded_files:
                tqdm.write(f"✅ Skipped existing PDF: {path}")
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
        link_lower = link.lower()
        fname = None

        if "/judgment/" in link_lower:
            prefix = "judgment"
            path_part = link_lower.split("/judgment/")[-1]
        elif "/act/" in link_lower:
            prefix = "act"
            path_part = link_lower.split("/act/")[-1]
        elif "actview.xql?actid=" in link_lower:
            prefix = "act"
            path_part = parse_qs(urlparse(link).query).get("actid", ["act"])[0]
        else:
            prefix = "document"
            path_part = os.path.basename(urlparse(link).path)

        fname_raw = f"{prefix}_{self.sanitize_filename(path_part)}.html"
        path = os.path.join(self.HTML_DIR, fname_raw)
        raw_path = path.replace(".html", ".raw.html")

        with self.downloaded_lock:
            if os.path.exists(path) or fname_raw in self.downloaded_files:
                tqdm.write(f"✅ Skipped existing HTML: {path}")
                return None

        tqdm.write(f"🌐 Rendering AKN/Act page: {from_page} → {link}")
        html = self.render_with_browser(link)
        if not html:
            return None

        try:
            with open(raw_path, "w", encoding="utf-8") as raw:
                raw.write(html)
            tqdm.write(f"📄 Raw HTML saved: {raw_path}")

            soup = BeautifulSoup(html, "html.parser")
            title = soup.title.string.strip() if soup.title else link
            clean_html = self.extract_act_body(html, title)
            metadata = self.extract_metadata(soup, link)

            with open(path, "w", encoding="utf-8") as f:
                f.write(clean_html)

            markdown_path = path.replace(".html", ".md")
            markdown = self.convert_to_markdown(soup, title)
            with open(markdown_path, "w", encoding="utf-8") as f:
                f.write(markdown)

            with self.downloaded_lock:
                self.downloaded_files.add(fname_raw)

            tqdm.write(f"✅ HTML saved: {path}")
            tqdm.write(f"✅ Markdown saved: {markdown_path}")
            return {
                "type": "akn",
                "url": link,
                "source": from_page,
                "file_html": fname_raw,
                "file_md": os.path.basename(markdown_path),
                "metadata": metadata
            }
        except Exception as e:
            tqdm.write(f"❌ Error saving AKN HTML from {link}: {e}")
            with open(self.LOG_FILE, "a", encoding="utf-8") as f:
                f.write(f"[FAIL HTML] {link}: {e}\n")
            return None

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

            elif (
                ".akn" in link.lower()
                or "/akn/ke/" in link.lower()
                or "actview.xql?actid=" in link.lower()
            ):
                info = self.save_akn_html(link, current_url)
                if info:
                    self.index.append(info)

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
                    pass

        with open(self.INDEX_JSON, "w", encoding="utf-8") as f:
            json.dump(self.index, f, indent=2)

        tqdm.write(f"\n✅ Done. Total documents saved: {len(self.index)}")
        tqdm.write(f"📦 PDFs: {len([i for i in self.index if i['type'] == 'pdf'])}")
        tqdm.write(f"🌐 AKN/Act Pages: {len([i for i in self.index if i['type'] == 'akn'])}")
        tqdm.write(f"📄 Crawl index saved to {self.INDEX_JSON}")


if __name__ == "__main__":
    crawler = KenyaLawWebCrawler(
        start_url="http://www.kenyalaw.org/lex//index.xql",
        max_depth=3,
        download_root="C:\\Users\\ZBOOK\\Downloads\\kenya_laws",
        max_workers=5
    )
    crawler.run()
