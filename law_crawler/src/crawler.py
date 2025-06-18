# crawler.py

import os
import time
import json
import threading
import requests
from urllib.parse import urljoin
from bs4 import BeautifulSoup

from logger import setup_logger
from browser_utils import setup_browser
from file_utils import FileUtils
from html_parser import HTMLParserUtils
from download_tracker import DownloadTracker  # Ensure path is correct
from dotenv import load_dotenv
import time

load_dotenv()


class KenyaLawWebCrawler:
    def __init__(self, start_url, max_depth, download_root, max_workers,rate_limit_delay=1,retry_count=3,user_agent="Mozilla",timeout=10,save_threshold=25):
        self.start_url = start_url
        self.max_depth = max_depth
        self.download_root = download_root
        self.max_workers = max_workers
        self.rate_limit_delay=rate_limit_delay
        self.retry_count=retry_count 
        self.user_agent=user_agent
        self-timeout=timeout
        self.entries_since_last_save = 0
        self.save_threshold = save_threshold  # Save index.json after every 25 entries


        self.logger = setup_logger(name="KenyaLawCrawler")
        self.visited = set()
        self.visited_lock = threading.Lock()
        self.stack = [(self.start_url, 0)]

        self.sections_by_act = {}
        self.failed_urls = []
        self.shutdown_event = threading.Event()

        self.pdf_dir = os.path.join(self.download_root, "pdfs")
        self.docx_dir = os.path.join(self.download_root, "docx")
        
        os.makedirs(self.pdf_dir, exist_ok=True)
        os.makedirs(self.docx_dir, exist_ok=True)
        os.makedirs(self.download_root, exist_ok=True)

        self.logger.info(f"📁 Created/verified download directory: {self.download_root}")

        index_path = os.path.join(self.download_root, "index.json")
        self.tracker = DownloadTracker(index_path)

    def run(self):
        self.logger.info(f"🚀 Starting crawl with {self.max_workers} worker(s)...")
        threads = []

        try:
            for i in range(self.max_workers):
                thread = threading.Thread(target=self.crawl_worker, name=f"Worker-{i+1}")
                thread.start()
                threads.append(thread)

            for thread in threads:
                thread.join()

        except KeyboardInterrupt:
            self.logger.warning("🛑 KeyboardInterrupt received! Attempting graceful shutdown...")
            self.shutdown_event.set()
            for thread in threads:
                thread.join()
        finally:
            self.save_index()
            self.logger.info("🧹 Cleanup complete. Exiting.")

    def crawl_worker(self):
        while not self.shutdown_event.is_set():
            with self.visited_lock:
                if not self.stack:
                    return
                url, depth = self.stack.pop()

            try:
                self.crawl_page(url, depth)
            except Exception as e:
                self.logger.error(f"❌ Unexpected error in worker while crawling {url}: {e}")
                with self.visited_lock:
                    self.failed_urls.append(url)

    def crawl_page(self, url, depth):
        if depth > self.max_depth:
            self.logger.info(f"⏭️ Skipping {url}: exceeded max depth {self.max_depth}")
            return

        with self.visited_lock:
            if url in self.visited:
                self.logger.debug(f"🔁 Already visited: {url}")
                return
            self.visited.add(url)

        self.logger.info(f"🌐 Crawling: {url} | Depth: {depth}")

        browser = setup_browser(self.logger)
        if not browser:
            self.logger.error("❌ Browser setup failed.")
            with self.visited_lock:
                self.failed_urls.append(url)
            return

        try:
            browser.get(url)
            time.sleep(2)
            html = browser.page_source
            soup = BeautifulSoup(html, "html.parser")

            # Extract and save page content
            metadata = HTMLParserUtils.extract_metadata(soup, url)
            act_id = metadata.get("act_id") or FileUtils.sanitize_filename(metadata.get("title"))
            act_dir = os.path.join(self.download_root, FileUtils.sanitize_filename(act_id))
            os.makedirs(act_dir, exist_ok=True)

            raw_path = os.path.join(act_dir, "raw.html")
            html_path = os.path.join(act_dir, "act.html")
            md_path = os.path.join(act_dir, "act.md")

            if not self.tracker.is_downloaded(raw_path):
                with open(raw_path, "w", encoding="utf-8") as f:
                    f.write(html)
                self.tracker.mark_downloaded(raw_path)
                self.tracker.add_entry({"type": "html", "file": raw_path, "url": url})

            act_body_html = HTMLParserUtils.extract_act_body(html, metadata["title"])
            if not self.tracker.is_downloaded(html_path):
                with open(html_path, "w", encoding="utf-8") as f:
                    f.write(act_body_html)
                self.tracker.mark_downloaded(html_path)
                self.tracker.add_entry({"type": "html", "file": html_path, "url": url})

            act_soup = BeautifulSoup(act_body_html, "html.parser")
            sections = HTMLParserUtils.extract_sections(act_soup)
            markdown = HTMLParserUtils.convert_to_markdown(act_soup, metadata["title"])

            if not self.tracker.is_downloaded(md_path):
                with open(md_path, "w", encoding="utf-8") as f:
                    f.write(markdown)
                self.tracker.mark_downloaded(md_path)
                self.tracker.add_entry({"type": "markdown", "file": md_path, "url": url})

            metadata["section_count"] = len(sections)
            metadata["source_url"] = url

            self.sections_by_act[act_id] = {
                "metadata": metadata,
                "sections": sections
            }

            self.logger.info(f"✅ Parsed {len(sections)} sections from {url}")

            # Handle all links
            for a in soup.find_all("a", href=True):
                href = a["href"]
                full_url = urljoin(url, href)

                if full_url.lower().endswith(".pdf"):
                    self.download_document(full_url, self.pdf_dir, "pdf")
                elif full_url.lower().endswith(".docx"):
                    self.download_document(full_url, self.docx_dir, "docx")
                elif full_url.startswith("http://www.kenyalaw.org"):
                    with self.visited_lock:
                        if full_url not in self.visited:
                            self.stack.append((full_url, depth + 1))

        except Exception as e:
            self.logger.error(f"❌ Error processing {url}: {e}")
            with self.visited_lock:
                self.failed_urls.append(url)
        finally:
            try:
                browser.quit()
            except Exception as e:
                self.logger.warning(f"⚠️ Failed to close browser properly for {url}: {e}")

    def download_document(self, url, save_dir, doc_type):
        fname = FileUtils.sanitize_filename(os.path.basename(url).split("?")[0])
        if not fname.lower().endswith(f".{doc_type}"):
            fname += f".{doc_type}"
        path = os.path.join(save_dir, fname)

        if self.tracker.is_downloaded(path):
            self.logger.debug(f"⏩ Skipped already downloaded: {fname}")
            return

        retry_count = self.retry_count
        base_delay = self.rate_limit_delay

        for attempt in range(1, retry_count + 1):
            try:
                self.logger.debug(f"📡 Attempt {attempt} for {url}")
                res = requests.get(url, timeout=self.timeout)
                res.raise_for_status()

                with open(path, "wb") as f:
                    f.write(res.content)

                self.logger.info(f"📥 Saved {doc_type.upper()}: {fname}")
                self.tracker.mark_downloaded(path)
                self.tracker.add_entry({"type": doc_type, "file": path, "url": url})

                # 🔁 Save index every 25 downloads
                self.entries_since_last_save += 1
                if self.entries_since_last_save >= self.save_threshold:
                    self.tracker.save_index()
                    self.entries_since_last_save = 0

                return

            except Exception as e:
                self.logger.warning(f"⚠️ Attempt {attempt} failed to download {doc_type.upper()} from {url}: {e}")
                if attempt == retry_count:
                    self.logger.error(f"❌ Giving up on {url} after {retry_count} attempts.")
                    self.failed_urls.append(url)
                else:
                    delay = base_delay * (2 ** (attempt - 1))  # 1s, 2s, 4s, etc.
                    self.logger.info(f"⏳ Waiting {delay:.1f}s before next retry...")
                    time.sleep(delay)

    def save_index(self):
        try:
            index_path = os.path.join(self.download_root, "sections_index.json")
            with open(index_path, "w", encoding="utf-8") as f:
                json.dump(self.sections_by_act, f, indent=2)
            self.logger.info(f"📄 Saved section index to {index_path}")
        except Exception as e:
            self.logger.error(f"❌ Failed to save sections_index.json: {e}")

        self.tracker.save_index()

        if self.failed_urls:
            try:
                failed_path = os.path.join(self.download_root, "failed_urls.txt")
                with open(failed_path, "w", encoding="utf-8") as f:
                    for url in self.failed_urls:
                        f.write(url + "\n")
                self.logger.warning(f"⚠️ {len(self.failed_urls)} URLs failed. See: {failed_path}")
            except Exception as e:
                self.logger.error(f"❌ Failed to write failed URLs: {e}")
