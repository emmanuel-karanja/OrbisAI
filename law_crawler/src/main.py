# main.py
import argparse
from config import get_config
from crawler import KenyaLawWebCrawler
from dotenv import load_dotenv

load_dotenv()

def parse_args():
    parser = argparse.ArgumentParser(description="Kenya Law Crawler")
    parser.add_argument("--url", type=str, help="Start URL")
    parser.add_argument("--depth", type=int, help="Max crawl depth")
    parser.add_argument("--workers", type=int, help="Max worker threads")
    parser.add_argument("--output", type=str, help="Output folder")
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    config = get_config()  # ✅ No args here

    crawler = KenyaLawWebCrawler(
        start_url=config["start_url"],       # ✅ Lowercase keys
        max_depth=config["max_depth"],
        download_root=config["download_root"],
        max_workers=config["max_workers"],
        rate_limit_delay=config["rate_limit_delay"],
        retry_count=config["retry_count"],
        user_agent=config["user_agent"],
        timeout=config["timeout"],
        save_threshold=config["save_threshold"]
    )
    crawler.run()
