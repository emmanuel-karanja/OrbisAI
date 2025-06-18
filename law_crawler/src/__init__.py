import os

def get_config():
    return {
        "start_url": os.getenv("CRAWL_START_URL"),
        "max_depth": int(os.getenv("CRAWL_MAX_DEPTH", 3)),
        "max_workers": int(os.getenv("CRAWL_MAX_WORKERS", 2)),
        "download_root": os.getenv("CRAWL_OUTPUT", "downloads"),
        "user_agent": os.getenv("USER_AGENT", "Mozilla/5.0"),
        "timeout": int(os.getenv("TIMEOUT", 10)),
        "retry_count": int(os.getenv("RETRY_COUNT", 3)),
        "rate_limit_delay": float(os.getenv("RATE_LIMIT_DELAY", 1))
    }
