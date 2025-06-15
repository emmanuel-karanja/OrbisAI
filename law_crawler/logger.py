# logger.py

import logging
import requests
import sys
from pathlib import Path
import os

from dotenv import load_dotenv
load_dotenv()

class WebhookLogHandler(logging.Handler):
    def __init__(self, webhook_url: str, service_name: str):
        super().__init__()
        self.webhook_url = webhook_url
        self.service_name = service_name

    def emit(self, record):
        try:
            log_entry = self.format(record)
            payload = {
                "service": self.service_name,
                "level": record.levelname,
                "message": log_entry
            }
            requests.post(self.webhook_url, json=payload, timeout=2)
        except Exception:
            pass  # Avoid log loop if webhook fails

def setup_logger(name="app", level=None, log_to_file=None, log_dir=None) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.hasHandlers():
        return logger

    # Read config from environment
    level_str = level or os.getenv("LOG_LEVEL", "INFO").upper()
    log_to_file = log_to_file if log_to_file is not None else os.getenv("LOG_TO_FILE", "False").lower() == "true"
    log_dir = log_dir or os.getenv("LOG_DIR", "logs")
    webhook_url = os.getenv("N8N_LOG_WEBHOOK")

    # Convert level string to logging constant
    level = getattr(logging, level_str, logging.INFO)
    logger.setLevel(level)

    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File (optional)
    if log_to_file:
        Path(log_dir).mkdir(exist_ok=True)
        file_handler = logging.FileHandler(f"{log_dir}/{name}.log", encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    # Webhook (optional)
    if webhook_url:
        webhook_handler = WebhookLogHandler(webhook_url, service_name=name)
        webhook_handler.setFormatter(formatter)
        logger.addHandler(webhook_handler)

    return logger
