import logging
import requests
import sys
from pathlib import Path
import os
from dotenv import load_dotenv

# Load environment variables from .env file
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
            # Fail silently to prevent recursive logging errors
            pass


def setup_logger(name="app", level=None, log_to_file=True, log_dir="logs") -> logging.Logger:
    logger = logging.getLogger(name)

    # Prevent duplicate handlers if logger is reused
    if logger.hasHandlers():
        return logger

    # Resolve values
    level = level or os.getenv("LOG_LEVEL", "INFO").upper()
    log_to_file = log_to_file if log_to_file is not None else os.getenv("LOG_TO_FILE", "True").lower() == "true"
    log_dir = log_dir or os.getenv("LOG_DIR", "logs")

    # Convert level string to logging level
    numeric_level = getattr(logging, level, logging.INFO)
    logger.setLevel(numeric_level)

    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Console logging
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File logging
    if log_to_file:
        Path(log_dir).mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(Path(log_dir) / f"{name}.log")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    # Webhook logging
    webhook_url = os.getenv("N8N_LOG_WEBHOOK")
    if webhook_url:
        webhook_handler = WebhookLogHandler(webhook_url, service_name=name)
        webhook_handler.setFormatter(formatter)
        logger.addHandler(webhook_handler)

    return logger
