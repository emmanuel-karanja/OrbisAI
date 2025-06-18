import logging
from api.ingestion_api import create_ingestion_app
from utils.logger import setup_logger
from dotenv import load_dotenv
import os

load_dotenv(override=True)

LOG_DIR = os.getenv("LOG_DIR", "logs")
logger = setup_logger(name="ingestion-app",log_dir=LOG_DIR,log_to_file=True)

try:
    app = create_ingestion_app()
    logger.info("Ingestion app created successfully.")
except Exception as e:
    logger.error(f"Failed to initialize ingestion app: {e}", exc_info=True)
    app = None  # optional: fallback logic or raise
