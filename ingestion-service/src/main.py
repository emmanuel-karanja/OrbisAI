import logging
from api.ingestion_api import create_ingestion_app

logger = logging.getLogger("ingestion-app")
logging.basicConfig(level=logging.INFO)

try:
    app = create_ingestion_app()
    logger.info("Ingestion app created successfully.")
except Exception as e:
    logger.error(f"Failed to initialize ingestion app: {e}", exc_info=True)
    app = None  # optional: fallback logic or raise
