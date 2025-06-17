from fastapi import FastAPI
from services.ingest_service import IngestService
from ai_engine.local_ai_engine import LocalAIEngine
# from ai_engine.openai_ai_engine import OpenAIAIEngine
from utils.logger import setup_logger
from api.routes.ingestion_routes import register_ingestion_routes
import os

LOG_DIR = os.getenv("LOG_DIR", "logs")

def create_ingestion_app() -> FastAPI:
    logger = setup_logger("ingestion-service-api",log_dir=LOG_DIR, log_to_file=True)
    logger.info("Creating FastAPI app instance...")

    app = FastAPI()

    @app.on_event("startup")
    async def startup_event():
        logger.info("Initializing AI engine...")
        ai_engine = LocalAIEngine()  # Switch to OpenAIAIEngine if needed
        # ai_engine=OpenAIAIEngine()
        app.state.ingest_service = IngestService(ai_engine)
        logger.info("IngestService initialized successfully.")

    register_ingestion_routes(app, logger)
    return app
