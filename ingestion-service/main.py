# ingestion_api.py

from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import JSONResponse
from models import IngestRequest, QueryRequest
from logger import setup_logger
from services import IngestService


class IngestionAPI:
    def __init__(self):
        self.logger = setup_logger("ingestion-service-api")
        self.logger.info("Creating FastAPI app instance...")
        self.app = FastAPI()
        self.ingest_service = None
        self._register_routes()

        @self.app.on_event("startup")
        async def startup_event():
            self.logger.info("Initializing IngestService...")
            self.ingest_service = IngestService()
            self.logger.info("IngestService initialized successfully.")

    def _register_routes(self):
        @self.app.get("/health")
        def health():
            return {"status": "ok"}

        @self.app.post("/ingest")
        async def ingest(request: IngestRequest, background_tasks: BackgroundTasks):
            background_tasks.add_task(self.ingest_service.ingest_document, request)
            self.logger.info(f"Ingestion scheduled for file: {request.filename}")
            return {"status": "accepted", "message": "Document ingestion started in background"}

        @self.app.get("/ingest-status/{filename}")
        def get_ingest_status(filename: str):
            return self.ingest_service.ingest_status(filename)

        @self.app.post("/query")
        def query(request: QueryRequest):
            return self.ingest_service.query_docs(request)

        @self.app.get("/list-documents")
        def list_documents():
            return self.ingest_service.list_all_documents()


# This is how you instantiate and expose the app if running via uvicorn or similar
api_instance = IngestionAPI()
app = api_instance.app
