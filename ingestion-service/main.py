from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import JSONResponse

from models import IngestRequest, QueryRequest
from logger import setup_logger
from services import IngestService 

logger = setup_logger("ingestion-service-api")
app = FastAPI()
logger.info("FastAPI app initialized.")

# Global instance of the service
ingest_service: IngestService = None

@app.on_event("startup")
async def startup_event():
    global ingest_service
    logger.info("Initializing IngestService...")
    ingest_service = IngestService()
    logger.info("IngestService initialized successfully.")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/ingest")
async def ingest(request: IngestRequest, background_tasks: BackgroundTasks):
    background_tasks.add_task(ingest_service.ingest_document, request)
    logger.info(f"Ingestion scheduled for file: {request.filename}")
    return {"status": "accepted", "message": "Document ingestion started in background"}


@app.get("/ingest-status/{filename}")
def get_ingest_status(filename: str):
    return ingest_service.ingest_status(filename)


@app.post("/query")
def query(request: QueryRequest):
    return ingest_service.query_docs(request)


@app.get("/list-documents")
def list_documents():
    return ingest_service.list_all_documents()
