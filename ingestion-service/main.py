from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import JSONResponse

from models import IngestRequest, QueryRequest
from services import ingest_document, query_docs, ingest_status
from logger import setup_logger

logger = setup_logger("ingestion-service")
app = FastAPI()
logger.info("FastAPI app initialized.")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/ingest")
async def ingest(request: IngestRequest, background_tasks: BackgroundTasks):
    background_tasks.add_task(ingest_document, request)
    logger.info(f"Ingestion scheduled for file: {request.filename}")
    return {"status": "accepted", "message": "Document ingestion started in background"}


@app.get("/ingest-status/{filename}")
def get_ingest_status(filename: str):
    return ingest_status(filename)


@app.post("/query")
def query(request: QueryRequest):
    return query_docs(request)
