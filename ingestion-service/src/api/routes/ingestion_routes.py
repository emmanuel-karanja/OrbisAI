from fastapi import FastAPI, BackgroundTasks, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from models.local_models import IngestionRequest, QueryRequest


def register_ingestion_routes(app: FastAPI, logger):
    @app.get("/health")
    async def health():
        return {"status": "ok"}

    @app.post("/ingest")
    async def ingest(request: IngestionRequest, background_tasks: BackgroundTasks):
        try:
            logger.info(f"Received ingestion request for: {request.filename}")
            background_tasks.add_task(app.state.ingest_service.ingest_document, request)
            return {"status": "accepted", "message": "Document ingestion started in background"}

        except ValidationError as ve:
            logger.warning(f"Validation error during ingestion: {ve}")
            raise HTTPException(status_code=422, detail=ve.errors())

        except Exception as e:
            logger.error(f"Ingestion failed for {request.filename}: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    @app.get("/ingest-status/{filename}")
    async def get_ingest_status(filename: str):
        try:
            return await app.state.ingest_service.ingest_status(filename)
        except Exception as e:
            logger.error(f"Failed to retrieve ingestion status for {filename}: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    @app.post("/query")
    async def query(request: QueryRequest):
        try:
            logger.info(f"Received query: {request.question}")
            return await app.state.ingest_service.query_docs(request)
        except ValidationError as ve:
            logger.warning(f"Query validation error: {ve}")
            raise HTTPException(status_code=422, detail=ve.errors())
        except Exception as e:
            logger.error(f"Query failed: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    @app.get("/list-documents")
    async def list_documents():
        try:
            return await app.state.ingest_service.list_all_documents()
        except Exception as e:
            logger.error(f"Error listing documents: {e}")
            raise HTTPException(status_code=500, detail=str(e))
