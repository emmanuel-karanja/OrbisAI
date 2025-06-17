from fastapi import FastAPI, BackgroundTasks
from models.local_models import IngestRequest, QueryRequest


def register_ingestion_routes(app: FastAPI, logger):
    @app.get("/health")
    def health():
        return {"status": "ok"}

    @app.post("/ingest")
    async def ingest(request: IngestRequest, background_tasks: BackgroundTasks):
        background_tasks.add_task(app.state.ingest_service.ingest_document, request)
        logger.info(f"Ingestion scheduled for file: {request.filename}")
        return {"status": "accepted", "message": "Document ingestion started in background"}

    @app.get("/ingest-status/{filename}")
    def get_ingest_status(filename: str):
        return app.state.ingest_service.ingest_status(filename)

    @app.post("/query")
    def query(request: QueryRequest):
        return app.state.ingest_service.query_docs(request)

    @app.get("/list-documents")
    def list_documents():
        return app.state.ingest_service.list_all_documents()
