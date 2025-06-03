from fastapi import FastAPI, Request
from sentence_transformers import SentenceTransformer
from utils.logger import setup_logger

logger = setup_logger("embedder-service")

app = FastAPI()
model = SentenceTransformer("all-MiniLM-L6-v2")
logger.info("SentenceTransformer model loaded.")

@app.post("/embed")
async def embed(request: Request):
    try:
        data = await request.json()
        texts = data.get("texts", [])
        logger.info(f"Received {len(texts)} texts for embedding.")
        embeddings = model.encode(texts).tolist()
        logger.info("Embeddings generated successfully.")
        return {"embeddings": embeddings}
    except Exception as e:
        logger.error(f"Embedding failed: {str(e)}", extra={"exception": str(e)})
        return {"error": "Embedding failed", "details": str(e)}
