from fastapi import FastAPI, Request
from sentence_transformers import SentenceTransformer

app = FastAPI()
model = SentenceTransformer("all-MiniLM-L6-v2")

@app.post("/embed")
async def embed(request: Request):
    data = await request.json()
    texts = data.get("texts", [])
    return {"embeddings": model.encode(texts).tolist()}
