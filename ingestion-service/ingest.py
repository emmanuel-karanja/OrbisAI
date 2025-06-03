from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel
import requests
import chromadb
import base64
import pdfplumber
from io import BytesIO
import markdown
from bs4 import BeautifulSoup
from transformers import pipeline
from langchain.text_splitter import RecursiveCharacterTextSplitter
from utils.logger import setup_logger
import redis
import hashlib
from typing import List
import asyncio
from functools import wraps
import random

logger = setup_logger("ingestion-service")

BATCH_SIZE = 50
SUMMARY_CHUNK_SIZE = 1000

app = FastAPI()

client = chromadb.HttpClient(host="chromadb", port=8000)
collection = client.get_or_create_collection("docs")

summarizer = pipeline("summarization", model="sshleifer/distilbart-cnn-12-6")
qa_pipeline = pipeline("question-answering", model="distilbert-base-cased-distilled-squad")

r = redis.Redis(host='redis', port=6379, decode_responses=True)

class IngestRequest(BaseModel):
    filename: str
    content: str

class QueryRequest(BaseModel):
    question: str

class SummarizeRequest(BaseModel):
    filename: str
    content: str

def extract_text_and_metadata(filename: str, base64_content: str):
    logger.info(f"Extracting text from file: {filename}")
    content_bytes = base64.b64decode(base64_content)
    ext = filename.split(".")[-1].lower()

    pages = []

    if ext == "pdf":
        with pdfplumber.open(BytesIO(content_bytes)) as pdf:
            for page_num, page in enumerate(pdf.pages, start=1):
                text = page.extract_text() or ""
                paragraphs = [p.strip() for p in text.split('\n\n') if p.strip()]
                for para_num, para in enumerate(paragraphs, start=1):
                    pages.append({"page": page_num, "paragraph": para_num, "text": para})

    elif ext == "md":
        md_text = content_bytes.decode('utf-8', errors='ignore')
        html = markdown.markdown(md_text)
        soup = BeautifulSoup(html, features="html.parser")
        paragraphs = soup.find_all('p')
        for para_num, para in enumerate(paragraphs, start=1):
            text = para.get_text(separator="\n").strip()
            if text:
                pages.append({"page": 1, "paragraph": para_num, "text": text})
    else:
        text = content_bytes.decode('utf-8', errors='ignore')
        pages = [{"page": 1, "paragraph": 1, "text": text}]

    logger.info(f"Extracted {len(pages)} sections from file")
    return pages

def chunk_text_with_metadata(pages, chunk_size=500, chunk_overlap=100):
    splitter = RecursiveCharacterTextSplitter(chunk_size=chunk_size, chunk_overlap=chunk_overlap)
    chunks = []
    metadata = []

    for page_info in pages:
        text = page_info["text"]
        split_chunks = splitter.split_text(text)
        for chunk in split_chunks:
            chunks.append(chunk)
            metadata.append({
                "doc_name": page_info.get("doc_name", ""),
                "page": page_info["page"],
                "paragraph": page_info["paragraph"]
            })

    logger.info(f"Chunked into {len(chunks)} segments")
    return chunks, metadata

def delete_docs_by_name(doc_name: str):
    results = collection.get(where={"doc_name": doc_name})
    ids_to_delete = results.get("ids", [])
    if ids_to_delete:
        collection.delete(ids=ids_to_delete)
        logger.info(f"Deleted {len(ids_to_delete)} embeddings for document '{doc_name}'")
    else:
        logger.info(f"No existing embeddings found to delete for document '{doc_name}'")

def document_exists_and_handle_update(filename: str, content_bytes: bytes) -> bool:
    checksum = hashlib.sha256(content_bytes).hexdigest()
    saved_checksum = r.get(f"doc_checksum:{filename}")
    return saved_checksum and saved_checksum == checksum

def save_document_checksum(filename: str, content_bytes: bytes):
    checksum = hashlib.sha256(content_bytes).hexdigest()
    r.set(f"doc_checksum:{filename}", checksum)

def ingest_document(request: IngestRequest, collection, summarizer, redis_client):
    logger.info(f"Starting ingestion for file: {request.filename}")
    content_bytes = base64.b64decode(request.content)

    if document_exists_and_handle_update(request.filename, content_bytes):
        logger.info(f"Document {request.filename} already ingested with same content, skipping ingestion.")
        return

    pages = extract_text_and_metadata(request.filename, request.content)
    for page in pages:
        page["doc_name"] = request.filename

    chunks, metadatas = chunk_text_with_metadata(pages)

    logger.info(f"Embedding {len(chunks)} chunks in batches of {BATCH_SIZE}...")

    for i in range(0, len(chunks), BATCH_SIZE):
        batch_chunks = chunks[i : i + BATCH_SIZE]
        batch_metadatas = metadatas[i : i + BATCH_SIZE]
        batch_embeddings = batch_embed_texts(batch_chunks)

        if not batch_embeddings:
            logger.warning(f"No embeddings returned for batch starting at chunk {i}, skipping batch.")
            continue

        batch_ids = [f"{request.filename}_chunk_{i + idx}" for idx in range(len(batch_chunks))]
        collection.add(
            documents=batch_chunks,
            embeddings=batch_embeddings,
            ids=batch_ids,
            metadatas=batch_metadatas,
        )

    full_text = "\n\n".join([p["text"] for p in pages])
    logger.info("Generating hierarchical summary...")
    summary = hierarchical_summarize(full_text, summarizer)

    logger.info("Embedding summary...")
    summary_embedding = batch_embed_texts([summary])
    if summary_embedding:
        collection.add(
            documents=[summary],
            embeddings=[summary_embedding[0]],
            ids=[f"{request.filename}_summary"],
            metadatas=[{"doc_name": request.filename, "page": 0, "paragraph": 0, "summary": True}],
        )
    else:
        logger.warning("No embedding returned for summary, skipping summary storage.")

    save_document_checksum(request.filename, content_bytes)
    logger.info(f"Ingestion completed for {request.filename}")

@async_retry()
async def batch_embed_texts(texts: List[str]) -> List[List[float]]:
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post("http://embedder:5000/embed", json={"texts": texts})
            response.raise_for_status()
            return response.json()["embeddings"]
    except Exception as e:
        logger.error(f"Embedding request failed: {e}")
        return []

def async_retry(retries=3, backoff=1.5, exceptions=(Exception,)):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            delay = 1
            for attempt in range(retries):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    if attempt == retries - 1:
                        raise
                    logger.warning(f"{func.__name__} failed (attempt {attempt+1}), retrying in {delay:.1f}s: {e}")
                    await asyncio.sleep(delay)
                    delay *= backoff + random.uniform(0, 0.5)
        return wrapper
    return decorator


@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/ingest")
async def ingest(request: IngestRequest, background_tasks: BackgroundTasks):
    background_tasks.add_task(ingest_document, request, collection, summarizer, r)
    logger.info(f"Ingestion scheduled for file: {request.filename}")
    return {"status": "accepted", "message": "Document ingestion started in background"}

@app.post("/query")
def query_docs(request: QueryRequest):
    logger.info(f"Received query: {request.question}")
    embedding_response = requests.post("http://embedder:5000/embed", json={"texts": [request.question]})
    question_embedding = embedding_response.json()["embeddings"][0]

    results = collection.query(query_embeddings=[question_embedding], n_results=3)
    documents = results["documents"][0]
    metadatas = results["metadatas"][0]

    summary_docs = collection.get(where={"summary": True})
    summary_text = summary_docs["documents"][0] if summary_docs["documents"] else ""

    context_parts = []
    if summary_text:
        context_parts.append("Summary:\n" + summary_text)
    context_parts.append("Details:\n" + "\n".join(documents))
    context = "\n\n".join(context_parts)

    logger.info("Running QA pipeline...")
    answer = qa_pipeline(question=request.question, context=context)

    logger.info("Query processed successfully")
    return {
        "question": request.question,
        "answer": answer["answer"],
        "score": answer["score"],
        "context": documents,
        "summary": summary_text,
        "sources": metadatas
    }

def hierarchical_summarize(text: str, summarizer, chunk_size=SUMMARY_CHUNK_SIZE) -> str:
    """
    Summarize long text by chunking it, summarizing each chunk,
    then summarizing the combined summaries.
    """
    chunks = [text[i : i + chunk_size] for i in range(0, len(text), chunk_size)]
    logger.info(f"Summarizing in {len(chunks)} chunks")

    summaries = []
    for i, chunk in enumerate(chunks):
        try:
            summary = summarizer(chunk, max_length=100, min_length=30, do_sample=False)[0]['summary_text']
        except Exception as e:
            logger.error(f"Error summarizing chunk {i}: {e}")
            summary = ""
        summaries.append(summary)

    combined_summary_text = " ".join(summaries)
    logger.info("Summarizing combined summaries")

    try:
        final_summary = summarizer(combined_summary_text, max_length=100, min_length=30, do_sample=False)[0]['summary_text']
    except Exception as e:
        logger.error(f"Error summarizing combined text: {e}")
        final_summary = combined_summary_text  # fallback

    return final_summary
