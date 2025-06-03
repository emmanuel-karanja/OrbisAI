from fastapi import FastAPI
from pydantic import BaseModel
import requests
import chromadb
import base64
import fitz  # PyMuPDF
import markdown
from bs4 import BeautifulSoup
from transformers import pipeline
from langchain.text_splitter import RecursiveCharacterTextSplitter
from utils.logger import setup_logger
import redis
import hashlib

logger = setup_logger("ingestion-service")

app = FastAPI()

client = chromadb.HttpClient(host="chromadb", port=8000)
collection = client.get_or_create_collection("docs")

summarizer = pipeline("summarization", model="sshleifer/distilbart-cnn-12-6")
qa_pipeline = pipeline("question-answering", model="distilbert-base-cased-distilled-squad")

class IngestRequest(BaseModel):
    filename: str
    content: str  # base64 encoded

class QueryRequest(BaseModel):
    question: str

class SummarizeRequest(BaseModel):
    filename: str
    content: str  # base64 encoded

def extract_text_and_metadata(filename: str, base64_content: str):
    logger.info(f"Extracting text from file: {filename}")
    content_bytes = base64.b64decode(base64_content)
    ext = filename.split(".")[-1].lower()

    pages = []

    if ext == "pdf":
        doc = fitz.open(stream=content_bytes, filetype="pdf")
        for page_num, page in enumerate(doc, start=1):
            text = page.get_text()
            paragraphs = [p.strip() for p in text.split('\n\n') if p.strip()]
            for para_num, para in enumerate(paragraphs, start=1):
                pages.append({
                    "page": page_num,
                    "paragraph": para_num,
                    "text": para
                })
    elif ext == "md":
        md_text = content_bytes.decode('utf-8', errors='ignore')
        html = markdown.markdown(md_text)
        soup = BeautifulSoup(html, features="html.parser")
        paragraphs = soup.find_all('p')
        for para_num, para in enumerate(paragraphs, start=1):
            text = para.get_text(separator="\n").strip()
            if text:
                pages.append({
                    "page": 1,
                    "paragraph": para_num,
                    "text": text
                })
    else:
        text = content_bytes.decode('utf-8', errors='ignore')
        pages = [{"page": 1, "paragraph": 1, "text": text}]

    logger.info(f"Extracted {len(pages)} sections from file")
    return pages

def chunk_text_with_metadata(pages, chunk_size=500, chunk_overlap=100):
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap
    )
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

# Connect to redis (assuming default port 6379 and no password)
r = redis.Redis(host='redis', port=6379, decode_responses=True)

def get_checksum(content_bytes):
    return hashlib.sha256(content_bytes).hexdigest()

def delete_docs_by_name(doc_name: str):
    results = collection.get(where={"doc_name": doc_name})
    ids_to_delete = results.get("ids", [])
    if ids_to_delete:
        collection.delete(ids=ids_to_delete)
        logger.info(f"Deleted {len(ids_to_delete)} embeddings for document '{doc_name}'")
    else:
        logger.info(f"No existing embeddings found to delete for document '{doc_name}'")

def document_exists_and_handle_update(filename, content_bytes):
    checksum = get_checksum(content_bytes)
    stored_checksum = r.get(filename)
    if stored_checksum:
        if stored_checksum == checksum:
            return True  # Document unchanged, skip ingestion
        else:
            # Document updated - delete old embeddings & redis key
            logger.info(f"Document {filename} changed - deleting old embeddings")
            delete_docs_by_name(filename)
            r.delete(filename)
            return False
    else:
        return False

def save_document_checksum(filename, content_bytes):
    checksum = get_checksum(content_bytes)
    r.set(filename, checksum)

@app.post("/ingest")
async def ingest(request: IngestRequest):
    logger.info(f"Starting ingestion for file: {request.filename}")

    content_bytes = base64.b64decode(request.content)

    if document_exists_and_handle_update(request.filename, content_bytes):
        logger.info(f"Document {request.filename} already ingested with same content, skipping.")
        return {"status": "ok", "summary": "Document already ingested with same content."}

    pages = extract_text_and_metadata(request.filename, request.content)
    for page_info in pages:
        page_info["doc_name"] = request.filename

    chunks, metadatas = chunk_text_with_metadata(pages)
    full_text = "\n\n".join([p["text"] for p in pages])

    logger.info("Generating summary...")
    summary = summarizer(full_text, max_length=100, min_length=30, do_sample=False)[0]['summary_text']

    logger.info("Embedding document chunks...")
    embeddings = []
    for chunk in chunks:
        r = requests.post("http://embedder:5000/embed", json={"texts": [chunk]})
        embedding = r.json()["embeddings"][0]
        embeddings.append(embedding)

    ids = [f"{request.filename}_chunk{i}" for i in range(len(chunks))]
    collection.add(documents=chunks, embeddings=embeddings, ids=ids, metadatas=metadatas)

    logger.info("Embedding summary...")
    r = requests.post("http://embedder:5000/embed", json={"texts": [summary]})
    summary_embedding = r.json()["embeddings"][0]
    collection.add(
        documents=[summary],
        embeddings=[summary_embedding],
        ids=[f"{request.filename}_summary"],
        metadatas=[{"doc_name": request.filename, "page": 0, "paragraph": 0, "summary": True}]
    )

    save_document_checksum(request.filename, content_bytes)

    logger.info(f"Ingestion completed for {request.filename}")
    return {"status": "ok", "summary": summary}

@app.post("/query")
def query_docs(request: QueryRequest):
    logger.info(f"Received query: {request.question}")
    r = requests.post("http://embedder:5000/embed", json={"texts": [request.question]})
    question_embedding = r.json()["embeddings"][0]

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

@app.post("/summarize")
async def summarize(request: SummarizeRequest):
    logger.info(f"Summarizing file: {request.filename}")
    pages = extract_text_and_metadata(request.filename, request.content)
    full_text = "\n\n".join([p["text"] for p in pages])

    summary = summarizer(full_text, max_length=150, min_length=40, do_sample=False)[0]['summary_text']

    logger.info("Summary generated")
    return {"filename": request.filename, "summary": summary}
