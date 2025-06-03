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

app = FastAPI()

client = chromadb.HttpClient(host="chromadb", port=8000)
collection = client.get_or_create_collection("docs")

summarizer = pipeline("summarization", model="sshleifer/distilbart-cnn-12-6")
qa_pipeline = pipeline("question-answering", model="distilbert-base-cased-distilled-squad")

class IngestRequest(BaseModel):
    filename: str
    content: str

class QueryRequest(BaseModel):
    question: str

def extract_text_and_metadata(filename: str, base64_content: str):
    content_bytes = base64.b64decode(base64_content)
    ext = filename.split(".")[-1].lower()
    
    if ext == "pdf":
        doc = fitz.open(stream=content_bytes, filetype="pdf")
        pages = []
        for page_num, page in enumerate(doc, start=1):
            text = page.get_text()
            paragraphs = [p.strip() for p in text.split('\n\n') if p.strip()]
            for para_num, para in enumerate(paragraphs, start=1):
                pages.append({
                    "page": page_num,
                    "paragraph": para_num,
                    "text": para
                })
        return pages
    
    elif ext == "md":
        md_text = content_bytes.decode('utf-8', errors='ignore')
        html = markdown.markdown(md_text)
        soup = BeautifulSoup(html, features="html.parser")
        paragraphs = soup.find_all('p')
        pages = []
        for para_num, para in enumerate(paragraphs, start=1):
            text = para.get_text(separator="\n").strip()
            if text:
                pages.append({
                    "page": 1,
                    "paragraph": para_num,
                    "text": text
                })
        return pages

    else:
        text = content_bytes.decode('utf-8', errors='ignore')
        return [{"page": 1, "paragraph": 1, "text": text}]

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
    return chunks, metadata

@app.post("/ingest")
async def ingest(request: IngestRequest):
    pages = extract_text_and_metadata(request.filename, request.content)
    for page_info in pages:
        page_info["doc_name"] = request.filename

    chunks, metadatas = chunk_text_with_metadata(pages)
    full_text = "\n\n".join([p["text"] for p in pages])
    summary = summarizer(full_text, max_length=100, min_length=30, do_sample=False)[0]['summary_text']

    embeddings = []
    for chunk in chunks:
        r = requests.post("http://embedder:5000/embed", json={"texts": [chunk]})
        embedding = r.json()["embeddings"][0]
        embeddings.append(embedding)

    ids = [f"{request.filename}_chunk{i}" for i in range(len(chunks))]
    collection.add(documents=chunks, embeddings=embeddings, ids=ids, metadatas=metadatas)

    r = requests.post("http://embedder:5000/embed", json={"texts": [summary]})
    summary_embedding = r.json()["embeddings"][0]
    collection.add(
        documents=[summary],
        embeddings=[summary_embedding],
        ids=[f"{request.filename}_summary"],
        metadatas=[{"doc_name": request.filename, "page": 0, "paragraph": 0, "summary": True}]
    )

    return {"status": "ok", "summary": summary}

@app.post("/query")
def query_docs(request: QueryRequest):
    r = requests.post("http://embedder:5000/embed", json={"texts": [request.question]})
    question_embedding = r.json()["embeddings"][0]

    results = collection.query(query_embeddings=[question_embedding], n_results=3)
    documents = results["documents"][0]
    metadatas = results["metadatas"][0]
    context = " ".join(documents)

    answer = qa_pipeline(question=request.question, context=context)

    return {
        "question": request.question,
        "answer": answer["answer"],
        "score": answer["score"],
        "context": documents,
        "sources": metadatas
    }
