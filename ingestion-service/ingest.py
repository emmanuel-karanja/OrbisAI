from fastapi import FastAPI
from pydantic import BaseModel
import requests
import chromadb
import base64
import fitz  # PyMuPDF
import markdown
from bs4 import BeautifulSoup
from transformers import pipeline
from langchain.text_splitter import RecursiveCharacterTextSplitter  # <- new import

app = FastAPI()

client = chromadb.HttpClient(host="chromadb", port=8000)
collection = client.get_or_create_collection("docs")

# Summarizer pipeline
summarizer = pipeline("summarization", model="facebook/bart-large-cnn")

class IngestRequest(BaseModel):
    filename: str
    content: str

def extract_text(filename: str, base64_content: str) -> str:
    content_bytes = base64.b64decode(base64_content)
    ext = filename.split(".")[-1].lower()
    if ext == "pdf":
        doc = fitz.open(stream=content_bytes, filetype="pdf")
        text = ""
        for page in doc:
            text += page.get_text()
        return text
    elif ext == "md":
        md_text = content_bytes.decode('utf-8', errors='ignore')
        html = markdown.markdown(md_text)
        soup = BeautifulSoup(html, features="html.parser")
        return soup.get_text(separator="\n")
    else:
        return content_bytes.decode('utf-8', errors='ignore')

def chunk_text(text, chunk_size=500, chunk_overlap=100):
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap
    )
    return splitter.split_text(text)

@app.post("/ingest")
async def ingest(request: IngestRequest):
    text = extract_text(request.filename, request.content)

    # Summarize the full text first
    summary = summarizer(text, max_length=100, min_length=30, do_sample=False)[0]['summary_text']

    # Chunk the text using RecursiveCharacterTextSplitter
    chunks = chunk_text(text)

    # Embed each chunk
    embeddings = []
    for chunk in chunks:
        r = requests.post("http://embedder:5000/embed", json={"texts": [chunk]})
        embedding = r.json()["embeddings"][0]
        embeddings.append(embedding)

    # Store chunks with ids referencing filename + chunk number
    ids = [f"{request.filename}_chunk{i}" for i in range(len(chunks))]
    collection.add(documents=chunks, embeddings=embeddings, ids=ids)

    # Optionally store summary as a separate doc (can be retrieved too)
    r = requests.post("http://embedder:5000/embed", json={"texts": [summary]})
    summary_embedding = r.json()["embeddings"][0]
    collection.add(documents=[summary], embeddings=[summary_embedding], ids=[f"{request.filename}_summary"])

    return {"status": "ok", "summary": summary}
