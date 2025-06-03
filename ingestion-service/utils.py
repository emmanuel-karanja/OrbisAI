import base64
import hashlib
import pdfplumber
from io import BytesIO
import markdown
from bs4 import BeautifulSoup
from langchain.text_splitter import RecursiveCharacterTextSplitter
from utils.logger import setup_logger
import redis
from logger import setup_logger


logger = setup_logger(name="ingest")

r = redis.Redis(host='redis', port=6379, decode_responses=True)

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


def document_exists_and_handle_update(filename: str, content_bytes: bytes) -> bool:
    checksum = hashlib.sha256(content_bytes).hexdigest()
    saved_checksum = r.get(f"doc_checksum:{filename}")
    return saved_checksum and saved_checksum == checksum


def save_document_checksum(filename: str, content_bytes: bytes):
    checksum = hashlib.sha256(content_bytes).hexdigest()
    r.set(f"doc_checksum:{filename}", checksum)
