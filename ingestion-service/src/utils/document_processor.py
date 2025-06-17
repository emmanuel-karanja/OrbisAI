import base64
import hashlib
import pdfplumber
from io import BytesIO
import markdown
from bs4 import BeautifulSoup
from langchain.text_splitter import RecursiveCharacterTextSplitter
import redis
from utils.logger import setup_logger
import os

LOG_DIR = os.getenv("LOG_DIR", "logs")
logger = setup_logger(name="document_processor", log_dir=LOG_DIR, log_to_file=True)



class DocumentProcessor:
    def __init__(self):
        try:
            self.redis = redis.Redis(host='redis', port=6379, decode_responses=True)
            logger.info("Connected to Redis.")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise

        self.splitter = RecursiveCharacterTextSplitter(chunk_size=500, chunk_overlap=100)

    def extract_text_and_metadata(self, filename: str, base64_content: str):
        logger.info(f"Extracting text from file: {filename}")
        pages = []

        try:
            content_bytes = base64.b64decode(base64_content)
            ext = filename.split(".")[-1].lower()

            if ext == "pdf":
                try:
                    with pdfplumber.open(BytesIO(content_bytes)) as pdf:
                        for page_num, page in enumerate(pdf.pages, start=1):
                            text = page.extract_text() or ""
                            paragraphs = [p.strip() for p in text.split('\n\n') if p.strip()]
                            for para_num, para in enumerate(paragraphs, start=1):
                                pages.append({"page": page_num, "paragraph": para_num, "text": para})
                except Exception as e:
                    logger.error(f"Failed to extract text from PDF: {e}")

            elif ext == "md":
                try:
                    md_text = content_bytes.decode('utf-8', errors='ignore')
                    html = markdown.markdown(md_text)
                    soup = BeautifulSoup(html, features="html.parser")
                    paragraphs = soup.find_all('p')
                    for para_num, para in enumerate(paragraphs, start=1):
                        text = para.get_text(separator="\n").strip()
                        if text:
                            pages.append({"page": 1, "paragraph": para_num, "text": text})
                except Exception as e:
                    logger.error(f"Failed to parse Markdown file: {e}")

            elif ext in {"akn", "xml"}:
                try:
                    xml_text = content_bytes.decode("utf-8", errors="ignore")
                    soup = BeautifulSoup(xml_text, "xml")
                    elements = soup.find_all(['article', 'section', 'clause', 'paragraph'])
                    if not elements:
                        elements = soup.find_all(['body', 'main', 'text'])  # fallback
                    for i, el in enumerate(elements, start=1):
                        text = el.get_text(separator="\n", strip=True)
                        if text:
                            pages.append({"page": 1, "paragraph": i, "text": text})
                except Exception as e:
                    logger.error(f"Failed to parse AKN/XML file: {e}")

            else:
                try:
                    text = content_bytes.decode('utf-8', errors='ignore')
                    pages = [{"page": 1, "paragraph": 1, "text": text}]
                except Exception as e:
                    logger.error(f"Failed to decode plain text file: {e}")

        except Exception as e:
            logger.error(f"Error processing file '{filename}': {e}")

        logger.info(f"Extracted {len(pages)} sections from file: {filename}")
        return pages

    def chunk_text_with_metadata(self, pages):
        chunks = []
        metadata = []

        try:
            for page_info in pages:
                text = page_info["text"]
                split_chunks = self.splitter.split_text(text)
                for chunk in split_chunks:
                    chunks.append(chunk)
                    metadata.append({
                        "doc_name": page_info.get("doc_name", ""),
                        "page": page_info["page"],
                        "paragraph": page_info["paragraph"]
                    })
            logger.info(f"Chunked into {len(chunks)} segments.")
        except Exception as e:
            logger.error(f"Error chunking text: {e}")

        return chunks, metadata

    def document_exists_and_handle_update(self, filename: str, content_bytes: bytes) -> bool:
        try:
            checksum = hashlib.sha256(content_bytes).hexdigest()
            saved_checksum = self.redis.get(f"doc_checksum:{filename}")
            exists = saved_checksum and saved_checksum == checksum
            logger.info(f"Checksum comparison for '{filename}': {'match' if exists else 'mismatch'}")
            return exists
        except Exception as e:
            logger.error(f"Error checking document checksum: {e}")
            return False

    def save_document_checksum(self, filename: str, content_bytes: bytes):
        try:
            checksum = hashlib.sha256(content_bytes).hexdigest()
            self.redis.set(f"doc_checksum:{filename}", checksum)
            logger.info(f"Checksum saved for file: {filename}")
        except Exception as e:
            logger.error(f"Error saving document checksum: {e}")
