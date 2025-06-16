import base64
import hashlib
import pdfplumber
from io import BytesIO
import markdown
from bs4 import BeautifulSoup
from langchain.text_splitter import RecursiveCharacterTextSplitter
import redis
from utils.logger import setup_logger

logger = setup_logger(name="document-processor")


class DocumentProcessor:
    def __init__(self):
        self.redis = redis.Redis(host='redis', port=6379, decode_responses=True)
        self.splitter = RecursiveCharacterTextSplitter(chunk_size=500, chunk_overlap=100)

    def extract_text_and_metadata(self, filename: str, base64_content: str):
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
                logger.error(f"Failed to parse AKN file: {e}")

        else:
            text = content_bytes.decode('utf-8', errors='ignore')
            pages = [{"page": 1, "paragraph": 1, "text": text}]

        logger.info(f"Extracted {len(pages)} sections from file")
        return pages

    def chunk_text_with_metadata(self, pages):
        chunks = []
        metadata = []

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

        logger.info(f"Chunked into {len(chunks)} segments")
        return chunks, metadata

    def document_exists_and_handle_update(self, filename: str, content_bytes: bytes) -> bool:
        checksum = hashlib.sha256(content_bytes).hexdigest()
        saved_checksum = self.redis.get(f"doc_checksum:{filename}")
        return saved_checksum and saved_checksum == checksum

    def save_document_checksum(self, filename: str, content_bytes: bytes):
        checksum = hashlib.sha256(content_bytes).hexdigest()
        self.redis.set(f"doc_checksum:{filename}", checksum)
