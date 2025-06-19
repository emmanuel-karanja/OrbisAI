import os
import base64
import re
import magic  # pip install python-magic
from pydantic import BaseModel, validator, root_validator, Field, ValidationError

ALLOWED_EXTENSIONS = {'.pdf', '.txt', '.md'}
MAX_FILE_SIZE_MB = 10  # Max ~10MB decoded
MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024


class IngestionRequest(BaseModel):
    filename: str
    content: str  # Base64 string

    @validator("filename")
    def validate_filename(cls, v):
        if not v:
            raise ValueError("Filename is required")
        if ".." in v or "/" in v or "\\" in v:
            raise ValueError("Filename contains unsafe path characters")
        ext = os.path.splitext(v)[1].lower()
        if ext not in ALLOWED_EXTENSIONS:
            raise ValueError(f"Unsupported file type: {ext}")
        return v

    @validator("content")
    def validate_base64_content(cls, v):
        try:
            decoded = base64.b64decode(v, validate=True)
        except Exception:
            raise ValueError("Content is not valid base64")
        if len(decoded) > MAX_FILE_SIZE_BYTES:
            raise ValueError(f"Decoded file exceeds {MAX_FILE_SIZE_MB}MB size limit")
        return v

    @root_validator
    def validate_file_mime_type(cls, values):
        filename = values.get("filename")
        content = values.get("content")

        if not filename or not content:
            return values  # Skip further checks

        ext = os.path.splitext(filename)[1].lower()
        decoded = base64.b64decode(content)

        try:
            mime = magic.from_buffer(decoded, mime=True)
        except Exception as e:
            raise ValueError(f"Failed to detect MIME type: {e}")

        valid = False
        if ext == ".pdf" and mime == "application/pdf":
            valid = True
        elif ext in {".txt", ".md"} and mime.startswith("text/"):
            valid = True

        if not valid:
            raise ValueError(f"File type mismatch: {ext} does not match detected MIME type '{mime}'")

        return values


class QueryRequest(BaseModel):
    question: str = Field(..., min_length=3, description="Must be at least 3 characters")


class SummarizeRequest(IngestionRequest):
    pass
