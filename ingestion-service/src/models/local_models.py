import os
import base64
import magic  # pip install python-magic
from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator

ALLOWED_EXTENSIONS = {'.pdf', '.txt', '.md'}
MAX_FILE_SIZE_MB = 10
MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024


class IngestionRequest(BaseModel):
    filename: str
    content: str  # base64-encoded string

    @field_validator("filename", mode="before")
    @classmethod
    def validate_filename(cls, v: str) -> str:
        if not v:
            raise ValueError("Filename is required")
        if ".." in v or "/" in v or "\\" in v:
            raise ValueError("Filename contains unsafe path characters")
        ext = os.path.splitext(v)[1].lower()
        if ext not in ALLOWED_EXTENSIONS:
            raise ValueError(f"Unsupported file type: {ext}")
        return v

    @field_validator("content", mode="before")
    @classmethod
    def validate_base64_content(cls, v: str) -> str:
        try:
            decoded = base64.b64decode(v, validate=True)
        except Exception:
            raise ValueError("Content is not valid base64")
        if len(decoded) > MAX_FILE_SIZE_BYTES:
            raise ValueError(f"Decoded file exceeds {MAX_FILE_SIZE_MB}MB size limit")
        return v

    @model_validator(mode="after")
    def validate_file_mime_type(self) -> "IngestionRequest":
        ext = os.path.splitext(self.filename)[1].lower()
        decoded = base64.b64decode(self.content)

        try:
            mime = magic.from_buffer(decoded, mime=True)
        except Exception as e:
            raise ValueError(f"Failed to detect MIME type: {e}")

        if ext == ".pdf" and mime != "application/pdf":
            raise ValueError(f"File type mismatch: {ext} does not match detected MIME type '{mime}'")
        elif ext in {".txt", ".md"} and not mime.startswith("text/"):
            raise ValueError(f"File type mismatch: {ext} does not match detected MIME type '{mime}'")

        return self


class QueryRequest(BaseModel):
    question: str = Field(..., min_length=3, description="Must be at least 3 characters")


class SummarizeRequest(IngestionRequest):
    pass
