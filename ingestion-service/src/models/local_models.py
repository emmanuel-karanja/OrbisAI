from pydantic import BaseModel


class IngestRequest(BaseModel):
    filename: str
    content: str


class QueryRequest(BaseModel):
    question: str


class SummarizeRequest(BaseModel):
    filename: str
    content: str
