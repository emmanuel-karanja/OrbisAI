import os
import base64
import uuid
from typing import List
from transformers import pipeline, AutoModelForQuestionAnswering, AutoTokenizer
from sentence_transformers import SentenceTransformer
from utils.logger import setup_logger
from utils.document_processor import DocumentProcessor
from utils.redis_client import r
from db.qdrant_db_client import QdrantVectorDB
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
import torch
from sentence_transformers import CrossEncoder
import numpy as np


# Load environment variables
load_dotenv(override=True)

# Set Torch thread count
torch.set_num_threads(int(os.getenv("TORCH_NUM_THREADS", 1)))

# Configuration from environment
BATCH_SIZE = int(os.getenv("EMBED_BATCH_SIZE", 50))
SUMMARY_CHUNK_SIZE = int(os.getenv("SUMMARY_CHUNK_SIZE", 500))
SENTENCE_MODEL = os.getenv("SENTENCE_MODEL", "nomic-ai/nomic-embed-text-v1")
SUMMARIZER_MODEL = os.getenv("SUMMARIZER_MODEL", "sshleifer/distilbart-cnn-12-6")
QA_MODEL = os.getenv("QA_MODEL", "allenai/longformer-base-4096")

LOG_DIR = os.getenv("LOG_DIR", "logs")
logger = setup_logger(name="ingestion-service", log_dir=LOG_DIR, log_to_file=True)

class IngestService:
    def __init__(self):
        logger.info("Initializing services...")
        self.load_sentence_model()
        logger.info("Loading summarizer pipeline...")
        self.summarizer = pipeline("summarization", model=SUMMARIZER_MODEL)
        logger.info("Summarizer pipeline loaded.")
        logger.info(f"SUMMARIZER MODEL: {self.summarizer}")
        self.load_qa_model()
        logger.info("Connecting to vector database (Qdrant)...")
        self.vector_db = QdrantVectorDB()
        logger.info("Vector DB initialized.")
        logger.info("Instantiating DocumentProcessor...")
        self.doc_processor = DocumentProcessor()
        logger.info("Loading CrossEncoder model for reranking...")
        self.cross_encoder = CrossEncoder("cross-encoder/ms-marco-MiniLM-L-6-v2")
        logger.info("CrossEncoder model loaded.")

    def batch_embed_texts(self, texts: List[str]) -> List[List[float]]:
        try:
            # Apply E5 formatting: prefix queries and passages
            formatted = [f"query: {t}" if t.endswith("?") else f"passage: {t}" for t in texts]
            return self.model.encode(formatted).tolist()
        except Exception as e:
            logger.error(f"Embedding failed: {e}")
            return []

    def load_sentence_model(self):
        logger.info("Loading SentenceTransformer model...")
        try:
            logger.info(f"Sentence Model: {SENTENCE_MODEL}")
            self.model = SentenceTransformer(SENTENCE_MODEL,trust_remote_code=True)
            logger.info("Model downloaded and loaded successfully.")
        except Exception as e:
            logger.error(f"Error loading model {SENTENCE_MODEL}: {e}")
            raise
        logger.info(f"Model embedding dim: {self.model.get_sentence_embedding_dimension()}")

    def load_qa_model(self):
        logger.info("Loading QA pipeline...")
        try:
            tokenizer = AutoTokenizer.from_pretrained(QA_MODEL)
            model = AutoModelForQuestionAnswering.from_pretrained(QA_MODEL)
            device = 0 if torch.cuda.is_available() else -1
            self.qa_pipeline = pipeline("question-answering", model=model, tokenizer=tokenizer, device=device)
            self.qa_tokenizer = tokenizer
            logger.info("QA pipeline loaded successfully.")
        except Exception as e:
            logger.error(f"Failed to load QA model '{QA_MODEL}': {e}")
            raise

    def hierarchical_summarize(self, text: str, chunk_size=SUMMARY_CHUNK_SIZE) -> str:
        chunks = [text[i:i + chunk_size] for i in range(0, len(text), chunk_size)]
        summaries = []
        for i, chunk in enumerate(chunks):
            try:
                chunk_length = len(chunk.split())
                max_len = min(500, int(chunk_length * 0.5))  # Make summary about half the length
                max_len = max(max_len, 30)  # Avoid too-short max length
                summary = self.summarizer(chunk, max_length=max_len, min_length=20, do_sample=False)[0]['summary_text']

            except Exception as e:
                logger.error(f"Error summarizing chunk {i}: {e}")
                summary = ""
            summaries.append(summary)
        combined_summary = " ".join(summaries)
        try:
            combined_length = len(combined_summary.split())
            final_max_len = min(100, int(combined_length * 0.5))
            final_max_len = max(final_max_len, 30)

            final_summary = self.summarizer(
                combined_summary,
                max_length=final_max_len,
                min_length=20,
                do_sample=False
            )[0]['summary_text']

        except Exception as e:
            logger.error(f"Error summarizing combined text: {e}")
            final_summary = combined_summary
        return final_summary

    def delete_docs_by_name(self, doc_name: str):
        self.vector_db.delete_documents({"doc_name": doc_name})

    def ingest_document(self, request):
        doc_key = f"ingestion_status:{request.filename}"
        r.set(doc_key, "started")
        logger.info(f"Starting ingestion for file: {request.filename}")
        try:
            content_bytes = base64.b64decode(request.content)
            if self.doc_processor.document_exists_and_handle_update(request.filename, content_bytes):
                msg = f"Document {request.filename} already ingested with same content, skipping."
                logger.info(msg)
                r.set(doc_key, f"Skipped: {msg}")
                return
            pages = self.doc_processor.extract_text_and_metadata(request.filename, request.content)
            for page in pages:
                page["doc_name"] = request.filename
            chunks, metadatas = self.doc_processor.chunk_text_with_metadata(pages)
            for i in range(0, len(chunks), BATCH_SIZE):
                batch_chunks = chunks[i:i + BATCH_SIZE]
                batch_metadatas = metadatas[i:i + BATCH_SIZE]
                batch_embeddings = self.batch_embed_texts(batch_chunks)
                if not batch_embeddings:
                    logger.warning(f"No embeddings returned for batch starting at chunk {i}, skipping.")
                    continue
                batch_ids = [str(uuid.uuid4()) for _ in range(len(batch_chunks))]
                self.vector_db.add_documents(
                    documents=batch_chunks,
                    embeddings=batch_embeddings,
                    ids=batch_ids,
                    metadatas=batch_metadatas
                )
            full_text = "\n\n".join([p["text"] for p in pages])
            summary = self.hierarchical_summarize(full_text)
            summary_embedding = self.batch_embed_texts([summary])
            if summary_embedding:
                self.vector_db.add_documents(
                    documents=[summary],
                    embeddings=[summary_embedding[0]],
                    ids=[str(uuid.uuid4())],
                    metadatas=[{"doc_name": request.filename, "page": 0, "paragraph": 0, "summary": True}]
                )
            else:
                logger.warning("No embedding returned for summary, skipping summary storage.")
            self.doc_processor.save_document_checksum(request.filename, content_bytes)
            r.set(doc_key, "completed")
        except Exception as e:
            error_msg = f"Ingestion failed for {request.filename}: {e}"
            logger.error(error_msg)
            r.set(doc_key, f"failed:{str(e)}")

    def ingest_status(self, filename: str):
        doc_key = f"ingestion_status:{filename}"
        status_msg = r.get(doc_key)
        if status_msg:
            return {"status": "ok", "message": status_msg}
        return JSONResponse(status_code=404, content={"status": "not_found", "message": "No status available"})

    # Replace your `query_docs` method with this improved version:

    def query_docs(self, request):
        logger.info(f"Received query: {request.question}")
        threshold = float(os.getenv("SIMILARITY_THRESHOLD", 0.5))
        max_tokens = int(os.getenv("MAX_QA_TOKENS", 3000))

        try:
            question_embedding = self.batch_embed_texts([request.question])[0]
            results = self.vector_db.query(question_embedding, top_k=10)

            docs = results.get("documents", [[]])[0]
            metas = results.get("metadatas", [[]])[0]
            distances = results.get("distances", [[]])[0]
            summaries = results.get("summaries", [])

            # Convert distance to similarity and filter
            all_docs = [
                {
                    "document": doc,
                    "metadata": metadata,
                    "score": 1 - distance
                }
                for doc, metadata, distance in zip(docs, metas, distances)
                if (1 - distance) >= threshold
            ]

            if not all_docs:
                logger.warning(f"No matches >= similarity threshold {threshold}")
                return {
                    "question": request.question,
                    "answer": "No relevant information found.",
                    "score": 0,
                    "context": [],
                    "summary": "",
                    "sources": [],
                    "ranked_matches": []
                }

            # Sort docs by score descending
            all_docs = sorted(all_docs, key=lambda d: d["score"], reverse=True)
            # Rerank with cross-encoder
            all_docs = self.rerank_with_cross_encoder(request.question, all_docs)


            # Build QA context
            summary_text = "\n\n".join(summaries)
            context_parts = []
            token_count = 0

            if summary_text:
                context_parts.append("Summary:\n" + summary_text)
                token_count += len(summary_text.split())

            for doc in all_docs:
                doc_tokens = len(doc["document"].split())
                if token_count + doc_tokens > max_tokens:
                    break
                context_parts.append(doc["document"])
                token_count += doc_tokens

            context = "\n\n".join(context_parts)
            logger.info(f"QA context contains ~{token_count} tokens")

            answer = self.qa_pipeline(question=request.question, context=context)
            answer_text = answer.get("answer", "").strip() or "I'm not sure based on the available information."
            rag_metrics = self.compute_rag_metrics(answer_text, [doc["document"] for doc in all_docs])


            return {
                "question": request.question,
                "answer": answer_text,
                "score": answer.get("score", 0),
                "context": [doc["document"] for doc in all_docs],
                "summary": summary_text,
                "sources": [doc["metadata"] for doc in all_docs],
                "ranked_matches": [
                    {
                        "text": doc["document"],
                        "metadata": doc["metadata"],
                        "similarity": doc["score"]
                    }
                    for doc in all_docs
                ],
                "rag_metrics":rag_metrics
            }

        except Exception as e:
            logger.error(f"QA pipeline error: {e}")
            return {
                "question": request.question,
                "answer": "An error occurred while processing your query.",
                "score": 0,
                "context": [],
                "summary": "",
                "sources": [],
                "ranked_matches": []
            }

    def list_all_documents(self):
        try:
            logger.info("Fetching all document names from collection...")
            doc_names = self.vector_db.get_all_doc_names()
            logger.info(f"Found {len(doc_names)} unique documents.")
            return {"status": "ok", "documents": doc_names}
        except Exception as e:
            logger.error(f"Failed to list documents: {e}")
            return JSONResponse(status_code=500, content={"status": "error", "message": str(e)})
    def rerank_with_cross_encoder(self, query: str, docs: List[dict]) -> List[dict]:
        pairs = [[query, doc["document"]] for doc in docs]
        scores = self.cross_encoder.predict(pairs)

        for doc, score in zip(docs, scores):
            doc["rerank_score"] = score

        return sorted(docs, key=lambda x: x["rerank_score"], reverse=True)
    def compute_rag_metrics(self, answer: str, docs: List[str]) -> dict:
        answer_lower = answer.lower()
        context_hits = sum(1 for doc in docs if answer_lower in doc.lower())
        return {
            "answer_in_context": context_hits > 0,
            "context_precision": context_hits / len(docs) if docs else 0.0
        }
