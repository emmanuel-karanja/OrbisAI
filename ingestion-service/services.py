import base64
from typing import List
from transformers import pipeline
from sentence_transformers import SentenceTransformer
from logger import setup_logger
from document_processor import DocumentProcessor
from redis_client import r
from chroma_db_client import ChromaDBClient  # Custom client implementing VectorDBInterface
from fastapi.responses import JSONResponse
import torch

torch.set_num_threads(1)

logger = setup_logger(name="ingestion-service")

BATCH_SIZE = 50
SUMMARY_CHUNK_SIZE = 500


class IngestService:
    def __init__(self):
        logger.info("Initializing services...")

        logger.info("Loading SentenceTransformer model...")
        self.model = SentenceTransformer("all-MiniLM-L6-v2")
        logger.info("SentenceTransformer loaded.")

        logger.info("Loading summarizer pipeline...")
        self.summarizer = pipeline("summarization", model="sshleifer/distilbart-cnn-12-6")
        logger.info("Summarizer pipeline loaded.")

        logger.info("Loading QA pipeline...")
        self.qa_pipeline = pipeline(
            "question-answering",
            model="deepset/roberta-base-squad2",
            tokenizer="deepset/roberta-base-squad2"
        )
        logger.info("QA pipeline loaded.")

        logger.info("Connecting to vector database (ChromaDB)...")
        self.vector_db = ChromaDBClient()

        logger.info("Instantiating DocumentProcessor...")
        self.doc_processor = DocumentProcessor()

    def batch_embed_texts(self, texts: List[str]) -> List[List[float]]:
        try:
            return self.model.encode(texts).tolist()
        except Exception as e:
            logger.error(f"Embedding failed: {e}")
            return []

    def hierarchical_summarize(self, text: str, chunk_size=SUMMARY_CHUNK_SIZE) -> str:
        chunks = [text[i: i + chunk_size] for i in range(0, len(text), chunk_size)]
        logger.info(f"Summarizing in {len(chunks)} chunks")

        summaries = []
        for i, chunk in enumerate(chunks):
            try:
                summary = self.summarizer(chunk, max_length=500, min_length=30, do_sample=False)[0]['summary_text']
            except Exception as e:
                logger.error(f"Error summarizing chunk {i}: {e}")
                summary = ""
            summaries.append(summary)

        combined_summary_text = " ".join(summaries)
        logger.info("Summarizing combined summaries")

        try:
            final_summary = self.summarizer(combined_summary_text, max_length=100, min_length=30, do_sample=False)[0]['summary_text']
        except Exception as e:
            logger.error(f"Error summarizing combined text: {e}")
            final_summary = combined_summary_text

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
            logger.info(f"Embedding {len(chunks)} chunks in batches of {BATCH_SIZE}...")

            for i in range(0, len(chunks), BATCH_SIZE):
                batch_chunks = chunks[i: i + BATCH_SIZE]
                batch_metadatas = metadatas[i: i + BATCH_SIZE]

                batch_embeddings = self.batch_embed_texts(batch_chunks)
                if not batch_embeddings:
                    logger.warning(f"No embeddings returned for batch starting at chunk {i}, skipping.")
                    continue

                batch_ids = [f"{request.filename}_chunk_{i + idx}" for idx in range(len(batch_chunks))]
                self.vector_db.add_documents(
                    documents=batch_chunks,
                    embeddings=batch_embeddings,
                    ids=batch_ids,
                    metadatas=batch_metadatas,
                )

            full_text = "\n\n".join([p["text"] for p in pages])
            logger.info("Generating hierarchical summary...")

            summary = self.hierarchical_summarize(full_text)

            logger.info("Embedding summary...")
            summary_embedding = self.batch_embed_texts([summary])
            if summary_embedding:
                self.vector_db.add_documents(
                    documents=[summary],
                    embeddings=[summary_embedding[0]],
                    ids=[f"{request.filename}_summary"],
                    metadatas=[{"doc_name": request.filename, "page": 0, "paragraph": 0, "summary": True}],
                )
            else:
                logger.warning("No embedding returned for summary, skipping summary storage.")

            self.doc_processor.save_document_checksum(request.filename, content_bytes)

            logger.info(f"Ingestion completed successfully for {request.filename}")
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

    def query_docs(self, request):
        logger.info(f"Received query: {request.question}")

        question_embedding = self.batch_embed_texts([request.question])[0]

        results = self.vector_db.query(question_embedding, top_k=3)

        documents = results["documents"][0]
        metadatas = results["metadatas"][0]
        distances = results.get("distances", [[None]])[0]

        summary_docs = self.vector_db.get_documents(where={"summary": True})
        summary_text = summary_docs["documents"][0] if summary_docs["documents"] else ""

        context_parts = []
        if summary_text:
            context_parts.append("Summary:\n" + summary_text)
        context_parts.append("Details:\n" + "\n".join(documents))
        context = "\n\n".join(context_parts)

        logger.info("Running QA pipeline...")
        answer = self.qa_pipeline(question=request.question, context=context)

        logger.info("Query processed successfully")
        return {
            "question": request.question,
            "answer": answer["answer"],
            "score": answer["score"],
            "context": documents,
            "summary": summary_text,
            "sources": metadatas,
            "ranked_matches": [
                {
                    "text": doc,
                    "metadata": meta,
                    "similarity": 1 - dist if dist is not None else None
                }
                for doc, meta, dist in zip(documents, metadatas, distances)
            ]
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
