import base64
from typing import List
from transformers import pipeline
from sentence_transformers import SentenceTransformer
from logger import setup_logger
from extract import (
    extract_text_and_metadata,
    chunk_text_with_metadata,
    document_exists_and_handle_update,
    save_document_checksum,
)
from redis_client import r
import chromadb
from fastapi.responses import JSONResponse

import torch
torch.set_num_threads(1)

logger = setup_logger(name="ingest")

BATCH_SIZE = 50
SUMMARY_CHUNK_SIZE = 500


def initialize_services():
    logger.info("Loading SentenceTransformer model...")

    model = SentenceTransformer("all-MiniLM-L6-v2")
    logger.info("SentenceTransformer loaded.")

    logger.info("Loading summarizer pipeline...")
    summarizer = pipeline("summarization", model="sshleifer/distilbart-cnn-12-6")

    logger.info("Summarizer pipeline loaded.")

    logger.info("Loading QA pipeline...")

    qa_pipeline = pipeline(
        "question-answering",
        model="distilbert-base-cased-distilled-squad",
        tokenizer="distilbert-base-cased-distilled-squad"
    )

   
    logger.info("QA pipeline loaded.")

    logger.info("Connecting to ChromaDB...")
    client = chromadb.HttpClient(host="chromadb", port=8000)
    collection = client.get_or_create_collection("docs")
    logger.info("ChromaDB client ready.")

    return model, summarizer, qa_pipeline, collection


def batch_embed_texts(texts: List[str], model) -> List[List[float]]:
    try:
        return model.encode(texts).tolist()
    except Exception as e:
        logger.error(f"Embedding failed: {e}")
        return []


def hierarchical_summarize(text: str, summarizer, chunk_size=SUMMARY_CHUNK_SIZE) -> str:
    chunks = [text[i: i + chunk_size] for i in range(0, len(text), chunk_size)]
    logger.info(f"Summarizing in {len(chunks)} chunks")

    summaries = []
    for i, chunk in enumerate(chunks):
        try:
            summary = summarizer(chunk, max_length=100, min_length=30, do_sample=False)[0]['summary_text']
        except Exception as e:
            logger.error(f"Error summarizing chunk {i}: {e}")
            summary = ""
        summaries.append(summary)

    combined_summary_text = " ".join(summaries)
    logger.info("Summarizing combined summaries")

    try:
        final_summary = summarizer(combined_summary_text, max_length=100, min_length=30, do_sample=False)[0]['summary_text']
    except Exception as e:
        logger.error(f"Error summarizing combined text: {e}")
        final_summary = combined_summary_text

    return final_summary


def delete_docs_by_name(doc_name: str, collection):
    results = collection.get(where={"doc_name": doc_name})
    ids_to_delete = results.get("ids", [])
    if ids_to_delete:
        collection.delete(ids=ids_to_delete)
        logger.info(f"Deleted {len(ids_to_delete)} embeddings for document '{doc_name}'")
    else:
        logger.info(f"No existing embeddings found to delete for document '{doc_name}'")


def ingest_document(request, model, summarizer, collection, redis_client=r):
    doc_key = f"ingestion_status:{request.filename}"
    redis_client.set(doc_key, "started")
    logger.info(f"Starting ingestion for file: {request.filename}")

    try:
        content_bytes = base64.b64decode(request.content)

        if document_exists_and_handle_update(request.filename, content_bytes):
            msg = f"Document {request.filename} already ingested with same content, skipping."
            logger.info(msg)
            redis_client.set(doc_key, "skipped")
            return

        pages = extract_text_and_metadata(request.filename, request.content)
        for page in pages:
            page["doc_name"] = request.filename

        chunks, metadatas = chunk_text_with_metadata(pages)

        logger.info(f"Embedding {len(chunks)} chunks in batches of {BATCH_SIZE}...")

        for i in range(0, len(chunks), BATCH_SIZE):
            batch_chunks = chunks[i: i + BATCH_SIZE]
            batch_metadatas = metadatas[i: i + BATCH_SIZE]

            batch_embeddings = batch_embed_texts(batch_chunks, model)
            if not batch_embeddings:
                logger.warning(f"No embeddings returned for batch starting at chunk {i}, skipping.")
                continue

            batch_ids = [f"{request.filename}_chunk_{i + idx}" for idx in range(len(batch_chunks))]
            collection.add(
                documents=batch_chunks,
                embeddings=batch_embeddings,
                ids=batch_ids,
                metadatas=batch_metadatas,
            )

        full_text = "\n\n".join([p["text"] for p in pages])
        logger.info("Generating hierarchical summary...")

        summary = hierarchical_summarize(full_text, summarizer)

        logger.info("Embedding summary...")
        summary_embedding = batch_embed_texts([summary], model)
        if summary_embedding:
            collection.add(
                documents=[summary],
                embeddings=[summary_embedding[0]],
                ids=[f"{request.filename}_summary"],
                metadatas=[{"doc_name": request.filename, "page": 0, "paragraph": 0, "summary": True}],
            )
        else:
            logger.warning("No embedding returned for summary, skipping summary storage.")

        save_document_checksum(request.filename, content_bytes)

        logger.info(f"Ingestion completed successfully for {request.filename}")
        redis_client.set(doc_key, "completed")

    except Exception as e:
        error_msg = f"Ingestion failed for {request.filename}: {e}"
        logger.error(error_msg)
        redis_client.set(doc_key, f"failed:{str(e)}")


def ingest_status(filename: str):
    doc_key = f"ingestion_status:{filename}"
    status_msg = r.get(doc_key)
    if status_msg:
        return {"status": "ok", "message": status_msg}
    return JSONResponse(status_code=404, content={"status": "not_found", "message": "No status available"})


def query_docs(request, model, qa_pipeline, collection):
    logger.info(f"Received query: {request.question}")

    question_embedding = batch_embed_texts([request.question], model)[0]

    # Get top 3 documents
    results = collection.query(query_embeddings=[question_embedding], n_results=3)

    documents = results["documents"][0]
    metadatas = results["metadatas"][0]
    distances = results.get("distances", [[None]])[0]  # or "scores" depending on vector DB

    # Fetch summary if any
    summary_docs = collection.get(where={"summary": True})
    summary_text = summary_docs["documents"][0] if summary_docs["documents"] else ""

    # Build context
    context_parts = []
    if summary_text:
        context_parts.append("Summary:\n" + summary_text)
    context_parts.append("Details:\n" + "\n".join(documents))
    context = "\n\n".join(context_parts)

    logger.info("Running QA pipeline...")
    answer = qa_pipeline(question=request.question, context=context)

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

