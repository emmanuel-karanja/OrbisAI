# ai/local_ai_engine.py

import os
import torch
import numpy as np
from typing import List, Dict
from transformers import pipeline, AutoTokenizer, AutoModelForQuestionAnswering
from sentence_transformers import SentenceTransformer, CrossEncoder
from ai_engine_interface import AIEngine
from utils.logger import setup_logger

logger = setup_logger("local-ai-engine")

class LocalAIEngine(AIEngine):
    def __init__(self):
        logger.info("Initializing LocalAIEngine...")

        sentence_model_name = os.getenv("SENTENCE_MODEL", "nomic-ai/nomic-embed-text-v1")
        summarizer_model_name = os.getenv("SUMMARIZER_MODEL", "sshleifer/distilbart-cnn-12-6")
        qa_model_name = os.getenv("QA_MODEL", "allenai/longformer-base-4096")

        logger.info(f"Loading sentence embedding model: {sentence_model_name}")
        self.embed_model = SentenceTransformer(sentence_model_name, trust_remote_code=True)

        logger.info(f"Loading summarizer model: {summarizer_model_name}")
        self.summarizer = pipeline("summarization", model=summarizer_model_name)

        logger.info(f"Loading QA model and tokenizer: {qa_model_name}")
        tokenizer = AutoTokenizer.from_pretrained(qa_model_name)
        model = AutoModelForQuestionAnswering.from_pretrained(qa_model_name)
        device = 0 if torch.cuda.is_available() else -1
        self.qa_pipeline = pipeline("question-answering", model=model, tokenizer=tokenizer, device=device)

        logger.info("Loading CrossEncoder reranker model...")
        self.cross_encoder = CrossEncoder("cross-encoder/ms-marco-MiniLM-L-6-v2")

        logger.info("LocalAIEngine initialized successfully.")

    def embed_texts(self, texts: List[str]) -> List[List[float]]:
        logger.info(f"Embedding {len(texts)} texts...")
        try:
            formatted = [f"query: {t}" if t.endswith("?") else f"passage: {t}" for t in texts]
            embeddings = self.embed_model.encode(formatted).tolist()
            logger.info("Embeddings generated successfully.")
            return embeddings
        except Exception as e:
            logger.error(f"Embedding failed: {e}")
            return []

    def summarize(self, text: str) -> str:
        logger.info("Starting hierarchical summarization...")
        chunks = [text[i:i + 500] for i in range(0, len(text), 500)]
        summaries = []

        for i, chunk in enumerate(chunks):
            chunk_len = len(chunk.split())
            max_len = min(500, int(chunk_len * 0.5))
            max_len = max(max_len, 30)
            try:
                summary = self.summarizer(chunk, max_length=max_len, min_length=20, do_sample=False)[0]['summary_text']
                logger.debug(f"Chunk {i} summarized successfully.")
            except Exception as e:
                logger.error(f"Error summarizing chunk {i}: {e}")
                summary = ""
            summaries.append(summary)

        combined = " ".join(summaries)
        final_len = min(100, int(len(combined.split()) * 0.5))
        final_len = max(final_len, 30)

        try:
            final_summary = self.summarizer(combined, max_length=final_len, min_length=20, do_sample=False)[0]['summary_text']
            logger.info("Final summary generated successfully.")
            return final_summary
        except Exception as e:
            logger.error(f"Error summarizing combined text: {e}")
            return combined

    def answer_question(self, question: str, context: str) -> Dict[str, str]:
        logger.info(f"Answering question: {question}")
        try:
            result = self.qa_pipeline(question=question, context=context)
            logger.info("Question answered successfully.")
            return result
        except Exception as e:
            logger.error(f"QA failed: {e}")
            return {"answer": "Error during question answering", "score": 0}

    def rerank(self, query: str, docs: List[Dict]) -> List[Dict]:
        logger.info(f"Reranking {len(docs)} documents for query: {query}")
        try:
            pairs = [[query, doc["document"]] for doc in docs]
            scores = self.cross_encoder.predict(pairs)
            for doc, score in zip(docs, scores):
                doc["rerank_score"] = score
            sorted_docs = sorted(docs, key=lambda d: d["rerank_score"], reverse=True)
            logger.info("Reranking completed.")
            return sorted_docs
        except Exception as e:
            logger.error(f"Reranking failed: {e}")
            return docs
