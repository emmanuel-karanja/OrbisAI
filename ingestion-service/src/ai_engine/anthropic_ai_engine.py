# ai/anthropic_ai_engine.py

import os
from typing import List, Dict
from anthropic import Anthropic, AsyncAnthropic, HUMAN_PROMPT, AI_PROMPT
from sentence_transformers import SentenceTransformer, CrossEncoder
from ai_engine.ai_engine_interface import AIEngine
from utils.logger import setup_logger

logger = setup_logger("anthropic-ai-engine")

class AnthropicAIEngine(AIEngine):
    def __init__(self):
        logger.info("Initializing AnthropicAIEngine...")

        self.client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        self.model = os.getenv("ANTHROPIC_MODEL", "claude-3-opus-20240229")

        embed_model_name = os.getenv("SENTENCE_MODEL", "all-MiniLM-L6-v2")
        crossencoder_model_name = os.getenv("CROSSENCODER_MODEL", "cross-encoder/ms-marco-MiniLM-L-6-v2")

        logger.info(f"Loading embedding model: {embed_model_name}")
        self.embed_model = SentenceTransformer(embed_model_name)

        logger.info(f"Loading reranker model: {crossencoder_model_name}")
        self.cross_encoder = CrossEncoder(crossencoder_model_name)

        logger.info(f"AnthropicAIEngine initialized with model: {self.model}")

    def embed_texts(self, texts: List[str]) -> List[List[float]]:
        logger.info(f"Embedding {len(texts)} texts...")
        try:
            return self.embed_model.encode(texts).tolist()
        except Exception as e:
            logger.error(f"Embedding failed: {e}")
            return []

    def summarize(self, text: str) -> str:
        logger.info("Calling Anthropic for summarization...")
        prompt = f"Summarize the following text:\n\n{text}\n\nSummary:"
        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=300,
                messages=[{"role": "user", "content": prompt}]
            )
            return response.content[0].text.strip()
        except Exception as e:
            logger.error(f"Summarization failed: {e}")
            return ""

    def answer_question(self, question: str, context: str) -> Dict[str, str]:
        logger.info(f"Answering question using Anthropic...")
        prompt = f"Context:\n{context}\n\nQuestion: {question}\nAnswer:"
        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=500,
                messages=[{"role": "user", "content": prompt}]
            )
            answer = response.content[0].text.strip()
            return {"answer": answer, "score": 1.0}
        except Exception as e:
            logger.error(f"QA failed: {e}")
            return {"answer": "Error during question answering", "score": 0}

    def rerank(self, query: str, docs: List[Dict]) -> List[Dict]:
        logger.info(f"Reranking {len(docs)} documents...")
        try:
            pairs = [[query, doc["document"]] for doc in docs]
            scores = self.cross_encoder.predict(pairs)
            for doc, score in zip(docs, scores):
                doc["rerank_score"] = score
            return sorted(docs, key=lambda d: d["rerank_score"], reverse=True)
        except Exception as e:
            logger.error(f"Reranking failed: {e}")
            return docs
