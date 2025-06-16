# ai/openai_ai_engine.py

import os
import openai
from typing import List, Dict
from ai_engine.ai_engine_interface import AIEngine
from utils.logger import setup_logger

logger = setup_logger("openai-ai-engine")

openai.api_key = os.getenv("OPENAI_API_KEY")


class OpenAIAIEngine(AIEngine):
    def __init__(self):
        logger.info("Initializing OpenAIAIEngine...")

        self.embedding_model = os.getenv("OPENAI_EMBEDDING_MODEL", "text-embedding-3-small")
        self.summarizer_model = os.getenv("OPENAI_SUMMARIZER_MODEL", "gpt-4")
        self.qa_model = os.getenv("OPENAI_QA_MODEL", "gpt-4")
        self.rerank_model = os.getenv("OPENAI_RERANK_MODEL", "gpt-4")

        logger.info(f"Embedding Model: {self.embedding_model}")
        logger.info(f"Summarizer Model: {self.summarizer_model}")
        logger.info(f"QA Model: {self.qa_model}")
        logger.info(f"Rerank Model: {self.rerank_model}")
        logger.info("OpenAIAIEngine initialized.")

    def embed_texts(self, texts: List[str]) -> List[List[float]]:
        logger.info(f"Generating embeddings for {len(texts)} texts using {self.embedding_model}")
        try:
            response = openai.embeddings.create(
                input=texts,
                model=self.embedding_model
            )
            logger.info("Embeddings generated successfully.")
            return [d.embedding for d in response.data]
        except Exception as e:
            logger.error(f"OpenAI embedding failed: {e}")
            raise RuntimeError(f"OpenAI embedding failed: {e}")

    def summarize(self, text: str) -> str:
        logger.info("Generating summary with OpenAI...")
        prompt = f"Summarize the following text:\n\n{text}"
        try:
            response = openai.chat.completions.create(
                model=self.summarizer_model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3,
                max_tokens=512
            )
            summary = response.choices[0].message.content.strip()
            logger.info("Summary generated successfully.")
            return summary
        except Exception as e:
            logger.error(f"OpenAI summarization failed: {e}")
            raise RuntimeError(f"OpenAI summarization failed: {e}")

    def answer_question(self, question: str, context: str) -> Dict[str, any]:
        logger.info(f"Answering question using OpenAI: {question}")
        prompt = f"Context:\n{context}\n\nQuestion: {question}\nAnswer:"
        try:
            response = openai.chat.completions.create(
                model=self.qa_model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2,
                max_tokens=512
            )
            answer = response.choices[0].message.content.strip()
            logger.info("Question answered successfully.")
            return {
                "answer": answer,
                "score": 1.0  # Static since OpenAI doesn't provide confidence scores
            }
        except Exception as e:
            logger.error(f"OpenAI QA failed: {e}")
            raise RuntimeError(f"OpenAI QA failed: {e}")

    def rerank(self, query: str, docs: List[Dict]) -> List[Dict]:
        logger.info(f"Reranking {len(docs)} documents using {self.rerank_model} for query: {query}")
        try:
            for i, doc in enumerate(docs):
                prompt = f"Rate how relevant this document is to the query.\n\nQuery: {query}\n\nDocument:\n{doc['document']}\n\nRelevance (0-1):"
                try:
                    response = openai.chat.completions.create(
                        model=self.rerank_model,
                        messages=[{"role": "user", "content": prompt}],
                        temperature=0.0,
                        max_tokens=10
                    )
                    score_text = response.choices[0].message.content.strip()
                    doc["rerank_score"] = float(score_text)
                    logger.debug(f"Doc {i} scored {doc['rerank_score']}")
                except ValueError:
                    logger.warning(f"Doc {i} returned invalid score: {score_text}")
                    doc["rerank_score"] = 0.0
            sorted_docs = sorted(docs, key=lambda x: x["rerank_score"], reverse=True)
            logger.info("Reranking completed.")
            return sorted_docs
        except Exception as e:
            logger.error(f"OpenAI reranking failed: {e}")
            raise RuntimeError(f"OpenAI reranking failed: {e}")
