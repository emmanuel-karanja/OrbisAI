import os
import torch
import asyncio
from typing import List, Dict
from transformers import pipeline, AutoTokenizer, AutoModelForCausalLM
from sentence_transformers import SentenceTransformer, CrossEncoder
from ai_engine.ai_engine_interface import AIEngine
from utils.logger import setup_logger

logger = setup_logger("mistral-ai-engine")

class MistralAIEngine(AIEngine):
    def __init__(self):
        logger.info("Initializing MistralAIEngine...")

        self.embed_model_name = os.getenv("SENTENCE_MODEL", "sentence-transformers/all-MiniLM-L6-v2")
        self.cross_encoder_model_name = os.getenv("CROSSENCODER_MODEL", "cross-encoder/ms-marco-MiniLM-L-6-v2")
        self.mistral_model_name = os.getenv("MISTRAL_MODEL", "mistralai/Mistral-7B-Instruct-v0.2")

        # Embedding model
        logger.info(f"Loading sentence embedding model: {self.embed_model_name}")
        self.embed_model = SentenceTransformer(self.embed_model_name)

        # Reranker
        logger.info(f"Loading reranker model: {self.cross_encoder_model_name}")
        self.cross_encoder = CrossEncoder(self.cross_encoder_model_name)

        # Mistral pipeline (summarization + QA via prompts)
        logger.info(f"Loading Mistral model and tokenizer: {self.mistral_model_name}")
        tokenizer = AutoTokenizer.from_pretrained(self.mistral_model_name, trust_remote_code=True)
        model = AutoModelForCausalLM.from_pretrained(self.mistral_model_name, trust_remote_code=True)
        device = 0 if torch.cuda.is_available() else -1
        self.llm = pipeline("text-generation", model=model, tokenizer=tokenizer, device=device)

        logger.info("MistralAIEngine initialized successfully.")

    async def embed_texts(self, texts: List[str]) -> List[List[float]]:
        logger.info(f"Embedding {len(texts)} texts...")
        try:
            embeddings = await asyncio.to_thread(self.embed_model.encode, texts)
            return embeddings.tolist()
        except Exception as e:
            logger.error(f"Embedding failed: {e}")
            return []

    async def summarize(self, text: str) -> str:
        logger.info("Summarizing with Mistral...")
        prompt = f"Summarize the following:\n\n{text}\n\nSummary:"
        try:
            output = await asyncio.to_thread(self.llm, prompt, max_new_tokens=128, do_sample=False)
            summary = output[0]["generated_text"].split("Summary:")[-1].strip()
            return summary
        except Exception as e:
            logger.error(f"Summarization failed: {e}")
            return ""

    async def answer_question(self, question: str, context: str) -> Dict[str, str]:
        logger.info("Answering question using Mistral prompt...")
        prompt = f"Context:\n{context}\n\nQuestion: {question}\nAnswer:"
        try:
            output = await asyncio.to_thread(self.llm, prompt, max_new_tokens=128, do_sample=False)
            answer = output[0]["generated_text"].split("Answer:")[-1].strip()
            # Why am I assigning a default score of 1.0 here???
            # TODO find a way to calculate this score.
            return {"answer": answer, "score": 1.0}
        except Exception as e:
            logger.error(f"QA failed: {e}")
            return {"answer": "Error during question answering", "score": 0}

    async def rerank(self, query: str, docs: List[Dict]) -> List[Dict]:
        logger.info(f"Reranking {len(docs)} documents...")
        try:
            pairs = [[query, doc["document"]] for doc in docs]
            scores = await asyncio.to_thread(self.cross_encoder.predict, pairs)
            for doc, score in zip(docs, scores):
                doc["rerank_score"] = score
            return sorted(docs, key=lambda d: d["rerank_score"], reverse=True)
        except Exception as e:
            logger.error(f"Reranking failed: {e}")
            return docs
