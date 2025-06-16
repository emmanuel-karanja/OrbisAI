# ai/ai_engine.py

from abc import ABC, abstractmethod
from typing import List, Dict

class AIEngine(ABC):
    @abstractmethod
    def embed_texts(self, texts: List[str]) -> List[List[float]]:
        pass

    @abstractmethod
    def summarize(self, text: str) -> str:
        pass

    @abstractmethod
    def answer_question(self, question: str, context: str) -> Dict[str, str]:
        pass

    @abstractmethod
    def rerank(self, query: str, docs: List[Dict]) -> List[Dict]:
        pass
