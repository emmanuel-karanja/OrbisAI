from abc import ABC, abstractmethod
from typing import List, Dict

class VectorDBInterface(ABC):
    @abstractmethod
    def add_documents(self, documents: List[str], embeddings: List[List[float]], ids: List[str], metadatas: List[Dict]):
        pass

    @abstractmethod
    def delete_documents(self, where: Dict):
        pass

    @abstractmethod
    def query(self, embedding: List[float], top_k: int) -> Dict:
        pass

    @abstractmethod
    def get_documents(self, where: Dict = None, include: List[str] = ["documents", "metadatas"]) -> Dict:
        pass

    @abstractmethod
    def get_all_doc_names(self) -> List[str]:
        pass
