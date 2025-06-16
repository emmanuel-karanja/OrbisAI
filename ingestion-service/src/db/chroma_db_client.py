import chromadb
from chromadb.api.types import Where
from typing import List, Dict, Optional
from utils.logger import setup_logger
from db.vector_db_interface import VectorDBInterface

logger = setup_logger("chroma-db")


class ChromaDBClient(VectorDBInterface):
    def __init__(self, host="chromadb", port=8000, collection_name="docs"):
        logger.info("Connecting to ChromaDB...")
        self.client = chromadb.HttpClient(host=host, port=port)
        self.collection = self.client.get_or_create_collection(collection_name)
        logger.info("ChromaDB client ready.")

    def add_documents(self, documents: List[str], embeddings: List[List[float]], ids: List[str], metadatas: List[Dict]):
        self.collection.add(documents=documents, embeddings=embeddings, ids=ids, metadatas=metadatas)

    def delete_documents(self, where: Dict):
        results = self.collection.get(where=where)
        ids_to_delete = results.get("ids", [])
        if ids_to_delete:
            self.collection.delete(ids=ids_to_delete)
            logger.info(f"Deleted {len(ids_to_delete)} documents.")
        else:
            logger.info("No documents matched for deletion.")

    def query(self, embedding: List[float], top_k: int = 3) -> Dict:
        return self.collection.query(query_embeddings=[embedding], n_results=top_k)

    def get_documents(self, where: Dict = None, include: List[str] = ["documents", "metadatas"]) -> Dict:
        return self.collection.get(where=where, include=include)

    def get_all_doc_names(self) -> List[str]:
        results = self.collection.get(include=["metadatas"])
        metadatas = results.get("metadatas", [])
        doc_names = {meta.get("doc_name") for meta in metadatas if meta.get("doc_name")}
        return sorted(doc_names)
