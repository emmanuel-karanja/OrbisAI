import chromadb
from chromadb.api.types import Where
from typing import List, Dict, Optional
from utils.logger import setup_logger
from db.vector_db_interface import VectorDBInterface
import os

LOG_DIR = os.getenv("LOG_DIR", "logs")
logger = setup_logger("chroma-db", log_dir=LOG_DIR, log_to_file=True)

CHROMADB_HOST=os.getenv("CHROMADB_HOST","chromadb")
CHROMADB_PORT=os.getenv("CHROMADB_PORT",8000)
CHROMADB_COLLECTION=os.getenv("CHROMADB_COLLECTION","docs")

class ChromaDBClient(VectorDBInterface):
    def __init__(self, host=CHROMADB_HOST, port=CHROMADB_PORT, collection_name=CHROMADB_COLLECTION):
        try:
            logger.info("Connecting to ChromaDB...")
            self.client = chromadb.HttpClient(host=host, port=port)
            self.collection = self.client.get_or_create_collection(collection_name)
            logger.info("ChromaDB client initialized with collection: %s", collection_name)
        except Exception as e:
            logger.error(f"Error initializing ChromaDBClient: {e}")
            raise

    def add_documents(self, documents: List[str], embeddings: List[List[float]], ids: List[str], metadatas: List[Dict]):
        try:
            self.collection.add(documents=documents, embeddings=embeddings, ids=ids, metadatas=metadatas)
            logger.info(f"Added {len(documents)} documents to ChromaDB collection.")
        except Exception as e:
            logger.error(f"Failed to add documents: {e}")
            raise

    def delete_documents(self, where: Dict):
        try:
            results = self.collection.get(where=where)
            ids_to_delete = results.get("ids", [])
            if ids_to_delete:
                self.collection.delete(ids=ids_to_delete)
                logger.info(f"Deleted {len(ids_to_delete)} documents matching filter: {where}")
            else:
                logger.info(f"No documents matched for deletion with filter: {where}")
        except Exception as e:
            logger.error(f"Error deleting documents with filter {where}: {e}")
            raise

    def query(self, embedding: List[float], top_k: int = 3) -> Dict:
        try:
            result = self.collection.query(query_embeddings=[embedding], n_results=top_k)
            logger.info(f"Query returned {len(result.get('ids', []))} results.")
            return result
        except Exception as e:
            logger.error(f"Error querying ChromaDB: {e}")
            raise

    def get_documents(self, where: Dict = None, include: List[str] = ["documents", "metadatas"]) -> Dict:
        try:
            results = self.collection.get(where=where, include=include)
            logger.info(f"Fetched {len(results.get('ids', []))} documents from ChromaDB.")
            return results
        except Exception as e:
            logger.error(f"Error fetching documents with filter {where}: {e}")
            raise

    def get_all_doc_names(self) -> List[str]:
        try:
            results = self.collection.get(include=["metadatas"])
            metadatas = results.get("metadatas", [])
            doc_names = {meta.get("doc_name") for meta in metadatas if meta.get("doc_name")}
            logger.info(f"Retrieved {len(doc_names)} unique document names.")
            return sorted(doc_names)
        except Exception as e:
            logger.error(f"Error retrieving document names: {e}")
            return []
