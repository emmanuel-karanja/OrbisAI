from abc import ABC, abstractmethod
from typing import List, Dict, Optional
from qdrant_client import QdrantClient
from qdrant_client.http import models
from qdrant_client.models import Distance, VectorParams
from db.vector_db_interface import VectorDBInterface
from utils.logger import setup_logger

import os

LOG_DIR = os.getenv("LOG_DIR", "logs")
logger = setup_logger("qdrant-db", log_dir=LOG_DIR, log_to_file=True)

QDRANT_HOST=os.getenv("QDRANT_HOST","qdrant")
QDRANT_PORT=os.getenv("QDRANT_PORT",6333)
QDRANT_VECTOR_SIZE=os.getenv("QDRANT_VECTOR_SIZE",768)
QDRANT_COLLECTION=os.getenv("QDRANT_COLLECTION","documents")

class QdrantVectorDB(VectorDBInterface):
    def __init__(self, collection_name: str = QDRANT_COLLECTION, vector_size: int = QDRANT_VECTOR_SIZE, host: str = QDRANT_HOST, port: int = QDRANT_PORT):
        try:
            self.collection_name = collection_name
            self.client = QdrantClient(host=host, port=port)
            logger.info(f"Connecting to Qdrant at {host}:{port} and checking collection '{collection_name}'.")

            collections = self.client.get_collections().collections
            if self.collection_name not in [col.name for col in collections]:
                self.client.recreate_collection(
                    collection_name=self.collection_name,
                    vectors_config=VectorParams(size=vector_size, distance=Distance.COSINE),
                )
                logger.info(f"Created new collection '{self.collection_name}'.")
        except Exception as e:
            logger.error(f"Error initializing QdrantVectorDB: {e}")
            raise

    def add_documents(self, documents: List[str], embeddings: List[List[float]], ids: List[str], metadatas: List[Dict]):
        try:
            points = []
            for idx, (doc, vector, metadata) in enumerate(zip(documents, embeddings, metadatas)):
                metadata = metadata.copy()
                metadata["document"] = doc
                points.append(models.PointStruct(id=ids[idx], vector=vector, payload=metadata))
            self.client.upsert(collection_name=self.collection_name, points=points)
            logger.info(f"Added {len(points)} documents to collection '{self.collection_name}'.")
        except Exception as e:
            logger.error(f"Error adding documents: {e}")
            raise

    def delete_documents(self, where: Dict):
        try:
            conditions = [models.FieldCondition(key=key, match=models.MatchValue(value=value)) for key, value in where.items()]
            self.client.delete(
                collection_name=self.collection_name,
                filter=models.Filter(must=conditions)
            )
            logger.info(f"Deleted documents from collection '{self.collection_name}' with filter {where}.")
        except Exception as e:
            logger.error(f"Error deleting documents with filter {where}: {e}")
            raise

    def query(self, embedding: List[float], top_k: int) -> Dict:
        try:
            search_result = self.client.search(
                collection_name=self.collection_name,
                query_vector=embedding,
                limit=top_k,
                with_payload=True
            )

            documents, metadatas, distances = [], [], []
            doc_names = set()

            for hit in search_result:
                payload = hit.payload or {}
                doc_text = payload.get("document", "")
                metadata = {k: v for k, v in payload.items() if k != "document"}
                doc_name = metadata.get("doc_name")

                documents.append(doc_text)
                metadatas.append(metadata)
                distances.append(hit.score)

                if doc_name:
                    doc_names.add(doc_name)

            summary_texts = []
            if doc_names:
                try:
                    summary_hits, _ = self.client.scroll(
                        collection_name=self.collection_name,
                        scroll_filter={
                            "must": [
                                {"key": "summary", "match": {"value": True}},
                                {"key": "doc_name", "match": {"any": list(doc_names)}}
                            ]
                        },
                        with_payload=True
                    )

                    summary_texts = [
                        hit.payload.get("document", "")
                        for hit in summary_hits
                        if hit.payload and "document" in hit.payload
                    ]
                except Exception as e:
                    logger.warning(f"Failed to fetch summaries for docs {doc_names}: {e}")

            logger.info(f"Query returned {len(documents)} documents.")
            return {
                "documents": [documents],
                "metadatas": [metadatas],
                "distances": [distances],
                "summaries": summary_texts
            }
        except Exception as e:
            logger.error(f"Error during query: {e}")
            raise

    def get_documents(self, where: Dict = None, include: List[str] = ["documents", "metadatas"]) -> Dict:
        try:
            filter_obj = None
            if where:
                filter_obj = models.Filter(
                    must=[models.FieldCondition(key=key, match=models.MatchValue(value=value)) for key, value in where.items()]
                )

            scroll = self.client.scroll(
                collection_name=self.collection_name,
                scroll_filter=filter_obj,
                with_payload=True,
                limit=1000
            )

            documents = []
            for point in scroll[0]:
                item = {}
                if "documents" in include:
                    item["document"] = point.payload.get("document")
                if "metadatas" in include:
                    item["metadata"] = {k: v for k, v in point.payload.items() if k != "document"}
                documents.append(item)

            logger.info(f"Fetched {len(documents)} documents from collection '{self.collection_name}'.")
            return {"documents": documents}
        except Exception as e:
            logger.error(f"Error getting documents with filter {where}: {e}")
            raise

    def get_all_doc_names(self) -> List[str]:
        try:
            scroll = self.client.scroll(
                collection_name=self.collection_name,
                with_payload=True,
                limit=1000
            )
            doc_names = list({point.payload.get("doc_name") for point in scroll[0] if point.payload.get("doc_name")})
            logger.info(f"Retrieved {len(doc_names)} document names.")
            return doc_names
        except Exception as e:
            logger.error(f"Error fetching document names: {e}")
            return []
