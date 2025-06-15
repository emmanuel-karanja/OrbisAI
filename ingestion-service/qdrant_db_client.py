from abc import ABC, abstractmethod
from typing import List, Dict, Optional
from qdrant_client import QdrantClient
from qdrant_client.http import models
from qdrant_client.models import Distance, VectorParams
from vector_db_interface import VectorDBInterface


class QdrantVectorDB(VectorDBInterface):
    def __init__(self, collection_name: str = "documents", vector_size: int = 768, host: str = "localhost", port: int = 6333):
        self.collection_name = collection_name
        self.client = QdrantClient(host=host, port=port)

        if self.collection_name not in [col.name for col in self.client.get_collections().collections]:
            self.client.create_collection(
                collection_name=self.collection_name,
                vectors_config=VectorParams(size=vector_size, distance=Distance.COSINE),
            )

    def add_documents(self, documents: List[str], embeddings: List[List[float]], ids: List[str], metadatas: List[Dict]):
        points = []
        for idx, (doc, vector, metadata) in enumerate(zip(documents, embeddings, metadatas)):
            metadata = metadata.copy()
            metadata["document"] = doc
            points.append(models.PointStruct(id=ids[idx], vector=vector, payload=metadata))
        
        self.client.upsert(collection_name=self.collection_name, points=points)

    def delete_documents(self, where: Dict):
        self.client.delete(
            collection_name=self.collection_name,
            filter=models.Filter(
                must=[models.FieldCondition(key=key, match=models.MatchValue(value=value)) for key, value in where.items()]
            )
        )

    def query(self, embedding: List[float], top_k: int) -> Dict:
        search_result = self.client.search(
            collection_name=self.collection_name,
            query_vector=embedding,
            limit=top_k,
            with_payload=True
        )

        documents = [hit.payload.get("document", "") for hit in search_result]
        metadatas = [{k: v for k, v in hit.payload.items() if k != "document"} for hit in search_result]
        distances = [hit.score for hit in search_result]

        return {
            "documents": [documents],    # wrap in list to match batching logic in IngestService
            "metadatas": [metadatas],
            "distances": [distances]
        }

    def get_documents(self, where: Dict = None, include: List[str] = ["documents", "metadatas"]) -> Dict:
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

        return {"documents": documents}

    def get_all_doc_names(self) -> List[str]:
        scroll = self.client.scroll(
            collection_name=self.collection_name,
            with_payload=True,
            limit=1000
        )
        return list({point.payload.get("doc_name") for point in scroll[0] if point.payload.get("doc_name")})

