from abc import ABC, abstractmethod
from typing import List, Dict, Optional
from qdrant_client import QdrantClient
from qdrant_client.http import models
from qdrant_client.models import Distance, VectorParams
from vector_db_interface import VectorDBInterface


class QdrantVectorDB(VectorDBInterface):
    def __init__(self, collection_name: str = "documents", vector_size: int = 768, host: str = "qdrant", port: int = 6333):
        self.collection_name = collection_name
        self.client = QdrantClient(host=host, port=port)

        if self.collection_name not in [col.name for col in self.client.get_collections().collections]:
            self.client.recreate_collection(
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
        # Main vector search
        search_result = self.client.search(
            collection_name=self.collection_name,
            query_vector=embedding,
            limit=top_k,
            with_payload=True
        )

        documents = []
        metadatas = []
        distances = []
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

        # Optional: Get summaries for those documents
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
                # If summaries fail, fallback to empty list
                logger.warning(f"Failed to fetch summaries for docs {doc_names}: {e}")
                summary_texts = []

        return {
            "documents": [documents],
            "metadatas": [metadatas],
            "distances": [distances],
            "summaries": summary_texts
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

