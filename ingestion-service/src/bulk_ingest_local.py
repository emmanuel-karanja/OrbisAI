import os
import base64
import asyncio
import aiohttp
import aiofiles
import argparse
from aiohttp import ClientSession
from dotenv import load_dotenv

load_dotenv()


class AsyncDocumentIngestor:
    def __init__(self, source_dir: str = None, api_url: str = None, concurrency: int = 10):
        self.source_dir = source_dir or os.getenv("DOCS_SOURCE_DIR", "./documents")
        self.api_url = api_url or os.getenv("INGEST_API_URL", "http://localhost:8001/ingest")
        self.concurrency = concurrency
        self.semaphore = asyncio.Semaphore(concurrency)

    async def _encode_file_to_base64(self, path: str) -> str:
        try:
            async with aiofiles.open(path, "rb") as f:
                content = await f.read()
                return base64.b64encode(content).decode("utf-8")
        except Exception as e:
            print(f"❌ Error reading file {path}: {e}")
            return None

    async def _post_document(self, session: ClientSession, filename: str, content: str, path: str):
        if not content:
            return
        try:
            payload = {
                "filename": filename,
                "content": content
            }
            async with session.post(self.api_url, json=payload, timeout=60) as response:
                text = await response.text()
                status = "✅" if response.status == 200 else "⚠️"
                print(f"{status} {filename} → {response.status}: {text}")
        except Exception as e:
            print(f"❌ Failed to upload {filename}: {e}")

    async def _process_file(self, session: ClientSession, path: str, fname: str):
        async with self.semaphore:
            content = await self._encode_file_to_base64(path)
            await self._post_document(session, fname, content, path)

    async def ingest(self):
        if not os.path.isdir(self.source_dir):
            print(f"❌ Source directory does not exist: {self.source_dir}")
            return

        print(f"📁 Scanning: {self.source_dir}")
        print(f"📡 API Endpoint: {self.api_url}")

        tasks = []
        async with aiohttp.ClientSession() as session:
            for root, _, files in os.walk(self.source_dir):
                for fname in files:
                    path = os.path.join(root, fname)
                    tasks.append(self._process_file(session, path, fname))
            await asyncio.gather(*tasks)

        print("🚀 Ingestion completed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Async Document Ingestor")
    parser.add_argument("--source-dir", help="Path to document directory")
    parser.add_argument("--api", help="Ingestion API endpoint")
    parser.add_argument("--concurrency", type=int, default=10, help="Max concurrent uploads")
    args = parser.parse_args()

    ingestor = AsyncDocumentIngestor(
        source_dir=args.source_dir,
        api_url=args.api,
        concurrency=args.concurrency
    )
    asyncio.run(ingestor.ingest())
