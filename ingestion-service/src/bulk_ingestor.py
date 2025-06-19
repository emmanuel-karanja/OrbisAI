import os
import sys
import base64
import asyncio
import argparse
from pathlib import Path
from datetime import datetime
from typing import List
from pydantic import BaseModel
from tqdm.asyncio import tqdm_asyncio
from dotenv import load_dotenv

from services.ingest_service import IngestService
from ai_engine.local_ai_engine import LocalAIEngine
from utils.redis_client import get_redis
from utils.logger import setup_logger

# Load .env configuration
load_dotenv(override=True)

DEFAULT_INPUT_DIR = os.getenv("DOCS_SOURCE_DIR", "C:\\Users\\ZBOOK\\Downloads\\kenya_laws\\pdfs")
DEFAULT_LOG_DIR = os.getenv("LOG_DIR", "./logs")
DEFAULT_CONCURRENCY = int(os.getenv("BULK_INGEST_CONCURRENCY", 20))
DEFAULT_PATTERN = "*.*"

logger = setup_logger(name="bulk-ingestor", log_dir=DEFAULT_LOG_DIR)


class IngestionRequest(BaseModel):
    filename: str
    content: str


class BulkFileIngestor:
    def __init__(self, input_dir: str, log_dir: str, concurrency: int = 20, pattern: str = "*.*"):
        self.input_dir = Path(input_dir)
        self.log_dir = Path(log_dir)
        self.pattern = pattern
        self.concurrency = concurrency

        self.success_log = self.log_dir / "success.log"
        self.failure_log = self.log_dir / "failed.log"

        os.makedirs(self.log_dir, exist_ok=True)

        self.ai_engine = LocalAIEngine()
        self.ingest_service = IngestService(ai_engine=self.ai_engine)

    def _log_to(self, file: Path, message: str):
        try:
            with open(file, "a") as f:
                f.write(message.strip() + "\n")
        except Exception as e:
            logger.warning(f"Could not write to log file {file}: {e}")

    def _read_successful_files(self) -> set:
        try:
            if not self.success_log.exists():
                return set()
            return {line.split(" - ")[2].strip() for line in self.success_log.read_text().splitlines()}
        except Exception as e:
            logger.warning(f"Failed to read success log: {e}")
            return set()

    def _read_failed_files(self) -> List[str]:
        try:
            if not self.failure_log.exists():
                return []
            return [line.split(" - ")[2].strip() for line in self.failure_log.read_text().splitlines()]
        except Exception as e:
            logger.warning(f"Failed to read failure log: {e}")
            return []

    def _encode_file(self, path: Path) -> str:
        try:
            with open(path, "rb") as f:
                return base64.b64encode(f.read()).decode("utf-8")
        except Exception as e:
            logger.error(f"Failed to encode file {path.name}: {e}")
            return ""

    async def _track_progress(self, redis_conn, filename, status):
        try:
            key = f"bulk_ingest:{filename}"
            await redis_conn.set(key, status)
        except Exception as e:
            logger.warning(f"Failed to set Redis progress for {filename}: {e}")

    async def _ingest_file(self, path: Path, semaphore: asyncio.Semaphore):
        redis_conn = await get_redis()
        try:
            await self._track_progress(redis_conn, path.name, "started")

            content = self._encode_file(path)
            if not content:
                raise ValueError("Encoded content is empty")

            request = IngestionRequest(filename=path.name, content=content)

            async with semaphore:
                await self.ingest_service.ingest_document(request)

            self._log_to(self.success_log, f"{datetime.now()} - SUCCESS - {path.name}")
            await self._track_progress(redis_conn, path.name, "success")
            logger.info(f"Ingested {path.name}")
            return True

        except Exception as e:
            self._log_to(self.failure_log, f"{datetime.now()} - FAIL - {path.name} - {str(e)}")
            await self._track_progress(redis_conn, path.name, f"failed:{str(e)}")
            logger.error(f"Failed to ingest {path.name}: {e}")
            return False

    async def run_ingestion(self):
        redis_conn = await get_redis()
        try:
            all_files = list(self.input_dir.rglob(self.pattern))
            done_files = self._read_successful_files()
            pending_files = [f for f in all_files if f.name not in done_files]

            await redis_conn.delete("bulk_ingest:all_files")
            if pending_files:
                await redis_conn.sadd("bulk_ingest:all_files", *[f.name for f in pending_files])

            logger.info(f"📁 Total: {len(all_files)} | ✅ Done: {len(done_files)} | 🚀 Pending: {len(pending_files)}")

            if not pending_files:
                logger.info("Nothing to ingest.")
                return

            semaphore = asyncio.Semaphore(self.concurrency)
            tasks = [self._ingest_file(path, semaphore) for path in pending_files]
            await tqdm_asyncio.gather(*tasks)

        except Exception as e:
            logger.critical(f"Ingestion run failed: {e}")

    async def retry_failures(self):
        try:
            failed_files = self._read_failed_files()
            if not failed_files:
                logger.info("No failed files to retry.")
                return

            logger.info(f"🔁 Retrying {len(failed_files)} failed files...")
            paths = [self.input_dir / fname for fname in failed_files if (self.input_dir / fname).exists()]
            self.failure_log.unlink(missing_ok=True)

            semaphore = asyncio.Semaphore(self.concurrency)
            tasks = [self._ingest_file(path, semaphore) for path in paths]
            await tqdm_asyncio.gather(*tasks)

        except Exception as e:
            logger.critical(f"Retry run failed: {e}")


def parse_args():
    parser = argparse.ArgumentParser(description="Bulk file ingestion CLI")

    parser.add_argument("command", choices=["run", "retry"], help="Choose 'run' or 'retry'")
    parser.add_argument("--input-dir", default=DEFAULT_INPUT_DIR, help="Directory containing documents")
    parser.add_argument("--log-dir", default=DEFAULT_LOG_DIR, help="Directory to store logs")
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY, help="Concurrent ingestion limit")
    parser.add_argument("--pattern", default=DEFAULT_PATTERN, help="File pattern to match, e.g. '*.pdf'")

    return parser.parse_args()


if __name__ == "__main__":
    try:
        args = parse_args()

        ingestor = BulkFileIngestor(
            input_dir=args.input_dir,
            log_dir=args.log_dir,
            concurrency=args.concurrency,
            pattern=args.pattern
        )

        if args.command == "run":
            asyncio.run(ingestor.run_ingestion())
        elif args.command == "retry":
            asyncio.run(ingestor.retry_failures())

    except Exception as e:
        logger.critical(f"Startup error: {e}")
