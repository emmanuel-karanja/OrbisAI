import os
import sys
import base64
import asyncio
import argparse
import signal
import random
from pathlib import Path
from datetime import datetime
import traceback
from typing import List
from pydantic import BaseModel
from tqdm.asyncio import tqdm_asyncio
from dotenv import load_dotenv
from colorama import Fore, init as colorama_init
import httpx
import platform
import redis.asyncio as aioredis  # ✅ Direct Redis import

from utils.logger import setup_logger

colorama_init(autoreset=True)
load_dotenv(override=True)

DEFAULT_INPUT_DIR = os.getenv("DOCS_SOURCE_DIR", "C:\\Users\\ZBOOK\\Downloads\\kenya_laws\\pdfs")
DEFAULT_LOG_DIR = os.getenv("LOG_DIR", "./logs")
DEFAULT_CONCURRENCY = int(os.getenv("BULK_INGEST_CONCURRENCY", 20))
DEFAULT_PATTERN = "*.*"
INGEST_ENDPOINT = os.getenv("INGEST_ENDPOINT", "http://localhost:8001/ingest")

logger = setup_logger(name="bulk-ingestor", log_dir=DEFAULT_LOG_DIR)

shutdown_event = asyncio.Event()


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

        self.redis = aioredis.Redis(host="localhost", port=6379, decode_responses=True)  # ✅ Direct Redis

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

    async def _track_progress(self, filename, status):
        try:
            key = f"bulk_ingest:{filename}"
            await self.redis.set(key, status)
        except Exception as e:
            logger.warning(f"[Redis] Could not track {filename}: {e}")

    async def _ingest_file(self, path: Path, index: int, total: int, semaphore: asyncio.Semaphore):
        try:
            if shutdown_event.is_set():
                return False

            logger.info(Fore.CYAN + f"[{index}/{total}] Ingesting: {path.name}")
            print(Fore.YELLOW + f"⏳ {index}/{total}: {path.name}")

            content = self._encode_file(path)
            if not content:
                raise ValueError("Encoded content is empty")

            request = IngestionRequest(filename=path.name, content=content)
            payload = request.dict()

            async with semaphore:
                await self._track_progress(path.name, "started")

                retries = 5
                delay = 1
                for attempt in range(1, retries + 1):
                    try:
                        async with httpx.AsyncClient() as client:
                            response = await client.post(INGEST_ENDPOINT, json=payload, timeout=60)
                        response.raise_for_status()
                        break
                    except Exception as e:
                        if attempt == retries:
                            raise e
                        sleep_time = delay * (2 ** attempt) + random.uniform(0, 1)
                        logger.warning(f"[Retry] {path.name} in {sleep_time:.1f}s (attempt {attempt}) due to: {e}")
                        await asyncio.sleep(sleep_time)

            self._log_to(self.success_log, f"{datetime.now()} - SUCCESS - {path.name}")
            await self._track_progress(path.name, "success")
            logger.info(Fore.GREEN + f"[✓] Completed: {path.name}")
            return True

        except Exception as e:
            self._log_to(self.failure_log, f"{datetime.now()} - FAIL - {path.name} - {str(e)}")
            await self._track_progress(path.name, f"failed:{str(e)}")
            logger.error(Fore.RED + f"[FAIL] {path.name} => {e}")
            return False

    async def run_ingestion(self):
        try:
            all_files = list(self.input_dir.rglob(self.pattern))
            done_files = self._read_successful_files()
            pending_files = [f for f in all_files if f.name not in done_files]

            await self.redis.delete("bulk_ingest:all_files")
            if pending_files:
                await self.redis.sadd("bulk_ingest:all_files", *[f.name for f in pending_files])

            logger.info(f"📁 Total: {len(all_files)} | ✅ Done: {len(done_files)} | 🚀 Pending: {len(pending_files)}")
            if not pending_files:
                logger.info("Nothing to ingest.")
                return

            semaphore = asyncio.Semaphore(self.concurrency)
            tasks = [
                self._ingest_file(path, idx + 1, len(pending_files), semaphore)
                for idx, path in enumerate(pending_files)
            ]

            await tqdm_asyncio.gather(*tasks)

        except Exception as e:
            logger.critical(Fore.RED + f"[CRITICAL] Ingestion run failed: {e}")

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
            tasks = [
                self._ingest_file(path, idx + 1, len(paths), semaphore)
                for idx, path in enumerate(paths)
            ]

            await tqdm_asyncio.gather(*tasks)

        except Exception as e:
            logger.critical(Fore.RED + f"[CRITICAL] Retry run failed: {e}")


def setup_signal_handlers():
    def shutdown():
        print(Fore.RED + "\n[SHUTDOWN] Graceful exit triggered...")
        shutdown_event.set()

    if platform.system() == "Windows":
        signal.signal(signal.SIGINT, lambda s, f: shutdown())
        signal.signal(signal.SIGTERM, lambda s, f: shutdown())
    else:
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, shutdown)


def parse_args():
    parser = argparse.ArgumentParser(description="Bulk file ingestion CLI")
    parser.add_argument("command", choices=["run", "retry"], help="Choose 'run' or 'retry'")
    parser.add_argument("--input-dir", default=DEFAULT_INPUT_DIR, help="Directory containing documents")
    parser.add_argument("--log-dir", default=DEFAULT_LOG_DIR, help="Directory to store logs")
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY, help="Concurrent ingestion limit")
    parser.add_argument("--pattern", default=DEFAULT_PATTERN, help="File pattern to match, e.g. '*.pdf'")
    return parser.parse_args()


async def main():
    args = parse_args()
    setup_signal_handlers()

    ingestor = BulkFileIngestor(
        input_dir=args.input_dir,
        log_dir=args.log_dir,
        concurrency=args.concurrency,
        pattern=args.pattern
    )

    if args.command == "run":
        await ingestor.run_ingestion()
    elif args.command == "retry":
        await ingestor.retry_failures()

    await shutdown_event.wait()
    logger.info(Fore.LIGHTMAGENTA_EX + "[EXIT] All tasks completed. Shutting down.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logger.critical(Fore.RED + f"[CRITICAL] Startup error: {e}")
        traceback.print_exc()
