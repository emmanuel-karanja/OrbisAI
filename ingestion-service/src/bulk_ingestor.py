"""
bulk_ingestor.py

Asynchronous bulk file ingestion tool for uploading files (e.g. PDFs) to an external ingestion API. These files
will be embedded and persisted into a vectorDB for indexed search via RAG(or any other tool that needs that)

--------------------------------------------------------------------------------
🔧 WHAT IT DOES
--------------------------------------------------------------------------------
- Recursively scans a directory for files matching a pattern (e.g. *.pdf)
- Reads and base64-encodes each file
- Sends the encoded content to a REST API endpoint (`INGEST_ENDPOINT`)
- Tracks progress in Redis and saves ingestion results to JSON files. Progress is tracked per file e.g. started, ongoing, successful or failed
  and the data can be scraped for display in a dashboard.
- Retries failed files with exponential backoff
- Handles graceful shutdown on SIGINT/SIGTERM
- Offers two commands: 
    - `run`   → Process all new files
    - `retry` → Reprocess previously failed files

--------------------------------------------------------------------------------
🚀 FEATURES
--------------------------------------------------------------------------------
- Async ingestion using `asyncio` and `httpx`
- Concurrency control using `asyncio.Semaphore`
- Retry with exponential backoff and jitter
- Progress tracking via Redis (`bulk_ingest:<filename>`)
- File result tracking via:
    - `success.json` → files ingested successfully
    - `failed.json`  → files that failed with errors
- Resilient to I/O, network, and Redis failures
- Terminal-friendly colored output (via `colorama`)
- Works on both Windows and Unix (with platform-aware signal handling)

--------------------------------------------------------------------------------
📁 DIRECTORY STRUCTURE EXPECTED
--------------------------------------------------------------------------------
Environment Variables (via .env or OS env):
- DOCS_SOURCE_DIR: path to the input folder of documents
- LOG_DIR: folder for saving logs and result JSON files
- INGEST_ENDPOINT: target API endpoint for ingestion
- BULK_INGEST_CONCURRENCY: max parallel ingestion workers
- DEFAULT_FILE_PATTERNS: comma-separated file extensions, e.g. .pdf,.txt,.md,.docx,.html

--------------------------------------------------------------------------------
✅ USAGE
--------------------------------------------------------------------------------
# Run ingestion for all pending files
$ python bulk_ingestor.py run

# Retry only failed files
$ python bulk_ingestor.py retry

Optional flags:
    --input-dir      (override DOCS_SOURCE_DIR)
    --log-dir        (override LOG_DIR)
    --concurrency    (override BULK_INGEST_CONCURRENCY)
    --patterns       (override DEFAULT_FILE_PATTERNS, e.g. .pdf,.txt)

--------------------------------------------------------------------------------
🧠 AUTHOR NOTES
--------------------------------------------------------------------------------
- Redis is used for live tracking (optional but recommended). What this can do is present 
   a display with progress bars showing the progress of the file currently being ingested with possibility to
   to enrich the data.
- JSON replaces log parsing for ingestion history (race-safe at batch level)
- Graceful shutdown ensures in-flight requests finish before exiting
"""

import os
import sys
import json
import base64
import asyncio
import argparse
import signal
import random
from pathlib import Path
from datetime import datetime
import traceback
from typing import List, Dict
from pydantic import BaseModel
from tqdm.asyncio import tqdm_asyncio
from dotenv import load_dotenv
from colorama import Fore, init as colorama_init
import httpx
import platform
import redis.asyncio as aioredis

from utils.logger import setup_logger

colorama_init(autoreset=True)
load_dotenv(override=True)

DEFAULT_INPUT_DIR = os.getenv("DOCS_SOURCE_DIR", "../file_source")
DEFAULT_LOG_DIR = os.getenv("LOG_DIR", "../logs")
DEFAULT_CONCURRENCY = int(os.getenv("BULK_INGEST_CONCURRENCY", 20))
DEFAULT_PATTERN_LIST = os.getenv("DEFAULT_FILE_PATTERNS", ".pdf").split(",")
INGEST_ENDPOINT = os.getenv("INGEST_ENDPOINT", "http://localhost:8001/ingest")

logger = setup_logger(name="bulk-ingestor", log_dir=DEFAULT_LOG_DIR)

shutdown_event = asyncio.Event()


class IngestionRequest(BaseModel):
    filename: str
    content: str


class BulkFileIngestor:
    def __init__(self, input_dir: str, log_dir: str, concurrency: int = 20, patterns: List[str] = DEFAULT_PATTERN_LIST):
        self.input_dir = Path(input_dir)
        self.log_dir = Path(log_dir)
        self.patterns = patterns or [".pdf"]
        self.concurrency = concurrency

        os.makedirs(self.log_dir, exist_ok=True)
        self.success_json = self.log_dir / "success.json"
        self.failure_json = self.log_dir / "failed.json"

        # We may need to move this to separate project on its own in future, and hence have its own thing going on of .env
        self.redis = aioredis.Redis(host="localhost", port=6379, decode_responses=True)

        self.success_map = self._read_json(self.success_json)
        self.failure_map = self._read_json(self.failure_json)

    def _read_json(self, file: Path) -> Dict[str, str]:
        try:
            if file.exists():
                with open(file, "r", encoding="utf-8") as f:
                    return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to read {file.name}: {e}")
        return {}

    def _write_json(self, file: Path, data: Dict[str, str]):
        try:
            with open(file, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to write to {file.name}: {e}")

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
            # Handle graceful exit
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
                        # Exponential backoff
                        sleep_time = delay * (2 ** attempt) + random.uniform(0, 1)
                        logger.warning(f"[Retry] {path.name} in {sleep_time:.1f}s (attempt {attempt}) due to: {e}")
                        await asyncio.sleep(sleep_time)

            # Keep track of failed and successful after retry
            self.success_map[path.name] = datetime.now().isoformat()
            if path.name in self.failure_map:
                del self.failure_map[path.name]
            await self._track_progress(path.name, "success")
            logger.info(Fore.GREEN + f"[✓] Completed: {path.name}")
            return True

        except Exception as e:
            self.failure_map[path.name] = f"{datetime.now().isoformat()} :: {str(e)}"
            await self._track_progress(path.name, f"failed:{str(e)}")
            logger.error(Fore.RED + f"[FAIL] {path.name} => {e}")
            return False

    async def run_ingestion(self):
        try:
            # Recursively get all files matching the given patterns
            all_files = [
                f for f in self.input_dir.rglob("*")
                if any(f.name.endswith(ext) for ext in self.patterns)
            ]
            done_files = set(self.success_map.keys())
            pending_files = [f for f in all_files if f.name not in done_files]

            await self.redis.delete("bulk_ingest:all_files")
            if pending_files:
                await self.redis.sadd("bulk_ingest:all_files", *[f.name for f in pending_files])

            logger.info(f"📁 Total: {len(all_files)} | ✅ Done: {len(done_files)} | 🚀 Pending: {len(pending_files)}")
            if not pending_files:
                logger.info("Nothing to ingest.")
                return

            # - Prepare and execute concurrent ingestion tasks with limited concurrency:
            # - Use a semaphore to restrict the number of concurrent tasks to self.concurrency
            # - For each pending file, create an async ingestion task with progress metadata
            # - Run all tasks concurrently using tqdm_asyncio.gather to show a progress bar
            # - After all tasks finish, write updated success and failure maps to their respective JSON files

            semaphore = asyncio.Semaphore(self.concurrency)
            tasks = [
                self._ingest_file(path, idx + 1, len(pending_files), semaphore)
                for idx, path in enumerate(pending_files)
            ]

            await tqdm_asyncio.gather(*tasks)
            self._write_json(self.success_json, self.success_map)
            self._write_json(self.failure_json, self.failure_map)

        except Exception as e:
            logger.critical(Fore.RED + f"[CRITICAL] Ingestion run failed: {e}")

    async def retry_failures(self):
        try:
            failed_files = list(self.failure_map.keys())
            if not failed_files:
                logger.info("No failed files to retry.")
                return

            logger.info(f"🔁 Retrying {len(failed_files)} failed files...")
            paths = [self.input_dir / fname for fname in failed_files if (self.input_dir / fname).exists()]
            self.failure_map.clear()

            semaphore = asyncio.Semaphore(self.concurrency)
            tasks = [
                self._ingest_file(path, idx + 1, len(paths), semaphore)
                for idx, path in enumerate(paths)
            ]

            await tqdm_asyncio.gather(*tasks)
            self._write_json(self.success_json, self.success_map)
            self._write_json(self.failure_json, self.failure_map)

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
    parser.add_argument("--patterns", default=','.join(DEFAULT_PATTERN_LIST), help="Comma-separated file extensions like .pdf,.txt")
    return parser.parse_args()


async def main():
    args = parse_args()
    setup_signal_handlers()

    ingestor = BulkFileIngestor(
        input_dir=args.input_dir,
        log_dir=args.log_dir,
        concurrency=args.concurrency,
        patterns=args.patterns.split(",")
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
