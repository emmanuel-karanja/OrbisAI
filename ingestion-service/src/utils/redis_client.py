# redis_client.py
import os
import redis.asyncio as redis
from utils.logger import setup_logger

LOG_DIR = os.getenv("LOG_DIR", "logs")
logger = setup_logger(name="redis-client", log_dir=LOG_DIR, log_to_file=True)

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

_redis_instance: redis.Redis = None

async def get_redis() -> redis.Redis:
    global _redis_instance
    if _redis_instance is None:
        await init_redis()
    return _redis_instance

async def init_redis() -> None:
    global _redis_instance
    try:
        _redis_instance = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            decode_responses=True
        )
        await _redis_instance.ping()
        logger.info(f"Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
    except redis.exceptions.ConnectionError as ce:
        logger.error(f"Redis connection failed: {ce}")
        _redis_instance = None
    except Exception as e:
        logger.error(f"Unexpected error during Redis initialization: {e}")
        _redis_instance = None
