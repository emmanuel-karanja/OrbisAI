# redis_client.py
import os
import redis
from utils.logger import setup_logger

LOG_DIR = os.getenv("LOG_DIR", "logs")
logger = setup_logger(name="redis-client", log_dir=LOG_DIR, log_to_file=True)

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

try:
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    r.ping()
    logger.info(f"Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
except redis.ConnectionError as ce:
    logger.error(f"Redis connection failed: {ce}")
    r = None
except Exception as e:
    logger.error(f"Unexpected error during Redis initialization: {e}")
    r = None
