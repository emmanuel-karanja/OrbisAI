import hashlib
from redis_client import r

class RedisDownloadTracker:
    def __init__(self, prefix="crawler:"):
        self.r = r
        self.prefix = prefix

    def _key(self, path):
        return f"{self.prefix}{hashlib.sha256(path.encode()).hexdigest()}"

    def is_downloaded(self, path):
        if not self.r:
            return False
        return self.r.exists(self._key(path))

    def mark_downloaded(self, path):
        if self.r:
            self.r.set(self._key(path), 1)

    def add_entry(self, entry):
        if self.r:
            self.r.rpush(f"{self.prefix}entries", str(entry))

    def save_index(self):
        # No-op for Redis
        pass
