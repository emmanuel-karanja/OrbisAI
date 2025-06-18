import re
from hashlib import sha256

class FileUtils:
    @staticmethod
    def sanitize_filename(text):
        text = re.sub(r"[^\w\-]+", "_", text)
        text = re.sub(r"_+", "_", text)
        return text.strip("_")[:80] or "untitled"

    @staticmethod
    def file_hash(content):
        return sha256(content).hexdigest()
