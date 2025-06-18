import os
import json
import threading

class DownloadTracker:
    def __init__(self, index_json_path):
        self.index_json_path = index_json_path
        self.downloaded_files = set()
        self.index = []
        self.lock = threading.Lock()

        self._load_existing_index()

    def _load_existing_index(self):
        if os.path.exists(self.index_json_path):
            try:
                with open(self.index_json_path, "r", encoding="utf-8") as f:
                    existing = json.load(f)
                    for entry in existing:
                        if entry["type"] == "pdf":
                            self.downloaded_files.add(entry.get("file", ""))
                        elif entry["type"] == "akn":
                            self.downloaded_files.add(entry.get("file_html", ""))
                        elif entry["type"] == "docx":
                            self.downloaded_files.add(entry.get("file", ""))
                        self.index.append(entry)
            except Exception as e:
                print(f"⚠️ Could not read or parse index.json: {e}")

    def is_downloaded(self, file_name):
        with self.lock:
            return file_name in self.downloaded_files or os.path.exists(file_name)

    def mark_downloaded(self, file_name):
        with self.lock:
            self.downloaded_files.add(file_name)

    def add_entry(self, entry):
        with self.lock:
            self.index.append(entry)

    def save_index(self):
        with self.lock:
            try:
                with open(self.index_json_path, "w", encoding="utf-8") as f:
                    json.dump(self.index, f, indent=2)
            except Exception as e:
                print(f"❌ Failed to write index.json: {e}")
