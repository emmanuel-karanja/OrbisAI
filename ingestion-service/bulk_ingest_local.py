import os
import base64
import requests
import argparse
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

DEFAULT_API_URL = os.getenv("INGEST_API_URL", "http://localhost:8001/ingest")
DEFAULT_DOCS_SOURCE_dir=os.getenv("DOCS_SOURCE_DIR","C:/Users/ZBOOK/Downloads/kenya_laws")

def ingest_law_documents(source_dir, api_url=DEFAULT_API_URL):
    """
    Ingests all files from all subdirectories under the given source directory.

    Args:
        source_dir (str): Root directory to scan.
        api_url (str): Ingestion endpoint.
    """
    if not os.path.isdir(source_dir):
        print(f"❌ Source directory does not exist: {source_dir}")
        return

    for root, _, files in os.walk(source_dir):
        for fname in files:
            fpath = os.path.join(root, fname)
            try:
                with open(fpath, "rb") as f:
                    encoded = base64.b64encode(f.read()).decode("utf-8")

                data = {
                    "filename": fname,
                    "content": encoded
                }

                response = requests.post(api_url, json=data, timeout=60)
                print(f"[{fpath}] → Status {response.status_code}: {response.text}")
            except Exception as e:
                print(f"❌ Error ingesting {fpath}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest documents from a given directory recursively.")
    parser.add_argument("source_dir", help="Path to the root directory containing documents to ingest.")
    parser.add_argument("--api", default=DEFAULT_API_URL, help="Ingestion API endpoint.")
    args = parser.parse_args()

    ingest_law_documents(args.source_dir, args.api)
