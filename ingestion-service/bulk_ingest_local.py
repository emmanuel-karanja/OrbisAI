import os
import base64
import requests

def ingest_law_documents(base_dir="kenyan_law", subdirs=("acts", "constitution"), api_url="http://localhost:8001/ingest"):
    """
    Ingests all files from specified subdirectories under the given base directory.

    Args:
        base_dir (str): Root directory containing legal document folders.
        subdirs (tuple): Subdirectories within base_dir to ingest files from.
        api_url (str): Endpoint to POST the ingested file content.
    """
    for subdir in subdirs:
        dir_path = os.path.join(base_dir, subdir)
        if not os.path.isdir(dir_path):
            print(f"⚠️ Skipping missing directory: {dir_path}")
            continue

        for fname in os.listdir(dir_path):
            fpath = os.path.join(dir_path, fname)
            if not os.path.isfile(fpath):
                continue

            with open(fpath, "rb") as f:
                encoded = base64.b64encode(f.read()).decode("utf-8")

            data = {
                "filename": fname,
                "content": encoded
            }

            try:
                response = requests.post(api_url, json=data, timeout=60)
                print(f"[{fname}] Status: {response.status_code}, Message: {response.text}")
            except Exception as e:
                print(f"❌ Failed to ingest {fname}: {e}")

# Example usage
if __name__ == "__main__":
    ingest_law_documents()
