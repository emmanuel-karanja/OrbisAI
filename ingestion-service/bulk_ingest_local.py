import os
import base64
import requests

API_URL = "http://localhost:8001/ingest"  # or docker host IP
SOURCE_DIR = "/path/to/docs"  # Replace with your directory

for fname in os.listdir(SOURCE_DIR):
    fpath = os.path.join(SOURCE_DIR, fname)
    if not os.path.isfile(fpath):
        continue
    with open(fpath, "rb") as f:
        encoded = base64.b64encode(f.read()).decode("utf-8")

    data = {
        "filename": fname,
        "content": encoded
    }

    response = requests.post(API_URL, json=data)
    print(f"[{fname}] Status: {response.status_code}, Message: {response.text}")