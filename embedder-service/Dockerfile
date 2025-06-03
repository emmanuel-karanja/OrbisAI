FROM python:3.11-slim

WORKDIR /app

# Install basic system packages needed for many wheels
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install with CPU-only PyTorch wheel index
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt \
     --extra-index-url https://download.pytorch.org/whl/cpu

# Preload the model to cache it in the image layer
RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('all-MiniLM-L6-v2')"

# Copy your application code
COPY main.py .

# Run your FastAPI app with uvicorn
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "5000"]
