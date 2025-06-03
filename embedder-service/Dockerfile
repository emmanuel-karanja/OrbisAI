FROM python:3.13-alpine

WORKDIR /app

# Install basic system packages needed for many wheels
RUN apk add --no-cache \
    build-base \
    curl \
    iputils

# Copy requirements and install with CPU-only PyTorch wheel index
COPY requirements.txt .

RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt \
     --extra-index-url https://download.pytorch.org/whl/cpu

# Preload the model, but continue even if this fails
RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('all-MiniLM-L6-v2')" \
    || echo "Model preload failed; it will download at runtime."

# Copy your application code
COPY main.py .
COPY utils /app/utils

# Run your FastAPI app with uvicorn
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "5000"]
