FROM python:3.11-slim

WORKDIR /app

# Copy requirements first to leverage caching
COPY requirements.txt .

# Install dependencies only if requirements.txt changed
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt --index-url https://download.pytorch.org/whl/cpu
RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('all-MiniLM-L6-v2')"


# Copy app code last (less likely to change dependencies)
COPY main.py .

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "5000"]
