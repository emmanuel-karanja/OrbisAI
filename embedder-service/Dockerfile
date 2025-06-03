FROM python:3.11-slim

WORKDIR /app

# Copy requirements first to leverage caching
COPY requirements.txt .

# Install dependencies only if requirements.txt changed
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code last (less likely to change dependencies)
COPY main.py .

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "5000"]
