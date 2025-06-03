FROM python:3.13-alpine

WORKDIR /app

# Install system dependencies needed by some Python packages
RUN apk add --no-cache \
    build-base \
    curl \
    iputils

# Copy requirements and install all dependencies with CPU-only PyTorch
COPY requirements.txt .

RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir \
    --extra-index-url https://download.pytorch.org/whl/cpu \
    -r requirements.txt

# Copy your app code
COPY app.py .
COPY utils /app/utils

# Expose Streamlit default port
EXPOSE 8501

# Run Streamlit
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
