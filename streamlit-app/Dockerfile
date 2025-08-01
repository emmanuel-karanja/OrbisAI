FROM python:3.11-slim

WORKDIR /app

# Install system dependencies needed by some Python packages
RUN set -e; \
    for i in 1 2 3; do \
        apt-get update && \
        apt-get install -y --no-install-recommends \
            build-essential \
            curl \
            iputils-ping && \
        rm -rf /var/lib/apt/lists/* && break || sleep 5; \
    done

# Upgrade pip to support --resume-retries
RUN pip install --no-cache-dir --upgrade "pip>=23.2"

# Copy requirements separately for caching
COPY requirements.txt .

# Install Python dependencies with retries and resumption
RUN set -e; \
    mkdir -p /root/.cache/pip/wheels; \
    for i in 1 2 3; do \
        pip install \
            --upgrade \
            --cache-dir=/root/.cache/pip \
            --find-links=/root/.cache/pip/wheels \
            --no-cache-dir \
            --retries 3 \
            --resume-retries 3 \
            --extra-index-url https://download.pytorch.org/whl/cpu \
            -r requirements.txt && break || sleep 10; \
    done

# Copy your app code
COPY app.py .
COPY logger.py .

# Expose Streamlit default port
EXPOSE 8501

# Run Streamlit
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
