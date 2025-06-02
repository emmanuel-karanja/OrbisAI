FROM python:3.13-slim

WORKDIR /app

COPY app.py .

RUN pip install streamlit requests

CMD ["uvicorn", "ingest:app", "--host", "0.0.0.0", "--port", "8001"]
