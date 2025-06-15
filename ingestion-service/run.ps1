# run.ps1

# Optional: Set environment variables
$env:INGEST_API_URL = "http://localhost:8001/ingest"
$env:DOCS_SOURCE_DIR = "C:\Users\ZBOOK\Downloads\kenya_laws\pdfs"

# Activate virtual environment if needed
# Replace 'venv' with your actual venv folder name if different
if (Test-Path ".\venv\Scripts\Activate.ps1") {
    .\venv\Scripts\Activate.ps1
} else {
    Write-Host "⚠️ Virtual environment not found. Make sure dependencies are installed."
}

# Start the FastAPI app with uvicorn
Write-Host "🚀 Starting FastAPI app..."
#uvicorn main:app --host 0.0.0.0 --port 8001 --reload

docker-compose up --build
