# ingest.ps1
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass

$venvPath = ".\.venv"
$activateScript = "$venvPath\Scripts\Activate.ps1"
$requirementsFile = "requirements.txt"
$ingestScript = "src\bulk_ingestor.py"

# Ensure Python is available
if (-not (Get-Command "python" -ErrorAction SilentlyContinue)) {
    Write-Error "[ERROR] Python is not installed or not in PATH."
    exit 1
}

# Step 1: Create virtual environment if missing
if (-not (Test-Path $venvPath)) {
    Write-Host "[SETUP] Creating virtual environment..."
    python -m venv $venvPath
}

# Step 2: Activate the virtual environment
Write-Host "[INFO] Activating virtual environment..."
& $activateScript

# Step 3: Upgrade pip explicitly before installing anything else
Write-Host "[INFO] Upgrading pip..."
pip install --upgrade pip

# Step 4: Install dependencies
if (Test-Path $requirementsFile) {
    Write-Host "[INFO] Installing dependencies from requirements.txt..."
    pip install -r $requirementsFile
} else {
    Write-Warning "[WARNING] requirements.txt not found. Skipping package installation."
}

# Step 5: Run ingestion
Write-Host "[INGEST] Running ingestion process..."
python $ingestScript run

# Step 6: Retry failed ingestions
Write-Host "[RETRY] Retrying any failed ingestions..."
python $ingestScript retry
