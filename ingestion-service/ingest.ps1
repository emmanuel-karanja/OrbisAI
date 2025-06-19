# ingest.ps1

$venvPath = ".\.venv"
$activateScript = "$venvPath\Scripts\Activate.ps1"
$requirementsFile = "requirements.txt"
$ingestScript = "src\bulk_ingestor.py"  # ✅ Corrected path

# Ensure Python is available
if (-not (Get-Command "python" -ErrorAction SilentlyContinue)) {
    Write-Error "❌ Python is not installed or not in PATH."
    exit 1
}

# Step 1: Create virtual environment if missing
if (-not (Test-Path $venvPath)) {
    Write-Host "🔧 Creating virtual environment..."
    python -m venv $venvPath
}

# Step 2: Activate the virtual environment
Write-Host "✅ Activating virtual environment..."
& $activateScript

# Step 3: Install dependencies
if (Test-Path $requirementsFile) {
    Write-Host "📦 Installing dependencies..."
    pip install --upgrade pip
    pip install -r $requirementsFile
} else {
    Write-Warning "⚠️ requirements.txt not found. Skipping package installation."
}

# Step 4: Run ingestion
Write-Host "`n🚀 Running ingestion process..."
python $ingestScript run

# Step 5: Retry failed ones
Write-Host "`n🔁 Retrying any failed ingestions..."
python $ingestScript retry
