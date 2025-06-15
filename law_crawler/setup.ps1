# Step 1: Create virtual environment
python -m venv venv

# Step 2: Activate the virtual environment
& .\venv\Scripts\Activate.ps1

# Step 3: Install dependencies (edit/add as needed)
pip install requests beautifulsoup4 selenium python-dotenv tqdm webdriver-manager

# Step 4: Save installed packages to requirements.txt
pip freeze > requirements.txt

# Optional: Output success message
Write-Host "`n✅ Virtual environment set up and dependencies installed.`n"
