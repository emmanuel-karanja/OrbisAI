# Step 1: Create virtual environment
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
python -m venv venv

# Step 2: Activate the virtual environment
& .\venv\Scripts\Activate.ps1

# Step 3: Install dependencies (edit/add as needed)


# Step 4: Save installed packages to requirements.txt
pip freeze > requirements.txt

# Optional: Output success message
Write-Host "`n✅ Virtual environment set up and dependencies installed.`n"
