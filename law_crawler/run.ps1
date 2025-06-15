
# Activate virtual environment
& .\venv\Scripts\Activate.ps1

# Run the Python crawler using correct env keys
python law_crawler.py `
    --url $env:CRAWL_START_URL `
    --depth $env:CRAWL_MAX_DEPTH `
    --workers $env:CRAWL_MAX_WORKERS `
    --output $env:CRAWL_OUTPUT
