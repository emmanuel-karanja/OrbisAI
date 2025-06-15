# OrbisAI RAG

Test RAG (Retrieval Augmented Generation)

STEPS:

1. Make sure Docker is installed. Check online how todo that.  Navigate to each subproject directory e.g. ../ingestion-service/
2. If it's the first time, first run the powesrshell script `./setup.ps1` to setup the .venv.
3. Run `pip install -r requirements.txt` to install dependencies.
4. Run `run.ps1` to start the application. This will start docker and run redis, Qrant db, n8n workflow manager, ingestion-service and the Streamlit UI for querying.
5. Go to  http://localhost:8501 to access the Streamlit UI

# LAW CRAWLER

Crawls the Kenya Law Review and downloads documents to be embedded:

STEPS:

1. Make sure Docker is installed. Check online how todo that.  Navigate to the subproject directory e.g. ../law_crawler/
2. If it's the first time, first run the powesrshell script `./setup.ps1` to setup the .venv.
3. Run `pip install -r requirements.txt` to install dependencies.
4. Run `run.ps1` to start the crawler application.