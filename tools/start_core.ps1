$ErrorActionPreference = "Stop"

python -m uvicorn apps.core_api:app --host 127.0.0.1 --port 8000 --reload

