#!/bin/bash

# Run the FastAPI server
uvicorn app.app:app --host 0.0.0.0 --port 8000
