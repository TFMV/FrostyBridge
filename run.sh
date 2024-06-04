#!/bin/bash

# Run the FastAPI server
uvicorn app.main:app --host 0.0.0.0 --port 8000
