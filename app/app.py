from fastapi import FastAPI
from app.routers import router

app = FastAPI()

app.include_router(router)

@app.get("/")
async def read_root():
    return {"message": "Welcome to FrostyBridge API"}
