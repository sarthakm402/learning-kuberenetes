from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import httpx
import os
import socket

app = FastAPI()

APP_MESSAGE = os.getenv("APP_MESSAGE", "chaos lab running")
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434")
MODEL_NAME = os.getenv("MODEL_NAME", "qwen3.5:2b")


class GenerateRequest(BaseModel):
    prompt: str


@app.get("/")
def home():
    return {
        "message": APP_MESSAGE,
        "hostname": socket.gethostname()
    }


@app.get("/status")
def status():
    return {"status": "healthy"}


@app.post("/generate")
async def generate(request: GenerateRequest):
    payload = {
        "model": MODEL_NAME,
        "messages": [
            {"role": "user", "content": request.prompt}
        ],
        "stream": False,
        "think": False,
    }

    try:
        async with httpx.AsyncClient(timeout=120.0) as client:
            response = await client.post(
                f"{OLLAMA_URL}/api/chat",
                json=payload
            )
            response.raise_for_status()

        result = response.json()

        return {
            "model": MODEL_NAME,
            "response": result["message"]["content"]
        }

    except httpx.TimeoutException:
        raise HTTPException(
            status_code=504,
            detail="The model took too long to respond."
        )

    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Ollama returned HTTP {exc.response.status_code}."
        )

    except httpx.RequestError:
        raise HTTPException(
            status_code=503,
            detail="Cannot connect to Ollama."
        )


@app.get("/crash")
def crash():
    os._exit(1)