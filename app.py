from fastapi import FastAPI
import os
import socket

app = FastAPI()

APP_MESSAGE = os.getenv("APP_MESSAGE", "chaos lab running")


@app.get("/")
def home():
    return {
        "message": APP_MESSAGE,
        "hostname": socket.gethostname()
    }


@app.get("/status")
def status():
    return {"status": "healthy"}


@app.get("/crash")
def crash():
    os._exit(1)