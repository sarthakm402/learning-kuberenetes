from fastapi import FastAPI
import os
import socket

app=FastAPI()
@app.get("/")
def home():
    return{
        "messsage":"cahos lab running",
        "hostname":socket.gethostname()
    }

@app.get("/status")
def status():
    return {"status": "healthy"}
@app.get("/crash")
def crash():
    os._exit(1)