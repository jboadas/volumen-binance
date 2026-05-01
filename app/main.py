import os
import asyncio
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from app.scanner import SniperScanner

app = FastAPI()

# Tus 10 pares de bajo precio
PARES = ['PEPEUSDT', 'SHIBUSDT', '1000SATSUSDT', 'BONKUSDT', 'FLOKIUSDT',
         'GALAUSDT', 'DOGEUSDT', 'LUNCUSDT', 'XECUSDT', 'WINUSDT']

CONFIG = {'umbral_volumen': 1.5, 'max_subida_precio': 1.02}

if os.path.exists("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

scanner_instance = SniperScanner(symbols=PARES, config=CONFIG)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(scanner_instance.start())

@app.get("/")
async def get_index():
    base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return FileResponse(os.path.join(base_path, "static", "index.html"))

@app.get("/api/status")
async def get_status():
    return scanner_instance.get_current_status()