import os
import asyncio
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from app.scanner import SniperScanner

app = FastAPI()

# Configuración de Activos (01 Mayo 2026)
PARES = [
    'PEPEUSDT', 'SHIBUSDT', '1000SATSUSDT', 'BONKUSDT', 'FLOKIUSDT',
    'GALAUSDT', 'DOGEUSDT', 'LUNCUSDT', 'XECUSDT', 'WINUSDT'
]

CONFIG = {
    'umbral_volumen': 1.5,
    'max_subida_precio': 1.02,
}

# Montar carpeta static para archivos CSS/JS/Images
if os.path.exists("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

# Instancia única del scanner
scanner_instance = SniperScanner(symbols=PARES, config=CONFIG)

@app.on_event("startup")
async def startup_event():
    # Iniciamos el loop de Binance en segundo plano
    asyncio.create_task(scanner_instance.start())

@app.get("/")
async def get_index():
    # Ruta absoluta para encontrar index.html dentro de /static
    base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    index_path = os.path.join(base_path, "static", "index.html")
    return FileResponse(index_path)

@app.get("/api/status")
async def get_status():
    # Endpoint que consume el JavaScript del frontend
    return scanner_instance.get_current_status()