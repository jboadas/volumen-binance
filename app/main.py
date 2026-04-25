import asyncio
import redis
import os
import json
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from app.scanner import binance_stream

app = FastAPI(title="Binance Volume Sniper V2")

# --- CONFIGURACIÓN DE REDIS ---
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

try:
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
    r.ping()
    print("✅ Conectado a Redis")
except Exception as e:
    print(f"❌ Error: Redis no disponible: {e}")
    r = None

# --- GESTOR DE WEBSOCKETS ---
class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        print(f"📡 Cliente conectado al WS. Total: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except:
                pass

manager = ConnectionManager()

# --- SERVIR ARCHIVOS ESTÁTICOS ---
if os.path.exists("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

# --- RUTAS ---

@app.get("/")
async def get_index():
    index_path = os.path.join("static", "index.html")
    if os.path.exists(index_path):
        return FileResponse(index_path)
    return {"error": "No se encontró index.html en carpeta static"}

@app.websocket("/ws/volumen")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        print("🔌 Cliente desconectado")

@app.on_event("startup")
async def startup_event():
    print("🚀 Iniciando Scanner...")
    asyncio.create_task(binance_stream())

@app.get("/historial")
async def get_history():
    if not r:
        return []
    # Buscamos todas las llaves de tsunamis
    keys = r.keys("tsunami:*")
    # Las ordenamos para tener lo más reciente primero
    sorted_keys = sorted(keys, reverse=True)[:20] # Limitamos a los últimos 20
    historial = []
    for k in sorted_keys:
        data = r.hgetall(k)
        if data:
            historial.append(data)
    return historial

@app.get("/status")
async def get_status():
    return {"status": "online", "redis": r.ping() if r else False}
