import asyncio
import redis
import os
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from app.scanner import binance_stream

app = FastAPI()

# --- REDIS ---
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)

# --- WS MANAGER ---
class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
    async def broadcast(self, message: str):
        for connection in self.active_connections:
            try: await connection.send_text(message)
            except: pass

manager = ConnectionManager()

# Servir estáticos
if os.path.exists("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
async def get_index():
    return FileResponse(os.path.join("static", "index.html"))

@app.websocket("/ws/volumen")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True: await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)

@app.on_event("startup")
async def startup_event():
    # Iniciamos el scanner enfocado
    asyncio.create_task(binance_stream())

@app.get("/historial")
async def get_history():
    keys = r.keys("tsunami:*")
    sorted_keys = sorted(keys, reverse=True)[:30]
    return [r.hgetall(k) for k in sorted_keys if r.exists(k)]