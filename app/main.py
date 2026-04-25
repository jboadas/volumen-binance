import json
import asyncio
import websockets
import os
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse

app = FastAPI(title="Binance Sniper V1 - Auto-Resilient")

# Configuración de carpetas estáticas
if not os.path.exists("static"):
    os.makedirs("static")
app.mount("/static", StaticFiles(directory="static"), name="static")

# Memoria para el cálculo de deltas
cache_volumen = {}

# Lista de pares afinada para no saturar tu red
PARES = [
    "btcusdt", "ethusdt", "solusdt", "bnbusdt",
    "xrpusdt", "dogeusdt", "pepeusdt", "linkusdt",
    "nearusdt", "avaxusdt", "shibusdt"
]

# Construcción de la URL de Binance (Modo Stream Combinado)
binance_ws_url = f"wss://stream.binance.com:443/ws/{'/'.join([p + '@ticker' for p in PARES])}"

@app.get("/", response_class=HTMLResponse)
async def get_index():
    try:
        with open("static/index.html", "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return "<h1>Falta static/index.html</h1>"

@app.websocket("/ws/volumen")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    print(f"✅ [Navegador] Cliente conectado. Monitoreando: {', '.join(PARES)}")

    # --- BUCLE EXTERNO DE RECONEXIÓN ---
    while True:
        try:
            print("🔗 [Binance] Intentando conectar al stream...")
            async with websockets.connect(binance_ws_url, ping_interval=20, ping_timeout=20) as binance_ws:
                print("🚀 [Binance] Conexión establecida. Recibiendo datos...")

                # --- BUCLE INTERNO DE PROCESAMIENTO ---
                while True:
                    raw_data = await binance_ws.recv()
                    ticker = json.loads(raw_data)

                    symbol = ticker['s']
                    vol_actual = float(ticker['q'])
                    precio_actual = float(ticker['c'])

                    if symbol in cache_volumen:
                        vol_previo = cache_volumen[symbol]['vol']
                        precio_previo = cache_volumen[symbol]['precio']
                        delta_vol = vol_actual - vol_previo

                        # Filtro de alerta: Solo movimientos > $5,000
                        if delta_vol > 5000:
                            # Clasificación de la alerta
                            tipo = "NORMAL"
                            if delta_vol > 50000: tipo = "WHALE"
                            if delta_vol > 200000: tipo = "TSUNAMI"

                            tendencia = "🚀" if precio_actual >= precio_previo else "🔻"

                            # Enviamos la alerta al navegador
                            await websocket.send_json({
                                "type": "VOLUME_SPIKE",
                                "data": [{
                                    "symbol": symbol,
                                    "delta_usdt": f"{delta_vol:,.2f}",
                                    "precio": f"{precio_actual:,.4f}",
                                    "dir": tendencia,
                                    "alert_type": tipo
                                }]
                            })

                    # Actualizar caché para el siguiente cálculo
                    cache_volumen[symbol] = {"vol": vol_actual, "precio": precio_actual}

        except (websockets.exceptions.ConnectionClosed, asyncio.TimeoutError) as e:
            print(f"⚠️ [Conexión] Se perdió el enlace con Binance ({e}). Reintentando en 5s...")
            await asyncio.sleep(5)
            continue
        except WebSocketDisconnect:
            print("❌ [Navegador] El usuario cerró la pestaña. Deteniendo escucha.")
            break
        except Exception as e:
            # Aquí corregimos el f-string que causó el error anterior
            print(f"🔥 [Error] Inesperado: {e}. Reiniciando en 5s...")
            await asyncio.sleep(5)
            continue
