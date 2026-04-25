import asyncio
import websockets
import json
import redis
from datetime import datetime

r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

VOLUMEN_MINIMO_USDT = 500.0
MULTIPLICADOR_TSUNAMI = 3.0

async def binance_stream():
    from app.main import manager
    url = "wss://stream.binance.com:9443/ws/!miniTicker@arr"
    volumen_previo = {}

    while True:
        try:
            async with websockets.connect(url) as websocket:
                print("✅ Scanner activo - Esperando Tsunamis...")
                while True:
                    msg = await websocket.recv()
                    data = json.loads(msg)
                    await manager.broadcast(json.dumps({"type": "tick"}))

                    for coin in data:
                        symbol = coin['s']
                        if not symbol.endswith('USDT'): continue
                        current_volume = float(coin['v'])

                        if symbol in volumen_previo:
                            prev = volumen_previo[symbol]
                            if current_volume > (prev * MULTIPLICADOR_TSUNAMI) and current_volume > VOLUMEN_MINIMO_USDT:
                                alert_payload = {
                                    "type": "alert",
                                    "symbol": symbol,
                                    "price": coin['c'],
                                    "volume": current_volume,
                                    "time": datetime.now().strftime("%H:%M:%S")
                                }
                                key = f"tsunami:{symbol}:{int(datetime.now().timestamp())}"
                                r.hset(key, mapping=alert_payload)
                                r.expire(key, 86400)
                                await manager.broadcast(json.dumps(alert_payload))
                        volumen_previo[symbol] = current_volume
        except Exception as e:
            await asyncio.sleep(5)
