import asyncio
import websockets
import json
import redis
from datetime import datetime

# Conexión a Redis
r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

# Configuración del Sniper
VOLUMEN_MINIMO_USDT = 50.0
MULTIPLICADOR_TSUNAMI = 3.0

async def binance_stream():
    """
    Motor del Sniper con inyección de alertas de prueba.
    """
    from app.main import manager  # Import local para evitar circular import

    url = "wss://stream.binance.com:9443/ws/!miniTicker@arr"
    volumen_previo = {}
    contador_debug = 0

    while True:
        try:
            async with websockets.connect(url) as websocket:
                print("✅ Scanner conectado y monitoreando Binance...")

                while True:
                    msg = await websocket.recv()
                    data = json.loads(msg)
                    contador_debug += 1

                    # 1. Avisar al Frontend del TICK (Mueve el contador de paquetes)
                    await manager.broadcast(json.dumps({"type": "tick"}))

                    # --- MODO TEST: Forzamos una alerta cada 20 paquetes ---
                    if contador_debug % 20 == 0:
                        test_alert = {
                            "type": "alert",
                            "symbol": "BTC-TEST",
                            "price": "99999.99",
                            "volume": "5000",
                            "time": datetime.now().strftime("%H:%M:%S")
                        }
                        await manager.broadcast(json.dumps(test_alert))
                        print(f"📡 [DEBUG] Alerta de prueba enviada ({contador_debug})")
                    # ------------------------------------------------------

                    for coin in data:
                        symbol = coin['s']
                        if not symbol.endswith('USDT'):
                            continue

                        current_volume = float(coin['v'])
                        close_price = coin['c']

                        # 2. Lógica real de detección de Tsunami
                        if symbol in volumen_previo:
                            prev = volumen_previo[symbol]

                            if current_volume > (prev * MULTIPLICADOR_TSUNAMI) and current_volume > VOLUMEN_MINIMO_USDT:
                                timestamp = datetime.now().strftime("%H:%M:%S")

                                alert_payload = {
                                    "type": "alert",
                                    "symbol": symbol,
                                    "price": close_price,
                                    "volume": current_volume,
                                    "time": timestamp
                                }

                                # Guardar en Redis
                                key = f"tsunami:{symbol}:{int(datetime.now().timestamp())}"
                                r.hset(key, mapping=alert_payload)
                                r.expire(key, 86400)

                                # Enviar alerta real al WebSocket
                                await manager.broadcast(json.dumps(alert_payload))
                                print(f"🌊 TSUNAMI DETECTADO: {symbol}")

                        volumen_previo[symbol] = current_volume

        except Exception as e:
            print(f"⚠️ Error en Scanner: {e}. Reintentando...")
            await asyncio.sleep(5)
