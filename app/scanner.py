import asyncio
import websockets
import json
import redis
import httpx
from datetime import datetime

r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

# --- CONFIGURACIÓN DE DISPARO ---
VOLUMEN_MINIMO_USDT = 500.0
MULTIPLICADOR_TSUNAMI = 1.5
PRECIO_MAXIMO = 100.0

async def obtener_top_50_volatiles():
    url = "https://api.binance.com/api/v3/ticker/24hr"
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
            data = response.json()

            excluir = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT', 'USDCUSDT', 'FDUSDUSDT', 'DAIUSDT', 'PYUSDUSDT']

            # Filtro estricto de precio y exclusión
            filtrados = []
            for d in data:
                simbolo = d['symbol']
                precio = float(d['lastPrice'])

                if simbolo.endswith('USDT') and precio < PRECIO_MAXIMO and simbolo not in excluir:
                    filtrados.append(d)

            top_50 = sorted(filtrados, key=lambda x: float(x['quoteVolume']), reverse=True)[:50]
            pares = [p['symbol'] for p in top_50]

            print(f"🎯 Sniper Calibrado: {len(pares)} activos por debajo de ${PRECIO_MAXIMO}")
            return pares
    except Exception as e:
        print(f"❌ Error API: {e}")
        return []

async def binance_stream():
    from app.main import manager

    pares_objetivo = await obtener_top_50_volatiles()
    if not pares_objetivo: return

    streams = "/".join([f"{p.lower()}@miniTicker" for p in pares_objetivo])
    url = f"wss://stream.binance.com:9443/ws/{streams}"

    memoria_mercado = {}

    while True:
        try:
            async with websockets.connect(url) as websocket:
                while True:
                    msg = await websocket.recv()
                    coin = json.loads(msg)
                    await manager.broadcast(json.dumps({"type": "tick"}))

                    symbol = coin['s']
                    current_vol = float(coin['v'])
                    current_price = float(coin['c'])

                    # Verificación doble de precio por si acaso
                    if current_price >= PRECIO_MAXIMO:
                        continue

                    if symbol in memoria_mercado:
                        prev_vol = memoria_mercado[symbol]['vol']
                        prev_price = memoria_mercado[symbol]['price']

                        if current_vol > (prev_vol * MULTIPLICADOR_TSUNAMI) and current_vol > VOLUMEN_MINIMO_USDT:
                            diff = current_price - prev_price
                            pct_change = (diff / prev_price) * 100 if prev_price > 0 else 0

                            alert_payload = {
                                "type": "alert",
                                "symbol": symbol,
                                "price": f"{current_price:.6f}",
                                "volume": current_vol,
                                "change": f"{pct_change:+.4f}%",
                                "time": datetime.now().strftime("%H:%M:%S")
                            }

                            # Persistencia
                            key = f"tsunami:{symbol}:{int(datetime.now().timestamp())}"
                            r.hset(key, mapping=alert_payload)
                            r.expire(key, 86400)

                            await manager.broadcast(json.dumps(alert_payload))
                            print(f"🚨 {symbol} | ${current_price} | Vol: {current_vol:.0f}")

                    memoria_mercado[symbol] = {'vol': current_vol, 'price': current_price}
        except Exception:
            await asyncio.sleep(5)