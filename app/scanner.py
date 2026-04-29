import asyncio
import websockets
import json
import redis
import httpx
from datetime import datetime

r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

VOLUMEN_MINIMO_USDT = 1000.0  # Subimos un poco el filtro dado que son Top 50
MULTIPLICADOR_TSUNAMI = 3.0

async def obtener_top_50_volumen():
    """Obtiene los 50 pares USDT con más volumen en las últimas 24h"""
    url = "https://api.binance.com/api/v3/ticker/24hr"
    async with httpx.AsyncClient() as client:
        response = await client.get(url)
        data = response.json()

        # Filtrar solo USDT y ordenar por volumen (quoteVolume es USDT)
        usdt_pairs = [dict for dict in data if dict['symbol'].endswith('USDT')]
        top_50 = sorted(usdt_pairs, key=lambda x: float(x['quoteVolume']), reverse=True)[:50]
        return [p['symbol'] for p in top_50]

async def binance_stream():
    from app.main import manager

    try:
        pares_objetivo = await obtener_top_50_volumen()
        print(f"🎯 Monitoreando Top 50 Vol: {pares_objetivo[:5]}... {len(pares_objetivo)} pares.")
    except Exception as e:
        print(f"❌ Error obteniendo top 50: {e}")
        return

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
                                "change": f"{pct_change:+.2f}%",
                                "time": datetime.now().strftime("%H:%M:%S")
                            }

                            key = f"tsunami:{symbol}:{int(datetime.now().timestamp())}"
                            r.hset(key, mapping=alert_payload)
                            r.expire(key, 86400)
                            await manager.broadcast(json.dumps(alert_payload))

                    memoria_mercado[symbol] = {'vol': current_vol, 'price': current_price}
        except Exception as e:
            await asyncio.sleep(5)