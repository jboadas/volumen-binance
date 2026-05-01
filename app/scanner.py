import asyncio
import datetime
import json
import redis
import websockets

class SniperScanner:
    def __init__(self, symbols, config):
        self.symbols_raw = [s.upper() for s in symbols]
        self.symbols_low = [s.lower() for s in symbols]
        self.config = config
        self.db = redis.Redis(host='localhost', port=6379, decode_responses=True)

    async def start(self):
        # Ticker stream de Binance para los 10 pares
        streams = "/".join([f"{s}@ticker" for s in self.symbols_low])
        uri = f"wss://stream.binance.com:9443/ws/{streams}"

        while True:
            try:
                async with websockets.connect(uri) as websocket:
                    while True:
                        message = await websocket.recv()
                        data = json.loads(message)

                        symbol = data['s']
                        precio_actual = float(data['c'])
                        vol_24h = float(data['q']) # Volumen en USDT

                        # Lógica de Impulso de Volumen
                        key_v_prev = f"vol_prev_{symbol}"
                        vol_prev = float(self.db.get(key_v_prev) or vol_24h)

                        # Calculamos el ratio de incremento
                        impulso = (vol_24h / vol_prev) if vol_prev > 0 else 1.0

                        # Actualizamos Redis
                        self.db.set(f"precio_actual_{symbol}", precio_actual)
                        self.db.set(f"vol_relativo_{symbol}", round(impulso, 4))
                        # Guardamos el volumen actual como base para el siguiente tick (expira en 1 min)
                        self.db.setex(key_v_prev, 60, vol_24h)

            except Exception:
                await asyncio.sleep(5)

    def evaluar_entrada(self, simbolo, precio_actual, vol_relativo):
        key_precio = f"precio_base_{simbolo}"
        precio_1h = self.db.get(key_precio)

        if not precio_1h or float(precio_1h) <= 0:
            if precio_actual > 0:
                self.db.setex(key_precio, 3600, precio_actual)
            return "Sincronizando...", "INFO"

        precio_1h = float(precio_1h)
        variacion = (precio_actual / precio_1h) - 1

        # Disparo basado en tu configuración (Umbral 1.5x)
        if vol_relativo >= self.config['umbral_volumen']:
            if precio_actual <= (precio_1h * self.config['max_subida_precio']):
                return f"🔥 COMPRAR (+{variacion:.2%})", "BUY"
            else:
                return f"⚠️ TARDE (+{variacion:.2%})", "SKIP"

        return "Escaneando...", "SCAN"

    def get_current_status(self):
        stats = []
        for s in self.symbols_raw:
            p = float(self.db.get(f"precio_actual_{s}") or 0)
            v = float(self.db.get(f"vol_relativo_{s}") or 1.0)
            msg, tipo = self.evaluar_entrada(s, p, v)
            stats.append({"pair": s, "price": p, "vol": v, "alert": msg, "type": tipo})
        return stats