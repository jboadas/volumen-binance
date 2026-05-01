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
        # Construcción de URL para múltiples streams (Ticker de 24h)
        streams = "/".join([f"{s}@ticker" for s in self.symbols_low])
        uri = f"wss://stream.binance.com:9443/ws/{streams}"

        print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] Conectando a Binance WebSocket...")

        while True:
            try:
                async with websockets.connect(uri) as websocket:
                    print("✅ Conexión establecida con Binance.")
                    while True:
                        message = await websocket.recv()
                        data = json.loads(message)

                        symbol = data['s']
                        precio_actual = float(data['c'])
                        # Aquí podrías calcular volumen relativo real comparando data['v']
                        # Por ahora mantenemos un valor de monitoreo
                        vol_rel = 1.05

                        # Actualizamos Redis
                        self.db.set(f"precio_actual_{symbol}", precio_actual)
                        self.db.set(f"vol_relativo_{symbol}", vol_rel)

            except Exception as e:
                print(f"❌ Error en el stream: {e}. Reintentando en 5 segundos...")
                await asyncio.sleep(5)

    def evaluar_entrada(self, simbolo, precio_actual, vol_relativo):
        key_precio = f"precio_base_{simbolo}"
        precio_1h = self.db.get(key_precio)

        # Evitar división por cero y sincronizar precio inicial
        if not precio_1h or float(precio_1h) <= 0:
            if precio_actual > 0:
                self.db.setex(key_precio, 3600, precio_actual)
            return "Sincronizando...", "INFO"

        precio_1h = float(precio_1h)
        variacion = (precio_actual / precio_1h) - 1

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