import asyncio
import json
import redis
import websockets
from collections import deque

class BinanceScanner:
    def __init__(self):
        self.r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        self.symbols = [
            'btcusdt', 'ethusdt', 'solusdt', 'bnbusdt', 'xrpusdt',
            'dogeusdt', 'adausdt', 'trxusdt', 'dotusdt'
        ]
        self.base_url = "wss://stream.binance.com:9443/ws"
        self.streams = "/".join([f"{s}@bookTicker" for s in self.symbols])

        # Guardamos en memoria el historial segundo a segundo (5 minutos = 300 muestras)
        self.price_history = {s.upper(): deque(maxlen=300) for s in self.symbols}
        self.last_tick_time = {s.upper(): 0 for s in self.symbols}

    async def start_scanning(self):
        url = f"{self.base_url}/{self.streams}"
        print("[INFO] SCANNER: Conectando al WebSocket de Binance (Filtro Multi-Temporal)...")

        while True:
            try:
                async with websockets.connect(url) as websocket:
                    market_data = {s.upper(): {} for s in self.symbols}
                    print("[INFO] SCANNER: Conexion WebSocket establecida con exito.")

                    while True:
                        try:
                            raw_data = await websocket.recv()
                            data = json.loads(raw_data)

                            symbol = data['s']
                            bid_p, bid_q = float(data['b']), float(data['B'])
                            ask_p, ask_q = float(data['a']), float(data['A'])

                            imbalance = bid_q / ask_q if ask_q > 0 else 1.0
                            current_mid = (bid_p + ask_p) / 2

                            now = asyncio.get_event_loop().time()

                            # Registramos una muestra limpia en el historial exactamente cada 1 segundo
                            if now - self.last_tick_time[symbol] >= 1.0:
                                self.price_history[symbol].append(current_mid)
                                self.last_tick_time[symbol] = now

                            # --- CALCULOS ESTRUCTURALES ---
                            history = self.price_history[symbol]
                            history_len = len(history)

                            trend_30s = "NEUTRAL"
                            trend_1m = "NEUTRAL"
                            range_pct = 50.0
                            min_p = current_mid  # Respaldo

                            # Tendencia corta de 30 segundos
                            if history_len >= 30:
                                if current_mid > history[-30]: trend_30s = "UP"
                                elif current_mid < history[-30]: trend_30s = "DOWN"

                            # Tendencia intermedia de 1 minuto
                            if history_len >= 60:
                                if current_mid > history[-60]: trend_1m = "UP"
                                elif current_mid < history[-60]: trend_1m = "DOWN"

                            # Ubicacion del Rango y Captura del Suelo Real de 5 minutos
                            if history_len > 1:
                                max_p = max(history)
                                min_p = min(history)  # 🔍 SUELO REAL AUDITADO POR WEBSOCKET
                                if max_p > min_p:
                                    range_pct = ((current_mid - min_p) / (max_p - min_p)) * 100

                            # Validacion de alineacion de tendencias para la señal de ejecucion
                            if trend_30s == "UP" and trend_1m == "UP":
                                price_direction = "UP"
                            elif trend_30s == "DOWN" and trend_1m == "DOWN":
                                price_direction = "DOWN"
                            else:
                                price_direction = "NEUTRAL"

                            market_data[symbol] = {
                                "symbol": symbol,
                                "bid": bid_p,
                                "ask": ask_p,
                                "imbalance": imbalance,
                                "price_direction": price_direction,
                                "range_pct": range_pct,
                                "min_price_5m": min_p  # ⬅️ ENVIADO AL BACKEND SIN PERDER RESOLUCION
                            }

                            # Inyeccion masiva del estado actual en Redis
                            self.r.set("market_status", json.dumps(list(market_data.values())))

                        except (websockets.exceptions.ConnectionClosed, asyncio.exceptions.CancelledError):
                            break
                        except Exception as e:
                            print(f"[ERROR] SCANNER: Fallo en el procesamiento de datos del stream: {e}")
                            await asyncio.sleep(1)

            except Exception as e:
                print(f"[ERROR] SCANNER: Desconexion del servidor WebSocket. Reintentando en 5s... ({e})")
                await asyncio.sleep(5)

if __name__ == "__main__":
    scanner = BinanceScanner()
    try:
        asyncio.run(scanner.start_scanning())
    except KeyboardInterrupt:
        print("\n[INFO] SCANNER: Radar apagado por el usuario.")