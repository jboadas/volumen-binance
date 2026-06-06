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
        self.streams = "/".join([f"{s}@bookTicker/{s}@24hrTicker" for s in self.symbols])

        # 15 min window = 900 samples fixed (1 per second)
        self.price_history = {s.upper(): deque(maxlen=900) for s in self.symbols}
        self.last_tick_time = {s.upper(): 0 for s in self.symbols}

    async def start_scanning(self):
        url = f"{self.base_url}/{self.streams}"
        print("[INFO] SCANNER: Connecting to Binance WebSocket (Multi-Timeframe 15m)...")

        while True:
            try:
                async with websockets.connect(url) as websocket:
                    market_data = {}
                    for s in self.symbols:
                        sym = s.upper()
                        market_data[sym] = {"high_24h": 0.0, "low_24h": 0.0}
                    print("[INFO] SCANNER: WebSocket connection established successfully.")

                    while True:
                        try:
                            raw_data = await websocket.recv()
                            data = json.loads(raw_data)

                            event_type = data.get('e', '')
                            symbol = data['s'].upper()

                            if event_type == '24hrTicker':
                                market_data[symbol]['high_24h'] = float(data['h'])
                                market_data[symbol]['low_24h'] = float(data['l'])
                                self.r.set("market_status", json.dumps(list(market_data.values())))
                                continue

                            bid_p, bid_q = float(data['b']), float(data['B'])
                            ask_p, ask_q = float(data['a']), float(data['A'])

                            imbalance = bid_q / ask_q if ask_q > 0 else 1.0
                            current_mid = (bid_p + ask_p) / 2

                            now = asyncio.get_event_loop().time()

                            # Registramos una muestra limpia en el historial exactamente cada 1 segundo
                            if now - self.last_tick_time[symbol] >= 1.0:
                                self.price_history[symbol].append(current_mid)
                                self.last_tick_time[symbol] = now

                            # --- CALCULOS ESTRUCTURALES UNIFICADOS ---
                            history = self.price_history[symbol]
                            history_len = len(history)

                            trend_1m = "NEUTRAL"
                            trend_5m = "NEUTRAL"
                            range_pct = 50.0
                            min_p = current_mid  # Respaldo

                            # Tendencia de 1 minuto (Fuerza inmediata)
                            if history_len >= 60:
                                if current_mid > history[-60]: trend_1m = "UP"
                                elif current_mid < history[-60]: trend_1m = "DOWN"

                            # Tendencia de 5 minutos (Confirmación de rebote estructural)
                            if history_len >= 300:
                                if current_mid > history[-300]: trend_5m = "UP"
                                elif current_mid < history[-300]: trend_5m = "DOWN"

                            # Ubicacion del Rango y Captura del Suelo Real de los 15 minutos (900 muestras)
                            if history_len > 1:
                                max_p = max(history)
                                min_p = min(history)
                                if max_p > min_p:
                                    range_pct = ((current_mid - min_p) / (max_p - min_p)) * 100

                            # Validacion CORRECTA de alineacion de tendencias (1m y 5m)
                            if trend_1m == "UP" and trend_5m == "UP":
                                price_direction = "UP"
                            elif trend_1m == "DOWN" and trend_5m == "DOWN":
                                price_direction = "DOWN"
                            else:
                                price_direction = "NEUTRAL"

                            market_data[symbol].update({
                                "symbol": symbol,
                                "bid": bid_p,
                                "ask": ask_p,
                                "imbalance": imbalance,
                                "price_direction": price_direction,
                                "range_pct": range_pct,
                                "min_price_15m": min_p,
                            })

                            # Inyeccion masiva del estado actual en Redis
                            self.r.set("market_status", json.dumps(list(market_data.values())))

                        except (websockets.exceptions.ConnectionClosed, asyncio.exceptions.CancelledError):
                            break
                        except Exception as e:
                            print(f"[ERROR] SCANNER: Stream data processing failed: {e}")
                            await asyncio.sleep(1)

            except Exception as e:
                print(f"[ERROR] SCANNER: WebSocket server disconnected. Retrying in 5s... ({e})")
                await asyncio.sleep(5)

if __name__ == "__main__":
    scanner = BinanceScanner()
    try:
        asyncio.run(scanner.start_scanning())
    except KeyboardInterrupt:
        print("\n[INFO] SCANNER: Scanner stopped by user.")