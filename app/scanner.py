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
            'dogeusdt', 'adausdt', 'trxusdt', 'shibusdt', 'dotusdt'
        ]
        self.base_url = "wss://stream.binance.com:9443/ws"
        self.streams = "/".join([f"{s}@bookTicker" for s in self.symbols])

        # Diccionario de colas para almacenar el precio medio segundo a segundo.
        # 5 minutos = 300 segundos. Ponemos un límite máximo (maxlen) para liberar memoria automáticamente.
        self.price_history = {s.upper(): deque(maxlen=300) for s in self.symbols}
        self.last_tick_time = {s.upper(): 0 for s in self.symbols}

    async def start_scanning(self):
        url = f"{self.base_url}/{self.streams}"
        print(f"📡 Conectando a Binance (Filtro Avanzado de Rango 5m)...")

        while True:
            try:
                async with websockets.connect(url) as websocket:
                    market_data = {s.upper(): {} for s in self.symbols}
                    print("✅ Conexión establecida. Calculando matrices de tendencia...")

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

                            # Registramos el precio histórico exactamente una vez por segundo
                            if now - self.last_tick_time[symbol] >= 1.0:
                                self.price_history[symbol].append(current_mid)
                                self.last_tick_time[symbol] = now

                            # --- CÁLCULO DE TENDENCIAS MULTI-TEMPORALES ---
                            history = self.price_history[symbol]
                            history_len = len(history)

                            trend_30s = "NEUTRAL"
                            trend_1m = "NEUTRAL"
                            range_pct = 50.0  # Si no hay historial suficiente, asumimos el medio por seguridad

                            if history_len >= 30:
                                # Compara precio actual contra el de hace 30 segundos
                                idx_30s = -30 if history_len >= 30 else -history_len
                                if current_mid > history[idx_30s]: trend_30s = "UP"
                                elif current_mid < history[idx_30s]: trend_30s = "DOWN"

                            if history_len >= 60:
                                # Compara precio actual contra el de hace 60 segundos (1 minuto)
                                idx_1m = -60 if history_len >= 60 else -history_len
                                if current_mid > history[idx_1m]: trend_1m = "UP"
                                elif current_mid < history[idx_1m]: trend_1m = "DOWN"

                            if history_len > 1:
                                # Rango de 5 minutos (busca el pico más alto y más bajo en la cola)
                                max_p = max(history)
                                min_p = min(history)
                                if max_p > min_p:
                                    # Posicionamiento matemático porcentual (Fórmula Min-Max)
                                    range_pct = ((current_mid - min_p) / (max_p - min_p)) * 100

                            # Unificamos criterios para el Frontend
                            # Para pintar la flecha principal, exigimos alineación de 30s y 1m
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
                                "range_pct": range_pct  # Enviamos el mapa de oscilación a la central
                            }

                            self.r.set("market_status", json.dumps(list(market_data.values())))

                        except (websockets.exceptions.ConnectionClosed, asyncio.exceptions.CancelledError):
                            break
                        except Exception as e:
                            print(f"❌ Error en datos: {e}")
                            await asyncio.sleep(1)

            except Exception as e:
                print(f"❌ Error de conexión: {e}. Reintentando en 5s...")
                await asyncio.sleep(5)

if __name__ == "__main__":
    scanner = BinanceScanner()
    try:
        asyncio.run(scanner.start_scanning())
    except KeyboardInterrupt:
        print("\n🛑 Scanner detenido.")