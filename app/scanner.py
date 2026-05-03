import asyncio
import json
import redis
import websockets

class BinanceScanner:
    def __init__(self):
        # decode_responses=True para leer strings directamente de Redis
        self.r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        self.symbols = [
            'btcusdt', 'ethusdt', 'solusdt', 'bnbusdt', 'xrpusdt',
            'dogeusdt', 'adausdt', 'trxusdt', 'shibusdt', 'dotusdt'
        ]
        self.base_url = "wss://stream.binance.com:9443/ws"
        self.streams = "/".join([f"{s}@bookTicker" for s in self.symbols])

    async def start_scanning(self):
        url = f"{self.base_url}/{self.streams}"
        print(f"📡 Conectando a Binance Streams...")

        while True: # Bucle de reconexión automática
            try:
                async with websockets.connect(url) as websocket:
                    market_data = {s.upper(): {} for s in self.symbols}
                    print("✅ Conexión establecida. Escaneando ratios...")

                    while True:
                        try:
                            raw_data = await websocket.recv()
                            data = json.loads(raw_data)

                            symbol = data['s']
                            bid_p, bid_q = float(data['b']), float(data['B'])
                            ask_p, ask_q = float(data['a']), float(data['A'])

                            # Cálculo de Imbalance (Ratio de presión)
                            imbalance = bid_q / ask_q if ask_q > 0 else 1.0

                            trend = "NEUTRAL"
                            if imbalance > 2.5: trend = "BULL"
                            elif imbalance < 0.4: trend = "BEAR"

                            market_data[symbol] = {
                                "symbol": symbol,
                                "bid": bid_p,
                                "ask": ask_p,
                                "imbalance": imbalance,
                                "trend": trend
                            }

                            # Guardamos el estado del mercado como STRING JSON
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