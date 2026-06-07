import asyncio
import json
import redis
import urllib.request
import websockets
from collections import deque

class BinanceScanner:
    def __init__(self):
        self.r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        self.symbols = [
            'btcusdt', 'ethusdt', 'solusdt', 'bnbusdt', 'xrpusdt',
            'adausdt', 'avaxusdt', 'linkusdt'
        ]
        self.market_data = {s.upper(): {"high_24h": 0.0, "low_24h": 0.0, "change_24h_pct": 0.0} for s in self.symbols}
        # 1 hour window = 3600 samples (1 per second)
        self.price_history = {s.upper(): deque(maxlen=3600) for s in self.symbols}
        self.last_tick_time = {s.upper(): 0 for s in self.symbols}
        self.prev_mid = {s.upper(): 0.0 for s in self.symbols}
        self.last_ticker_v = {s.upper(): 0.0 for s in self.symbols}
        self.volume_history = {s.upper(): deque(maxlen=300) for s in self.symbols}

    async def _ticker_poller(self):
        url = "https://api.binance.com/api/v3/ticker/24hr"
        while True:
            try:
                loop = asyncio.get_event_loop()
                resp = await loop.run_in_executor(None, lambda: urllib.request.urlopen(url, timeout=10))
                data = json.loads(resp.read().decode())
                for item in data:
                    sym = item['symbol']
                    if sym in self.market_data:
                        self.market_data[sym]['high_24h'] = float(item['highPrice'])
                        self.market_data[sym]['low_24h'] = float(item['lowPrice'])
                        self.market_data[sym]['change_24h_pct'] = float(item['priceChangePercent'])
                self.r.set("market_status", json.dumps(list(self.market_data.values())))
            except Exception as e:
                print(f"[ERROR] TICKER: Poll failed. Retrying in 60s... ({e})")
            await asyncio.sleep(60)

    async def _book_scanner(self):
        streams = "/".join([f"{s}@bookTicker" for s in self.symbols])
        url = f"wss://stream.binance.com:9443/ws/{streams}"
        print("[INFO] SCANNER: Connecting to Binance bookTicker WebSocket...")

        while True:
            try:
                async with websockets.connect(url) as websocket:
                    print("[INFO] SCANNER: WebSocket connection established successfully.")

                    while True:
                        try:
                            raw_data = await websocket.recv()
                            data = json.loads(raw_data)

                            symbol = data['s'].upper()
                            bid_p, bid_q = float(data['b']), float(data['B'])
                            ask_p, ask_q = float(data['a']), float(data['A'])

                            imbalance = bid_q / ask_q if ask_q > 0 else 1.0
                            current_mid = (bid_p + ask_p) / 2

                            now = asyncio.get_event_loop().time()

                            if now - self.last_tick_time[symbol] >= 1.0:
                                self.price_history[symbol].append(current_mid)
                                self.last_tick_time[symbol] = now

                            history = self.price_history[symbol]
                            history_len = len(history)

                            trend_1m = "NEUTRAL"
                            trend_5m = "NEUTRAL"
                            range_pct = 50.0
                            min_p = current_mid
                            max_p = current_mid

                            if history_len >= 60:
                                if current_mid > history[-60]: trend_1m = "UP"
                                elif current_mid < history[-60]: trend_1m = "DOWN"

                            if history_len >= 300:
                                if current_mid > history[-300]: trend_5m = "UP"
                                elif current_mid < history[-300]: trend_5m = "DOWN"

                            if history_len > 1:
                                max_p = max(history)
                                min_p = min(history)
                                if max_p > min_p:
                                    range_pct = ((current_mid - min_p) / (max_p - min_p)) * 100

                            bid_rising = current_mid > self.prev_mid[symbol] if self.prev_mid[symbol] > 0 else False
                            self.prev_mid[symbol] = current_mid

                            if trend_1m == "UP" and trend_5m == "UP":
                                price_direction = "UP"
                            elif trend_1m == "DOWN" and trend_5m == "DOWN":
                                price_direction = "DOWN"
                            else:
                                price_direction = "NEUTRAL"

                            self.market_data[symbol].update({
                                "symbol": symbol,
                                "bid": bid_p,
                                "ask": ask_p,
                                "imbalance": imbalance,
                                "price_direction": price_direction,
                                "trend_1m": trend_1m,
                                "trend_5m": trend_5m,
                                "range_pct": range_pct,
                                "low_1h": min_p,
                                "high_1h": max_p,
                                "bid_rising": bid_rising,
                            })

                            self.r.set("market_status", json.dumps(list(self.market_data.values())))

                        except (websockets.exceptions.ConnectionClosed, asyncio.exceptions.CancelledError):
                            break
                        except Exception as e:
                            print(f"[ERROR] SCANNER: Stream data processing failed: {e}")
                            await asyncio.sleep(1)

            except Exception as e:
                print(f"[ERROR] SCANNER: WebSocket server disconnected. Retrying in 5s... ({e})")
                await asyncio.sleep(5)

    async def _ticker_stream(self):
        url = "wss://stream.binance.com:9443/ws/!ticker@arr"
        print("[INFO] SCANNER: Connecting to Binance !ticker@arr WebSocket for volume...")
        while True:
            try:
                async with websockets.connect(url) as websocket:
                    print("[INFO] SCANNER: !ticker@arr connected successfully.")
                    while True:
                        raw = await websocket.recv()
                        data = json.loads(raw)
                        for item in data:
                            sym = item['s'].upper()
                            if sym not in self.market_data:
                                continue
                            cur_v = float(item['v'])
                            if self.last_ticker_v[sym] > 0:
                                delta = cur_v - self.last_ticker_v[sym]
                                if delta > 0:
                                    self.volume_history[sym].append(delta)
                            self.last_ticker_v[sym] = cur_v

                            hist = self.volume_history[sym]
                            if len(hist) >= 10:
                                avg = sum(hist) / len(hist)
                                spike = hist[-1] / avg if avg > 0 else 1.0
                            else:
                                avg = 0.0
                                spike = 1.0

                            self.market_data[sym].update({
                                "volume_spike": round(spike, 2),
                                "avg_volume": round(avg, 4),
                            })
            except Exception as e:
                print(f"[ERROR] TICKER WS: Disconnected. Retrying in 5s... ({e})")
                await asyncio.sleep(5)

    async def start_scanning(self):
        await asyncio.gather(self._book_scanner(), self._ticker_poller(), self._ticker_stream())

if __name__ == "__main__":
    scanner = BinanceScanner()
    try:
        asyncio.run(scanner.start_scanning())
    except KeyboardInterrupt:
        print("\n[INFO] SCANNER: Scanner stopped by user.")
