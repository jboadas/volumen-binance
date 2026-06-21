import asyncio
import json
import redis
import websockets
import logging, os, sys
from collections import deque

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

LOGFILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "bot.log")

log = logging.getLogger("bot")
log.setLevel(logging.INFO)
log.handlers.clear()
log.propagate = False
fmt = logging.Formatter("%(asctime)s | %(levelname)-5s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
fh = logging.FileHandler(LOGFILE, mode="a")
fh.setFormatter(fmt)
log.addHandler(fh)

EXCHANGES_CFG = {}
cfg_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "exchanges.json")
try:
    with open(cfg_path) as f:
        EXCHANGES_CFG = json.load(f)
except FileNotFoundError:
    log.error(f"[INIT] exchanges.json not found at {cfg_path}")
    sys.exit(1)


def _murphy_trend(history, window, n_segments=3):
    if len(history) < window or window < n_segments * 2:
        return "NEUTRAL"
    seg_size = window // n_segments
    recent = history[-window:]
    seg_highs = []
    seg_lows = []
    for i in range(n_segments):
        start = i * seg_size
        seg = recent[start:start + seg_size]
        seg_highs.append(max(seg))
        seg_lows.append(min(seg))
    highs_up = all(seg_highs[i] < seg_highs[i + 1] for i in range(n_segments - 1))
    lows_up = all(seg_lows[i] < seg_lows[i + 1] for i in range(n_segments - 1))
    highs_down = all(seg_highs[i] > seg_highs[i + 1] for i in range(n_segments - 1))
    lows_down = all(seg_lows[i] > seg_lows[i + 1] for i in range(n_segments - 1))
    if highs_up and lows_up:
        return "UP"
    if highs_down and lows_down:
        return "DOWN"
    return "NEUTRAL"


class MarketScanner:
    def __init__(self, exchange_id, symbols):
        self.exchange_id = exchange_id
        self.cfg = EXCHANGES_CFG.get(exchange_id, {})
        ws_cfg = self.cfg.get("ws", {})
        self.symbols = symbols
        self.r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        self.market_data = {s.upper(): {} for s in symbols}
        self.price_history = {s.upper(): deque(maxlen=3600) for s in symbols}
        self.last_tick_time = {s.upper(): 0 for s in symbols}
        self.prev_mid = {s.upper(): 0.0 for s in symbols}

        suffix = ws_cfg.get("book_ticker_suffix", "@bookTicker")
        base = ws_cfg.get("base_url", "wss://stream.binance.com:9443/ws")
        case = ws_cfg.get("symbol_case", "lower")
        ss = [s.lower() if case == "lower" else s for s in symbols]
        streams = "/".join([f"{s}{suffix}" for s in ss])
        self._ws_url = f"{base}/{streams}"

    async def _book_scanner(self):
        log.info(f"[INFO] SCANNER: Connecting to {self.exchange_id} bookTicker WebSocket...")

        while True:
            try:
                async with websockets.connect(self._ws_url) as websocket:
                    log.info(f"[INFO] SCANNER: {self.exchange_id} WebSocket connection established successfully.")

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
                                trend_1m = _murphy_trend(list(history), 60, 3)

                            if history_len >= 300:
                                trend_5m = _murphy_trend(list(history), 300, 3)

                            if history_len > 1:
                                max_p = max(history)
                                min_p = min(history)
                                if max_p > min_p:
                                    range_pct = ((current_mid - min_p) / (max_p - min_p)) * 100

                            change_1h_pct = 0.0
                            if history_len >= 60 and history[0] > 0:
                                window = min(history_len, 3600)
                                change_1h_pct = ((current_mid - history[-window]) / history[-window]) * 100

                            bid_rising = False
                            if current_mid > self.prev_mid[symbol] and self.prev_mid[symbol] > 0:
                                bid_rising = True
                            if current_mid != self.prev_mid[symbol]:
                                self.prev_mid[symbol] = current_mid

                            if trend_1m == "UP" and trend_5m == "UP":
                                price_direction = "UP"
                            elif trend_1m == "DOWN" and trend_5m == "DOWN":
                                price_direction = "DOWN"
                            else:
                                price_direction = "NEUTRAL"

                            liquidity = bid_q * bid_p + ask_q * ask_p

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
                                "change_1h_pct": round(change_1h_pct, 2),
                                "liquidity": round(liquidity, 2),
                            })

                            market_list = [v for v in self.market_data.values() if v]
                            self.r.set("market_status", json.dumps(market_list))

                        except (websockets.exceptions.ConnectionClosed, asyncio.exceptions.CancelledError):
                            break
                        except Exception as e:
                            log.error(f"[ERROR] SCANNER: Stream data processing failed: {e}")
                            await asyncio.sleep(1)

            except Exception as e:
                log.error(f"[ERROR] SCANNER: {self.exchange_id} WebSocket disconnected. Retrying in 5s... ({e})")
                await asyncio.sleep(5)

    async def start_scanning(self):
        await self._book_scanner()


if __name__ == "__main__":
    exchange_id = "binance"
    args = sys.argv[1:]
    if args and args[0].startswith("--exchange="):
        exchange_id = args[0].split("=", 1)[1]
        args = args[1:]
    scanner = MarketScanner(exchange_id, args)
    try:
        asyncio.run(scanner.start_scanning())
    except KeyboardInterrupt:
        log.info(f"\n[INFO] SCANNER: Scanner stopped by user.")
