import json
import redis
import asyncio
import time
import logging, os
import urllib.request
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse

LOGFILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "bot.log")
TRADESFILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "trades.log")

log = logging.getLogger("bot")
log.setLevel(logging.INFO)
log.handlers.clear()
log.propagate = False
fmt = logging.Formatter("%(asctime)s | %(levelname)-5s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
fh = logging.FileHandler(LOGFILE, mode="w")
fh.setFormatter(fmt)
log.addHandler(fh)

trades_log = logging.getLogger("trades")
trades_log.setLevel(logging.INFO)
trades_log.handlers.clear()
trades_log.propagate = False
tfh = logging.FileHandler(TRADESFILE, mode="w")
tfh.setFormatter(fmt)
trades_log.addHandler(tfh)

r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
lock = asyncio.Lock()
scanner_process = None
_scanner_stop_event = asyncio.Event()

TOPFILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "top_pairs.json")

CONFIG = {"imbalance": 20, "tp_pct": 1.5, "trail_pct": 0.6, "sl_pct": 2.0}

def load_traded_symbols():
    try:
        with open(TOPFILE) as f:
            data = json.load(f)
        symbols = [p['symbol'] for p in data]
        if symbols:
            return symbols[:16]
    except FileNotFoundError:
        log.info("[INIT] top_pairs.json not found.")
    except (json.JSONDecodeError, KeyError, IndexError) as e:
        log.error(f"[INIT] Failed to parse top_pairs.json: {e}")
    return []

async def run_screener():
    url = "https://api.binance.com/api/v3/ticker/24hr"
    try:
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(None, lambda: urllib.request.urlopen(url).read())
        tickers = json.loads(response.decode('utf-8'))
        usdt_pairs = [t for t in tickers if t['symbol'].endswith('USDT')]
        scored = []
        for t in usdt_pairs:
            try:
                volume = float(t['quoteVolume'])
                high = float(t['highPrice'])
                low = float(t['lowPrice'])
                amplitude = ((high - low) / low) * 100 if low > 0 else 0
                scored.append({"symbol": t['symbol'], "volume": round(volume, 2), "amplitude": round(amplitude, 2), "score": round(volume * amplitude, 2)})
            except (ValueError, KeyError):
                continue
        scored.sort(key=lambda x: x['score'], reverse=True)
        top10 = scored[:10]
        with open(TOPFILE, "w") as f:
            json.dump(top10, f, indent=2)
        log.info("[SCREEN] Top 10 pairs written to top_pairs.json")
        for p in top10:
            log.info(f"[SCREEN] {p['symbol']}: vol=${p['volume']/1_000_000:.1f}M, amp={p['amplitude']}%, score={p['score']:.0f}")
    except Exception as e:
        log.error(f"[SCREEN] Error: {e}")

def get_config(symbol):
    return dict(CONFIG)

TRADED_SYMBOLS = load_traded_symbols()

async def _scanner_watchdog():
    global scanner_process
    while not _scanner_stop_event.is_set():
        if scanner_process is None or scanner_process.returncode is not None:
            await asyncio.sleep(0.5)
            if _scanner_stop_event.is_set():
                break
            log.info("[WATCHDOG] Scanner down. Restarting in 2s...")
            await asyncio.sleep(2)
            if _scanner_stop_event.is_set():
                break
            try:
                scanner_process = await asyncio.create_subprocess_exec(
                    "python3", "app/scanner.py", *TRADED_SYMBOLS
                )
                log.info("[WATCHDOG] Scanner restarted.")
            except Exception as e:
                log.error(f"[WATCHDOG] Failed to restart scanner: {e}")
        try:
            await asyncio.wait_for(scanner_process.wait(), timeout=5)
        except asyncio.TimeoutError:
            continue

async def _restart_scanner(new_symbols):
    global scanner_process, TRADED_SYMBOLS
    log.info("[SCREEN] New symbols detected, rotating scanner...")
    r.set("trading_locked", "1")
    log.info("[SCREEN] Trading locked.")

    while r.hlen("open_positions") > 0:
        log.info(f"[SCREEN] Waiting for {r.hlen('open_positions')} position(s) to close...")
        await asyncio.sleep(5)

    if scanner_process and scanner_process.returncode is None:
        scanner_process.terminate()
        try:
            await asyncio.wait_for(scanner_process.wait(), timeout=5)
        except asyncio.TimeoutError:
            scanner_process.kill()
            await scanner_process.wait()

    TRADED_SYMBOLS = new_symbols
    scanner_process = await asyncio.create_subprocess_exec(
        "python3", "app/scanner.py", *TRADED_SYMBOLS
    )
    log.info(f"[SCREEN] Scanner restarted with {len(TRADED_SYMBOLS)} symbols: {', '.join(TRADED_SYMBOLS)}")
    r.delete("trading_locked")
    log.info("[SCREEN] Trading unlocked.")

async def _periodic_screener():
    while True:
        await asyncio.sleep(21600)  # 6 hours
        await run_screener()
        new_symbols = load_traded_symbols()
        if new_symbols and new_symbols != TRADED_SYMBOLS:
            await _restart_scanner(new_symbols)

# --- MANEJO DE LIFESPAN (Arranque y Apagado Moderno de la Aplicacion) ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    global scanner_process

    for lgr in (log, trades_log):
        lgr.handlers.clear()
        lgr.setLevel(logging.INFO)
        lgr.propagate = False
        fh2 = logging.FileHandler(LOGFILE if lgr is log else TRADESFILE, mode="a")
        fh2.setFormatter(fmt)
        lgr.addHandler(fh2)
    log.info("[INIT] Lifespan started, logger reconfigured.")

    global TRADED_SYMBOLS
    if not TRADED_SYMBOLS:
        log.info("[INIT] No symbols found, running market screener...")
        await run_screener()
        TRADED_SYMBOLS = load_traded_symbols()

    log.info(f"[INIT] Trading {len(TRADED_SYMBOLS)} symbols: {', '.join(TRADED_SYMBOLS)}")
    _scanner_stop_event.clear()
    scanner_process = await asyncio.create_subprocess_exec(
        "python3", "app/scanner.py", *TRADED_SYMBOLS
    )
    asyncio.create_task(_scanner_watchdog())

    keys_map = {"market_status": "string", "open_positions": "hash"}
    for k, t in keys_map.items():
        if r.type(k) != "none" and r.type(k) != t:
            r.delete(k)

    if not r.exists("wallet"):
        r.set("wallet", json.dumps({"balance": 100.0, "pnl": 0.0}))
        log.info("[INIT] Redis empty: Created new simulation wallet with $100.00.")
    else:
        wallet_actual = load_wallet()
        log.info(f"[INIT] WALLET FOUND: Keeping progress. Available balance: ${wallet_actual['balance']:.2f}")

    asyncio.create_task(monitoring_loop())
    asyncio.create_task(_periodic_screener())

    yield

    _scanner_stop_event.set()
    if scanner_process and scanner_process.returncode is None:
        scanner_process.terminate()
        try:
            await asyncio.wait_for(scanner_process.wait(), timeout=5)
        except asyncio.TimeoutError:
            scanner_process.kill()
            await scanner_process.wait()

# Inicializamos FastAPI inyectandole el ciclo de vida moderno libre de warnings
app = FastAPI(lifespan=lifespan)

app.mount("/static", StaticFiles(directory="static"), name="static")

def load_wallet():
    wallet_raw = r.get("wallet")
    if wallet_raw:
        wallet = json.loads(wallet_raw)
        wallet['balance'] = float(wallet.get('balance', 0.0))
        wallet['pnl'] = float(wallet.get('pnl', 0.0))
        return wallet
    return {"balance": 0.0, "pnl": 0.0}

def compute_effective_buy_amount(invest, market_price):
    market_price = float(market_price)
    price_effective = market_price * 1.0005
    amount = (float(invest) / price_effective) * 0.999
    return amount, price_effective

def compute_effective_sell_return(amount, market_price):
    market_price = float(market_price)
    amount = float(amount)
    price_effective = market_price * 0.9995
    retorno_bruto = amount * price_effective
    return retorno_bruto * 0.999, price_effective

def compute_unrealized_net_pct(position, market_price):
    cost = float(position.get('cost', 0.0))
    if cost <= 0:
        return 0.0
    val_retorno, _ = compute_effective_sell_return(position['amount'], market_price)
    return ((val_retorno - cost) / cost) * 100

def should_buy(data, cfg):
    imbalance = float(data['imbalance'])
    if imbalance < cfg['imbalance']:
        return False, f"imbalance {imbalance:.1f}x < {cfg['imbalance']}x"

    trend_1m = data.get('trend_1m', 'NEUTRAL')
    trend_5m = data.get('trend_5m', 'NEUTRAL')
    if trend_1m != 'UP' or trend_5m != 'UP':
        return False, f"trend 1m={trend_1m} 5m={trend_5m} (need UP/UP)"

    range_pct = float(data.get('range_pct', 100))
    if range_pct > 50:
        return False, f"range {range_pct:.0f}% > 50%"

    change_1h = float(data.get('change_1h_pct', 0))
    if change_1h < -8.0:
        return False, f"1h change {change_1h:.1f}% < -8%"

    return True, f"imbalance {imbalance:.1f}x, range {range_pct:.0f}%, 1h {change_1h:.1f}%, trend UP/UP"

async def monitoring_loop():
    while True:
        await asyncio.sleep(5)

        positions = r.hgetall("open_positions")
        market_raw = r.get("market_status")
        if not market_raw:
            continue

        try:
            market_data = json.loads(market_raw)
            # Filtro defensivo robusto contra nulos o arranques parciales
            market = {item['symbol']: item for item in market_data if isinstance(item, dict) and 'symbol' in item}
        except Exception as e:
            log.error(f"[ERROR] Error processing market_status from Redis: {e}")
            continue

        if not market:
            continue

        async with lock:
            # --- 1. PROCESAR VENTAS CON TRAILING TP ---
            for symbol, pos_raw in positions.items():
                pos = json.loads(pos_raw)
                if symbol not in market:
                    continue
                m = market[symbol]
                cur_price = (float(m['bid']) + float(m['ask'])) / 2
                cfg = get_config(symbol)

                stop_loss_price = float(pos.get("stop_loss_price", float(pos.get("buy_price", 0)) * 0.995))
                high_water_mark = float(pos.get("high_water_mark", float(pos.get("buy_price", 0))))
                trailing_active = pos.get("trailing_active", False)

                # Update high water mark and trailing stop loss
                if cur_price > high_water_mark:
                    high_water_mark = cur_price
                    new_sl = high_water_mark * (1 - cfg['sl_pct'] / 100)
                    if new_sl > stop_loss_price:
                        stop_loss_price = new_sl

                # Activate trailing on first touch of min TP
                if not trailing_active:
                    pnl = compute_unrealized_net_pct(pos, cur_price)
                    if pnl >= cfg['tp_pct']:
                        trailing_active = True

                # Persist tracking state
                pos['high_water_mark'] = high_water_mark
                pos['trailing_active'] = trailing_active
                pos['stop_loss_price'] = stop_loss_price
                r.hset("open_positions", symbol, json.dumps(pos))

                # Sell checks
                pnl = compute_unrealized_net_pct(pos, cur_price)
                should_sell = False
                reason = ""

                if cur_price <= stop_loss_price:
                    reason = "SL"
                    should_sell = True
                elif trailing_active and cur_price <= high_water_mark * (1 - cfg['trail_pct'] / 100):
                    reason = "TRAIL"
                    should_sell = True
                elif trailing_active and pnl < cfg['tp_pct']:
                    reason = "TP"
                    should_sell = True

                if should_sell:
                    wallet = load_wallet()
                    amount = float(pos['amount'])
                    cost = float(pos.get('cost', 0.0))
                    val_retorno, sell_price_effective = compute_effective_sell_return(amount, cur_price)
                    pct_ganancia = ((val_retorno - cost) / cost) * 100 if cost > 0 else 0.0

                    wallet['balance'] = float(wallet['balance']) + float(val_retorno)
                    wallet['pnl'] = float(wallet['pnl']) + float(pct_ganancia)

                    r.set("wallet", json.dumps(wallet))
                    r.hdel("open_positions", symbol)

                    trade = {
                        "symbol": symbol,
                        "entry": float(pos.get('buy_price', 0)),
                        "buy_ts": float(pos.get('buy_ts', 0)),
                        "exit": cur_price,
                        "qty": amount,
                        "cost": cost,
                        "return": round(val_retorno, 4),
                        "pnl_pct": round(pct_ganancia, 2),
                        "reason": reason,
                        "ts": time.time()
                    }
                    r.lpush("trade_history", json.dumps(trade))
                    r.ltrim("trade_history", 0, 199)

                    if reason == "SL":
                        r.setex(f"cooldown:{symbol}", 300, "blocked")
                        log.warning(f"[ALERT] SL on {symbol} at {sell_price_effective:.4f} (PnL: {pct_ganancia:.2f}%)")
                        trades_log.warning(f"[ALERT] SL on {symbol} at {sell_price_effective:.4f} (PnL: {pct_ganancia:.2f}%)")
                    else:
                        r.setex(f"cooldown:{symbol}", 60, "blocked")
                        log.info(f"[LOG] {reason} on {symbol} at {sell_price_effective:.4f} (PnL: {pct_ganancia:.2f}%) - Balance: {wallet['balance']:.2f}")
                        trades_log.info(f"[LOG] {reason} on {symbol} at {sell_price_effective:.4f} (PnL: {pct_ganancia:.2f}%) - Balance: {wallet['balance']:.2f}")

            # --- 2. PROCESAR COMPRAS (imbalance + rango + macro) ---
            if r.get("trading_locked"):
                continue

            for item in market.values():
                symbol = item['symbol']
                cfg = get_config(symbol)

                wallet = load_wallet()
                in_cooldown = r.exists(f"cooldown:{symbol}")
                has_position = symbol in r.hkeys("open_positions")

                if in_cooldown:
                    log.info(f"[SKIP] {symbol} cooldown active")
                    continue
                if has_position:
                    log.info(f"[SKIP] {symbol} already in position")
                    continue
                if wallet['balance'] < 10.0:
                    log.info(f"[SKIP] {symbol} balance ${wallet['balance']:.2f} < $10")
                    continue

                ok, reason = should_buy(item, cfg)

                if ok:
                    price = (float(item['bid']) + float(item['ask'])) / 2
                    invest = 10.0
                    qty, buy_price_effective = compute_effective_buy_amount(invest, price)
                    low_1h = float(item.get('low_1h', 0.0))

                    sl_price = low_1h if low_1h > 0 else price * (1 - cfg['sl_pct'] / 100)
                    sl_price = min(sl_price, price * (1 - cfg['sl_pct'] / 100))

                    wallet['balance'] = float(wallet['balance']) - float(invest)
                    r.set("wallet", json.dumps(wallet))

                    r.hset("open_positions", symbol, json.dumps({
                        "buy_price": price,
                        "buy_ts": time.time(),
                        "entry_price_effective": buy_price_effective,
                        "amount": qty,
                        "cost": invest,
                        "stop_loss_price": sl_price,
                        "high_water_mark": price,
                        "trailing_active": False,
                    }))

                    log.info(f"[BUY] {symbol} at {buy_price_effective:.4f} ({reason})")
                    trades_log.info(f"[BUY] {symbol} at {buy_price_effective:.4f} ({reason})")
                else:
                    log.info(f"[SKIP] {symbol} {reason}")

@app.get("/", response_class=HTMLResponse)
async def get_index():
    with open("static/index.html", "r") as f:
        return f.read()

@app.get("/api/status")
async def get_status():
    m = r.get("market_status")
    wallet = load_wallet()
    return {
        "market": json.loads(m) if m else [],
        "wallet": wallet,
        "trading_locked": bool(r.get("trading_locked"))
    }

@app.get("/api/positions")
async def get_open_positions():
    raw_positions = r.hgetall("open_positions")
    market_raw = r.get("market_status")
    market = {}
    if market_raw:
        market_data = json.loads(market_raw)
        if isinstance(market_data, list):
            for item in market_data:
                if isinstance(item, dict) and 'symbol' in item:
                    market[item['symbol']] = item

    positions = {}
    for symbol, pos_raw in raw_positions.items():
        pos = json.loads(pos_raw)
        if symbol in market:
            price = (float(market[symbol]['bid']) + float(market[symbol]['ask'])) / 2
            pos['unrealized_pnl'] = compute_unrealized_net_pct(pos, price)
            pos['current_price'] = price
        else:
            pos['unrealized_pnl'] = None
            pos['current_price'] = None
        positions[symbol] = pos

    return positions

@app.post("/api/simulate/buy")
async def simulate_buy(request: Request):
    params = await request.json()
    symbol = params['symbol']
    market_price = float(params['price'])

    async with lock:
        wallet = load_wallet()
        if wallet['balance'] < 10.0:
            return {"status": "error", "msg": "Insufficient balance"}

        if r.hexists("open_positions", symbol):
            return {"status": "error", "msg": "Already have an open position"}

        qty, buy_price_effective = compute_effective_buy_amount(10.0, market_price)
        wallet['balance'] = float(wallet['balance']) - 10.0
        r.set("wallet", json.dumps(wallet))

        market_raw = r.get("market_status")
        low_1h = 0.0
        if market_raw:
            for item in json.loads(market_raw):
                if item.get('symbol') == symbol:
                    low_1h = float(item.get('low_1h', 0.0))
                    break

        cfg = get_config
        sl_price = low_1h if low_1h > 0 else market_price * (1 - cfg['sl_pct'] / 100)
        sl_price = min(sl_price, market_price * (1 - cfg['sl_pct'] / 100))

        r.hset("open_positions", symbol, json.dumps({
            "buy_price": market_price,
            "buy_ts": time.time(),
            "entry_price_effective": buy_price_effective,
            "amount": qty,
            "cost": 10.0,
            "stop_loss_price": sl_price,
            "high_water_mark": market_price,
            "trailing_active": False,
        }))
        return {"status": "ok"}

@app.post("/api/simulate/sell")
async def simulate_sell(request: Request):
    params = await request.json()
    symbol = params['symbol']
    sell_price = float(params['price'])

    async with lock:
        raw_pos = r.hget("open_positions", symbol)
        if not raw_pos:
            return {"status": "error", "message": "No position"}

        pos = json.loads(raw_pos)
        amount = float(pos['amount'])
        cost = float(pos.get('cost', 0.0))

        val_retorno, sell_price_effective = compute_effective_sell_return(amount, sell_price)
        pct_ganancia = ((val_retorno - cost) / cost) * 100 if cost > 0 else 0.0

        wallet = load_wallet()
        wallet['balance'] = float(wallet['balance']) + float(val_retorno)
        wallet['pnl'] = float(wallet['pnl']) + float(pct_ganancia)
        r.set("wallet", json.dumps(wallet))

        r.hdel("open_positions", symbol)

        r.lpush("trade_history", json.dumps({
            "symbol": symbol,
            "entry": float(pos.get('buy_price', 0)),
            "exit": sell_price,
            "qty": amount,
            "cost": cost,
            "return": round(val_retorno, 4),
            "pnl_pct": round(pct_ganancia, 2),
            "reason": "MANUAL",
            "ts": time.time()
        }))
        r.ltrim("trade_history", 0, 199)
        return {"status": "ok"}

@app.get("/api/trading/lock")
async def get_lock():
    return {"locked": bool(r.get("trading_locked"))}

@app.post("/api/trading/lock")
async def toggle_lock():
    v = r.get("trading_locked")
    if v:
        r.delete("trading_locked")
        log.info("[LOCK] Trading unlocked — new buys allowed")
    else:
        r.set("trading_locked", "1")
        log.warning("[LOCK] Trading locked — new buys blocked")
    return {"locked": bool(r.get("trading_locked"))}

@app.post("/api/wallet/reset")
async def reset_wallet():
    async with lock:
        r.set("wallet", json.dumps({"balance": 100.0, "pnl": 0.0}))
    return {"status": "ok"}

def get_trade_markers(symbol):
    markers = []
    raw_trades = r.lrange("trade_history", 0, -1)
    for t_raw in raw_trades:
        t = json.loads(t_raw)
        if t['symbol'] != symbol:
            continue
        buy_ts = t.get('buy_ts', 0)
        if buy_ts:
            markers.append({"time": int(buy_ts), "type": "buy", "price": float(t['entry']), "pnl": t['pnl_pct']})
        markers.append({"time": int(t['ts']), "type": "sell", "price": float(t['exit']), "pnl": t['pnl_pct'], "reason": t['reason']})
    raw_pos = r.hget("open_positions", symbol)
    if raw_pos:
        pos = json.loads(raw_pos)
        if pos.get('buy_ts'):
            markers.append({"time": int(pos['buy_ts']), "type": "buy", "price": float(pos.get('buy_price', 0)), "pnl": None})
    markers.sort(key=lambda m: m['time'])
    return markers

@app.get("/api/klines/{symbol}")
async def get_klines(symbol: str):
    symbol = symbol.upper()
    interval = "5m"
    now = time.time()
    cached = r.hget("klines_data", f"{symbol}:{interval}")
    if cached:
        entry = json.loads(cached)
        if now - entry.get("ts", 0) < 300:
            return {"symbol": symbol, "klines": entry["klines"], "levels": calc_sr_levels(entry["klines"]), "trendlines": calc_trend_lines(entry["klines"]), "markers": get_trade_markers(symbol)}

    url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit=96"
    try:
        loop = asyncio.get_event_loop()
        raw = await loop.run_in_executor(None, lambda: urllib.request.urlopen(url, timeout=10).read())
        data = json.loads(raw)
        klines = []
        for k in data:
            klines.append({
                "t": k[0],
                "o": float(k[1]),
                "h": float(k[2]),
                "l": float(k[3]),
                "c": float(k[4]),
                "v": float(k[5]),
            })
        r.hset("klines_data", f"{symbol}:{interval}", json.dumps({"ts": now, "klines": klines}))
        levels = calc_sr_levels(klines)
        trendlines = calc_trend_lines(klines)
        markers = get_trade_markers(symbol)
        return {"symbol": symbol, "klines": klines, "levels": levels, "trendlines": trendlines, "markers": markers}
    except Exception as e:
        return {"symbol": symbol, "error": str(e), "klines": [], "levels": [], "trendlines": [], "markers": []}

def calc_sr_levels(klines):
    if len(klines) < 10:
        return []
    pivots = []
    for i in range(2, len(klines) - 2):
        if all(klines[i]["h"] > klines[i + j]["h"] for j in (-2, -1, 1, 2)):
            pivots.append({"price": klines[i]["h"], "type": "R"})
        if all(klines[i]["l"] < klines[i + j]["l"] for j in (-2, -1, 1, 2)):
            pivots.append({"price": klines[i]["l"], "type": "S"})

    if len(pivots) < 2:
        return []

    pivots.sort(key=lambda x: x["price"])
    tol = max(k["h"] for k in klines) * 0.002
    clusters = []
    for p in pivots:
        merged = False
        for c in clusters:
            if abs(c["price"] - p["price"]) / c["price"] < 0.002:
                c["price"] = (c["price"] * c["count"] + p["price"]) / (c["count"] + 1)
                c["count"] += 1
                if p["type"] == "R":
                    c["type"] = "R"
                merged = True
                break
        if not merged:
            clusters.append({"price": p["price"], "type": p["type"], "count": 1})

    clusters.sort(key=lambda x: x["count"], reverse=True)
    current = klines[-1]["c"]
    supports = sorted([c for c in clusters if c["price"] < current and c["count"] >= 1], key=lambda x: x["price"], reverse=True)[:3]
    resistances = sorted([c for c in clusters if c["price"] > current], key=lambda x: x["price"])[:3]
    return {"supports": [round(s["price"], 2) for s in supports], "resistances": [round(r["price"], 2) for r in resistances]}

def calc_trend_lines(klines):
    if len(klines) < 20:
        return []
    pivots = []
    for i in range(2, len(klines) - 2):
        t = klines[i]["t"] // 1000
        if all(klines[i]["h"] > klines[i + j]["h"] for j in (-2, -1, 1, 2)):
            pivots.append({"time": t, "price": klines[i]["h"], "type": "R"})
        if all(klines[i]["l"] < klines[i + j]["l"] for j in (-2, -1, 1, 2)):
            pivots.append({"time": t, "price": klines[i]["l"], "type": "S"})
    pivots.sort(key=lambda x: x["time"])
    highs = [p for p in pivots if p["type"] == "R"]
    lows = [p for p in pivots if p["type"] == "S"]

    result = []
    if len(lows) >= 2:
        best_seq = [lows[-1]]
        for i in range(len(lows) - 2, -1, -1):
            if lows[i]["price"] < best_seq[0]["price"]:
                best_seq.insert(0, lows[i])
        if len(best_seq) >= 2:
            result.append({"type": "uptrend", "start_time": best_seq[0]["time"], "start_price": best_seq[0]["price"], "end_time": best_seq[-1]["time"], "end_price": best_seq[-1]["price"]})

    if len(highs) >= 2:
        best_seq = [highs[-1]]
        for i in range(len(highs) - 2, -1, -1):
            if highs[i]["price"] > best_seq[0]["price"]:
                best_seq.insert(0, highs[i])
        if len(best_seq) >= 2:
            result.append({"type": "downtrend", "start_time": best_seq[0]["time"], "start_price": best_seq[0]["price"], "end_time": best_seq[-1]["time"], "end_price": best_seq[-1]["price"]})

    return result

@app.post("/api/screener/run")
async def manual_rescan():
    asyncio.create_task(_rescan_and_rotate())
    return {"status": "ok", "msg": "Screener started, rotation will happen when positions close"}

async def _rescan_and_rotate():
    await run_screener()
    new_symbols = load_traded_symbols()
    if new_symbols and new_symbols != TRADED_SYMBOLS:
        await _restart_scanner(new_symbols)

@app.get("/api/trades")
async def get_trades():
    raw = r.lrange("trade_history", 0, 49)
    return [json.loads(t) for t in raw]

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000, log_level="warning")