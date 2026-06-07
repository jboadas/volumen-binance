import json
import redis
import asyncio
import time
import logging, os
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse

LOGFILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "bot.log")

log = logging.getLogger("bot")
log.setLevel(logging.INFO)
log.handlers.clear()
log.propagate = False
fmt = logging.Formatter("%(asctime)s | %(levelname)-5s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
fh = logging.FileHandler(LOGFILE, mode="w")
fh.setFormatter(fmt)
log.addHandler(fh)

r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
lock = asyncio.Lock()
scanner_process = None
_scanner_stop_event = asyncio.Event()

SYMBOL_CONFIG = {
    "BTCUSDT": {"imbalance": 3, "tp_pct": 1.2, "trail_pct": 0.8, "sl_pct": 1.5},
    "ETHUSDT": {"imbalance": 3, "tp_pct": 1.2, "trail_pct": 0.8, "sl_pct": 1.5},
    "SOLUSDT": {"imbalance": 3, "tp_pct": 1.2, "trail_pct": 0.8, "sl_pct": 1.5},
    "BNBUSDT": {"imbalance": 3, "tp_pct": 1.2, "trail_pct": 0.8, "sl_pct": 1.5},
    "XRPUSDT": {"imbalance": 5, "tp_pct": 1.5, "trail_pct": 1.0, "sl_pct": 2.0},
    "ADAUSDT": {"imbalance": 5, "tp_pct": 1.5, "trail_pct": 1.0, "sl_pct": 2.0},
    "AVAXUSDT": {"imbalance": 5, "tp_pct": 1.5, "trail_pct": 1.0, "sl_pct": 2.0},
    "LINKUSDT": {"imbalance": 5, "tp_pct": 1.5, "trail_pct": 1.0, "sl_pct": 2.0},
}

DEFAULT_CONFIG = {"imbalance": 5, "tp_pct": 1.5, "trail_pct": 0.6, "sl_pct": 2.0}

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
                    "python3", "app/scanner.py"
                )
                log.info("[WATCHDOG] Scanner restarted.")
            except Exception as e:
                log.error(f"[WATCHDOG] Failed to restart scanner: {e}")
        try:
            await asyncio.wait_for(scanner_process.wait(), timeout=5)
        except asyncio.TimeoutError:
            continue

# --- MANEJO DE LIFESPAN (Arranque y Apagado Moderno de la Aplicacion) ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    global scanner_process

    log.handlers.clear()
    log.setLevel(logging.INFO)
    log.propagate = False
    fh = logging.FileHandler(LOGFILE, mode="a")
    fh.setFormatter(fmt)
    log.addHandler(fh)
    log.info("[INIT] Lifespan started, logger reconfigured.")
    _scanner_stop_event.clear()
    scanner_process = await asyncio.create_subprocess_exec(
        "python3", "app/scanner.py"
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

    range_pct = float(data.get('range_pct', 100))
    if range_pct > 90:
        return False, f"range {range_pct:.0f}% > 90%"

    change_1h = float(data.get('change_1h_pct', 0))
    if change_1h < -8.0:
        return False, f"1h change {change_1h:.1f}% < -8%"

    return True, f"imbalance {imbalance:.1f}x, range {range_pct:.0f}%, 1h {change_1h:.1f}%"

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
                cfg = SYMBOL_CONFIG.get(symbol, DEFAULT_CONFIG)

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
                    else:
                        r.setex(f"cooldown:{symbol}", 60, "blocked")
                        log.info(f"[LOG] {reason} on {symbol} at {sell_price_effective:.4f} (PnL: {pct_ganancia:.2f}%) - Balance: {wallet['balance']:.2f}")

            # --- 2. PROCESAR COMPRAS (imbalance + rango + macro) ---
            for item in market.values():
                symbol = item['symbol']
                cfg = SYMBOL_CONFIG.get(symbol, DEFAULT_CONFIG)

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
                        "entry_price_effective": buy_price_effective,
                        "amount": qty,
                        "cost": invest,
                        "stop_loss_price": sl_price,
                        "high_water_mark": price,
                        "trailing_active": False,
                    }))

                    log.info(f"[BUY] {symbol} at {buy_price_effective:.4f} ({reason})")
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
        "wallet": wallet
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

        cfg = SYMBOL_CONFIG.get(symbol, DEFAULT_CONFIG)
        sl_price = low_1h if low_1h > 0 else market_price * (1 - cfg['sl_pct'] / 100)
        sl_price = min(sl_price, market_price * (1 - cfg['sl_pct'] / 100))

        r.hset("open_positions", symbol, json.dumps({
            "buy_price": market_price,
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

@app.post("/api/wallet/reset")
async def reset_wallet():
    async with lock:
        r.set("wallet", json.dumps({"balance": 100.0, "pnl": 0.0}))
    return {"status": "ok"}

@app.get("/api/trades")
async def get_trades():
    raw = r.lrange("trade_history", 0, 49)
    return [json.loads(t) for t in raw]

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000, log_level="warning")