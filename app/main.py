import json
import redis
import asyncio
import time
import logging, os, sys
import urllib.request
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.strategy import (
    get_config, analyze_volume as _strategy_analyze_volume,
    check_candle_wick as _strategy_check_candle_wick,
    should_buy_pure, calc_atr, calc_position_sizing, calc_atr_sl,
    compute_effective_buy_amount, compute_effective_sell_return,
    compute_unrealized_net_pct, calc_sr_levels, calc_trend_lines
)
from app.backtest import run_backtest

LOGFILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "logs", "bot.log")
TRADESFILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "logs", "trades.log")

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

EXCHANGES_CFG = {}
_cfg_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "exchanges.json")
try:
    with open(_cfg_path) as f:
        EXCHANGES_CFG = json.load(f)
except FileNotFoundError:
    log.error(f"[INIT] exchanges.json not found at {_cfg_path}")
    sys.exit(1)

EXCHANGE_ID = "binance"
EXCHANGE = EXCHANGES_CFG.get(EXCHANGE_ID, {})

r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
lock = asyncio.Lock()
scanner_process = None
_scanner_stop_event = asyncio.Event()

TOPFILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "top_pairs.json")

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
    rest = EXCHANGE.get("rest", {})
    url = f"{rest.get('base_url', 'https://api.binance.com')}{rest.get('ticker_24hr', '/api/v3/ticker/24hr')}"
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
                change_pct = float(t['priceChangePercent'])
                if volume < 500_000 or low <= 0:
                    continue
                range_pct = ((high - low) / low) * 100
                # Excluir stablecoins y pares sin movimiento real
                if range_pct < 1.0:
                    continue
                # Sharpe: retorno 24h / rango (proxy de volatilidad)
                # Solo pares con retorno positivo (long bias)
                if change_pct <= 0:
                    continue
                sharpe = change_pct / range_pct if range_pct > 0 else 0
                score = volume * sharpe
                scored.append({
                    "symbol": t['symbol'],
                    "volume": round(volume, 2),
                    "range_pct": round(range_pct, 2),
                    "change_24h": round(change_pct, 2),
                    "sharpe": round(sharpe, 4),
                    "score": round(score, 2)
                })
            except (ValueError, KeyError):
                continue
        scored.sort(key=lambda x: x['score'], reverse=True)
        top10 = scored[:10]
        with open(TOPFILE, "w") as f:
            json.dump(top10, f, indent=2)
        log.info("[SCREEN] Top 10 pairs written to top_pairs.json")
        for p in top10:
            log.info(f"[SCREEN] {p['symbol']}: vol=${p['volume']/1_000_000:.1f}M, range={p['range_pct']}%, 24h={p['change_24h']}%, Sharpe={p['sharpe']:.3f}")
    except Exception as e:
        log.error(f"[SCREEN] Error: {e}")

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
                    "python3", "app/scanner.py", f"--exchange={EXCHANGE_ID}", *TRADED_SYMBOLS
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
        "python3", "app/scanner.py", f"--exchange={EXCHANGE_ID}", *TRADED_SYMBOLS
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
    log.info("[INIT] Running market screener...")
    await run_screener()
    TRADED_SYMBOLS = load_traded_symbols()
    if not TRADED_SYMBOLS:
        log.warning("[INIT] Screener returned no symbols, cannot start.")
        yield
        return

    log.info(f"[INIT] Trading {len(TRADED_SYMBOLS)} symbols: {', '.join(TRADED_SYMBOLS)}")
    _sync_dynamic_symbols()
    _scanner_stop_event.clear()
    scanner_process = await asyncio.create_subprocess_exec(
        "python3", "app/scanner.py", f"--exchange={EXCHANGE_ID}", *TRADED_SYMBOLS
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



_klines_cache = {}

async def _fetch_klines_1m(symbol, limit=60):
    rest = EXCHANGE.get("rest", {})
    base = rest.get('base_url', 'https://api.binance.com')
    path = rest.get('klines', '/api/v3/klines')
    try:
        loop = asyncio.get_event_loop()
        raw = await loop.run_in_executor(None, lambda: urllib.request.urlopen(
            f"{base}{path}?symbol={symbol}&interval=1m&limit={limit}", timeout=5).read())
        data = json.loads(raw)
        return [(float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[5])) for k in data]
    except Exception:
        return None

async def _analyze_klines(symbol):
    now = time.time()
    entry = _klines_cache.get(symbol)
    if entry and now - entry["ts"] < 60:
        return entry
    klines = await _fetch_klines_1m(symbol)
    if not klines or len(klines) < 10:
        _klines_cache[symbol] = {"ts": now, "vol_ratio": 0.0, "vol_trend": False, "polarity_ok": True}
        return _klines_cache[symbol]
    try:
        redis_klines = [{"t": int(time.time() * 1000), "o": o, "h": h, "l": l, "c": c, "v": v} for o, h, l, c, v in klines]
        r.hset("candle_check", f"{symbol}:1m", json.dumps({"ts": now, "klines": redis_klines}))
    except Exception:
        pass

    ce = _strategy_analyze_volume(klines)
    ce["ts"] = now

    mid = len(klines) // 2
    old = [{"h": h, "l": l} for _, h, l, _, _ in klines[:mid]]
    recent = klines[mid:]
    current = klines[-1][3]
    polarity_ok = True
    pivots = []
    for i in range(2, len(old) - 2):
        if all(old[i]["h"] > old[i + j]["h"] for j in (-2, -1, 1, 2)):
            pivots.append({"price": old[i]["h"], "type": "R"})
        if all(old[i]["l"] < old[i + j]["l"] for j in (-2, -1, 1, 2)):
            pivots.append({"price": old[i]["l"], "type": "S"})
    if len(pivots) >= 2:
        pivots.sort(key=lambda x: x["price"])
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
        for c in clusters:
            if c["type"] == "S" and any(l < c["price"] for _, _, l, _, _ in recent):
                if c["price"] > current and (c["price"] - current) / current < 0.003:
                    polarity_ok = False
                    break
    ce["polarity_ok"] = polarity_ok
    _klines_cache[symbol] = ce
    return ce

def _check_candle_wick(symbol, price):
    try:
        cached = r.hget("candle_check", f"{symbol}:1m")
        if not cached:
            return True, ""
        entry = json.loads(cached)
        if time.time() - entry.get("ts", 0) > 120:
            return True, ""
        klines = entry["klines"]
        bt_klines = [(float(k["o"]), float(k["h"]), float(k["l"]), float(k["c"]), float(k["v"])) for k in klines]
        return _strategy_check_candle_wick(bt_klines, price)
    except Exception:
        pass
    return True, ""


async def should_buy(data, cfg, btc_weak=False, regime="normal"):
    symbol = data['symbol']
    price = (float(data['bid']) + float(data['ask'])) / 2

    wick_ok, wick_reason = _check_candle_wick(symbol, price)

    vol_candles = await _analyze_klines(symbol)
    vol_analysis = {
        "vol_activity": vol_candles.get("vol_activity", 0),
        "vol_ratio": vol_candles.get("vol_ratio", 0),
        "vol_detail": vol_candles.get("vol_detail", "?/3"),
        "vol_trend": vol_candles.get("vol_trend", False),
    }
    polarity_ok = vol_candles.get("polarity_ok", True)

    ok, reason, conviction = should_buy_pure(
        data, cfg, vol_analysis, wick_ok, wick_reason,
        btc_weak=btc_weak, regime=regime
    )

    if ok and not polarity_ok:
        return False, f"polarity: flipped support-resistance blocking upside", "none"

    return ok, reason, conviction

def _sync_dynamic_symbols():
    positions = r.hkeys("open_positions")
    if positions:
        r.sadd("dynamic_symbols", *positions)
    current = r.smembers("dynamic_symbols")
    stale = current - set(positions)
    if stale:
        r.srem("dynamic_symbols", *stale)

async def monitoring_loop():
    while True:
        await asyncio.sleep(5)

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

        neutral_count = sum(1 for it in market.values() if it.get('trend_1m') == 'NEUTRAL' and it.get('trend_5m') == 'NEUTRAL')
        total_count = len(market)
        if total_count > 0:
            neutral_ratio = neutral_count / total_count
            if neutral_ratio > 0.6:
                regime = "rango"
            elif (1 - neutral_ratio) > 0.5:
                regime = "trending"
            else:
                regime = "normal"
        else:
            regime = "normal"
        prev_regime = r.get("market_regime")
        if regime != prev_regime:
            r.set("market_regime", regime)
            imb_bonus = 2 if regime == "rango" else 0
            rlimit = "50-80" if regime == "trending" else "50" if regime == "rango" else "50/70"
            tp_str = "1.3%" if regime == "rango" else "1.5%"
            tr_str = "0.8%" if regime == "rango" else "1.5%" if regime == "trending" else "1.0%"
            neutral_syms = [s for s, it in market.items() if it.get('trend_1m') == 'NEUTRAL' and it.get('trend_5m') == 'NEUTRAL']
            trending_syms = [s for s, it in market.items() if it.get('trend_1m') != 'NEUTRAL' or it.get('trend_5m') != 'NEUTRAL']
            log.info(f"[REGIME] {regime.upper()} | range≤{rlimit} | imb_bonus={imb_bonus:+d} | tp={tp_str} | trail={tr_str} | {neutral_count}/{total_count} neutral | N:{','.join(neutral_syms)} | T:{','.join(trending_syms)}" + (f" (was {prev_regime})" if prev_regime else ""))

        async with lock:
            positions = r.hgetall("open_positions")
            _sync_dynamic_symbols()
            if regime == "rango" and prev_regime and prev_regime != "rango":
                for symbol, pos_raw in positions.items():
                    pos = json.loads(pos_raw)
                    if pos.get('trailing_active') and not pos.get('scale_trail_pct'):
                        pos['scale_trail_pct'] = 0.8
                        r.hset("open_positions", symbol, json.dumps(pos))
                        log.info(f"[REGIME] Forced tight trail on {symbol} (regime → rango)")

            # --- 1. PROCESAR VENTAS CON TRAILING TP ---
            for symbol, pos_raw in positions.items():
                pos = json.loads(pos_raw)
                if symbol not in market:
                    continue
                m = market[symbol]
                cur_price = (float(m['bid']) + float(m['ask'])) / 2
                cfg = get_config(symbol)
                effective_tp = 1.3 if regime == "rango" else cfg['tp_pct']
                effective_sl_pct = 3.5 if regime == "rango" else cfg['sl_pct']
                effective_trail = 0.8 if regime == "rango" else 1.5 if regime == "trending" else cfg['trail_pct']

                stop_loss_price = float(pos.get("stop_loss_price", float(pos.get("buy_price", 0)) * 0.995))
                high_water_mark = float(pos.get("high_water_mark", float(pos.get("buy_price", 0))))
                trailing_active = pos.get("trailing_active", False)

                # Update high water mark and trailing stop loss
                if cur_price > high_water_mark:
                    high_water_mark = cur_price
                    new_sl = high_water_mark * (1 - effective_sl_pct / 100)
                    if new_sl > stop_loss_price:
                        stop_loss_price = new_sl

                # Activate trailing on first touch of min TP
                if not trailing_active:
                    pnl = compute_unrealized_net_pct(pos, cur_price)
                    if pnl >= effective_tp:
                        if not pos.get('partial_closed', False):
                            amount_full = float(pos['amount'])
                            cost_full = float(pos.get('cost', 0.0))
                            half_amount = amount_full / 2
                            half_cost = cost_full / 2
                            if half_cost < 10.0:
                                log.info(f"[PARTIAL] {symbol}: half_cost ${half_cost:.1f} < $10, closing full position")
                                val_retorno, sell_price_effective = compute_effective_sell_return(amount_full, cur_price)
                                pct_ganancia = ((val_retorno - cost_full) / cost_full) * 100 if cost_full > 0 else 0.0
                                wallet_full = load_wallet()
                                wallet_full['balance'] = float(wallet_full['balance']) + float(val_retorno)
                                wallet_full['pnl'] = float(wallet_full['pnl']) + float(val_retorno) - float(cost_full)
                                r.set("wallet", json.dumps(wallet_full))
                                trade = {
                                    "symbol": symbol, "entry": float(pos.get('buy_price', 0)),
                                    "buy_ts": float(pos.get('buy_ts', 0)), "exit": cur_price,
                                    "qty": amount_full, "cost": cost_full,
                                    "return": round(val_retorno, 4), "pnl_pct": round(pct_ganancia, 2),
                                    "reason": "TP_FULL", "ts": time.time()
                                }
                                r.lpush("trade_history", json.dumps(trade))
                                r.ltrim("trade_history", 0, 199)
                                ps_key = f"pair_stats:{symbol}"
                                ps_existing = r.hgetall(ps_key)
                                ps_ret = float(ps_existing.get("total_return", 0)) if ps_existing else 0.0
                                ps_cost = float(ps_existing.get("total_cost", 0)) if ps_existing else 0.0
                                ps_cnt = int(ps_existing.get("trade_count", 0)) if ps_existing else 0
                                r.hset(ps_key, mapping={
                                    "total_return": str(round(ps_ret + val_retorno, 4)),
                                    "total_cost": str(round(ps_cost + cost_full, 4)),
                                    "trade_count": str(ps_cnt + 1),
                                })
                                r.hdel("open_positions", symbol)
                                r.delete(f"sl_streak:{symbol}")
                                r.setex(f"cooldown:{symbol}", 60, "blocked")
                                log.info(f"[LOG] TP_FULL on {symbol} at {sell_price_effective:.4f} (PnL: {pct_ganancia:.2f}%) - Balance: {wallet_full['balance']:.2f}")
                                trades_log.info(f"[LOG] TP_FULL on {symbol} at {sell_price_effective:.4f} (PnL: {pct_ganancia:.2f}%) - Balance: {wallet_full['balance']:.2f}")
                                continue
                            else:
                                val_retorno, sell_price_effective = compute_effective_sell_return(half_amount, cur_price)
                                pct_ganancia = ((val_retorno - half_cost) / half_cost) * 100 if half_cost > 0 else 0.0

                                wallet_partial = load_wallet()
                                wallet_partial['balance'] = float(wallet_partial['balance']) + float(val_retorno)
                                wallet_partial['pnl'] = float(wallet_partial['pnl']) + float(val_retorno) - float(half_cost)
                                r.set("wallet", json.dumps(wallet_partial))

                                partial_trade = {
                                    "symbol": symbol,
                                    "entry": float(pos.get('buy_price', 0)),
                                    "buy_ts": float(pos.get('buy_ts', 0)),
                                    "exit": cur_price,
                                    "qty": half_amount,
                                    "cost": half_cost,
                                    "return": round(val_retorno, 4),
                                    "pnl_pct": round(pct_ganancia, 2),
                                    "reason": "TP_PARTIAL",
                                    "ts": time.time()
                                }
                                r.lpush("trade_history", json.dumps(partial_trade))
                                r.ltrim("trade_history", 0, 199)

                                ps_key = f"pair_stats:{symbol}"
                                ps_existing = r.hgetall(ps_key)
                                ps_ret = float(ps_existing.get("total_return", 0)) if ps_existing else 0.0
                                ps_cost = float(ps_existing.get("total_cost", 0)) if ps_existing else 0.0
                                ps_cnt = int(ps_existing.get("trade_count", 0)) if ps_existing else 0
                                r.hset(ps_key, mapping={
                                    "total_return": str(round(ps_ret + val_retorno, 4)),
                                    "total_cost": str(round(ps_cost + half_cost, 4)),
                                    "trade_count": str(ps_cnt + 1),
                                })

                                log.info(f"[PARTIAL] {symbol}: closed 50% at {sell_price_effective:.4f} (PnL: {pct_ganancia:.2f}%)")
                                trades_log.info(f"[PARTIAL] {symbol}: closed 50% at {sell_price_effective:.4f} (PnL: {pct_ganancia:.2f}%)")

                                pos['amount'] = half_amount
                                pos['cost'] = half_cost
                                pos['partial_closed'] = True
                                pos['scale_trail_pct'] = 0.5
                                pos['high_water_mark'] = cur_price

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

                trail_pct_effective = float(pos.get('scale_trail_pct', effective_trail))

                if cur_price <= stop_loss_price:
                    reason = "SL"
                    should_sell = True
                elif trailing_active and cur_price <= high_water_mark * (1 - trail_pct_effective / 100):
                    reason = "TRAIL"
                    should_sell = True
                elif trailing_active and pnl < effective_tp and not pos.get('partial_closed', False):
                    reason = "TP"
                    should_sell = True

                if should_sell:
                    wallet = load_wallet()
                    amount = float(pos['amount'])
                    cost = float(pos.get('cost', 0.0))
                    val_retorno, sell_price_effective = compute_effective_sell_return(amount, cur_price)
                    pct_ganancia = ((val_retorno - cost) / cost) * 100 if cost > 0 else 0.0

                    wallet['balance'] = float(wallet['balance']) + float(val_retorno)
                    wallet['pnl'] = float(wallet['pnl']) + float(val_retorno) - float(cost)

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

                    pair_stats_key = f"pair_stats:{symbol}"
                    existing_stats = r.hgetall(pair_stats_key)
                    prev_return = float(existing_stats.get("total_return", 0)) if existing_stats else 0.0
                    prev_cost = float(existing_stats.get("total_cost", 0)) if existing_stats else 0.0
                    prev_count = int(existing_stats.get("trade_count", 0)) if existing_stats else 0
                    r.hset(pair_stats_key, mapping={
                        "total_return": str(round(prev_return + val_retorno, 4)),
                        "total_cost": str(round(prev_cost + cost, 4)),
                        "trade_count": str(prev_count + 1),
                    })

                    if reason == "SL":
                        streak = r.incr(f"sl_streak:{symbol}")
                        r.expire(f"sl_streak:{symbol}", 86400)

                        cur_imb = float(m.get('imbalance', 0))
                        if cur_imb > 10:
                            cooldown_sec = 60
                            cd_reason = f"imbalance {cur_imb:.1f}x > 10x"
                        elif streak >= 2:
                            cooldown_sec = 3600
                            cd_reason = f"{int(streak)} consecutive SLs"
                        else:
                            pair_liquidity = float(m.get('liquidity', 50000))
                            liq_factor = max(0.2, min(5.0, 50000 / max(pair_liquidity, 1)))
                            cooldown_sec = int(300 * liq_factor)
                            cd_reason = f"liquidity ${pair_liquidity:,.0f} (x{liq_factor:.1f})"

                        r.setex(f"cooldown:{symbol}", cooldown_sec, "blocked")
                        log.warning(f"[ALERT] SL on {symbol} at {sell_price_effective:.4f} (PnL: {pct_ganancia:.2f}%) - cooldown {cooldown_sec}s ({cd_reason})")
                        trades_log.warning(f"[ALERT] SL on {symbol} at {sell_price_effective:.4f} (PnL: {pct_ganancia:.2f}%)")
                    else:
                        r.delete(f"sl_streak:{symbol}")
                        pair_liquidity = float(m.get('liquidity', 50000))
                        liq_factor = max(0.2, min(5.0, 50000 / max(pair_liquidity, 1)))
                        cooldown_sec = int(60 * liq_factor)
                        r.setex(f"cooldown:{symbol}", cooldown_sec, "blocked")
                        log.info(f"[LOG] {reason} on {symbol} at {sell_price_effective:.4f} (PnL: {pct_ganancia:.2f}%) - Balance: {wallet['balance']:.2f}")
                        trades_log.info(f"[LOG] {reason} on {symbol} at {sell_price_effective:.4f} (PnL: {pct_ganancia:.2f}%) - Balance: {wallet['balance']:.2f}")

            # --- 2. PROCESAR COMPRAS (imbalance + rango + macro) ---
            if r.get("trading_locked"):
                continue

            btc_weak = False
            if 'BTCUSDT' in market:
                btc_imb = float(market['BTCUSDT']['imbalance'])
                btc_weak = btc_imb < 2.0

            for item in market.values():
                symbol = item['symbol']
                cfg = get_config(symbol)

                wallet = load_wallet()
                in_cooldown = r.exists(f"cooldown:{symbol}")
                has_position = symbol in positions

                if in_cooldown:
                    log.info(f"[SKIP][{regime.upper()}] {symbol} cooldown active")
                    continue
                if has_position:
                    log.info(f"[SKIP][{regime.upper()}] {symbol} already in position")
                    continue

                min_balance = 10.0
                if wallet['balance'] < min_balance:
                    log.info(f"[SKIP][{regime.upper()}] {symbol} balance ${wallet['balance']:.2f} < ${min_balance:.0f}")
                    continue

                pair_stats_raw = r.hgetall(f"pair_stats:{symbol}")
                if pair_stats_raw:
                    total_return = float(pair_stats_raw.get("total_return", 0))
                    total_cost = float(pair_stats_raw.get("total_cost", 0))
                    net_pnl = total_return - total_cost
                    if total_cost > 0 and (net_pnl / total_cost) < -0.10:
                        log.info(f"[SKIP][{regime.upper()}] {symbol} net PnL {net_pnl/total_cost*100:.1f}% < -10% (blacklisted)")
                        continue

                ok, reason, conviction = await should_buy(item, cfg, btc_weak=btc_weak, regime=regime)

                if ok:
                    price = (float(item['bid']) + float(item['ask'])) / 2
                    low_1h = float(item.get('low_1h', 0.0))

                    atr_val = _klines_cache.get(symbol, {}).get("atr")
                    sl_price = calc_atr_sl(price, atr_val, regime)
                    if sl_price is None:
                        effective_sl_pct = 3.5 if regime == "rango" else cfg['sl_pct']
                        sl_price = low_1h if low_1h > 0 else price * (1 - effective_sl_pct / 100)
                        log.warning(f"[ATR] {symbol}: ATR unavailable, fixed SL=${sl_price:.4f} ({effective_sl_pct}%)")
                    elif atr_val and atr_val > 0:
                        atr_mult = 3.0 if regime == "rango" else 2.5
                        max_sl = price * 0.05
                        if price - sl_price > max_sl:
                            log.info(f"[ATR] {symbol}: ATR=${atr_val:.4f}, capped SL=${sl_price:.4f} (atr_sl was ${price - atr_val * atr_mult:.4f})")
                        else:
                            log.info(f"[ATR] {symbol}: ATR=${atr_val:.4f}, SL=${sl_price:.4f} (x{atr_mult})")
                    invest, risk_per_trade = calc_position_sizing(wallet['balance'], price, sl_price, conviction, regime)
                    qty, buy_price_effective = compute_effective_buy_amount(invest, price)

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
                        "partial_closed": False,
                    }))

                    atr_str = f"ATR=${atr_val:.4f}" if (atr_val and atr_val > 0) else "ATR=na"
                    sl_pct_str = f"SL={((price-sl_price)/price*100):.1f}%"
                    log.info(f"[BUY] {symbol} at {buy_price_effective:.4f} (${invest:.0f}, {conviction}, {atr_str}, {sl_pct_str}, risk=${risk_per_trade:.2f}, {reason})")
                    trades_log.info(f"[BUY] {symbol} at {buy_price_effective:.4f} (${invest:.0f}, {conviction}, {atr_str}, {sl_pct_str}, risk=${risk_per_trade:.2f}, {reason})")
                else:
                    log.info(f"[SKIP][{regime.upper()}] {symbol} {reason}")

@app.get("/", response_class=HTMLResponse)
async def get_index():
    with open("static/index.html", "r") as f:
        return f.read()

@app.get("/api/status")
async def get_status():
    m = r.get("market_status")
    wallet = load_wallet()
    locked = bool(r.get("trading_locked"))
    positions_count = r.hlen("open_positions")
    market_data = json.loads(m) if m else []
    regime = r.get("market_regime") or "unknown"
    return {
        "market": market_data,
        "wallet": wallet,
        "trading_locked": locked,
        "trading_active": not locked and len(market_data) > 0 and wallet['balance'] >= 10.0,
        "positions_count": positions_count,
        "exchange": EXCHANGE_ID,
        "regime": regime
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

        cfg = get_config(symbol)
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
            "partial_closed": False,
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
        wallet['pnl'] = float(wallet['pnl']) + float(val_retorno) - float(cost)
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
    interval = "1m"
    now = time.time()
    cached = r.hget("klines_data", f"{symbol}:{interval}")
    if cached:
        entry = json.loads(cached)
        if now - entry.get("ts", 0) < 60:
            return {"symbol": symbol, "klines": entry["klines"], "levels": calc_sr_levels(entry["klines"]), "trendlines": calc_trend_lines(entry["klines"]), "markers": get_trade_markers(symbol)}

    rest = EXCHANGE.get("rest", {})
    base = rest.get('base_url', 'https://api.binance.com')
    path = rest.get('klines', '/api/v3/klines')
    url = f"{base}{path}?symbol={symbol}&interval={interval}&limit=1000"
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
        return {"symbol": symbol, "error": str(e), "klines": [], "levels": {"supports": [], "resistances": [], "flipped": []}, "trendlines": [], "markers": []}



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

@app.get("/api/backtest/{symbol}")
async def backtest_symbol(symbol: str, days: int = 7):
    result = await run_backtest(symbol, days, EXCHANGE)
    if "error" in result:
        return result
    stats = result["stats"]
    trades = result["trades"]
    log.info(f"[BACKTEST] {symbol} {days}d: {stats['total_trades']} trades, WR {stats['win_rate']}%, PnL {stats['total_pnl_pct']}%, DD {stats['max_drawdown']}%")
    btf = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "logs", "backtest.log")
    try:
        with open(btf, "a") as f:
            f.write(json.dumps({"ts": time.time(), "symbol": symbol, "days": days, "stats": stats, "trades": trades}) + "\n")
    except Exception as e:
        log.error(f"[BACKTEST] Write error: {e}")
    return result

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000, log_level="warning")