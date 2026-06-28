import json
import urllib.request
import asyncio
from collections import deque

from app.strategy import (
    murphy_trend, calc_atr, analyze_volume, check_candle_wick,
    should_buy_pure, compute_effective_buy_amount,
    compute_effective_sell_return, calc_position_sizing, calc_atr_sl,
    CONFIG, get_config
)


def _synthetic_imbalance(o, h, l, c, v, vol_avg, avg_hl):
    hl_range = h - l
    volatility_spike = hl_range > avg_hl * 1.5 if avg_hl > 0 else False
    if c > o and v > vol_avg * 1.5 and volatility_spike:
        return min(12.0, 8.0 + ((hl_range / max(avg_hl, 1e-10)) - 1.5) * 5.0)
    elif c > o and v > vol_avg * 1.5:
        return 6.0
    elif c > o and v > vol_avg:
        return 4.0
    elif c > o:
        return 3.0
    elif volatility_spike and v > vol_avg:
        return 2.5
    else:
        return 1.0


async def run_backtest(symbol, days, exchange_cfg):
    symbol = symbol.upper()
    rest = exchange_cfg.get("rest", {})
    base = rest.get('base_url', 'https://api.binance.com')
    path = rest.get('klines', '/api/v3/klines')

    all_klines = []
    end_time = None
    days = max(1, min(days, 30))
    for _ in range(days):
        url = f"{base}{path}?symbol={symbol}&interval=1m&limit=1440"
        if end_time:
            url += f"&endTime={end_time}"
        try:
            loop = asyncio.get_event_loop()
            raw = await loop.run_in_executor(None, lambda: urllib.request.urlopen(url, timeout=10).read())
            data = json.loads(raw)
            if not data:
                break
            klines = [(float(k[0]), float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[5])) for k in data]
            all_klines = klines + all_klines
            end_time = int(data[0][0]) - 60000
        except Exception as e:
            return {"symbol": symbol, "error": str(e), "trades": [], "stats": {}}

    if len(all_klines) < 60:
        return {"symbol": symbol, "error": "Not enough data (< 60 candles)", "trades": [], "stats": {}}

    price_history = deque(maxlen=3600)
    prev_mid = 0.0
    trades = []
    open_pos = None
    wallet = 100.0
    peak_wallet = 100.0
    cfg = get_config(symbol)

    for idx, (t, o, h, l, c, v) in enumerate(all_klines):
        mid = (h + l) / 2
        if idx >= 1:
            price_history.append(mid)
        hl = len(price_history)

        trend_1m = murphy_trend(list(price_history), 60, 3) if hl >= 60 else "NEUTRAL"
        trend_5m = murphy_trend(list(price_history), 300, 3) if hl >= 300 else "NEUTRAL"

        range_pct = 50.0
        min_p = mid
        max_p = mid
        if hl > 1:
            max_p = max(price_history)
            min_p = min(price_history)
            if max_p > min_p:
                range_pct = ((mid - min_p) / (max_p - min_p)) * 100

        change_1h = 0.0
        if hl >= 1 and price_history[0] > 0:
            window = min(hl, 60)
            change_1h = ((mid - price_history[-window]) / price_history[-window]) * 100

        bid_rising = mid > prev_mid and prev_mid > 0
        if mid != prev_mid:
            prev_mid = mid

        vol_avg = sum(k[5] for k in all_klines[max(0, idx-20):idx]) / max(idx, 1)
        avg_hl = sum(all_klines[i][2] - all_klines[i][3] for i in range(max(0, idx-10), max(0, idx))) / max(min(idx, 10), 1)
        imb = _synthetic_imbalance(o, h, l, c, v, vol_avg, avg_hl)

        if trend_1m == "UP" and trend_5m == "UP":
            price_dir = "UP"
        elif trend_1m == "DOWN" and trend_5m == "DOWN":
            price_dir = "DOWN"
        else:
            price_dir = "NEUTRAL"

        mid_velocity_5s = mid - list(price_history)[-1] if hl >= 1 else 0.0
        mid_velocity_ok = mid_velocity_5s > mid * 0.0001
        size_ratio_ok = imb > 1.2

        high_5 = max(all_klines[i][2] for i in range(max(0, idx-4), idx+1))
        low_5 = min(all_klines[i][3] for i in range(max(0, idx-4), idx+1))

        candle_body_pct = abs(c - o) / mid * 100 if mid > 0 else 0
        candle_vol_ratio = v / vol_avg if vol_avg > 0 else 0

        data = {
            "symbol": symbol, "bid": mid, "ask": mid, "imbalance": imb,
            "price_direction": price_dir, "trend_1m": trend_1m, "trend_5m": trend_5m,
            "range_pct": range_pct, "low_1h": min_p, "high_1h": max_p,
            "bid_rising": bid_rising, "change_1h_pct": round(change_1h, 2),
            "spread_ok": True, "mid_velocity_5s": round(mid_velocity_5s, 6),
            "mid_velocity_ok": mid_velocity_ok, "size_ratio": round(imb, 2),
            "size_ratio_ok": size_ratio_ok,
            "high_5": high_5, "low_5": low_5,
            "candle_body_pct": round(candle_body_pct, 3),
            "candle_vol_ratio": round(candle_vol_ratio, 2),
        }

        # --- Position management (honest OHLCV) ---
        if open_pos:
            bp = open_pos["buy_price"]
            hwm = open_pos["high_water_mark"]
            sl = open_pos["stop_loss_price"]
            trailing = open_pos["trailing_active"]
            partial = open_pos.get("partial_closed", False)
            remaining_qty = open_pos["remaining_qty"]
            remaining_cost = open_pos["remaining_cost"]
            scale_trail_pct = open_pos.get("scale_trail_pct", 1.0)
            tp_price = bp * (1 + cfg["tp_pct"] / 100)

            # 1. Update HWM from candle high (trailing updates during the minute)
            if h > hwm:
                hwm = h
                new_sl = hwm * (1 - scale_trail_pct / 100)
                if new_sl > sl:
                    sl = new_sl

            # 2. Check events in order (SL first — conservative)
            trail_trigger = hwm * (1 - scale_trail_pct / 100)
            sl_hit = l <= sl
            tp_hit = not partial and not trailing and h >= tp_price
            trail_hit = trailing and l <= trail_trigger

            if sl_hit:
                exit_price = min(sl, mid)
                ret = remaining_qty * exit_price * 0.999 * 0.9995
                pnl_pct = ((ret - remaining_cost) / remaining_cost) * 100
                wallet += ret
                label = "TRAIL" if trailing else "SL"
                trades.append({"t": int(t/1000), "type": label, "entry": bp, "exit": exit_price, "pnl": round(pnl_pct, 2), "cost": round(remaining_cost, 2), "peak": round(((hwm - bp) / bp) * 100, 2)})
                open_pos = None
            elif tp_hit:
                if remaining_cost < 10.0:
                    exit_price = tp_price
                    ret = remaining_qty * exit_price * 0.999 * 0.9995
                    pnl_pct = ((ret - remaining_cost) / remaining_cost) * 100
                    wallet += ret
                    trades.append({"t": int(t/1000), "type": "TP_FULL", "entry": bp, "exit": exit_price, "pnl": round(pnl_pct, 2), "cost": round(remaining_cost, 2), "peak": round(((hwm - bp) / bp) * 100, 2)})
                    open_pos = None
                else:
                    exit_price = tp_price
                    half_qty = remaining_qty / 2
                    half_cost = remaining_cost / 2
                    ret = half_qty * exit_price * 0.999 * 0.9995
                    pnl_pct = ((ret - half_cost) / half_cost) * 100
                    wallet += ret
                    trades.append({"t": int(t/1000), "type": "TP_PARTIAL", "entry": bp, "exit": exit_price, "pnl": round(pnl_pct, 2), "cost": round(half_cost, 2), "peak": round(((hwm - bp) / bp) * 100, 2)})
                    open_pos["remaining_qty"] = half_qty
                    open_pos["remaining_cost"] = half_cost
                    open_pos["partial_closed"] = True
                    open_pos["scale_trail_pct"] = 0.5
                    trailing = True
            elif trail_hit:
                exit_price = trail_trigger
                ret = remaining_qty * exit_price * 0.999 * 0.9995
                pnl_pct = ((ret - remaining_cost) / remaining_cost) * 100
                wallet += ret
                trades.append({"t": int(t/1000), "type": "TRAIL", "entry": bp, "exit": exit_price, "pnl": round(pnl_pct, 2), "cost": round(remaining_cost, 2), "peak": round(((hwm - bp) / bp) * 100, 2)})
                open_pos = None
            elif trailing and not partial:
                pnl_mid = ((mid - bp) / bp) * 100
                if pnl_mid < 1.5:
                    exit_price = mid
                    ret = remaining_qty * exit_price * 0.999 * 0.9995
                    pnl_pct = ((ret - remaining_cost) / remaining_cost) * 100
                    wallet += ret
                    trades.append({"t": int(t/1000), "type": "TP", "entry": bp, "exit": exit_price, "pnl": round(pnl_pct, 2), "cost": round(remaining_cost, 2), "peak": round(((hwm - bp) / bp) * 100, 2)})
                    open_pos = None

            if open_pos:
                open_pos["high_water_mark"] = hwm
                open_pos["stop_loss_price"] = sl
                open_pos["trailing_active"] = trailing

            if wallet > peak_wallet:
                peak_wallet = wallet

        # --- Entry logic (shared should_buy_pure) ---
        else:
            if wallet < 10.0:
                continue

            wick_ok, wick_reason = True, ""
            if hl >= 4:
                klines_window = all_klines[max(0, idx-3):idx+1]
                bt_klines = [(k[1], k[2], k[3], k[4], k[5]) for k in klines_window]
                wick_ok, wick_reason = check_candle_wick(bt_klines, mid)

            vol_window_k = all_klines[max(0, idx-59):idx+1]
            vol_klines = [(k[1], k[2], k[3], k[4], k[5]) for k in vol_window_k]
            vol_analysis = analyze_volume(vol_klines)
            vol_analysis.pop("atr", None)

            ok, reason, conviction = should_buy_pure(data, cfg, vol_analysis, wick_ok, wick_reason, btc_weak=False, regime="normal")

            if ok:
                atr_val = calc_atr(all_klines[max(0, idx-14):idx+1], 14)
                sl_price = calc_atr_sl(mid, atr_val, "normal")
                if sl_price is None:
                    sl_price = mid * 0.98

                invest, risk_per_trade = calc_position_sizing(wallet, mid, sl_price, conviction, "normal")
                qty, _ = compute_effective_buy_amount(invest, mid)
                wallet -= invest

                open_pos = {
                    "buy_price": mid, "amount": qty, "cost": invest,
                    "remaining_qty": qty, "remaining_cost": invest,
                    "stop_loss_price": sl_price, "high_water_mark": mid,
                    "trailing_active": False, "partial_closed": False,
                    "entry_ts": int(t/1000),
                }

    # Force close any open position at end
    if open_pos:
        ret = open_pos["remaining_qty"] * mid * 0.999 * 0.9995
        pnl_pct = ((ret - open_pos["remaining_cost"]) / open_pos["remaining_cost"]) * 100
        wallet += ret
        fp = open_pos["buy_price"]
        trades.append({"t": int(t/1000), "type": "FORCE_CLOSE", "entry": fp, "exit": mid, "pnl": round(pnl_pct, 2), "cost": round(open_pos["remaining_cost"], 2), "peak": round(((open_pos["high_water_mark"] - fp) / fp) * 100, 2)})
        open_pos = None

    final_pnl = wallet - 100.0
    win_trades = [t for t in trades if t["pnl"] > 0]
    loss_trades = [t for t in trades if t["pnl"] <= 0]
    dd = ((peak_wallet - wallet) / peak_wallet) * 100 if peak_wallet > 0 else 0

    stats = {
        "total_trades": len(trades),
        "wins": len(win_trades),
        "losses": len(loss_trades),
        "win_rate": round(len(win_trades) / max(len(trades), 1) * 100, 1),
        "final_balance": round(wallet, 2),
        "total_pnl": round(final_pnl, 2),
        "total_pnl_pct": round((final_pnl / 100) * 100, 2),
        "max_drawdown": round(dd, 2),
        "candles_scanned": len(all_klines),
    }

    return {"symbol": symbol, "stats": stats, "trades": trades}
