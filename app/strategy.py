import json
from collections import deque

CONFIG = {"imbalance": 4, "tp_pct": 2.0, "trail_pct": 1.0, "sl_pct": 2.0}

def get_config(symbol):
    return dict(CONFIG)


def murphy_trend(history, window, n_segments=3):
    if len(history) < window or window < n_segments * 2:
        return "NEUTRAL"
    seg_size = window // n_segments
    recent = history[-window:]
    seg_highs = [max(recent[i*seg_size:(i+1)*seg_size]) for i in range(n_segments)]
    seg_lows = [min(recent[i*seg_size:(i+1)*seg_size]) for i in range(n_segments)]
    highs_up = all(seg_highs[i] < seg_highs[i+1] for i in range(n_segments-1))
    lows_up = all(seg_lows[i] < seg_lows[i+1] for i in range(n_segments-1))
    highs_down = all(seg_highs[i] > seg_highs[i+1] for i in range(n_segments-1))
    lows_down = all(seg_lows[i] > seg_lows[i+1] for i in range(n_segments-1))
    if highs_up and lows_up:
        return "UP"
    if highs_down and lows_down:
        return "DOWN"
    return "NEUTRAL"


def calc_atr(klines, period=14):
    if not klines or len(klines) < period + 1:
        return None
    tr_sum = 0.0
    for i in range(1, period + 1):
        h, l, pc = klines[i][1], klines[i][2], klines[i - 1][3]
        tr_sum += max(h - l, abs(h - pc), abs(l - pc))
    return tr_sum / period


def analyze_volume(klines):
    result = {}
    avg_vol = sum(v for _, _, _, _, v in klines[:-1]) / len(klines[:-1]) if len(klines) > 1 else 0
    latest_vol = klines[-1][4]
    result["vol_activity"] = latest_vol / avg_vol if avg_vol > 0 else 1.0
    if avg_vol > 0:
        bullish = sum(1 for o, _, _, c, v in klines[-3:] if c > o and v >= avg_vol)
        result["vol_ratio"] = bullish / 3.0
        result["vol_detail"] = f"{bullish}/3"
    else:
        result["vol_ratio"] = 0.0
        result["vol_detail"] = "0/3"
    if len(klines) >= 10:
        vol_last5 = sum(v for _, _, _, _, v in klines[-5:]) / 5
        vol_prior5 = sum(v for _, _, _, _, v in klines[-10:-5]) / 5
        result["vol_trend"] = vol_last5 > vol_prior5
    else:
        result["vol_trend"] = False
    atr = calc_atr(klines, 14)
    if atr is not None:
        result["atr"] = atr
    return result


def check_candle_wick(klines, price):
    if len(klines) < 4:
        return True, ""
    completed = klines[-2]
    o, h, l, c, v = completed
    if h <= l:
        return True, ""
    price_pos = (price - l) / (h - l)
    if price_pos > 0.75 and c < h:
        return False, f"candle wick: price at {price_pos*100:.0f}% of completed 1m candle"
    prior = klines[:-2]
    avg_range = sum((k[1] - k[2]) for k in prior) / len(prior)
    if avg_range > 0 and (h - l) > avg_range * 3:
        return False, f"candle spike: range {(h-l)*100:.4f} > 3x avg {avg_range*100:.4f}"
    return True, ""


def should_buy_pure(data, cfg, vol_analysis, wick_ok, wick_reason, btc_weak=False, regime="normal"):
    imbalance = float(data['imbalance'])
    trend_1m = data.get('trend_1m', 'NEUTRAL')
    trend_5m = data.get('trend_5m', 'NEUTRAL')
    change_1h = float(data.get('change_1h_pct', 0))
    bid_rising = data.get('bid_rising', False)
    price_dir = data.get('price_direction', 'NEUTRAL')
    range_pct = float(data.get('range_pct', 100))
    symbol = data['symbol']
    price = (float(data['bid']) + float(data['ask'])) / 2

    if regime == "trending":
        range_limit = 80
    elif change_1h > 5.0:
        range_limit = 65
    else:
        range_limit = 50
    if regime == "rango":
        range_limit = 20

    low_1h = float(data.get('low_1h', 0))
    high_1h = float(data.get('high_1h', 0))

    if range_pct > range_limit:
        return False, f"range {range_pct:.0f}% > {range_limit}% (1h low={low_1h:.2f} high={high_1h:.2f} cur={price:.2f})", "none"

    if range_pct > 75 and not bid_rising:
        return False, f"spike trap: range {range_pct:.0f}% at top (1h low={low_1h:.2f} high={high_1h:.2f}) no bid momentum", "none"

    if change_1h < -8.0:
        return False, f"1h change {change_1h:.1f}% < -8%", "none"

    if not wick_ok:
        return False, wick_reason, "none"

    if trend_1m != "UP" or trend_5m != "UP":
        return False, f"no entry: trend_1m={trend_1m} trend_5m={trend_5m} (need both UP)", "none"

    vol_activity = vol_analysis.get("vol_activity", 0)
    if vol_activity < 0.5:
        return False, f"low volume activity: {vol_activity:.2f}x of avg (need ≥0.5x)", "none"

    vol_ratio = vol_analysis.get("vol_ratio", 0)
    vol_detail = vol_analysis.get("vol_detail", "?/3")
    if vol_ratio < 0.66:
        return False, f"volume {vol_detail} bullish < 66% vol_ratio={vol_ratio:.2f}", "none"

    if not vol_analysis.get("vol_trend", False):
        return False, f"volume trend declining (last 5 avg < prior 5 avg) vol_ratio={vol_ratio:.2f}", "none"

    spread_ok = data.get("spread_ok", True)
    if not spread_ok:
        spread_pct = data.get("spread_pct", 0)
        return False, f"spread widened: {spread_pct:.4f}% > 1.1x mean", "none"

    size_ratio_ok = data.get("size_ratio_ok", True)
    if not size_ratio_ok:
        sr = data.get("size_ratio", 0)
        return False, f"bid/ask size ratio low: {sr:.2f}x (need >1.2x)", "none"

    if imbalance >= 8 and vol_ratio >= 0.8:
        conviction = "high"
    elif imbalance >= 4:
        conviction = "medium"
    else:
        conviction = "low"

    return True, f"momentum: imbalance {imbalance:.1f}x, vol {vol_ratio:.2f}x, range {range_pct:.0f}%, 1h {change_1h:.1f}%", conviction


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


def calc_position_sizing(wallet_balance, price, sl_price, conviction, regime="normal"):
    sl_dist_pct = (price - sl_price) / price
    risk_per_trade = wallet_balance * 0.01
    risk_based = risk_per_trade / sl_dist_pct if sl_dist_pct > 0 else wallet_balance

    if conviction == "high":
        conviction_max = 20.0
    elif conviction == "medium":
        conviction_max = 10.0
    else:
        conviction_max = 5.0
    if regime == "trending" and conviction in ("medium", "high"):
        conviction_max += 5.0
    if regime == "rango":
        conviction_max = min(conviction_max, 10.0)

    invest = min(risk_based, conviction_max, wallet_balance)
    return invest, risk_per_trade


def calc_atr_sl(price, atr_val, regime="normal"):
    if atr_val and atr_val > 0:
        atr_mult = 3.0 if regime == "rango" else 2.5
        sl_price = price - atr_val * atr_mult
        max_sl = price * 0.05
        if price - sl_price > max_sl:
            sl_price = price - max_sl
    else:
        sl_price = None
    min_sl_price = price * 0.992
    if sl_price is not None:
        if sl_price > min_sl_price:
            sl_price = min_sl_price
    return sl_price


def _find_pivot_clusters(klines):
    pivots = []
    for i in range(2, len(klines) - 2):
        if all(klines[i]["h"] > klines[i + j]["h"] for j in (-2, -1, 1, 2)):
            pivots.append({"price": klines[i]["h"], "type": "R"})
        if all(klines[i]["l"] < klines[i + j]["l"] for j in (-2, -1, 1, 2)):
            pivots.append({"price": klines[i]["l"], "type": "S"})
    if len(pivots) < 2:
        return []
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
    clusters.sort(key=lambda x: x["count"], reverse=True)
    return clusters


def calc_sr_levels(klines):
    if len(klines) < 20:
        return {"supports": [], "resistances": [], "flipped": []}
    mid = len(klines) // 2
    old_klines = klines[:mid]
    recent_klines = klines[mid:]
    clusters = _find_pivot_clusters(old_klines)
    if not clusters:
        return {"supports": [], "resistances": [], "flipped": []}
    current = klines[-1]["c"]
    supports, resistances, flipped = [], [], []
    for c in clusters:
        p = round(c["price"], 2)
        if c["type"] == "S":
            broken = any(k["l"] < c["price"] for k in recent_klines)
            if broken:
                if c["price"] > current:
                    flipped.append({"price": p, "type": "S-R"})
            else:
                if c["price"] < current:
                    supports.append(p)
        else:
            broken = any(k["h"] > c["price"] for k in recent_klines)
            if broken:
                if c["price"] < current:
                    flipped.append({"price": p, "type": "R-S"})
            else:
                if c["price"] > current:
                    resistances.append(p)
    supports.sort(reverse=True)
    resistances.sort()
    return {"supports": supports[:3], "resistances": resistances[:3], "flipped": flipped[:3]}


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
        if len(best_seq) >= 3:
            result.append({"type": "uptrend", "start_time": best_seq[0]["time"], "start_price": best_seq[0]["price"], "end_time": best_seq[-1]["time"], "end_price": best_seq[-1]["price"]})

    if len(highs) >= 2:
        best_seq = [highs[-1]]
        for i in range(len(highs) - 2, -1, -1):
            if highs[i]["price"] > best_seq[0]["price"]:
                best_seq.insert(0, highs[i])
        if len(best_seq) >= 3:
            result.append({"type": "downtrend", "start_time": best_seq[0]["time"], "start_price": best_seq[0]["price"], "end_time": best_seq[-1]["time"], "end_price": best_seq[-1]["price"]})

    return result
