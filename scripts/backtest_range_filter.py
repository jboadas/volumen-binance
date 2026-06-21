import json
import urllib.request
import sys
from collections import deque

SYMBOL = "SYNUSDT"
DAYS = 7
LIMIT = 1440 * DAYS

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

def fetch_klines(symbol, limit=1000):
    base = "https://api.binance.com"
    path = "/api/v3/klines"
    url = f"{base}{path}?symbol={symbol}&interval=1m&limit={limit}"
    try:
        raw = urllib.request.urlopen(url, timeout=10).read()
        data = json.loads(raw)
        return [(float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[5])) for k in data]
    except Exception as e:
        print(f"Error fetching klines: {e}")
        return None

def simulate():
    print(f"Fetching {DAYS} days of 1m klines for {SYMBOL}...")
    all_klines = []
    # Binance returns up to 1000 per request; fetch in batches going back
    end_time = None
    for batch in range(DAYS):
        limit = 1440
        base = "https://api.binance.com"
        path = "/api/v3/klines"
        url = f"{base}{path}?symbol={SYMBOL}&interval=1m&limit={limit}"
        if end_time:
            url += f"&endTime={end_time}"
        try:
            raw = urllib.request.urlopen(url, timeout=10).read()
            data = json.loads(raw)
            if not data:
                break
            klines = [(float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[5])) for k in data]
            all_klines = klines + all_klines
            end_time = int(data[0][0]) - 60000  # 1min before first candle
            print(f"  Fetched {len(klines)} candles, total: {len(all_klines)}")
        except Exception as e:
            print(f"Error: {e}")
            break

    if len(all_klines) < 60:
        print("Not enough data")
        return

    print(f"\nTotal candles: {len(all_klines)}")
    print(f"Date range: {all_klines[0][0]} - {all_klines[-1][0]}")
    print()

    price_history = deque(maxlen=3600)
    prev_mid = 0.0

    entries_old = 0
    entries_new = 0
    entry_prices_old = []
    entry_prices_new = []

    for i, (o, h, l, c, v) in enumerate(all_klines):
        current_mid = (h + l) / 2
        now = i

        if now >= 1:
            price_history.append(current_mid)

        history = price_history
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
        if current_mid > prev_mid and prev_mid > 0:
            bid_rising = True
        if current_mid != prev_mid:
            prev_mid = current_mid

        imbalance = 1.0

        data = {
            "symbol": SYMBOL,
            "bid": current_mid,
            "ask": current_mid,
            "imbalance": imbalance,
            "trend_1m": trend_1m,
            "trend_5m": trend_5m,
            "range_pct": range_pct,
            "change_1h_pct": change_1h_pct,
            "bid_rising": bid_rising,
        }

        # --- OLD filter ---
        change_1h = float(data.get('change_1h_pct', 0))
        if range_pct <= 50 and change_1h >= -8.0:
            entries_old += 1
            entry_prices_old.append(current_mid)

        # --- NEW filter ---
        range_limit = 70 if (change_1h > 5.0 or imbalance > 3.0) else 50
        if range_pct <= range_limit and change_1h >= -8.0:
            entries_new += 1
            entry_prices_new.append(current_mid)

    print("=" * 60)
    print(f"BACKTEST: RANGE FILTER SENSITIVITY - {SYMBOL}")
    print("=" * 60)
    print(f"Period: {len(all_klines)} minutes ({len(all_klines)/60:.1f} hours)")
    print(f"Total candles scanned: {len(all_klines)}")
    print()
    print(f"{'Metric':<40} {'OLD (50%)':<15} {'NEW (70%)':<15}")
    print("-" * 70)
    print(f"{'Entry signals triggered':<40} {entries_old:<15} {entries_new:<15}")

    min_ts = all_klines[0][0]
    max_ts = all_klines[-1][0]
    additional = entries_new - entries_old
    pct_increase = ((entries_new - entries_old) / max(entries_old, 1)) * 100
    print(f"{'Additional entries with new filter':<40} {'-':<15} {'+' + str(additional):<15}")
    print(f"{'Increase':<40} {'-':<15} {f'+{pct_increase:.1f}%':<15}")

    if entry_prices_old and entry_prices_new:
        old_high = max(entry_prices_old)
        old_low = min(entry_prices_old)
        new_high = max(entry_prices_new)
        new_low = min(entry_prices_new)
        old_dd = ((old_high - old_low) / old_high) * 100 if old_high > 0 else 0
        new_dd = ((new_high - new_low) / new_high) * 100 if new_high > 0 else 0
        print(f"{'Entry price range (high-low)':<40} {f'${old_high:.4f}-${old_low:.4f}':<15} {f'${new_high:.4f}-${new_low:.4f}':<15}")
        print(f"{'Entry price drawdown (max)':<40} {f'{old_dd:.2f}%':<15} {f'{new_dd:.2f}%':<15}")

    print()
    print("NOTE: Imbalance is not available from klines data;")
    print("      the NEW filter bypass (imbalance > 3x) always evaluates to False")
    print("      so this is a CONSERVATIVE estimate of additional entries.")
    print("      With real bid/ask depth data, additional entries would be higher.")
    print("=" * 60)

if __name__ == "__main__":
    simulate()
