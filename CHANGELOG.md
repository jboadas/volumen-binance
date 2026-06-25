# Changelog

## 2026-06-25 — Anti-trap filters

### Problem detected
The bot was falling into market liquidity grabs (stop hunts). Analysis of 9 trades showed:
- SYNUSDT: +4.68% SL in **41 seconds** — bought a flash spike that reversed instantly
- SPCXBUSDT, MUBUSDT, AAVEBUSDT, SNDKBUSDT: all stopped out after buying into pump&dump candles
- Common pattern: high imbalance from a brief price spike, entry at/near candle top, immediate reversal

### Changes in `app/main.py`

1. **Fixed range_limit relaxation bug** (`should_buy`, line 383):
   - Before: `elif change_1h > 5.0 or imbalance > 3.0: range_limit = 70`
   - After: `elif change_1h > 5.0: range_limit = 65`
   - Removed `or imbalance > 3.0` — high imbalance during a spike IS the trap signal, not a reason to relax filters

2. **New spike trap filter** (`should_buy`, lines 392-394):
   - If `range_pct > 75` AND `bid_rising == False` → reject entry
   - Catches cases where price is near the top of its range but buying momentum has already faded

3. **New spoof guard** (`should_buy`, lines 419-421):
   - If imbalance meets threshold but `price_direction != "UP"`, require +3x imbalance
   - Catches order book manipulation where bid/qty ratio is high but price isn't actually rising

4. **New candle wick check** (`_check_candle_wick`, lines 345-369):
   - Reads 1m klines from Redis cache (best-effort, fails open)
   - Rejects if price is in top 25% of latest candle AND close < high (mecha)
   - Rejects if latest candle range is > 3x the 20-candle average (volatility spike)
