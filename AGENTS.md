# Volumen Binance

Real-time simulated trading bot for Binance. Two-process architecture: `app/main.py` (FastAPI + trading loop) and `app/scanner.py` (WebSocket bookTicker streamer), connected via Redis.

## Project Structure

- `app/main.py` — FastAPI server, monitoring loop (5s), strategy logic, REST API, static frontend, **backtest endpoint**
- `app/scanner.py` — Subprocess: Binance WebSocket → order book imbalance, Murphy trend → Redis
- `static/index.html` — Dashboard with TradingView Lightweight Charts, backtest UI
- `scripts/backtest_range_filter.py` — Standalone backtest for range filters
- `logs/` — Runtime logs (bot.log, trades.log, backtest.log)
- `top_pairs.json` — Top 10 USDT pairs from screener (generated at runtime)

## Key Architecture

- Redis is the sole IPC mechanism between main.py and scanner.py
- Scanner writes `market_status` key on every bookTicker tick
- Main reads it every 5s, evaluates buys/sells
- Wallet is virtual, stored in Redis ($100 initial)
- **Dynamic symbols**: Scanner tracks `_base_symbols` (from CLI args) + `dynamic_symbols` (Redis set, synced from `open_positions` by main.py). Scanner polls Redis every ~60s and reconnects WS when the set changes. This prevents orphaned positions when symbols drop out of the top 10.

## Trading Strategy (v2 — June 2026)

### Entry: Triple Confirmación
1. **Trend filter (base)**: `trend_1m == "UP"` obligatorio — sin tendencia alcista no se entra
2. **Imbalance trigger (disparador)**: order book imbalance ≥ threshold (4x, 6x para DOGE)
3. **Volume confirmation**: volume ratio ≥ 66% bullish + dynamic volume activity ≥ 0.5x avg

### Risk Management
- **SL dinámico ATR(14)**: `ATR * 2.5x` (3.0x en rango), con hard floor 0.8% mínimo
- **Hard cap**: SL nunca más ancho que 5% del precio
- **TP asimétrico**: 2.0% con 50% partial close en primer toque
- **Trailing stop**: 1.0% escala a 0.5% tras partial close
- **Position sizing**: 1% de wallet como riesgo base, capped por convicción ($20 high / $10 medium)

### Backtest (`/api/backtest/{symbol}?days=N`)
- **Imbalance sintético**: usa volatilidad relativa (HL vs media 10 velas) + volumen para aproximar order book
- **FORCE_CLOSE**: posiciones abiertas al final del loop se cierran al último precio
- **Wallet tracking**: fees simulados (0.1% maker + 0.05% taker)

## Key Functions

- `monitoring_loop()` in main.py — main 5s loop
- `should_buy()` — entry decision logic (triple confirmación)
- `backtest_symbol()` in main.py — async backtest endpoint, 7-30 días
- `_murphy_trend()` in scanner.py — trend detection via segment comparison
- `run_screener()` — re-scores market every 6h
- `calc_sr_levels()` — pivot-based support/resistance
- `get_config(symbol)` — returns CONFIG dict
- `_calc_atr()` — ATR(14) on 1m candles, cached in `_klines_cache`

## Backtest Results (7d, $100 wallet, June 2026)

| Symbol   | Trades | Win%  | PnL%    | Final    |
|----------|--------|-------|---------|----------|
| SUIUSDT  | 14     | 57.1% | +0.30%  | $100.30  |
| SOLUSDT  | 10     | 60.0% | +0.21%  | $100.21  |
| AAVEUSDT | 7      | 57.1% | +0.17%  | $100.17  |
| ETHUSDT  | 12     | 50.0% | -0.33%  | $99.67   |
| BNBUSDT  | 8      | 25.0% | -0.82%  | $99.18   |
| BTCUSDT  | 10     | 30.0% | -0.91%  | $99.09   |
| XRPUSDT  | 12     | 33.3% | -0.95%  | $99.05   |
| ZECUSDT  | 11     | 18.2% | -1.33%  | $98.67   |
| DOGEUSDT | 9      | 11.1% | -1.67%  | $98.33   |
| NEARUSDT | 18     | 22.2% | -2.30%  | $97.70   |

### Key Milestones (por orden)
1. **Linea base**: win rates 6-12%, todas las wallets en negativo
2. **ATR 2.5x + hard floor 0.8%**: SL más ancho, menos stops por ruido
3. **Triple confirmación**: trend_1m==UP + imbalance + volumen — WR saltó a ~42%
4. **FORCE_CLOSE bugfix**: wallet tracking correcto (quitó falsos -21% en BTC/DOGE)
5. **Imbalance sintético por volatilidad**: backtest más realista
6. **TP 2.0%** (asimétrico 1:2.5 con SL 0.8%)
7. **Volumen dinámico**: filtro adaptativo reemplazó time filter fijo

### Pendiente
- Whitelist de pares rentables (SOL, SUI, AAVE)
- Posible ajuste fino de TP (1.8% como punto medio)

## Common Tasks

- Start: `./start.sh`
- Stop: `./stop.sh`
- Backtest single: `curl "http://127.0.0.1:8000/api/backtest/SOLUSDT?days=7"`
- Backtest all: `for s in BNBUSDT NEARUSDT ETHUSDT AAVEUSDT ZECUSDT DOGEUSDT SUIUSDT BTCUSDT SOLUSDT XRPUSDT; do curl -s "http://127.0.0.1:8000/api/backtest/$s?days=7" > /tmp/bt_$s.json; done`
- Manual rescan: `curl -X POST http://127.0.0.1:8000/api/screener/run`
- View dashboard: http://127.0.0.1:8000

## Config

- `exchanges.json` — Binance REST/WS endpoints
- `CONFIG` dict in main.py — `{"imbalance": 4, "tp_pct": 2.0, "trail_pct": 1.0, "sl_pct": 2.0}`
- `.env` — Binance testnet API keys (not actively used)
