# Volumen Binance

Real-time simulated trading bot for Binance. Two-process architecture: `app/main.py` (FastAPI + trading loop) and `app/scanner.py` (WebSocket bookTicker streamer), connected via Redis.

## Project Structure

- `app/main.py` — FastAPI server, monitoring loop (5s), strategy logic, REST API, static frontend
- `app/scanner.py` — Subprocess: Binance WebSocket → order book imbalance, Murphy trend → Redis
- `static/index.html` — Dashboard with TradingView Lightweight Charts
- `scripts/backtest_range_filter.py` — Standalone backtest for range filters
- `logs/` — Runtime logs (bot.log, trades.log)
- `top_pairs.json` — Top 10 USDT pairs from screener (generated at runtime)

## Key Architecture

- Redis is the sole IPC mechanism between main.py and scanner.py
- Scanner writes `market_status` key on every bookTicker tick
- Main reads it every 5s, evaluates buys/sells
- Wallet is virtual, stored in Redis ($100 initial)
- **Dynamic symbols**: Scanner tracks `_base_symbols` (from CLI args) + `dynamic_symbols` (Redis set, synced from `open_positions` by main.py). Scanner polls Redis every ~60s and reconnects WS when the set changes. This prevents orphaned positions when symbols drop out of the top 10.

## Trading Strategy

- **Entry**: order book imbalance ≥ threshold (adjustable), uptrend on 1m+5m Murphy trend, rising volume ratio ≥ 66%, bounded price range, polarity check
- **Exit**: trailing stop-loss / take-profit with 50% partial close on first TP touch
- **Market regime**: dynamically adjusts SL based on `range`/`trending`/`normal`
- **Cooldown**: 5min per symbol after sell; 60min blacklist after 3 consecutive losses
- **Screener**: runs every 6h, scores by `volume * (change_24h / range_24h)`, top 10 USDT pairs

## Key Functions

- `monitoring_loop()` in main.py — main 5s loop
- `should_buy()` — entry decision logic
- `_murphy_trend()` in scanner.py — trend detection via segment comparison
- `run_screener()` — re-scores market every 6h
- `calc_sr_levels()` — pivot-based support/resistance
- `get_config(symbol)` — returns CONFIG dict (same for all symbols)

## Common Tasks

- Start: `./start.sh`
- Stop: `./stop.sh`
- Manual rescan: `curl -X POST http://127.0.0.1:8000/api/screener/run`
- View dashboard: http://127.0.0.1:8000

## Config

- `exchanges.json` — Binance REST/WS endpoints
- `CONFIG` dict in main.py — `{"imbalance": 4, "tp_pct": 1.5, "trail_pct": 1.0, "sl_pct": 2.0}`
- `.env` — Binance testnet API keys (not actively used)
