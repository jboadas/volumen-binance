# Volumen Binance

Real-time simulated trading bot for Binance. Two-process architecture: `app/main.py` (FastAPI + trading loop) and `app/scanner.py` (WebSocket bookTicker streamer), connected via Redis.

## Project Structure

- `app/main.py` — FastAPI server, monitoring loop (5s), REST API, static frontend, **backtest endpoint** (slimmed)
- `app/scanner.py` — Subprocess: Binance WebSocket → order book imbalance, Murphy trend → Redis
- `app/strategy.py` — **Pure strategy functions** (no Redis/FastAPI): murphy_trend, calc_atr, should_buy_pure, position sizing, S/R levels. Shared between live trading and backtest.
- `app/backtest.py` — Standalone backtest engine using `strategy.py`. No Redis dependency.
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
1. **Trend filter (base)**: `trend_1m == "UP"` + `trend_5m == "UP"` — ambas obligatorias (dead cat bounce gate)
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
- `should_buy()` in main.py — wrapper que busca datos en Redis/Binance, delega en `should_buy_pure` de strategy
- `run_backtest()` in backtest.py — engine autónomo (usa OHLCV real para SL/TP con low/high de velas), usa `strategy.py` para decisiones
- `should_buy_pure()` in strategy.py — lógica de entrada pura: trend_1m==UP + trend_5m==UP + imbalance + volumen + L1 checks
- `murphy_trend()` in strategy.py — trend detection via segment comparison
- `run_screener()` — re-scores market every 6h
- `calc_sr_levels()` in strategy.py — pivot-based support/resistance
- `get_config(symbol)` in strategy.py — returns CONFIG dict
- `calc_atr()` in strategy.py — ATR(14) on 1m candles

## Backtest Results (7d, $100 wallet, June 2026)

Estado actual (OHLCV honesta + trend_5m==UP):

| Symbol   | Trades | Win%  | PnL%    | Salidas               |
|----------|--------|-------|---------|-----------------------|
| ETHUSDT  | 2      | 100%  | +0.17%  | TP_PARTIAL + TRAIL    |
| BNBUSDT  | 1      | 100%  | +0.02%  | SL (breakeven)        |
| BTCUSDT  | 1      | 100%  | +0.00%  | SL (breakeven)        |
| DOGEUSDT | 2      | 50%   | -0.00%  | 2× SL (breakeven)     |
| XRPUSDT  | 1      | 0%    | -0.10%  | SL                    |
| NEARUSDT | 0      | —     | —       | —                     |
| AAVEUSDT | 0      | —     | —       | —                     |
| ZECUSDT  | 0      | —     | —       | —                     |
| SUIUSDT  | 0      | —     | —       | —                     |
| SOLUSDT  | 0      | —     | —       | —                     |
| **TOTAL**| **7**  |**71%**|**+$0.09**| 5 SL + 1 TP_PARTIAL + 1 TRAIL |

### Key Milestones (por orden)
1. **Refactor modular**: `app/strategy.py` + `app/backtest.py` — lógica compartida, backtest sin Redis
2. **OHLCV honesta**: SL usa candle low, TP usa candle high, HWM usa candle high. Elimina SL intra-vela no detectados
3. **Trend_5m gate**: `trend_1m==UP && trend_5m==UP` — bloquea dead cat bounces, WR salta de 35% a 71%
4. **Imbalance como convicción** (no gate): imbalance define tamaño de posición ($5/$10/$20), no bloquea entrada
5. **3 confirmaciones L1**: spread compression, mid-velocity 5s, bid/ask size ratio
6. **ATR dinámico**: SL = ATR × 2.5, hard floor 0.8%, hard cap 5%

### Pendiente
- Escalar posición en trades con trend_5m alcista (high conviction automático)
- Monitorear frecuencia en backtest de 30 días (más trades para validar WR)
- Evaluar whitelist manual (ETH, DOGE funcionan; XRP, SUI no)

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
