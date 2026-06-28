# Volumen Binance — Simulated Trading Bot

Automated trading bot for Binance with order book imbalance detection, trend analysis, and dynamic position management. Fully simulated on a virtual wallet.

## Architecture

```
start.sh ──► app/main.py (FastAPI + trading loop)
                  │
                  ├── spawns ──► app/scanner.py (WebSocket bookTicker)
                  │                   │
                  │                   └── writes ──► Redis (market_status)
                  │
                  ├── reads Redis every 5s (monitoring_loop)
                  ├── buy/sell decisions with trailing SL/TP
                  ├── periodic screener every 6h (top 10 USDT pairs)
                  └── serves HTML frontend at http://127.0.0.1:8000
```

## Components

| File | Role |
|---|---|---|
| `app/main.py` | Main orchestrator. FastAPI, trading loop, REST endpoints |
| `app/scanner.py` | Subprocess: WebSocket → imbalance, trends → Redis |
| `app/strategy.py` | Pure strategy functions (shared between live + backtest) |
| `app/backtest.py` | Standalone backtest engine (no Redis dependency) |
| `app/__init__.py` | Package init |
| `static/index.html` | Real-time dashboard with TradingView charts |
| `scripts/backtest_range_filter.py` | Range filter backtest |

## Strategy

- **Entry**: order book imbalance ≥ threshold, uptrend on 1m and 5m, rising volume, bounded price range
- **Exit**: trailing stop-loss / take-profit with partial close (50% on first TP)
- **Market regime**: dynamically adjusts SL based on `range` / `trending` / `normal`
- **Cooldown**: 5 min per symbol after a sell; 60 min blacklist after 3 consecutive losses
- **Screener**: ranked by `volume * (change_24h / range_24h)` every 6h, top 10 USDT pairs

## Requirements

- Python 3.8+
- Redis (auto-started by `start.sh`)

## Usage

```bash
pip install -r requirements.txt
./start.sh        # Starts Redis + bot
./stop.sh         # Stops everything and flushes Redis
```

Open `http://127.0.0.1:8000` in your browser.

## API Endpoints

| Method | Path | Description |
|---|---|---|
| GET | `/` | HTML dashboard |
| GET | `/api/status` | Market status, wallet, and lock state |
| GET | `/api/positions` | Open positions with unrealized PnL |
| GET | `/api/trades` | Trade history (last 50) |
| GET | `/api/klines/{symbol}` | Candlesticks + S/R levels + trendlines |
| POST | `/api/simulate/buy` | Manual simulated buy |
| POST | `/api/simulate/sell` | Manual simulated sell |
| POST | `/api/trading/lock` | Lock/unlock trading |
| POST | `/api/wallet/reset` | Reset wallet to $100 |
| POST | `/api/screener/run` | Run screener manually |

## Tech Stack

- **Python asyncio** + **FastAPI** + **uvicorn**
- **Redis** (shared state between processes)
- **websockets** (Binance streaming)
- **TradingView Lightweight Charts** (frontend)
