import json
import redis
import subprocess
import asyncio
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse

app = FastAPI()
r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

scanner_process = None

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


@app.on_event("startup")
async def startup_event():
    global scanner_process
    scanner_process = subprocess.Popen(["python3", "app/scanner.py"])

    keys_map = {"wallet": "string", "market_status": "string", "open_positions": "hash"}
    for k, t in keys_map.items():
        if r.type(k) != "none" and r.type(k) != t:
            r.delete(k)

    # Inicializamos el wallet solo si Redis no tiene la clave.
    if not r.exists("wallet"):
        r.set("wallet", json.dumps({"balance": 100.0, "pnl": 0.0}))
        print("🚀 Wallet inicializado con balance $100.00 en Redis.")
    else:
        print("🚀 Wallet existente cargado desde Redis.")

    # Start monitoring loop
    asyncio.create_task(monitoring_loop())

@app.on_event("shutdown")
async def shutdown_event():
    global scanner_process
    if scanner_process:
        scanner_process.terminate()
        scanner_process.wait()

async def monitoring_loop():
    while True:
        await asyncio.sleep(5)

        positions = r.hgetall("open_positions")
        market_raw = r.get("market_status")
        if not market_raw:
            continue
        market = {item['symbol']: item for item in json.loads(market_raw)}

        wallet = load_wallet()

        for symbol, pos_raw in positions.items():
            pos = json.loads(pos_raw)
            if symbol not in market:
                continue
            m = market[symbol]
            cur_price = (float(m['bid']) + float(m['ask'])) / 2

            pnl = compute_unrealized_net_pct(pos, cur_price)
            if pnl >= 1.5 or pnl <= -0.5:
                amount = float(pos['amount'])
                cost = float(pos.get('cost', 0.0))
                val_retorno, sell_price_effective = compute_effective_sell_return(amount, cur_price)
                pct_ganancia = ((val_retorno - cost) / cost) * 100 if cost > 0 else 0.0

                wallet['balance'] = float(wallet['balance']) + float(val_retorno)
                wallet['pnl'] = float(wallet['pnl']) + float(pct_ganancia)

                r.set("wallet", json.dumps(wallet))
                r.hdel("open_positions", symbol)

                print(f"🤖 BOT: Vendiendo {symbol} a {sell_price_effective:.2f} (PnL: {pct_ganancia:.2f}%) - Balance: {wallet['balance']:.2f}")

        for item in market.values():
            symbol = item['symbol']
            imbalance = float(item['imbalance'])
            if imbalance >= 15 and symbol not in positions and wallet['balance'] >= 10.0:
                price = (float(item['bid']) + float(item['ask'])) / 2
                invest = 10.0
                qty, buy_price_effective = compute_effective_buy_amount(invest, price)

                wallet['balance'] = float(wallet['balance']) - float(invest)
                r.set("wallet", json.dumps(wallet))

                r.hset("open_positions", symbol, json.dumps({
                    "buy_price": price,
                    "entry_price_effective": buy_price_effective,
                    "amount": qty,
                    "cost": invest
                }))

                print(f"🤖 BOT: Comprando {symbol} a {buy_price_effective:.2f} (mid {price:.2f}) - Balance: {wallet['balance']:.2f}")

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
        elif isinstance(market_data, dict):
            for symbol, item in market_data.items():
                if isinstance(item, dict):
                    item.setdefault('symbol', symbol)
                    market[symbol] = item

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

    invest = 10.0
    wallet = load_wallet()
    if wallet['balance'] < invest:
        return {"status": "error", "msg": "Saldo insuficiente para trade de $10"}

    if r.hexists("open_positions", symbol):
        return {"status": "error", "msg": "Ya tienes una posición abierta en este símbolo"}

    qty, buy_price_effective = compute_effective_buy_amount(invest, market_price)
    wallet['balance'] = float(wallet['balance']) - float(invest)
    r.set("wallet", json.dumps(wallet))

    r.hset("open_positions", symbol, json.dumps({
        "buy_price": market_price,
        "entry_price_effective": buy_price_effective,
        "amount": qty,
        "cost": invest
    }))
    return {"status": "ok"}

@app.post("/api/simulate/sell")
async def simulate_sell(request: Request):
    params = await request.json()
    symbol = params['symbol']
    sell_price = float(params['price'])

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
    return {"status": "ok", "new_balance": wallet['balance'], "sell_price_effective": sell_price_effective, "pct_ganancia": pct_ganancia}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)