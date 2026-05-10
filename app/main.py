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

@app.on_event("startup")
async def startup_event():
    global scanner_process
    scanner_process = subprocess.Popen(["python3", "app/scanner.py"])

    keys_map = {"wallet": "string", "market_status": "string", "open_positions": "hash"}
    for k, t in keys_map.items():
        if r.type(k) != "none" and r.type(k) != t:
            r.delete(k)

    # NUEVO: Inicializamos con $100.00 de presupuesto máximo
    if not r.exists("wallet"):
        r.set("wallet", json.dumps({"balance": 100.0, "pnl": 0.0}))
    print("🚀 Gestión de Capital: Máximo $100 | Trades de $10.")

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

        # Check open positions for selling
        positions = r.hgetall("open_positions")
        market_raw = r.get("market_status")
        if not market_raw:
            continue
        market = {item['symbol']: item for item in json.loads(market_raw)}

        wallet_raw = r.get("wallet")
        if not wallet_raw:
            continue
        wallet = json.loads(wallet_raw)

        for symbol, pos_raw in positions.items():
            pos = json.loads(pos_raw)
            if symbol not in market:
                continue
            m = market[symbol]
            cur_price = (float(m['bid']) + float(m['ask'])) / 2
            buy_price = float(pos['buy_price'])
            pnl = ((cur_price - buy_price) / buy_price) * 100

            if pnl >= 1.5 or pnl <= -0.5:
                # Sell
                amount = float(pos['amount'])
                val_retorno = amount * cur_price
                pct_ganancia = pnl

                wallet['balance'] = float(wallet['balance']) + val_retorno
                wallet['pnl'] = float(wallet['pnl']) + pct_ganancia

                r.set("wallet", json.dumps(wallet))
                r.hdel("open_positions", symbol)

                print(f"🤖 BOT: Vendiendo {symbol} a {cur_price:.2f} (PnL: {pnl:.2f}%) - Balance: {wallet['balance']:.2f}")

        # Check for buying
        for item in market.values():
            symbol = item['symbol']
            imbalance = float(item['imbalance'])
            if imbalance >= 15 and symbol not in positions and wallet['balance'] >= 10.0:
                price = (float(item['bid']) + float(item['ask'])) / 2
                invest = 10.0
                qty = invest / price

                wallet['balance'] -= invest
                r.set("wallet", json.dumps(wallet))

                r.hset("open_positions", symbol, json.dumps({
                    "buy_price": price,
                    "amount": qty,
                    "cost": invest
                }))

                print(f"🤖 BOT: Comprando {symbol} a {price:.2f} (Ratio: {imbalance:.1f}x) - Balance: {wallet['balance']:.2f}")

@app.get("/", response_class=HTMLResponse)
async def get_index():
    with open("static/index.html", "r") as f:
        return f.read()

@app.get("/api/status")
async def get_status():
    m = r.get("market_status")
    w = r.get("wallet")
    return {
        "market": json.loads(m) if m else [],
        "wallet": json.loads(w) if w else {"balance": 100.0, "pnl": 0.0}
    }

@app.get("/api/positions")
async def get_open_positions():
    return r.hgetall("open_positions")

@app.post("/api/simulate/buy")
async def simulate_buy(request: Request):
    params = await request.json()
    symbol, price = params['symbol'], params['price']

    # NUEVO: Cada compra es de exactamente $10.00
    invest = 10.0

    wallet = json.loads(r.get("wallet"))
    if wallet['balance'] < invest:
        return {"status": "error", "msg": "Saldo insuficiente para trade de $10"}

    # Check if position already exists
    if r.hexists("open_positions", symbol):
        return {"status": "error", "msg": "Ya tienes una posición abierta en este símbolo"}

    qty = invest / price
    wallet['balance'] -= invest
    r.set("wallet", json.dumps(wallet))

    r.hset("open_positions", symbol, json.dumps({
        "buy_price": price,
        "amount": qty,
        "cost": invest
    }))
    return {"status": "ok"}

@app.post("/api/simulate/sell")
async def simulate_sell(request: Request):
    params = await request.json()
    symbol = params['symbol']
    # Aseguramos que el precio de venta sea un número
    sell_price = float(params['price'])

    raw_pos = r.hget("open_positions", symbol)
    if not raw_pos: return {"status": "error", "message": "No position"}

    pos = json.loads(raw_pos)

    # Aseguramos que la cantidad de monedas sea un número
    val_retorno = float(pos['amount']) * sell_price

    # Calculamos el porcentaje (solo para el historial/log)
    buy_price = float(pos['buy_price'])
    pct_ganancia = ((sell_price - buy_price) / buy_price) * 100

    # Manejo seguro de la billetera
    wallet_raw = r.get("wallet")
    wallet = json.loads(wallet_raw) if wallet_raw else {"balance": 100.0, "pnl": 0.0}

    # Actualizamos el balance
    wallet['balance'] = float(wallet['balance']) + val_retorno
    wallet['pnl'] = float(wallet['pnl']) + pct_ganancia

    # Guardamos de vuelta en Redis
    r.set("wallet", json.dumps(wallet))

    r.hdel("open_positions", symbol)
    return {"status": "ok", "new_balance": wallet['balance']}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)