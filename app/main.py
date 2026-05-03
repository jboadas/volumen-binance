import json
import redis
import subprocess
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse

app = FastAPI()
r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

app.mount("/static", StaticFiles(directory="static"), name="static")

@app.on_event("startup")
async def startup_event():
    subprocess.Popen(["python3", "app/scanner.py"])

    keys_map = {"wallet": "string", "market_status": "string", "open_positions": "hash"}
    for k, t in keys_map.items():
        if r.type(k) != "none" and r.type(k) != t:
            r.delete(k)

    # NUEVO: Inicializamos con $100.00 de presupuesto máximo
    if not r.exists("wallet"):
        r.set("wallet", json.dumps({"balance": 100.0, "pnl": 0.0}))
    print("🚀 Gestión de Capital: Máximo $100 | Trades de $10.")

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
    symbol, sell_price = params['symbol'], params['price']

    raw_pos = r.hget("open_positions", symbol)
    if not raw_pos: return {"status": "error"}

    pos = json.loads(raw_pos)
    val_retorno = pos['amount'] * sell_price
    pct_ganancia = ((sell_price - pos['buy_price']) / pos['buy_price']) * 100

    wallet = json.loads(r.get("wallet"))
    wallet['balance'] += val_retorno
    wallet['pnl'] += pct_ganancia
    r.set("wallet", json.dumps(wallet))

    r.hdel("open_positions", symbol)
    return {"status": "ok"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)