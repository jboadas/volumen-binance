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