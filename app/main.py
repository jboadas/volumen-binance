import json
import redis
import subprocess
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse

r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
scanner_process = None

# --- MANEJO DE LIFESPAN (Arranque y Apagado Moderno de la Aplicacion) ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    # LO QUE SE EJECUTA AL ARRANCAR (STARTUP)
    global scanner_process
    scanner_process = subprocess.Popen(["python3", "app/scanner.py"])

    keys_map = {"market_status": "string", "open_positions": "hash"}
    for k, t in keys_map.items():
        if r.type(k) != "none" and r.type(k) != t:
            r.delete(k)

    if not r.exists("wallet"):
        r.set("wallet", json.dumps({"balance": 100.0, "pnl": 0.0}))
        print("[INIT] Redis vacio: Se ha creado una wallet nueva de simulacion con $100.00.")
    else:
        wallet_actual = load_wallet()
        print(f"[INIT] WALLET DETECTADA: Conservando progreso. Balance disponible: ${wallet_actual['balance']:.2f}")

    asyncio.create_task(monitoring_loop())

    yield  # Aqui se mantiene la aplicacion ejecutandose normalmente

    # LO QUE SE EJECUTA AL APAGAR (SHUTDOWN)
    if scanner_process:
        scanner_process.terminate()
        scanner_process.wait()

# Inicializamos FastAPI inyectandole el ciclo de vida moderno libre de warnings
app = FastAPI(lifespan=lifespan)

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

async def monitoring_loop():
    while True:
        await asyncio.sleep(5)

        positions = r.hgetall("open_positions")
        market_raw = r.get("market_status")
        if not market_raw:
            continue

        try:
            market_data = json.loads(market_raw)
            # Filtro defensivo robusto contra nulos o arranques parciales
            market = {item['symbol']: item for item in market_data if isinstance(item, dict) and 'symbol' in item}
        except Exception as e:
            print(f"[ERROR] Procesando market_status desde Redis: {e}")
            continue

        if not market:
            continue

        # --- 1. PROCESAR VENTAS INDEPENDIENTES ---
        for symbol, pos_raw in positions.items():
            pos = json.loads(pos_raw)
            if symbol not in market:
                continue
            m = market[symbol]
            cur_price = (float(m['bid']) + float(m['ask'])) / 2

            pnl = compute_unrealized_net_pct(pos, cur_price)

            # Recuperamos el precio del minimo exacto en dolares guardado al comprar.
            # Respaldo: Si es un trade antiguo que no lo tenia, calcula el -0.5% clasico por seguridad.
            stop_loss_price = float(pos.get("stop_loss_price", float(pos.get("buy_price", 0)) * 0.995))

            # CONDICION DE SALIDA: Take Profit Fijo (+1.5%) O ruptura del Suelo Estructurado (Precio <= SL)
            if pnl >= 1.5 or cur_price <= stop_loss_price:
                wallet = load_wallet()
                amount = float(pos['amount'])
                cost = float(pos.get('cost', 0.0))
                val_retorno, sell_price_effective = compute_effective_sell_return(amount, cur_price)
                pct_ganancia = ((val_retorno - cost) / cost) * 100 if cost > 0 else 0.0

                wallet['balance'] = float(wallet['balance']) + float(val_retorno)
                wallet['pnl'] = float(wallet['pnl']) + float(pct_ganancia)

                r.set("wallet", json.dumps(wallet))
                r.hdel("open_positions", symbol)

                if cur_price <= stop_loss_price:
                    r.setex(f"cooldown:{symbol}", 300, "bloqueado")
                    print(f"[ALERTA] SL ESTRUCTURADO GATILLADO en {symbol}. El precio rompio el suelo de ({stop_loss_price:.4f}). Salida real en {sell_price_effective:.4f} (PnL: {pct_ganancia:.2f}%)")
                else:
                    r.setex(f"cooldown:{symbol}", 60, "bloqueado")
                    print(f"[LOG] TAKE PROFIT ALCANZADO en {symbol} a {sell_price_effective:.2f} (PnL: {pct_ganancia:.2f}%) - Balance: {wallet['balance']:.2f}")

        # --- 2. PROCESAR COMPRAS CON CONFIRMACION DE REBOTE ---
        for item in market.values():
            symbol = item['symbol']
            imbalance = float(item['imbalance'])
            price_direction = item.get('price_direction', 'NEUTRAL')
            range_pct = float(item.get('range_pct', 50.0))
            min_price_5m = float(item.get('min_price_5m', 0.0))  # Capturamos el precio suelo enviado por el scanner

            wallet = load_wallet()
            in_cooldown = r.exists(f"cooldown:{symbol}")

            # FILTRO DE REBOTE CONFIRMADO (25% <= range_pct <= 50%):
            # No compra caidas libres peligrosas y exige que el precio se despegue del piso con volumen.
            if imbalance >= 15 and price_direction == "UP" and (25.0 <= range_pct <= 50.0) and not in_cooldown and symbol not in r.hkeys("open_positions") and wallet['balance'] >= 10.0:
                price = (float(item['bid']) + float(item['ask'])) / 2
                invest = 10.0
                qty, buy_price_effective = compute_effective_buy_amount(invest, price)

                wallet['balance'] = float(wallet['balance']) - float(invest)
                r.set("wallet", json.dumps(wallet))

                # Registramos la posicion inyectando el Stop Loss fijo en dolares
                r.hset("open_positions", symbol, json.dumps({
                    "buy_price": price,
                    "entry_price_effective": buy_price_effective,
                    "amount": qty,
                    "cost": invest,
                    "stop_loss_price": min_price_5m  # El muro de contencion queda grabado en el 0% de la estructura
                }))

                print(f"[LOG] Compra por Rebote Confirmado en {symbol} a {buy_price_effective:.4f}. Rango: {range_pct:.1f}% | SL en Suelo Fijo: {min_price_5m:.4f} | Balance: {wallet['balance']:.2f}")

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
        return {"status": "error", "msg": "Saldo insuficiente"}

    if r.hexists("open_positions", symbol):
        return {"status": "error", "msg": "Ya tienes una posicion abierta"}

    qty, buy_price_effective = compute_effective_buy_amount(invest, market_price)
    wallet['balance'] = float(wallet['balance']) - float(invest)
    r.set("wallet", json.dumps(wallet))

    # Busqueda rapida del suelo actual por si se fuerza una compra manual
    market_raw = r.get("market_status")
    sl_manual = market_price * 0.995  # Respaldo clasico
    if market_raw:
        for item in json.loads(market_raw):
            if item.get('symbol') == symbol:
                sl_manual = float(item.get('min_price_5m', market_price * 0.995))
                break

    r.hset("open_positions", symbol, json.dumps({
        "buy_price": market_price,
        "entry_price_effective": buy_price_effective,
        "amount": qty,
        "cost": invest,
        "stop_loss_price": sl_manual
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
    return {"status": "ok"}

@app.post("/api/wallet/reset")
async def reset_wallet():
    r.set("wallet", json.dumps({"balance": 100.0, "pnl": 0.0}))
    return {"status": "ok"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000, log_level="warning")