import asyncio
import time
import json
import websockets
import redis.asyncio as redis

# Configuración de la estrategia y persistencia
RANGE_WINDOW = 3600  # Ventana de 1 hora para el cálculo de volatilidad
r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

async def update_range(symbol, price, volume):
    """
    Calcula si el precio actual rompe el máximo o mínimo de la última hora
    utilizando un Pipeline de Redis para máxima velocidad.
    """
    now = time.time()
    tick_data = json.dumps({'p': price, 'v': volume, 't': now})

    try:
        async with r.pipeline(transaction=True) as pipe:
            # 1. Guardar el nuevo tick
            pipe.zadd(f"series:{symbol}", {tick_data: now})
            # 2. Limpiar datos antiguos (fuera de la ventana de 1h)
            pipe.zremrangebyscore(f"series:{symbol}", 0, now - RANGE_WINDOW)
            # 3. Obtener el historial para el cálculo
            pipe.zrange(f"series:{symbol}", 0, -1)
            results = await pipe.execute()

        history = results[2]
        if len(history) < 10:
            return None

        prices = [json.loads(t)['p'] for t in history]
        volumes = [json.loads(t)['v'] for t in history]

        # Máximo y mínimo excluyendo el tick actual
        high_1h = max(prices[:-1])
        low_1h = min(prices[:-1])
        avg_vol = sum(volumes) / len(volumes)

        # Lógica de Ruptura de Rango (Volatility Breakout)
        # Confirmación: Precio rompe el nivel + Volumen > 1.5x el promedio
        if price > high_1h and volume > (avg_vol * 1.5):
            return "LONG_BREAKOUT", high_1h, low_1h
        elif price < low_1h and volume > (avg_vol * 1.5):
            return "SHORT_BREAKOUT", high_1h, low_1h

    except Exception:
        # Silenciamos errores de procesamiento para mantener el flujo
        pass

    return None

async def procesar_tick(data):
    """
    Extrae los datos del tick y actualiza las estadísticas en Redis.
    """
    try:
        # Formato de stream directo: 's'=symbol, 'p'=price, 'q'=quantity/volume
        symbol = data['s']
        price = float(data['p'])
        volume = float(data['q'])

        # Incrementar contador global de ticks en Redis
        await r.incr("stats:total_ticks")

        analysis = await update_range(symbol, price, volume)
        if analysis:
            alert_type, h, l = analysis
            await r.incr("stats:alerts_today")
            # Log de la ruptura detectada en la terminal
            print(f"[{time.strftime('%H:%M:%S')}] 🎯 {symbol} {alert_type} | P: {price} | Rango: {l}-{h}")

    except Exception:
        pass

async def iniciar_escaneo_binance(symbols):
    """
    Conexión directa vía Websockets con gestión de Keepalive (Ping/Pong).
    """
    # Construcción de la URL de streams multiplexados
    streams = "/".join([f"{s.lower()}@trade" for s in symbols])
    url = f"wss://stream.binance.com:9443/stream?streams={streams}"

    while True:
        print(f"📡 {time.strftime('%H:%M:%S')} - Conectando directamente a Binance...")
        try:
            # Configuración robusta para evitar timeouts de red
            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=30,
                close_timeout=10
            ) as ws:
                print(f"✅ Sniper Directo Activo | Pares: {len(symbols)}")

                while True:
                    try:
                        # Esperamos el mensaje con un timeout superior al ping_interval
                        mensaje = await asyncio.wait_for(ws.recv(), timeout=40)
                        data = json.loads(mensaje)

                        if 'data' in data:
                            # Delegamos el procesamiento a una tarea de fondo
                            asyncio.create_task(procesar_tick(data['data']))

                    except asyncio.TimeoutError:
                        # Si hay silencio en el stream, verificamos si la conexión sigue viva
                        print("🔍 Verificando latido del socket...")
                        pong_waiter = await ws.ping()
                        await asyncio.wait_for(pong_waiter, timeout=10)

                    # Pequeña pausa para el loop asíncrono
                    await asyncio.sleep(0)

        except Exception as e:
            print(f"⚠️ Error de conexión: {e}")
            print("🔄 Reiniciando socket en 5 segundos...")
            await asyncio.sleep(5)