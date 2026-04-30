import sys
import os
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
import uvicorn

# Forzamos la ruta para asegurar que encuentre scanner.py en el entorno local
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from scanner import iniciar_escaneo_binance, r

# CONFIGURACIÓN DE RUTAS SEGÚN TU IMAGEN
# BASE_DIR es la carpeta 'app'
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# ROOT_DIR es la carpeta raíz donde está 'static'
ROOT_DIR = os.path.dirname(BASE_DIR)
# Apuntamos a la carpeta static que está fuera de app
templates = Jinja2Templates(directory=os.path.join(ROOT_DIR, "static"))

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Gestiona el ciclo de vida de la aplicación.
    Lanza el Sniper de Ruptura de Rangos al iniciar y lo detiene al cerrar.
    """
    pares = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT', 'XRPUSDT', 'ADAUSDT']

    # Tarea de fondo para el escáner asíncrono
    task = asyncio.create_task(iniciar_escaneo_binance(pares))

    yield

    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        print("🛑 Sniper detenido correctamente.")

app = FastAPI(lifespan=lifespan)

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """
    Renderiza la interfaz obteniendo datos en tiempo real de Redis.
    """
    # Obtenemos las estadísticas de forma asíncrona desde Redis
    ticks = await r.get("stats:total_ticks") or 0
    alerts = await r.get("stats:alerts_today") or 0

    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "ticks": "{:,}".format(int(ticks)),
            "alerts": alerts
        }
    )

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)