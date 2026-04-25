#!/bin/bash

# Mensaje visual para saber que está arrancando
echo "🚀 Arrancando el Scanner de Volumen de Binance..."

# Si usas un entorno virtual (venv), lo activamos automáticamente
if [ -d "venv" ]; then
    echo "📦 Activando entorno virtual..."
    source venv/bin/activate
fi

# Ejecutamos el servidor
uvicorn app.main:app --reload --port 8000
