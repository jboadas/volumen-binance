#!/bin/bash
# Verificar si Redis ya está corriendo
if ! pgrep -x "redis-server" > /dev/null
then
    echo "🚀 Redis no está iniciado. Arrancando..."
    sudo service redis-server start
else
    echo "✅ Redis ya está en ejecución."
fi
# Mensaje visual para saber que está arrancando
echo "🚀 Arrancando el Scanner de Volumen de Binance..."

# Si usas un entorno virtual (venv), lo activamos automáticamente
if [ -d "venv" ]; then
    echo "📦 Activando entorno virtual..."
    source venv/bin/activate
fi

# Ejecutamos el servidor
uvicorn app.main:app --reload --port 8000
