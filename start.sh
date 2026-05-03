#!/bin/bash
# 1. Activar el entorno virtual
source venv/bin/activate

# 2. Asegurar que Redis esté listo
if ! redis-cli ping > /dev/null 2>&1; then
    redis-server --daemonize yes
    sleep 1
fi

# 3. Lanzar la aplicación integrada
python3 app/main.py