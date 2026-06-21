#!/bin/bash
pkill -f "python3 app/main.py" 2>/dev/null
pkill -f "python3 app/scanner.py" 2>/dev/null
redis-cli FLUSHALL 2>/dev/null
echo "Bot detenido y Redis limpiado."
