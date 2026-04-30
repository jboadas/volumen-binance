from flask import Flask, render_template
import redis

app = Flask(__name__)
r = redis.Redis(host='localhost', port=6379, db=0)

@app.route('/')
def index():
    # Total de ticks procesados (el grind acumulado)
    total_ticks = r.get("stats:total_ticks") or 0
    # Alertas detectadas hoy
    daily_alerts = r.get("stats:alerts_today") or 0

    return render_template('index.html',
                           ticks=total_ticks,
                           alerts=daily_alerts,
                           status="Escaneando Rupturas (1h)")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)