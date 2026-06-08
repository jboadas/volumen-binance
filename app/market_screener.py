import asyncio
import json
import urllib.request
import logging, os

LOGFILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "bot.log")
log = logging.getLogger("bot")
log.setLevel(logging.INFO)
log.handlers.clear()
log.propagate = False
fmt = logging.Formatter("%(asctime)s | %(levelname)-5s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
fh = logging.FileHandler(LOGFILE, mode="a")
fh.setFormatter(fmt)
log.addHandler(fh)

TOPFILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "top_pairs.json")

async def screen_market():
    url = "https://api.binance.com/api/v3/ticker/24hr"

    try:
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(None, lambda: urllib.request.urlopen(url).read())
        tickers = json.loads(response.decode('utf-8'))

        usdt_pairs = [t for t in tickers if t['symbol'].endswith('USDT')]

        scored = []
        for t in usdt_pairs:
            try:
                volume = float(t['quoteVolume'])
                high = float(t['highPrice'])
                low = float(t['lowPrice'])
                amplitude = ((high - low) / low) * 100 if low > 0 else 0
                scored.append({
                    "symbol": t['symbol'],
                    "volume": round(volume, 2),
                    "amplitude": round(amplitude, 2),
                    "score": round(volume * amplitude, 2),
                })
            except (ValueError, KeyError):
                continue

        scored.sort(key=lambda x: x['score'], reverse=True)
        top10 = scored[:10]

        with open(TOPFILE, "w") as f:
            json.dump(top10, f, indent=2)

        log.info("[SCREEN] Top 10 pairs written to top_pairs.json")
        for p in top10:
            log.info(f"[SCREEN] {p['symbol']}: vol=${p['volume']/1_000_000:.1f}M, amp={p['amplitude']}%, score={p['score']:.0f}")

    except Exception as e:
        log.error(f"[SCREEN] Error: {e}")

if __name__ == "__main__":
    asyncio.run(screen_market())
