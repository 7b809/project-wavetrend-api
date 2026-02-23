from fastapi import FastAPI
from fastapi.responses import JSONResponse
import requests
import time
from datetime import datetime, timedelta
import pytz

from data_convert import generate_7day_batches
from wavetrend_processor import process_wavetrend

app = FastAPI()


@app.get("/")
async def root():
    return {"status": "WaveTrend API Running"}


@app.get("/api/history")
async def get_history(
    index_name: str,
    year: str,
    month: str,
    strike: str,
    option_type: str
):

    exchange_map = {
        "NIFTY": "NSE",
        "BANKNIFTY": "NSE",
        "FINNIFTY": "NSE",
        "SENSEX": "BSE",
    }

    index_name = index_name.upper()

    if index_name not in exchange_map:
        return JSONResponse({"error": "Unsupported index"}, status_code=400)

    exchange = exchange_map[index_name]
    symbol = f"{index_name}{year}{month}{strike}{option_type}"

    # ==========================
    # LAST 30 DAYS WINDOW
    # ==========================
    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.now(ist)
    past = now - timedelta(days=30)

    batches = generate_7day_batches(
        past.strftime("%d-%m-%Y"),
        now.strftime("%d-%m-%Y")
    )

    all_candles = []

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
    }

    for start_ms, end_ms in batches:

        url = (
            f"https://groww.in/v1/api/stocks_fo_data/v1/charting_service/"
            f"delayed/chart/exchange/{exchange}/segment/FNO/{symbol}"
            f"?endTimeInMillis={end_ms}"
            f"&intervalInMinutes=1"
            f"&startTimeInMillis={start_ms}"
        )

        try:
            response = requests.get(url, headers=headers, timeout=20)

            if response.status_code == 200:
                data = response.json()
                all_candles.extend(data.get("candles", []))

        except Exception as e:
            return JSONResponse({"error": str(e)}, status_code=500)

        time.sleep(1)

    all_candles = list({tuple(c): c for c in all_candles}.values())
    all_candles.sort(key=lambda x: x[0])

    signals = process_wavetrend(symbol, all_candles)

    return {
        "symbol": symbol,
        "total_candles": len(all_candles),
        "total_signals": len(signals),
        "signals": signals
    }