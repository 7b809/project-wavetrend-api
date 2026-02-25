from fastapi import FastAPI
from fastapi.responses import JSONResponse
import asyncio
import httpx
from datetime import datetime, timedelta
import pytz
from typing import List

from data_convert import generate_7day_batches
from wavetrend_processor import process_wavetrend

app = FastAPI(title="WaveTrend API", version="2.0")

# ==========================
# CONFIG
# ==========================
MAX_CONCURRENT_REQUESTS = 3
REQUEST_TIMEOUT = 20
RETRY_COUNT = 2


def month_short_or_number(month_short: str):
    month_short = month_short.strip().upper()

    month_map = {
        "JAN": 1, "FEB": 2, "MAR": 3,
        "APR": 4, "MAY": 5, "JUN": 6,
        "JUL": 7, "AUG": 8, "SEP": 9,
        "OCT": 10, "NOV": 11, "DEC": 12
    }

    if month_short not in month_map:
        raise ValueError("Invalid month short form")

    return month_map[month_short]


# ==========================
# ROOT
# ==========================
@app.get("/")
async def root():
    return {"status": "WaveTrend API Running (Optimized)"}


# ==========================
# SAFE FETCH FUNCTION
# ==========================
async def fetch_batch(
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
    url: str
) -> List[list]:

    async with semaphore:
        for attempt in range(RETRY_COUNT):
            try:
                response = await client.get(url, timeout=REQUEST_TIMEOUT)

                if response.status_code == 200:
                    data = response.json()
                    return data.get("candles", [])

            except Exception:
                if attempt == RETRY_COUNT - 1:
                    return []

        return []


# ==========================
# MAIN API
# ==========================
@app.get("/api/history")
async def get_history(
    index_name: str,
    year: str,
    month: str,
    expiry_day: str,
    strike: str,
    option_type: str,
    hard_fetch: bool = False
):

    exchange_map = {
        "NIFTY": "NSE",
        "BANKNIFTY": "NSE",
        "FINNIFTY": "NSE",
        "SENSEX": "BSE",
    }

    index_name = index_name.upper()
    month = month.upper()
    option_type = option_type.upper()
    expiry_day = expiry_day.zfill(2)

    if index_name not in exchange_map:
        return JSONResponse(
            {"error": "Unsupported index"},
            status_code=400
        )

    exchange = exchange_map[index_name]

    # ==========================
    # SYMBOL LOGIC
    # ==========================
    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.now(ist)

    current_year_short = now.strftime("%y")
    current_month_number = now.month

    try:
        user_month_number = month_short_or_number(month)
    except ValueError:
        return JSONResponse(
            {"error": "Invalid month format. Use JAN, FEB, MAR etc."},
            status_code=400
        )

    # HARD FETCH MODE
    if hard_fetch:
        symbol = f"{index_name}{year[-2:]}{user_month_number}{expiry_day}{strike}{option_type}"

    # NORMAL MODE
    else:
        if year == current_year_short and user_month_number == current_month_number:
            symbol = f"{index_name}{year[-2:]}{user_month_number}{expiry_day}{strike}{option_type}"
        else:
            symbol = f"{index_name}{year[-2:]}{month}{strike}{option_type}"

    # ==========================
    # LAST 30 DAYS WINDOW
    # ==========================
    past = now - timedelta(days=30)

    batches = generate_7day_batches(
        past.strftime("%d-%m-%Y"),
        now.strftime("%d-%m-%Y")
    )

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
    }

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    all_candles = []

    async with httpx.AsyncClient(headers=headers) as client:

        tasks = []

        for start_ms, end_ms in batches:

            url = (
                f"https://groww.in/v1/api/stocks_fo_data/v1/charting_service/"
                f"delayed/chart/exchange/{exchange}/segment/FNO/{symbol}"
                f"?endTimeInMillis={end_ms}"
                f"&intervalInMinutes=1"
                f"&startTimeInMillis={start_ms}"
            )

            print("---------------------------")
            print("Scheduling batch:", url)
            print("---------------------------")

            tasks.append(fetch_batch(client, semaphore, url))

        results = await asyncio.gather(*tasks)

    # ==========================
    # MERGE RESULTS
    # ==========================
    for candles in results:
        if candles:
            all_candles.extend(candles)

    if not all_candles:
        return {
            "symbol": symbol,
            "total_candles": 0,
            "total_signals": 0,
            "signals": {},
            "message": "No candle data found"
        }

    all_candles = list(set(tuple(c) for c in all_candles))
    all_candles.sort(key=lambda x: x[0])

    signals = process_wavetrend(symbol, all_candles)

    # ==========================
    # GROUP SIGNALS DATE-WISE
    # ==========================
    signals_by_date = {}

    for sig in signals:
        date_key = sig.get("date")

        if date_key not in signals_by_date:
            signals_by_date[date_key] = []

        signals_by_date[date_key].append(sig)

    return {
        "symbol": symbol,
        "exchange": exchange,
        "total_batches": len(batches),
        "total_candles": len(all_candles),
        "total_signals": len(signals),
        "signals": signals_by_date
    }