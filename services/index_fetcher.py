# services/index_fetcher.py

import asyncio
import httpx
from datetime import datetime, timedelta
import pytz
from typing import Tuple, List
from data_convert import generate_7day_batches


MAX_CONCURRENT_REQUESTS = 3
REQUEST_TIMEOUT = 20
RETRY_COUNT = 2


INDEX_CONFIG = {
    "NIFTY": {
        "exchange": "NSE",
        "symbol": "NIFTY"
    },
    "BANKNIFTY": {
        "exchange": "NSE",
        "symbol": "BANKNIFTY"
    },
    "SENSEX": {
        "exchange": "BSE",
        "symbol": "1"   # Groww uses 1 for Sensex
    }
}


async def fetch_batch(client, semaphore, url):

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


async def fetch_index_data(
    index_name: str,
    start_date: str = None,
    end_date: str = None
) -> Tuple[List[list], int, str, str]:

    index_name = index_name.upper()

    if index_name not in INDEX_CONFIG:
        raise ValueError("Unsupported index for CASH segment")

    exchange = INDEX_CONFIG[index_name]["exchange"]
    symbol = INDEX_CONFIG[index_name]["symbol"]

    ist = pytz.timezone("Asia/Kolkata")

    if not start_date or not end_date:
        now = datetime.now(ist)
        past = now - timedelta(days=30)
        start_date = past.strftime("%d-%m-%Y")
        end_date = now.strftime("%d-%m-%Y")

    batches = generate_7day_batches(start_date, end_date)

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
                f"https://groww.in/v1/api/charting_service/v2/chart/delayed/"
                f"exchange/{exchange}/segment/CASH/{symbol}"
                f"?endTimeInMillis={end_ms}"
                f"&intervalInMinutes=1"
                f"&startTimeInMillis={start_ms}"
            )

            tasks.append(fetch_batch(client, semaphore, url))

        results = await asyncio.gather(*tasks)

    for candles in results:
        if candles:
            all_candles.extend(candles)

    # Remove duplicates + sort
    all_candles = list(set(tuple(c) for c in all_candles))
    all_candles.sort(key=lambda x: x[0])

    return all_candles, len(batches), symbol, exchange