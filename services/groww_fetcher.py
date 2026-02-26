# services/groww_fetcher.py

import asyncio
import httpx
from datetime import datetime, timedelta
import pytz
from typing import List
from data_convert import generate_7day_batches

MAX_CONCURRENT_REQUESTS = 3
REQUEST_TIMEOUT = 20
RETRY_COUNT = 2


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


async def fetch_last_30_days(symbol: str, exchange: str):

    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.now(ist)
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

            tasks.append(fetch_batch(client, semaphore, url))

        results = await asyncio.gather(*tasks)

    for candles in results:
        if candles:
            all_candles.extend(candles)

    # Remove duplicates + sort
    all_candles = list(set(tuple(c) for c in all_candles))
    all_candles.sort(key=lambda x: x[0])

    return all_candles, len(batches)

async def fetch_last_5_minutes(symbol: str, exchange: str):
    """
    Fetch last 5 minutes 1-min interval candles
    Returns:
        candles (list),
        total_minutes (int)
    """

    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.now(ist)

    # Round to current minute
    now = now.replace(second=0, microsecond=0)

    past = now - timedelta(minutes=5)

    start_ms = int(past.timestamp() * 1000)
    end_ms = int(now.timestamp() * 1000)

    url = (
        f"https://groww.in/v1/api/stocks_fo_data/v1/charting_service/"
        f"delayed/chart/exchange/{exchange}/segment/FNO/{symbol}"
        f"?endTimeInMillis={end_ms}"
        f"&intervalInMinutes=1"
        f"&startTimeInMillis={start_ms}"
    )

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
    }

    async with httpx.AsyncClient(headers=headers) as client:
        try:
            response = await client.get(url, timeout=REQUEST_TIMEOUT)

            if response.status_code == 200:
                data = response.json()
                candles = data.get("candles", [])

                # Sort just in case
                candles.sort(key=lambda x: x[0])

                return candles, len(candles)

        except Exception:
            pass

    return [], 0