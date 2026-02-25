# api/index.py

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from services.symbol_service import build_symbol
from services.groww_fetcher import fetch_last_30_days
from services.index_fetcher import fetch_index_data
from wavetrend_processor import process_wavetrend


app = FastAPI(
    title="WaveTrend API",
    version="2.0"
)


# ==========================
# ROOT
# ==========================
@app.get("/")
async def root():
    return {
        "status": "WaveTrend API Running (Optimized + Modular)"
    }


# ==========================
# HISTORY API
# ==========================
@app.get("/api/history")
async def get_history(
    index_name: str,
    year: str,
    month: str,
    expiry_day: str,
    strike: str,
    option_type: str,
    hard_fetch: bool = False,
    historic_data: bool = False,
    reverse_trade: bool = False
):
    """
    Fetch last 30 days candle data from Groww
    Process WaveTrend signals
    Return grouped signals date-wise
    """

    # ==========================
    # BUILD SYMBOL
    # ==========================
    try:
        symbol, exchange = build_symbol(
            index_name=index_name,
            year=year,
            month=month,
            expiry_day=expiry_day,
            strike=strike,
            option_type=option_type,
            hard_fetch=hard_fetch
        )

    except ValueError as e:
        return JSONResponse(
            {"error": str(e)},
            status_code=400
        )

    # ==========================
    # FETCH CANDLES
    # ==========================
    all_candles, total_batches = await fetch_last_30_days(
        symbol=symbol,
        exchange=exchange
    )

    if not all_candles:
        return {
            "symbol": symbol,
            "exchange": exchange,
            "total_candles": 0,
            "total_signals": 0,
            "signals": {},
            "message": "No candle data found"
        }

    # ==========================
    # PROCESS WAVETREND
    # ==========================
    signals = process_wavetrend(
        symbol,
        all_candles,
        reverse_trade
    )

    # ==========================
    # GROUP SIGNALS DATE-WISE
    # ==========================
    signals_by_date = {}

    for sig in signals:
        date_key = sig.get("date")
        signals_by_date.setdefault(date_key, []).append(sig)

    # ==========================
    # RESPONSE
    # ==========================
    return {
        "symbol": symbol,
        "exchange": exchange,
        "total_batches": total_batches,
        "total_candles": len(all_candles),
        "total_signals": len(signals),
        "signals": signals_by_date,
        "candles": all_candles if historic_data else []
    }

@app.get("/api/index-history")
async def get_index_history(
    index_name: str,
    start_date: str = None,  # format: DD-MM-YYYY
    end_date: str = None,
    historic_data: bool = False,
    reverse_trade: bool = False
):

    try:
        all_candles, total_batches, symbol, exchange = await fetch_index_data(
            index_name=index_name,
            start_date=start_date,
            end_date=end_date
        )

    except ValueError as e:
        return JSONResponse({"error": str(e)}, status_code=400)

    if not all_candles:
        return {
            "symbol": symbol,
            "exchange": exchange,
            "total_candles": 0,
            "total_signals": 0,
            "signals": {},
            "message": "No candle data found"
        }

    # Same WaveTrend logic
    signals = process_wavetrend(
        symbol,
        all_candles,
        reverse_trade
    )

    signals_by_date = {}
    for sig in signals:
        signals_by_date.setdefault(sig["date"], []).append(sig)

    return {
        "symbol": symbol,
        "exchange": exchange,
        "total_batches": total_batches,
        "total_candles": len(all_candles),
        "total_signals": len(signals),
        "signals": signals_by_date,
        "candles": all_candles if historic_data else []
    }    