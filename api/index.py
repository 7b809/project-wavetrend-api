# api/index.py
from datetime import datetime,timezone

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from services.symbol_service import build_symbol
from services.groww_fetcher import fetch_last_30_days
from services.index_fetcher import fetch_index_data
from services.trade_matcher import match_confirmed_trades
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
from datetime import datetime, timezone

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
@app.get("/api/history-data")
async def get_history(
    index_name: str,
    year: str,
    month: str,
    expiry_day: str,
    strike: str,
    option_type: str,
    test_data: bool = False
):
    """
    Fetch last 30 days candle data from Groww

    Always returns:
        - candles
        - test_day_candles

    If test_data=False:
        test_day_candles = []

    If test_data=True:
        last 1 day → test_day_candles
        remaining → candles
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
            "total_batches": total_batches,
            "total_candles": 0,
            "candles": [],
            "test_day_candles": []
        }

    # Default values
    candles_data = all_candles
    test_day_candles = []

    # ==========================
    # SPLIT IF TEST MODE ENABLED
    # ==========================
    if test_data:

        last_timestamp = all_candles[-1][0]

        # timezone-aware UTC date
        last_date = datetime.fromtimestamp(
            last_timestamp,
            tz=timezone.utc
        ).date()

        candles_data = []
        test_day_candles = []

        for candle in all_candles:
            candle_date = datetime.fromtimestamp(
                candle[0],
                tz=timezone.utc
            ).date()

            if candle_date == last_date:
                test_day_candles.append(candle)
            else:
                candles_data.append(candle)

    # ==========================
    # FINAL RESPONSE (ALWAYS SAME FORMAT)
    # ==========================
    return {
        "symbol": symbol,
        "exchange": exchange,
        "total_batches": total_batches,
        "total_candles": len(all_candles),
        "candles": candles_data,
        "test_day_candles": test_day_candles
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
    reverse_trade: bool = False,
    target: float = None   # 👈 ADD THIS
    
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
            hard_fetch=hard_fetch,
            
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
        reverse_trade,
        target=target
        
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
    reverse_trade: bool = False,
    target: float = None  
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
        reverse_trade,
        target=target
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
    
  
@app.get("/api/confirmed-history")
async def get_confirmed_history(
    index_name: str,
    year: str,
    month: str,
    expiry_day: str,
    strike: str,
    hard_fetch: bool = True,   # ✅ ADD THIS
    target: float = None
):
    try:
        # ==========================
        # BUILD CE
        # ==========================
        ce_symbol, exchange = build_symbol(
            index_name=index_name,
            year=year,
            month=month,
            expiry_day=expiry_day,
            strike=strike,
            option_type="CE",
            hard_fetch=hard_fetch
        )

        # ==========================
        # BUILD PE
        # ==========================
        pe_symbol, _ = build_symbol(
            index_name=index_name,
            year=year,
            month=month,
            expiry_day=expiry_day,
            strike=strike,
            option_type="PE",
            hard_fetch=hard_fetch
        )

    except ValueError as e:
        return JSONResponse({"error": str(e)}, status_code=400)

    # ==========================
    # FETCH CE
    # ==========================
    ce_candles, _ = await fetch_last_30_days(ce_symbol, exchange)

    ce_events = process_wavetrend(
        ce_symbol,
        ce_candles,
        reverse_trade=False,
        target=target
    )

    # ==========================
    # FETCH PE (reverse logic)
    # ==========================
    pe_candles, _ = await fetch_last_30_days(pe_symbol, exchange)

    pe_events = process_wavetrend(
        pe_symbol,
        pe_candles,
        reverse_trade=True,
        target=target
    )

    # ==========================
    # FETCH INDEX
    # ==========================
    idx_candles, _, idx_symbol, idx_exchange = await fetch_index_data(index_name)

    index_events = process_wavetrend(
        idx_symbol,
        idx_candles,
        reverse_trade=False
    )

    # ==========================
    # MATCH TRADES
    # ==========================
    confirmed = match_confirmed_trades(
        ce_events,
        pe_events,
        index_events
    )

    # ==========================
    # GROUP CONFIRMED TRADES DATE-WISE
    # ==========================
    trades_by_date = {}

    for trade in confirmed:
        date_key = trade["entry"]["date"]
        trades_by_date.setdefault(date_key, []).append(trade)

    return {
        "ce_symbol": ce_symbol,
        "pe_symbol": pe_symbol,
        "index": idx_symbol,
        "total_confirmed_trades": len(confirmed),
        "total_trading_days": len(trades_by_date),
        "trades": trades_by_date
    }