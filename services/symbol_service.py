# services/symbol_service.py

from datetime import datetime
import pytz

def month_short_to_number(month_short: str):
    month_map = {
        "JAN": 1, "FEB": 2, "MAR": 3,
        "APR": 4, "MAY": 5, "JUN": 6,
        "JUL": 7, "AUG": 8, "SEP": 9,
        "OCT": 10, "NOV": 11, "DEC": 12
    }

    month_short = month_short.strip().upper()

    if month_short not in month_map:
        raise ValueError("Invalid month short form")

    return month_map[month_short]


def build_symbol(index_name, year, month, expiry_day, strike, option_type, hard_fetch):

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
        raise ValueError("Unsupported index")

    exchange = exchange_map[index_name]

    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.now(ist)

    current_year_short = now.strftime("%y")
    current_month_number = now.month

    user_month_number = month_short_to_number(month)

    if hard_fetch:
        symbol = f"{index_name}{year[-2:]}{user_month_number}{expiry_day}{strike}{option_type}"

    else:
        if year == current_year_short and user_month_number == current_month_number:
            symbol = f"{index_name}{year[-2:]}{user_month_number}{expiry_day}{strike}{option_type}"
        else:
            symbol = f"{index_name}{year[-2:]}{month}{strike}{option_type}"

    return symbol, exchange