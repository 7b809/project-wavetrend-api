import pandas as pd
import pytz


def process_wavetrend(symbol, candles):

    df = pd.DataFrame(candles, columns=[
        "timestamp", "open", "high", "low", "close", "volume"
    ])

    df["datetime"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    df["datetime"] = df["datetime"].dt.tz_convert("Asia/Kolkata")

    # ==========================
    # WAVETREND CALCULATION
    # ==========================
    ap = (df["high"] + df["low"] + df["close"]) / 3
    esa = ap.ewm(span=10, adjust=False).mean()
    d = abs(ap - esa).ewm(span=10, adjust=False).mean()
    ci = (ap - esa) / (0.015 * d)
    tci = ci.ewm(span=21, adjust=False).mean()

    df["wt1"] = tci
    df["wt2"] = df["wt1"].rolling(4).mean()

    # ==========================
    # SIGNAL ENGINE
    # ==========================
    events = []
    bull_count = 0
    bear_count = 0

    last_bull_index = {}
    last_bull_price = {}
    last_bull_time = {}

    for i in range(1, len(df)):
        prev = df.iloc[i - 1]
        curr = df.iloc[i]

        bull = curr.wt1 > curr.wt2 and prev.wt1 <= prev.wt2
        bear = curr.wt1 < curr.wt2 and prev.wt1 >= prev.wt2

        # ==========================
        # BULLISH SIGNAL (ENTRY)
        # ==========================
        if bull:
            bull_count += 1

            last_bull_index[bull_count] = i
            last_bull_price[bull_count] = float(curr.close)
            last_bull_time[bull_count] = curr.datetime

            events.append({
                "symbol": symbol,
                "type": "bullish",
                "count": bull_count,
                "date": curr.datetime.strftime("%Y-%m-%d"),
                "time": curr.datetime.strftime("%H:%M"),
                "price": float(curr.close)
            })

        # ==========================
        # BEARISH SIGNAL (EXIT)
        # ==========================
        if bear:
            bear_count += 1

            event = {
                "symbol": symbol,
                "type": "bearish",
                "count": bear_count,
                "date": curr.datetime.strftime("%Y-%m-%d"),
                "time": curr.datetime.strftime("%H:%M"),
                "price": float(curr.close)
            }

            # Only calculate if matching bullish exists
            if bear_count in last_bull_index:

                entry_price = last_bull_price[bear_count]
                exit_price = float(curr.close)
                entry_time = last_bull_time[bear_count]

                # ==========================
                # SWING DATA
                # ==========================
                bull_i = last_bull_index[bear_count]
                swing_df = df.iloc[bull_i:i+1]

                min_idx = swing_df["low"].idxmin()
                max_idx = swing_df["high"].idxmax()

                min_price = float(df.loc[min_idx, "low"])
                max_price = float(df.loc[max_idx, "high"])

                min_time = df.loc[min_idx, "datetime"]
                max_time = df.loc[max_idx, "datetime"]

                # ==========================
                # PNL CALCULATION
                # ==========================
                points = round(exit_price - entry_price, 2)
                percent = round((points / entry_price) * 100, 2)

                holding_minutes = int(
                    (curr.datetime - entry_time).total_seconds() / 60
                )

                time_to_min_minutes = int(
                    (min_time - entry_time).total_seconds() / 60
                )

                time_to_max_minutes = int(
                    (max_time - entry_time).total_seconds() / 60
                )

                event.update({
                    "entry_price": entry_price,
                    "exit_price": exit_price,
                    "points": points,
                    "percent": percent,
                    "holding_minutes": holding_minutes,
                    "result": "profit" if points > 0 else "loss",

                    "swing_min": min_price,
                    "swing_min_time": min_time.strftime("%H:%M"),
                    "swing_max": max_price,
                    "swing_max_time": max_time.strftime("%H:%M"),
                    "swing_range": round(max_price - min_price, 2),

                    "time_to_min_minutes": time_to_min_minutes,
                    "time_to_max_minutes": time_to_max_minutes
                })

            events.append(event)

    return events