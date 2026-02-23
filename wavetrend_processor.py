import pandas as pd
import pytz


def process_wavetrend(symbol, candles):

    df = pd.DataFrame(candles, columns=[
        "timestamp", "open", "high", "low", "close", "volume"
    ])

    df["datetime"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    df["datetime"] = df["datetime"].dt.tz_convert("Asia/Kolkata")

    ap = (df["high"] + df["low"] + df["close"]) / 3
    esa = ap.ewm(span=10, adjust=False).mean()
    d = abs(ap - esa).ewm(span=10, adjust=False).mean()
    ci = (ap - esa) / (0.015 * d)
    tci = ci.ewm(span=21, adjust=False).mean()

    df["wt1"] = tci
    df["wt2"] = df["wt1"].rolling(4).mean()

    events = []
    bull_count = 0
    bear_count = 0

    for i in range(1, len(df)):
        prev = df.iloc[i - 1]
        curr = df.iloc[i]

        bull = curr.wt1 > curr.wt2 and prev.wt1 <= prev.wt2
        bear = curr.wt1 < curr.wt2 and prev.wt1 >= prev.wt2

        if bull:
            bull_count += 1
            events.append({
                "symbol": symbol,
                "type": "bullish",
                "count": bull_count,
                "date": curr.datetime.strftime("%Y-%m-%d"),
                "time": curr.datetime.strftime("%H:%M"),
                "price": float(curr.close)
            })

        if bear:
            bear_count += 1
            events.append({
                "symbol": symbol,
                "type": "bearish",
                "count": bear_count,
                "date": curr.datetime.strftime("%Y-%m-%d"),
                "time": curr.datetime.strftime("%H:%M"),
                "price": float(curr.close)
            })

    return events