import pandas as pd
import pytz


def process_wavetrend(symbol, candles, reverse_trade=False, target=None):

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

    events = []
    trade_count = 0
    active_trade = None

    for i in range(1, len(df)):
        prev = df.iloc[i - 1]
        curr = df.iloc[i]

        bull = curr.wt1 > curr.wt2 and prev.wt1 <= prev.wt2
        bear = curr.wt1 < curr.wt2 and prev.wt1 >= prev.wt2

        # ==========================
        # DEFINE ENTRY / EXIT
        # ==========================
        if not reverse_trade:
            entry_signal = bull
            exit_signal = bear
            entry_type = "bullish"
            exit_type = "bearish"
        else:
            entry_signal = bear
            exit_signal = bull
            entry_type = "bearish"
            exit_type = "bullish"

        # ==========================
        # ENTRY
        # ==========================
        if entry_signal and active_trade is None:

            trade_count += 1

            active_trade = {
                "index": i,
                "price": float(curr.close),
                "time": curr.datetime
            }

            events.append({
                "symbol": symbol,
                "type": entry_type,
                "count": trade_count,
                "date": curr.datetime.strftime("%Y-%m-%d"),
                "time": curr.datetime.strftime("%H:%M"),
                "price": float(curr.close),
                "trade_side": "ENTRY"
            })

        # ==========================
        # EXIT
        # ==========================
        elif exit_signal and active_trade is not None:

            entry_price = active_trade["price"]
            entry_time = active_trade["time"]
            entry_index = active_trade["index"]

            exit_price = float(curr.close)

            # Direction aware PnL
            if not reverse_trade:
                points = round(exit_price - entry_price, 2)
            else:
                points = round(entry_price - exit_price, 2)

            percent = round((points / entry_price) * 100, 2)

            holding_minutes = int(
                (curr.datetime - entry_time).total_seconds() / 60
            )

            # ==========================
            # TARGET CHECK (NEW LOGIC)
            # ==========================
            target_hit = False
            target_price = None
            target_time = None

            if target:
                swing_df = df.iloc[entry_index:i+1]

                if not reverse_trade:
                    # LONG TRADE
                    possible_hits = swing_df[swing_df["high"] >= entry_price + target]
                else:
                    # SHORT TRADE
                    possible_hits = swing_df[swing_df["low"] <= entry_price - target]

                if not possible_hits.empty:
                    first_hit = possible_hits.iloc[0]
                    target_hit = True
                    target_price = round(
                        entry_price + target if not reverse_trade
                        else entry_price - target,
                        2
                    )
                    target_time = first_hit["datetime"].strftime("%H:%M")

            # ==========================
            # Swing data (original logic)
            # ==========================
            swing_df = df.iloc[entry_index:i+1]

            min_idx = swing_df["low"].idxmin()
            max_idx = swing_df["high"].idxmax()

            min_price = float(df.loc[min_idx, "low"])
            max_price = float(df.loc[max_idx, "high"])

            min_time = df.loc[min_idx, "datetime"]
            max_time = df.loc[max_idx, "datetime"]

            events.append({
                "symbol": symbol,
                "type": exit_type,
                "count": trade_count,
                "date": curr.datetime.strftime("%Y-%m-%d"),
                "time": curr.datetime.strftime("%H:%M"),
                "price": exit_price,
                "trade_side": "EXIT",

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

                # NEW TARGET FIELDS
                "target": target,
                "target_hit": target_hit,
                "target_price": target_price,
                "target_time": target_time
            })

            active_trade = None

    return events