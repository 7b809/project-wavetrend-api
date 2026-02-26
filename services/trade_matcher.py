from datetime import datetime, timedelta


def parse_dt(date_str, time_str):
    return datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")


def match_confirmed_trades(
    ce_events,
    pe_events,
    index_events,
    slippage_minutes=2
):
    confirmed = []

    # Only ENTRY trades
    ce_entries = [e for e in ce_events if e["trade_side"] == "ENTRY"]
    pe_entries = [e for e in pe_events if e["trade_side"] == "ENTRY"]
    index_entries = [
        e for e in index_events
        if e["trade_side"] == "ENTRY" and e["type"] == "bullish"
    ]

    for ce in ce_entries:

        ce_dt = parse_dt(ce["date"], ce["time"])

        # ==========================
        # Match PE
        # ==========================
        pe_match = None
        pe_slippage_seconds = None

        for pe in pe_entries:
            pe_dt = parse_dt(pe["date"], pe["time"])
            diff = abs((pe_dt - ce_dt).total_seconds())

            if diff <= slippage_minutes * 60:
                pe_match = pe
                pe_slippage_seconds = int(diff)
                break

        if not pe_match:
            continue

        # ==========================
        # Match INDEX
        # ==========================
        index_match = None
        index_slippage_seconds = None

        for idx in index_entries:
            idx_dt = parse_dt(idx["date"], idx["time"])
            diff = abs((idx_dt - ce_dt).total_seconds())

            if diff <= slippage_minutes * 60:
                index_match = idx
                index_slippage_seconds = int(diff)
                break

        if not index_match:
            continue

        # ==========================
        # Get CE EXIT
        # ==========================
        ce_exit = next(
            (e for e in ce_events
             if e["trade_side"] == "EXIT"
             and e["count"] == ce["count"]),
            None
        )

        if ce_exit:
            confirmed.append({
                "entry": ce,
                "exit": ce_exit,

                # ==========================
                # META DATA (NEW ADDITION)
                # ==========================
                "meta": {
                    "matched_pe": pe_match,
                    "matched_index": index_match,
                    "pe_slippage_seconds": pe_slippage_seconds,
                    "index_slippage_seconds": index_slippage_seconds,
                    "slippage_allowed_minutes": slippage_minutes,
                    "confirmation_type": "3-level"
                }
            })

    return confirmed