"""Simulate bracket trade outcomes using historical OHLC data."""
import json
import logging
import os
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
from config import SLIPPAGE_PCT, COMMISSION_PCT

log = logging.getLogger(__name__)

# ─── Quarterly data cache (persistent) ─────────────────────────────────────
_QUARTERLY_CACHE_PATH = os.path.join(os.path.dirname(__file__), ".quarterly_cache.json")
_quarterly_cache = {}


def _load_quarterly_cache():
    """Load quarterly balance sheet cache from disk."""
    global _quarterly_cache
    if os.path.exists(_QUARTERLY_CACHE_PATH):
        try:
            with open(_QUARTERLY_CACHE_PATH) as f:
                _quarterly_cache = json.load(f)
        except Exception:
            _quarterly_cache = {}


def _save_quarterly_cache():
    """Save quarterly cache to disk."""
    with open(_QUARTERLY_CACHE_PATH, "w") as f:
        json.dump(_quarterly_cache, f, indent=2)


# Load cache on module import
_load_quarterly_cache()


def simulate_trade(ticker: str, award_date: str, take_profit_pct: float,
                   stop_loss_pct: float, max_hold_days: int):
    """Simulate a bracket trade starting the day after the award_date.

    Entry: next market open after award_date.
    Exit:  first of (TP hit, SL hit, max_hold_days elapsed).

    Returns dict with keys:
        entry_date, entry_price, exit_date, exit_price,
        exit_reason, pnl_pct, hit_tp, hit_sl, timed_out
    Returns None if price data unavailable.
    """
    award_dt = datetime.strptime(award_date[:10], "%Y-%m-%d")

    # Fetch extra days in case of weekends/holidays
    fetch_start = award_dt - timedelta(days=1)
    fetch_end = award_dt + timedelta(days=max_hold_days + 10)

    try:
        df = yf.download(
            ticker,
            start=fetch_start.strftime("%Y-%m-%d"),
            end=fetch_end.strftime("%Y-%m-%d"),
            progress=False,
            auto_adjust=True,
        )
    except Exception as e:
        log.warning(f"yfinance download failed for {ticker}: {e}")
        return None

    if df is None or df.empty or len(df) < 2:
        return None

    # Flatten MultiIndex columns if present (yfinance sometimes returns MultiIndex)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    # Entry: open price on the first trading day ON OR AFTER award_date
    df.index = pd.to_datetime(df.index)
    trading_days = df.index[df.index >= pd.Timestamp(award_dt)]

    if len(trading_days) == 0:
        return None

    entry_day = trading_days[0]
    entry_price = float(df.loc[entry_day, "Open"]) * (1 + SLIPPAGE_PCT)  # Adverse fill on buy
    if entry_price <= 0:
        return None

    take_profit_price = entry_price * (1 + take_profit_pct)
    stop_loss_price = entry_price * (1 - stop_loss_pct)

    # Walk forward through subsequent days, tracking peak for MFE
    subsequent = df.index[df.index > entry_day][:max_hold_days]
    peak_high = entry_price

    for day in subsequent:
        row = df.loc[day]
        high = float(row["High"])
        low = float(row["Low"])

        if high > peak_high:
            peak_high = high

        # Check SL first (conservative — assume worst intraday)
        if low <= stop_loss_price:
            r = _result(ticker, entry_day, entry_price,
                        day, stop_loss_price, "stop_loss", take_profit_pct, stop_loss_pct)
            r["peak_pnl_pct"] = round((peak_high - entry_price) / entry_price * 100, 3)
            return r

        # Then TP
        if high >= take_profit_price:
            r = _result(ticker, entry_day, entry_price,
                        day, take_profit_price, "take_profit", take_profit_pct, stop_loss_pct)
            r["peak_pnl_pct"] = round((peak_high - entry_price) / entry_price * 100, 3)
            return r

    # Time exit: close of last day
    if len(subsequent) == 0:
        return None

    last_day = subsequent[-1]
    exit_price = float(df.loc[last_day, "Close"])
    r = _result(ticker, entry_day, entry_price,
                last_day, exit_price, "time_exit", take_profit_pct, stop_loss_pct)
    r["peak_pnl_pct"] = round((peak_high - entry_price) / entry_price * 100, 3)
    return r


def _get_quarterly_shares(yf_ticker, ticker_symbol: str, date_str: str) -> int:
    """Try to get shares outstanding from quarterly balance sheet near date_str.

    Results are cached to avoid re-fetching balance sheets for the same ticker.
    """
    # Check cache first
    if ticker_symbol in _quarterly_cache:
        cached_shares = _quarterly_cache[ticker_symbol].get("shares", 0)
        if cached_shares:
            return cached_shares

    try:
        bs = yf_ticker.quarterly_balance_sheet
        if bs is None or bs.empty:
            return 0
        target = pd.Timestamp(date_str[:10])
        cols = pd.to_datetime(bs.columns)
        candidates = cols[cols <= target + pd.DateOffset(months=6)]
        if len(candidates) == 0:
            return 0
        closest = candidates[candidates.get_indexer([target], method="nearest")[0]]
        for row_name in ("Ordinary Shares Number", "Share Issued",
                         "Common Stock Shares Outstanding"):
            if row_name in bs.index:
                val = bs.loc[row_name, closest]
                if pd.notna(val) and int(val) > 0:
                    shares = int(val)
                    # Cache the result
                    if ticker_symbol not in _quarterly_cache:
                        _quarterly_cache[ticker_symbol] = {}
                    _quarterly_cache[ticker_symbol]["shares"] = shares
                    _save_quarterly_cache()
                    return shares
    except Exception:
        pass
    return 0


def get_historical_market_cap(ticker: str, date: str) -> float:
    """Approximate market cap on a given date.

    Uses quarterly balance sheet shares when available, falls back to
    current shares outstanding. Results are cached to minimize API calls.
    """
    try:
        t = yf.Ticker(ticker)
        shares = _get_quarterly_shares(t, ticker, date) or t.info.get("sharesOutstanding", 0)
        if not shares:
            return 0.0

        date_dt = datetime.strptime(date[:10], "%Y-%m-%d")
        df = yf.download(
            ticker,
            start=(date_dt - timedelta(days=5)).strftime("%Y-%m-%d"),
            end=(date_dt + timedelta(days=1)).strftime("%Y-%m-%d"),
            progress=False,
            auto_adjust=True,
        )
        if df is None or df.empty:
            return 0.0

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        price = float(df["Close"].iloc[-1])
        return price * shares
    except Exception:
        return 0.0


def simulate_trade_from_row(row: dict, take_profit_pct: float,
                            stop_loss_pct: float, max_hold_days: int):
    """Simulate a bracket trade using stored OHLC columns (open/high/low/close_tN).

    Uses open_t0 as entry price. Checks high/low for T1..T{max_hold_days} against
    TP/SL targets. Falls back to close_t{N} on time exit.

    Returns same dict shape as simulate_trade, or None if data missing.
    """
    entry_price_raw = row.get("open_t0", "")
    if entry_price_raw in ("", None, "None"):
        return None
    try:
        entry_price = float(entry_price_raw) * (1 + SLIPPAGE_PCT)  # Adverse fill on buy
    except (ValueError, TypeError):
        return None
    if entry_price <= 0:
        return None

    take_profit_price = entry_price * (1 + take_profit_pct)
    stop_loss_price   = entry_price * (1 - stop_loss_pct)

    # entry date from open_t0's day index — use posted_date as T0 date
    entry_date_str = row.get("posted_date", "")
    # Handle both YYYY-MM-DD and M/D/YYYY formats
    if entry_date_str and len(entry_date_str) > 0:
        if entry_date_str.count('/') == 2:  # M/D/YYYY format
            parts = entry_date_str.split('/')
            if len(parts) == 3:
                m, d, y = parts
                entry_date_str = f"{y}-{int(m):02d}-{int(d):02d}"
    entry_date = datetime.strptime(entry_date_str[:10], "%Y-%m-%d")
    ticker = row.get("ticker", "")

    # Walk T1..T{max_hold_days}, tracking peak high for MFE
    peak_high = entry_price
    for i in range(1, max_hold_days + 1):
        high_raw  = row.get(f"high_t{i}",  "")
        low_raw   = row.get(f"low_t{i}",   "")
        close_raw = row.get(f"close_t{i}", "")

        if high_raw in ("", None, "None"):
            # Ran out of stored days — time exit on last available close
            for j in range(i - 1, 0, -1):
                c = row.get(f"close_t{j}", "")
                if c not in ("", None, "None"):
                    exit_price = float(c)
                    exit_date = entry_date + timedelta(days=j)
                    r = _result(ticker, entry_date, entry_price,
                                exit_date, exit_price, "time_exit",
                                take_profit_pct, stop_loss_pct)
                    r["peak_pnl_pct"] = round((peak_high - entry_price) / entry_price * 100, 3)
                    return r
            return None

        high  = float(high_raw)
        low   = float(low_raw)
        close = float(close_raw) if close_raw not in ("", None, "None") else entry_price

        if high > peak_high:
            peak_high = high

        # SL checked first (conservative)
        if low <= stop_loss_price:
            exit_date = entry_date + timedelta(days=i)
            r = _result(ticker, entry_date, entry_price,
                        exit_date, stop_loss_price, "stop_loss",
                        take_profit_pct, stop_loss_pct)
            r["peak_pnl_pct"] = round((peak_high - entry_price) / entry_price * 100, 3)
            return r

        if high >= take_profit_price:
            exit_date = entry_date + timedelta(days=i)
            r = _result(ticker, entry_date, entry_price,
                        exit_date, take_profit_price, "take_profit",
                        take_profit_pct, stop_loss_pct)
            r["peak_pnl_pct"] = round((peak_high - entry_price) / entry_price * 100, 3)
            return r

    # Time exit: close of last held day
    last_close_raw = row.get(f"close_t{max_hold_days}", "")
    if last_close_raw in ("", None, "None"):
        return None
    exit_price = float(last_close_raw)
    exit_date = entry_date + timedelta(days=max_hold_days)
    r = _result(ticker, entry_date, entry_price,
                exit_date, exit_price, "time_exit",
                take_profit_pct, stop_loss_pct)
    r["peak_pnl_pct"] = round((peak_high - entry_price) / entry_price * 100, 3)
    return r


def simulate_asymmetric_from_row(row: dict, tp_pct: float, sl_pct: float, max_hold_days: int):
    """Asymmetric exit simulation: TP on intraday high, SL on EOD close.

    Entry: open_t0 with slippage.
    Each day N:
      - TP fires if intraday high_tN >= tp_price  → exit at tp_price (limit order filled)
      - SL fires if EOD close_tN <= sl_price      → exit at close_tN (close-only, avoids noise)
      - TP is checked first: if high hits TP and close is also below SL, TP wins
        (you would have exited intraday before the close)
    Time exit: close of max_hold_days.
    MFE tracked via intraday high for reporting.
    """
    entry_price_raw = row.get("open_t0", "")
    if entry_price_raw in ("", None, "None"):
        return None
    try:
        entry_price = float(entry_price_raw) * (1 + SLIPPAGE_PCT)
    except (ValueError, TypeError):
        return None
    if entry_price <= 0:
        return None

    tp_price = entry_price * (1 + tp_pct)
    sl_price = entry_price * (1 - sl_pct)

    entry_date_str = row.get("posted_date", "")
    if entry_date_str and entry_date_str.count('/') == 2:
        m, d, y = entry_date_str.split('/')
        entry_date_str = f"{y}-{int(m):02d}-{int(d):02d}"
    try:
        entry_date = datetime.strptime(entry_date_str[:10], "%Y-%m-%d")
    except ValueError:
        return None
    ticker = row.get("ticker", "")

    peak_high = entry_price

    # Check t0 close — if it's already below SL on entry day, exit at SL price
    close_t0_raw = row.get("close_t0", "")
    if close_t0_raw not in ("", None, "None"):
        close_t0 = float(close_t0_raw)
        if close_t0 <= sl_price:
            r = _result(ticker, entry_date, entry_price,
                        entry_date, sl_price, "stop_loss", tp_pct, sl_pct)
            r["peak_pnl_pct"] = 0.0
            return r

    for i in range(1, max_hold_days + 1):
        high_raw  = row.get(f"high_t{i}", "")
        close_raw = row.get(f"close_t{i}", "")

        if high_raw in ("", None, "None"):
            # Ran out of stored data — time exit on last available close
            for j in range(i - 1, 0, -1):
                c = row.get(f"close_t{j}", "")
                if c not in ("", None, "None"):
                    exit_date = entry_date + timedelta(days=j)
                    r = _result(ticker, entry_date, entry_price,
                                exit_date, float(c), "time_exit", tp_pct, sl_pct)
                    r["peak_pnl_pct"] = round((peak_high - entry_price) / entry_price * 100, 3)
                    return r
            return None

        high  = float(high_raw)
        close = float(close_raw) if close_raw not in ("", None, "None") else entry_price

        if high > peak_high:
            peak_high = high

        exit_date = entry_date + timedelta(days=i)

        # TP checked first: limit order fills intraday when high reaches target
        if high >= tp_price:
            r = _result(ticker, entry_date, entry_price,
                        exit_date, tp_price, "take_profit", tp_pct, sl_pct)
            r["peak_pnl_pct"] = round((peak_high - entry_price) / entry_price * 100, 3)
            return r

        # SL checked on EOD close only — avoids intraday noise stop-outs
        # Exit at sl_price (not close) so losses are capped at the stop level
        if close <= sl_price:
            r = _result(ticker, entry_date, entry_price,
                        exit_date, sl_price, "stop_loss", tp_pct, sl_pct)
            r["peak_pnl_pct"] = round((peak_high - entry_price) / entry_price * 100, 3)
            return r

    # Time exit: close of last held day
    last_close_raw = row.get(f"close_t{max_hold_days}", "")
    if last_close_raw in ("", None, "None"):
        return None
    exit_price = float(last_close_raw)
    exit_date = entry_date + timedelta(days=max_hold_days)
    r = _result(ticker, entry_date, entry_price,
                exit_date, exit_price, "time_exit", tp_pct, sl_pct)
    r["peak_pnl_pct"] = round((peak_high - entry_price) / entry_price * 100, 3)
    return r


def simulate_eod_from_row(row: dict, tp_pct: float, sl_pct: float, max_hold_days: int):
    """EOD close-based fixed TP/SL simulation using stored OHLC columns.

    Entry: open_t0 with slippage.
    Exit conditions checked at end-of-day close only (not intraday highs/lows):
      - TP: close_tN >= entry * (1 + tp_pct)
      - SL: close_tN <= entry * (1 - sl_pct)
      - Time: close of day max_hold_days

    Peak (MFE) is still tracked via intraday high for reporting purposes only.
    Returns same dict shape as simulate_trade_from_row, or None if data missing.
    """
    entry_price_raw = row.get("open_t0", "")
    if entry_price_raw in ("", None, "None"):
        return None
    try:
        entry_price = float(entry_price_raw) * (1 + SLIPPAGE_PCT)
    except (ValueError, TypeError):
        return None
    if entry_price <= 0:
        return None

    tp_price = entry_price * (1 + tp_pct)
    sl_price = entry_price * (1 - sl_pct)

    entry_date_str = row.get("posted_date", "")
    if entry_date_str and entry_date_str.count('/') == 2:
        m, d, y = entry_date_str.split('/')
        entry_date_str = f"{y}-{int(m):02d}-{int(d):02d}"
    try:
        entry_date = datetime.strptime(entry_date_str[:10], "%Y-%m-%d")
    except ValueError:
        return None
    ticker = row.get("ticker", "")

    peak_high = entry_price

    for i in range(1, max_hold_days + 1):
        close_raw = row.get(f"close_t{i}", "")
        high_raw  = row.get(f"high_t{i}", "")

        if close_raw in ("", None, "None"):
            # Ran out of stored data — time exit on last available close
            for j in range(i - 1, 0, -1):
                c = row.get(f"close_t{j}", "")
                if c not in ("", None, "None"):
                    exit_date = entry_date + timedelta(days=j)
                    r = _result(ticker, entry_date, entry_price,
                                exit_date, float(c), "time_exit", tp_pct, sl_pct)
                    r["peak_pnl_pct"] = round((peak_high - entry_price) / entry_price * 100, 3)
                    return r
            return None

        close = float(close_raw)

        # Track intraday peak for MFE reporting only — not used for exit decisions
        if high_raw not in ("", None, "None"):
            high = float(high_raw)
            if high > peak_high:
                peak_high = high

        exit_date = entry_date + timedelta(days=i)

        # SL checked first (conservative)
        if close <= sl_price:
            r = _result(ticker, entry_date, entry_price,
                        exit_date, close, "stop_loss", tp_pct, sl_pct)
            r["peak_pnl_pct"] = round((peak_high - entry_price) / entry_price * 100, 3)
            return r

        if close >= tp_price:
            r = _result(ticker, entry_date, entry_price,
                        exit_date, close, "take_profit", tp_pct, sl_pct)
            r["peak_pnl_pct"] = round((peak_high - entry_price) / entry_price * 100, 3)
            return r

    # Time exit: close of last held day
    last_close_raw = row.get(f"close_t{max_hold_days}", "")
    if last_close_raw in ("", None, "None"):
        return None
    exit_price = float(last_close_raw)
    exit_date = entry_date + timedelta(days=max_hold_days)
    r = _result(ticker, entry_date, entry_price,
                exit_date, exit_price, "time_exit", tp_pct, sl_pct)
    r["peak_pnl_pct"] = round((peak_high - entry_price) / entry_price * 100, 3)
    return r


def simulate_ratchet_from_row(row: dict, gap_pct: float, max_hold_days: int):
    """Trailing-stop (ratchet) simulation using stored OHLC columns.

    Entry: open_t0 with slippage.
    Stop:  trails peak on an absolute basis — stop = entry * (1 + peak_gain - gap_pct).
           Starts at entry * (1 - gap_pct). Ratchets up with gains, never down.
    Exit:  day's low breaches trailing stop, OR max_hold_days elapsed.
    """
    entry_price_raw = row.get("open_t0", "")
    if entry_price_raw in ("", None, "None"):
        return None
    try:
        entry_price = float(entry_price_raw) * (1 + SLIPPAGE_PCT)
    except (ValueError, TypeError):
        return None
    if entry_price <= 0:
        return None

    peak_gain_pct  = 0.0
    trailing_stop  = entry_price * (1 - gap_pct)

    entry_date_str = row.get("posted_date", "")
    if entry_date_str and entry_date_str.count('/') == 2:
        m, d, y = entry_date_str.split('/')
        entry_date_str = f"{y}-{int(m):02d}-{int(d):02d}"
    try:
        entry_date = datetime.strptime(entry_date_str[:10], "%Y-%m-%d")
    except ValueError:
        return None
    ticker = row.get("ticker", "")

    for i in range(1, max_hold_days + 1):
        high_raw  = row.get(f"high_t{i}", "")
        low_raw   = row.get(f"low_t{i}", "")
        close_raw = row.get(f"close_t{i}", "")

        if high_raw in ("", None, "None"):
            # Ran out of stored data — time exit on last available close
            for j in range(i - 1, 0, -1):
                c = row.get(f"close_t{j}", "")
                if c not in ("", None, "None"):
                    exit_date = entry_date + timedelta(days=j)
                    return _ratchet_result(ticker, entry_date, entry_price,
                                          exit_date, float(c), "time_exit",
                                          gap_pct, peak_gain_pct)
            return None

        high = float(high_raw)
        low  = float(low_raw)

        # Check stop breach before updating peak (stop is from start of day)
        if low <= trailing_stop:
            exit_date = entry_date + timedelta(days=i)
            return _ratchet_result(ticker, entry_date, entry_price,
                                   exit_date, trailing_stop, "trailing_stop",
                                   gap_pct, peak_gain_pct)

        # Ratchet up peak and trailing stop with today's high
        day_gain = (high - entry_price) / entry_price
        if day_gain > peak_gain_pct:
            peak_gain_pct = day_gain
            trailing_stop = entry_price * (1 + peak_gain_pct - gap_pct)

    # Time exit: close of last held day
    last_close_raw = row.get(f"close_t{max_hold_days}", "")
    if last_close_raw in ("", None, "None"):
        return None
    exit_date = entry_date + timedelta(days=max_hold_days)
    return _ratchet_result(ticker, entry_date, entry_price,
                           exit_date, float(last_close_raw), "time_exit",
                           gap_pct, peak_gain_pct)


def simulate_ratchet(ticker: str, award_date: str, gap_pct: float, max_hold_days: int):
    """Trailing-stop simulation using yfinance price data (for from-cache mode)."""
    award_dt = datetime.strptime(award_date[:10], "%Y-%m-%d")
    fetch_start = award_dt - timedelta(days=1)
    fetch_end   = award_dt + timedelta(days=max_hold_days + 10)
    try:
        df = yf.download(ticker,
                         start=fetch_start.strftime("%Y-%m-%d"),
                         end=fetch_end.strftime("%Y-%m-%d"),
                         progress=False, auto_adjust=True)
    except Exception as e:
        log.warning(f"yfinance download failed for {ticker}: {e}")
        return None
    if df is None or df.empty or len(df) < 2:
        return None
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df.index = pd.to_datetime(df.index)
    trading_days = df.index[df.index >= pd.Timestamp(award_dt)]
    if len(trading_days) == 0:
        return None

    entry_day  = trading_days[0]
    entry_price = float(df.loc[entry_day, "Open"]) * (1 + SLIPPAGE_PCT)
    if entry_price <= 0:
        return None

    peak_gain_pct = 0.0
    trailing_stop = entry_price * (1 - gap_pct)
    subsequent    = df.index[df.index > entry_day][:max_hold_days]

    for day in subsequent:
        row = df.loc[day]
        high = float(row["High"])
        low  = float(row["Low"])
        if low <= trailing_stop:
            return _ratchet_result(ticker, entry_day, entry_price,
                                   day, trailing_stop, "trailing_stop",
                                   gap_pct, peak_gain_pct)
        day_gain = (high - entry_price) / entry_price
        if day_gain > peak_gain_pct:
            peak_gain_pct = day_gain
            trailing_stop = entry_price * (1 + peak_gain_pct - gap_pct)

    if len(subsequent) == 0:
        return None
    last_day    = subsequent[-1]
    exit_price  = float(df.loc[last_day, "Close"])
    return _ratchet_result(ticker, entry_day, entry_price,
                           last_day, exit_price, "time_exit",
                           gap_pct, peak_gain_pct)


def _ratchet_result(ticker, entry_day, entry_price, exit_day, exit_price,
                    reason, gap_pct, peak_gain_pct):
    """Build result dict for ratchet simulation with slippage and commission."""
    slipped_exit = exit_price * (1 - SLIPPAGE_PCT)
    commission   = (entry_price + slipped_exit) * COMMISSION_PCT
    pnl          = (slipped_exit - entry_price) - commission
    pnl_pct      = pnl / entry_price * 100
    entry_date_s = str(entry_day.date()) if hasattr(entry_day, "date") else str(entry_day)
    exit_date_s  = str(exit_day.date())  if hasattr(exit_day,  "date") else str(exit_day)
    return {
        "ticker":       ticker,
        "entry_date":   entry_date_s,
        "entry_price":  round(entry_price,  4),
        "exit_date":    exit_date_s,
        "exit_price":   round(slipped_exit, 4),
        "exit_reason":  reason,
        "pnl_pct":      round(pnl_pct, 3),
        "hit_stop":     reason == "trailing_stop",
        "timed_out":    reason == "time_exit",
        "gap_pct":      round(gap_pct * 100, 1),
        "peak_pnl_pct": round(peak_gain_pct * 100, 3),
    }


def _result(ticker, entry_day, entry_price, exit_day, exit_price,
            reason, tp_pct, sl_pct):
    # Apply exit-side slippage (adverse fill) and round-trip commission
    slipped_exit = exit_price * (1 - SLIPPAGE_PCT)
    commission = (entry_price + slipped_exit) * COMMISSION_PCT
    pnl = (slipped_exit - entry_price) - commission
    pnl_pct = pnl / entry_price
    return {
        "ticker": ticker,
        "entry_date": str(entry_day.date()),
        "entry_price": round(entry_price, 4),
        "exit_date": str(exit_day.date()),
        "exit_price": round(slipped_exit, 4),
        "exit_reason": reason,
        "pnl_pct": round(pnl_pct * 100, 3),
        "hit_tp": reason == "take_profit",
        "hit_sl": reason == "stop_loss",
        "timed_out": reason == "time_exit",
        "tp_target": round(tp_pct * 100, 1),
        "sl_target": round(sl_pct * 100, 1),
    }
