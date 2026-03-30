"""Step 5: Execute bracket trades via Alpaca and manage open positions."""
import logging
import csv
import os
from datetime import datetime, timedelta
import pytz
import alpaca_trade_api as tradeapi
from config import (
    ALPACA_API_KEY, ALPACA_SECRET_KEY, ALPACA_BASE_URL,
    TAKE_PROFIT_PCT, STOP_LOSS_PCT, POSITION_SIZE, MAX_HOLD_DAYS, TZ,
)

log = logging.getLogger(__name__)

POSITIONS_FILE = os.path.join(os.path.dirname(__file__), "positions.csv")
TRADE_LOG_FILE = os.path.join(os.path.dirname(__file__), "trade_log.csv")

POSITIONS_FIELDS = [
    "ticker", "entry_date", "entry_price", "qty", "order_id",
    "take_profit_price", "stop_loss_price", "exit_by_date", "status",
]
TRADE_LOG_FIELDS = [
    "ticker", "entry_date", "entry_price", "exit_date", "exit_price",
    "qty", "pnl", "exit_reason", "score", "contract_value", "awardee",
]


def get_api():
    """Get Alpaca API client."""
    return tradeapi.REST(ALPACA_API_KEY, ALPACA_SECRET_KEY, ALPACA_BASE_URL, api_version="v2")


def execute_trade(ticker, score, contract):
    """Place a bracket order: market buy + take profit + stop loss."""
    api = get_api()
    tz = pytz.timezone(TZ)

    # Duplicate guard 1: existing Alpaca position
    try:
        existing = api.get_position(ticker)
        if existing:
            log.warning(f"SKIP {ticker}: position already open ({existing.qty} shares)")
            return None
    except Exception:
        pass  # no position — good

    # Duplicate guard 2: pending open orders
    try:
        open_orders = api.list_orders(status="open", symbols=[ticker])
        if open_orders:
            log.warning(f"SKIP {ticker}: open order already exists")
            return None
    except Exception:
        pass

    # Get current price
    try:
        quote = api.get_latest_quote(ticker)
        price = float(quote.ap)  # ask price
        if price <= 0:
            price = float(quote.bp)  # bid price fallback
    except Exception as e:
        log.error(f"Failed to get quote for {ticker}: {e}")
        return None

    # Hard budget guard: skip rather than buy 1 share at any price
    if price > POSITION_SIZE:
        log.warning(f"SKIP {ticker}: price ${price:.2f} exceeds position budget ${POSITION_SIZE}")
        return None

    # Calculate order params
    qty = max(1, int(POSITION_SIZE / price))
    take_profit_price = round(price * (1 + TAKE_PROFIT_PCT), 2)
    stop_loss_price = round(price * (1 - STOP_LOSS_PCT), 2)

    # Calculate exit date (4 trading days from now)
    exit_date = _add_trading_days(datetime.now(tz), MAX_HOLD_DAYS)

    try:
        order = api.submit_order(
            symbol=ticker,
            qty=qty,
            side="buy",
            type="market",
            time_in_force="day",
            order_class="bracket",
            take_profit={"limit_price": take_profit_price},
            stop_loss={"stop_price": stop_loss_price},
        )

        log.info(f"TRADE PLACED: {ticker} qty={qty} @ ~${price:.2f} "
                 f"TP=${take_profit_price:.2f} SL=${stop_loss_price:.2f}")

        # Record position
        position = {
            "ticker": ticker,
            "entry_date": datetime.now(tz).strftime("%Y-%m-%d %H:%M"),
            "entry_price": price,
            "qty": qty,
            "order_id": order.id,
            "take_profit_price": take_profit_price,
            "stop_loss_price": stop_loss_price,
            "exit_by_date": exit_date.strftime("%Y-%m-%d"),
            "status": "open",
        }
        _append_csv(POSITIONS_FILE, POSITIONS_FIELDS, position)

        return position

    except Exception as e:
        log.error(f"Failed to place order for {ticker}: {e}")
        return None


def check_and_exit_expired_positions():
    """Check open positions and force-exit any past their hold date."""
    api = get_api()
    tz = pytz.timezone(TZ)
    today = datetime.now(tz).strftime("%Y-%m-%d")

    positions = _read_positions()
    updated = []

    for pos in positions:
        if pos["status"] != "open":
            updated.append(pos)
            continue

        if pos["exit_by_date"] <= today:
            # Force exit
            ticker = pos["ticker"]
            try:
                # Check if we still have the position
                try:
                    alpaca_pos = api.get_position(ticker)
                except Exception:
                    # Position not found — TP or SL bracket leg already filled
                    exit_price = None
                    exit_reason = "auto_closed_unknown"
                    try:
                        parent = api.get_order(pos["order_id"])
                        for leg in (parent.legs or []):
                            if leg.status == "filled":
                                fp = float(leg.filled_avg_price or 0)
                                if fp > 0:
                                    exit_price = fp
                                    lp = float(getattr(leg, "limit_price", None) or 0)
                                    exit_reason = "take_profit" if lp > float(pos["entry_price"]) else "stop_loss"
                                    break
                    except Exception as qe:
                        log.warning(f"Could not query broker fills for {ticker}: {qe}")
                    real_pnl = (exit_price - float(pos["entry_price"])) * int(pos["qty"]) if exit_price else 0
                    pos["status"] = "closed_auto"
                    updated.append(pos)
                    _log_trade_exit(pos, exit_reason, real_pnl, exit_price or 0)
                    continue

                # Sell at market
                api.submit_order(
                    symbol=ticker,
                    qty=abs(int(float(alpaca_pos.qty))),
                    side="sell",
                    type="market",
                    time_in_force="day",
                )
                exit_price = float(alpaca_pos.current_price)
                pnl = (exit_price - float(pos["entry_price"])) * int(pos["qty"])

                log.info(f"FORCED EXIT: {ticker} @ ${exit_price:.2f} PnL=${pnl:.2f}")

                pos["status"] = "closed_time"
                updated.append(pos)
                _log_trade_exit(pos, "time_exit", pnl, exit_price)

            except Exception as e:
                log.error(f"Failed to exit {ticker}: {e}")
                updated.append(pos)
        else:
            updated.append(pos)

    _write_positions(updated)


def _add_trading_days(start, days):
    """Add N NYSE trading days (weekends + holidays) to a date."""
    try:
        import pandas_market_calendars as mcal
        nyse = mcal.get_calendar("NYSE")
        end_search = start + timedelta(days=days + 20)
        schedule = nyse.schedule(
            start_date=start.strftime("%Y-%m-%d"),
            end_date=end_search.strftime("%Y-%m-%d"),
        )
        trading_days = schedule.index
        target = trading_days[days] if len(trading_days) > days else trading_days[-1]
        return target.to_pydatetime().replace(tzinfo=start.tzinfo)
    except ImportError:
        log.warning("pandas_market_calendars not installed; falling back to weekend-only calendar")
        current = start
        added = 0
        while added < days:
            current += timedelta(days=1)
            if current.weekday() < 5:
                added += 1
        return current


def _append_csv(filepath, fields, row):
    """Append a row to a CSV file, creating it with headers if needed."""
    exists = os.path.exists(filepath)
    with open(filepath, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        if not exists:
            writer.writeheader()
        writer.writerow(row)


def _read_positions():
    """Read all positions from CSV."""
    if not os.path.exists(POSITIONS_FILE):
        return []
    with open(POSITIONS_FILE, "r") as f:
        return list(csv.DictReader(f))


def _write_positions(positions):
    """Rewrite the positions CSV."""
    with open(POSITIONS_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=POSITIONS_FIELDS)
        writer.writeheader()
        writer.writerows(positions)


def _log_trade_exit(pos, reason, pnl, exit_price=0):
    """Log a completed trade."""
    row = {
        "ticker": pos["ticker"],
        "entry_date": pos["entry_date"],
        "entry_price": pos["entry_price"],
        "exit_date": datetime.now(pytz.timezone(TZ)).strftime("%Y-%m-%d %H:%M"),
        "exit_price": exit_price,
        "qty": pos["qty"],
        "pnl": round(pnl, 2),
        "exit_reason": reason,
        "score": "",
        "contract_value": "",
        "awardee": "",
    }
    _append_csv(TRADE_LOG_FILE, TRADE_LOG_FIELDS, row)
