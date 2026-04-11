"""tests/resolver/test_issuer_master.py"""
import os, tempfile
from resolver.storage import get_db, ensure_schema
from resolver.issuer_master import build_issuer_master_from_fixtures, is_eligible_common_equity

MOCK_TICKERS = {
    "0": {"cik_str": 12345, "ticker": "AAPL", "title": "Apple Inc."},
    "1": {"cik_str": 67890, "ticker": "MSFT", "title": "Microsoft Corporation"},
    "2": {"cik_str": 11111, "ticker": "SPYUS", "title": "SPDR S&P 500 ETF Trust"},
}
MOCK_EXCHANGE = {
    "fields": ["cik", "name", "ticker", "exchange"],
    "data": [
        [12345, "Apple Inc.", "AAPL", "Nasdaq"],
        [67890, "Microsoft Corporation", "MSFT", "Nasdaq"],
        [11111, "SPDR S&P 500 ETF Trust", "SPYUS", "NYSEArca"],
    ],
}
MOCK_NASDAQ = (
    "Symbol|Security Name|Market Category|Test Issue|Financial Status|Round Lot Size\n"
    "AAPL|Apple Inc. - Common Stock|Q|N|N|100\n"
    "MSFT|Microsoft Corporation - Common Stock|Q|N|N|100\n"
    "File Creation Time: 0000\n"
)


def _setup():
    d = tempfile.mkdtemp()
    con = get_db(os.path.join(d, "t.duckdb"))
    ensure_schema(con)
    return con


def test_etf_flagged_not_common_equity():
    con = _setup()
    build_issuer_master_from_fixtures(con, MOCK_TICKERS, MOCK_EXCHANGE, MOCK_NASDAQ, "")
    rows = con.execute(
        "SELECT is_etf, is_common_equity FROM issuer_master WHERE ticker_current='SPYUS'"
    ).fetchall()
    assert rows, "SPYUS not in issuer_master"
    assert rows[0][0] is True
    assert rows[0][1] is False
    con.close()


def test_common_stock_flagged_eligible():
    con = _setup()
    build_issuer_master_from_fixtures(con, MOCK_TICKERS, MOCK_EXCHANGE, MOCK_NASDAQ, "")
    rows = con.execute(
        "SELECT is_common_equity FROM issuer_master WHERE ticker_current='AAPL'"
    ).fetchall()
    assert rows[0][0] is True
    con.close()


def test_aliases_populated_for_apple():
    con = _setup()
    build_issuer_master_from_fixtures(con, MOCK_TICKERS, MOCK_EXCHANGE, MOCK_NASDAQ, "")
    aliases = con.execute(
        "SELECT alias_normalized_conservative FROM issuer_aliases "
        "WHERE alias_normalized_conservative LIKE 'APPLE%'"
    ).fetchall()
    assert len(aliases) >= 1
    con.close()


def test_is_eligible_common_equity():
    assert is_eligible_common_equity("Apple Inc. - Common Stock", "Nasdaq") is True
    assert is_eligible_common_equity("SPDR S&P 500 ETF Trust", "NYSEArca") is False
    assert is_eligible_common_equity("Boeing Preferred Stock Series A", "NYSE") is False
